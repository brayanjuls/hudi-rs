/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
//! File-group optimistic concurrency control (Hudi RFC-22).
//!
//! A writer captures the latest completed action at request time, performs its
//! data work without the table lock, then reacquires the lock and compares its
//! `(partition path, file id)` write set with every action completed after that
//! snapshot. Deletes use those file-group conflicts. Dynamic partition
//! overwrites conflict with every write into a target partition, and full-table
//! overwrites conflict with every concurrent data write.

use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use crate::Result;
use crate::config::write::{WriteConcurrencyConfig, WriteConcurrencyMode};
use crate::error::CoreError;
use crate::metadata::commit::HoodieCommitMetadata;
use crate::metadata::replace_commit::HoodieReplaceCommitMetadata;
use crate::table::Table;
use crate::timeline::instant::{Action, Instant, State};

#[derive(Debug)]
struct WriterHeartbeat {
    stopped: AtomicBool,
    expired: AtomicBool,
    last_success_ms: AtomicI64,
    tolerance_ms: i64,
    stop_signal: tokio::sync::Notify,
}

#[derive(Clone, Debug)]
struct Snapshot {
    completion_watermark: Option<String>,
    heartbeat: Arc<WriterHeartbeat>,
}

#[derive(Clone, Debug)]
struct CompletedAction {
    requested: String,
    completion: String,
    action: Action,
    metadata: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct WriteSet(HashSet<(String, String)>);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct WriteIntent {
    operation_type: Option<String>,
    write_set: WriteSet,
    touched_partitions: HashSet<String>,
    schema: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ConflictScope {
    Table,
    Partitions(Vec<String>),
    FileGroups(Vec<(String, String)>),
}

fn transactions() -> &'static Mutex<HashMap<String, Snapshot>> {
    static TRANSACTIONS: OnceLock<Mutex<HashMap<String, Snapshot>>> = OnceLock::new();
    TRANSACTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn transaction_key(table: &Table, instant: &str) -> String {
    format!("{}#{instant}", table.file_system_view.storage.base_url)
}

fn heartbeat_path(instant: &str) -> String {
    format!(".hoodie/.heartbeat/{instant}")
}

/// Owns a fenced instant for the duration of a write operation.
///
/// An error path drops this guard, stops refreshing the standard Hudi
/// heartbeat, and deliberately leaves the pending files for LAZY recovery.
/// Successful commit/abort paths remove the registry entry first.
pub(crate) struct PendingInstant {
    instant: String,
    table: Table,
}

impl Deref for PendingInstant {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.instant
    }
}

impl std::fmt::Display for PendingInstant {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.instant)
    }
}

impl Drop for PendingInstant {
    fn drop(&mut self) {
        abandon_transaction(&self.table, &self.instant);
    }
}

fn lock_transactions() -> std::sync::MutexGuard<'static, HashMap<String, Snapshot>> {
    match transactions().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn enabled(table: &Table) -> Result<bool> {
    Ok(
        WriteConcurrencyConfig::from_configs(&table.hudi_configs)?.mode
            == WriteConcurrencyMode::OptimisticConcurrencyControl,
    )
}

/// Capture and retain the writer's completed-action snapshot.
///
/// This is called while holding critical section 1, before the requested and
/// inflight timeline files are published.
pub(crate) async fn begin_transaction(table: &mut Table, instant: &str) -> Result<PendingInstant> {
    if !enabled(table)? {
        return Ok(PendingInstant {
            instant: instant.to_string(),
            table: table.clone(),
        });
    }
    // CS1 owns the table lock here. Refresh the exact timeline and file-system
    // view that all subsequent planning will use before capturing the OCC
    // watermark. Otherwise C1 can land between the caller's initial reload and
    // this snapshot, producing a C1 watermark with a stale C0 write plan.
    validate_pending_table_services(table).await?;
    crate::write::rollback::rollback_expired_failed_writes(table).await?;
    table.timeline.reload_completed_commits().await?;
    table.file_system_view.clear_cache();
    let completion_watermark = load_completion_watermark(table).await?;
    let config = WriteConcurrencyConfig::from_configs(&table.hudi_configs)?;
    let heartbeat = start_heartbeat(
        table,
        instant,
        Duration::from_millis(config.client_heartbeat_interval_ms as u64),
        config.client_heartbeat_tolerable_misses,
    )
    .await?;
    lock_transactions().insert(
        transaction_key(table, instant),
        Snapshot {
            completion_watermark,
            heartbeat,
        },
    );
    Ok(PendingInstant {
        instant: instant.to_string(),
        table: table.clone(),
    })
}

/// Forget a completed or aborted writer transaction.
pub(crate) async fn finish_transaction(table: &Table, instant: &str) {
    let snapshot = { lock_transactions().remove(&transaction_key(table, instant)) };
    if let Some(snapshot) = snapshot {
        snapshot.heartbeat.stopped.store(true, Ordering::Release);
        snapshot.heartbeat.stop_signal.notify_one();
        let _ = table
            .file_system_view
            .storage
            .delete_file(&heartbeat_path(instant))
            .await;
    }
}

fn abandon_transaction(table: &Table, instant: &str) {
    if let Some(snapshot) = lock_transactions().remove(&transaction_key(table, instant)) {
        snapshot.heartbeat.stopped.store(true, Ordering::Release);
        snapshot.heartbeat.stop_signal.notify_one();
        // Keep the last heartbeat object. Its age provides the interoperable
        // grace period before Java or Rust LAZY cleanup rolls this instant back.
    }
}

/// Validate a pending commit against actions completed since its snapshot.
///
/// The caller must hold and validate the table lock for the entire check and
/// subsequent timeline publication.
pub(crate) async fn validate_transaction(
    table: &Table,
    instant: &str,
    action: Action,
    metadata: &[u8],
) -> Result<()> {
    if !enabled(table)? {
        return Ok(());
    }
    let snapshot = lock_transactions()
        .get(&transaction_key(table, instant))
        .cloned()
        .ok_or_else(|| {
            CoreError::Write(format!(
                "missing OCC snapshot for pending instant {instant}; refusing an unsafe commit"
            ))
        })?;
    let now_ms = chrono::Utc::now().timestamp_millis();
    if snapshot.heartbeat.expired.load(Ordering::Acquire)
        || now_ms.saturating_sub(snapshot.heartbeat.last_success_ms.load(Ordering::Acquire))
            > snapshot.heartbeat.tolerance_ms
    {
        return Err(CoreError::Write(format!(
            "writer heartbeat expired for pending instant {instant}"
        )));
    }
    let remote_heartbeat = table
        .file_system_view
        .storage
        .file_last_modified(&heartbeat_path(instant))
        .await
        .map_err(|error| {
            CoreError::Write(format!(
                "writer heartbeat for pending instant {instant} is missing or unreadable: {error}"
            ))
        })?;
    let remote_is_expired = remote_heartbeat.is_none_or(|last_modified| {
        chrono::Utc::now()
            .signed_duration_since(last_modified)
            .num_milliseconds()
            > snapshot.heartbeat.tolerance_ms
    });
    if remote_is_expired {
        return Err(CoreError::Write(format!(
            "writer heartbeat expired in storage for pending instant {instant}"
        )));
    }
    let requested = format!(
        "{}/{instant}.{}.requested",
        crate::write::append::timeline_dir(table),
        action.as_ref()
    );
    table
        .file_system_view
        .storage
        .get_file_data(&requested)
        .await
        .map_err(|error| {
            CoreError::Write(format!(
                "timeline fence for pending instant {instant} is missing or unreadable: {error}"
            ))
        })?;
    let current = WriteIntent::from_metadata(&action, metadata)?;

    let mut completed =
        load_completed_actions_after(table, snapshot.completion_watermark.as_deref()).await?;
    completed.sort_by(|left, right| left.completion.cmp(&right.completion));
    for candidate in completed {
        if candidate.requested == instant
            || snapshot
                .completion_watermark
                .as_ref()
                .is_some_and(|watermark| candidate.completion <= *watermark)
        {
            continue;
        }
        let candidate_intent = WriteIntent::from_metadata(&candidate.action, &candidate.metadata)?;
        if current.schema.is_some()
            && candidate_intent.schema.is_some()
            && current.schema != candidate_intent.schema
        {
            let conflict = CoreError::WriteConflict(format!(
                "instant {instant} has a schema incompatible with action {} completed at {}",
                candidate.requested, candidate.completion
            ));
            if let Err(error) =
                crate::write::rollback::rollback_failed_instant(table, instant).await
            {
                log::warn!("failed to roll back schema-conflicting instant {instant}: {error}");
            }
            finish_transaction(table, instant).await;
            return Err(conflict);
        }
        let Some(scope) = current.conflict_scope(&candidate_intent) else {
            continue;
        };
        let scope = match scope {
            ConflictScope::Table => "because one operation replaces the full table".to_string(),
            ConflictScope::Partitions(partitions) => format!(
                "in overwrite target partition(s) {}",
                partitions
                    .into_iter()
                    .map(|partition| format!("{partition:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            ConflictScope::FileGroups(groups) => format!(
                "on file groups {}",
                groups
                    .into_iter()
                    .map(|(partition, file_id)| format!("({partition:?}, {file_id})"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        };
        let conflict = CoreError::WriteConflict(format!(
            "instant {instant} ({}) conflicts with action {} ({}) completed at {} {scope}",
            current.operation_name(),
            candidate.requested,
            candidate_intent.operation_name(),
            candidate.completion
        ));
        if let Err(error) = crate::write::rollback::rollback_failed_instant(table, instant).await {
            log::warn!("failed to roll back conflicting instant {instant}: {error}");
        }
        finish_transaction(table, instant).await;
        return Err(conflict);
    }
    Ok(())
}

fn normalized_schema(extra_metadata: Option<&HashMap<String, String>>) -> Option<String> {
    extra_metadata
        .and_then(|metadata| metadata.get("schema"))
        .map(|schema| {
            serde_json::from_str::<serde_json::Value>(schema)
                .map(|value| value.to_string())
                .unwrap_or_else(|_| schema.trim().to_string())
        })
}

async fn validate_pending_table_services(table: &Table) -> Result<()> {
    let storage = table.file_system_view.storage.as_ref();
    let timeline_dir = crate::write::append::timeline_dir(table);
    for file in storage.list_files(Some(&timeline_dir)).await? {
        let name = file.name;
        if !(name.ends_with(".requested") || name.ends_with(".inflight")) {
            continue;
        }
        if name.contains(".compaction.") || name.contains(".logcompaction.") {
            return Err(CoreError::WriteConflict(format!(
                "unsupported pending table service {name}; refusing to plan an OCC write"
            )));
        }
        if !name.ends_with(".replacecommit.requested") {
            continue;
        }
        let bytes = storage
            .get_file_data(&format!("{timeline_dir}/{name}"))
            .await?;
        let requested =
            crate::metadata::replace_commit::HoodieRequestedReplaceMetadata::from_avro_bytes(
                &bytes,
            )?;
        if requested.clustering_plan.is_some() {
            return Err(CoreError::WriteConflict(format!(
                "pending clustering plan {name} is not supported by the Rust OCC writer"
            )));
        }
    }
    Ok(())
}

async fn start_heartbeat(
    table: &Table,
    instant: &str,
    interval: Duration,
    tolerable_misses: usize,
) -> Result<Arc<WriterHeartbeat>> {
    let path = heartbeat_path(instant);
    let instant = instant.to_string();
    let storage = table.file_system_view.storage.clone();
    storage.put_file(&path, Vec::new()).await?;
    let tolerance_ms = i64::try_from(interval.as_millis())
        .unwrap_or(i64::MAX)
        .saturating_mul(i64::try_from(tolerable_misses).unwrap_or(i64::MAX));
    let heartbeat = Arc::new(WriterHeartbeat {
        stopped: AtomicBool::new(false),
        expired: AtomicBool::new(false),
        last_success_ms: AtomicI64::new(chrono::Utc::now().timestamp_millis()),
        tolerance_ms,
        stop_signal: tokio::sync::Notify::new(),
    });
    let task_state = heartbeat.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = task_state.stop_signal.notified() => return,
            }
            if task_state.stopped.load(Ordering::Acquire) {
                return;
            }
            match storage.put_file(&path, Vec::new()).await {
                Ok(()) => task_state
                    .last_success_ms
                    .store(chrono::Utc::now().timestamp_millis(), Ordering::Release),
                Err(error) => {
                    let elapsed = chrono::Utc::now()
                        .timestamp_millis()
                        .saturating_sub(task_state.last_success_ms.load(Ordering::Acquire));
                    log::warn!("writer heartbeat update failed for {instant}: {error}");
                    if elapsed > tolerance_ms {
                        task_state.expired.store(true, Ordering::Release);
                        return;
                    }
                }
            }
        }
    });
    Ok(heartbeat)
}

impl WriteIntent {
    fn from_metadata(action: &Action, bytes: &[u8]) -> Result<Self> {
        let mut intent = Self::default();
        match action {
            Action::Commit | Action::DeltaCommit => {
                let metadata = HoodieCommitMetadata::from_avro_bytes(bytes)
                    .or_else(|_| HoodieCommitMetadata::from_json_bytes(bytes))?;
                intent.operation_type = metadata.operation_type;
                intent.schema = normalized_schema(metadata.extra_metadata.as_ref());
                intent.add_write_stats(metadata.partition_to_write_stats.as_ref());
            }
            Action::ReplaceCommit => {
                let metadata = HoodieReplaceCommitMetadata::from_avro_bytes(bytes)
                    .or_else(|_| HoodieReplaceCommitMetadata::from_json_bytes(bytes))?;
                intent.operation_type = metadata.operation_type;
                intent.schema = normalized_schema(metadata.extra_metadata.as_ref());
                intent.add_write_stats(metadata.partition_to_write_stats.as_ref());
                if let Some(replaced) = metadata.partition_to_replace_file_ids {
                    for (partition, file_ids) in replaced {
                        intent.touched_partitions.insert(partition.clone());
                        for file_id in file_ids {
                            if !file_id.is_empty() {
                                intent.write_set.0.insert((partition.clone(), file_id));
                            }
                        }
                    }
                }
            }
        }
        Ok(intent)
    }

    fn add_write_stats(
        &mut self,
        stats: Option<&HashMap<String, Vec<crate::metadata::commit::HoodieWriteStat>>>,
    ) {
        for (partition, stats) in stats.into_iter().flatten() {
            self.touched_partitions.insert(partition.clone());
            for stat in stats {
                if let Some(file_id) = stat.file_id.as_ref().filter(|file_id| !file_id.is_empty()) {
                    self.write_set
                        .0
                        .insert((partition.clone(), file_id.clone()));
                }
            }
        }
    }

    fn is_operation(&self, expected: &str) -> bool {
        self.operation_type
            .as_deref()
            .is_some_and(|operation| operation.eq_ignore_ascii_case(expected))
    }

    fn is_table_overwrite(&self) -> bool {
        self.is_operation("INSERT_OVERWRITE_TABLE")
    }

    fn is_partition_overwrite(&self) -> bool {
        self.is_operation("INSERT_OVERWRITE")
    }

    fn operation_name(&self) -> &str {
        self.operation_type.as_deref().unwrap_or("UNKNOWN")
    }

    /// Return the strongest overlapping scope between two writes.
    ///
    /// Apache Hudi's default strategy intersects file groups. Overwrite intent
    /// needs a wider scope: a writer can create a brand-new group after the
    /// overwrite snapshot, so that group cannot appear in the replace list.
    fn conflict_scope(&self, other: &Self) -> Option<ConflictScope> {
        if self.is_table_overwrite() || other.is_table_overwrite() {
            return Some(ConflictScope::Table);
        }

        if self.is_partition_overwrite() || other.is_partition_overwrite() {
            let overwrite_partitions = if self.is_partition_overwrite() {
                &self.touched_partitions
            } else {
                &other.touched_partitions
            };
            let other_partitions = if self.is_partition_overwrite() {
                &other.touched_partitions
            } else {
                &self.touched_partitions
            };
            let mut overlap: Vec<_> = overwrite_partitions
                .intersection(other_partitions)
                .cloned()
                .collect();
            if !overlap.is_empty() {
                overlap.sort();
                return Some(ConflictScope::Partitions(overlap));
            }
        }

        let mut overlap: Vec<_> = self
            .write_set
            .0
            .intersection(&other.write_set.0)
            .cloned()
            .collect();
        if overlap.is_empty() {
            None
        } else {
            overlap.sort();
            Some(ConflictScope::FileGroups(overlap))
        }
    }
}

async fn load_completion_watermark(table: &Table) -> Result<Option<String>> {
    let storage = table.file_system_view.storage.as_ref();
    let timeline_dir = crate::write::append::timeline_dir(table);
    let mut watermark = None;
    for file in storage.list_files(Some(&timeline_dir)).await? {
        let Ok(instant) = Instant::try_from_file_name_and_timezone(&file.name, &table.timezone())
        else {
            continue;
        };
        if instant.state != State::Completed {
            continue;
        }
        let completion = instant
            .completion_timestamp
            .unwrap_or_else(|| instant.timestamp.clone());
        if watermark
            .as_ref()
            .is_none_or(|current| completion > *current)
        {
            watermark = Some(completion);
        }
    }
    if watermark.is_none() {
        watermark = crate::write::archival::archived_instant_records(storage, &timeline_dir)
            .await?
            .into_iter()
            .map(|record| record.completion)
            .max();
    }
    Ok(watermark)
}

async fn load_completed_actions_after(
    table: &Table,
    watermark: Option<&str>,
) -> Result<Vec<CompletedAction>> {
    let storage = table.file_system_view.storage.as_ref();
    let timeline_dir = crate::write::append::timeline_dir(table);
    let mut actions = HashMap::<(String, String), CompletedAction>::new();
    for file in storage.list_files(Some(&timeline_dir)).await? {
        if !file
            .name
            .chars()
            .next()
            .is_some_and(|first| first.is_ascii_digit())
        {
            continue;
        }
        let instant = match Instant::try_from_file_name_and_timezone(&file.name, &table.timezone())
        {
            Ok(instant) if instant.state == State::Completed => instant,
            _ => continue,
        };
        let completion = instant
            .completion_timestamp
            .clone()
            .unwrap_or_else(|| instant.timestamp.clone());
        if watermark.is_some_and(|watermark| completion.as_str() <= watermark) {
            continue;
        }
        let metadata = storage
            .get_file_data(&format!("{timeline_dir}/{}", file.name))
            .await?
            .to_vec();
        actions.insert(
            (
                instant.timestamp.clone(),
                instant.action.as_ref().to_string(),
            ),
            CompletedAction {
                requested: instant.timestamp,
                completion,
                action: instant.action,
                metadata,
            },
        );
    }
    for record in crate::write::archival::archived_instant_records(storage, &timeline_dir).await? {
        if watermark.is_some_and(|watermark| record.completion.as_str() <= watermark) {
            continue;
        }
        let Ok(action) = Action::from_str(&record.action) else {
            continue;
        };
        actions.insert(
            (record.requested.clone(), record.action),
            CompletedAction {
                requested: record.requested,
                completion: record.completion,
                action,
                metadata: record.metadata,
            },
        );
    }
    Ok(actions.into_values().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HudiConfigs;
    use crate::config::table::HudiTableConfig;
    use crate::config::write::{HudiWriteConfig, STORAGE_BASED_LOCK_PROVIDER_CLASS};
    use crate::metadata::commit::HoodieWriteStat;
    use crate::storage::Storage;
    use object_store::memory::InMemory;
    use std::sync::Arc;
    use url::Url;

    fn write_stats(groups: &[(&str, &str)]) -> HashMap<String, Vec<HoodieWriteStat>> {
        let mut stats = HashMap::<String, Vec<HoodieWriteStat>>::new();
        for (partition, file_id) in groups {
            stats
                .entry((*partition).to_string())
                .or_default()
                .push(HoodieWriteStat {
                    file_id: Some((*file_id).to_string()),
                    ..Default::default()
                });
        }
        stats
    }

    fn commit(groups: &[(&str, &str)]) -> Vec<u8> {
        HoodieCommitMetadata {
            partition_to_write_stats: Some(write_stats(groups)),
            ..Default::default()
        }
        .to_json_bytes()
        .unwrap()
    }

    fn commit_with_operation(groups: &[(&str, &str)], operation: &str) -> Vec<u8> {
        HoodieCommitMetadata {
            partition_to_write_stats: Some(write_stats(groups)),
            operation_type: Some(operation.to_string()),
            ..Default::default()
        }
        .to_json_bytes()
        .unwrap()
    }

    fn replace_commit(
        operation: &str,
        writes: &[(&str, &str)],
        replaced: &[(&str, &[&str])],
    ) -> Vec<u8> {
        HoodieReplaceCommitMetadata {
            partition_to_write_stats: Some(write_stats(writes)),
            operation_type: Some(operation.to_string()),
            partition_to_replace_file_ids: Some(
                replaced
                    .iter()
                    .map(|(partition, file_ids)| {
                        (
                            (*partition).to_string(),
                            file_ids
                                .iter()
                                .map(|file_id| (*file_id).to_string())
                                .collect(),
                        )
                    })
                    .collect(),
            ),
            ..Default::default()
        }
        .to_json_bytes()
        .unwrap()
    }

    fn commit_with_schema(groups: &[(&str, &str)], schema: &str) -> Vec<u8> {
        let mut metadata = HoodieCommitMetadata::from_json_bytes(&commit(groups)).unwrap();
        metadata.extra_metadata = Some(HashMap::from([("schema".to_string(), schema.to_string())]));
        metadata.to_json_bytes().unwrap()
    }

    #[test]
    fn write_sets_conflict_only_on_partition_and_file_id() {
        let first = WriteIntent::from_metadata(&Action::Commit, &commit(&[("p1", "f1")])).unwrap();
        let same = WriteIntent::from_metadata(&Action::Commit, &commit(&[("p1", "f1")])).unwrap();
        let other_partition =
            WriteIntent::from_metadata(&Action::Commit, &commit(&[("p2", "f1")])).unwrap();
        assert_eq!(
            first.conflict_scope(&same),
            Some(ConflictScope::FileGroups(vec![(
                "p1".to_string(),
                "f1".to_string()
            )]))
        );
        assert_eq!(first.conflict_scope(&other_partition), None);
    }

    #[test]
    fn replace_file_ids_join_the_write_set() {
        let metadata = HoodieReplaceCommitMetadata {
            partition_to_replace_file_ids: Some(HashMap::from([(
                "p".to_string(),
                vec!["old-file".to_string()],
            )])),
            ..Default::default()
        }
        .to_json_bytes()
        .unwrap();
        let intent = WriteIntent::from_metadata(&Action::ReplaceCommit, &metadata).unwrap();
        assert!(
            intent
                .write_set
                .0
                .contains(&("p".to_string(), "old-file".to_string()))
        );
    }

    #[test]
    fn full_table_overwrite_conflicts_with_every_data_write() {
        let overwrite = WriteIntent::from_metadata(
            &Action::ReplaceCommit,
            &replace_commit(
                "insert_overwrite_table",
                &[("p1", "replacement")],
                &[("p1", &["old-file"])],
            ),
        )
        .unwrap();
        let append = WriteIntent::from_metadata(
            &Action::Commit,
            &commit_with_operation(&[("brand-new-partition", "brand-new-file")], "INSERT"),
        )
        .unwrap();
        assert_eq!(
            overwrite.conflict_scope(&append),
            Some(ConflictScope::Table)
        );
        assert_eq!(
            append.conflict_scope(&overwrite),
            Some(ConflictScope::Table)
        );
    }

    #[test]
    fn dynamic_overwrite_conflicts_by_partition_not_only_existing_file_id() {
        let overwrite = WriteIntent::from_metadata(
            &Action::ReplaceCommit,
            &replace_commit(
                "INSERT_OVERWRITE",
                &[("target", "replacement")],
                &[("target", &["old-file"])],
            ),
        )
        .unwrap();
        let target_append = WriteIntent::from_metadata(
            &Action::Commit,
            &commit_with_operation(&[("target", "brand-new-file")], "INSERT"),
        )
        .unwrap();
        let other_append = WriteIntent::from_metadata(
            &Action::Commit,
            &commit_with_operation(&[("other", "brand-new-file")], "INSERT"),
        )
        .unwrap();
        assert_eq!(
            overwrite.conflict_scope(&target_append),
            Some(ConflictScope::Partitions(vec!["target".to_string()]))
        );
        assert_eq!(overwrite.conflict_scope(&other_append), None);
    }

    #[test]
    fn delete_conflicts_only_with_the_same_file_group() {
        let delete = WriteIntent::from_metadata(
            &Action::Commit,
            &commit_with_operation(&[("p", "file-1")], "DELETE"),
        )
        .unwrap();
        let same_group = WriteIntent::from_metadata(
            &Action::Commit,
            &commit_with_operation(&[("p", "file-1")], "UPSERT"),
        )
        .unwrap();
        let other_group = WriteIntent::from_metadata(
            &Action::Commit,
            &commit_with_operation(&[("p", "file-2")], "UPSERT"),
        )
        .unwrap();
        assert!(matches!(
            delete.conflict_scope(&same_group),
            Some(ConflictScope::FileGroups(_))
        ));
        assert_eq!(delete.conflict_scope(&other_group), None);
    }

    async fn occ_table() -> Table {
        let configs = Arc::new(HudiConfigs::new([
            (HudiTableConfig::BasePath.as_ref(), "memory:///table"),
            (HudiTableConfig::TableVersion.as_ref(), "8"),
            (HudiTableConfig::TimelineLayoutVersion.as_ref(), "2"),
            (HudiTableConfig::TimelinePath.as_ref(), "timeline"),
            (HudiWriteConfig::ConcurrencyMode.as_ref(), "occ"),
            (
                HudiWriteConfig::LockProvider.as_ref(),
                STORAGE_BASED_LOCK_PROVIDER_CLASS,
            ),
            (HudiWriteConfig::FailedWritesCleaningPolicy.as_ref(), "LAZY"),
        ]));
        let storage = Storage::new_with_object_store(
            Url::parse("memory:///table").unwrap(),
            Arc::new(InMemory::new()),
        );
        Table::new_with_storage_for_test(configs, storage)
            .await
            .unwrap()
    }

    async fn begin_action(table: &mut Table, instant: &str, action: Action) -> PendingInstant {
        let transaction = begin_transaction(table, instant).await.unwrap();
        table
            .file_system_view
            .storage
            .put_file(
                &format!(".hoodie/timeline/{instant}.{}.requested", action.as_ref()),
                Vec::new(),
            )
            .await
            .unwrap();
        transaction
    }

    async fn begin_commit(table: &mut Table, instant: &str) -> PendingInstant {
        begin_action(table, instant, Action::Commit).await
    }

    #[tokio::test]
    async fn later_writer_aborts_when_an_earlier_writer_commits_same_file_group() {
        let mut table = occ_table().await;
        let first = "20260824000000000";
        let second = "20260824000000001";
        let first_transaction = begin_commit(&mut table, first).await;
        let second_transaction = begin_commit(&mut table, second).await;

        let first_bytes = commit(&[("p", "file-1")]);
        validate_transaction(&table, &first_transaction, Action::Commit, &first_bytes)
            .await
            .unwrap();
        table
            .file_system_view
            .storage
            .put_file(
                &format!(".hoodie/timeline/{first}_20260824000000002.commit"),
                first_bytes,
            )
            .await
            .unwrap();
        finish_transaction(&table, &first_transaction).await;

        let error = validate_transaction(
            &table,
            &second_transaction,
            Action::Commit,
            &commit(&[("p", "file-1")]),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, CoreError::WriteConflict(_)));
    }

    #[tokio::test]
    async fn concurrent_disjoint_file_groups_both_validate() {
        let mut table = occ_table().await;
        let first = "20260824000000100";
        let second = "20260824000000101";
        let first_transaction = begin_commit(&mut table, first).await;
        let second_transaction = begin_commit(&mut table, second).await;

        let first_bytes = commit(&[("p", "file-1")]);
        table
            .file_system_view
            .storage
            .put_file(
                &format!(".hoodie/timeline/{first}_20260824000000102.commit"),
                first_bytes,
            )
            .await
            .unwrap();
        finish_transaction(&table, &first_transaction).await;
        validate_transaction(
            &table,
            &second_transaction,
            Action::Commit,
            &commit(&[("p", "file-2")]),
        )
        .await
        .unwrap();
        finish_transaction(&table, &second_transaction).await;
    }

    #[tokio::test]
    async fn concurrent_full_overwrite_rejects_a_brand_new_file_group() {
        let mut table = occ_table().await;
        let append_instant = "20260824000000110";
        let overwrite_instant = "20260824000000111";
        let append_transaction = begin_commit(&mut table, append_instant).await;
        let overwrite_transaction =
            begin_action(&mut table, overwrite_instant, Action::ReplaceCommit).await;

        let append_bytes = commit_with_operation(&[("new-partition", "new-file")], "INSERT");
        table
            .file_system_view
            .storage
            .put_file(
                &format!(".hoodie/timeline/{append_instant}_20260824000000112.commit"),
                append_bytes,
            )
            .await
            .unwrap();
        finish_transaction(&table, &append_transaction).await;

        let error = validate_transaction(
            &table,
            &overwrite_transaction,
            Action::ReplaceCommit,
            &replace_commit(
                "INSERT_OVERWRITE_TABLE",
                &[("p", "replacement")],
                &[("p", &["old-file"])],
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, CoreError::WriteConflict(_)));
        assert!(error.to_string().contains("replaces the full table"));
    }

    #[tokio::test]
    async fn concurrent_dynamic_overwrite_rejects_new_group_in_target_partition() {
        let mut table = occ_table().await;
        let append_instant = "20260824000000120";
        let overwrite_instant = "20260824000000121";
        let append_transaction = begin_commit(&mut table, append_instant).await;
        let overwrite_transaction =
            begin_action(&mut table, overwrite_instant, Action::ReplaceCommit).await;

        table
            .file_system_view
            .storage
            .put_file(
                &format!(".hoodie/timeline/{append_instant}_20260824000000122.commit"),
                commit_with_operation(&[("target", "new-file")], "INSERT"),
            )
            .await
            .unwrap();
        finish_transaction(&table, &append_transaction).await;

        let error = validate_transaction(
            &table,
            &overwrite_transaction,
            Action::ReplaceCommit,
            &replace_commit(
                "INSERT_OVERWRITE",
                &[("target", "replacement")],
                &[("target", &["old-file"])],
            ),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, CoreError::WriteConflict(_)));
        assert!(error.to_string().contains("target partition"));
    }

    #[tokio::test]
    async fn conflict_detection_follows_a_commit_into_lsm_history() {
        let mut table = occ_table().await;
        let first = "20260824000000200";
        let second = "20260824000000201";
        let second_transaction = begin_commit(&mut table, second).await;

        table
            .file_system_view
            .storage
            .put_file(
                &format!(".hoodie/timeline/{first}_20260824000000202.commit"),
                commit(&[("p", "file-1")]),
            )
            .await
            .unwrap();
        crate::write::archival::archive_timeline_if_needed(
            table.file_system_view.storage.as_ref(),
            ".hoodie/timeline",
            0,
            0,
        )
        .await
        .unwrap();

        let error = validate_transaction(
            &table,
            &second_transaction,
            Action::Commit,
            &commit(&[("p", "file-1")]),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, CoreError::WriteConflict(_)));
    }

    #[tokio::test]
    async fn disjoint_file_groups_with_incompatible_schemas_conflict() {
        let mut table = occ_table().await;
        let first = "20260824000000300";
        let second = "20260824000000301";
        let first_transaction = begin_commit(&mut table, first).await;
        let second_transaction = begin_commit(&mut table, second).await;
        table
            .file_system_view
            .storage
            .put_file(
                &format!(".hoodie/timeline/{first}_20260824000000302.commit"),
                commit_with_schema(&[("p", "file-1")], r#"{"type":"record","name":"a"}"#),
            )
            .await
            .unwrap();
        finish_transaction(&table, &first_transaction).await;
        let error = validate_transaction(
            &table,
            &second_transaction,
            Action::Commit,
            &commit_with_schema(&[("p", "file-2")], r#"{"type":"record","name":"b"}"#),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, CoreError::WriteConflict(_)));
    }

    #[tokio::test]
    async fn standard_heartbeat_lives_for_pending_transaction_and_is_deleted_on_finish() {
        let mut table = occ_table().await;
        let instant = "20260824000000400";
        let transaction = begin_commit(&mut table, instant).await;
        assert!(
            table
                .file_system_view
                .storage
                .exists(&heartbeat_path(instant))
                .await
                .unwrap()
        );
        finish_transaction(&table, &transaction).await;
        assert!(
            !table
                .file_system_view
                .storage
                .exists(&heartbeat_path(instant))
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn cs1_refreshes_timeline_before_capturing_snapshot() {
        let mut table = occ_table().await;
        let completed = "20260824000000500";
        table
            .file_system_view
            .storage
            .put_file(
                &format!(".hoodie/timeline/{completed}_20260824000000501.commit"),
                commit(&[("p", "file-1")]),
            )
            .await
            .unwrap();
        assert!(
            table
                .timeline
                .get_latest_commit_timestamp_as_option()
                .is_none()
        );
        let transaction = begin_commit(&mut table, "20260824000000502").await;
        assert_eq!(
            table.timeline.get_latest_commit_timestamp_as_option(),
            Some(completed)
        );
        finish_transaction(&table, &transaction).await;
    }

    #[tokio::test]
    async fn pending_clustering_plan_fails_closed() {
        let mut table = occ_table().await;
        let requested = crate::metadata::replace_commit::HoodieRequestedReplaceMetadata {
            clustering_plan: Some(serde_json::json!({"inputGroups": []})),
            ..Default::default()
        };
        table
            .file_system_view
            .storage
            .put_file(
                ".hoodie/timeline/20260824000000600.replacecommit.requested",
                requested.to_avro_bytes().unwrap(),
            )
            .await
            .unwrap();
        let error = begin_transaction(&mut table, "20260824000000601")
            .await
            .err()
            .expect("pending clustering must reject OCC planning");
        assert!(matches!(error, CoreError::WriteConflict(_)));
    }
}
