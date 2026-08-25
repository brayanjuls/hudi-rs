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
//! Write-path locking (Java `LockProvider` / `TransactionManager`).
//!
//! Timeline mutations happen in short critical sections. Single-writer tables
//! use [`InProcessLockProvider`]. Multi-writer tables use
//! [`StorageBasedLockProvider`], which follows Hudi RFC-91: a lease represented
//! by `.hoodie/.locks/table_lock.json` and acquired with an atomic create or
//! compare-and-swap update.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use crate::Result;
use crate::config::table::HudiTableConfig;
use crate::config::write::{WriteConcurrencyConfig, WriteLockProvider};
use crate::error::CoreError;
use crate::storage::{ConditionalPutResult, Storage};
use crate::table::Table;

const TABLE_LOCK_PATH: &str = ".hoodie/.locks/table_lock.json";
const CLOCK_DRIFT_BUFFER_MS: i64 = 500;

type LeaseFuture = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
type LeaseCheck = Arc<dyn Fn() -> LeaseFuture + Send + Sync>;
type LeaseRelease = Box<dyn FnOnce() -> LeaseFuture + Send>;

/// A held table lock.
///
/// Call [`LockLease::validate`] immediately before publishing a timeline
/// mutation and [`LockLease::release`] when the critical section is complete.
/// Dropping a storage lease schedules a best-effort asynchronous release.
pub struct LockLease {
    inner: Option<Box<dyn std::any::Any + Send>>,
    check: LeaseCheck,
    release: Option<LeaseRelease>,
}

impl std::fmt::Debug for LockLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LockLease")
            .field("is_storage_lease", &self.release.is_some())
            .finish_non_exhaustive()
    }
}

impl LockLease {
    fn in_process(guard: tokio::sync::OwnedMutexGuard<()>) -> Self {
        Self {
            inner: Some(Box::new(guard)),
            check: Arc::new(|| Box::pin(async { Ok(()) })),
            release: None,
        }
    }

    fn storage(check: LeaseCheck, release: LeaseRelease) -> Self {
        Self {
            inner: None,
            check,
            release: Some(release),
        }
    }

    /// Confirm that this writer still owns a non-expired distributed lease.
    pub async fn validate(&self) -> Result<()> {
        (self.check)().await
    }

    /// Release this lease and wait until the release attempt has completed.
    pub async fn release(mut self) -> Result<()> {
        self.inner.take();
        match self.release.take() {
            Some(release) => release().await,
            None => Ok(()),
        }
    }
}

impl Drop for LockLease {
    fn drop(&mut self) {
        self.inner.take();
        let Some(release) = self.release.take() else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Err(error) = release().await {
                    log::warn!("failed to release storage lock after dropping its lease: {error}");
                }
            });
        }
    }
}

/// Mutual exclusion for timeline-mutating critical sections.
pub trait LockProvider: Send + Sync + std::fmt::Debug {
    /// Acquire the table lock, waiting up to the configured deadline.
    fn lock(&self) -> BoxFuture<'_, Result<LockLease>>;
}

/// One lock per table base path, shared by every writer in this process.
#[derive(Debug)]
pub struct InProcessLockProvider {
    base_path: String,
}

impl InProcessLockProvider {
    /// Create a process-local provider for `base_path`.
    pub fn new(base_path: impl Into<String>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }

    fn lock_for_base_path(&self) -> Arc<tokio::sync::Mutex<()>> {
        static LOCKS: OnceLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> =
            OnceLock::new();
        let registry = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let mut map = match registry.lock() {
            Ok(map) => map,
            Err(poisoned) => poisoned.into_inner(),
        };
        map.entry(self.base_path.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

impl LockProvider for InProcessLockProvider {
    fn lock(&self) -> BoxFuture<'_, Result<LockLease>> {
        let lock = self.lock_for_base_path();
        Box::pin(async move { Ok(LockLease::in_process(lock.lock_owned().await)) })
    }
}

/// RFC-91 compatible conditional-write lease stored alongside table metadata.
#[derive(Clone, Debug)]
pub struct StorageBasedLockProvider {
    storage: Arc<Storage>,
    wait_timeout: Duration,
    retry_wait: Duration,
    validity: Duration,
    renew_interval: Duration,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StorageLockData {
    expired: bool,
    valid_until: i64,
    owner: String,
}

impl StorageBasedLockProvider {
    /// Construct an RFC-91 provider. Each instance has a unique lock owner.
    pub fn new(
        storage: Arc<Storage>,
        wait_timeout: Duration,
        retry_wait: Duration,
        validity: Duration,
        renew_interval: Duration,
    ) -> Result<Self> {
        if wait_timeout.is_zero()
            || retry_wait.is_zero()
            || validity < Duration::from_secs(10)
            || renew_interval.is_zero()
            || validity < renew_interval.saturating_mul(10)
        {
            return Err(CoreError::Write(
                "storage lock durations must be non-zero; validity must be at least 10 seconds and 10x the renewal interval"
                    .to_string(),
            ));
        }
        Ok(Self {
            storage,
            wait_timeout,
            retry_wait,
            validity,
            renew_interval,
        })
    }

    fn now_ms() -> i64 {
        chrono::Utc::now().timestamp_millis()
    }

    fn new_lock_data(&self, owner: &str) -> StorageLockData {
        let validity_ms = i64::try_from(self.validity.as_millis()).unwrap_or(i64::MAX);
        StorageLockData {
            expired: false,
            valid_until: Self::now_ms().saturating_add(validity_ms),
            owner: owner.to_string(),
        }
    }

    fn definitely_expired(lock: &StorageLockData) -> bool {
        lock.expired || Self::now_ms() >= lock.valid_until.saturating_add(CLOCK_DRIFT_BUFFER_MS)
    }

    fn serialize(lock: &StorageLockData) -> Result<Vec<u8>> {
        serde_json::to_vec(lock)
            .map_err(|error| CoreError::Write(format!("serialize storage lock: {error}")))
    }

    fn deserialize(bytes: &[u8]) -> Result<StorageLockData> {
        serde_json::from_slice(bytes)
            .map_err(|error| CoreError::Write(format!("invalid {TABLE_LOCK_PATH}: {error}")))
    }

    async fn try_acquire_once(&self, owner: &str) -> Result<bool> {
        let observed = self.storage.get_versioned_file(TABLE_LOCK_PATH).await?;
        let new_lock = self.new_lock_data(owner);
        let bytes = Self::serialize(&new_lock)?;
        let result = match observed {
            None => {
                self.storage
                    .create_versioned_file(TABLE_LOCK_PATH, bytes)
                    .await?
            }
            Some(current) => {
                let current_data = Self::deserialize(&current.bytes)?;
                if !Self::definitely_expired(&current_data) {
                    return Ok(false);
                }
                self.storage
                    .update_versioned_file(TABLE_LOCK_PATH, bytes, current.version)
                    .await?
            }
        };
        Ok(matches!(result, ConditionalPutResult::Written(_)))
    }

    /// Renew with CAS. `None` is a definitive fencing loss; storage errors are
    /// transient until the last successfully observed validity expires.
    async fn renew_lease(
        storage: &Storage,
        owner: &str,
        validity: Duration,
    ) -> Result<Option<i64>> {
        let Some(current) = storage.get_versioned_file(TABLE_LOCK_PATH).await? else {
            return Ok(None);
        };
        let mut data = Self::deserialize(&current.bytes)?;
        if data.owner != owner || Self::definitely_expired(&data) {
            return Ok(None);
        }
        let validity_ms = i64::try_from(validity.as_millis()).unwrap_or(i64::MAX);
        data.valid_until = Self::now_ms().saturating_add(validity_ms);
        let valid_until = data.valid_until;
        match storage
            .update_versioned_file(TABLE_LOCK_PATH, Self::serialize(&data)?, current.version)
            .await?
        {
            ConditionalPutResult::Written(_) => Ok(Some(valid_until)),
            ConditionalPutResult::Conflict => Ok(None),
        }
    }

    fn lease(&self, owner: String) -> LockLease {
        let stopped = Arc::new(AtomicBool::new(false));
        let lost = Arc::new(AtomicBool::new(false));
        let heartbeat_gate = Arc::new(tokio::sync::Mutex::new(()));
        let stop_signal = Arc::new(tokio::sync::Notify::new());
        let validity_ms = i64::try_from(self.validity.as_millis()).unwrap_or(i64::MAX);
        let last_valid_until = Arc::new(AtomicI64::new(Self::now_ms().saturating_add(validity_ms)));

        let heartbeat_storage = self.storage.clone();
        let heartbeat_owner = owner.clone();
        let heartbeat_stopped = stopped.clone();
        let heartbeat_lost = lost.clone();
        let heartbeat_gate_task = heartbeat_gate.clone();
        let heartbeat_stop_signal = stop_signal.clone();
        let heartbeat_validity = self.validity;
        let heartbeat_interval = self.renew_interval;
        let heartbeat_last_valid_until = last_valid_until.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(heartbeat_interval) => {}
                    _ = heartbeat_stop_signal.notified() => return,
                }
                if heartbeat_stopped.load(Ordering::Acquire) {
                    return;
                }
                let _guard = heartbeat_gate_task.lock().await;
                if heartbeat_stopped.load(Ordering::Acquire) {
                    return;
                }
                match StorageBasedLockProvider::renew_lease(
                    heartbeat_storage.as_ref(),
                    &heartbeat_owner,
                    heartbeat_validity,
                )
                .await
                {
                    Ok(Some(valid_until)) => {
                        heartbeat_last_valid_until.store(valid_until, Ordering::Release)
                    }
                    Ok(None) => {
                        heartbeat_lost.store(true, Ordering::Release);
                        log::warn!("storage lock heartbeat lost ownership");
                        return;
                    }
                    Err(error) => {
                        if StorageBasedLockProvider::now_ms()
                            >= heartbeat_last_valid_until
                                .load(Ordering::Acquire)
                                .saturating_add(CLOCK_DRIFT_BUFFER_MS)
                        {
                            heartbeat_lost.store(true, Ordering::Release);
                            log::warn!(
                                "storage lock heartbeat expired after renewal errors: {error}"
                            );
                            return;
                        }
                        log::warn!("transient storage lock heartbeat error: {error}");
                    }
                }
            }
        });

        let check_storage = self.storage.clone();
        let check_owner = owner.clone();
        let check_lost = lost;
        let check_gate = heartbeat_gate.clone();
        let check_validity = self.validity;
        let check_last_valid_until = last_valid_until;
        let check: LeaseCheck = Arc::new(move || {
            let storage = check_storage.clone();
            let owner = check_owner.clone();
            let lost = check_lost.clone();
            let gate = check_gate.clone();
            let last_valid_until = check_last_valid_until.clone();
            Box::pin(async move {
                if lost.load(Ordering::Acquire) {
                    return Err(CoreError::Write(
                        "storage lock heartbeat lost ownership".to_string(),
                    ));
                }
                let _guard = gate.lock().await;
                // The heartbeat may have failed while this validation was
                // waiting for the shared gate. Re-check after acquiring it so
                // a transiently readable but permanently lost lease can never
                // authorize a timeline publication.
                if lost.load(Ordering::Acquire) {
                    return Err(CoreError::Write(
                        "storage lock heartbeat lost ownership".to_string(),
                    ));
                }
                match StorageBasedLockProvider::renew_lease(
                    storage.as_ref(),
                    &owner,
                    check_validity,
                )
                .await?
                {
                    Some(valid_until) => {
                        last_valid_until.store(valid_until, Ordering::Release);
                    }
                    None => {
                        lost.store(true, Ordering::Release);
                        return Err(CoreError::Write(format!(
                            "storage lock ownership was lost (expected owner {owner})"
                        )));
                    }
                }
                Ok(())
            })
        });

        let release_storage = self.storage.clone();
        let release_owner = owner;
        let release_stopped = stopped;
        let release_gate = heartbeat_gate;
        let release_stop_signal = stop_signal;
        let release_retry_wait = self.retry_wait;
        let release: LeaseRelease = Box::new(move || {
            Box::pin(async move {
                release_stopped.store(true, Ordering::Release);
                release_stop_signal.notify_one();
                // Wait out a renewal already in progress. Future renewals see
                // `stopped`, so none can resurrect this expired lease.
                let _guard = release_gate.lock().await;
                let mut attempt = 0;
                loop {
                    let result = async {
                        let Some(current) =
                            release_storage.get_versioned_file(TABLE_LOCK_PATH).await?
                        else {
                            return Ok(());
                        };
                        let mut data = StorageBasedLockProvider::deserialize(&current.bytes)?;
                        if data.owner != release_owner {
                            return Ok(());
                        }
                        data.expired = true;
                        data.valid_until = StorageBasedLockProvider::now_ms();
                        let bytes = StorageBasedLockProvider::serialize(&data)?;
                        let _ = release_storage
                            .update_versioned_file(TABLE_LOCK_PATH, bytes, current.version)
                            .await?;
                        Ok(())
                    }
                    .await;
                    match result {
                        Ok(()) => return Ok(()),
                        Err(error) if attempt < 2 => {
                            attempt += 1;
                            log::warn!("transient storage lock release error: {error}");
                            tokio::time::sleep(release_retry_wait).await;
                        }
                        Err(error) => return Err(error),
                    }
                }
            })
        });
        LockLease::storage(check, release)
    }
}

impl LockProvider for StorageBasedLockProvider {
    fn lock(&self) -> BoxFuture<'_, Result<LockLease>> {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + self.wait_timeout;
            // A provider can be cloned and reused. Every acquisition needs a
            // distinct fencing identity so a stale lease from an earlier
            // acquisition can never renew or release a newer one.
            let owner = uuid::Uuid::new_v4().to_string();
            let mut last_error = None;
            loop {
                match self.try_acquire_once(&owner).await {
                    Ok(true) => return Ok(self.lease(owner)),
                    Ok(false) => {}
                    Err(error) => {
                        log::warn!("transient error acquiring {TABLE_LOCK_PATH}: {error}");
                        last_error = Some(error);
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    if let Some(error) = last_error {
                        return Err(CoreError::Write(format!(
                            "timed out after {} ms acquiring {TABLE_LOCK_PATH}; last storage error: {error}",
                            self.wait_timeout.as_millis()
                        )));
                    }
                    return Err(CoreError::Write(format!(
                        "timed out after {} ms acquiring {TABLE_LOCK_PATH}",
                        self.wait_timeout.as_millis()
                    )));
                }
                tokio::time::sleep(self.retry_wait).await;
            }
        })
    }
}

/// Resolve the lock provider configured for a table.
pub(crate) fn lock_provider_for(table: &Table) -> Result<Arc<dyn LockProvider>> {
    let config = WriteConcurrencyConfig::from_configs(&table.hudi_configs)?;
    match config.lock_provider {
        WriteLockProvider::InProcess => {
            let base_path: String = table
                .hudi_configs
                .get_or_default(HudiTableConfig::BasePath)
                .into();
            Ok(Arc::new(InProcessLockProvider::new(base_path)))
        }
        WriteLockProvider::StorageBased => Ok(Arc::new(StorageBasedLockProvider::new(
            {
                let storage = table.file_system_view.storage.clone();
                if storage.base_url.scheme() == "file" {
                    return Err(CoreError::Unsupported(
                        "the RFC-91 storage lock requires an object store with atomic conditional writes; local file:// storage is not supported"
                            .to_string(),
                    ));
                }
                storage
            },
            Duration::from_millis(config.lock_wait_timeout_ms as u64),
            Duration::from_millis(config.lock_retry_wait_ms as u64),
            Duration::from_secs(config.storage_lock_validity_seconds as u64),
            Duration::from_secs(config.storage_lock_renew_interval_seconds as u64),
        )?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use url::Url;

    fn provider(storage: Arc<Storage>, wait_ms: u64) -> StorageBasedLockProvider {
        StorageBasedLockProvider::new(
            storage,
            Duration::from_millis(wait_ms),
            Duration::from_millis(5),
            Duration::from_secs(10),
            Duration::from_secs(1),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn storage_lock_serializes_independent_providers() {
        let storage = Storage::new_with_object_store(
            Url::parse("memory:///table").unwrap(),
            Arc::new(InMemory::new()),
        );
        let first = provider(storage.clone(), 50);
        let second = provider(storage.clone(), 20);

        let lease = first.lock().await.unwrap();
        assert!(second.lock().await.is_err());
        lease.validate().await.unwrap();
        lease.release().await.unwrap();

        let second_lease = second.lock().await.unwrap();
        second_lease.validate().await.unwrap();
        second_lease.release().await.unwrap();
    }

    #[tokio::test]
    async fn stale_lease_cannot_expire_a_new_owner() {
        let storage = Storage::new_with_object_store(
            Url::parse("memory:///table").unwrap(),
            Arc::new(InMemory::new()),
        );
        let first = provider(storage.clone(), 50);
        let first_lease = first.lock().await.unwrap();
        let observed = storage
            .get_versioned_file(TABLE_LOCK_PATH)
            .await
            .unwrap()
            .unwrap();
        let mut expired = StorageBasedLockProvider::deserialize(&observed.bytes).unwrap();
        expired.expired = true;
        storage
            .update_versioned_file(
                TABLE_LOCK_PATH,
                StorageBasedLockProvider::serialize(&expired).unwrap(),
                observed.version,
            )
            .await
            .unwrap();

        let second = provider(storage.clone(), 50);
        let second_lease = second.lock().await.unwrap();
        first_lease.release().await.unwrap();
        second_lease.validate().await.unwrap();
        second_lease.release().await.unwrap();
    }

    #[tokio::test]
    async fn heartbeat_renews_the_storage_lease() {
        let storage = Storage::new_with_object_store(
            Url::parse("memory:///table").unwrap(),
            Arc::new(InMemory::new()),
        );
        let short_provider = |storage: Arc<Storage>, wait_timeout| StorageBasedLockProvider {
            storage,
            wait_timeout,
            retry_wait: Duration::from_millis(5),
            validity: Duration::from_millis(200),
            renew_interval: Duration::from_millis(20),
        };
        let first = short_provider(storage.clone(), Duration::from_millis(50));
        let second = short_provider(storage, Duration::from_millis(30));
        let lease = first.lock().await.unwrap();
        // Longer than validity + the conservative clock-drift buffer: without
        // heartbeat renewal the second provider could take over.
        tokio::time::sleep(Duration::from_millis(750)).await;
        lease.validate().await.unwrap();
        assert!(second.lock().await.is_err());
        lease.release().await.unwrap();
    }
}
