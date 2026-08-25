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
//! Hudi write-concurrency and lock configurations.

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

use strum_macros::{AsRefStr, EnumIter, IntoStaticStr};

use crate::Result;
use crate::config::error::ConfigError;
use crate::config::error::ConfigError::{InvalidValue, NotFound, ParseInt};
use crate::config::{ConfigAlias, ConfigParser, HudiConfigValue, HudiConfigs};
use crate::error::CoreError;

/// Apache Hudi's Java class name for its process-local lock provider.
pub const IN_PROCESS_LOCK_PROVIDER_CLASS: &str =
    "org.apache.hudi.client.transaction.lock.InProcessLockProvider";

/// Apache Hudi's Java class name for its conditional object-store lock provider.
pub const STORAGE_BASED_LOCK_PROVIDER_CLASS: &str =
    "org.apache.hudi.client.transaction.lock.StorageBasedLockProvider";

/// Concurrency mode for write operations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, AsRefStr)]
pub enum WriteConcurrencyMode {
    /// One active writer. Locks coordinate table handles in this process only.
    #[default]
    #[strum(serialize = "single_writer")]
    SingleWriter,
    /// File-group-level optimistic concurrency control for independent writers.
    #[strum(serialize = "optimistic_concurrency_control")]
    OptimisticConcurrencyControl,
}

impl WriteConcurrencyMode {
    /// Whether this mode permits independent concurrent writers.
    pub fn supports_multi_writer(self) -> bool {
        matches!(self, Self::OptimisticConcurrencyControl)
    }
}

impl Display for WriteConcurrencyMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl FromStr for WriteConcurrencyMode {
    type Err = ConfigError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "single_writer" | "single-writer" => Ok(Self::SingleWriter),
            "optimistic_concurrency_control" | "optimistic-concurrency-control" | "occ" => {
                Ok(Self::OptimisticConcurrencyControl)
            }
            other => Err(InvalidValue(format!(
                "hoodie.write.concurrency.mode={other}"
            ))),
        }
    }
}

/// Lock providers implemented by hudi-rs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WriteLockProvider {
    /// Process-local async mutex.
    #[default]
    InProcess,
    /// Conditional-write lease stored under `.hoodie/.locks`.
    StorageBased,
}

impl FromStr for WriteLockProvider {
    type Err = ConfigError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let normalized = value.trim();
        if normalized.eq_ignore_ascii_case(IN_PROCESS_LOCK_PROVIDER_CLASS)
            || normalized.eq_ignore_ascii_case("in_process")
            || normalized.eq_ignore_ascii_case("in-process")
        {
            Ok(Self::InProcess)
        } else if normalized.eq_ignore_ascii_case(STORAGE_BASED_LOCK_PROVIDER_CLASS)
            || normalized.eq_ignore_ascii_case("storage_based")
            || normalized.eq_ignore_ascii_case("storage-based")
        {
            Ok(Self::StorageBased)
        } else {
            Err(InvalidValue(format!(
                "hoodie.write.lock.provider={normalized} (hudi-rs supports {IN_PROCESS_LOCK_PROVIDER_CLASS} and {STORAGE_BASED_LOCK_PROVIDER_CLASS})"
            )))
        }
    }
}

/// Policy for cleaning files left by failed writes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, AsRefStr)]
pub enum FailedWritesCleaningPolicy {
    /// Roll pending instants back before the next write. Single-writer only.
    #[default]
    #[strum(serialize = "EAGER")]
    Eager,
    /// Defer cleanup until the failed writer is known to be dead.
    #[strum(serialize = "LAZY")]
    Lazy,
    /// Never clean failed writes automatically.
    #[strum(serialize = "NEVER")]
    Never,
}

impl FromStr for FailedWritesCleaningPolicy {
    type Err = ConfigError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "eager" => Ok(Self::Eager),
            "lazy" => Ok(Self::Lazy),
            "never" => Ok(Self::Never),
            other => Err(InvalidValue(format!(
                "hoodie.cleaner.policy.failed.writes={other}"
            ))),
        }
    }
}

/// Typed write-side configurations used by the native writer.
#[derive(Clone, Debug, PartialEq, Eq, Hash, EnumIter, IntoStaticStr)]
pub enum HudiWriteConfig {
    /// [`WriteConcurrencyMode`].
    ConcurrencyMode,
    /// [`WriteLockProvider`].
    LockProvider,
    /// Overall lock acquisition timeout in milliseconds.
    LockAcquireWaitTimeoutMs,
    /// Delay between lock acquisition attempts in milliseconds.
    LockAcquireRetryWaitMs,
    /// Storage-lock lease lifetime in seconds.
    StorageLockValiditySeconds,
    /// Storage-lock heartbeat interval in seconds.
    StorageLockRenewIntervalSeconds,
    /// Standard writer heartbeat interval in milliseconds.
    ClientHeartbeatIntervalMs,
    /// Number of missed writer heartbeats tolerated before cleanup.
    ClientHeartbeatTolerableMisses,
    /// [`FailedWritesCleaningPolicy`].
    FailedWritesCleaningPolicy,
}

impl HudiWriteConfig {
    /// Canonical Apache Hudi configuration key.
    pub const fn key_str(&self) -> &'static str {
        match self {
            Self::ConcurrencyMode => "hoodie.write.concurrency.mode",
            Self::LockProvider => "hoodie.write.lock.provider",
            Self::LockAcquireWaitTimeoutMs => "hoodie.write.lock.wait_time_ms",
            Self::LockAcquireRetryWaitMs => "hoodie.write.lock.wait_time_ms_between_retry",
            Self::StorageLockValiditySeconds => "hoodie.write.lock.storage.validity.timeout.secs",
            Self::StorageLockRenewIntervalSeconds => {
                "hoodie.write.lock.storage.renew.interval.secs"
            }
            Self::ClientHeartbeatIntervalMs => "hoodie.client.heartbeat.interval_in_ms",
            Self::ClientHeartbeatTolerableMisses => "hoodie.client.heartbeat.tolerable.misses",
            Self::FailedWritesCleaningPolicy => "hoodie.cleaner.policy.failed.writes",
        }
    }
}

impl AsRef<str> for HudiWriteConfig {
    fn as_ref(&self) -> &str {
        self.key_str()
    }
}

impl Display for HudiWriteConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_ref())
    }
}

impl ConfigParser for HudiWriteConfig {
    type Output = HudiConfigValue;

    fn default_value(&self) -> Option<Self::Output> {
        match self {
            Self::ConcurrencyMode => Some(HudiConfigValue::String(
                WriteConcurrencyMode::default().as_ref().to_string(),
            )),
            Self::LockProvider => None,
            Self::LockAcquireWaitTimeoutMs => Some(HudiConfigValue::UInteger(60_000)),
            Self::LockAcquireRetryWaitMs => Some(HudiConfigValue::UInteger(1_000)),
            Self::StorageLockValiditySeconds => Some(HudiConfigValue::UInteger(300)),
            Self::StorageLockRenewIntervalSeconds => Some(HudiConfigValue::UInteger(30)),
            Self::ClientHeartbeatIntervalMs => Some(HudiConfigValue::UInteger(60_000)),
            Self::ClientHeartbeatTolerableMisses => Some(HudiConfigValue::UInteger(10)),
            Self::FailedWritesCleaningPolicy => Some(HudiConfigValue::String(
                FailedWritesCleaningPolicy::default().as_ref().to_string(),
            )),
        }
    }

    fn aliases(&self) -> &[ConfigAlias] {
        match self {
            Self::StorageLockRenewIntervalSeconds => {
                const ALIASES: &[ConfigAlias] = &[ConfigAlias::deprecated(
                    "hoodie.write.lock.storage.heartbeat.poll.secs",
                )];
                ALIASES
            }
            _ => &[],
        }
    }

    fn parse_value(
        &self,
        configs: &HashMap<String, String>,
    ) -> crate::config::Result<Self::Output> {
        let raw = self.resolve_raw_value(configs);
        match self {
            Self::ConcurrencyMode => raw
                .and_then(WriteConcurrencyMode::from_str)
                .map(|value| HudiConfigValue::String(value.as_ref().to_string())),
            Self::LockProvider => raw.and_then(WriteLockProvider::from_str).map(|value| {
                let class = match value {
                    WriteLockProvider::InProcess => IN_PROCESS_LOCK_PROVIDER_CLASS,
                    WriteLockProvider::StorageBased => STORAGE_BASED_LOCK_PROVIDER_CLASS,
                };
                HudiConfigValue::String(class.to_string())
            }),
            Self::LockAcquireWaitTimeoutMs
            | Self::LockAcquireRetryWaitMs
            | Self::StorageLockValiditySeconds
            | Self::StorageLockRenewIntervalSeconds
            | Self::ClientHeartbeatIntervalMs
            | Self::ClientHeartbeatTolerableMisses => raw
                .and_then(|value| {
                    value
                        .parse::<usize>()
                        .map_err(|error| ParseInt(self.key(), value.to_string(), error))
                })
                .and_then(|value| {
                    if value == 0 {
                        Err(InvalidValue(format!("{}=0 (must be > 0)", self.key())))
                    } else {
                        Ok(value)
                    }
                })
                .map(HudiConfigValue::UInteger),
            Self::FailedWritesCleaningPolicy => raw
                .and_then(FailedWritesCleaningPolicy::from_str)
                .map(|value| HudiConfigValue::String(value.as_ref().to_string())),
        }
    }
}

/// Resolved and cross-validated settings for one table writer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteConcurrencyConfig {
    pub mode: WriteConcurrencyMode,
    pub lock_provider: WriteLockProvider,
    pub lock_wait_timeout_ms: usize,
    pub lock_retry_wait_ms: usize,
    pub storage_lock_validity_seconds: usize,
    pub storage_lock_renew_interval_seconds: usize,
    pub client_heartbeat_interval_ms: usize,
    pub client_heartbeat_tolerable_misses: usize,
    pub failed_writes_cleaning_policy: FailedWritesCleaningPolicy,
}

impl WriteConcurrencyConfig {
    /// Resolve write settings, enforcing the invariants required for multi-writer safety.
    pub fn from_configs(configs: &HudiConfigs) -> Result<Self> {
        let mode_raw: String = configs
            .try_get(HudiWriteConfig::ConcurrencyMode)?
            .ok_or_else(|| {
                CoreError::Write("write concurrency mode default is missing".to_string())
            })?
            .into();
        let mode = WriteConcurrencyMode::from_str(&mode_raw)?;

        let lock_provider = match configs.get(HudiWriteConfig::LockProvider) {
            Ok(value) => WriteLockProvider::from_str(&String::from(value))?,
            Err(NotFound(_)) if mode == WriteConcurrencyMode::SingleWriter => {
                WriteLockProvider::InProcess
            }
            Err(NotFound(_)) => {
                return Err(CoreError::Write(format!(
                    "{} requires an external {}; configure {}={}",
                    HudiWriteConfig::ConcurrencyMode,
                    HudiWriteConfig::LockProvider,
                    HudiWriteConfig::LockProvider,
                    STORAGE_BASED_LOCK_PROVIDER_CLASS
                )));
            }
            Err(error) => return Err(error.into()),
        };

        if mode.supports_multi_writer() && lock_provider == WriteLockProvider::InProcess {
            return Err(CoreError::Write(format!(
                "{} cannot coordinate independent writers; use {}={} for {}={}",
                IN_PROCESS_LOCK_PROVIDER_CLASS,
                HudiWriteConfig::LockProvider,
                STORAGE_BASED_LOCK_PROVIDER_CLASS,
                HudiWriteConfig::ConcurrencyMode,
                WriteConcurrencyMode::OptimisticConcurrencyControl
            )));
        }

        let failed_policy_raw: String = configs
            .try_get(HudiWriteConfig::FailedWritesCleaningPolicy)?
            .ok_or_else(|| CoreError::Write("failed-writes policy default is missing".to_string()))?
            .into();
        let failed_writes_cleaning_policy =
            FailedWritesCleaningPolicy::from_str(&failed_policy_raw)?;
        if mode.supports_multi_writer()
            && failed_writes_cleaning_policy != FailedWritesCleaningPolicy::Lazy
        {
            return Err(CoreError::Write(format!(
                "{}={} requires {}=LAZY so one writer cannot roll back another active writer",
                HudiWriteConfig::ConcurrencyMode,
                mode,
                HudiWriteConfig::FailedWritesCleaningPolicy
            )));
        }

        let get_usize = |key: HudiWriteConfig| -> Result<usize> {
            configs
                .try_get(key)?
                .map(Into::into)
                .ok_or_else(|| CoreError::Write("write config default is missing".to_string()))
        };
        let storage_lock_validity_seconds = get_usize(HudiWriteConfig::StorageLockValiditySeconds)?;
        let storage_lock_renew_interval_seconds =
            get_usize(HudiWriteConfig::StorageLockRenewIntervalSeconds)?;
        let client_heartbeat_interval_ms = get_usize(HudiWriteConfig::ClientHeartbeatIntervalMs)?;
        let client_heartbeat_tolerable_misses =
            get_usize(HudiWriteConfig::ClientHeartbeatTolerableMisses)?;
        if client_heartbeat_interval_ms < 1_000 {
            return Err(CoreError::Write(format!(
                "{} must be at least 1000 ms",
                HudiWriteConfig::ClientHeartbeatIntervalMs
            )));
        }
        if lock_provider == WriteLockProvider::StorageBased
            && (storage_lock_validity_seconds < 10
                || storage_lock_validity_seconds / 10 < storage_lock_renew_interval_seconds)
        {
            return Err(CoreError::Write(format!(
                "{} must be at least 10 seconds and at least 10x {}",
                HudiWriteConfig::StorageLockValiditySeconds,
                HudiWriteConfig::StorageLockRenewIntervalSeconds
            )));
        }

        Ok(Self {
            mode,
            lock_provider,
            lock_wait_timeout_ms: get_usize(HudiWriteConfig::LockAcquireWaitTimeoutMs)?,
            lock_retry_wait_ms: get_usize(HudiWriteConfig::LockAcquireRetryWaitMs)?,
            storage_lock_validity_seconds,
            storage_lock_renew_interval_seconds,
            client_heartbeat_interval_ms,
            client_heartbeat_tolerable_misses,
            failed_writes_cleaning_policy,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_occ_requires_storage_lock_and_lazy_cleaning() {
        let configs = HudiConfigs::new([
            (
                HudiWriteConfig::ConcurrencyMode,
                "optimistic_concurrency_control",
            ),
            (
                HudiWriteConfig::LockProvider,
                STORAGE_BASED_LOCK_PROVIDER_CLASS,
            ),
            (HudiWriteConfig::FailedWritesCleaningPolicy, "LAZY"),
        ]);
        let resolved = WriteConcurrencyConfig::from_configs(&configs).unwrap();
        assert_eq!(
            resolved.mode,
            WriteConcurrencyMode::OptimisticConcurrencyControl
        );
        assert_eq!(resolved.lock_provider, WriteLockProvider::StorageBased);
    }

    #[test]
    fn test_occ_rejects_in_process_provider() {
        let configs = HudiConfigs::new([
            (HudiWriteConfig::ConcurrencyMode, "occ"),
            (
                HudiWriteConfig::LockProvider,
                IN_PROCESS_LOCK_PROVIDER_CLASS,
            ),
            (HudiWriteConfig::FailedWritesCleaningPolicy, "LAZY"),
        ]);
        let error = WriteConcurrencyConfig::from_configs(&configs).unwrap_err();
        assert!(error.to_string().contains("cannot coordinate"));
    }

    #[test]
    fn test_occ_rejects_eager_cleaning() {
        let configs = HudiConfigs::new([
            (HudiWriteConfig::ConcurrencyMode, "occ"),
            (
                HudiWriteConfig::LockProvider,
                STORAGE_BASED_LOCK_PROVIDER_CLASS,
            ),
        ]);
        let error = WriteConcurrencyConfig::from_configs(&configs).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("requires hoodie.cleaner.policy.failed.writes=LAZY")
        );
    }

    #[test]
    fn test_storage_lock_lease_must_cover_ten_renewals() {
        let configs = HudiConfigs::new([
            (
                HudiWriteConfig::LockProvider,
                STORAGE_BASED_LOCK_PROVIDER_CLASS,
            ),
            (HudiWriteConfig::StorageLockValiditySeconds, "20"),
            (HudiWriteConfig::StorageLockRenewIntervalSeconds, "3"),
        ]);
        let error = WriteConcurrencyConfig::from_configs(&configs).unwrap_err();
        assert!(error.to_string().contains("at least 10x"));
    }

    #[test]
    fn test_invalid_concurrency_mode_fails_closed() {
        let configs = HudiConfigs::new([(HudiWriteConfig::ConcurrencyMode, "occc")]);
        let error = WriteConcurrencyConfig::from_configs(&configs).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("hoodie.write.concurrency.mode=occc")
        );
    }

    #[test]
    fn test_writer_heartbeat_interval_has_java_minimum() {
        let configs = HudiConfigs::new([(HudiWriteConfig::ClientHeartbeatIntervalMs, "999")]);
        let error = WriteConcurrencyConfig::from_configs(&configs).unwrap_err();
        assert!(error.to_string().contains("at least 1000 ms"));
    }
}
