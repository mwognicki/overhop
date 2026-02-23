use std::fmt;

use crate::logging::{LogLevel, Logger};
use crate::storage::{StorageError, StorageFacade};
use uuid::Uuid;

use super::{Queue, QueueConfig, QueuePool, QueuePoolError};

pub const SYSTEM_QUEUE_NAME: &str = "_system";

pub struct PersistentQueuePool {
    pool: QueuePool,
}

impl PersistentQueuePool {
    pub fn bootstrap(storage: &StorageFacade, logger: &Logger) -> Result<Self, PersistentQueuePoolError> {
        let mut queues = storage.load_queues().map_err(PersistentQueuePoolError::Storage)?;
        if queues.is_empty() {
            logger.info(
                Some("orchestrator::queues::bootstrap"),
                "No persisted queues found, initializing default _system queue",
            );
            queues.push(Queue::new(SYSTEM_QUEUE_NAME, None));
            storage
                .replace_queues(&queues)
                .map_err(PersistentQueuePoolError::Storage)?;
        }

        let mut pool = QueuePool::reconstruct(queues).map_err(PersistentQueuePoolError::QueuePool)?;
        pool.mark_bootstrapped();

        logger.log(
            LogLevel::Info,
            Some("orchestrator::queues::bootstrap"),
            "Queue pool loaded from persistence",
            Some(serde_json::json!({
                "queues_count": pool.snapshot().queues.len(),
                "bootstrapped": pool.is_bootstrapped()
            })),
        );

        Ok(Self { pool })
    }

    pub fn queue_pool(&self) -> &QueuePool {
        &self.pool
    }

    pub fn register_queue(
        &mut self,
        storage: &StorageFacade,
        name: impl Into<String>,
        config: Option<QueueConfig>,
    ) -> Result<Uuid, PersistentQueuePoolError> {
        self.mutate_and_reload(storage, |pool| pool.register_queue(name.into(), config))
    }

    pub fn pause_queue(
        &mut self,
        storage: &StorageFacade,
        name: &str,
    ) -> Result<(), PersistentQueuePoolError> {
        self.mutate_and_reload(storage, |pool| pool.pause_queue(name))
    }

    pub fn resume_queue(
        &mut self,
        storage: &StorageFacade,
        name: &str,
    ) -> Result<(), PersistentQueuePoolError> {
        self.mutate_and_reload(storage, |pool| pool.resume_queue(name))
    }

    pub fn remove_queue(
        &mut self,
        storage: &StorageFacade,
        name: &str,
    ) -> Result<(), PersistentQueuePoolError> {
        self.mutate_and_reload(storage, |pool| pool.remove_queue(name))
    }

    pub fn persist_current_state(&self, storage: &StorageFacade) -> Result<(), PersistentQueuePoolError> {
        storage
            .replace_queues(&self.pool.snapshot().queues)
            .map_err(PersistentQueuePoolError::Storage)?;
        storage.flush().map_err(PersistentQueuePoolError::Storage)
    }

    fn mutate_and_reload<F, R>(
        &mut self,
        storage: &StorageFacade,
        mutation: F,
    ) -> Result<R, PersistentQueuePoolError>
    where
        F: FnOnce(&mut QueuePool) -> Result<R, QueuePoolError>,
    {
        let current_snapshot = self.pool.snapshot();
        let mut candidate = QueuePool::reconstruct(current_snapshot.queues)
            .map_err(PersistentQueuePoolError::QueuePool)?;
        candidate.mark_bootstrapped();

        let mutation_result = mutation(&mut candidate).map_err(PersistentQueuePoolError::QueuePool)?;

        let desired = candidate.snapshot().queues;
        storage
            .replace_queues(&desired)
            .map_err(PersistentQueuePoolError::Storage)?;

        self.reload_from_storage(storage)?;
        Ok(mutation_result)
    }

    fn reload_from_storage(&mut self, storage: &StorageFacade) -> Result<(), PersistentQueuePoolError> {
        let persisted = storage
            .load_queues()
            .map_err(PersistentQueuePoolError::Storage)?;
        let mut reloaded = QueuePool::reconstruct(persisted).map_err(PersistentQueuePoolError::QueuePool)?;
        reloaded.mark_bootstrapped();
        self.pool = reloaded;
        Ok(())
    }
}

#[derive(Debug)]
pub enum PersistentQueuePoolError {
    Storage(StorageError),
    QueuePool(QueuePoolError),
}

impl fmt::Display for PersistentQueuePoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(source) => write!(f, "queue persistence error: {source}"),
            Self::QueuePool(source) => write!(f, "queue pool error: {source}"),
        }
    }
}

impl std::error::Error for PersistentQueuePoolError {}

#[cfg(test)]
mod tests {
    use crate::config::{
        AppConfig, HeartbeatConfig, LoggingConfig, ServerConfig, SledConfig, StorageConfig,
        WireConfig, WireSessionConfig,
    };
    use crate::logging::{LogLevel, Logger, LoggerConfig};
    use crate::storage::StorageFacade;

    use super::{PersistentQueuePool, SYSTEM_QUEUE_NAME};

    fn test_storage(path: &str, logger: &Logger) -> StorageFacade {
        let app_config = AppConfig {
            logging: LoggingConfig {
                level: "debug".to_owned(),
                human_friendly: false,
            },
            heartbeat: HeartbeatConfig { interval_ms: 1000 },
            server: ServerConfig {
                host: "127.0.0.1".to_owned(),
                port: 9876,
                tls_enabled: false,
            },
            wire: WireConfig {
                max_envelope_size_bytes: 8_388_608,
                session: WireSessionConfig::default(),
            },
            storage: StorageConfig {
                engine: "sled".to_owned(),
                path: path.to_owned(),
                self_debug_path: None,
                sled: SledConfig::default(),
            },
        };

        StorageFacade::initialize(&app_config, logger).expect("storage should initialize")
    }

    fn unique_temp_path(label: &str) -> String {
        std::env::temp_dir()
            .join(format!(
                "overhop-persistent-queues-test-{label}-{}-{}",
                std::process::id(),
                chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
            ))
            .to_string_lossy()
            .to_string()
    }

    fn test_logger() -> Logger {
        Logger::new(LoggerConfig {
            min_level: LogLevel::Error,
            human_friendly: false,
        })
    }

    #[test]
    fn bootstrap_creates_system_queue_on_first_run() {
        let path = unique_temp_path("bootstrap-system");
        let logger = test_logger();
        let storage = test_storage(&path, &logger);

        let persistent =
            PersistentQueuePool::bootstrap(&storage, &logger).expect("bootstrap should pass");
        let system = persistent
            .queue_pool()
            .get_queue(SYSTEM_QUEUE_NAME)
            .expect("system queue should exist");
        assert_eq!(system.name, SYSTEM_QUEUE_NAME);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn bootstrap_restores_existing_persisted_queues() {
        let path = unique_temp_path("bootstrap-restore");
        let logger = test_logger();
        let storage = test_storage(&path, &logger);

        storage
            .replace_queues(&[
                super::Queue::new(SYSTEM_QUEUE_NAME, None),
                super::Queue::new("critical", None),
            ])
            .expect("seeded queues should persist");

        let persistent =
            PersistentQueuePool::bootstrap(&storage, &logger).expect("bootstrap should pass");
        assert!(persistent.queue_pool().get_queue("critical").is_some());
        assert!(persistent.queue_pool().is_bootstrapped());

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn queue_mutation_is_persist_first_then_reload() {
        let path = unique_temp_path("mutate-reload");
        let logger = test_logger();
        let storage = test_storage(&path, &logger);

        let mut persistent =
            PersistentQueuePool::bootstrap(&storage, &logger).expect("bootstrap should pass");
        persistent
            .register_queue(&storage, "jobs", None)
            .expect("queue register should pass");

        let restored = storage.load_queues().expect("queues should load from storage");
        assert!(restored.iter().any(|q| q.name == "jobs"));
        assert!(persistent.queue_pool().get_queue("jobs").is_some());

        persistent
            .pause_queue(&storage, "jobs")
            .expect("pause should pass");
        let paused = persistent
            .queue_pool()
            .get_queue("jobs")
            .expect("jobs queue should exist");
        assert!(paused.is_paused());
        persistent
            .resume_queue(&storage, "jobs")
            .expect("resume should pass");
        let resumed = persistent
            .queue_pool()
            .get_queue("jobs")
            .expect("jobs queue should exist");
        assert!(!resumed.is_paused());

        let _ = std::fs::remove_dir_all(path);
    }
}
