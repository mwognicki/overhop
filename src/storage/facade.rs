use std::fs;
use std::path::PathBuf;

use serde_json::json;
use uuid::Uuid;

use crate::config::AppConfig;
use crate::logging::{LogLevel, Logger};
use crate::orchestrator::queues::Queue;

use super::{
    QueueStatusCount, SledMode, SledStorage, StorageBackend, StorageEngine, StorageError,
    expand_home_path,
};

pub struct StorageFacade {
    engine: StorageEngine,
    data_path: PathBuf,
    backend: Box<dyn StorageBackend>,
}

impl StorageFacade {
    pub fn initialize(app_config: &AppConfig, logger: &Logger) -> Result<Self, StorageError> {
        let Some(engine) = StorageEngine::parse(&app_config.storage.engine) else {
            return Err(StorageError::UnsupportedEngine {
                engine: app_config.storage.engine.clone(),
            });
        };
        let data_path = expand_home_path(&app_config.storage.path)?;

        logger.log(
            LogLevel::Info,
            Some("storage::init"),
            "Initializing storage facade",
            Some(json!({
                "engine": engine.as_str(),
                "configured_path": app_config.storage.path,
                "resolved_path": data_path.display().to_string(),
                "sled": {
                    "cache_capacity": app_config.storage.sled.cache_capacity,
                    "mode": app_config.storage.sled.mode
                }
            })),
        );

        fs::create_dir_all(&data_path).map_err(|source| StorageError::CreateDataDir {
            path: data_path.clone(),
            source,
        })?;

        let backend: Box<dyn StorageBackend> = match engine {
            StorageEngine::Sled => {
                let mode = match app_config.storage.sled.mode.as_deref() {
                    Some(raw) => {
                        let Some(mode) = SledMode::parse(raw) else {
                            return Err(StorageError::InvalidSledMode {
                                mode: raw.to_owned(),
                            });
                        };
                        Some(mode)
                    }
                    None => None,
                };

                Box::new(SledStorage::open(
                    &data_path,
                    app_config.storage.sled.cache_capacity,
                    mode,
                )?)
            }
        };

        logger.log(
            LogLevel::Info,
            Some("storage::init"),
            "Storage facade initialized",
            Some(json!({
                "engine": engine.as_str(),
                "resolved_path": data_path.display().to_string(),
            })),
        );

        Ok(Self {
            engine,
            data_path,
            backend,
        })
    }

    pub fn engine(&self) -> StorageEngine {
        self.engine
    }

    pub fn data_path(&self) -> &PathBuf {
        &self.data_path
    }

    pub fn flush(&self) -> Result<(), StorageError> {
        self.backend.flush()
    }

    pub fn load_queues(&self) -> Result<Vec<Queue>, StorageError> {
        self.backend.load_queues()
    }

    pub fn replace_queues(&self, queues: &[Queue]) -> Result<(), StorageError> {
        self.backend.replace_queues(queues)
    }

    pub fn list_job_uuids_by_status(&self, status: &str) -> Result<Vec<Uuid>, StorageError> {
        self.backend.list_job_uuids_by_status(status)
    }

    pub fn list_job_uuids_by_status_fifo(&self, status: &str) -> Result<Vec<Uuid>, StorageError> {
        self.backend.list_job_uuids_by_status_fifo(status)
    }

    pub fn list_queue_status_counts(&self) -> Result<Vec<QueueStatusCount>, StorageError> {
        self.backend.list_queue_status_counts()
    }

    pub fn list_job_records_by_queue_and_status(
        &self,
        queue_name: &str,
        status: &str,
        page: u32,
        page_size: u32,
    ) -> Result<Vec<serde_json::Value>, StorageError> {
        self.backend
            .list_job_records_by_queue_and_status(queue_name, status, page, page_size)
    }

    pub fn upsert_job_record(
        &self,
        job_uuid: Uuid,
        record: &serde_json::Value,
        execution_start_ms: i64,
        created_at_ms: i64,
        queue_name: &str,
        status: &str,
    ) -> Result<(), StorageError> {
        self.backend
            .upsert_job_record(job_uuid, record, execution_start_ms, created_at_ms, queue_name, status)
    }

    pub fn get_job_payload_by_uuid(
        &self,
        job_uuid: Uuid,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        self.backend.get_job_payload_by_uuid(job_uuid)
    }

    pub fn remove_job_record(&self, job_uuid: Uuid) -> Result<bool, StorageError> {
        self.backend.remove_job_record(job_uuid)
    }
}
