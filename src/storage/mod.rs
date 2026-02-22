use std::fmt;
use std::fs;
use std::path::PathBuf;

use serde_json::json;
use uuid::Uuid;

use crate::config::AppConfig;
use crate::logging::{LogLevel, Logger};
use crate::orchestrator::queues::Queue;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageEngine {
    Sled,
}

impl StorageEngine {
    fn parse(raw: &str) -> Result<Self, StorageError> {
        match raw {
            "sled" => Ok(Self::Sled),
            other => Err(StorageError::UnsupportedEngine {
                engine: other.to_owned(),
            }),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Sled => "sled",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SledMode {
    LowSpace,
    HighThroughput,
}

impl SledMode {
    fn parse(raw: &str) -> Result<Self, StorageError> {
        match raw {
            "low_space" => Ok(Self::LowSpace),
            "high_throughput" => Ok(Self::HighThroughput),
            other => Err(StorageError::InvalidSledMode {
                mode: other.to_owned(),
            }),
        }
    }
}

pub trait StorageBackend: Send + Sync {
    fn flush(&self) -> Result<(), StorageError>;
    fn load_queues(&self) -> Result<Vec<Queue>, StorageError>;
    fn replace_queues(&self, queues: &[Queue]) -> Result<(), StorageError>;
    fn get_job_payload_by_uuid(&self, job_uuid: Uuid) -> Result<Option<serde_json::Value>, StorageError>;
}

pub struct SledStorage {
    db: sled::Db,
}

const KEYSPACE_VERSION: &str = "v1";
const QUEUE_PREFIX: &[u8] = b"v1:q:";

fn queue_key(queue_name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(QUEUE_PREFIX.len() + queue_name.len());
    key.extend_from_slice(QUEUE_PREFIX);
    key.extend_from_slice(queue_name.as_bytes());
    key
}

fn job_key(job_uuid: Uuid) -> String {
    format!("{KEYSPACE_VERSION}:j:{job_uuid}")
}

impl StorageBackend for SledStorage {
    fn flush(&self) -> Result<(), StorageError> {
        self.db.flush().map(|_| ()).map_err(StorageError::Sled)
    }

    fn load_queues(&self) -> Result<Vec<Queue>, StorageError> {
        let mut queues = Vec::new();
        for entry in self.db.scan_prefix(QUEUE_PREFIX) {
            let (_, value) = entry.map_err(StorageError::Sled)?;
            let queue: Queue = serde_json::from_slice(value.as_ref()).map_err(StorageError::DeserializeQueue)?;
            queues.push(queue);
        }
        queues.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(queues)
    }

    fn replace_queues(&self, queues: &[Queue]) -> Result<(), StorageError> {
        let mut batch = sled::Batch::default();
        for entry in self.db.scan_prefix(QUEUE_PREFIX) {
            let (key, _) = entry.map_err(StorageError::Sled)?;
            batch.remove(key);
        }

        for queue in queues {
            let value = serde_json::to_vec(queue).map_err(StorageError::SerializeQueue)?;
            batch.insert(queue_key(&queue.name), value);
        }

        self.db.apply_batch(batch).map_err(StorageError::Sled)?;
        self.db.flush().map_err(StorageError::Sled)?;
        Ok(())
    }

    fn get_job_payload_by_uuid(
        &self,
        job_uuid: Uuid,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        let key = job_key(job_uuid);
        let value = self.db.get(key.as_bytes()).map_err(StorageError::Sled)?;
        value
            .map(|raw| serde_json::from_slice(raw.as_ref()).map_err(StorageError::DeserializeJob))
            .transpose()
    }
}

pub struct StorageFacade {
    engine: StorageEngine,
    data_path: PathBuf,
    backend: Box<dyn StorageBackend>,
}

impl StorageFacade {
    pub fn initialize(app_config: &AppConfig, logger: &Logger) -> Result<Self, StorageError> {
        let engine = StorageEngine::parse(&app_config.storage.engine)?;
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
                let mut config = sled::Config::new().path(&data_path);

                if let Some(cache_capacity) = app_config.storage.sled.cache_capacity {
                    config = config.cache_capacity(cache_capacity);
                }

                if let Some(mode) = app_config.storage.sled.mode.as_deref() {
                    let parsed_mode = SledMode::parse(mode)?;
                    config = config.mode(match parsed_mode {
                        SledMode::LowSpace => sled::Mode::LowSpace,
                        SledMode::HighThroughput => sled::Mode::HighThroughput,
                    });
                }

                let db = config.open().map_err(StorageError::Sled)?;
                Box::new(SledStorage { db })
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

    pub fn get_job_payload_by_uuid(
        &self,
        job_uuid: Uuid,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        self.backend.get_job_payload_by_uuid(job_uuid)
    }
}

#[derive(Debug)]
pub enum StorageError {
    UnsupportedEngine {
        engine: String,
    },
    InvalidSledMode {
        mode: String,
    },
    HomeDirectoryUnavailable,
    CreateDataDir {
        path: PathBuf,
        source: std::io::Error,
    },
    SerializeQueue(serde_json::Error),
    DeserializeQueue(serde_json::Error),
    DeserializeJob(serde_json::Error),
    Sled(sled::Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedEngine { engine } => write!(
                f,
                "unsupported storage.engine '{}'; currently only 'sled' is supported",
                engine
            ),
            Self::InvalidSledMode { mode } => write!(
                f,
                "invalid storage.sled.mode '{}'; allowed values: low_space, high_throughput",
                mode
            ),
            Self::HomeDirectoryUnavailable => {
                write!(f, "cannot resolve storage path because HOME is not set")
            }
            Self::CreateDataDir { path, source } => write!(
                f,
                "failed to create storage data directory '{}': {source}",
                path.display()
            ),
            Self::SerializeQueue(source) => {
                write!(f, "failed to serialize queue for storage: {source}")
            }
            Self::DeserializeQueue(source) => {
                write!(f, "failed to deserialize queue from storage: {source}")
            }
            Self::DeserializeJob(source) => {
                write!(f, "failed to deserialize job payload from storage: {source}")
            }
            Self::Sled(source) => write!(f, "sled storage error: {source}"),
        }
    }
}

impl std::error::Error for StorageError {}

fn expand_home_path(raw_path: &str) -> Result<PathBuf, StorageError> {
    if raw_path.starts_with("~/") {
        let home = std::env::var("HOME").map_err(|_| StorageError::HomeDirectoryUnavailable)?;
        return Ok(PathBuf::from(home).join(raw_path.trim_start_matches("~/")));
    }

    if raw_path == "$HOME" || raw_path.starts_with("$HOME/") {
        let home = std::env::var("HOME").map_err(|_| StorageError::HomeDirectoryUnavailable)?;
        let suffix = raw_path.strip_prefix("$HOME").unwrap_or_default();
        return Ok(PathBuf::from(format!("{home}{suffix}")));
    }

    Ok(PathBuf::from(raw_path))
}

#[cfg(test)]
mod tests {
    use crate::config::{AppConfig, HeartbeatConfig, LoggingConfig, ServerConfig, SledConfig, StorageConfig, WireConfig};
    use crate::logging::LoggerConfig;
    use crate::orchestrator::queues::Queue;
    use crate::storage::{StorageFacade, StorageEngine};

    use super::expand_home_path;

    fn test_storage(path: &str) -> StorageFacade {
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
            },
            storage: StorageConfig {
                engine: "sled".to_owned(),
                path: path.to_owned(),
                sled: SledConfig::default(),
            },
        };
        let logger = crate::logging::Logger::new(LoggerConfig {
            min_level: crate::logging::LogLevel::Error,
            human_friendly: false,
        });
        StorageFacade::initialize(&app_config, &logger).expect("storage init should work")
    }

    fn unique_temp_path(label: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "overhop-storage-test-{label}-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        path.to_string_lossy().to_string()
    }

    #[test]
    fn expands_tilde_prefix_to_home() {
        let home = std::env::var("HOME").expect("HOME should be available in tests");
        let expanded = expand_home_path("~/.overhop/data").expect("expansion should work");
        assert_eq!(expanded, std::path::Path::new(&home).join(".overhop/data"));
    }

    #[test]
    fn expands_home_env_prefix() {
        let home = std::env::var("HOME").expect("HOME should be available in tests");
        let expanded = expand_home_path("$HOME/.overhop/data").expect("expansion should work");
        assert_eq!(expanded, std::path::Path::new(&home).join(".overhop/data"));
    }

    #[test]
    fn keeps_absolute_paths_unchanged() {
        let expanded = expand_home_path("/var/lib/overhop/data").expect("expansion should work");
        assert_eq!(expanded, std::path::PathBuf::from("/var/lib/overhop/data"));
    }

    #[test]
    fn queue_replace_and_load_roundtrip() {
        let path = unique_temp_path("queues-roundtrip");
        let storage = test_storage(&path);
        assert_eq!(storage.engine(), StorageEngine::Sled);
        assert!(storage.data_path().to_string_lossy().contains("overhop-storage-test"));

        let queue = Queue::new("critical", None);
        storage
            .replace_queues(&[queue.clone()])
            .expect("queue persist should pass");

        let loaded = storage.load_queues().expect("queue load should pass");
        assert_eq!(loaded, vec![queue]);
        let missing_job = storage
            .get_job_payload_by_uuid(uuid::Uuid::new_v4())
            .expect("job lookup should work");
        assert!(missing_job.is_none());

        let _ = std::fs::remove_dir_all(path);
    }
}
