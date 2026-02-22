use std::fmt;
use std::fs;
use std::path::PathBuf;

use serde_json::json;

use crate::config::AppConfig;
use crate::logging::{LogLevel, Logger};

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
}

pub struct SledStorage {
    _db: sled::Db,
}

impl StorageBackend for SledStorage {}

pub struct StorageFacade {
    _engine: StorageEngine,
    _data_path: PathBuf,
    _backend: Box<dyn StorageBackend>,
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
                Box::new(SledStorage { _db: db })
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
            _engine: engine,
            _data_path: data_path,
            _backend: backend,
        })
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
    use super::expand_home_path;

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
}
