use std::fmt;
use std::path::PathBuf;

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
    SerializeJob(serde_json::Error),
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
            Self::SerializeJob(source) => {
                write!(f, "failed to serialize job record for storage: {source}")
            }
            Self::DeserializeJob(source) => {
                write!(f, "failed to deserialize job record from storage: {source}")
            }
            Self::Sled(source) => write!(f, "sled storage error: {source}"),
        }
    }
}

impl std::error::Error for StorageError {}
