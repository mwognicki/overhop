use std::path::Path;

use uuid::Uuid;

use crate::orchestrator::queues::Queue;

use super::{SledMode, StorageBackend, StorageError};

pub struct SledStorage {
    db: sled::Db,
}

const KEYSPACE_VERSION: &str = "v1";
const QUEUE_PREFIX: &[u8] = b"v1:q:";

impl SledStorage {
    pub fn open(
        data_path: &Path,
        cache_capacity: Option<u64>,
        mode: Option<SledMode>,
    ) -> Result<Self, StorageError> {
        let mut config = sled::Config::new().path(data_path);

        if let Some(cache_capacity) = cache_capacity {
            config = config.cache_capacity(cache_capacity);
        }

        if let Some(mode) = mode {
            config = config.mode(match mode {
                SledMode::LowSpace => sled::Mode::LowSpace,
                SledMode::HighThroughput => sled::Mode::HighThroughput,
            });
        }

        let db = config.open().map_err(StorageError::Sled)?;
        Ok(Self { db })
    }
}

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
            let queue: Queue =
                serde_json::from_slice(value.as_ref()).map_err(StorageError::DeserializeQueue)?;
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
