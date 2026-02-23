use std::path::Path;

use chrono::DateTime;
use uuid::Uuid;

use crate::orchestrator::queues::Queue;

use super::{SledMode, StorageBackend, StorageError};

pub struct SledStorage {
    db: sled::Db,
}

const KEYSPACE_VERSION: &str = "v1";
const QUEUE_PREFIX: &[u8] = b"v1:q:";
const JOB_STATUS_PREFIX: &[u8] = b"v1:status:";
const JOB_STATUS_FIFO_PREFIX: &[u8] = b"v1:status_fifo:";

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

fn job_queue_time_key(execution_start_ms: i64, queue_name: &str, job_uuid: Uuid) -> Vec<u8> {
    format!("{KEYSPACE_VERSION}:j_qt:{execution_start_ms}:{queue_name}:{job_uuid}").into_bytes()
}

fn job_status_key(job_uuid: Uuid) -> Vec<u8> {
    let mut key = Vec::with_capacity(JOB_STATUS_PREFIX.len() + 36);
    key.extend_from_slice(JOB_STATUS_PREFIX);
    key.extend_from_slice(job_uuid.to_string().as_bytes());
    key
}

fn i64_to_ordered_be_bytes(value: i64) -> [u8; 8] {
    let sortable = (value as u64) ^ (1_u64 << 63);
    sortable.to_be_bytes()
}

fn job_status_fifo_prefix(status: &str) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(JOB_STATUS_FIFO_PREFIX.len() + status.len() + 1);
    prefix.extend_from_slice(JOB_STATUS_FIFO_PREFIX);
    prefix.extend_from_slice(status.as_bytes());
    prefix.push(b':');
    prefix
}

fn job_status_fifo_key(status: &str, created_at_ms: i64, job_uuid: Uuid) -> Vec<u8> {
    let mut key = job_status_fifo_prefix(status);
    key.extend_from_slice(&i64_to_ordered_be_bytes(created_at_ms));
    key.extend_from_slice(job_uuid.as_bytes());
    key
}

fn parse_rfc3339_timestamp_ms(value: &serde_json::Value, field: &str) -> Option<i64> {
    let raw = value.get(field)?.as_str()?;
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|parsed| parsed.timestamp_millis())
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

    fn list_job_uuids_by_status(&self, status: &str) -> Result<Vec<Uuid>, StorageError> {
        let mut uuids = Vec::new();
        for entry in self.db.scan_prefix(JOB_STATUS_PREFIX) {
            let (key, value) = entry.map_err(StorageError::Sled)?;
            if value.as_ref() != status.as_bytes() {
                continue;
            }
            let suffix = &key.as_ref()[JOB_STATUS_PREFIX.len()..];
            if let Ok(raw_uuid) = std::str::from_utf8(suffix) {
                if let Ok(parsed) = Uuid::parse_str(raw_uuid) {
                    uuids.push(parsed);
                }
            }
        }
        Ok(uuids)
    }

    fn list_job_uuids_by_status_fifo(&self, status: &str) -> Result<Vec<Uuid>, StorageError> {
        let mut uuids = Vec::new();
        let prefix = job_status_fifo_prefix(status);
        for entry in self.db.scan_prefix(prefix.clone()) {
            let (key, _) = entry.map_err(StorageError::Sled)?;
            let key_ref = key.as_ref();
            if key_ref.len() != prefix.len() + 8 + 16 {
                continue;
            }
            let uuid_offset = prefix.len() + 8;
            if let Ok(uuid) = Uuid::from_slice(&key_ref[uuid_offset..uuid_offset + 16]) {
                uuids.push(uuid);
            }
        }
        Ok(uuids)
    }

    fn upsert_job_record(
        &self,
        job_uuid: Uuid,
        record: &serde_json::Value,
        execution_start_ms: i64,
        created_at_ms: i64,
        queue_name: &str,
        status: &str,
    ) -> Result<(), StorageError> {
        let primary_key = job_key(job_uuid);
        let queue_time_key = job_queue_time_key(execution_start_ms, queue_name, job_uuid);
        let status_key = job_status_key(job_uuid);
        let status_fifo_key = job_status_fifo_key(status, created_at_ms, job_uuid);
        let raw = serde_json::to_vec(record).map_err(StorageError::SerializeJob)?;

        let mut batch = sled::Batch::default();
        if let Some(existing_raw) = self.db.get(primary_key.as_bytes()).map_err(StorageError::Sled)? {
            if let Ok(existing) = serde_json::from_slice::<serde_json::Value>(existing_raw.as_ref()) {
                let existing_status = existing
                    .get("status")
                    .and_then(serde_json::Value::as_str);
                let existing_created_at_ms = parse_rfc3339_timestamp_ms(&existing, "created_at");
                if let (Some(existing_status), Some(existing_created_at_ms)) =
                    (existing_status, existing_created_at_ms)
                {
                    batch.remove(job_status_fifo_key(
                        existing_status,
                        existing_created_at_ms,
                        job_uuid,
                    ));
                }

                let existing_queue_name = existing.get("queue_name").and_then(serde_json::Value::as_str);
                let existing_execution_start_ms =
                    parse_rfc3339_timestamp_ms(&existing, "execution_start_at");
                if let (Some(existing_queue_name), Some(existing_execution_start_ms)) =
                    (existing_queue_name, existing_execution_start_ms)
                {
                    batch.remove(job_queue_time_key(
                        existing_execution_start_ms,
                        existing_queue_name,
                        job_uuid,
                    ));
                }
            }
        }

        batch.insert(primary_key.as_bytes(), raw);
        batch.insert(queue_time_key, &[1_u8]);
        batch.insert(status_key, status.as_bytes());
        batch.insert(status_fifo_key, &[1_u8]);
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

    fn remove_job_record(&self, job_uuid: Uuid) -> Result<bool, StorageError> {
        let primary_key = job_key(job_uuid);
        let Some(existing_raw) = self.db.get(primary_key.as_bytes()).map_err(StorageError::Sled)? else {
            return Ok(false);
        };

        let parsed = serde_json::from_slice::<serde_json::Value>(existing_raw.as_ref())
            .map_err(StorageError::DeserializeJob)?;
        let existing_status = parsed.get("status").and_then(serde_json::Value::as_str);
        let existing_created_at_ms = parse_rfc3339_timestamp_ms(&parsed, "created_at");
        let existing_queue_name = parsed.get("queue_name").and_then(serde_json::Value::as_str);
        let existing_execution_start_ms = parse_rfc3339_timestamp_ms(&parsed, "execution_start_at");

        let mut batch = sled::Batch::default();
        batch.remove(primary_key.as_bytes());
        batch.remove(job_status_key(job_uuid));
        if let (Some(existing_status), Some(existing_created_at_ms)) =
            (existing_status, existing_created_at_ms)
        {
            batch.remove(job_status_fifo_key(
                existing_status,
                existing_created_at_ms,
                job_uuid,
            ));
        }
        if let (Some(existing_queue_name), Some(existing_execution_start_ms)) =
            (existing_queue_name, existing_execution_start_ms)
        {
            batch.remove(job_queue_time_key(
                existing_execution_start_ms,
                existing_queue_name,
                job_uuid,
            ));
        }

        self.db.apply_batch(batch).map_err(StorageError::Sled)?;
        self.db.flush().map_err(StorageError::Sled)?;
        Ok(true)
    }
}
