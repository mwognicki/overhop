use uuid::Uuid;

use crate::orchestrator::queues::Queue;

use super::StorageError;

pub trait StorageBackend: Send + Sync {
    fn flush(&self) -> Result<(), StorageError>;
    fn load_queues(&self) -> Result<Vec<Queue>, StorageError>;
    fn replace_queues(&self, queues: &[Queue]) -> Result<(), StorageError>;
    fn list_job_uuids_by_status(&self, status: &str) -> Result<Vec<Uuid>, StorageError>;
    fn list_job_uuids_by_status_fifo(&self, status: &str) -> Result<Vec<Uuid>, StorageError>;
    fn upsert_job_record(
        &self,
        job_uuid: Uuid,
        record: &serde_json::Value,
        execution_start_ms: i64,
        created_at_ms: i64,
        queue_name: &str,
        status: &str,
    ) -> Result<(), StorageError>;
    fn get_job_payload_by_uuid(&self, job_uuid: Uuid) -> Result<Option<serde_json::Value>, StorageError>;
}
