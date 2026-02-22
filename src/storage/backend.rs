use uuid::Uuid;

use crate::orchestrator::queues::Queue;

use super::StorageError;

pub trait StorageBackend: Send + Sync {
    fn flush(&self) -> Result<(), StorageError>;
    fn load_queues(&self) -> Result<Vec<Queue>, StorageError>;
    fn replace_queues(&self, queues: &[Queue]) -> Result<(), StorageError>;
    fn get_job_payload_by_uuid(&self, job_uuid: Uuid) -> Result<Option<serde_json::Value>, StorageError>;
}
