use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::orchestrator::queues::QueuePool;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobStatus {
    New,
}

#[derive(Clone, Debug)]
pub struct Job {
    pub uuid: Uuid,
    pub job_id: String,
    pub queue_name: String,
    pub payload: Option<Value>,
    pub status: JobStatus,
    pub execution_start_at: DateTime<Utc>,
    pub max_attempts: Option<u32>,
    pub retry_interval_ms: Option<u64>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Default)]
pub struct NewJobOptions {
    pub payload: Option<Value>,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub max_attempts: Option<u32>,
    pub retry_interval_ms: Option<u64>,
}

impl NewJobOptions {
    pub fn with_serializable_payload<T: Serialize>(mut self, payload: T) -> Result<Self, JobsPoolError> {
        self.payload = Some(
            serde_json::to_value(payload).map_err(JobsPoolError::PayloadSerializationFailed)?,
        );
        Ok(self)
    }
}

#[derive(Clone, Debug, Default)]
pub struct JobsPoolSnapshot {
    pub jobs: Vec<Job>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PersistenceFlowMode {
    PoolThenPersist,
    PersistThenPoolReserved,
}

pub trait JobsStorageBackend: Send + Sync {
    fn persist_new_job(&self, job: &Job) -> Result<(), JobsStorageError>;
}

#[derive(Clone, Copy, Debug)]
pub struct NoopJobsStorageBackend;

impl JobsStorageBackend for NoopJobsStorageBackend {
    fn persist_new_job(&self, _job: &Job) -> Result<(), JobsStorageError> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum JobsStorageError {
    PersistFailed { reason: String },
}

impl fmt::Display for JobsStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PersistFailed { reason } => write!(f, "jobs persistence failed: {reason}"),
        }
    }
}

impl std::error::Error for JobsStorageError {}

#[derive(Debug)]
pub enum JobsPoolError {
    QueueNotFound { queue_name: String },
    InvalidRetryIntervalMs { retry_interval_ms: u64 },
    PayloadSerializationFailed(serde_json::Error),
    Storage(JobsStorageError),
}

impl fmt::Display for JobsPoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueNotFound { queue_name } => {
                write!(f, "queue '{queue_name}' not found")
            }
            Self::InvalidRetryIntervalMs { retry_interval_ms } => {
                write!(f, "retry interval must be > 0 when provided, got {retry_interval_ms}")
            }
            Self::PayloadSerializationFailed(source) => {
                write!(f, "job payload serialization failed: {source}")
            }
            Self::Storage(source) => write!(f, "{source}"),
        }
    }
}

impl std::error::Error for JobsPoolError {}

pub struct JobsPool {
    jobs: HashMap<Uuid, Job>,
    storage: Box<dyn JobsStorageBackend>,
    persistence_flow_mode: PersistenceFlowMode,
}

impl Default for JobsPool {
    fn default() -> Self {
        Self::new()
    }
}

impl JobsPool {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            storage: Box::new(NoopJobsStorageBackend),
            persistence_flow_mode: PersistenceFlowMode::PoolThenPersist,
        }
    }

    pub fn with_storage(storage: Box<dyn JobsStorageBackend>) -> Self {
        Self {
            jobs: HashMap::new(),
            storage,
            persistence_flow_mode: PersistenceFlowMode::PoolThenPersist,
        }
    }

    pub fn persistence_flow_mode(&self) -> PersistenceFlowMode {
        self.persistence_flow_mode
    }

    pub fn reserve_persist_then_pool_mode(&mut self) {
        self.persistence_flow_mode = PersistenceFlowMode::PersistThenPoolReserved;
    }

    pub fn enqueue_job(
        &mut self,
        queue_pool: &QueuePool,
        queue_name: &str,
        options: NewJobOptions,
    ) -> Result<Uuid, JobsPoolError> {
        if queue_pool.get_queue(queue_name).is_none() {
            return Err(JobsPoolError::QueueNotFound {
                queue_name: queue_name.to_owned(),
            });
        }

        if let Some(retry_interval_ms) = options.retry_interval_ms {
            if retry_interval_ms == 0 {
                return Err(JobsPoolError::InvalidRetryIntervalMs { retry_interval_ms });
            }
        }

        let uuid = Uuid::new_v4();
        let execution_start_at = options.scheduled_at.unwrap_or_else(Utc::now);
        let job = Job {
            uuid,
            job_id: format!("{queue_name}:{uuid}"),
            queue_name: queue_name.to_owned(),
            payload: options.payload,
            status: JobStatus::New,
            execution_start_at,
            max_attempts: options.max_attempts,
            retry_interval_ms: options.retry_interval_ms,
            created_at: Utc::now(),
        };

        self.jobs.insert(uuid, job);

        if let Some(stored_job) = self.jobs.get(&uuid) {
            if let Err(error) = self.storage.persist_new_job(stored_job) {
                let _ = self.jobs.remove(&uuid);
                return Err(JobsPoolError::Storage(error));
            }
        }

        Ok(uuid)
    }

    pub fn get_job(&self, uuid: Uuid) -> Option<&Job> {
        self.jobs.get(&uuid)
    }

    pub fn snapshot(&self) -> JobsPoolSnapshot {
        let mut jobs = self.jobs.values().cloned().collect::<Vec<_>>();
        jobs.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.uuid.cmp(&b.uuid)));
        JobsPoolSnapshot { jobs }
    }

    pub fn count(&self) -> usize {
        self.jobs.len()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use serde_json::json;

    use crate::orchestrator::queues::QueuePool;

    use super::{
        JobStatus, JobsPool, JobsPoolError, JobsStorageBackend, JobsStorageError, NewJobOptions,
        PersistenceFlowMode,
    };

    struct FailingStorage;

    impl JobsStorageBackend for FailingStorage {
        fn persist_new_job(&self, _job: &super::Job) -> Result<(), JobsStorageError> {
            Err(JobsStorageError::PersistFailed {
                reason: "storage unavailable".to_owned(),
            })
        }
    }

    #[test]
    fn enqueue_requires_existing_queue() {
        let queue_pool = QueuePool::new();
        let mut jobs_pool = JobsPool::new();

        let err = jobs_pool
            .enqueue_job(&queue_pool, "missing", NewJobOptions::default())
            .expect_err("enqueue should fail for unknown queue");
        assert!(matches!(err, JobsPoolError::QueueNotFound { .. }));
    }

    #[test]
    fn enqueue_creates_new_job_with_default_status_and_immediate_start() {
        let mut queue_pool = QueuePool::new();
        queue_pool
            .register_queue("critical", None)
            .expect("queue should register");
        let mut jobs_pool = JobsPool::new();

        let before = chrono::Utc::now();
        let job_uuid = jobs_pool
            .enqueue_job(&queue_pool, "critical", NewJobOptions::default())
            .expect("enqueue should pass");
        let after = chrono::Utc::now();

        let job = jobs_pool.get_job(job_uuid).expect("job should exist");
        assert_eq!(job.status, JobStatus::New);
        assert_eq!(job.uuid, job_uuid);
        assert_eq!(job.queue_name, "critical");
        assert_eq!(job.job_id, format!("critical:{job_uuid}"));
        assert!(job.execution_start_at >= before && job.execution_start_at <= after);
        assert!(job.created_at >= before && job.created_at <= after);
        assert!(job.payload.is_none());
        assert_eq!(job.max_attempts, None);
        assert_eq!(job.retry_interval_ms, None);
    }

    #[test]
    fn enqueue_uses_scheduled_timestamp_and_json_payload() {
        let mut queue_pool = QueuePool::new();
        queue_pool
            .register_queue("emails", None)
            .expect("queue should register");
        let mut jobs_pool = JobsPool::new();

        let scheduled_at = chrono::Utc::now() + Duration::minutes(5);
        let options = NewJobOptions {
            payload: Some(json!({"to": "ops@example.com", "priority": 1})),
            scheduled_at: Some(scheduled_at),
            max_attempts: Some(3),
            retry_interval_ms: Some(250),
        };

        let job_uuid = jobs_pool
            .enqueue_job(&queue_pool, "emails", options)
            .expect("enqueue should pass");
        let job = jobs_pool.get_job(job_uuid).expect("job should exist");

        assert_eq!(job.execution_start_at, scheduled_at);
        assert_eq!(job.max_attempts, Some(3));
        assert_eq!(job.retry_interval_ms, Some(250));
        assert_eq!(
            job.payload,
            Some(json!({"to": "ops@example.com", "priority": 1}))
        );
    }

    #[test]
    fn enqueue_rejects_zero_retry_interval() {
        let mut queue_pool = QueuePool::new();
        queue_pool
            .register_queue("jobs", None)
            .expect("queue should register");
        let mut jobs_pool = JobsPool::new();

        let err = jobs_pool
            .enqueue_job(
                &queue_pool,
                "jobs",
                NewJobOptions {
                    retry_interval_ms: Some(0),
                    ..NewJobOptions::default()
                },
            )
            .expect_err("zero retry interval should fail");

        assert!(matches!(err, JobsPoolError::InvalidRetryIntervalMs { .. }));
    }

    #[test]
    fn enqueue_rolls_back_when_storage_fails() {
        let mut queue_pool = QueuePool::new();
        queue_pool
            .register_queue("persisted", None)
            .expect("queue should register");
        let mut jobs_pool = JobsPool::with_storage(Box::new(FailingStorage));

        let err = jobs_pool
            .enqueue_job(&queue_pool, "persisted", NewJobOptions::default())
            .expect_err("enqueue should fail when storage fails");
        assert!(matches!(err, JobsPoolError::Storage(_)));
        assert_eq!(jobs_pool.count(), 0);
    }

    #[test]
    fn can_prepare_reserved_persistence_flow_mode() {
        let mut jobs_pool = JobsPool::new();
        assert_eq!(
            jobs_pool.persistence_flow_mode(),
            PersistenceFlowMode::PoolThenPersist
        );

        jobs_pool.reserve_persist_then_pool_mode();
        assert_eq!(
            jobs_pool.persistence_flow_mode(),
            PersistenceFlowMode::PersistThenPoolReserved
        );
    }

    #[test]
    fn snapshot_exposes_jobs_collection() {
        let mut queue_pool = QueuePool::new();
        queue_pool
            .register_queue("snapshot", None)
            .expect("queue should register");
        let mut jobs_pool = JobsPool::new();
        let uuid = jobs_pool
            .enqueue_job(&queue_pool, "snapshot", NewJobOptions::default())
            .expect("enqueue should pass");

        let snapshot = jobs_pool.snapshot();
        assert_eq!(snapshot.jobs.len(), 1);
        assert_eq!(snapshot.jobs[0].uuid, uuid);
    }

    #[test]
    fn serializable_payload_helper_accepts_generic_values() {
        let options = NewJobOptions::default()
            .with_serializable_payload(vec!["a", "b", "c"])
            .expect("payload should serialize");

        assert_eq!(options.payload, Some(json!(["a", "b", "c"])));
    }
}
