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
    Waiting,
    Delayed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobRuntimeMetadata {
    pub attempts_so_far: u32,
}

#[derive(Clone, Debug, PartialEq)]
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
    pub runtime: JobRuntimeMetadata,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobLookup {
    pub job_id: String,
    pub queue_name: String,
    pub job_uuid: Uuid,
}

#[derive(Debug)]
pub enum JobsPoolError {
    QueueNotFound { queue_name: String },
    SystemQueueForbidden { queue_name: String },
    SystemQueueAccessForbidden { queue_name: String },
    InvalidMaxAttempts { max_attempts: u32 },
    InvalidRetryIntervalMs { retry_interval_ms: u64 },
    InvalidJobId { job_id: String },
    PayloadSerializationFailed(serde_json::Error),
}

impl fmt::Display for JobsPoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueNotFound { queue_name } => {
                write!(f, "queue '{queue_name}' not found")
            }
            Self::SystemQueueForbidden { queue_name } => {
                write!(f, "queue '{queue_name}' is a system queue and cannot accept jobs")
            }
            Self::SystemQueueAccessForbidden { queue_name } => {
                write!(f, "queue '{queue_name}' is a system queue and cannot be accessed")
            }
            Self::InvalidMaxAttempts { max_attempts } => {
                write!(f, "max_attempts must be >= 1 when provided, got {max_attempts}")
            }
            Self::InvalidRetryIntervalMs { retry_interval_ms } => {
                write!(f, "retry interval must be > 0 when provided, got {retry_interval_ms}")
            }
            Self::InvalidJobId { job_id } => {
                write!(f, "job id '{job_id}' is invalid; expected '<queue-name>:<uuid>'")
            }
            Self::PayloadSerializationFailed(source) => {
                write!(f, "job payload serialization failed: {source}")
            }
        }
    }
}

impl std::error::Error for JobsPoolError {}

pub struct JobsPool {
    jobs: HashMap<Uuid, Job>,
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
        }
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
        if queue_name.starts_with('_') {
            return Err(JobsPoolError::SystemQueueForbidden {
                queue_name: queue_name.to_owned(),
            });
        }

        if let Some(max_attempts) = options.max_attempts {
            if max_attempts == 0 {
                return Err(JobsPoolError::InvalidMaxAttempts { max_attempts });
            }
        }

        if let Some(retry_interval_ms) = options.retry_interval_ms {
            if retry_interval_ms == 0 {
                return Err(JobsPoolError::InvalidRetryIntervalMs { retry_interval_ms });
            }
        }

        let uuid = Uuid::new_v4();
        let execution_start_at = options.scheduled_at.unwrap_or_else(Utc::now);
        let staged_job = Job {
            uuid,
            job_id: format!("{queue_name}:{uuid}"),
            queue_name: queue_name.to_owned(),
            payload: options.payload,
            status: JobStatus::New,
            execution_start_at,
            max_attempts: options.max_attempts,
            retry_interval_ms: options.retry_interval_ms,
            created_at: Utc::now(),
            runtime: JobRuntimeMetadata { attempts_so_far: 0 },
        };

        self.jobs.insert(uuid, staged_job);

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

    pub fn remove_job(&mut self, uuid: Uuid) -> Option<Job> {
        self.jobs.remove(&uuid)
    }

    pub fn resolve_job_lookup(&self, job_id: &str) -> Result<JobLookup, JobsPoolError> {
        let Some((queue_name, uuid_raw)) = job_id.split_once(':') else {
            return Err(JobsPoolError::InvalidJobId {
                job_id: job_id.to_owned(),
            });
        };
        if queue_name.is_empty() {
            return Err(JobsPoolError::InvalidJobId {
                job_id: job_id.to_owned(),
            });
        }
        let Ok(job_uuid) = Uuid::parse_str(uuid_raw) else {
            return Err(JobsPoolError::InvalidJobId {
                job_id: job_id.to_owned(),
            });
        };
        if queue_name.starts_with('_') {
            return Err(JobsPoolError::SystemQueueAccessForbidden {
                queue_name: queue_name.to_owned(),
            });
        }
        Ok(JobLookup {
            job_id: job_id.to_owned(),
            queue_name: queue_name.to_owned(),
            job_uuid,
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use serde_json::json;
    use uuid::Uuid;

    use crate::orchestrator::queues::QueuePool;

    use super::{JobStatus, JobsPool, JobsPoolError, NewJobOptions};

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
        assert_eq!(job.runtime.attempts_so_far, 0);
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
    fn enqueue_rejects_zero_max_attempts() {
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
                    max_attempts: Some(0),
                    ..NewJobOptions::default()
                },
            )
            .expect_err("zero max_attempts should fail");

        assert!(matches!(err, JobsPoolError::InvalidMaxAttempts { .. }));
    }

    #[test]
    fn enqueue_rejects_system_queue() {
        let queue_pool = QueuePool::reconstruct(vec![crate::orchestrator::queues::Queue::new(
            "_system",
            None,
        )])
        .expect("queue reconstruct should pass");
        let mut jobs_pool = JobsPool::new();

        let err = jobs_pool
            .enqueue_job(&queue_pool, "_system", NewJobOptions::default())
            .expect_err("enqueue should fail for system queue");
        assert!(matches!(err, JobsPoolError::SystemQueueForbidden { .. }));
    }

    #[test]
    fn can_remove_staged_job_from_pool() {
        let mut queue_pool = QueuePool::new();
        queue_pool
            .register_queue("persisted", None)
            .expect("queue should register");
        let mut jobs_pool = JobsPool::new();

        let staged_uuid = jobs_pool
            .enqueue_job(&queue_pool, "persisted", NewJobOptions::default())
            .expect("enqueue should stage job");
        assert_eq!(jobs_pool.count(), 1);
        let removed = jobs_pool.remove_job(staged_uuid);
        assert!(removed.is_some());
        assert_eq!(jobs_pool.count(), 0);
    }

    #[test]
    fn resolve_job_lookup_validates_and_extracts_parts() {
        let jobs_pool = JobsPool::new();
        let uuid = Uuid::new_v4();
        let lookup = jobs_pool
            .resolve_job_lookup(&format!("critical:{uuid}"))
            .expect("jid should parse");
        assert_eq!(lookup.queue_name, "critical");
        assert_eq!(lookup.job_uuid, uuid);

        let invalid = jobs_pool
            .resolve_job_lookup("invalid-jid")
            .expect_err("invalid jid should fail");
        assert!(matches!(invalid, JobsPoolError::InvalidJobId { .. }));

        let sys = jobs_pool
            .resolve_job_lookup(&format!("_system:{uuid}"))
            .expect_err("system queue jid should fail");
        assert!(matches!(sys, JobsPoolError::SystemQueueAccessForbidden { .. }));
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
