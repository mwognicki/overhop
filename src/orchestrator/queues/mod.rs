pub mod persistent;

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueueConfig {
    pub concurrency_limit: Option<u32>,
    pub allow_job_overrides: bool,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            concurrency_limit: None,
            allow_job_overrides: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueueState {
    Active,
    Paused,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Queue {
    pub name: String,
    pub config: QueueConfig,
    pub state: QueueState,
}

impl Queue {
    pub fn new(name: impl Into<String>, config: Option<QueueConfig>) -> Self {
        Self {
            name: name.into(),
            config: config.unwrap_or_default(),
            state: QueueState::Active,
        }
    }

    pub fn pause(&mut self) {
        self.state = QueueState::Paused;
    }

    pub fn resume(&mut self) {
        self.state = QueueState::Active;
    }

    pub fn is_paused(&self) -> bool {
        self.state == QueueState::Paused
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueuePoolSnapshot {
    pub queues: Vec<Queue>,
    pub bootstrapped: bool,
}

#[derive(Debug)]
pub enum QueuePoolError {
    DuplicateQueueName { name: String },
    QueueNotFound { name: String },
    Serialize(serde_json::Error),
    Deserialize(serde_json::Error),
}

impl fmt::Display for QueuePoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateQueueName { name } => {
                write!(f, "queue '{name}' is already registered")
            }
            Self::QueueNotFound { name } => write!(f, "queue '{name}' not found"),
            Self::Serialize(source) => write!(f, "failed to serialize queue pool: {source}"),
            Self::Deserialize(source) => write!(f, "failed to deserialize queue pool: {source}"),
        }
    }
}

impl std::error::Error for QueuePoolError {}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueuePool {
    queues: HashMap<String, Queue>,
    bootstrapped: bool,
}

impl QueuePool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reconstruct(queues: Vec<Queue>) -> Result<Self, QueuePoolError> {
        let mut pool = Self::new();
        for queue in queues {
            pool.register_queue(queue.name.clone(), Some(queue.config.clone()))?;
            if queue.is_paused() {
                pool.pause_queue(&queue.name)?;
            }
        }
        Ok(pool)
    }

    pub fn from_json(json: &str) -> Result<Self, QueuePoolError> {
        let snapshot: QueuePoolSnapshot =
            serde_json::from_str(json).map_err(QueuePoolError::Deserialize)?;
        let mut pool = Self::new();
        for queue in snapshot.queues {
            pool.register_queue(queue.name.clone(), Some(queue.config.clone()))?;
            if queue.is_paused() {
                pool.pause_queue(&queue.name)?;
            }
        }
        if snapshot.bootstrapped {
            pool.mark_bootstrapped();
        }
        Ok(pool)
    }

    pub fn to_json(&self) -> Result<String, QueuePoolError> {
        let snapshot = self.snapshot();
        serde_json::to_string_pretty(&snapshot).map_err(QueuePoolError::Serialize)
    }

    pub fn snapshot(&self) -> QueuePoolSnapshot {
        let mut queues = self.queues.values().cloned().collect::<Vec<_>>();
        queues.sort_by(|a, b| a.name.cmp(&b.name));

        QueuePoolSnapshot {
            queues,
            bootstrapped: self.bootstrapped,
        }
    }

    pub fn register_queue(
        &mut self,
        name: impl Into<String>,
        config: Option<QueueConfig>,
    ) -> Result<(), QueuePoolError> {
        let name = name.into();
        if self.queues.contains_key(&name) {
            return Err(QueuePoolError::DuplicateQueueName { name });
        }

        let queue = Queue::new(name.clone(), config);
        self.queues.insert(name, queue);
        Ok(())
    }

    pub fn get_queue(&self, name: &str) -> Option<&Queue> {
        self.queues.get(name)
    }

    pub fn list_queues(&self) -> Vec<&Queue> {
        let mut queues = self.queues.values().collect::<Vec<_>>();
        queues.sort_by(|a, b| a.name.cmp(&b.name));
        queues
    }

    pub fn pause_queue(&mut self, name: &str) -> Result<(), QueuePoolError> {
        let queue = self
            .queues
            .get_mut(name)
            .ok_or_else(|| QueuePoolError::QueueNotFound {
                name: name.to_owned(),
            })?;
        queue.pause();
        Ok(())
    }

    pub fn resume_queue(&mut self, name: &str) -> Result<(), QueuePoolError> {
        let queue = self
            .queues
            .get_mut(name)
            .ok_or_else(|| QueuePoolError::QueueNotFound {
                name: name.to_owned(),
            })?;
        queue.resume();
        Ok(())
    }

    pub fn mark_bootstrapped(&mut self) {
        self.bootstrapped = true;
    }

    pub fn is_bootstrapped(&self) -> bool {
        self.bootstrapped
    }
}

#[cfg(test)]
mod tests {
    use super::{QueueConfig, QueuePool, QueuePoolError, QueueState};

    #[test]
    fn queue_name_must_be_unique() {
        let mut pool = QueuePool::new();
        pool.register_queue("email", None)
            .expect("first queue register should pass");

        let err = pool
            .register_queue("email", None)
            .expect_err("duplicate queue should fail");
        assert!(matches!(err, QueuePoolError::DuplicateQueueName { .. }));
    }

    #[test]
    fn queue_defaults_allow_overrides_and_active_state() {
        let mut pool = QueuePool::new();
        pool.register_queue("critical", None)
            .expect("queue register should pass");

        let queue = pool.get_queue("critical").expect("queue should exist");
        assert_eq!(queue.config.concurrency_limit, None);
        assert!(queue.config.allow_job_overrides);
        assert_eq!(queue.state, QueueState::Active);
    }

    #[test]
    fn pause_resume_queue_is_persisted_in_json_snapshot() {
        let mut pool = QueuePool::new();
        pool.register_queue(
            "payments",
            Some(QueueConfig {
                concurrency_limit: Some(8),
                allow_job_overrides: false,
            }),
        )
        .expect("queue register should pass");

        pool.pause_queue("payments")
            .expect("pause should work for existing queue");

        let json = pool.to_json().expect("serialization should pass");
        let restored = QueuePool::from_json(&json).expect("deserialization should pass");
        let queue = restored
            .get_queue("payments")
            .expect("queue should exist after restore");

        assert_eq!(queue.state, QueueState::Paused);
        assert_eq!(queue.config.concurrency_limit, Some(8));
        assert!(!queue.config.allow_job_overrides);
    }

    #[test]
    fn can_resume_and_list_queues() {
        let mut pool = QueuePool::new();
        pool.register_queue("zeta", None)
            .expect("zeta queue should register");
        pool.register_queue("alpha", None)
            .expect("alpha queue should register");
        pool.pause_queue("zeta").expect("pause should work");
        pool.resume_queue("zeta").expect("resume should work");

        let zeta = pool.get_queue("zeta").expect("zeta queue should exist");
        assert_eq!(zeta.state, QueueState::Active);

        let names = pool
            .list_queues()
            .into_iter()
            .map(|q| q.name.clone())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["alpha".to_owned(), "zeta".to_owned()]);
    }

    #[test]
    fn reconstruct_from_stored_queues_is_headless_and_not_bootstrapped() {
        let mut queue = super::Queue::new("bulk", None);
        queue.pause();

        let pool = QueuePool::reconstruct(vec![queue]).expect("reconstruct should pass");
        assert!(!pool.is_bootstrapped());
        let restored_queue = pool.get_queue("bulk").expect("queue should be present");
        assert_eq!(restored_queue.state, QueueState::Paused);
    }

    #[test]
    fn bootstrapped_state_is_explicitly_marked() {
        let mut pool = QueuePool::new();
        assert!(!pool.is_bootstrapped());

        pool.mark_bootstrapped();
        assert!(pool.is_bootstrapped());
    }
}
