pub mod persistent;

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
    #[serde(default = "new_queue_id")]
    pub id: Uuid,
    pub name: String,
    pub config: QueueConfig,
    pub state: QueueState,
}

impl Queue {
    pub fn new(name: impl Into<String>, config: Option<QueueConfig>) -> Self {
        Self {
            id: new_queue_id(),
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
    InvalidQueueName { name: String },
    QueueNotFound { name: String },
    SystemQueueRemovalForbidden { name: String },
    Serialize(serde_json::Error),
    Deserialize(serde_json::Error),
}

impl fmt::Display for QueuePoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateQueueName { name } => {
                write!(f, "queue '{name}' is already registered")
            }
            Self::InvalidQueueName { name } => write!(
                f,
                "queue '{name}' is invalid (allowed: [A-Za-z0-9_-], first char must be alphanumeric)"
            ),
            Self::QueueNotFound { name } => write!(f, "queue '{name}' not found"),
            Self::SystemQueueRemovalForbidden { name } => {
                write!(f, "queue '{name}' is a system queue and cannot be removed")
            }
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
            if pool.queues.contains_key(&queue.name) {
                return Err(QueuePoolError::DuplicateQueueName {
                    name: queue.name.clone(),
                });
            }
            pool.queues.insert(queue.name.clone(), queue);
        }
        Ok(pool)
    }

    pub fn from_json(json: &str) -> Result<Self, QueuePoolError> {
        let snapshot: QueuePoolSnapshot =
            serde_json::from_str(json).map_err(QueuePoolError::Deserialize)?;
        let mut pool = Self::new();
        for queue in snapshot.queues {
            if pool.queues.contains_key(&queue.name) {
                return Err(QueuePoolError::DuplicateQueueName {
                    name: queue.name.clone(),
                });
            }
            pool.queues.insert(queue.name.clone(), queue);
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
    ) -> Result<Uuid, QueuePoolError> {
        let name = name.into();
        validate_queue_name(&name)?;
        if self.queues.contains_key(&name) {
            return Err(QueuePoolError::DuplicateQueueName { name });
        }

        let queue = Queue::new(name.clone(), config);
        let queue_id = queue.id;
        self.queues.insert(name, queue);
        Ok(queue_id)
    }

    pub fn get_queue(&self, name: &str) -> Option<&Queue> {
        self.queues.get(name)
    }

    pub fn remove_queue(&mut self, name: &str) -> Result<(), QueuePoolError> {
        if name.starts_with('_') {
            return Err(QueuePoolError::SystemQueueRemovalForbidden {
                name: name.to_owned(),
            });
        }

        self.queues
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| QueuePoolError::QueueNotFound {
                name: name.to_owned(),
            })
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

fn new_queue_id() -> Uuid {
    Uuid::new_v4()
}

fn validate_queue_name(name: &str) -> Result<(), QueuePoolError> {
    let mut chars = name.chars();
    let Some(first_char) = chars.next() else {
        return Err(QueuePoolError::InvalidQueueName {
            name: name.to_owned(),
        });
    };
    if !first_char.is_ascii_alphanumeric() {
        return Err(QueuePoolError::InvalidQueueName {
            name: name.to_owned(),
        });
    }
    if chars.any(|ch| !ch.is_ascii_alphanumeric() && ch != '-' && ch != '_') {
        return Err(QueuePoolError::InvalidQueueName {
            name: name.to_owned(),
        });
    }
    Ok(())
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

    #[test]
    fn remove_queue_rejects_system_queue_and_removes_regular_queue() {
        let mut pool = QueuePool::reconstruct(vec![
            super::Queue::new("_system", None),
            super::Queue::new("critical", None),
        ])
        .expect("seeded queue reconstruct should pass");

        let err = pool
            .remove_queue("_system")
            .expect_err("system queue removal should fail");
        assert!(matches!(
            err,
            QueuePoolError::SystemQueueRemovalForbidden { .. }
        ));

        pool.remove_queue("critical")
            .expect("regular queue removal should pass");
        assert!(pool.get_queue("critical").is_none());
    }

    #[test]
    fn queue_name_must_match_allowed_pattern() {
        let mut pool = QueuePool::new();
        pool.register_queue("queue_01-prod", None)
            .expect("valid name should pass");

        let invalid_names = ["", "-bad", "_bad", "bad space", "bad.dot", "ąlpha"];
        for invalid in invalid_names {
            let err = pool
                .register_queue(invalid, None)
                .expect_err("invalid queue name should fail");
            assert!(matches!(err, QueuePoolError::InvalidQueueName { .. }));
        }
    }
}
