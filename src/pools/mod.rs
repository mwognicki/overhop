use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::server::PersistentConnection;

#[derive(Debug)]
pub enum PoolError {
    AnonymousConnectionNotFound { connection_id: u64 },
    WorkerNotFound { worker_id: Uuid },
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AnonymousConnectionNotFound { connection_id } => {
                write!(f, "anonymous connection {connection_id} not found")
            }
            Self::WorkerNotFound { worker_id } => write!(f, "worker {worker_id} not found"),
        }
    }
}

impl std::error::Error for PoolError {}

#[derive(Clone)]
pub struct AnonymousConnectionRecord {
    pub connection_id: u64,
    pub connection: Arc<PersistentConnection>,
    pub connected_at: DateTime<Utc>,
    pub helloed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug)]
pub struct AnonymousConnectionSnapshot {
    pub connection_id: u64,
    pub connected_at: DateTime<Utc>,
    pub helloed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default)]
pub struct WorkerMetadata {
    pub queue_capabilities: Option<Value>,
}

#[derive(Clone)]
pub struct WorkerRecord {
    pub worker_id: Uuid,
    pub connection: Arc<PersistentConnection>,
    pub promoted_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub metadata: WorkerMetadata,
}

#[derive(Clone, Debug)]
pub struct WorkerSnapshot {
    pub worker_id: Uuid,
    pub promoted_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub metadata: WorkerMetadata,
}

#[derive(Default)]
pub struct AnonymousConnectionsPool {
    records: Mutex<HashMap<u64, AnonymousConnectionRecord>>,
}

impl AnonymousConnectionsPool {
    pub fn register(&self, connection: Arc<PersistentConnection>) -> u64 {
        let connection_id = connection.id();
        let record = AnonymousConnectionRecord {
            connection_id,
            connection,
            connected_at: Utc::now(),
            helloed_at: None,
        };

        self.records
            .lock()
            .expect("anonymous pool lock poisoned")
            .insert(connection_id, record);

        connection_id
    }

    pub fn mark_helloed_now(&self, connection_id: u64) -> Result<(), PoolError> {
        let mut records = self.records.lock().expect("anonymous pool lock poisoned");
        let record = records
            .get_mut(&connection_id)
            .ok_or(PoolError::AnonymousConnectionNotFound { connection_id })?;
        record.helloed_at = Some(Utc::now());
        Ok(())
    }

    pub fn snapshot(&self, connection_id: u64) -> Option<AnonymousConnectionSnapshot> {
        self.records
            .lock()
            .expect("anonymous pool lock poisoned")
            .get(&connection_id)
            .map(|record| AnonymousConnectionSnapshot {
                connection_id: record.connection_id,
                connected_at: record.connected_at,
                helloed_at: record.helloed_at,
            })
    }

    fn take_for_promotion(&self, connection_id: u64) -> Result<AnonymousConnectionRecord, PoolError> {
        self.records
            .lock()
            .expect("anonymous pool lock poisoned")
            .remove(&connection_id)
            .ok_or(PoolError::AnonymousConnectionNotFound { connection_id })
    }

    pub fn terminate(&self, connection_id: u64, reason: Option<String>) -> Result<(), PoolError> {
        let record = self
            .records
            .lock()
            .expect("anonymous pool lock poisoned")
            .remove(&connection_id)
            .ok_or(PoolError::AnonymousConnectionNotFound { connection_id })?;

        let _termination_reason = reason;
        let _ = record.connection.shutdown();
        Ok(())
    }

    pub fn count(&self) -> usize {
        self.records
            .lock()
            .expect("anonymous pool lock poisoned")
            .len()
    }
}

#[derive(Default)]
pub struct WorkersPool {
    records: Mutex<HashMap<Uuid, WorkerRecord>>,
}

impl WorkersPool {
    fn insert_promoted(
        &self,
        connection: Arc<PersistentConnection>,
        metadata: WorkerMetadata,
    ) -> Uuid {
        let promoted_at = Utc::now();
        let worker_id = Uuid::new_v4();
        let record = WorkerRecord {
            worker_id,
            connection,
            promoted_at,
            last_seen_at: promoted_at,
            metadata,
        };

        self.records
            .lock()
            .expect("workers pool lock poisoned")
            .insert(worker_id, record);

        worker_id
    }

    pub fn touch_now(&self, worker_id: Uuid) -> Result<(), PoolError> {
        let mut records = self.records.lock().expect("workers pool lock poisoned");
        let record = records
            .get_mut(&worker_id)
            .ok_or(PoolError::WorkerNotFound { worker_id })?;

        record.last_seen_at = Utc::now();
        Ok(())
    }

    pub fn snapshot(&self, worker_id: Uuid) -> Option<WorkerSnapshot> {
        self.records
            .lock()
            .expect("workers pool lock poisoned")
            .get(&worker_id)
            .map(|record| WorkerSnapshot {
                worker_id: record.worker_id,
                promoted_at: record.promoted_at,
                last_seen_at: record.last_seen_at,
                metadata: record.metadata.clone(),
            })
    }

    pub fn terminate(&self, worker_id: Uuid, reason: Option<String>) -> Result<(), PoolError> {
        let record = self
            .records
            .lock()
            .expect("workers pool lock poisoned")
            .remove(&worker_id)
            .ok_or(PoolError::WorkerNotFound { worker_id })?;

        let _termination_reason = reason;
        let _ = record.connection.shutdown();
        Ok(())
    }

    pub fn count(&self) -> usize {
        self.records
            .lock()
            .expect("workers pool lock poisoned")
            .len()
    }
}

#[derive(Default)]
pub struct ConnectionWorkerPools {
    pub anonymous: AnonymousConnectionsPool,
    pub workers: WorkersPool,
}

impl ConnectionWorkerPools {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_anonymous(&self, connection: Arc<PersistentConnection>) -> u64 {
        self.anonymous.register(connection)
    }

    pub fn promote_anonymous_to_worker(
        &self,
        connection_id: u64,
        metadata: WorkerMetadata,
    ) -> Result<Uuid, PoolError> {
        let record = self.anonymous.take_for_promotion(connection_id)?;
        Ok(self.workers.insert_promoted(record.connection, metadata))
    }

    pub fn terminate_anonymous(
        &self,
        connection_id: u64,
        reason: Option<String>,
    ) -> Result<(), PoolError> {
        self.anonymous.terminate(connection_id, reason)
    }

    pub fn terminate_worker(
        &self,
        worker_id: Uuid,
        reason: Option<String>,
    ) -> Result<(), PoolError> {
        self.workers.terminate(worker_id, reason)
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::server::{ServerConfig, TcpServer};

    use super::{ConnectionWorkerPools, PoolError, WorkerMetadata};

    fn accepted_connection() -> Arc<crate::server::PersistentConnection> {
        let server = TcpServer::bind(&ServerConfig {
            host: "127.0.0.1".to_owned(),
            port: 0,
            tls_enabled: false,
        })
        .expect("server bind should work");
        let addr = server.local_addr().expect("local addr should be available");
        let _client = TcpStream::connect(addr).expect("client connect should work");

        for _ in 0..50 {
            if let Some(connection) = server
                .try_accept_persistent()
                .expect("accept should not fail")
            {
                return connection;
            }
            thread::sleep(Duration::from_millis(10));
        }

        panic!("failed to accept test connection");
    }

    #[test]
    fn registers_anonymous_and_tracks_timestamps() {
        let pools = ConnectionWorkerPools::new();
        let conn = accepted_connection();

        let connection_id = pools.register_anonymous(conn);
        assert_eq!(pools.anonymous.count(), 1);
        let snapshot = pools
            .anonymous
            .snapshot(connection_id)
            .expect("anonymous connection should exist");

        assert_eq!(snapshot.connection_id, connection_id);
        assert!(snapshot.helloed_at.is_none());
    }

    #[test]
    fn can_mark_anonymous_connection_helloed() {
        let pools = ConnectionWorkerPools::new();
        let connection_id = pools.register_anonymous(accepted_connection());

        pools
            .anonymous
            .mark_helloed_now(connection_id)
            .expect("mark hello should work");

        let snapshot = pools
            .anonymous
            .snapshot(connection_id)
            .expect("anonymous connection should exist");
        assert!(snapshot.helloed_at.is_some());
    }

    #[test]
    fn promotion_moves_connection_to_workers_pool() {
        let pools = ConnectionWorkerPools::new();
        let connection_id = pools.register_anonymous(accepted_connection());

        let worker_id = pools
            .promote_anonymous_to_worker(connection_id, WorkerMetadata::default())
            .expect("promotion should work");

        assert_eq!(pools.anonymous.count(), 0);
        assert_eq!(pools.workers.count(), 1);
        assert!(pools.anonymous.snapshot(connection_id).is_none());
        let worker = pools
            .workers
            .snapshot(worker_id)
            .expect("worker should exist");
        assert_eq!(worker.worker_id, worker_id);
        assert_eq!(worker.promoted_at, worker.last_seen_at);
        assert_eq!(worker.metadata.queue_capabilities, None);
    }

    #[test]
    fn touch_updates_worker_last_seen() {
        let pools = ConnectionWorkerPools::new();
        let connection_id = pools.register_anonymous(accepted_connection());
        let worker_id = pools
            .promote_anonymous_to_worker(connection_id, WorkerMetadata::default())
            .expect("promotion should work");
        let before = pools
            .workers
            .snapshot(worker_id)
            .expect("worker should exist")
            .last_seen_at;

        thread::sleep(Duration::from_millis(5));
        pools
            .workers
            .touch_now(worker_id)
            .expect("touch should work");

        let after = pools
            .workers
            .snapshot(worker_id)
            .expect("worker should exist")
            .last_seen_at;
        assert!(after >= before);
    }

    #[test]
    fn terminate_functions_remove_entries() {
        let pools = ConnectionWorkerPools::new();

        let anon_id = pools.register_anonymous(accepted_connection());
        pools
            .terminate_anonymous(anon_id, Some("test".to_owned()))
            .expect("anon termination should work");
        assert_eq!(pools.anonymous.count(), 0);
        assert!(matches!(
            pools.terminate_anonymous(anon_id, None),
            Err(PoolError::AnonymousConnectionNotFound { .. })
        ));

        let promoted_conn = pools.register_anonymous(accepted_connection());
        let worker_id = pools
            .promote_anonymous_to_worker(promoted_conn, WorkerMetadata::default())
            .expect("promotion should work");
        pools
            .terminate_worker(worker_id, Some("cleanup".to_owned()))
            .expect("worker termination should work");
        assert_eq!(pools.workers.count(), 0);
        assert!(matches!(
            pools.terminate_worker(worker_id, None),
            Err(PoolError::WorkerNotFound { .. })
        ));
    }
}
