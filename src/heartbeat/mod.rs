use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{json, Value};

use crate::events::EventEmitter;

pub const HEARTBEAT_EVENT: &str = "on-heartbeat";
pub const MIN_INTERVAL_MS: u64 = 100;
pub const MAX_INTERVAL_MS: u64 = 1_000;

#[derive(Clone, Copy, Debug)]
pub struct HeartbeatConfig {
    pub interval_ms: u64,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval_ms: MAX_INTERVAL_MS,
        }
    }
}

#[derive(Debug)]
pub enum HeartbeatError {
    InvalidInterval { provided_ms: u64 },
    AlreadyRunning,
    JoinFailed,
}

impl fmt::Display for HeartbeatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInterval { provided_ms } => write!(
                f,
                "heartbeat interval must be between {MIN_INTERVAL_MS}ms and {MAX_INTERVAL_MS}ms, got {provided_ms}ms"
            ),
            Self::AlreadyRunning => write!(f, "heartbeat is already running"),
            Self::JoinFailed => write!(f, "heartbeat worker thread join failed"),
        }
    }
}

impl std::error::Error for HeartbeatError {}

pub struct Heartbeat {
    pub initiated_at: DateTime<Utc>,
    interval_ms: u64,
    emitter: Arc<EventEmitter>,
    stop_signal: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl Heartbeat {
    pub fn new(emitter: Arc<EventEmitter>, config: HeartbeatConfig) -> Result<Self, HeartbeatError> {
        Self::validate_interval(config.interval_ms)?;

        Ok(Self {
            initiated_at: Utc::now(),
            interval_ms: config.interval_ms,
            emitter,
            stop_signal: Arc::new(AtomicBool::new(false)),
            worker: None,
        })
    }

    pub fn start(&mut self) -> Result<(), HeartbeatError> {
        if self.worker.is_some() {
            return Err(HeartbeatError::AlreadyRunning);
        }

        self.stop_signal.store(false, Ordering::SeqCst);
        let stop_signal = Arc::clone(&self.stop_signal);
        let emitter = Arc::clone(&self.emitter);
        let initiated_at = self.initiated_at;
        let interval_ms = self.interval_ms;

        self.worker = Some(thread::spawn(move || {
            loop {
                if stop_signal.load(Ordering::SeqCst) {
                    break;
                }

                let emitted_at = Utc::now();
                let payload = json!({
                    "initiated_at": initiated_at.to_rfc3339_opts(SecondsFormat::Millis, true),
                    "emitted_at": emitted_at.to_rfc3339_opts(SecondsFormat::Millis, true),
                    "interval_ms": interval_ms
                });

                emitter.emit_or_exit(HEARTBEAT_EVENT, Some(payload));
                thread::sleep(Duration::from_millis(interval_ms));
            }
        }));

        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), HeartbeatError> {
        self.stop_signal.store(true, Ordering::SeqCst);

        if let Some(handle) = self.worker.take() {
            return handle.join().map_err(|_| HeartbeatError::JoinFailed);
        }

        Ok(())
    }

    pub fn initial_metadata_payload(&self) -> Value {
        json!({
            "event": HEARTBEAT_EVENT,
            "initiated_at": self.initiated_at.to_rfc3339_opts(SecondsFormat::Millis, true),
            "interval_ms": self.interval_ms
        })
    }

    fn validate_interval(interval_ms: u64) -> Result<(), HeartbeatError> {
        if (MIN_INTERVAL_MS..=MAX_INTERVAL_MS).contains(&interval_ms) {
            Ok(())
        } else {
            Err(HeartbeatError::InvalidInterval {
                provided_ms: interval_ms,
            })
        }
    }
}

impl Drop for Heartbeat {
    fn drop(&mut self) {
        self.stop_signal.store(true, Ordering::SeqCst);
        if let Some(handle) = self.worker.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    use chrono::SecondsFormat;
    use serde_json::Value;

    use crate::events::EventEmitter;

    use super::{Heartbeat, HeartbeatConfig, HeartbeatError, HEARTBEAT_EVENT, MAX_INTERVAL_MS};

    #[test]
    fn default_config_interval_is_one_second() {
        let config = HeartbeatConfig::default();
        assert_eq!(config.interval_ms, MAX_INTERVAL_MS);
    }

    #[test]
    fn rejects_intervals_outside_allowed_range() {
        let emitter = Arc::new(EventEmitter::new());

        let low = Heartbeat::new(Arc::clone(&emitter), HeartbeatConfig { interval_ms: 99 });
        let high = Heartbeat::new(Arc::clone(&emitter), HeartbeatConfig { interval_ms: 1_001 });

        assert!(matches!(
            low,
            Err(HeartbeatError::InvalidInterval { provided_ms: 99 })
        ));
        assert!(matches!(
            high,
            Err(HeartbeatError::InvalidInterval { provided_ms: 1_001 })
        ));
    }

    #[test]
    fn emits_heartbeat_with_initiated_timestamp() {
        let emitter = Arc::new(EventEmitter::new());
        let (tx, rx) = mpsc::channel::<String>();

        emitter.on(HEARTBEAT_EVENT, move |event| {
            let initiated_at = event
                .payload
                .as_ref()
                .and_then(|payload| payload.get("initiated_at"))
                .and_then(Value::as_str)
                .ok_or_else(|| "missing initiated_at in heartbeat payload".to_owned())?;

            tx.send(initiated_at.to_owned())
                .map_err(|send_error| send_error.to_string())
        });

        let mut heartbeat = Heartbeat::new(Arc::clone(&emitter), HeartbeatConfig { interval_ms: 100 })
            .expect("heartbeat should be created");
        let expected_initiated_at = heartbeat
            .initiated_at
            .to_rfc3339_opts(SecondsFormat::Millis, true);

        heartbeat.start().expect("heartbeat should start");
        let received = rx
            .recv_timeout(Duration::from_millis(350))
            .expect("heartbeat event should arrive");
        heartbeat.stop().expect("heartbeat should stop");

        assert_eq!(received, expected_initiated_at);
    }
}
