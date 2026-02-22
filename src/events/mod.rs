use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, RwLock};
use std::thread;

use serde_json::Value;

pub type ListenerResult = Result<(), String>;

type SyncListener = Arc<dyn Fn(&Event) -> ListenerResult + Send + Sync>;
type AsyncListener = Arc<dyn Fn(Event) -> ListenerResult + Send + Sync>;

#[derive(Clone, Debug)]
pub struct Event {
    pub name: String,
    pub payload: Option<Value>,
}

impl Event {
    pub fn new(name: impl Into<String>, payload: Option<Value>) -> Self {
        Self {
            name: name.into(),
            payload,
        }
    }
}

#[derive(Debug)]
pub enum EmitError {
    ListenerFailed {
        event: String,
        listener_index: usize,
        message: String,
    },
    ListenerPanicked {
        event: String,
        listener_index: usize,
    },
}

impl fmt::Display for EmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ListenerFailed {
                event,
                listener_index,
                message,
            } => write!(
                f,
                "sync listener #{listener_index} failed for event '{event}': {message}"
            ),
            Self::ListenerPanicked {
                event,
                listener_index,
            } => write!(
                f,
                "sync listener #{listener_index} panicked for event '{event}'"
            ),
        }
    }
}

impl Error for EmitError {}

#[derive(Default)]
pub struct EventEmitter {
    sync_listeners: RwLock<HashMap<String, Vec<SyncListener>>>,
    async_listeners: RwLock<HashMap<String, Vec<AsyncListener>>>,
}

impl EventEmitter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on<F>(&self, event_name: impl Into<String>, listener: F)
    where
        F: Fn(&Event) -> ListenerResult + Send + Sync + 'static,
    {
        let event_name = event_name.into();
        let mut listeners = self
            .sync_listeners
            .write()
            .expect("sync listener map lock poisoned");
        listeners
            .entry(event_name)
            .or_default()
            .push(Arc::new(listener));
    }

    pub fn on_async<F>(&self, event_name: impl Into<String>, listener: F)
    where
        F: Fn(Event) -> ListenerResult + Send + Sync + 'static,
    {
        let event_name = event_name.into();
        let mut listeners = self
            .async_listeners
            .write()
            .expect("async listener map lock poisoned");
        listeners
            .entry(event_name)
            .or_default()
            .push(Arc::new(listener));
    }

    pub fn emit(&self, event_name: impl Into<String>, payload: Option<Value>) -> Result<(), EmitError> {
        let event = Event::new(event_name, payload);
        self.run_sync(&event)?;
        self.dispatch_async(event);
        Ok(())
    }

    pub fn emit_or_exit(&self, event_name: impl Into<String>, payload: Option<Value>) {
        if let Err(error) = self.emit(event_name, payload) {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }

    fn run_sync(&self, event: &Event) -> Result<(), EmitError> {
        let listeners = self
            .sync_listeners
            .read()
            .expect("sync listener map lock poisoned");

        let Some(handlers) = listeners.get(&event.name) else {
            return Ok(());
        };

        for (idx, handler) in handlers.iter().enumerate() {
            let result = catch_unwind(AssertUnwindSafe(|| handler(event)));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(message)) => {
                    return Err(EmitError::ListenerFailed {
                        event: event.name.clone(),
                        listener_index: idx,
                        message,
                    });
                }
                Err(_) => {
                    return Err(EmitError::ListenerPanicked {
                        event: event.name.clone(),
                        listener_index: idx,
                    });
                }
            }
        }

        Ok(())
    }

    fn dispatch_async(&self, event: Event) {
        let listeners = self
            .async_listeners
            .read()
            .expect("async listener map lock poisoned");

        let Some(handlers) = listeners.get(&event.name) else {
            return;
        };

        for (idx, handler) in handlers.iter().enumerate() {
            let listener = Arc::clone(handler);
            let event_for_listener = event.clone();
            let event_name = event.name.clone();
            thread::spawn(move || {
                let result = catch_unwind(AssertUnwindSafe(|| listener(event_for_listener)));
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(message)) => {
                        eprintln!(
                            "async listener #{idx} failed for event '{}': {message}",
                            event_name
                        );
                    }
                    Err(_) => {
                        eprintln!("async listener #{idx} panicked for event '{}'", event_name);
                    }
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::json;

    use super::{EmitError, EventEmitter};

    #[test]
    fn sync_listener_receives_emitted_payload() {
        let emitter = EventEmitter::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = Arc::clone(&calls);

        emitter.on("worker.registered", move |event| {
            if event.payload == Some(json!({"worker":"alpha"})) {
                calls_clone.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        });

        let result = emitter.emit("worker.registered", Some(json!({"worker":"alpha"})));
        assert!(result.is_ok());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn sync_listener_error_bubbles_up() {
        let emitter = EventEmitter::new();
        emitter.on("job.failed", |_event| Err("sync listener failure".to_owned()));

        let result = emitter.emit("job.failed", None);
        assert!(matches!(
            result,
            Err(EmitError::ListenerFailed {
                listener_index: 0,
                ..
            })
        ));
    }

    #[test]
    fn sync_listener_panic_is_recovered_and_returned() {
        let emitter = EventEmitter::new();
        emitter.on("job.failed", |_event| panic!("boom"));

        let result = emitter.emit("job.failed", None);
        assert!(matches!(
            result,
            Err(EmitError::ListenerPanicked {
                listener_index: 0,
                ..
            })
        ));
    }

    #[test]
    fn async_listener_executes_in_isolation() {
        let emitter = EventEmitter::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = Arc::clone(&calls);

        emitter.on_async("queue.updated", move |_event| {
            calls_clone.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });

        let result = emitter.emit("queue.updated", None);
        assert!(result.is_ok());

        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn async_listener_failure_is_isolated() {
        let emitter = EventEmitter::new();
        emitter.on_async("queue.updated", |_event| Err("non-fatal async error".to_owned()));

        let result = emitter.emit("queue.updated", None);
        assert!(result.is_ok());
    }
}
