mod events;
mod heartbeat;
mod logging;

use std::sync::Arc;
use std::time::Duration;

use events::EventEmitter;
use heartbeat::{Heartbeat, HeartbeatConfig};
use logging::{LogLevel, Logger, LoggerConfig};
use serde_json::json;

fn main() {
    let logger = Logger::new(LoggerConfig::default());
    let emitter = Arc::new(EventEmitter::new());

    emitter.on("app.started", |event| {
        println!("sync event observed: {}", event.name);
        Ok(())
    });
    emitter.on_async("app.started", |event| {
        eprintln!("async event observed: {}", event.name);
        Ok(())
    });

    logger.info(Some("main"), "Starting Overhop");
    emitter.emit_or_exit("app.started", Some(json!({"component":"main"})));

    let mut heartbeat = Heartbeat::new(Arc::clone(&emitter), HeartbeatConfig::default())
        .expect("heartbeat configuration should be valid");
    logger.log(
        LogLevel::Info,
        Some("main::heartbeat"),
        "Heartbeat initialized",
        Some(heartbeat.initial_metadata_payload()),
    );
    heartbeat.start().expect("heartbeat should start");
    std::thread::sleep(Duration::from_millis(120));
    heartbeat.stop().expect("heartbeat should stop");

    println!("{}", greeting());

    logger.log(
        LogLevel::Debug,
        Some("main::greeting"),
        "Rendered greeting",
        Some(json!({"message":"Overhop!"})),
    );
}

fn greeting() -> &'static str {
    "Overhop!"
}

#[cfg(test)]
mod tests {
    use super::greeting;

    #[test]
    fn greeting_is_stable() {
        assert_eq!(greeting(), "Overhop!");
    }
}
