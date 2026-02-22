mod events;
mod logging;

use events::EventEmitter;
use logging::{LogLevel, Logger, LoggerConfig};
use serde_json::json;

fn main() {
    let logger = Logger::new(LoggerConfig::default());
    let emitter = EventEmitter::new();

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
