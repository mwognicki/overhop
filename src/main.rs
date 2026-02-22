mod config;
mod events;
mod logging;

use std::process;

use config::AppConfig;
use events::EventEmitter;
use logging::{LogLevel, Logger, LoggerConfig};
use serde_json::json;

fn main() {
    let app_config = load_config_or_exit();
    let log_level =
        LogLevel::from_config_value(&app_config.logging.level).unwrap_or_else(|| {
            eprintln!(
                "invalid logging.level '{}'. Allowed values: error, warn, info, debug, verbose",
                app_config.logging.level
            );
            process::exit(2);
        });

    let logger = Logger::new(LoggerConfig {
        min_level: log_level,
        human_friendly: app_config.logging.human_friendly,
    });
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
        Some(json!({
            "message":"Overhop!",
            "heartbeat_interval_ms": app_config.heartbeat.interval_ms
        })),
    );
}

fn load_config_or_exit() -> AppConfig {
    match AppConfig::load_from_toml_with_args("config/overhop.toml", std::env::args().skip(1)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("configuration error: {error}");
            process::exit(2);
        }
    }
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
