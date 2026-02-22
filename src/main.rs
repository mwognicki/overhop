mod config;
mod events;
mod heartbeat;
mod logging;
mod server;
mod shutdown;
mod wire;

use std::process;
use std::sync::Arc;
use std::time::Duration;

use config::AppConfig;
use events::EventEmitter;
use heartbeat::Heartbeat;
use logging::{LogLevel, Logger, LoggerConfig};
use serde_json::json;
use server::TcpServer;
use shutdown::ShutdownHooks;
use wire::codec::WireCodec;

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
    let server = TcpServer::from_app_config(&app_config).unwrap_or_else(|error| {
        eprintln!("server startup error: {error}");
        process::exit(2);
    });
    let bound_addr = server.local_addr().unwrap_or_else(|error| {
        eprintln!("server startup error: failed to read local address: {error}");
        process::exit(2);
    });
    logger.log(
        LogLevel::Info,
        Some("main::server"),
        &format!(
            "{} v{} started non-blocking TCP server",
            env!("CARGO_PKG_NAME"),
            env!("CARGO_PKG_VERSION")
        ),
        Some(json!({
            "bind_address": bound_addr.to_string(),
            "host": app_config.server.host,
            "port": app_config.server.port,
            "tls_enabled": app_config.server.tls_enabled
        })),
    );
    if !app_config.server.tls_enabled {
        logger.info(
            Some("main::server"),
            "TLS is disabled for now; TCP connections are plain and persistent full-duplex",
        );
    }
    let wire_codec = WireCodec::from_app_config(&app_config).unwrap_or_else(|error| {
        eprintln!("wire codec configuration error: {error}");
        process::exit(2);
    });
    logger.log(
        LogLevel::Info,
        Some("main::wire"),
        "Wire codec initialized",
        Some(json!({
            "max_envelope_size_bytes": wire_codec.max_envelope_size_bytes()
        })),
    );

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

    let mut heartbeat = Heartbeat::from_app_config(Arc::clone(&emitter), &app_config)
        .expect("heartbeat configuration should be valid");
    logger.log(
        LogLevel::Info,
        Some("main::heartbeat"),
        "Heartbeat initialized",
        Some(heartbeat.initial_metadata_payload()),
    );
    heartbeat.start().expect("heartbeat should start");

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

    let shutdown_hooks = ShutdownHooks::install().unwrap_or_else(|error| {
        eprintln!("failed to install shutdown hooks: {error}");
        process::exit(2);
    });
    logger.info(
        Some("main::shutdown"),
        "Shutdown hooks installed for SIGINT/SIGTERM",
    );

    while !shutdown_hooks.is_triggered() {
        if let Some(connection) = server.try_accept_persistent().unwrap_or_else(|error| {
            eprintln!("server accept error: {error}");
            process::exit(2);
        }) {
            logger.log(
                LogLevel::Info,
                Some("main::server"),
                "Accepted persistent full-duplex TCP connection",
                Some(json!({
                    "connection_id": connection.id(),
                    "peer_addr": connection.peer_addr().to_string(),
                    "tls_enabled": app_config.server.tls_enabled
                })),
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    logger.info(
        Some("main::shutdown"),
        "Shutdown signal received, starting graceful shutdown",
    );
    emitter.begin_shutdown();
    heartbeat.stop().expect("heartbeat should stop");
    server.shutdown_all_connections();

    let drained = emitter.wait_for_idle(Duration::from_secs(3));
    if drained {
        logger.info(
            Some("main::shutdown"),
            "All running listeners completed before timeout",
        );
    } else {
        logger.warn(
            Some("main::shutdown"),
            "Listener drain timeout reached; continuing shutdown",
        );
    }

    drop(server);
    let _wire_codec = wire_codec;
    logger.info(
        Some("main::shutdown"),
        "TCP server stopped and shutdown completed",
    );
}

fn load_config_or_exit() -> AppConfig {
    match AppConfig::load_with_discovery(std::env::args().skip(1)) {
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
