mod config;
mod events;
mod heartbeat;
mod logging;
mod orchestrator;
mod pools;
mod server;
mod storage;
mod shutdown;
mod wire;

use std::process;
use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};

use config::AppConfig;
use events::EventEmitter;
use heartbeat::Heartbeat;
use logging::{LogLevel, Logger, LoggerConfig};
use orchestrator::queues::persistent::PersistentQueuePool;
use pools::ConnectionWorkerPools;
use serde_json::json;
use server::TcpServer;
use storage::StorageFacade;
use shutdown::ShutdownHooks;
use wire::codec::WireCodec;
use wire::envelope::{PayloadMap, WireEnvelope};
use wire::session::{
    AnonymousProtocolAction, PROTOCOL_VIOLATION_CODE, build_protocol_error_frame,
    evaluate_anonymous_client_frame,
};

fn main() {
    ensure_posix_or_exit();
    print_startup_banner();

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
    let storage = StorageFacade::initialize(&app_config, &logger).unwrap_or_else(|error| {
        eprintln!("storage initialization error: {error}");
        process::exit(2);
    });
    let persistent_queues =
        PersistentQueuePool::bootstrap(&storage, &logger).unwrap_or_else(|error| {
            eprintln!("queue bootstrap error: {error}");
            process::exit(2);
        });
    logger.log(
        LogLevel::Info,
        Some("main::orchestrator"),
        "Queue pool bootstrapped from persistence",
        Some(json!({
            "queues_count": persistent_queues.queue_pool().snapshot().queues.len(),
            "includes_system_queue": persistent_queues.queue_pool().get_queue("_system").is_some()
        })),
    );
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
    let pools = ConnectionWorkerPools::new();

    emitter.on("app.started", |event| {
        println!("sync event observed: {}", event.name);
        Ok(())
    });
    emitter.on_async("app.started", |event| {
        eprintln!("async event observed: {}", event.name);
        Ok(())
    });

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
            let connection_id = pools.register_anonymous(Arc::clone(&connection));
            let anon_metadata = pools
                .anonymous
                .snapshot(connection_id)
                .expect("anonymous connection should be present right after registration");
            logger.log(
                LogLevel::Info,
                Some("main::server"),
                "Accepted persistent full-duplex TCP connection",
                Some(json!({
                    "connection_id": connection.id(),
                    "anonymous_connection_id": connection_id,
                    "peer_addr": connection.peer_addr().to_string(),
                    "connected_at": anon_metadata.connected_at.to_rfc3339(),
                    "helloed_at": anon_metadata.helloed_at.map(|v| v.to_rfc3339()),
                    "tls_enabled": app_config.server.tls_enabled
                })),
            );
        }
        process_anonymous_hello_handshakes(&pools, &wire_codec, &logger);
        thread::sleep(Duration::from_millis(100));
    }

    logger.info(
        Some("main::shutdown"),
        "Shutdown signal received, starting graceful shutdown",
    );
    emitter.begin_shutdown();
    heartbeat.stop().expect("heartbeat should stop");
    if let Err(error) = persistent_queues.persist_current_state(&storage) {
        eprintln!("queue persistence error during shutdown: {error}");
        process::exit(2);
    }
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

fn ensure_posix_or_exit() {
    if !cfg!(unix) {
        eprintln!("unsupported platform: Overhop is intended for POSIX systems");
        process::exit(2);
    }
}

fn print_startup_banner() {
    const RESET: &str = "\x1b[0m";
    const BANNER_COLOR: &str = "\x1b[38;5;66m";
    const DIM_GRAY: &str = "\x1b[2;90m";
    const BANNER: &str = r#"
                                         █████                         ███
                                        ░░███                         ░███
  ██████  █████ █████  ██████  ████████  ░███████    ██████  ████████ ░███
 ███░░███░░███ ░░███  ███░░███░░███░░███ ░███░░███  ███░░███░░███░░███░███
░███ ░███ ░███  ░███ ░███████  ░███ ░░░  ░███ ░███ ░███ ░███ ░███ ░███░███
░███ ░███ ░░███ ███  ░███░░░   ░███      ░███ ░███ ░███ ░███ ░███ ░███░░░
░░██████   ░░█████   ░░██████  █████     ████ █████░░██████  ░███████  ███
 ░░░░░░     ░░░░░     ░░░░░░  ░░░░░     ░░░░ ░░░░░  ░░░░░░   ░███░░░  ░░░
                                                             ░███
                                                             █████
                                                            ░░░░░         "#;
    const APP_DESCRIPTION: &str =
        "Persistent queue orchestration and worker coordination runtime over TCP.";
    const REPO_URL: &str = "https://github.com/mwognicki/overhop";
    const COPYRIGHT_NOTICE: &str = "Copyright (c) 2026 Marek Kapusta-Ognicki";
    const LIABILITY_NOTICE: &str =
        "MIT License disclaimer: software is provided \"AS IS\", without warranty or liability.";

    println!("{BANNER_COLOR}");
    println!("{BANNER}{RESET}");
    println!(
        "{} v{} | build {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        env!("OVERHOP_BUILD_DATE_UTC")
    );
    println!("{APP_DESCRIPTION}");
    println!("Repository: {REPO_URL}");
    println!("{DIM_GRAY}{COPYRIGHT_NOTICE}{RESET}");
    println!("{DIM_GRAY}{LIABILITY_NOTICE}{RESET}");
    println!();
    println!("================================================================");
    println!();
}

fn process_anonymous_hello_handshakes(
    pools: &ConnectionWorkerPools,
    wire_codec: &WireCodec,
    logger: &Logger,
) {
    let read_buffer_len = wire_codec.max_envelope_size_bytes() + wire::codec::FRAME_HEADER_SIZE_BYTES;
    let active_connections = pools.anonymous.active_connections();

    for (connection_id, connection) in active_connections {
        let mut read_buffer = vec![0_u8; read_buffer_len];
        match connection.try_read(&mut read_buffer) {
            Ok(0) => {}
            Ok(size) => {
                let frame = &read_buffer[..size];
                let helloed = pools
                    .anonymous
                    .snapshot(connection_id)
                    .map(|snapshot| snapshot.helloed_at.is_some())
                    .unwrap_or(false);

                match evaluate_anonymous_client_frame(wire_codec, frame, helloed) {
                    Ok(AnonymousProtocolAction::HelloAccepted { response_frame }) => {
                        if let Err(error) = pools.anonymous.mark_helloed_now(connection_id) {
                            logger.warn(
                                Some("main::wire"),
                                &format!("failed to mark hello timestamp: {error}"),
                            );
                            continue;
                        }

                        write_response_frame(
                            &connection,
                            &response_frame,
                            connection_id,
                            logger,
                            "HI response",
                        );
                        logger.info(
                            Some("main::wire"),
                            &format!(
                                "HELLO handshake completed for anonymous connection {}",
                                connection_id
                            ),
                        );
                    }
                    Ok(AnonymousProtocolAction::RegisterRequested { request_id }) => {
                        let worker_id = match pools
                            .promote_anonymous_to_worker(connection_id, pools::WorkerMetadata::default())
                        {
                            Ok(worker_id) => worker_id,
                            Err(error) => {
                                logger.warn(
                                    Some("main::wire"),
                                    &format!(
                                        "failed to promote anonymous connection {connection_id}: {error}"
                                    ),
                                );
                                continue;
                            }
                        };

                        let mut payload = PayloadMap::new();
                        payload.insert(
                            "wid".to_owned(),
                            rmpv::Value::String(worker_id.to_string().into()),
                        );
                        let ok_frame = match wire_codec
                            .encode_frame(&WireEnvelope::ok(request_id, Some(payload)).into_raw())
                        {
                            Ok(frame) => frame,
                            Err(error) => {
                                logger.warn(
                                    Some("main::wire"),
                                    &format!(
                                        "failed to encode REGISTER success response for worker {worker_id}: {error}"
                                    ),
                                );
                                continue;
                            }
                        };

                        write_response_frame(
                            &connection,
                            &ok_frame,
                            connection.id(),
                            logger,
                            "REGISTER OK response",
                        );
                        logger.info(
                            Some("main::wire"),
                            &format!(
                                "anonymous connection {connection_id} promoted to worker {worker_id}"
                            ),
                        );
                    }
                    Err(wire::session::SessionError::ProtocolViolation {
                        request_id,
                        code,
                        message,
                    }) => {
                        let rid = request_id.as_deref().unwrap_or("0");
                        match build_protocol_error_frame(wire_codec, rid, &code, &message) {
                            Ok(error_frame) => {
                                write_response_frame(
                                    &connection,
                                    &error_frame,
                                    connection_id,
                                    logger,
                                    "ERR response",
                                );
                            }
                            Err(build_error) => {
                                logger.warn(
                                    Some("main::wire"),
                                    &format!(
                                        "failed to build protocol ERR response for connection {connection_id}: {build_error}"
                                    ),
                                );
                            }
                        }
                        logger.warn(
                            Some("main::wire"),
                            &format!(
                                "protocol violation on anonymous connection {connection_id}: {message}; closing connection"
                            ),
                        );
                        let _ = pools.terminate_anonymous(
                            connection_id,
                            Some(format!("wire protocol violation ({PROTOCOL_VIOLATION_CODE}): {message}")),
                        );
                    }
                    Err(error) => {
                        logger.warn(
                            Some("main::wire"),
                            &format!(
                                "wire session error on anonymous connection {connection_id}: {error}; closing connection"
                            ),
                        );
                        let _ = pools.terminate_anonymous(
                            connection_id,
                            Some(format!("wire session error: {error}")),
                        );
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => {
                logger.warn(
                    Some("main::wire"),
                    &format!(
                        "read error on anonymous connection {connection_id}: {error}; closing connection"
                    ),
                );
                let _ = pools.terminate_anonymous(
                    connection_id,
                    Some(format!("socket read error: {error}")),
                );
            }
        }
    }
}

fn write_response_frame(
    connection: &Arc<server::PersistentConnection>,
    response_frame: &[u8],
    connection_id: u64,
    logger: &Logger,
    label: &str,
) {
    match connection.try_write(response_frame) {
        Ok(written) if written == response_frame.len() => {}
        Ok(written) => {
            logger.warn(
                Some("main::wire"),
                &format!(
                    "partial {label} write on connection {connection_id}: wrote {written} of {} bytes",
                    response_frame.len()
                ),
            );
        }
        Err(error) => {
            logger.warn(
                Some("main::wire"),
                &format!(
                    "failed to write {label} to connection {connection_id}: {error}"
                ),
            );
        }
    }
}
