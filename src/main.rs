mod config;
mod diagnostics;
mod events;
mod heartbeat;
mod logging;
mod orchestrator;
mod pools;
mod self_debug;
mod server;
mod storage;
mod shutdown;
mod utils;
mod wire;

use std::collections::HashSet;
use std::process;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{io, thread};
use std::{path::PathBuf, sync::mpsc::TryRecvError};

use chrono::{DateTime, Utc};
use config::AppConfig;
use diagnostics::build_status_payload;
use events::EventEmitter;
use heartbeat::{HEARTBEAT_EVENT, Heartbeat};
use logging::{LogLevel, Logger, LoggerConfig};
use orchestrator::jobs::{Job, JobStatus, JobsPool, JobsPoolError, NewJobOptions};
use orchestrator::queues::{Queue, QueueState};
use orchestrator::queues::persistent::{PersistentQueuePool, PersistentQueuePoolError};
use pools::{ConnectionWorkerPools, PoolError};
use serde_json::json;
use server::TcpServer;
use storage::StorageFacade;
use shutdown::ShutdownHooks;
use utils::timing::measure_execution;
use wire::codec::WireCodec;
use wire::envelope::{PayloadMap, WireEnvelope};
use wire::session::{
    AnonymousProtocolAction, CONNECTION_TIMEOUT_CODE, REGISTER_TIMEOUT_CODE,
    WorkerProtocolAction, build_ident_frame, build_pong_frame, build_protocol_error_frame,
    evaluate_anonymous_client_frame, evaluate_worker_client_frame,
};

const JOB_PERSIST_REQUESTED_EVENT: &str = "jobs.persist.requested";

fn main() {
    ensure_posix_or_exit();
    print_startup_banner();
    let app_started_at = Utc::now();

    let runtime_args = std::env::args().skip(1).collect::<Vec<_>>();
    let (runtime_flags, config_args) = self_debug::extract_runtime_flags(runtime_args);
    let mut app_config = load_config_or_exit(config_args);
    if runtime_flags.enabled {
        app_config.storage.path = self_debug::resolve_storage_path(&app_config.storage);
    }
    let log_level = if runtime_flags.enabled {
        LogLevel::Verbose
    } else {
        LogLevel::from_config_value(&app_config.logging.level).unwrap_or_else(|| {
            eprintln!(
                "invalid logging.level '{}'. Allowed values: error, warn, info, debug, verbose",
                app_config.logging.level
            );
            process::exit(2);
        })
    };

    let logger = Arc::new(Logger::new(LoggerConfig {
        min_level: log_level,
        human_friendly: app_config.logging.human_friendly,
    }));
    let storage = Arc::new(measure_execution(
        "storage.initialize",
        Some("main::startup"),
        logger.as_ref(),
        || StorageFacade::initialize(&app_config, logger.as_ref()),
    )
    .unwrap_or_else(|error| {
        eprintln!("storage initialization error: {error}");
        process::exit(2);
    }));
    let self_debug_artifacts_path: Option<PathBuf> = if runtime_flags.enabled {
        Some(storage.data_path().to_path_buf())
    } else {
        None
    };
    let mut persistent_queues =
        PersistentQueuePool::bootstrap(storage.as_ref(), logger.as_ref()).unwrap_or_else(|error| {
            eprintln!("queue bootstrap error: {error}");
            process::exit(2);
        });
    let jobs_pool = Arc::new(Mutex::new(JobsPool::new()));
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
    let wire_codec = Arc::new(WireCodec::from_app_config(&app_config).unwrap_or_else(|error| {
        eprintln!("wire codec configuration error: {error}");
        process::exit(2);
    }));
    logger.log(
        LogLevel::Info,
        Some("main::wire"),
        "Wire codec initialized",
        Some(json!({
            "max_envelope_size_bytes": wire_codec.max_envelope_size_bytes()
        })),
    );

    let emitter = Arc::new(EventEmitter::new());
    let pools = Arc::new(ConnectionWorkerPools::new());
    let timeout_policy = AnonymousTimeoutPolicy {
        unhelloed_max_lifetime_seconds: app_config
            .wire
            .session
            .unhelloed_max_lifetime_seconds,
        helloed_unregistered_max_lifetime_seconds: app_config
            .wire
            .session
            .helloed_unregistered_max_lifetime_seconds,
        ident_register_timeout_seconds: app_config.wire.session.ident_register_timeout_seconds,
    };

    emitter.on("app.started", |event| {
        println!("sync event observed: {}", event.name);
        Ok(())
    });
    emitter.on_async("app.started", |event| {
        eprintln!("async event observed: {}", event.name);
        Ok(())
    });
    let storage_for_job_persist = Arc::clone(&storage);
    let jobs_pool_for_persist = Arc::clone(&jobs_pool);
    let logger_for_job_persist = Arc::clone(&logger);
    emitter.on(JOB_PERSIST_REQUESTED_EVENT, move |event| {
        let Some(payload) = event.payload.as_ref() else {
            return Err("job persist event payload is missing".to_owned());
        };

        let Some(job_uuid_raw) = payload.get("job_uuid").and_then(serde_json::Value::as_str) else {
            return Err("job persist event missing string 'job_uuid'".to_owned());
        };
        let job_uuid = uuid::Uuid::parse_str(job_uuid_raw)
            .map_err(|_| "job persist event has invalid 'job_uuid'".to_owned())?;
        let Some(record) = payload.get("record") else {
            return Err("job persist event missing 'record'".to_owned());
        };
        let Some(execution_start_ms) = payload
            .get("execution_start_ms")
            .and_then(serde_json::Value::as_i64)
        else {
            return Err("job persist event missing i64 'execution_start_ms'".to_owned());
        };
        let Some(queue_name) = payload.get("queue_name").and_then(serde_json::Value::as_str) else {
            return Err("job persist event missing string 'queue_name'".to_owned());
        };
        let Some(status) = payload.get("status").and_then(serde_json::Value::as_str) else {
            return Err("job persist event missing string 'status'".to_owned());
        };

        storage_for_job_persist
            .upsert_job_record(job_uuid, record, execution_start_ms, queue_name, status)
            .map_err(|error| error.to_string())?;

        let mut guard = jobs_pool_for_persist
            .lock()
            .map_err(|_| "jobs pool lock poisoned while removing persisted job".to_owned())?;
        let _ = guard.remove_job(job_uuid);

        logger_for_job_persist.debug(
            Some("main::jobs"),
            &format!("Persisted staged job '{job_uuid}' and removed it from transient pool"),
        );
        Ok(())
    });
    let pools_for_maintenance = Arc::clone(&pools);
    let wire_codec_for_maintenance = Arc::clone(&wire_codec);
    let logger_for_maintenance = Arc::clone(&logger);
    let storage_for_status_progression = Arc::clone(&storage);
    let logger_for_status_progression = Arc::clone(&logger);
    let maintenance_interval_seconds = timeout_policy.maintenance_interval_seconds();
    let heartbeat_maintenance_last_run = Arc::new(Mutex::new(None::<DateTime<Utc>>));
    let heartbeat_maintenance_last_run_for_listener = Arc::clone(&heartbeat_maintenance_last_run);
    emitter.on(HEARTBEAT_EVENT, move |_| {
        let now = Utc::now();
        let mut guard = heartbeat_maintenance_last_run_for_listener
            .lock()
            .map_err(|_| "heartbeat maintenance lock poisoned".to_owned())?;
        if let Some(last_run) = *guard {
            if (now - last_run).num_seconds() < maintenance_interval_seconds as i64 {
                return Ok(());
            }
        }
        *guard = Some(now);
        purge_stale_anonymous_connections(
            pools_for_maintenance.as_ref(),
            wire_codec_for_maintenance.as_ref(),
            logger_for_maintenance.as_ref(),
            timeout_policy,
        );
        Ok(())
    });
    emitter.on(HEARTBEAT_EVENT, move |_| {
        advance_persisted_jobs_statuses(
            storage_for_status_progression.as_ref(),
            logger_for_status_progression.as_ref(),
        )
        .map_err(|error| format!("jobs status heartbeat progression failed: {error}"))
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

    let self_debug_runner = if runtime_flags.enabled {
        Some(self_debug::spawn_runner(bound_addr, (*wire_codec).clone()))
    } else {
        None
    };
    let mut self_debug_result: Option<Result<(), self_debug::SelfDebugError>> = None;

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
                    "ident_reply_deadline_at": anon_metadata.ident_reply_deadline_at.map(|v| v.to_rfc3339()),
                    "tls_enabled": app_config.server.tls_enabled
                })),
            );
        }
        process_anonymous_client_messages(
            pools.as_ref(),
            wire_codec.as_ref(),
            logger.as_ref(),
        );
        process_worker_client_messages(
            pools.as_ref(),
            emitter.as_ref(),
            wire_codec.as_ref(),
            logger.as_ref(),
            storage.as_ref(),
            &mut persistent_queues,
            jobs_pool.as_ref(),
            app_started_at,
        );

        if let Some(rx) = &self_debug_runner {
            match rx.try_recv() {
                Ok(result) => {
                    self_debug_result = Some(result);
                    break;
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    self_debug_result = Some(Err(self_debug::SelfDebugError::Io(
                        io::Error::other("self-debug runner disconnected"),
                    )));
                    break;
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }

    logger.info(
        Some("main::shutdown"),
        "Shutdown signal received, starting graceful shutdown",
    );
    emitter.begin_shutdown();
    heartbeat.stop().expect("heartbeat should stop");
    if let Err(error) = persistent_queues.persist_current_state(storage.as_ref()) {
        eprintln!("queue persistence error during shutdown: {error}");
        process::exit(2);
    }
    server.shutdown_all_connections();
    drain_transient_jobs_pool_before_exit(storage.as_ref(), jobs_pool.as_ref(), logger.as_ref());
    thread::sleep(Duration::from_millis(100));

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
    drop(persistent_queues);
    drop(storage);
    let _wire_codec = wire_codec;

    let mut self_debug_cleanup_error: Option<self_debug::SelfDebugError> = None;
    if !runtime_flags.keep_artifacts {
        if let Some(path) = self_debug_artifacts_path {
            if let Err(error) = self_debug::cleanup_artifacts(path.as_path()) {
                self_debug_cleanup_error = Some(error);
            }
        }
    }

    if let Some(Err(error)) = self_debug_result {
        if let Some(cleanup_error) = self_debug_cleanup_error {
            eprintln!("self-debug cleanup error (after run failure): {cleanup_error}");
        }
        eprintln!("self-debug run failed: {error}");
        process::exit(2);
    }
    if let Some(cleanup_error) = self_debug_cleanup_error {
        eprintln!("self-debug cleanup error: {cleanup_error}");
        process::exit(2);
    }

    logger.info(
        Some("main::shutdown"),
        "TCP server stopped and shutdown completed",
    );
}

fn load_config_or_exit(args: Vec<String>) -> AppConfig {
    match AppConfig::load_with_discovery(args) {
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

#[derive(Clone, Copy)]
struct AnonymousTimeoutPolicy {
    unhelloed_max_lifetime_seconds: u64,
    helloed_unregistered_max_lifetime_seconds: u64,
    ident_register_timeout_seconds: u64,
}

impl AnonymousTimeoutPolicy {
    fn maintenance_interval_seconds(self) -> u64 {
        self.unhelloed_max_lifetime_seconds
            .min(self.helloed_unregistered_max_lifetime_seconds)
            .min(self.ident_register_timeout_seconds)
    }
}

fn process_anonymous_client_messages(
    pools: &ConnectionWorkerPools,
    wire_codec: &WireCodec,
    logger: &Logger,
) {
    let read_buffer_len = wire_codec.max_envelope_size_bytes() + wire::codec::FRAME_HEADER_SIZE_BYTES;
    let active_connections = pools.anonymous.maintenance_snapshot();

    for connection_metadata in active_connections {
        let connection_id = connection_metadata.connection_id;
        let connection = connection_metadata.connection;
        let helloed = connection_metadata.helloed_at.is_some();
        let ident_reply_deadline_at = connection_metadata.ident_reply_deadline_at;
        let mut read_buffer = vec![0_u8; read_buffer_len];
        match connection.try_read(&mut read_buffer) {
            Ok(0) => {}
            Ok(size) => {
                let frame = &read_buffer[..size];

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
                        if let Some(deadline) = ident_reply_deadline_at {
                            if Utc::now() > deadline {
                                send_protocol_error_and_close(
                                    pools,
                                    wire_codec,
                                    logger,
                                    connection_id,
                                    &connection,
                                    &request_id,
                                    REGISTER_TIMEOUT_CODE,
                                    "REGISTER timeout after IDENT challenge",
                                );
                                continue;
                            }
                        }

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
                            connection_id,
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
                        send_protocol_error_and_close(
                            pools,
                            wire_codec,
                            logger,
                            connection_id,
                            &connection,
                            rid,
                            &code,
                            &message,
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
) -> bool {
    match connection.try_write(response_frame) {
        Ok(written) if written == response_frame.len() => true,
        Ok(written) => {
            logger.warn(
                Some("main::wire"),
                &format!(
                    "partial {label} write on connection {connection_id}: wrote {written} of {} bytes",
                    response_frame.len()
                ),
            );
            false
        }
        Err(error) => {
            logger.warn(
                Some("main::wire"),
                &format!(
                    "failed to write {label} to connection {connection_id}: {error}"
                ),
            );
            false
        }
    }
}

fn send_protocol_error_and_close(
    pools: &ConnectionWorkerPools,
    wire_codec: &WireCodec,
    logger: &Logger,
    connection_id: u64,
    connection: &Arc<server::PersistentConnection>,
    request_id: &str,
    code: &str,
    message: &str,
) {
    match build_protocol_error_frame(wire_codec, request_id, code, message) {
        Ok(error_frame) => {
            write_response_frame(connection, &error_frame, connection_id, logger, "ERR response");
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
            "protocol error on anonymous connection {connection_id} ({code}): {message}; closing connection"
        ),
    );
    let _ = pools.terminate_anonymous(
        connection_id,
        Some(format!("wire protocol error ({code}): {message}")),
    );
}

fn purge_stale_anonymous_connections(
    pools: &ConnectionWorkerPools,
    wire_codec: &WireCodec,
    logger: &Logger,
    timeout_policy: AnonymousTimeoutPolicy,
) {
    let now = Utc::now();
    for metadata in pools.anonymous.maintenance_snapshot() {
        if metadata.helloed_at.is_none() {
            let elapsed = (now - metadata.connected_at).num_seconds();
            if elapsed >= timeout_policy.unhelloed_max_lifetime_seconds as i64 {
                logger.info(
                    Some("main::wire"),
                    &format!(
                        "terminating unhelloed anonymous connection {} after {} seconds",
                        metadata.connection_id, elapsed
                    ),
                );
                let _ = pools.terminate_anonymous(
                    metadata.connection_id,
                    Some("unhelloed connection timeout".to_owned()),
                );
            }
            continue;
        }

        let helloed_at = metadata.helloed_at.expect("checked above");
        let helloed_elapsed = (now - helloed_at).num_seconds();
        if helloed_elapsed < timeout_policy.helloed_unregistered_max_lifetime_seconds as i64 {
            continue;
        }

        if let Some(deadline) = metadata.ident_reply_deadline_at {
            if now >= deadline {
                send_protocol_error_and_close(
                    pools,
                    wire_codec,
                    logger,
                    metadata.connection_id,
                    &metadata.connection,
                    "0",
                    CONNECTION_TIMEOUT_CODE,
                    "IDENT timeout expired without REGISTER",
                );
            }
            continue;
        }

        let reply_deadline = now
            + chrono::Duration::seconds(timeout_policy.ident_register_timeout_seconds as i64);
        let ident_frame = match build_ident_frame(
            wire_codec,
            timeout_policy.ident_register_timeout_seconds,
            &reply_deadline.to_rfc3339(),
        ) {
            Ok(frame) => frame,
            Err(error) => {
                logger.warn(
                    Some("main::wire"),
                    &format!(
                        "failed to build IDENT frame for connection {}: {}",
                        metadata.connection_id, error
                    ),
                );
                continue;
            }
        };

        if write_response_frame(
            &metadata.connection,
            &ident_frame,
            metadata.connection_id,
            logger,
            "IDENT response",
        ) {
            if let Err(error) = pools
                .anonymous
                .mark_ident_reply_deadline(metadata.connection_id, reply_deadline)
            {
                logger.warn(
                    Some("main::wire"),
                    &format!(
                        "failed to mark IDENT reply deadline for connection {}: {}",
                        metadata.connection_id, error
                    ),
                );
            } else {
                logger.info(
                    Some("main::wire"),
                    &format!(
                        "IDENT challenge sent to anonymous connection {} with {}s timeout",
                        metadata.connection_id, timeout_policy.ident_register_timeout_seconds
                    ),
                );
            }
        }
    }
}

fn process_worker_client_messages(
    pools: &ConnectionWorkerPools,
    emitter: &EventEmitter,
    wire_codec: &WireCodec,
    logger: &Logger,
    storage: &StorageFacade,
    persistent_queues: &mut PersistentQueuePool,
    jobs_pool: &Mutex<JobsPool>,
    app_started_at: DateTime<Utc>,
) {
    let read_buffer_len = wire_codec.max_envelope_size_bytes() + wire::codec::FRAME_HEADER_SIZE_BYTES;
    let active_workers = pools.workers.active_connections();

    for (worker_id, connection) in active_workers {
        let mut read_buffer = vec![0_u8; read_buffer_len];
        match connection.try_read(&mut read_buffer) {
            Ok(0) => {}
            Ok(size) => {
                let frame = &read_buffer[..size];
                match evaluate_worker_client_frame(wire_codec, frame) {
                    Ok(WorkerProtocolAction::PingRequested { request_id }) => {
                        let now_rfc3339 = Utc::now().to_rfc3339();
                        let pong_frame =
                            match build_pong_frame(wire_codec, &request_id, &now_rfc3339) {
                                Ok(frame) => frame,
                                Err(error) => {
                                    logger.warn(
                                        Some("main::wire"),
                                        &format!(
                                            "failed to build PONG response for worker {worker_id}: {error}"
                                        ),
                                    );
                                    continue;
                                }
                            };

                        write_response_frame(
                            &connection,
                            &pong_frame,
                            connection.id(),
                            logger,
                            "PONG response",
                        );
                        let _ = pools.workers.touch_now(worker_id);
                    }
                    Ok(WorkerProtocolAction::QueueRequested {
                        request_id,
                        queue_name,
                    }) => {
                        let ok_frame = match persistent_queues.queue_pool().get_queue(&queue_name) {
                            Some(queue) => {
                                let queue_payload = queue_metadata_payload(queue);
                                wire_codec.encode_frame(
                                    &WireEnvelope::ok(request_id, Some(queue_payload)).into_raw(),
                                )
                            }
                            None => {
                                wire_codec
                                    .encode_frame(&WireEnvelope::ok(request_id, None).into_raw())
                            }
                        };

                        match ok_frame {
                            Ok(frame) => {
                                write_response_frame(
                                    &connection,
                                    &frame,
                                    connection.id(),
                                    logger,
                                    "QUEUE OK response",
                                );
                                let _ = pools.workers.touch_now(worker_id);
                            }
                            Err(error) => {
                                logger.warn(
                                    Some("main::wire"),
                                    &format!(
                                        "failed to build QUEUE response for worker {worker_id}: {error}"
                                    ),
                                );
                            }
                        }
                    }
                    Ok(WorkerProtocolAction::LsQueueRequested { request_id }) => {
                        let queues = persistent_queues
                            .queue_pool()
                            .snapshot()
                            .queues
                            .into_iter()
                            .map(queue_to_wire_value)
                            .collect::<Vec<_>>();
                        let mut payload = PayloadMap::new();
                        payload.insert("queues".to_owned(), rmpv::Value::Array(queues));
                        let ok_frame = wire_codec
                            .encode_frame(&WireEnvelope::ok(request_id, Some(payload)).into_raw());

                        match ok_frame {
                            Ok(frame) => {
                                write_response_frame(
                                    &connection,
                                    &frame,
                                    connection.id(),
                                    logger,
                                    "LSQUEUE OK response",
                                );
                                let _ = pools.workers.touch_now(worker_id);
                            }
                            Err(error) => {
                                logger.warn(
                                    Some("main::wire"),
                                    &format!(
                                        "failed to build LSQUEUE response for worker {worker_id}: {error}"
                                    ),
                                );
                            }
                        }
                    }
                    Ok(WorkerProtocolAction::AddQueueRequested {
                        request_id,
                        queue_name,
                        config,
                    }) => match persistent_queues.register_queue(storage, &queue_name, config) {
                        Ok(queue_id) => {
                            let mut payload = PayloadMap::new();
                            payload.insert(
                                "qid".to_owned(),
                                rmpv::Value::String(queue_id.to_string().into()),
                            );
                            match wire_codec
                                .encode_frame(&WireEnvelope::ok(request_id, Some(payload)).into_raw())
                            {
                                Ok(frame) => {
                                    write_response_frame(
                                        &connection,
                                        &frame,
                                        connection.id(),
                                        logger,
                                        "ADDQUEUE OK response",
                                    );
                                    let _ = pools.workers.touch_now(worker_id);
                                }
                                Err(error) => {
                                    logger.warn(
                                        Some("main::wire"),
                                        &format!(
                                            "failed to build ADDQUEUE response for worker {worker_id}: {error}"
                                        ),
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            let (code, message) =
                                map_persistent_queue_error_for_worker_error(&error, &queue_name);
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                code,
                                &message,
                            );
                        }
                    },
                    Ok(WorkerProtocolAction::EnqueueRequested {
                        request_id,
                        queue_name,
                        job_payload,
                        scheduled_at,
                        max_attempts,
                        retry_interval_ms,
                    }) => {
                        if let Err((code, message)) =
                            ensure_non_system_queue_for_worker(persistent_queues, &queue_name)
                        {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                code,
                                &message,
                            );
                            continue;
                        }

                        let staged_job = match jobs_pool.lock() {
                            Ok(mut guard) => {
                                let enqueue_result = guard.enqueue_job(
                                    persistent_queues.queue_pool(),
                                    &queue_name,
                                    NewJobOptions {
                                        payload: job_payload,
                                        scheduled_at,
                                        max_attempts,
                                        retry_interval_ms,
                                    },
                                );
                                match enqueue_result {
                                    Ok(job_uuid) => {
                                        let Some(job) = guard.get_job(job_uuid).cloned() else {
                                            let _ = send_worker_protocol_error(
                                                wire_codec,
                                                logger,
                                                &connection,
                                                worker_id,
                                                &request_id,
                                                "JOB_NOT_FOUND",
                                                "enqueued job is missing in transient jobs pool",
                                            );
                                            continue;
                                        };
                                        job
                                    }
                                    Err(error) => {
                                        let (code, message) =
                                            map_jobs_pool_error_for_worker_error(&error);
                                        let _ = send_worker_protocol_error(
                                            wire_codec,
                                            logger,
                                            &connection,
                                            worker_id,
                                            &request_id,
                                            code,
                                            &message,
                                        );
                                        continue;
                                    }
                                }
                            }
                            Err(_) => {
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    "JOBS_POOL_LOCK_POISONED",
                                    "jobs pool lock poisoned while staging enqueue request",
                                );
                                continue;
                            }
                        };

                        let persist_payload = build_job_persist_event_payload(&staged_job);
                        match emitter.emit(JOB_PERSIST_REQUESTED_EVENT, Some(persist_payload)) {
                            Ok(()) => {
                                let mut payload = PayloadMap::new();
                                payload.insert(
                                    "jid".to_owned(),
                                    rmpv::Value::String(staged_job.job_id.clone().into()),
                                );

                                match wire_codec
                                    .encode_frame(&WireEnvelope::ok(request_id, Some(payload)).into_raw())
                                {
                                    Ok(frame) => {
                                        write_response_frame(
                                            &connection,
                                            &frame,
                                            connection.id(),
                                            logger,
                                            "ENQUEUE OK response",
                                        );
                                        let _ = pools.workers.touch_now(worker_id);
                                    }
                                    Err(error) => {
                                        logger.warn(
                                            Some("main::wire"),
                                            &format!(
                                                "failed to build ENQUEUE response for worker {worker_id}: {error}"
                                            ),
                                        );
                                    }
                                }
                            }
                            Err(error) => {
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    "JOB_PERSIST_EVENT_FAILED",
                                    &format!("failed to persist staged job via event flow: {error}"),
                                );
                            }
                        }
                    }
                    Ok(WorkerProtocolAction::JobRequested { request_id, job_id }) => {
                        let lookup = match jobs_pool.lock() {
                            Ok(guard) => match guard.resolve_job_lookup(&job_id) {
                                Ok(lookup) => lookup,
                                Err(error) => {
                                    let (code, message) =
                                        map_jobs_pool_error_for_worker_error(&error);
                                    let _ = send_worker_protocol_error(
                                        wire_codec,
                                        logger,
                                        &connection,
                                        worker_id,
                                        &request_id,
                                        code,
                                        &message,
                                    );
                                    continue;
                                }
                            },
                            Err(_) => {
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    "JOBS_POOL_LOCK_POISONED",
                                    "jobs pool lock poisoned while resolving job lookup",
                                );
                                continue;
                            }
                        };

                        match storage.get_job_payload_by_uuid(lookup.job_uuid) {
                            Ok(Some(record)) => {
                                let stored_jid = record
                                    .get("jid")
                                    .and_then(serde_json::Value::as_str)
                                    .unwrap_or_default();
                                if stored_jid != job_id {
                                    match wire_codec
                                        .encode_frame(&WireEnvelope::ok(request_id, None).into_raw())
                                    {
                                        Ok(frame) => {
                                            write_response_frame(
                                                &connection,
                                                &frame,
                                                connection.id(),
                                                logger,
                                                "JOB OK response",
                                            );
                                            let _ = pools.workers.touch_now(worker_id);
                                        }
                                        Err(error) => {
                                            logger.warn(
                                                Some("main::wire"),
                                                &format!(
                                                    "failed to build JOB response for worker {worker_id}: {error}"
                                                ),
                                            );
                                        }
                                    }
                                    continue;
                                }
                                match json_object_to_payload_map(&record) {
                                    Ok(payload) => match wire_codec
                                        .encode_frame(&WireEnvelope::ok(request_id, Some(payload)).into_raw())
                                    {
                                        Ok(frame) => {
                                            write_response_frame(
                                                &connection,
                                                &frame,
                                                connection.id(),
                                                logger,
                                                "JOB OK response",
                                            );
                                            let _ = pools.workers.touch_now(worker_id);
                                        }
                                        Err(error) => {
                                            logger.warn(
                                                Some("main::wire"),
                                                &format!(
                                                    "failed to build JOB response for worker {worker_id}: {error}"
                                                ),
                                            );
                                        }
                                    },
                                    Err(message) => {
                                        let _ = send_worker_protocol_error(
                                            wire_codec,
                                            logger,
                                            &connection,
                                            worker_id,
                                            &request_id,
                                            "JOB_RECORD_INVALID",
                                            message,
                                        );
                                    }
                                }
                            }
                            Ok(None) => {
                                match wire_codec
                                    .encode_frame(&WireEnvelope::ok(request_id, None).into_raw())
                                {
                                    Ok(frame) => {
                                        write_response_frame(
                                            &connection,
                                            &frame,
                                            connection.id(),
                                            logger,
                                            "JOB OK response",
                                        );
                                        let _ = pools.workers.touch_now(worker_id);
                                    }
                                    Err(error) => {
                                        logger.warn(
                                            Some("main::wire"),
                                            &format!(
                                                "failed to build JOB response for worker {worker_id}: {error}"
                                            ),
                                        );
                                    }
                                }
                            }
                            Err(error) => {
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    "JOB_PERSISTENCE_ERROR",
                                    &format!("failed to load job by id from storage: {error}"),
                                );
                            }
                        }
                    }
                    Ok(WorkerProtocolAction::RemoveQueueRequested {
                        request_id,
                        queue_name,
                    }) => {
                        if let Err((code, message)) =
                            ensure_queue_exists_for_worker(persistent_queues, &queue_name)
                        {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                code,
                                &message,
                            );
                            continue;
                        }

                        // TODO: validate queue jobs statuses before allowing removal.
                        if !queue_jobs_allow_removal(&queue_name) {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                "QUEUE_NOT_EMPTY",
                                &format!("queue '{queue_name}' cannot be removed yet"),
                            );
                            continue;
                        }

                        match persistent_queues.remove_queue(storage, &queue_name) {
                            Ok(()) => {
                                match wire_codec
                                    .encode_frame(&WireEnvelope::ok(request_id, None).into_raw())
                                {
                                    Ok(frame) => {
                                        write_response_frame(
                                            &connection,
                                            &frame,
                                            connection.id(),
                                            logger,
                                            "RMQUEUE OK response",
                                        );
                                        let _ = pools.workers.touch_now(worker_id);
                                    }
                                    Err(error) => {
                                        logger.warn(
                                            Some("main::wire"),
                                            &format!(
                                                "failed to build RMQUEUE response for worker {worker_id}: {error}"
                                            ),
                                        );
                                    }
                                }
                            }
                            Err(error) => {
                                let (code, message) =
                                    map_persistent_queue_error_for_worker_error(&error, &queue_name);
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    code,
                                    &message,
                                );
                            }
                        }
                    }
                    Ok(WorkerProtocolAction::PauseQueueRequested {
                        request_id,
                        queue_name,
                    }) => {
                        let queue = match ensure_non_system_queue_for_worker(
                            persistent_queues,
                            &queue_name,
                        ) {
                            Ok(queue) => queue,
                            Err((code, message)) => {
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    code,
                                    &message,
                                );
                                continue;
                            }
                        };

                        if queue.is_paused() {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                "QUEUE_ALREADY_PAUSED",
                                &format!("queue '{queue_name}' is already paused"),
                            );
                            continue;
                        }

                        match persistent_queues.pause_queue(storage, &queue_name) {
                            Ok(()) => match wire_codec
                                .encode_frame(&WireEnvelope::ok(request_id, None).into_raw())
                            {
                                Ok(frame) => {
                                    write_response_frame(
                                        &connection,
                                        &frame,
                                        connection.id(),
                                        logger,
                                        "PAUSE OK response",
                                    );
                                    let _ = pools.workers.touch_now(worker_id);
                                }
                                Err(error) => {
                                    logger.warn(
                                        Some("main::wire"),
                                        &format!(
                                            "failed to build PAUSE response for worker {worker_id}: {error}"
                                        ),
                                    );
                                }
                            },
                            Err(error) => {
                                let (code, message) =
                                    map_persistent_queue_error_for_worker_error(&error, &queue_name);
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    code,
                                    &message,
                                );
                            }
                        }
                    }
                    Ok(WorkerProtocolAction::ResumeQueueRequested {
                        request_id,
                        queue_name,
                    }) => {
                        let queue = match ensure_non_system_queue_for_worker(
                            persistent_queues,
                            &queue_name,
                        ) {
                            Ok(queue) => queue,
                            Err((code, message)) => {
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    code,
                                    &message,
                                );
                                continue;
                            }
                        };

                        if !queue.is_paused() {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                "QUEUE_ALREADY_RUNNING",
                                &format!("queue '{queue_name}' is not paused"),
                            );
                            continue;
                        }

                        match persistent_queues.resume_queue(storage, &queue_name) {
                            Ok(()) => match wire_codec
                                .encode_frame(&WireEnvelope::ok(request_id, None).into_raw())
                            {
                                Ok(frame) => {
                                    write_response_frame(
                                        &connection,
                                        &frame,
                                        connection.id(),
                                        logger,
                                        "RESUME OK response",
                                    );
                                    let _ = pools.workers.touch_now(worker_id);
                                }
                                Err(error) => {
                                    logger.warn(
                                        Some("main::wire"),
                                        &format!(
                                            "failed to build RESUME response for worker {worker_id}: {error}"
                                        ),
                                    );
                                }
                            },
                            Err(error) => {
                                let (code, message) =
                                    map_persistent_queue_error_for_worker_error(&error, &queue_name);
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    code,
                                    &message,
                                );
                            }
                        }
                    }
                    Ok(WorkerProtocolAction::SubscribeRequested {
                        request_id,
                        queue_name,
                        credits,
                    }) => {
                        if let Err((code, message)) =
                            ensure_queue_exists_for_worker(persistent_queues, &queue_name)
                        {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                code,
                                &message,
                            );
                            continue;
                        }

                        match pools.subscribe_worker_to_queue(
                            worker_id,
                            &queue_name,
                            persistent_queues.queue_pool(),
                        ) {
                            Ok(subscription_id) => {
                                if credits > 0 {
                                    if let Err(error) = pools.add_worker_subscription_credits(
                                        worker_id,
                                        subscription_id,
                                        credits,
                                    ) {
                                        let _ = pools.unsubscribe_worker_from_queue(
                                            worker_id,
                                            subscription_id,
                                        );
                                        let (code, message) = map_pool_error_for_worker_error(&error);
                                        let _ = send_worker_protocol_error(
                                            wire_codec,
                                            logger,
                                            &connection,
                                            worker_id,
                                            &request_id,
                                            code,
                                            &message,
                                        );
                                        continue;
                                    }
                                }

                                let mut payload = PayloadMap::new();
                                payload.insert(
                                    "sid".to_owned(),
                                    rmpv::Value::String(subscription_id.to_string().into()),
                                );
                                match wire_codec.encode_frame(
                                    &WireEnvelope::ok(request_id, Some(payload)).into_raw(),
                                ) {
                                    Ok(frame) => {
                                        write_response_frame(
                                            &connection,
                                            &frame,
                                            connection.id(),
                                            logger,
                                            "SUBSCRIBE OK response",
                                        );
                                        let _ = pools.workers.touch_now(worker_id);
                                    }
                                    Err(error) => {
                                        logger.warn(
                                            Some("main::wire"),
                                            &format!(
                                                "failed to build SUBSCRIBE response for worker {worker_id}: {error}"
                                            ),
                                        );
                                    }
                                }
                            }
                            Err(error) => {
                                let (code, message) = map_pool_error_for_worker_error(&error);
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    code,
                                    &message,
                                );
                            }
                        }
                    }
                    Ok(WorkerProtocolAction::UnsubscribeRequested {
                        request_id,
                        subscription_id,
                    }) => match pools.unsubscribe_worker_from_queue(worker_id, subscription_id) {
                        Ok(()) => {
                            match wire_codec
                                .encode_frame(&WireEnvelope::ok(request_id, None).into_raw())
                            {
                                Ok(frame) => {
                                    write_response_frame(
                                        &connection,
                                        &frame,
                                        connection.id(),
                                        logger,
                                        "UNSUBSCRIBE OK response",
                                    );
                                    let _ = pools.workers.touch_now(worker_id);
                                }
                                Err(error) => {
                                    logger.warn(
                                        Some("main::wire"),
                                        &format!(
                                            "failed to build UNSUBSCRIBE response for worker {worker_id}: {error}"
                                        ),
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            let (code, message) = map_pool_error_for_worker_error(&error);
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                code,
                                &message,
                            );
                        }
                    },
                    Ok(WorkerProtocolAction::CreditRequested {
                        request_id,
                        subscription_id,
                        credits,
                    }) => match pools
                        .add_worker_subscription_credits(worker_id, subscription_id, credits)
                    {
                        Ok(_) => match wire_codec
                            .encode_frame(&WireEnvelope::ok(request_id, None).into_raw())
                        {
                            Ok(frame) => {
                                write_response_frame(
                                    &connection,
                                    &frame,
                                    connection.id(),
                                    logger,
                                    "CREDIT OK response",
                                );
                                let _ = pools.workers.touch_now(worker_id);
                            }
                            Err(error) => {
                                logger.warn(
                                    Some("main::wire"),
                                    &format!(
                                        "failed to build CREDIT response for worker {worker_id}: {error}"
                                    ),
                                );
                            }
                        },
                        Err(error) => {
                            let (code, message) = map_pool_error_for_worker_error(&error);
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                code,
                                &message,
                            );
                        }
                    },
                    Ok(WorkerProtocolAction::StatusRequested { request_id }) => {
                        match build_status_payload(
                            app_started_at,
                            pools,
                            storage,
                            persistent_queues.queue_pool(),
                        ) {
                            Ok(payload) => {
                                match wire_codec
                                    .encode_frame(&WireEnvelope::ok(request_id, Some(payload)).into_raw())
                                {
                                    Ok(frame) => {
                                        write_response_frame(
                                            &connection,
                                            &frame,
                                            connection.id(),
                                            logger,
                                            "STATUS OK response",
                                        );
                                        let _ = pools.workers.touch_now(worker_id);
                                    }
                                    Err(error) => {
                                        logger.warn(
                                            Some("main::wire"),
                                            &format!(
                                                "failed to build STATUS response for worker {worker_id}: {error}"
                                            ),
                                        );
                                    }
                                }
                            }
                            Err(error) => {
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    "DIAGNOSTICS_ERROR",
                                    &format!("failed to collect diagnostics: {error}"),
                                );
                            }
                        }
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
                                    connection.id(),
                                    logger,
                                    "ERR response",
                                );
                            }
                            Err(build_error) => {
                                logger.warn(
                                    Some("main::wire"),
                                    &format!(
                                        "failed to build worker protocol ERR for worker {worker_id}: {build_error}"
                                    ),
                                );
                            }
                        }
                        logger.warn(
                            Some("main::wire"),
                            &format!(
                                "protocol violation from worker {worker_id}: {message}; terminating worker connection"
                            ),
                        );
                        let _ = pools.terminate_worker(
                            worker_id,
                            Some(format!("worker protocol violation ({code}): {message}")),
                        );
                    }
                    Err(error) => {
                        logger.warn(
                            Some("main::wire"),
                            &format!(
                                "worker session error on worker {worker_id}: {error}; terminating worker connection"
                            ),
                        );
                        let _ = pools
                            .terminate_worker(worker_id, Some(format!("worker session error: {error}")));
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => {
                logger.warn(
                    Some("main::wire"),
                    &format!("read error on worker {worker_id}: {error}; terminating worker"),
                );
                let _ = pools.terminate_worker(worker_id, Some(format!("socket read error: {error}")));
            }
        }
    }
}

fn send_worker_protocol_error(
    wire_codec: &WireCodec,
    logger: &Logger,
    connection: &Arc<server::PersistentConnection>,
    worker_id: uuid::Uuid,
    request_id: &str,
    code: &str,
    message: &str,
) -> bool {
    match build_protocol_error_frame(wire_codec, request_id, code, message) {
        Ok(error_frame) => {
            write_response_frame(connection, &error_frame, connection.id(), logger, "ERR response")
        }
        Err(build_error) => {
            logger.warn(
                Some("main::wire"),
                &format!("failed to build worker ERR for worker {worker_id}: {build_error}"),
            );
            false
        }
    }
}

fn ensure_queue_exists_for_worker<'a>(
    persistent_queues: &'a PersistentQueuePool,
    queue_name: &str,
) -> Result<&'a Queue, (&'static str, String)> {
    persistent_queues
        .queue_pool()
        .get_queue(queue_name)
        .ok_or_else(|| {
            (
                "QUEUE_NOT_FOUND",
                format!("queue '{queue_name}' not found"),
            )
        })
}

fn ensure_non_system_queue_for_worker<'a>(
    persistent_queues: &'a PersistentQueuePool,
    queue_name: &str,
) -> Result<&'a Queue, (&'static str, String)> {
    let queue = ensure_queue_exists_for_worker(persistent_queues, queue_name)?;
    if queue_name.starts_with('_') {
        return Err((
            "SYSTEM_QUEUE_FORBIDDEN",
            format!("queue '{queue_name}' is a system queue and cannot be modified"),
        ));
    }
    Ok(queue)
}

fn map_pool_error_for_worker_error(error: &PoolError) -> (&'static str, String) {
    match error {
        PoolError::QueueNotFound { queue_name } => {
            ("QUEUE_NOT_FOUND", format!("queue '{queue_name}' not found"))
        }
        PoolError::DuplicateSubscriptionForQueue { queue_name, .. } => (
            "DUPLICATE_SUBSCRIPTION",
            format!("worker already subscribed to queue '{queue_name}'"),
        ),
        PoolError::SubscriptionNotFound { subscription_id, .. } => (
            "SUBSCRIPTION_NOT_FOUND",
            format!("subscription '{subscription_id}' not found for worker"),
        ),
        PoolError::WorkerNotFound { worker_id } => (
            "WORKER_NOT_FOUND",
            format!("worker '{worker_id}' not found in worker pool"),
        ),
        PoolError::InvalidCreditDelta { amount } => (
            "INVALID_CREDITS",
            format!("invalid credit delta '{amount}'"),
        ),
        PoolError::InsufficientCredits { .. } => (
            "INSUFFICIENT_CREDITS",
            "not enough credits for requested subtraction".to_owned(),
        ),
        PoolError::AnonymousConnectionNotFound { connection_id } => (
            "ANONYMOUS_CONNECTION_NOT_FOUND",
            format!("anonymous connection '{connection_id}' not found"),
        ),
    }
}

fn map_jobs_pool_error_for_worker_error(error: &JobsPoolError) -> (&'static str, String) {
    match error {
        JobsPoolError::QueueNotFound { queue_name } => {
            ("QUEUE_NOT_FOUND", format!("queue '{queue_name}' not found"))
        }
        JobsPoolError::SystemQueueForbidden { queue_name } => (
            "SYSTEM_QUEUE_FORBIDDEN",
            format!("queue '{queue_name}' is a system queue and cannot accept jobs"),
        ),
        JobsPoolError::SystemQueueAccessForbidden { queue_name } => (
            "SYSTEM_QUEUE_FORBIDDEN",
            format!("queue '{queue_name}' is a system queue and cannot be accessed"),
        ),
        JobsPoolError::InvalidMaxAttempts { max_attempts } => (
            "INVALID_MAX_ATTEMPTS",
            format!("max_attempts must be >= 1, got {max_attempts}"),
        ),
        JobsPoolError::InvalidRetryIntervalMs { retry_interval_ms } => (
            "INVALID_RETRY_INTERVAL_MS",
            format!("retry_interval_ms must be > 0, got {retry_interval_ms}"),
        ),
        JobsPoolError::InvalidJobId { job_id } => (
            "INVALID_JOB_ID",
            format!("job id '{job_id}' is invalid; expected '<queue-name>:<uuid>'"),
        ),
        JobsPoolError::PayloadSerializationFailed(inner) => (
            "INVALID_JOB_PAYLOAD",
            format!("job payload serialization failed: {inner}"),
        ),
    }
}

fn map_persistent_queue_error_for_worker_error(
    error: &PersistentQueuePoolError,
    queue_name: &str,
) -> (&'static str, String) {
    match error {
        PersistentQueuePoolError::QueuePool(orchestrator::queues::QueuePoolError::DuplicateQueueName {
            ..
        }) => (
            "DUPLICATE_QUEUE",
            format!("queue '{queue_name}' is already registered"),
        ),
        PersistentQueuePoolError::QueuePool(
            orchestrator::queues::QueuePoolError::InvalidQueueName { .. },
        ) => (
            "INVALID_QUEUE_NAME",
            format!(
                "queue '{queue_name}' is invalid (allowed: [A-Za-z0-9_-], first char must be alphanumeric)"
            ),
        ),
        PersistentQueuePoolError::QueuePool(orchestrator::queues::QueuePoolError::QueueNotFound {
            ..
        }) => (
            "QUEUE_NOT_FOUND",
            format!("queue '{queue_name}' not found"),
        ),
        PersistentQueuePoolError::QueuePool(
            orchestrator::queues::QueuePoolError::SystemQueueRemovalForbidden { .. },
        ) => (
            "SYSTEM_QUEUE_FORBIDDEN",
            format!("queue '{queue_name}' is a system queue and cannot be removed"),
        ),
        PersistentQueuePoolError::QueuePool(inner) => {
            ("QUEUE_POOL_ERROR", format!("queue pool operation failed: {inner}"))
        }
        PersistentQueuePoolError::Storage(inner) => (
            "QUEUE_PERSISTENCE_ERROR",
            format!("queue persistence operation failed: {inner}"),
        ),
    }
}

fn queue_jobs_allow_removal(_queue_name: &str) -> bool {
    true
}

fn drain_transient_jobs_pool_before_exit(
    storage: &StorageFacade,
    jobs_pool: &Mutex<JobsPool>,
    logger: &Logger,
) {
    let max_wait = Duration::from_secs(10);
    let started_at = std::time::Instant::now();

    loop {
        let pending = match jobs_pool.lock() {
            Ok(guard) => guard.snapshot().jobs,
            Err(_) => {
                logger.warn(
                    Some("main::shutdown"),
                    "jobs pool lock poisoned during shutdown flush; skipping transient drain",
                );
                return;
            }
        };

        if pending.is_empty() {
            logger.info(
                Some("main::shutdown"),
                "Transient jobs pool drained before exit",
            );
            return;
        }

        let mut persisted_count = 0usize;
        for job in pending {
            let record = build_job_record_json(&job);
            let persisted = storage
                .upsert_job_record(
                    job.uuid,
                    &record,
                    job.execution_start_at.timestamp_millis(),
                    &job.queue_name,
                    job_status_to_str(job.status),
                )
                .is_ok();
            if persisted {
                if let Ok(mut guard) = jobs_pool.lock() {
                    let _ = guard.remove_job(job.uuid);
                }
                persisted_count += 1;
            }
        }

        let remaining = match jobs_pool.lock() {
            Ok(guard) => guard.count(),
            Err(_) => 0,
        };
        logger.debug(
            Some("main::shutdown"),
            &format!(
                "Transient jobs flush iteration persisted {persisted_count} jobs, remaining {remaining}"
            ),
        );

        if remaining == 0 {
            logger.info(
                Some("main::shutdown"),
                "Transient jobs pool drained before exit",
            );
            return;
        }

        if started_at.elapsed() >= max_wait {
            logger.warn(
                Some("main::shutdown"),
                &format!(
                    "Transient jobs pool still has {remaining} jobs after {:?}; best-effort drain exhausted",
                    max_wait
                ),
            );
            return;
        }

        thread::sleep(Duration::from_millis(50));
    }
}

fn build_job_record_json(job: &Job) -> serde_json::Value {
    serde_json::json!({
        "uuid": job.uuid.to_string(),
        "jid": job.job_id,
        "queue_name": job.queue_name,
        "payload": job.payload,
        "status": job_status_to_str(job.status),
        "execution_start_at": job.execution_start_at.to_rfc3339(),
        "max_attempts": job.max_attempts,
        "retry_interval_ms": job.retry_interval_ms,
        "created_at": job.created_at.to_rfc3339(),
        "runtime": {
            "attempts_so_far": job.runtime.attempts_so_far
        }
    })
}

fn build_job_persist_event_payload(job: &Job) -> serde_json::Value {
    serde_json::json!({
        "job_uuid": job.uuid.to_string(),
        "record": build_job_record_json(job),
        "execution_start_ms": job.execution_start_at.timestamp_millis(),
        "queue_name": job.queue_name,
        "status": job_status_to_str(job.status),
    })
}

fn advance_persisted_jobs_statuses(storage: &StorageFacade, logger: &Logger) -> Result<(), String> {
    let now = Utc::now();
    let mut transitioned = 0usize;
    let paused_queues = load_paused_queue_names(storage)?;

    let new_jobs = storage
        .list_job_uuids_by_status("new")
        .map_err(|error| error.to_string())?;
    for job_uuid in new_jobs {
        let Some(record) = storage
            .get_job_payload_by_uuid(job_uuid)
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        let Some(queue_name) = extract_queue_name(&record) else {
            continue;
        };
        if paused_queues.contains(&queue_name) {
            continue;
        }
        let target_status = match extract_execution_start_at(&record) {
            Some(execution_start_at) if execution_start_at > now => JobStatus::Delayed,
            Some(_) => JobStatus::Waiting,
            None => JobStatus::Waiting,
        };
        if apply_persisted_job_status_transition(storage, job_uuid, record, target_status)? {
            transitioned += 1;
        }
    }

    let delayed_jobs = storage
        .list_job_uuids_by_status("delayed")
        .map_err(|error| error.to_string())?;
    for job_uuid in delayed_jobs {
        let Some(record) = storage
            .get_job_payload_by_uuid(job_uuid)
            .map_err(|error| error.to_string())?
        else {
            continue;
        };
        let Some(queue_name) = extract_queue_name(&record) else {
            continue;
        };
        if paused_queues.contains(&queue_name) {
            continue;
        }
        let Some(execution_start_at) = extract_execution_start_at(&record) else {
            continue;
        };
        if execution_start_at <= now
            && apply_persisted_job_status_transition(storage, job_uuid, record, JobStatus::Waiting)?
        {
            transitioned += 1;
        }
    }

    if transitioned > 0 {
        logger.debug(
            Some("main::jobs"),
            &format!("Heartbeat job-status progression transitioned {transitioned} jobs"),
        );
    }
    Ok(())
}

fn load_paused_queue_names(storage: &StorageFacade) -> Result<HashSet<String>, String> {
    let queues = storage.load_queues().map_err(|error| error.to_string())?;
    Ok(queues
        .into_iter()
        .filter(|queue| queue.is_paused())
        .map(|queue| queue.name)
        .collect())
}

fn apply_persisted_job_status_transition(
    storage: &StorageFacade,
    job_uuid: uuid::Uuid,
    mut record: serde_json::Value,
    target_status: JobStatus,
) -> Result<bool, String> {
    let Some(execution_start_at) = extract_execution_start_at(&record) else {
        return Ok(false);
    };
    let Some(queue_name) = record
        .get("queue_name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
    else {
        return Ok(false);
    };
    let current_status = record
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("new");
    let target_status_str = job_status_to_str(target_status);
    if current_status == target_status_str {
        return Ok(false);
    }
    record["status"] = serde_json::Value::String(target_status_str.to_owned());
    storage
        .upsert_job_record(
            job_uuid,
            &record,
            execution_start_at.timestamp_millis(),
            &queue_name,
            target_status_str,
        )
        .map_err(|error| error.to_string())?;
    Ok(true)
}

fn extract_execution_start_at(record: &serde_json::Value) -> Option<DateTime<Utc>> {
    let raw = record.get("execution_start_at")?.as_str()?;
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn extract_queue_name(record: &serde_json::Value) -> Option<String> {
    record
        .get("queue_name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn json_object_to_payload_map(value: &serde_json::Value) -> Result<PayloadMap, &'static str> {
    let serde_json::Value::Object(entries) = value else {
        return Err("persisted job record is not a map object");
    };
    let mut payload = PayloadMap::new();
    for (key, entry) in entries {
        payload.insert(key.clone(), json_value_to_rmpv(entry)?);
    }
    Ok(payload)
}

fn json_value_to_rmpv(value: &serde_json::Value) -> Result<rmpv::Value, &'static str> {
    match value {
        serde_json::Value::Null => Ok(rmpv::Value::Nil),
        serde_json::Value::Bool(v) => Ok(rmpv::Value::Boolean(*v)),
        serde_json::Value::Number(v) => {
            if let Some(raw) = v.as_i64() {
                Ok(rmpv::Value::Integer(raw.into()))
            } else if let Some(raw) = v.as_u64() {
                if raw > i64::MAX as u64 {
                    Err("persisted job record contains integer above int64 range")
                } else {
                    Ok(rmpv::Value::Integer((raw as i64).into()))
                }
            } else {
                Err("persisted job record contains unsupported numeric value")
            }
        }
        serde_json::Value::String(v) => Ok(rmpv::Value::String(v.as_str().into())),
        serde_json::Value::Array(values) => {
            let mut out = Vec::with_capacity(values.len());
            for entry in values {
                out.push(json_value_to_rmpv(entry)?);
            }
            Ok(rmpv::Value::Array(out))
        }
        serde_json::Value::Object(values) => {
            let mut out = Vec::with_capacity(values.len());
            for (k, v) in values {
                out.push((rmpv::Value::String(k.as_str().into()), json_value_to_rmpv(v)?));
            }
            Ok(rmpv::Value::Map(out))
        }
    }
}

fn job_status_to_str(status: JobStatus) -> &'static str {
    match status {
        JobStatus::New => "new",
        JobStatus::Waiting => "waiting",
        JobStatus::Delayed => "delayed",
    }
}

fn queue_metadata_payload(queue: &Queue) -> PayloadMap {
    let mut payload = PayloadMap::new();
    let queue_value = queue_to_wire_value(queue.clone());
    let rmpv::Value::Map(entries) = queue_value else {
        return payload;
    };
    for (k, v) in entries {
        if let rmpv::Value::String(s) = k {
            if let Some(key) = s.as_str() {
                payload.insert(key.to_owned(), v);
            }
        }
    }
    payload
}

fn queue_to_wire_value(queue: Queue) -> rmpv::Value {
    let mut config_entries = Vec::new();
    let concurrency = match queue.config.concurrency_limit {
        Some(limit) => rmpv::Value::Integer((limit as i64).into()),
        None => rmpv::Value::Nil,
    };
    config_entries.push((rmpv::Value::String("concurrency_limit".into()), concurrency));
    config_entries.push((
        rmpv::Value::String("allow_job_overrides".into()),
        rmpv::Value::Boolean(queue.config.allow_job_overrides),
    ));

    let state = match queue.state {
        QueueState::Active => "running",
        QueueState::Paused => "paused",
    };

    rmpv::Value::Map(vec![
        (
            rmpv::Value::String("id".into()),
            rmpv::Value::String(queue.id.to_string().into()),
        ),
        (
            rmpv::Value::String("name".into()),
            rmpv::Value::String(queue.name.into()),
        ),
        (
            rmpv::Value::String("state".into()),
            rmpv::Value::String(state.into()),
        ),
        (
            rmpv::Value::String("config".into()),
            rmpv::Value::Map(config_entries),
        ),
    ])
}
