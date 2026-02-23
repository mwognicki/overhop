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

use std::process;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{io, thread};
use std::{path::PathBuf, sync::mpsc::TryRecvError};

use chrono::{DateTime, Utc};
use events::EventEmitter;
use heartbeat::{HEARTBEAT_EVENT, Heartbeat};
use logging::{LogLevel, Logger, LoggerConfig};
use orchestrator::jobs::{
    advance_persisted_jobs_statuses, drain_transient_jobs_pool_before_exit, JobsPool,
};
use orchestrator::queues::persistent::PersistentQueuePool;
use pools::ConnectionWorkerPools;
use serde_json::json;
use server::TcpServer;
use storage::StorageFacade;
use shutdown::ShutdownHooks;
use utils::{runtime::ensure_posix_or_exit, startup_banner::print_startup_banner, timing::measure_execution};
use wire::codec::WireCodec;
use wire::session::runtime::{
    process_anonymous_client_messages, process_worker_client_messages,
    purge_stale_anonymous_connections, AnonymousTimeoutPolicy,
};

const JOB_PERSIST_REQUESTED_EVENT: &str = "jobs.persist.requested";

fn main() {
    ensure_posix_or_exit();
    print_startup_banner();
    let app_started_at = Utc::now();

    let runtime_args = std::env::args().skip(1).collect::<Vec<_>>();
    let (runtime_flags, config_args) = self_debug::extract_runtime_flags(runtime_args);
    let mut app_config = config::load_or_exit(config_args);
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
        let Some(created_at_ms) = payload
            .get("created_at_ms")
            .and_then(serde_json::Value::as_i64)
        else {
            return Err("job persist event missing i64 'created_at_ms'".to_owned());
        };
        let Some(queue_name) = payload.get("queue_name").and_then(serde_json::Value::as_str) else {
            return Err("job persist event missing string 'queue_name'".to_owned());
        };
        let Some(status) = payload.get("status").and_then(serde_json::Value::as_str) else {
            return Err("job persist event missing string 'status'".to_owned());
        };

        storage_for_job_persist
            .upsert_job_record(job_uuid, record, execution_start_ms, created_at_ms, queue_name, status)
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
            app_config.pagination.page_size,
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
