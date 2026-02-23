use std::sync::{Arc, Mutex};
use std::io;

use chrono::{DateTime, Utc};

use crate::diagnostics::build_status_payload;
use crate::events::EventEmitter;
use crate::logging::Logger;
use crate::orchestrator;
use crate::orchestrator::jobs::{
    build_job_persist_event_payload, can_remove_job_by_status, is_lsjob_status_allowed,
    is_qstats_status_allowed, qstats_statuses, queue_jobs_allow_removal,
    JobsPool, JobsPoolError, NewJobOptions,
};
use crate::orchestrator::queues::persistent::{PersistentQueuePool, PersistentQueuePoolError};
use crate::orchestrator::queues::{Queue, QueueState};
use crate::pools::{self, ConnectionWorkerPools, PoolError};
use crate::server;
use crate::storage::StorageFacade;
use crate::wire;
use crate::wire::codec::{json_object_to_payload_map, json_value_to_rmpv, WireCodec};
use crate::wire::envelope::{PayloadMap, WireEnvelope};
use crate::JOB_PERSIST_REQUESTED_EVENT;

use super::{
    build_ident_frame, build_pong_frame, build_protocol_error_frame,
    evaluate_anonymous_client_frame, evaluate_worker_client_frame, AnonymousProtocolAction,
    WorkerProtocolAction, CONNECTION_TIMEOUT_CODE, REGISTER_TIMEOUT_CODE,
};

#[derive(Clone, Copy)]
pub struct AnonymousTimeoutPolicy {
    pub unhelloed_max_lifetime_seconds: u64,
    pub helloed_unregistered_max_lifetime_seconds: u64,
    pub ident_register_timeout_seconds: u64,
}

impl AnonymousTimeoutPolicy {
    // Computes heartbeat maintenance cadence from anonymous lifecycle limits.
    pub fn maintenance_interval_seconds(self) -> u64 {
        self.unhelloed_max_lifetime_seconds
            .min(self.helloed_unregistered_max_lifetime_seconds)
            .min(self.ident_register_timeout_seconds)
    }
}

// Processes anonymous-connection wire frames and applies HELLO/REGISTER lifecycle actions.
pub fn process_anonymous_client_messages(
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

// Writes a pre-encoded wire response frame to a persistent connection.
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

// Builds/sends protocol `ERR` for anonymous flow and terminates that anonymous connection.
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

// Purges stale anonymous connections and enforces IDENT/register timeout lifecycle.
pub fn purge_stale_anonymous_connections(
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

// Processes registered-worker wire frames and routes supported worker protocol actions.
pub fn process_worker_client_messages(
    pools: &ConnectionWorkerPools,
    emitter: &EventEmitter,
    wire_codec: &WireCodec,
    logger: &Logger,
    storage: &StorageFacade,
    persistent_queues: &mut PersistentQueuePool,
    jobs_pool: &Mutex<JobsPool>,
    default_page_size: u32,
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
                    Ok(WorkerProtocolAction::QueueStatsRequested {
                        request_id,
                        queue_name,
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

                        let stats = match storage.list_queue_status_counts() {
                            Ok(stats) => stats,
                            Err(error) => {
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    "JOB_PERSISTENCE_ERROR",
                                    &format!(
                                        "failed to load queue status counters from storage: {error}"
                                    ),
                                );
                                continue;
                            }
                        };

                        let mut counts_by_status = std::collections::HashMap::<String, u64>::new();
                        for stat in stats {
                            if stat.queue_name == queue_name && is_qstats_status_allowed(&stat.status)
                            {
                                counts_by_status.insert(stat.status, stat.count);
                            }
                        }

                        let mut summary = Vec::new();
                        for status in qstats_statuses() {
                            let count = *counts_by_status.get(*status).unwrap_or(&0);
                            summary.push((
                                rmpv::Value::String((*status).into()),
                                rmpv::Value::Integer((count as i64).into()),
                            ));
                        }

                        let mut payload = PayloadMap::new();
                        payload.insert("q".to_owned(), rmpv::Value::String(queue_name.into()));
                        payload.insert("stats".to_owned(), rmpv::Value::Map(summary));

                        match wire_codec
                            .encode_frame(&WireEnvelope::ok(request_id, Some(payload)).into_raw())
                        {
                            Ok(frame) => {
                                write_response_frame(
                                    &connection,
                                    &frame,
                                    connection.id(),
                                    logger,
                                    "QSTATS OK response",
                                );
                                let _ = pools.workers.touch_now(worker_id);
                            }
                            Err(error) => {
                                logger.warn(
                                    Some("main::wire"),
                                    &format!(
                                        "failed to build QSTATS response for worker {worker_id}: {error}"
                                    ),
                                );
                            }
                        }
                    }
                    Ok(WorkerProtocolAction::LsJobRequested {
                        request_id,
                        queue_name,
                        status,
                        page_size,
                        page,
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

                        if !is_lsjob_status_allowed(&status) {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                "INVALID_JOB_STATUS_FILTER",
                                &format!(
                                    "unsupported status '{status}'; allowed values: new,waiting,delayed,completed,failed,active"
                                ),
                            );
                            continue;
                        }

                        let effective_page_size = page_size.unwrap_or(default_page_size);
                        if effective_page_size == 0 {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                "INVALID_PAGE_SIZE",
                                "page_size must be >= 1",
                            );
                            continue;
                        }
                        let effective_page = page.unwrap_or(1);
                        if effective_page == 0 {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                "INVALID_PAGE",
                                "page must be >= 1",
                            );
                            continue;
                        }

                        let records = match storage.list_job_records_by_queue_and_status(
                            &queue_name,
                            &status,
                            effective_page,
                            effective_page_size,
                        ) {
                            Ok(records) => records,
                            Err(error) => {
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    "JOB_PERSISTENCE_ERROR",
                                    &format!(
                                        "failed to list jobs by queue/status from storage: {error}"
                                    ),
                                );
                                continue;
                            }
                        };

                        let mut jobs = Vec::with_capacity(records.len());
                        let mut invalid_record = None;
                        for record in records {
                            match json_value_to_rmpv(&record) {
                                Ok(value) => jobs.push(value),
                                Err(message) => {
                                    invalid_record = Some(message);
                                    break;
                                }
                            }
                        }
                        if let Some(message) = invalid_record {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                "JOB_RECORD_INVALID",
                                message,
                            );
                            continue;
                        }

                        let mut payload = PayloadMap::new();
                        payload.insert("q".to_owned(), rmpv::Value::String(queue_name.into()));
                        payload.insert("status".to_owned(), rmpv::Value::String(status.into()));
                        payload.insert(
                            "page".to_owned(),
                            rmpv::Value::Integer((effective_page as i64).into()),
                        );
                        payload.insert(
                            "page_size".to_owned(),
                            rmpv::Value::Integer((effective_page_size as i64).into()),
                        );
                        payload.insert("jobs".to_owned(), rmpv::Value::Array(jobs));

                        match wire_codec
                            .encode_frame(&WireEnvelope::ok(request_id, Some(payload)).into_raw())
                        {
                            Ok(frame) => {
                                write_response_frame(
                                    &connection,
                                    &frame,
                                    connection.id(),
                                    logger,
                                    "LSJOB OK response",
                                );
                                let _ = pools.workers.touch_now(worker_id);
                            }
                            Err(error) => {
                                logger.warn(
                                    Some("main::wire"),
                                    &format!(
                                        "failed to build LSJOB response for worker {worker_id}: {error}"
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
                    Ok(WorkerProtocolAction::RemoveJobRequested { request_id, job_id }) => {
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

                        let record = match storage.get_job_payload_by_uuid(lookup.job_uuid) {
                            Ok(record) => record,
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
                                continue;
                            }
                        };

                        let Some(record) = record else {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                "JOB_NOT_FOUND",
                                "job not found for provided jid",
                            );
                            continue;
                        };

                        let stored_jid = record
                            .get("jid")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or_default();
                        if stored_jid != job_id {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                "JOB_NOT_FOUND",
                                "job not found for provided jid",
                            );
                            continue;
                        }

                        let status = record
                            .get("status")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("new");
                        if !can_remove_job_by_status(status) {
                            let _ = send_worker_protocol_error(
                                wire_codec,
                                logger,
                                &connection,
                                worker_id,
                                &request_id,
                                "JOB_STATUS_NOT_REMOVABLE",
                                &format!(
                                    "job status '{status}' cannot be removed; allowed statuses: delayed,new,failed,completed"
                                ),
                            );
                            continue;
                        }

                        match storage.remove_job_record(lookup.job_uuid) {
                            Ok(true) => {
                                match wire_codec
                                    .encode_frame(&WireEnvelope::ok(request_id, None).into_raw())
                                {
                                    Ok(frame) => {
                                        write_response_frame(
                                            &connection,
                                            &frame,
                                            connection.id(),
                                            logger,
                                            "RMJOB OK response",
                                        );
                                        let _ = pools.workers.touch_now(worker_id);
                                    }
                                    Err(error) => {
                                        logger.warn(
                                            Some("main::wire"),
                                            &format!(
                                                "failed to build RMJOB response for worker {worker_id}: {error}"
                                            ),
                                        );
                                    }
                                }
                            }
                            Ok(false) => {
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    "JOB_NOT_FOUND",
                                    "job not found for provided jid",
                                );
                            }
                            Err(error) => {
                                let _ = send_worker_protocol_error(
                                    wire_codec,
                                    logger,
                                    &connection,
                                    worker_id,
                                    &request_id,
                                    "JOB_REMOVE_FAILED",
                                    &format!("failed to remove job from storage: {error}"),
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

// Builds/sends protocol `ERR` frame to a worker connection.
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

// Resolves a queue by name for worker flows and returns wire-friendly not-found errors.
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

// Resolves queue and rejects system queues for worker mutating operations.
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

// Maps pool-layer errors to wire protocol error codes/messages for worker responses.
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

// Maps jobs-layer errors to wire protocol error codes/messages for worker responses.
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

// Maps persistent-queue-layer errors to wire protocol error codes/messages.
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

// Placeholder policy hook deciding whether queue jobs allow queue removal.
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

// Serializes queue domain struct into `rmpv::Value` map for wire transport.
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
