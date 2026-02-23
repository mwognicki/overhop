use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::orchestrator::queues::QueueConfig;
use crate::wire::codec::WireCodec;
use crate::wire::envelope::WireEnvelope;

use super::{
    ADDQUEUE_MESSAGE_TYPE, CREDIT_MESSAGE_TYPE, ENQUEUE_MESSAGE_TYPE, JOB_MESSAGE_TYPE,
    LSJOB_MESSAGE_TYPE, LSQUEUE_MESSAGE_TYPE, PAUSE_MESSAGE_TYPE, PING_MESSAGE_TYPE,
    PROTOCOL_VIOLATION_CODE, QSTATS_MESSAGE_TYPE, QUEUE_MESSAGE_TYPE, RESUME_MESSAGE_TYPE,
    RMJOB_MESSAGE_TYPE, RMQUEUE_MESSAGE_TYPE, STATUS_MESSAGE_TYPE, SUBSCRIBE_MESSAGE_TYPE,
    SessionError, UNSUBSCRIBE_MESSAGE_TYPE,
};

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerProtocolAction {
    PingRequested { request_id: String },
    QueueRequested { request_id: String, queue_name: String },
    LsQueueRequested { request_id: String },
    AddQueueRequested {
        request_id: String,
        queue_name: String,
        config: Option<QueueConfig>,
    },
    RemoveQueueRequested {
        request_id: String,
        queue_name: String,
    },
    PauseQueueRequested {
        request_id: String,
        queue_name: String,
    },
    ResumeQueueRequested {
        request_id: String,
        queue_name: String,
    },
    EnqueueRequested {
        request_id: String,
        queue_name: String,
        job_payload: Option<serde_json::Value>,
        scheduled_at: Option<DateTime<Utc>>,
        max_attempts: Option<u32>,
        retry_interval_ms: Option<u64>,
    },
    JobRequested {
        request_id: String,
        job_id: String,
    },
    RemoveJobRequested {
        request_id: String,
        job_id: String,
    },
    LsJobRequested {
        request_id: String,
        queue_name: String,
        status: String,
        page_size: Option<u32>,
        page: Option<u32>,
    },
    QueueStatsRequested {
        request_id: String,
        queue_name: String,
    },
    SubscribeRequested {
        request_id: String,
        queue_name: String,
        credits: u32,
    },
    UnsubscribeRequested {
        request_id: String,
        subscription_id: Uuid,
    },
    CreditRequested {
        request_id: String,
        subscription_id: Uuid,
        credits: u32,
    },
    StatusRequested { request_id: String },
}

pub fn evaluate_worker_client_frame(
    codec: &WireCodec,
    frame: &[u8],
) -> Result<WorkerProtocolAction, SessionError> {
    let raw = codec.decode_frame(frame).map_err(SessionError::Codec)?;
    let envelope = WireEnvelope::from_raw(&raw).map_err(SessionError::Envelope)?;
    envelope
        .validate_client_to_server()
        .map_err(SessionError::Envelope)?;

    if envelope.message_type != PING_MESSAGE_TYPE {
        if envelope.message_type == QUEUE_MESSAGE_TYPE {
            let queue_name = envelope
                .payload
                .get("q")
                .and_then(rmpv::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "QUEUE payload must contain string key 'q'".to_owned(),
                })?;
            return Ok(WorkerProtocolAction::QueueRequested {
                request_id: envelope.request_id,
                queue_name,
            });
        }

        if envelope.message_type == LSQUEUE_MESSAGE_TYPE {
            if !envelope.payload.is_empty() {
                return Err(SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "LSQUEUE payload must be an empty map".to_owned(),
                });
            }

            return Ok(WorkerProtocolAction::LsQueueRequested {
                request_id: envelope.request_id,
            });
        }

        if envelope.message_type == ADDQUEUE_MESSAGE_TYPE {
            let queue_name = envelope
                .payload
                .get("name")
                .and_then(rmpv::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "ADDQUEUE payload must contain string key 'name'".to_owned(),
                })?;

            let config = match envelope.payload.get("config") {
                Some(rmpv::Value::Map(entries)) => {
                    let mut concurrency_limit: Option<u32> = None;
                    let mut allow_job_overrides: Option<bool> = None;

                    for (key, value) in entries {
                        let Some(key_str) = key.as_str() else {
                            continue;
                        };
                        if key_str == "concurrency_limit" {
                            if value.is_nil() {
                                concurrency_limit = None;
                                continue;
                            }
                            let Some(raw) = value.as_i64() else {
                                return Err(SessionError::ProtocolViolation {
                                    request_id: Some(envelope.request_id.clone()),
                                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                                    message: "ADDQUEUE config.concurrency_limit must be positive integer or nil"
                                        .to_owned(),
                                });
                            };
                            if raw <= 0 || raw > u32::MAX as i64 {
                                return Err(SessionError::ProtocolViolation {
                                    request_id: Some(envelope.request_id.clone()),
                                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                                    message: "ADDQUEUE config.concurrency_limit must be in range 1..=u32::MAX"
                                        .to_owned(),
                                });
                            }
                            concurrency_limit = Some(raw as u32);
                            continue;
                        }
                        if key_str == "allow_job_overrides" {
                            let Some(flag) = value.as_bool() else {
                                return Err(SessionError::ProtocolViolation {
                                    request_id: Some(envelope.request_id.clone()),
                                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                                    message: "ADDQUEUE config.allow_job_overrides must be bool"
                                        .to_owned(),
                                });
                            };
                            allow_job_overrides = Some(flag);
                            continue;
                        }
                    }

                    Some(crate::orchestrator::queues::QueueConfig {
                        concurrency_limit,
                        allow_job_overrides: allow_job_overrides.unwrap_or(true),
                    })
                }
                Some(_) => {
                    return Err(SessionError::ProtocolViolation {
                        request_id: Some(envelope.request_id.clone()),
                        code: PROTOCOL_VIOLATION_CODE.to_owned(),
                        message: "ADDQUEUE payload key 'config' must be map".to_owned(),
                    });
                }
                None => None,
            };

            return Ok(WorkerProtocolAction::AddQueueRequested {
                request_id: envelope.request_id,
                queue_name,
                config,
            });
        }

        if envelope.message_type == RMQUEUE_MESSAGE_TYPE {
            let queue_name = envelope
                .payload
                .get("q")
                .and_then(rmpv::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "RMQUEUE payload must contain string key 'q'".to_owned(),
                })?;

            return Ok(WorkerProtocolAction::RemoveQueueRequested {
                request_id: envelope.request_id,
                queue_name,
            });
        }

        if envelope.message_type == PAUSE_MESSAGE_TYPE {
            let queue_name = envelope
                .payload
                .get("q")
                .and_then(rmpv::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "PAUSE payload must contain string key 'q'".to_owned(),
                })?;

            return Ok(WorkerProtocolAction::PauseQueueRequested {
                request_id: envelope.request_id,
                queue_name,
            });
        }

        if envelope.message_type == RESUME_MESSAGE_TYPE {
            let queue_name = envelope
                .payload
                .get("q")
                .and_then(rmpv::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "RESUME payload must contain string key 'q'".to_owned(),
                })?;

            return Ok(WorkerProtocolAction::ResumeQueueRequested {
                request_id: envelope.request_id,
                queue_name,
            });
        }

        if envelope.message_type == ENQUEUE_MESSAGE_TYPE {
            let queue_name = envelope
                .payload
                .get("q")
                .and_then(rmpv::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "ENQUEUE payload must contain string key 'q'".to_owned(),
                })?;

            let job_payload = match envelope.payload.get("job_payload") {
                Some(value) => Some(rmpv_value_to_json(value).map_err(|message| {
                    SessionError::ProtocolViolation {
                        request_id: Some(envelope.request_id.clone()),
                        code: PROTOCOL_VIOLATION_CODE.to_owned(),
                        message: format!("ENQUEUE payload key 'job_payload' {message}"),
                    }
                })?),
                None => None,
            };

            let scheduled_at = match envelope.payload.get("scheduled_at") {
                Some(value) => {
                    let Some(raw) = value.as_str() else {
                        return Err(SessionError::ProtocolViolation {
                            request_id: Some(envelope.request_id.clone()),
                            code: PROTOCOL_VIOLATION_CODE.to_owned(),
                            message: "ENQUEUE payload key 'scheduled_at' must be RFC3339 string"
                                .to_owned(),
                        });
                    };
                    Some(DateTime::parse_from_rfc3339(raw).map_err(|_| {
                        SessionError::ProtocolViolation {
                            request_id: Some(envelope.request_id.clone()),
                            code: PROTOCOL_VIOLATION_CODE.to_owned(),
                            message:
                                "ENQUEUE payload key 'scheduled_at' must be valid RFC3339 timestamp"
                                    .to_owned(),
                        }
                    })?
                    .with_timezone(&Utc))
                }
                None => None,
            };

            let max_attempts = match envelope.payload.get("max_attempts") {
                Some(value) => {
                    let Some(raw) = value.as_i64() else {
                        return Err(SessionError::ProtocolViolation {
                            request_id: Some(envelope.request_id.clone()),
                            code: PROTOCOL_VIOLATION_CODE.to_owned(),
                            message:
                                "ENQUEUE payload key 'max_attempts' must be integer >= 1".to_owned(),
                        });
                    };
                    if raw < 1 || raw > u32::MAX as i64 {
                        return Err(SessionError::ProtocolViolation {
                            request_id: Some(envelope.request_id.clone()),
                            code: PROTOCOL_VIOLATION_CODE.to_owned(),
                            message:
                                "ENQUEUE payload key 'max_attempts' must be in range 1..=u32::MAX"
                                    .to_owned(),
                        });
                    }
                    Some(raw as u32)
                }
                None => None,
            };

            let retry_interval_ms = match envelope.payload.get("retry_interval_ms") {
                Some(value) => {
                    let Some(raw) = value.as_i64() else {
                        return Err(SessionError::ProtocolViolation {
                            request_id: Some(envelope.request_id.clone()),
                            code: PROTOCOL_VIOLATION_CODE.to_owned(),
                            message:
                                "ENQUEUE payload key 'retry_interval_ms' must be integer > 0"
                                    .to_owned(),
                        });
                    };
                    if raw < 1 {
                        return Err(SessionError::ProtocolViolation {
                            request_id: Some(envelope.request_id.clone()),
                            code: PROTOCOL_VIOLATION_CODE.to_owned(),
                            message:
                                "ENQUEUE payload key 'retry_interval_ms' must be integer > 0"
                                    .to_owned(),
                        });
                    }
                    Some(raw as u64)
                }
                None => None,
            };

            return Ok(WorkerProtocolAction::EnqueueRequested {
                request_id: envelope.request_id,
                queue_name,
                job_payload,
                scheduled_at,
                max_attempts,
                retry_interval_ms,
            });
        }

        if envelope.message_type == JOB_MESSAGE_TYPE {
            let job_id = envelope
                .payload
                .get("jid")
                .and_then(rmpv::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "JOB payload must contain string key 'jid'".to_owned(),
                })?;
            return Ok(WorkerProtocolAction::JobRequested {
                request_id: envelope.request_id,
                job_id,
            });
        }

        if envelope.message_type == RMJOB_MESSAGE_TYPE {
            let job_id = envelope
                .payload
                .get("jid")
                .and_then(rmpv::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "RMJOB payload must contain string key 'jid'".to_owned(),
                })?;
            return Ok(WorkerProtocolAction::RemoveJobRequested {
                request_id: envelope.request_id,
                job_id,
            });
        }

        if envelope.message_type == LSJOB_MESSAGE_TYPE {
            let queue_name = envelope
                .payload
                .get("q")
                .and_then(rmpv::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "LSJOB payload must contain string key 'q'".to_owned(),
                })?;
            let status = envelope
                .payload
                .get("status")
                .and_then(rmpv::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "LSJOB payload must contain string key 'status'".to_owned(),
                })?;
            let page_size = match envelope.payload.get("page_size") {
                Some(value) => {
                    let Some(raw) = value.as_i64() else {
                        return Err(SessionError::ProtocolViolation {
                            request_id: Some(envelope.request_id.clone()),
                            code: PROTOCOL_VIOLATION_CODE.to_owned(),
                            message: "LSJOB payload key 'page_size' must be integer >= 1"
                                .to_owned(),
                        });
                    };
                    if raw < 1 || raw > u32::MAX as i64 {
                        return Err(SessionError::ProtocolViolation {
                            request_id: Some(envelope.request_id.clone()),
                            code: PROTOCOL_VIOLATION_CODE.to_owned(),
                            message: "LSJOB payload key 'page_size' must be in range 1..=u32::MAX"
                                .to_owned(),
                        });
                    }
                    Some(raw as u32)
                }
                None => None,
            };
            let page = match envelope.payload.get("page") {
                Some(value) => {
                    let Some(raw) = value.as_i64() else {
                        return Err(SessionError::ProtocolViolation {
                            request_id: Some(envelope.request_id.clone()),
                            code: PROTOCOL_VIOLATION_CODE.to_owned(),
                            message: "LSJOB payload key 'page' must be integer >= 1".to_owned(),
                        });
                    };
                    if raw < 1 || raw > u32::MAX as i64 {
                        return Err(SessionError::ProtocolViolation {
                            request_id: Some(envelope.request_id.clone()),
                            code: PROTOCOL_VIOLATION_CODE.to_owned(),
                            message: "LSJOB payload key 'page' must be in range 1..=u32::MAX"
                                .to_owned(),
                        });
                    }
                    Some(raw as u32)
                }
                None => None,
            };
            return Ok(WorkerProtocolAction::LsJobRequested {
                request_id: envelope.request_id,
                queue_name,
                status,
                page_size,
                page,
            });
        }

        if envelope.message_type == QSTATS_MESSAGE_TYPE {
            let queue_name = envelope
                .payload
                .get("q")
                .and_then(rmpv::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "QSTATS payload must contain string key 'q'".to_owned(),
                })?;

            return Ok(WorkerProtocolAction::QueueStatsRequested {
                request_id: envelope.request_id,
                queue_name,
            });
        }

        if envelope.message_type == SUBSCRIBE_MESSAGE_TYPE {
            let queue_name = envelope
                .payload
                .get("q")
                .and_then(rmpv::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "SUBSCRIBE payload must contain string key 'q'".to_owned(),
                })?;

            let credits = if let Some(credits_value) = envelope.payload.get("credits") {
                let Some(raw) = credits_value.as_i64() else {
                    return Err(SessionError::ProtocolViolation {
                        request_id: Some(envelope.request_id.clone()),
                        code: PROTOCOL_VIOLATION_CODE.to_owned(),
                        message: "SUBSCRIBE payload key 'credits' must be non-negative integer"
                            .to_owned(),
                    });
                };
                if raw < 0 || raw > u32::MAX as i64 {
                    return Err(SessionError::ProtocolViolation {
                        request_id: Some(envelope.request_id.clone()),
                        code: PROTOCOL_VIOLATION_CODE.to_owned(),
                        message: "SUBSCRIBE payload key 'credits' must be in u32 range"
                            .to_owned(),
                    });
                }
                raw as u32
            } else {
                0
            };

            return Ok(WorkerProtocolAction::SubscribeRequested {
                request_id: envelope.request_id,
                queue_name,
                credits,
            });
        }

        if envelope.message_type == UNSUBSCRIBE_MESSAGE_TYPE {
            let subscription_id_raw = envelope
                .payload
                .get("sid")
                .and_then(rmpv::Value::as_str)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "UNSUBSCRIBE payload must contain string key 'sid'".to_owned(),
                })?;
            let subscription_id =
                Uuid::parse_str(subscription_id_raw).map_err(|_| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "UNSUBSCRIBE payload key 'sid' must be valid UUID".to_owned(),
                })?;

            return Ok(WorkerProtocolAction::UnsubscribeRequested {
                request_id: envelope.request_id,
                subscription_id,
            });
        }

        if envelope.message_type == CREDIT_MESSAGE_TYPE {
            let subscription_id_raw = envelope
                .payload
                .get("sid")
                .and_then(rmpv::Value::as_str)
                .ok_or_else(|| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "CREDIT payload must contain string key 'sid'".to_owned(),
                })?;
            let subscription_id =
                Uuid::parse_str(subscription_id_raw).map_err(|_| SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "CREDIT payload key 'sid' must be valid UUID".to_owned(),
                })?;

            let Some(raw_credits) = envelope.payload.get("credits").and_then(rmpv::Value::as_i64)
            else {
                return Err(SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "CREDIT payload must contain positive integer key 'credits'"
                        .to_owned(),
                });
            };
            if raw_credits <= 0 || raw_credits > u32::MAX as i64 {
                return Err(SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id.clone()),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "CREDIT payload key 'credits' must be a positive u32 integer"
                        .to_owned(),
                });
            }

            return Ok(WorkerProtocolAction::CreditRequested {
                request_id: envelope.request_id,
                subscription_id,
                credits: raw_credits as u32,
            });
        }

        if envelope.message_type == STATUS_MESSAGE_TYPE {
            if !envelope.payload.is_empty() {
                return Err(SessionError::ProtocolViolation {
                    request_id: Some(envelope.request_id),
                    code: PROTOCOL_VIOLATION_CODE.to_owned(),
                    message: "STATUS payload must be an empty map".to_owned(),
                });
            }

            return Ok(WorkerProtocolAction::StatusRequested {
                request_id: envelope.request_id,
            });
        }

        return Err(SessionError::ProtocolViolation {
            request_id: Some(envelope.request_id),
            code: PROTOCOL_VIOLATION_CODE.to_owned(),
            message:
                "registered workers can currently send only PING, QUEUE, LSQUEUE, SUBSCRIBE, UNSUBSCRIBE, CREDIT, ADDQUEUE, RMQUEUE, PAUSE, RESUME, ENQUEUE, JOB, RMJOB, LSJOB, QSTATS, or STATUS"
                    .to_owned(),
        });
    }

    if !envelope.payload.is_empty() {
        return Err(SessionError::ProtocolViolation {
            request_id: Some(envelope.request_id),
            code: PROTOCOL_VIOLATION_CODE.to_owned(),
            message: "PING payload must be an empty map".to_owned(),
        });
    }

    Ok(WorkerProtocolAction::PingRequested {
        request_id: envelope.request_id,
    })
}

fn rmpv_value_to_json(value: &rmpv::Value) -> Result<serde_json::Value, &'static str> {
    match value {
        rmpv::Value::Nil => Ok(serde_json::Value::Null),
        rmpv::Value::Boolean(v) => Ok(serde_json::Value::Bool(*v)),
        rmpv::Value::Integer(v) => {
            if let Some(raw) = v.as_i64() {
                Ok(serde_json::json!(raw))
            } else {
                Err("contains unsupported integer representation")
            }
        }
        rmpv::Value::String(v) => Ok(serde_json::json!(v.as_str().unwrap_or_default())),
        rmpv::Value::Binary(v) => Ok(serde_json::json!(v)),
        rmpv::Value::Array(values) => {
            let mut converted = Vec::with_capacity(values.len());
            for entry in values {
                converted.push(rmpv_value_to_json(entry)?);
            }
            Ok(serde_json::Value::Array(converted))
        }
        rmpv::Value::Map(entries) => {
            let mut map = serde_json::Map::new();
            for (key, value) in entries {
                let Some(text_key) = key.as_str() else {
                    return Err("contains non-string map key");
                };
                map.insert(text_key.to_owned(), rmpv_value_to_json(value)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        _ => Err("contains unsupported value type"),
    }
}
