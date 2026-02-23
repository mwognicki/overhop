use std::fmt;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::orchestrator::queues::QueueConfig;
use crate::wire::codec::{CodecError, WireCodec};
use crate::wire::envelope::{EnvelopeError, WireEnvelope};
use crate::wire::handshake::{process_client_handshake_frame, HELLO_MESSAGE_TYPE};

pub const REGISTER_MESSAGE_TYPE: i64 = 2;
pub const PING_MESSAGE_TYPE: i64 = 3;
pub const QUEUE_MESSAGE_TYPE: i64 = 4;
pub const LSQUEUE_MESSAGE_TYPE: i64 = 5;
pub const SUBSCRIBE_MESSAGE_TYPE: i64 = 6;
pub const UNSUBSCRIBE_MESSAGE_TYPE: i64 = 7;
pub const CREDIT_MESSAGE_TYPE: i64 = 8;
pub const ADDQUEUE_MESSAGE_TYPE: i64 = 9;
pub const STATUS_MESSAGE_TYPE: i64 = 10;
pub const RMQUEUE_MESSAGE_TYPE: i64 = 11;
pub const PAUSE_MESSAGE_TYPE: i64 = 12;
pub const RESUME_MESSAGE_TYPE: i64 = 13;
pub const ENQUEUE_MESSAGE_TYPE: i64 = 14;
pub const JOB_MESSAGE_TYPE: i64 = 15;
pub const RMJOB_MESSAGE_TYPE: i64 = 16;
pub const IDENT_MESSAGE_TYPE: i64 = 104;
pub const PONG_MESSAGE_TYPE: i64 = 105;
pub const PROTOCOL_VIOLATION_CODE: &str = "PROTOCOL_VIOLATION";
pub const REGISTER_TIMEOUT_CODE: &str = "REGISTER_TIMEOUT";
pub const CONNECTION_TIMEOUT_CODE: &str = "CONNECTION_TIMEOUT";

#[derive(Debug, PartialEq, Eq)]
pub enum AnonymousProtocolAction {
    HelloAccepted { response_frame: Vec<u8> },
    RegisterRequested { request_id: String },
}

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

#[derive(Debug)]
pub enum SessionError {
    Codec(CodecError),
    Envelope(EnvelopeError),
    ProtocolViolation {
        request_id: Option<String>,
        code: String,
        message: String,
    },
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(source) => write!(f, "session codec error: {source}"),
            Self::Envelope(source) => write!(f, "session envelope error: {source}"),
            Self::ProtocolViolation {
                request_id,
                code,
                message,
            } => write!(
                f,
                "session protocol violation (rid={:?}, code={}): {}",
                request_id, code, message
            ),
        }
    }
}

impl std::error::Error for SessionError {}

pub fn evaluate_anonymous_client_frame(
    codec: &WireCodec,
    frame: &[u8],
    helloed: bool,
) -> Result<AnonymousProtocolAction, SessionError> {
    let raw = codec.decode_frame(frame).map_err(SessionError::Codec)?;
    let envelope = WireEnvelope::from_raw(&raw).map_err(SessionError::Envelope)?;
    envelope
        .validate_client_to_server()
        .map_err(SessionError::Envelope)?;

    if !helloed {
        if envelope.message_type != HELLO_MESSAGE_TYPE {
            return Err(SessionError::ProtocolViolation {
                request_id: Some(envelope.request_id),
                code: PROTOCOL_VIOLATION_CODE.to_owned(),
                message: "HELLO must be the first message on a new connection".to_owned(),
            });
        }

        let Some(response_frame) =
            process_client_handshake_frame(codec, frame).map_err(to_protocol_if_handshake_error)?
        else {
            return Err(SessionError::ProtocolViolation {
                request_id: Some(envelope.request_id),
                code: PROTOCOL_VIOLATION_CODE.to_owned(),
                message: "HELLO handshake did not produce HI response".to_owned(),
            });
        };

        return Ok(AnonymousProtocolAction::HelloAccepted { response_frame });
    }

    if envelope.message_type != REGISTER_MESSAGE_TYPE {
        return Err(SessionError::ProtocolViolation {
            request_id: Some(envelope.request_id),
            code: PROTOCOL_VIOLATION_CODE.to_owned(),
            message: "after HELLO, only REGISTER is currently allowed".to_owned(),
        });
    }

    if !envelope.payload.is_empty() {
        return Err(SessionError::ProtocolViolation {
            request_id: Some(envelope.request_id),
            code: PROTOCOL_VIOLATION_CODE.to_owned(),
            message: "REGISTER payload must be an empty map".to_owned(),
        });
    }

    Ok(AnonymousProtocolAction::RegisterRequested {
        request_id: envelope.request_id,
    })
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

                    Some(QueueConfig {
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
                "registered workers can currently send only PING, QUEUE, LSQUEUE, SUBSCRIBE, UNSUBSCRIBE, CREDIT, ADDQUEUE, RMQUEUE, PAUSE, RESUME, ENQUEUE, JOB, RMJOB, or STATUS"
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

pub fn build_protocol_error_frame(
    codec: &WireCodec,
    request_id: &str,
    code: &str,
    message: &str,
) -> Result<Vec<u8>, SessionError> {
    codec
        .encode_frame(
            &WireEnvelope::err(
                request_id,
                code,
                Some(message.to_owned()),
                None,
            )
            .into_raw(),
        )
        .map_err(SessionError::Codec)
}

pub fn build_ident_frame(
    codec: &WireCodec,
    register_timeout_seconds: u64,
    reply_deadline_rfc3339: &str,
) -> Result<Vec<u8>, SessionError> {
    let mut payload = crate::wire::envelope::PayloadMap::new();
    payload.insert(
        "register_timeout_seconds".to_owned(),
        rmpv::Value::Integer((register_timeout_seconds as i64).into()),
    );
    payload.insert(
        "reply_deadline".to_owned(),
        rmpv::Value::String(reply_deadline_rfc3339.into()),
    );

    codec
        .encode_frame(
            &WireEnvelope::new(
                IDENT_MESSAGE_TYPE,
                crate::wire::envelope::SERVER_PUSH_REQUEST_ID,
                payload,
            )
            .into_raw(),
        )
        .map_err(SessionError::Codec)
}

pub fn build_pong_frame(
    codec: &WireCodec,
    request_id: &str,
    server_time_rfc3339: &str,
) -> Result<Vec<u8>, SessionError> {
    let mut payload = crate::wire::envelope::PayloadMap::new();
    payload.insert(
        "server_time".to_owned(),
        rmpv::Value::String(server_time_rfc3339.into()),
    );

    codec
        .encode_frame(
            &WireEnvelope::new(PONG_MESSAGE_TYPE, request_id, payload).into_raw(),
        )
        .map_err(SessionError::Codec)
}

fn to_protocol_if_handshake_error(
    source: crate::wire::handshake::HandshakeError,
) -> SessionError {
    match source {
        crate::wire::handshake::HandshakeError::Codec(inner) => SessionError::Codec(inner),
        crate::wire::handshake::HandshakeError::Envelope(inner) => SessionError::Envelope(inner),
    }
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

#[cfg(test)]
mod tests {
    use crate::wire::codec::CodecConfig;
    use crate::wire::envelope::PayloadMap;
    use uuid::Uuid;

    use super::{
        ADDQUEUE_MESSAGE_TYPE, AnonymousProtocolAction, CREDIT_MESSAGE_TYPE, ENQUEUE_MESSAGE_TYPE,
        IDENT_MESSAGE_TYPE, JOB_MESSAGE_TYPE, LSQUEUE_MESSAGE_TYPE, PAUSE_MESSAGE_TYPE,
        PROTOCOL_VIOLATION_CODE, QUEUE_MESSAGE_TYPE, RESUME_MESSAGE_TYPE, RMQUEUE_MESSAGE_TYPE,
        RMJOB_MESSAGE_TYPE, SUBSCRIBE_MESSAGE_TYPE, PING_MESSAGE_TYPE, PONG_MESSAGE_TYPE,
        REGISTER_MESSAGE_TYPE, STATUS_MESSAGE_TYPE, WorkerProtocolAction,
        UNSUBSCRIBE_MESSAGE_TYPE,
        build_ident_frame, build_pong_frame, build_protocol_error_frame,
        evaluate_anonymous_client_frame, evaluate_worker_client_frame,
    };

    #[test]
    fn first_message_must_be_hello() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let register = crate::wire::envelope::WireEnvelope::new(
            REGISTER_MESSAGE_TYPE,
            "rid-1",
            PayloadMap::new(),
        );
        let frame = codec
            .encode_frame(&register.into_raw())
            .expect("register should encode");

        let err = evaluate_anonymous_client_frame(&codec, &frame, false)
            .expect_err("register before hello should fail");
        assert!(matches!(
            err,
            super::SessionError::ProtocolViolation {
                code,
                request_id: Some(_),
                ..
            } if code == PROTOCOL_VIOLATION_CODE
        ));
    }

    #[test]
    fn hello_then_register_path_is_allowed() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let hello = crate::wire::envelope::WireEnvelope::new(
            crate::wire::handshake::HELLO_MESSAGE_TYPE,
            "rid-h",
            PayloadMap::new(),
        );
        let hello_frame = codec
            .encode_frame(&hello.into_raw())
            .expect("hello should encode");

        let hello_action =
            evaluate_anonymous_client_frame(&codec, &hello_frame, false).expect("hello should pass");
        assert!(matches!(
            hello_action,
            AnonymousProtocolAction::HelloAccepted { .. }
        ));

        let register = crate::wire::envelope::WireEnvelope::new(
            REGISTER_MESSAGE_TYPE,
            "rid-r",
            PayloadMap::new(),
        );
        let register_frame = codec
            .encode_frame(&register.into_raw())
            .expect("register should encode");

        let register_action = evaluate_anonymous_client_frame(&codec, &register_frame, true)
            .expect("register should pass after hello");
        assert_eq!(
            register_action,
            AnonymousProtocolAction::RegisterRequested {
                request_id: "rid-r".to_owned()
            }
        );
    }

    #[test]
    fn register_requires_empty_payload() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let mut payload = PayloadMap::new();
        payload.insert("x".to_owned(), rmpv::Value::Boolean(true));
        let register =
            crate::wire::envelope::WireEnvelope::new(REGISTER_MESSAGE_TYPE, "rid-x", payload);
        let frame = codec
            .encode_frame(&register.into_raw())
            .expect("register should encode");

        let err = evaluate_anonymous_client_frame(&codec, &frame, true)
            .expect_err("register payload should be empty");
        assert!(matches!(err, super::SessionError::ProtocolViolation { .. }));
    }

    #[test]
    fn can_build_protocol_error_frame() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let frame = build_protocol_error_frame(
            &codec,
            "rid-e",
            PROTOCOL_VIOLATION_CODE,
            "bad sequence",
        )
        .expect("error frame should build");

        let decoded = codec.decode_frame(&frame).expect("frame should decode");
        let envelope =
            crate::wire::envelope::WireEnvelope::from_raw(&decoded).expect("envelope parse");
        assert_eq!(envelope.message_type, crate::wire::envelope::SERVER_ERR_MESSAGE_TYPE);
        assert_eq!(envelope.request_id, "rid-e");
        assert_eq!(
            envelope.payload.get("code"),
            Some(&rmpv::Value::String(PROTOCOL_VIOLATION_CODE.into()))
        );
    }

    #[test]
    fn can_build_ident_frame_with_deadline_payload() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let frame = build_ident_frame(&codec, 2, "2026-02-22T12:00:00.000Z")
            .expect("ident frame should build");

        let decoded = codec.decode_frame(&frame).expect("frame should decode");
        let envelope =
            crate::wire::envelope::WireEnvelope::from_raw(&decoded).expect("envelope parse");
        assert_eq!(envelope.message_type, IDENT_MESSAGE_TYPE);
        assert_eq!(
            envelope.payload.get("register_timeout_seconds"),
            Some(&rmpv::Value::Integer(2_i64.into()))
        );
        assert_eq!(
            envelope.payload.get("reply_deadline"),
            Some(&rmpv::Value::String("2026-02-22T12:00:00.000Z".into()))
        );
    }

    #[test]
    fn worker_ping_is_accepted_and_can_build_pong() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let ping = crate::wire::envelope::WireEnvelope::new(
            PING_MESSAGE_TYPE,
            "rid-ping",
            PayloadMap::new(),
        );
        let ping_frame = codec
            .encode_frame(&ping.into_raw())
            .expect("ping should encode");

        let action =
            evaluate_worker_client_frame(&codec, &ping_frame).expect("ping should be accepted");
        assert_eq!(
            action,
            WorkerProtocolAction::PingRequested {
                request_id: "rid-ping".to_owned()
            }
        );

        let pong_frame = build_pong_frame(&codec, "rid-ping", "2026-02-22T12:00:00.000Z")
            .expect("pong frame should build");
        let decoded = codec.decode_frame(&pong_frame).expect("decode pong frame");
        let envelope = crate::wire::envelope::WireEnvelope::from_raw(&decoded)
            .expect("parse pong envelope");
        assert_eq!(envelope.message_type, PONG_MESSAGE_TYPE);
        assert_eq!(envelope.request_id, "rid-ping");
        assert_eq!(
            envelope.payload.get("server_time"),
            Some(&rmpv::Value::String("2026-02-22T12:00:00.000Z".into()))
        );
    }

    #[test]
    fn worker_ping_requires_empty_payload() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let mut payload = PayloadMap::new();
        payload.insert("x".to_owned(), rmpv::Value::Boolean(true));
        let ping = crate::wire::envelope::WireEnvelope::new(PING_MESSAGE_TYPE, "rid-p", payload);
        let frame = codec
            .encode_frame(&ping.into_raw())
            .expect("ping should encode");

        let err =
            evaluate_worker_client_frame(&codec, &frame).expect_err("ping payload must be empty");
        assert!(matches!(err, super::SessionError::ProtocolViolation { .. }));
    }

    #[test]
    fn worker_queue_requires_q_string_and_accepts_valid_payload() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let mut payload = PayloadMap::new();
        payload.insert("q".to_owned(), rmpv::Value::String("critical".into()));
        let queue = crate::wire::envelope::WireEnvelope::new(QUEUE_MESSAGE_TYPE, "rid-g", payload);
        let frame = codec
            .encode_frame(&queue.into_raw())
            .expect("queue should encode");

        let action =
            evaluate_worker_client_frame(&codec, &frame).expect("queue should be accepted");
        assert_eq!(
            action,
            WorkerProtocolAction::QueueRequested {
                request_id: "rid-g".to_owned(),
                queue_name: "critical".to_owned()
            }
        );
    }

    #[test]
    fn worker_lsqueue_requires_empty_payload() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let empty = crate::wire::envelope::WireEnvelope::new(
            LSQUEUE_MESSAGE_TYPE,
            "rid-l",
            PayloadMap::new(),
        );
        let empty_frame = codec
            .encode_frame(&empty.into_raw())
            .expect("lsqueue should encode");

        let action =
            evaluate_worker_client_frame(&codec, &empty_frame).expect("lsqueue should pass");
        assert_eq!(
            action,
            WorkerProtocolAction::LsQueueRequested {
                request_id: "rid-l".to_owned()
            }
        );

        let mut payload = PayloadMap::new();
        payload.insert("unexpected".to_owned(), rmpv::Value::Boolean(true));
        let nonempty =
            crate::wire::envelope::WireEnvelope::new(LSQUEUE_MESSAGE_TYPE, "rid-bad", payload);
        let nonempty_frame = codec
            .encode_frame(&nonempty.into_raw())
            .expect("lsqueue should encode");
        let err = evaluate_worker_client_frame(&codec, &nonempty_frame)
            .expect_err("non-empty lsqueue payload should fail");
        assert!(matches!(err, super::SessionError::ProtocolViolation { .. }));
    }

    #[test]
    fn worker_addqueue_accepts_name_and_optional_config() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let mut config = PayloadMap::new();
        config.insert(
            "concurrency_limit".to_owned(),
            rmpv::Value::Integer(4_i64.into()),
        );
        config.insert(
            "allow_job_overrides".to_owned(),
            rmpv::Value::Boolean(false),
        );
        let mut payload = PayloadMap::new();
        payload.insert("name".to_owned(), rmpv::Value::String("bulk".into()));
        payload.insert(
            "config".to_owned(),
            rmpv::Value::Map(
                config
                    .into_iter()
                    .map(|(k, v)| (rmpv::Value::String(k.into()), v))
                    .collect(),
            ),
        );

        let addqueue =
            crate::wire::envelope::WireEnvelope::new(ADDQUEUE_MESSAGE_TYPE, "rid-a", payload);
        let frame = codec
            .encode_frame(&addqueue.into_raw())
            .expect("addqueue should encode");

        let action =
            evaluate_worker_client_frame(&codec, &frame).expect("addqueue should be accepted");
        assert_eq!(
            action,
            WorkerProtocolAction::AddQueueRequested {
                request_id: "rid-a".to_owned(),
                queue_name: "bulk".to_owned(),
                config: Some(crate::orchestrator::queues::QueueConfig {
                    concurrency_limit: Some(4),
                    allow_job_overrides: false,
                }),
            }
        );
    }

    #[test]
    fn worker_subscribe_requires_q_and_accepts_optional_credits() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let mut payload = PayloadMap::new();
        payload.insert("q".to_owned(), rmpv::Value::String("critical".into()));
        payload.insert("credits".to_owned(), rmpv::Value::Integer(3_i64.into()));
        let subscribe =
            crate::wire::envelope::WireEnvelope::new(SUBSCRIBE_MESSAGE_TYPE, "rid-s", payload);
        let frame = codec
            .encode_frame(&subscribe.into_raw())
            .expect("subscribe should encode");

        let action =
            evaluate_worker_client_frame(&codec, &frame).expect("subscribe should be accepted");
        assert_eq!(
            action,
            WorkerProtocolAction::SubscribeRequested {
                request_id: "rid-s".to_owned(),
                queue_name: "critical".to_owned(),
                credits: 3,
            }
        );

        let mut default_payload = PayloadMap::new();
        default_payload.insert("q".to_owned(), rmpv::Value::String("bulk".into()));
        let subscribe_default = crate::wire::envelope::WireEnvelope::new(
            SUBSCRIBE_MESSAGE_TYPE,
            "rid-s-default",
            default_payload,
        );
        let frame_default = codec
            .encode_frame(&subscribe_default.into_raw())
            .expect("subscribe should encode");
        let action_default = evaluate_worker_client_frame(&codec, &frame_default)
            .expect("subscribe should accept missing credits");
        assert_eq!(
            action_default,
            WorkerProtocolAction::SubscribeRequested {
                request_id: "rid-s-default".to_owned(),
                queue_name: "bulk".to_owned(),
                credits: 0,
            }
        );
    }

    #[test]
    fn worker_subscribe_rejects_invalid_credits() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let mut payload = PayloadMap::new();
        payload.insert("q".to_owned(), rmpv::Value::String("critical".into()));
        payload.insert("credits".to_owned(), rmpv::Value::Integer((-1_i64).into()));
        let subscribe =
            crate::wire::envelope::WireEnvelope::new(SUBSCRIBE_MESSAGE_TYPE, "rid-bad", payload);
        let frame = codec
            .encode_frame(&subscribe.into_raw())
            .expect("subscribe should encode");

        let err = evaluate_worker_client_frame(&codec, &frame)
            .expect_err("negative credits should fail");
        assert!(matches!(err, super::SessionError::ProtocolViolation { .. }));
    }

    #[test]
    fn worker_rmqueue_requires_q_string_and_accepts_valid_payload() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let mut payload = PayloadMap::new();
        payload.insert("q".to_owned(), rmpv::Value::String("critical".into()));
        let rmqueue =
            crate::wire::envelope::WireEnvelope::new(RMQUEUE_MESSAGE_TYPE, "rid-rm", payload);
        let frame = codec
            .encode_frame(&rmqueue.into_raw())
            .expect("rmqueue should encode");

        let action =
            evaluate_worker_client_frame(&codec, &frame).expect("rmqueue should be accepted");
        assert_eq!(
            action,
            WorkerProtocolAction::RemoveQueueRequested {
                request_id: "rid-rm".to_owned(),
                queue_name: "critical".to_owned(),
            }
        );

        let missing_q = crate::wire::envelope::WireEnvelope::new(
            RMQUEUE_MESSAGE_TYPE,
            "rid-rm-bad",
            PayloadMap::new(),
        );
        let missing_q_frame = codec
            .encode_frame(&missing_q.into_raw())
            .expect("rmqueue should encode");
        let err = evaluate_worker_client_frame(&codec, &missing_q_frame)
            .expect_err("rmqueue without q should fail");
        assert!(matches!(err, super::SessionError::ProtocolViolation { .. }));
    }

    #[test]
    fn worker_pause_and_resume_require_q_string_and_accept_valid_payload() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());

        let mut pause_payload = PayloadMap::new();
        pause_payload.insert("q".to_owned(), rmpv::Value::String("critical".into()));
        let pause =
            crate::wire::envelope::WireEnvelope::new(PAUSE_MESSAGE_TYPE, "rid-pause", pause_payload);
        let pause_frame = codec
            .encode_frame(&pause.into_raw())
            .expect("pause should encode");
        let pause_action =
            evaluate_worker_client_frame(&codec, &pause_frame).expect("pause should pass");
        assert_eq!(
            pause_action,
            WorkerProtocolAction::PauseQueueRequested {
                request_id: "rid-pause".to_owned(),
                queue_name: "critical".to_owned(),
            }
        );

        let mut resume_payload = PayloadMap::new();
        resume_payload.insert("q".to_owned(), rmpv::Value::String("critical".into()));
        let resume = crate::wire::envelope::WireEnvelope::new(
            RESUME_MESSAGE_TYPE,
            "rid-resume",
            resume_payload,
        );
        let resume_frame = codec
            .encode_frame(&resume.into_raw())
            .expect("resume should encode");
        let resume_action =
            evaluate_worker_client_frame(&codec, &resume_frame).expect("resume should pass");
        assert_eq!(
            resume_action,
            WorkerProtocolAction::ResumeQueueRequested {
                request_id: "rid-resume".to_owned(),
                queue_name: "critical".to_owned(),
            }
        );
    }

    #[test]
    fn worker_enqueue_validates_and_accepts_payload() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let mut payload = PayloadMap::new();
        payload.insert("q".to_owned(), rmpv::Value::String("critical".into()));
        payload.insert(
            "scheduled_at".to_owned(),
            rmpv::Value::String("2026-02-23T12:00:00Z".into()),
        );
        payload.insert("max_attempts".to_owned(), rmpv::Value::Integer(3_i64.into()));
        payload.insert(
            "retry_interval_ms".to_owned(),
            rmpv::Value::Integer(250_i64.into()),
        );
        payload.insert(
            "job_payload".to_owned(),
            rmpv::Value::Map(vec![(
                rmpv::Value::String("op".into()),
                rmpv::Value::String("email".into()),
            )]),
        );

        let enqueue =
            crate::wire::envelope::WireEnvelope::new(ENQUEUE_MESSAGE_TYPE, "rid-enq", payload);
        let frame = codec
            .encode_frame(&enqueue.into_raw())
            .expect("enqueue should encode");
        let action = evaluate_worker_client_frame(&codec, &frame).expect("enqueue should pass");
        assert!(matches!(
            action,
            WorkerProtocolAction::EnqueueRequested {
                request_id,
                queue_name,
                max_attempts: Some(3),
                retry_interval_ms: Some(250),
                ..
            } if request_id == "rid-enq" && queue_name == "critical"
        ));
    }

    #[test]
    fn worker_job_requires_jid_string() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let mut payload = PayloadMap::new();
        payload.insert(
            "jid".to_owned(),
            rmpv::Value::String("critical:123e4567-e89b-12d3-a456-426614174000".into()),
        );
        let job = crate::wire::envelope::WireEnvelope::new(JOB_MESSAGE_TYPE, "rid-job", payload);
        let frame = codec
            .encode_frame(&job.into_raw())
            .expect("job should encode");

        let action = evaluate_worker_client_frame(&codec, &frame).expect("job should pass");
        assert_eq!(
            action,
            WorkerProtocolAction::JobRequested {
                request_id: "rid-job".to_owned(),
                job_id: "critical:123e4567-e89b-12d3-a456-426614174000".to_owned(),
            }
        );

        let bad = crate::wire::envelope::WireEnvelope::new(
            JOB_MESSAGE_TYPE,
            "rid-bad-job",
            PayloadMap::new(),
        );
        let bad_frame = codec
            .encode_frame(&bad.into_raw())
            .expect("job should encode");
        let err = evaluate_worker_client_frame(&codec, &bad_frame)
            .expect_err("job without jid should fail");
        assert!(matches!(err, super::SessionError::ProtocolViolation { .. }));
    }

    #[test]
    fn worker_rmjob_requires_jid_string() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let mut payload = PayloadMap::new();
        payload.insert(
            "jid".to_owned(),
            rmpv::Value::String("critical:123e4567-e89b-12d3-a456-426614174000".into()),
        );
        let rmjob =
            crate::wire::envelope::WireEnvelope::new(RMJOB_MESSAGE_TYPE, "rid-rmjob", payload);
        let frame = codec
            .encode_frame(&rmjob.into_raw())
            .expect("rmjob should encode");

        let action = evaluate_worker_client_frame(&codec, &frame).expect("rmjob should pass");
        assert_eq!(
            action,
            WorkerProtocolAction::RemoveJobRequested {
                request_id: "rid-rmjob".to_owned(),
                job_id: "critical:123e4567-e89b-12d3-a456-426614174000".to_owned(),
            }
        );

        let bad = crate::wire::envelope::WireEnvelope::new(
            RMJOB_MESSAGE_TYPE,
            "rid-bad-rmjob",
            PayloadMap::new(),
        );
        let bad_frame = codec
            .encode_frame(&bad.into_raw())
            .expect("rmjob should encode");
        let err = evaluate_worker_client_frame(&codec, &bad_frame)
            .expect_err("rmjob without jid should fail");
        assert!(matches!(err, super::SessionError::ProtocolViolation { .. }));
    }

    #[test]
    fn worker_unsubscribe_requires_uuid_sid() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let sid = Uuid::new_v4();
        let mut payload = PayloadMap::new();
        payload.insert("sid".to_owned(), rmpv::Value::String(sid.to_string().into()));
        let unsubscribe =
            crate::wire::envelope::WireEnvelope::new(UNSUBSCRIBE_MESSAGE_TYPE, "rid-u", payload);
        let frame = codec
            .encode_frame(&unsubscribe.into_raw())
            .expect("unsubscribe should encode");

        let action =
            evaluate_worker_client_frame(&codec, &frame).expect("unsubscribe should be accepted");
        assert_eq!(
            action,
            WorkerProtocolAction::UnsubscribeRequested {
                request_id: "rid-u".to_owned(),
                subscription_id: sid,
            }
        );
    }

    #[test]
    fn worker_credit_requires_sid_and_positive_credits() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let sid = Uuid::new_v4();
        let mut payload = PayloadMap::new();
        payload.insert("sid".to_owned(), rmpv::Value::String(sid.to_string().into()));
        payload.insert("credits".to_owned(), rmpv::Value::Integer(5_i64.into()));
        let credit = crate::wire::envelope::WireEnvelope::new(CREDIT_MESSAGE_TYPE, "rid-c", payload);
        let frame = codec
            .encode_frame(&credit.into_raw())
            .expect("credit should encode");

        let action = evaluate_worker_client_frame(&codec, &frame).expect("credit should pass");
        assert_eq!(
            action,
            WorkerProtocolAction::CreditRequested {
                request_id: "rid-c".to_owned(),
                subscription_id: sid,
                credits: 5,
            }
        );
    }

    #[test]
    fn worker_credit_rejects_non_positive_credits() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let sid = Uuid::new_v4();
        let mut payload = PayloadMap::new();
        payload.insert("sid".to_owned(), rmpv::Value::String(sid.to_string().into()));
        payload.insert("credits".to_owned(), rmpv::Value::Integer(0_i64.into()));
        let credit = crate::wire::envelope::WireEnvelope::new(CREDIT_MESSAGE_TYPE, "rid-c0", payload);
        let frame = codec
            .encode_frame(&credit.into_raw())
            .expect("credit should encode");

        let err = evaluate_worker_client_frame(&codec, &frame)
            .expect_err("zero credit should be rejected");
        assert!(matches!(err, super::SessionError::ProtocolViolation { .. }));
    }

    #[test]
    fn worker_status_requires_empty_payload() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let status = crate::wire::envelope::WireEnvelope::new(
            STATUS_MESSAGE_TYPE,
            "rid-status",
            PayloadMap::new(),
        );
        let status_frame = codec
            .encode_frame(&status.into_raw())
            .expect("status should encode");
        let action =
            evaluate_worker_client_frame(&codec, &status_frame).expect("status should pass");
        assert_eq!(
            action,
            WorkerProtocolAction::StatusRequested {
                request_id: "rid-status".to_owned(),
            }
        );

        let mut payload = PayloadMap::new();
        payload.insert("unexpected".to_owned(), rmpv::Value::Boolean(true));
        let bad_status =
            crate::wire::envelope::WireEnvelope::new(STATUS_MESSAGE_TYPE, "rid-bad", payload);
        let bad_status_frame = codec
            .encode_frame(&bad_status.into_raw())
            .expect("status should encode");
        let err = evaluate_worker_client_frame(&codec, &bad_status_frame)
            .expect_err("non-empty status payload should fail");
        assert!(matches!(err, super::SessionError::ProtocolViolation { .. }));
    }
}
