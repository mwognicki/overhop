use std::fmt;

use crate::wire::codec::{CodecError, WireCodec};
use crate::wire::envelope::{EnvelopeError, WireEnvelope};
use crate::wire::handshake::{process_client_handshake_frame, HELLO_MESSAGE_TYPE};

pub const REGISTER_MESSAGE_TYPE: i64 = 2;
pub const PING_MESSAGE_TYPE: i64 = 3;
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
        return Err(SessionError::ProtocolViolation {
            request_id: Some(envelope.request_id),
            code: PROTOCOL_VIOLATION_CODE.to_owned(),
            message: "registered workers can currently send only PING".to_owned(),
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

#[cfg(test)]
mod tests {
    use crate::wire::codec::CodecConfig;
    use crate::wire::envelope::PayloadMap;

    use super::{
        AnonymousProtocolAction, IDENT_MESSAGE_TYPE, PROTOCOL_VIOLATION_CODE,
        PING_MESSAGE_TYPE, PONG_MESSAGE_TYPE, REGISTER_MESSAGE_TYPE, WorkerProtocolAction,
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
}
