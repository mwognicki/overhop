use std::collections::BTreeMap;
use std::fmt;

use rmpv::Value;

use crate::wire::codec::MessageEnvelope;

pub const PROTOCOL_VERSION: i64 = 2;
pub const SERVER_PUSH_REQUEST_ID: &str = "0";
pub const NOT_IMPLEMENTED_CODE: &str = "NOT_IMPLEMENTED";

pub type PayloadMap = BTreeMap<String, Value>;

#[derive(Clone, Debug, PartialEq)]
pub struct WireEnvelope {
    pub version: i64,
    pub message_type: i64,
    pub request_id: String,
    pub payload: PayloadMap,
}

#[derive(Debug, PartialEq)]
pub enum EnvelopeError {
    MissingField { field: &'static str },
    InvalidFieldType { field: &'static str, expected: &'static str },
    InvalidProtocolVersion { expected: i64, actual: i64 },
    IntegerOutOfRange { field: &'static str },
    PayloadMapKeyMustBeUtf8String,
    RequestIdRequired,
    RequestIdMustBeServerPush,
    RequestIdMismatch { expected: String, actual: String },
}

impl fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingField { field } => write!(f, "missing envelope field '{field}'"),
            Self::InvalidFieldType { field, expected } => {
                write!(f, "invalid field type for '{field}', expected {expected}")
            }
            Self::InvalidProtocolVersion { expected, actual } => {
                write!(f, "invalid protocol version {actual}, expected {expected}")
            }
            Self::IntegerOutOfRange { field } => {
                write!(f, "integer field '{field}' must fit signed int64")
            }
            Self::PayloadMapKeyMustBeUtf8String => {
                write!(f, "payload map keys must be UTF-8 strings")
            }
            Self::RequestIdRequired => write!(f, "request id must be non-empty"),
            Self::RequestIdMustBeServerPush => {
                write!(f, "server push envelopes must use rid='0'")
            }
            Self::RequestIdMismatch { expected, actual } => write!(
                f,
                "server response rid mismatch, expected '{expected}', actual '{actual}'"
            ),
        }
    }
}

impl std::error::Error for EnvelopeError {}

impl WireEnvelope {
    pub fn new(message_type: i64, request_id: impl Into<String>, payload: PayloadMap) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            message_type,
            request_id: request_id.into(),
            payload,
        }
    }

    pub fn from_raw(raw: &MessageEnvelope) -> Result<Self, EnvelopeError> {
        let version = parse_i64(raw.get("v"), "v")?;
        if version != PROTOCOL_VERSION {
            return Err(EnvelopeError::InvalidProtocolVersion {
                expected: PROTOCOL_VERSION,
                actual: version,
            });
        }

        let message_type = parse_i64(raw.get("t"), "t")?;
        let request_id = parse_string(raw.get("rid"), "rid")?;
        let payload = parse_payload_map(raw.get("p"))?;

        Ok(Self {
            version,
            message_type,
            request_id,
            payload,
        })
    }

    pub fn into_raw(self) -> MessageEnvelope {
        let mut raw = MessageEnvelope::new();
        raw.insert("v".to_owned(), Value::Integer(self.version.into()));
        raw.insert("t".to_owned(), Value::Integer(self.message_type.into()));
        raw.insert("rid".to_owned(), Value::String(self.request_id.into()));

        let payload_map = self
            .payload
            .into_iter()
            .map(|(k, v)| (Value::String(k.into()), v))
            .collect::<Vec<_>>();
        raw.insert("p".to_owned(), Value::Map(payload_map));

        raw
    }

    pub fn validate_client_to_server(&self) -> Result<(), EnvelopeError> {
        if self.request_id.is_empty() {
            return Err(EnvelopeError::RequestIdRequired);
        }

        Ok(())
    }

    pub fn validate_server_push(&self) -> Result<(), EnvelopeError> {
        if self.request_id != SERVER_PUSH_REQUEST_ID {
            return Err(EnvelopeError::RequestIdMustBeServerPush);
        }

        Ok(())
    }

    pub fn validate_server_response(&self, expected_request_id: &str) -> Result<(), EnvelopeError> {
        if self.request_id != expected_request_id {
            return Err(EnvelopeError::RequestIdMismatch {
                expected: expected_request_id.to_owned(),
                actual: self.request_id.clone(),
            });
        }

        Ok(())
    }

    pub fn not_implemented_error(
        request_id: impl Into<String>,
        unsupported_type: i64,
        error_message_type: i64,
    ) -> Self {
        let mut payload = PayloadMap::new();
        payload.insert("code".to_owned(), Value::String(NOT_IMPLEMENTED_CODE.into()));
        payload.insert(
            "message".to_owned(),
            Value::String("message type is not implemented".into()),
        );
        payload.insert(
            "unsupported_t".to_owned(),
            Value::Integer(unsupported_type.into()),
        );

        Self {
            version: PROTOCOL_VERSION,
            message_type: error_message_type,
            request_id: request_id.into(),
            payload,
        }
    }
}

fn parse_i64(value: Option<&Value>, field: &'static str) -> Result<i64, EnvelopeError> {
    let value = value.ok_or(EnvelopeError::MissingField { field })?;
    let Value::Integer(integer) = value else {
        return Err(EnvelopeError::InvalidFieldType {
            field,
            expected: "int",
        });
    };

    if let Some(v) = integer.as_i64() {
        return Ok(v);
    }

    match integer.as_u64() {
        Some(v) if v <= i64::MAX as u64 => Ok(v as i64),
        _ => Err(EnvelopeError::IntegerOutOfRange { field }),
    }
}

fn parse_string(value: Option<&Value>, field: &'static str) -> Result<String, EnvelopeError> {
    let value = value.ok_or(EnvelopeError::MissingField { field })?;
    let Value::String(text) = value else {
        return Err(EnvelopeError::InvalidFieldType {
            field,
            expected: "string",
        });
    };

    let Some(text) = text.as_str() else {
        return Err(EnvelopeError::InvalidFieldType {
            field,
            expected: "string",
        });
    };

    Ok(text.to_owned())
}

fn parse_payload_map(value: Option<&Value>) -> Result<PayloadMap, EnvelopeError> {
    let value = value.ok_or(EnvelopeError::MissingField { field: "p" })?;
    let Value::Map(entries) = value else {
        return Err(EnvelopeError::InvalidFieldType {
            field: "p",
            expected: "map",
        });
    };

    let mut payload = PayloadMap::new();
    for (key, value) in entries {
        let Value::String(text) = key else {
            return Err(EnvelopeError::PayloadMapKeyMustBeUtf8String);
        };
        let Some(text) = text.as_str() else {
            return Err(EnvelopeError::PayloadMapKeyMustBeUtf8String);
        };

        payload.insert(text.to_owned(), value.clone());
    }

    Ok(payload)
}

#[cfg(test)]
mod tests {
    use rmpv::Value;

    use super::{
        EnvelopeError, PayloadMap, WireEnvelope, NOT_IMPLEMENTED_CODE, PROTOCOL_VERSION,
        SERVER_PUSH_REQUEST_ID,
    };

    fn valid_raw() -> crate::wire::codec::MessageEnvelope {
        let mut raw = crate::wire::codec::MessageEnvelope::new();
        raw.insert("v".to_owned(), Value::Integer(PROTOCOL_VERSION.into()));
        raw.insert("t".to_owned(), Value::Integer(10.into()));
        raw.insert("rid".to_owned(), Value::String("abc-123".into()));
        raw.insert(
            "p".to_owned(),
            Value::Map(vec![(Value::String("x".into()), Value::Boolean(true))]),
        );
        raw
    }

    #[test]
    fn parses_valid_envelope_and_ignores_unknown_fields() {
        let mut raw = valid_raw();
        raw.insert("unknown".to_owned(), Value::String("ignored".into()));

        let envelope = WireEnvelope::from_raw(&raw).expect("envelope should parse");
        assert_eq!(envelope.version, PROTOCOL_VERSION);
        assert_eq!(envelope.message_type, 10);
        assert_eq!(envelope.request_id, "abc-123");
        assert_eq!(envelope.payload.get("x"), Some(&Value::Boolean(true)));
    }

    #[test]
    fn rejects_invalid_protocol_version() {
        let mut raw = valid_raw();
        raw.insert("v".to_owned(), Value::Integer(99.into()));

        let err = WireEnvelope::from_raw(&raw).expect_err("version should fail");
        assert!(matches!(err, EnvelopeError::InvalidProtocolVersion { .. }));
    }

    #[test]
    fn rejects_missing_rid() {
        let mut raw = valid_raw();
        raw.remove("rid");

        let err = WireEnvelope::from_raw(&raw).expect_err("rid is required");
        assert!(matches!(
            err,
            EnvelopeError::MissingField { field: "rid" }
        ));
    }

    #[test]
    fn client_to_server_requires_non_empty_rid() {
        let envelope = WireEnvelope::new(1, "", PayloadMap::new());
        let err = envelope
            .validate_client_to_server()
            .expect_err("empty rid should fail");
        assert!(matches!(err, EnvelopeError::RequestIdRequired));
    }

    #[test]
    fn server_push_requires_zero_rid() {
        let envelope = WireEnvelope::new(1, "not-zero", PayloadMap::new());
        let err = envelope
            .validate_server_push()
            .expect_err("push rid should be zero");
        assert!(matches!(err, EnvelopeError::RequestIdMustBeServerPush));

        let push = WireEnvelope::new(1, SERVER_PUSH_REQUEST_ID, PayloadMap::new());
        assert!(push.validate_server_push().is_ok());
    }

    #[test]
    fn server_response_must_echo_rid() {
        let envelope = WireEnvelope::new(1, "rid-1", PayloadMap::new());

        let err = envelope
            .validate_server_response("rid-2")
            .expect_err("rid mismatch should fail");
        assert!(matches!(err, EnvelopeError::RequestIdMismatch { .. }));

        assert!(envelope.validate_server_response("rid-1").is_ok());
    }

    #[test]
    fn builds_not_implemented_error_envelope() {
        let envelope = WireEnvelope::not_implemented_error("abc", 777, 999);

        assert_eq!(envelope.version, PROTOCOL_VERSION);
        assert_eq!(envelope.message_type, 999);
        assert_eq!(envelope.request_id, "abc");
        assert_eq!(
            envelope.payload.get("code"),
            Some(&Value::String(NOT_IMPLEMENTED_CODE.into()))
        );
        assert_eq!(
            envelope.payload.get("unsupported_t"),
            Some(&Value::Integer(777.into()))
        );
    }

    #[test]
    fn round_trip_raw_conversion() {
        let mut payload = PayloadMap::new();
        payload.insert("nested".to_owned(), Value::Array(vec![Value::Integer(1.into())]));

        let envelope = WireEnvelope::new(42, "rid-42", payload);
        let raw = envelope.clone().into_raw();
        let decoded = WireEnvelope::from_raw(&raw).expect("envelope should decode");

        assert_eq!(decoded, envelope);
    }
}
