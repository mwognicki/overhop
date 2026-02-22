use std::collections::BTreeMap;
use std::fmt;
use std::io::Cursor;

use rmpv::{Integer, Value};

use crate::config;

pub const DEFAULT_MAX_ENVELOPE_SIZE_BYTES: usize = 8 * 1024 * 1024;
pub const MIN_MAX_ENVELOPE_SIZE_BYTES: usize = 64 * 1024;
pub const MAX_MAX_ENVELOPE_SIZE_BYTES: usize = 32 * 1024 * 1024;
pub const FRAME_HEADER_SIZE_BYTES: usize = 4;

pub type MessageEnvelope = BTreeMap<String, Value>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CodecConfig {
    pub max_envelope_size_bytes: usize,
}

impl Default for CodecConfig {
    fn default() -> Self {
        Self {
            max_envelope_size_bytes: DEFAULT_MAX_ENVELOPE_SIZE_BYTES,
        }
    }
}

impl TryFrom<config::WireConfig> for CodecConfig {
    type Error = CodecError;

    fn try_from(value: config::WireConfig) -> Result<Self, Self::Error> {
        let raw = usize::try_from(value.max_envelope_size_bytes).map_err(|_| {
            CodecError::InvalidConfiguredLimit {
                value: value.max_envelope_size_bytes,
                min: MIN_MAX_ENVELOPE_SIZE_BYTES,
                max: MAX_MAX_ENVELOPE_SIZE_BYTES,
            }
        })?;

        Self::new(raw)
    }
}

impl CodecConfig {
    pub fn new(max_envelope_size_bytes: usize) -> Result<Self, CodecError> {
        if !(MIN_MAX_ENVELOPE_SIZE_BYTES..=MAX_MAX_ENVELOPE_SIZE_BYTES)
            .contains(&max_envelope_size_bytes)
        {
            return Err(CodecError::InvalidConfiguredLimit {
                value: max_envelope_size_bytes as u64,
                min: MIN_MAX_ENVELOPE_SIZE_BYTES,
                max: MAX_MAX_ENVELOPE_SIZE_BYTES,
            });
        }

        Ok(Self {
            max_envelope_size_bytes,
        })
    }
}

#[derive(Clone, Debug)]
pub struct WireCodec {
    config: CodecConfig,
}

impl Default for WireCodec {
    fn default() -> Self {
        Self {
            config: CodecConfig::default(),
        }
    }
}

impl WireCodec {
    pub fn new(config: CodecConfig) -> Self {
        Self { config }
    }

    pub fn from_app_config(app_config: &config::AppConfig) -> Result<Self, CodecError> {
        let cfg = CodecConfig::try_from(app_config.wire)?;
        Ok(Self::new(cfg))
    }

    pub fn max_envelope_size_bytes(&self) -> usize {
        self.config.max_envelope_size_bytes
    }

    pub fn encode_frame(&self, envelope: &MessageEnvelope) -> Result<Vec<u8>, CodecError> {
        let payload = self.encode_payload(envelope)?;

        if payload.is_empty() {
            return Err(CodecError::ProtocolZeroLength);
        }
        if payload.len() > self.config.max_envelope_size_bytes {
            return Err(CodecError::PayloadTooLarge {
                size: payload.len(),
                limit: self.config.max_envelope_size_bytes,
            });
        }

        let mut frame = Vec::with_capacity(FRAME_HEADER_SIZE_BYTES + payload.len());
        let len = payload.len() as u32;
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&payload);
        Ok(frame)
    }

    pub fn decode_frame(&self, frame: &[u8]) -> Result<MessageEnvelope, CodecError> {
        if frame.len() < FRAME_HEADER_SIZE_BYTES {
            return Err(CodecError::FrameTooShort { size: frame.len() });
        }

        let declared_len = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
        if declared_len == 0 {
            return Err(CodecError::ProtocolZeroLength);
        }
        if declared_len > self.config.max_envelope_size_bytes {
            return Err(CodecError::ProtocolLengthTooLarge {
                length: declared_len,
                limit: self.config.max_envelope_size_bytes,
            });
        }

        let payload = &frame[FRAME_HEADER_SIZE_BYTES..];
        if payload.len() != declared_len {
            return Err(CodecError::FrameLengthMismatch {
                declared: declared_len,
                actual_payload: payload.len(),
            });
        }

        self.decode_payload(payload)
    }

    pub fn encode_payload(&self, envelope: &MessageEnvelope) -> Result<Vec<u8>, CodecError> {
        let mut map_pairs = Vec::with_capacity(envelope.len());

        for (key, value) in envelope {
            validate_value(value)?;
            map_pairs.push((Value::String(key.as_str().into()), value.clone()));
        }

        let value = Value::Map(map_pairs);

        let mut encoded = Vec::new();
        rmpv::encode::write_value(&mut encoded, &value).map_err(CodecError::MessagePackEncode)?;

        if encoded.len() > self.config.max_envelope_size_bytes {
            return Err(CodecError::PayloadTooLarge {
                size: encoded.len(),
                limit: self.config.max_envelope_size_bytes,
            });
        }

        Ok(encoded)
    }

    pub fn decode_payload(&self, payload: &[u8]) -> Result<MessageEnvelope, CodecError> {
        if payload.is_empty() {
            return Err(CodecError::ProtocolZeroLength);
        }
        if payload.len() > self.config.max_envelope_size_bytes {
            return Err(CodecError::PayloadTooLarge {
                size: payload.len(),
                limit: self.config.max_envelope_size_bytes,
            });
        }

        let mut cursor = Cursor::new(payload);
        let value = rmpv::decode::read_value(&mut cursor).map_err(CodecError::MessagePackDecode)?;
        if cursor.position() as usize != payload.len() {
            return Err(CodecError::TrailingDataInPayload);
        }

        parse_envelope(value)
    }
}

#[derive(Debug)]
pub enum CodecError {
    PayloadTooLarge { size: usize, limit: usize },
    FrameTooShort { size: usize },
    FrameLengthMismatch { declared: usize, actual_payload: usize },
    ProtocolZeroLength,
    ProtocolLengthTooLarge { length: usize, limit: usize },
    MessagePackEncode(rmpv::encode::Error),
    MessagePackDecode(rmpv::decode::Error),
    TrailingDataInPayload,
    EnvelopeMustBeMap,
    MapKeyMustBeUtf8String,
    FloatNotAllowed,
    ExtensionTypeNotAllowed,
    IntegerOutOfRange,
    InvalidConfiguredLimit { value: u64, min: usize, max: usize },
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLarge { size, limit } => {
                write!(f, "payload size {size} exceeds limit {limit}")
            }
            Self::FrameTooShort { size } => {
                write!(f, "frame size {size} is smaller than 4-byte header")
            }
            Self::FrameLengthMismatch {
                declared,
                actual_payload,
            } => write!(
                f,
                "frame length mismatch: declared {declared} bytes, actual payload {actual_payload} bytes"
            ),
            Self::ProtocolZeroLength => {
                write!(f, "protocol error: frame length cannot be zero")
            }
            Self::ProtocolLengthTooLarge { length, limit } => write!(
                f,
                "protocol error: frame length {length} exceeds max {limit}"
            ),
            Self::MessagePackEncode(source) => write!(f, "messagepack encode error: {source}"),
            Self::MessagePackDecode(source) => write!(f, "messagepack decode error: {source}"),
            Self::TrailingDataInPayload => write!(f, "payload contains trailing MessagePack data"),
            Self::EnvelopeMustBeMap => write!(f, "message envelope must be a map"),
            Self::MapKeyMustBeUtf8String => write!(f, "map keys must be UTF-8 strings"),
            Self::FloatNotAllowed => write!(f, "floats are not allowed in message envelope"),
            Self::ExtensionTypeNotAllowed => {
                write!(f, "MessagePack extension values are not allowed")
            }
            Self::IntegerOutOfRange => write!(f, "integer value must fit in signed int64"),
            Self::InvalidConfiguredLimit { value, min, max } => write!(
                f,
                "invalid configured wire max envelope size {value}, expected range {min}..={max} bytes"
            ),
        }
    }
}

impl std::error::Error for CodecError {}

pub fn encode_frame(envelope: &MessageEnvelope) -> Result<Vec<u8>, CodecError> {
    WireCodec::default().encode_frame(envelope)
}

pub fn decode_frame(frame: &[u8]) -> Result<MessageEnvelope, CodecError> {
    WireCodec::default().decode_frame(frame)
}

pub fn encode_payload(envelope: &MessageEnvelope) -> Result<Vec<u8>, CodecError> {
    WireCodec::default().encode_payload(envelope)
}

pub fn decode_payload(payload: &[u8]) -> Result<MessageEnvelope, CodecError> {
    WireCodec::default().decode_payload(payload)
}

fn parse_envelope(value: Value) -> Result<MessageEnvelope, CodecError> {
    let Value::Map(entries) = value else {
        return Err(CodecError::EnvelopeMustBeMap);
    };

    let mut envelope = MessageEnvelope::new();
    for (key, value) in entries {
        let key = parse_key(key)?;
        validate_value(&value)?;
        envelope.insert(key, value);
    }

    Ok(envelope)
}

fn parse_key(key: Value) -> Result<String, CodecError> {
    let Value::String(text) = key else {
        return Err(CodecError::MapKeyMustBeUtf8String);
    };

    let Some(text) = text.as_str() else {
        return Err(CodecError::MapKeyMustBeUtf8String);
    };

    Ok(text.to_owned())
}

fn validate_value(value: &Value) -> Result<(), CodecError> {
    match value {
        Value::Nil | Value::Boolean(_) | Value::String(_) | Value::Binary(_) => Ok(()),
        Value::Integer(number) => validate_integer(number),
        Value::Array(values) => {
            for value in values {
                validate_value(value)?;
            }
            Ok(())
        }
        Value::Map(entries) => {
            for (key, value) in entries {
                parse_key(key.clone())?;
                validate_value(value)?;
            }
            Ok(())
        }
        Value::F32(_) | Value::F64(_) => Err(CodecError::FloatNotAllowed),
        Value::Ext(_, _) => Err(CodecError::ExtensionTypeNotAllowed),
    }
}

fn validate_integer(number: &Integer) -> Result<(), CodecError> {
    if number.as_i64().is_some() {
        return Ok(());
    }

    match number.as_u64() {
        Some(value) if value <= i64::MAX as u64 => Ok(()),
        _ => Err(CodecError::IntegerOutOfRange),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        decode_frame, decode_payload, encode_frame, encode_payload, CodecConfig, CodecError,
        MessageEnvelope,
        WireCodec, DEFAULT_MAX_ENVELOPE_SIZE_BYTES, MAX_MAX_ENVELOPE_SIZE_BYTES,
        MIN_MAX_ENVELOPE_SIZE_BYTES,
    };
    use crate::config::{
        AppConfig, HeartbeatConfig, LoggingConfig, ServerConfig, StorageConfig, WireSessionConfig,
        WireConfig as AppWireConfig,
    };
    use rmpv::Value;

    fn sample_envelope() -> MessageEnvelope {
        let mut envelope = MessageEnvelope::new();
        envelope.insert("messageType".to_owned(), Value::String("PING".into()));
        envelope.insert("requestId".to_owned(), Value::Integer(42.into()));
        envelope.insert("ok".to_owned(), Value::Boolean(true));
        envelope.insert(
            "lockToken".to_owned(),
            Value::Binary(vec![0xde, 0xad, 0xbe, 0xef]),
        );
        envelope.insert(
            "items".to_owned(),
            Value::Array(vec![Value::Nil, Value::String("x".into())]),
        );
        envelope
    }

    #[test]
    fn round_trip_frame_encode_decode() {
        let envelope = sample_envelope();
        let frame = encode_frame(&envelope).expect("frame should encode");
        let decoded = decode_frame(&frame).expect("frame should decode");

        assert_eq!(decoded.get("messageType"), Some(&Value::String("PING".into())));
        assert_eq!(decoded.get("requestId"), Some(&Value::Integer(42.into())));
        assert_eq!(decoded.get("ok"), Some(&Value::Boolean(true)));
    }

    #[test]
    fn encodes_payload_directly() {
        let envelope = sample_envelope();
        let payload = encode_payload(&envelope).expect("payload should encode");
        assert!(!payload.is_empty());
    }

    #[test]
    fn rejects_float_values() {
        let mut envelope = MessageEnvelope::new();
        envelope.insert("ratio".to_owned(), Value::F64(1.5));

        let error = encode_frame(&envelope).expect_err("float should be rejected");
        assert!(matches!(error, CodecError::FloatNotAllowed));
    }

    #[test]
    fn rejects_extension_values() {
        let mut envelope = MessageEnvelope::new();
        envelope.insert("ext".to_owned(), Value::Ext(1, vec![1, 2, 3]));

        let error = encode_frame(&envelope).expect_err("ext should be rejected");
        assert!(matches!(error, CodecError::ExtensionTypeNotAllowed));
    }

    #[test]
    fn rejects_unsigned_integers_above_i64() {
        let mut envelope = MessageEnvelope::new();
        envelope.insert(
            "tooBig".to_owned(),
            Value::Integer((i64::MAX as u64 + 1).into()),
        );

        let error = encode_frame(&envelope).expect_err("out-of-range int should be rejected");
        assert!(matches!(error, CodecError::IntegerOutOfRange));
    }

    #[test]
    fn rejects_zero_length_frame() {
        let frame = [0_u8, 0, 0, 0];
        let error = decode_frame(&frame).expect_err("zero-length frame should fail");

        assert!(matches!(error, CodecError::ProtocolZeroLength));
    }

    #[test]
    fn rejects_frame_larger_than_limit() {
        let declared = (DEFAULT_MAX_ENVELOPE_SIZE_BYTES as u32 + 1).to_be_bytes();
        let frame = declared.to_vec();
        let error = decode_frame(&frame).expect_err("oversized frame should fail");

        assert!(matches!(error, CodecError::ProtocolLengthTooLarge { .. }));
    }

    #[test]
    fn rejects_payload_larger_than_limit_on_encode() {
        let mut envelope = MessageEnvelope::new();
        envelope.insert(
            "lockToken".to_owned(),
            Value::Binary(vec![0x41; DEFAULT_MAX_ENVELOPE_SIZE_BYTES + 1]),
        );

        let error = encode_frame(&envelope).expect_err("oversized payload should fail");
        assert!(matches!(error, CodecError::PayloadTooLarge { .. }));
    }

    #[test]
    fn rejects_non_string_map_keys() {
        let value = Value::Map(vec![(Value::Integer(1.into()), Value::String("x".into()))]);
        let mut payload = Vec::new();
        rmpv::encode::write_value(&mut payload, &value).expect("test payload should encode");

        let mut frame = Vec::new();
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);

        let error = decode_frame(&frame).expect_err("non-string key should fail");
        assert!(matches!(error, CodecError::MapKeyMustBeUtf8String));
    }

    #[test]
    fn rejects_trailing_data_in_payload() {
        let mut payload = Vec::new();
        rmpv::encode::write_value(&mut payload, &Value::Map(vec![]))
            .expect("first object should encode");
        rmpv::encode::write_value(&mut payload, &Value::Nil).expect("second object should encode");

        let error = decode_payload(&payload).expect_err("trailing data should fail");
        assert!(matches!(error, CodecError::TrailingDataInPayload));
    }

    #[test]
    fn accepts_configured_bound_inside_range() {
        let config = CodecConfig::new(1_048_576).expect("valid bound should pass");
        assert_eq!(config.max_envelope_size_bytes, 1_048_576);
    }

    #[test]
    fn rejects_configured_bound_outside_range() {
        let low = CodecConfig::new(MIN_MAX_ENVELOPE_SIZE_BYTES - 1)
            .expect_err("lower-than-min should fail");
        let high = CodecConfig::new(MAX_MAX_ENVELOPE_SIZE_BYTES + 1)
            .expect_err("higher-than-max should fail");

        assert!(matches!(low, CodecError::InvalidConfiguredLimit { .. }));
        assert!(matches!(high, CodecError::InvalidConfiguredLimit { .. }));
    }

    #[test]
    fn builds_from_app_config() {
        let app_config = AppConfig {
            logging: LoggingConfig {
                level: "debug".to_owned(),
                human_friendly: false,
            },
            heartbeat: HeartbeatConfig { interval_ms: 1000 },
            server: ServerConfig {
                host: "127.0.0.1".to_owned(),
                port: 9876,
                tls_enabled: false,
            },
            wire: AppWireConfig {
                max_envelope_size_bytes: 1_048_576,
                session: WireSessionConfig::default(),
            },
            storage: StorageConfig::default(),
        };

        let codec = WireCodec::from_app_config(&app_config).expect("codec should build");
        assert_eq!(codec.max_envelope_size_bytes(), 1_048_576);
    }
}
