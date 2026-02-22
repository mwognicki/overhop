use std::fmt;

use crate::wire::codec::{CodecError, WireCodec};
use crate::wire::envelope::{EnvelopeError, PayloadMap, WireEnvelope};

pub const HELLO_MESSAGE_TYPE: i64 = 1;
pub const HI_MESSAGE_TYPE: i64 = 103;

#[derive(Debug)]
pub enum HandshakeError {
    Codec(CodecError),
    Envelope(EnvelopeError),
}

impl fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(source) => write!(f, "handshake codec error: {source}"),
            Self::Envelope(source) => write!(f, "handshake envelope error: {source}"),
        }
    }
}

impl std::error::Error for HandshakeError {}

pub fn process_client_handshake_frame(
    codec: &WireCodec,
    frame: &[u8],
) -> Result<Option<Vec<u8>>, HandshakeError> {
    let raw = codec.decode_frame(frame).map_err(HandshakeError::Codec)?;
    let envelope = WireEnvelope::from_raw(&raw).map_err(HandshakeError::Envelope)?;
    envelope
        .validate_client_to_server()
        .map_err(HandshakeError::Envelope)?;

    if envelope.message_type != HELLO_MESSAGE_TYPE {
        return Ok(None);
    }

    let response = WireEnvelope::new(HI_MESSAGE_TYPE, envelope.request_id, PayloadMap::new());
    codec
        .encode_frame(&response.into_raw())
        .map(Some)
        .map_err(HandshakeError::Codec)
}

#[cfg(test)]
mod tests {
    use crate::wire::codec::{CodecConfig, FRAME_HEADER_SIZE_BYTES};
    use crate::wire::envelope::{PayloadMap, WireEnvelope};

    use super::{process_client_handshake_frame, HELLO_MESSAGE_TYPE, HI_MESSAGE_TYPE};

    #[test]
    fn hello_message_returns_hi_with_empty_payload() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let hello = WireEnvelope::new(HELLO_MESSAGE_TYPE, "rid-1", PayloadMap::new());
        let frame = codec
            .encode_frame(&hello.into_raw())
            .expect("hello should encode");

        let response = process_client_handshake_frame(&codec, &frame)
            .expect("hello handling should pass")
            .expect("hello should return response frame");
        let decoded = codec
            .decode_frame(&response)
            .expect("response frame should decode");
        let envelope =
            WireEnvelope::from_raw(&decoded).expect("response envelope should parse correctly");

        assert_eq!(envelope.message_type, HI_MESSAGE_TYPE);
        assert_eq!(envelope.request_id, "rid-1");
        assert!(envelope.payload.is_empty());
    }

    #[test]
    fn non_hello_message_returns_no_response() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let ping = WireEnvelope::new(2, "rid-2", PayloadMap::new());
        let frame = codec
            .encode_frame(&ping.into_raw())
            .expect("ping should encode");

        let response =
            process_client_handshake_frame(&codec, &frame).expect("processing should pass");
        assert!(response.is_none());
    }

    #[test]
    fn malformed_frame_is_rejected() {
        let codec = crate::wire::codec::WireCodec::new(CodecConfig::default());
        let malformed = vec![0_u8; FRAME_HEADER_SIZE_BYTES - 1];
        let err = process_client_handshake_frame(&codec, &malformed)
            .expect_err("malformed frame should fail");
        assert!(matches!(err, super::HandshakeError::Codec(_)));
    }
}
