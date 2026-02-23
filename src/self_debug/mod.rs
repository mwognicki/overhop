use std::fmt;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use rmpv::Value;

use crate::config::StorageConfig;
use crate::wire::codec::{FRAME_HEADER_SIZE_BYTES, WireCodec};
use crate::wire::envelope::{PayloadMap, WireEnvelope};
use crate::wire::handshake::HELLO_MESSAGE_TYPE;
use crate::wire::session::{
    ADDQUEUE_MESSAGE_TYPE, CREDIT_MESSAGE_TYPE, ENQUEUE_MESSAGE_TYPE, JOB_MESSAGE_TYPE,
    LSJOB_MESSAGE_TYPE, LSQUEUE_MESSAGE_TYPE, PAUSE_MESSAGE_TYPE, PING_MESSAGE_TYPE,
    QUEUE_MESSAGE_TYPE, REGISTER_MESSAGE_TYPE, RESUME_MESSAGE_TYPE, RMJOB_MESSAGE_TYPE,
    RMQUEUE_MESSAGE_TYPE, STATUS_MESSAGE_TYPE, SUBSCRIBE_MESSAGE_TYPE, UNSUBSCRIBE_MESSAGE_TYPE,
};

const COLOR_HEADER: &str = "\x1b[38;5;214m";
const COLOR_OUT: &str = "\x1b[38;5;81m";
const COLOR_IN: &str = "\x1b[38;5;120m";
const COLOR_ERROR: &str = "\x1b[38;5;196m";
const COLOR_DIM: &str = "\x1b[2;90m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

#[derive(Debug)]
pub enum SelfDebugError {
    Io(std::io::Error),
    Encode(crate::wire::codec::CodecError),
    Decode(crate::wire::codec::CodecError),
    Envelope(crate::wire::envelope::EnvelopeError),
    MissingField(&'static str),
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RuntimeFlags {
    pub enabled: bool,
    pub keep_artifacts: bool,
}

impl fmt::Display for SelfDebugError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(source) => write!(f, "io error: {source}"),
            Self::Encode(source) => write!(f, "encode error: {source}"),
            Self::Decode(source) => write!(f, "decode error: {source}"),
            Self::Envelope(source) => write!(f, "envelope error: {source}"),
            Self::MissingField(field) => write!(f, "response missing expected field '{field}'"),
        }
    }
}

impl std::error::Error for SelfDebugError {}

impl From<std::io::Error> for SelfDebugError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub fn extract_runtime_flags(args: Vec<String>) -> (RuntimeFlags, Vec<String>) {
    let mut flags = RuntimeFlags::default();
    let mut config_args = Vec::new();

    for arg in args {
        if arg == "--self-debug" {
            flags.enabled = true;
        } else if arg == "--self-debug-keep-artifacts" {
            flags.keep_artifacts = true;
        } else {
            config_args.push(arg);
        }
    }

    (flags, config_args)
}

pub fn spawn_runner(addr: SocketAddr, codec: WireCodec) -> Receiver<Result<(), SelfDebugError>> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = run_self_debug(addr, codec);
        let _ = tx.send(result);
    });
    rx
}

pub fn resolve_storage_path(storage: &StorageConfig) -> String {
    match storage.self_debug_path.as_deref() {
        Some(path) if !path.trim().is_empty() => path.to_owned(),
        _ => format!("{}-self-debug", storage.path),
    }
}

pub fn cleanup_artifacts(path: &Path) -> Result<(), SelfDebugError> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

pub fn run_self_debug(addr: SocketAddr, codec: WireCodec) -> Result<(), SelfDebugError> {
    println!("{COLOR_HEADER}========== SELF DEBUG MODE =========={RESET}");
    println!(
        "{COLOR_DIM}connecting to local overhop server at {}{RESET}",
        addr
    );
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    stream.set_write_timeout(Some(Duration::from_secs(3)))?;

    let hi = send_and_receive(
        &mut stream,
        &codec,
        HELLO_MESSAGE_TYPE,
        "sd-1",
        PayloadMap::new(),
    )?;
    let _ = hi;

    let register = send_and_receive(
        &mut stream,
        &codec,
        REGISTER_MESSAGE_TYPE,
        "sd-2",
        PayloadMap::new(),
    )?;
    let _wid = require_string(&register.payload, "wid")?;

    let _ = send_and_receive(
        &mut stream,
        &codec,
        LSQUEUE_MESSAGE_TYPE,
        "sd-3",
        PayloadMap::new(),
    )?;

    let mut addqueue_payload = PayloadMap::new();
    let queue_name = format!(
        "self_debug_{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    addqueue_payload.insert("name".to_owned(), Value::String(queue_name.clone().into()));
    let mut config_entries = Vec::new();
    config_entries.push((
        Value::String("concurrency_limit".into()),
        Value::Integer(1_i64.into()),
    ));
    config_entries.push((
        Value::String("allow_job_overrides".into()),
        Value::Boolean(true),
    ));
    addqueue_payload.insert("config".to_owned(), Value::Map(config_entries));

    let addqueue = send_and_receive(
        &mut stream,
        &codec,
        ADDQUEUE_MESSAGE_TYPE,
        "sd-4",
        addqueue_payload,
    )?;
    let _qid = require_string(&addqueue.payload, "qid")?;

    let mut queue_payload = PayloadMap::new();
    queue_payload.insert("q".to_owned(), Value::String(queue_name.clone().into()));
    let _ = send_and_receive(
        &mut stream,
        &codec,
        QUEUE_MESSAGE_TYPE,
        "sd-5",
        queue_payload,
    )?;

    let mut subscribe_payload = PayloadMap::new();
    subscribe_payload.insert("q".to_owned(), Value::String(queue_name.clone().into()));
    subscribe_payload.insert("credits".to_owned(), Value::Integer(1_i64.into()));
    let subscribe = send_and_receive(
        &mut stream,
        &codec,
        SUBSCRIBE_MESSAGE_TYPE,
        "sd-6",
        subscribe_payload,
    )?;
    let sid = require_string(&subscribe.payload, "sid")?;

    let mut credit_payload = PayloadMap::new();
    credit_payload.insert("sid".to_owned(), Value::String(sid.clone().into()));
    credit_payload.insert("credits".to_owned(), Value::Integer(2_i64.into()));
    let _ = send_and_receive(
        &mut stream,
        &codec,
        CREDIT_MESSAGE_TYPE,
        "sd-7",
        credit_payload,
    )?;

    let _ = send_and_receive(
        &mut stream,
        &codec,
        STATUS_MESSAGE_TYPE,
        "sd-8",
        PayloadMap::new(),
    )?;

    let _ = send_and_receive(
        &mut stream,
        &codec,
        PING_MESSAGE_TYPE,
        "sd-9",
        PayloadMap::new(),
    )?;

    let mut unsubscribe_payload = PayloadMap::new();
    unsubscribe_payload.insert("sid".to_owned(), Value::String(sid.into()));
    let _ = send_and_receive(
        &mut stream,
        &codec,
        UNSUBSCRIBE_MESSAGE_TYPE,
        "sd-10",
        unsubscribe_payload,
    )?;

    let mut rmqueue_payload = PayloadMap::new();
    rmqueue_payload.insert("q".to_owned(), Value::String(queue_name.clone().into()));
    let _ = send_and_receive(
        &mut stream,
        &codec,
        RMQUEUE_MESSAGE_TYPE,
        "sd-11",
        rmqueue_payload,
    )?;

    let mut addqueue_persisted_payload = PayloadMap::new();
    let persisted_queue_name = format!(
        "self_debug_persisted_{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    addqueue_persisted_payload.insert(
        "name".to_owned(),
        Value::String(persisted_queue_name.clone().into()),
    );
    let _ = send_and_receive(
        &mut stream,
        &codec,
        ADDQUEUE_MESSAGE_TYPE,
        "sd-12",
        addqueue_persisted_payload,
    )?;

    let mut pause_payload = PayloadMap::new();
    pause_payload.insert(
        "q".to_owned(),
        Value::String(persisted_queue_name.clone().into()),
    );
    let _ = send_and_receive(
        &mut stream,
        &codec,
        PAUSE_MESSAGE_TYPE,
        "sd-13",
        pause_payload,
    )?;

    let mut resume_payload = PayloadMap::new();
    resume_payload.insert("q".to_owned(), Value::String(persisted_queue_name.clone().into()));
    let _ = send_and_receive(
        &mut stream,
        &codec,
        RESUME_MESSAGE_TYPE,
        "sd-14",
        resume_payload,
    )?;

    let mut enqueue_payload = PayloadMap::new();
    enqueue_payload.insert(
        "q".to_owned(),
        Value::String(persisted_queue_name.clone().into()),
    );
    enqueue_payload.insert(
        "scheduled_at".to_owned(),
        Value::String((chrono::Utc::now() - chrono::Duration::seconds(10)).to_rfc3339().into()),
    );
    enqueue_payload.insert("max_attempts".to_owned(), Value::Integer(3_i64.into()));
    enqueue_payload.insert("retry_interval_ms".to_owned(), Value::Integer(200_i64.into()));
    enqueue_payload.insert(
        "job_payload".to_owned(),
        Value::Map(vec![
            (
                Value::String("task".into()),
                Value::String("self-debug-enqueue-check".into()),
            ),
            (Value::String("kind".into()), Value::String("poc".into())),
        ]),
    );
    enqueue_then_fetch_job(&mut stream, &codec, "sd-15", "sd-16", enqueue_payload)?;

    let mut enqueue_for_remove_payload = PayloadMap::new();
    enqueue_for_remove_payload.insert(
        "q".to_owned(),
        Value::String(persisted_queue_name.clone().into()),
    );
    enqueue_for_remove_payload.insert(
        "job_payload".to_owned(),
        Value::Map(vec![(
            Value::String("task".into()),
            Value::String("self-debug-remove-job-check".into()),
        )]),
    );
    let enqueue_for_remove = send_and_receive(
        &mut stream,
        &codec,
        ENQUEUE_MESSAGE_TYPE,
        "sd-17",
        enqueue_for_remove_payload,
    )?;
    let jid_to_remove = require_string(&enqueue_for_remove.payload, "jid")?;

    let mut lsjob_payload = PayloadMap::new();
    lsjob_payload.insert(
        "q".to_owned(),
        Value::String(persisted_queue_name.clone().into()),
    );
    lsjob_payload.insert("status".to_owned(), Value::String("new".into()));
    let _ = send_and_receive(
        &mut stream,
        &codec,
        LSJOB_MESSAGE_TYPE,
        "sd-18",
        lsjob_payload,
    )?;

    let mut rmjob_payload = PayloadMap::new();
    rmjob_payload.insert("jid".to_owned(), Value::String(jid_to_remove.into()));
    let _ = send_and_receive(
        &mut stream,
        &codec,
        RMJOB_MESSAGE_TYPE,
        "sd-19",
        rmjob_payload,
    )?;

    println!("{COLOR_HEADER}====== SELF DEBUG MODE COMPLETE ======{RESET}");
    Ok(())
}

fn send_and_receive(
    stream: &mut TcpStream,
    codec: &WireCodec,
    message_type: i64,
    request_id: &str,
    payload: PayloadMap,
) -> Result<WireEnvelope, SelfDebugError> {
    let outgoing = WireEnvelope::new(message_type, request_id, payload);
    print_decoded("OUT", &outgoing, COLOR_OUT);
    let frame = codec
        .encode_frame(&outgoing.into_raw())
        .map_err(SelfDebugError::Encode)?;
    stream.write_all(&frame)?;
    stream.flush()?;

    let incoming_frame = read_frame(stream)?;
    let decoded = codec
        .decode_frame(&incoming_frame)
        .map_err(SelfDebugError::Decode)?;
    let incoming = WireEnvelope::from_raw(&decoded).map_err(SelfDebugError::Envelope)?;
    print_decoded("IN ", &incoming, COLOR_IN);

    if incoming.message_type == crate::wire::envelope::SERVER_ERR_MESSAGE_TYPE {
        println!(
            "{COLOR_ERROR}self-debug received ERR for rid='{}'{RESET}",
            incoming.request_id
        );
    }

    Ok(incoming)
}

fn enqueue_then_fetch_job(
    stream: &mut TcpStream,
    codec: &WireCodec,
    enqueue_request_id: &str,
    job_request_id: &str,
    enqueue_payload: PayloadMap,
) -> Result<(), SelfDebugError> {
    let enqueue = send_and_receive(
        stream,
        codec,
        ENQUEUE_MESSAGE_TYPE,
        enqueue_request_id,
        enqueue_payload,
    )?;
    let jid = require_string(&enqueue.payload, "jid")?;

    let mut job_payload = PayloadMap::new();
    job_payload.insert("jid".to_owned(), Value::String(jid.into()));
    let _ = send_and_receive(stream, codec, JOB_MESSAGE_TYPE, job_request_id, job_payload)?;

    Ok(())
}

fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, SelfDebugError> {
    let mut header = [0_u8; FRAME_HEADER_SIZE_BYTES];
    stream.read_exact(&mut header)?;
    let payload_len = u32::from_be_bytes(header) as usize;
    let mut payload = vec![0_u8; payload_len];
    stream.read_exact(&mut payload)?;

    let mut frame = Vec::with_capacity(FRAME_HEADER_SIZE_BYTES + payload_len);
    frame.extend_from_slice(&header);
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn print_decoded(label: &str, envelope: &WireEnvelope, color: &str) {
    let json = envelope_to_json_line(envelope);
    let message_name = message_type_name(envelope.message_type);
    println!("{color}[{label}] {BOLD}{message_name}{RESET} {json}");
}

fn envelope_to_json_line(envelope: &WireEnvelope) -> String {
    let payload = value_to_json(&Value::Map(
        envelope
            .payload
            .iter()
            .map(|(k, v)| (Value::String(k.as_str().into()), v.clone()))
            .collect(),
    ));
    let body = serde_json::json!({
        "v": envelope.version,
        "t": envelope.message_type,
        "rid": envelope.request_id,
        "p": payload
    });
    body.to_string()
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Nil => serde_json::Value::Null,
        Value::Boolean(v) => serde_json::Value::Bool(*v),
        Value::Integer(v) => {
            if let Some(i) = v.as_i64() {
                serde_json::json!(i)
            } else if let Some(u) = v.as_u64() {
                serde_json::json!(u)
            } else {
                serde_json::Value::Null
            }
        }
        Value::F32(v) => serde_json::json!(v),
        Value::F64(v) => serde_json::json!(v),
        Value::String(v) => serde_json::json!(v.as_str().unwrap_or_default()),
        Value::Binary(v) => {
            let hex = v.iter().map(|b| format!("{:02x}", b)).collect::<String>();
            serde_json::json!({ "bin_hex": hex })
        }
        Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(value_to_json).collect())
        }
        Value::Map(entries) => {
            let mut map = serde_json::Map::new();
            for (k, v) in entries {
                let key = if let Some(text) = k.as_str() {
                    text.to_owned()
                } else {
                    format!("{k:?}")
                };
                map.insert(key, value_to_json(v));
            }
            serde_json::Value::Object(map)
        }
        Value::Ext(_, data) => {
            let hex = data.iter().map(|b| format!("{:02x}", b)).collect::<String>();
            serde_json::json!({ "ext_hex": hex })
        }
    }
}

fn require_string(payload: &PayloadMap, key: &'static str) -> Result<String, SelfDebugError> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(SelfDebugError::MissingField(key))
}

fn message_type_name(message_type: i64) -> &'static str {
    match message_type {
        HELLO_MESSAGE_TYPE => "HELLO",
        REGISTER_MESSAGE_TYPE => "REGISTER",
        PING_MESSAGE_TYPE => "PING",
        QUEUE_MESSAGE_TYPE => "QUEUE",
        LSQUEUE_MESSAGE_TYPE => "LSQUEUE",
        SUBSCRIBE_MESSAGE_TYPE => "SUBSCRIBE",
        UNSUBSCRIBE_MESSAGE_TYPE => "UNSUBSCRIBE",
        CREDIT_MESSAGE_TYPE => "CREDIT",
        ADDQUEUE_MESSAGE_TYPE => "ADDQUEUE",
        RMQUEUE_MESSAGE_TYPE => "RMQUEUE",
        PAUSE_MESSAGE_TYPE => "PAUSE",
        RESUME_MESSAGE_TYPE => "RESUME",
        ENQUEUE_MESSAGE_TYPE => "ENQUEUE",
        LSJOB_MESSAGE_TYPE => "LSJOB",
        JOB_MESSAGE_TYPE => "JOB",
        RMJOB_MESSAGE_TYPE => "RMJOB",
        STATUS_MESSAGE_TYPE => "STATUS",
        crate::wire::envelope::SERVER_OK_MESSAGE_TYPE => "OK",
        crate::wire::envelope::SERVER_ERR_MESSAGE_TYPE => "ERR",
        crate::wire::handshake::HI_MESSAGE_TYPE => "HI",
        crate::wire::session::IDENT_MESSAGE_TYPE => "IDENT",
        crate::wire::session::PONG_MESSAGE_TYPE => "PONG",
        _ => "UNKNOWN",
    }
}
