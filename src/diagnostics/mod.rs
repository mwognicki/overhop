use std::fmt;
use std::fs;
use std::path::Path;

use chrono::{DateTime, Utc};
use rmpv::Value;

use crate::orchestrator::queues::QueuePool;
use crate::pools::ConnectionWorkerPools;
use crate::storage::{StorageError, StorageFacade};
use crate::wire::envelope::PayloadMap;

#[derive(Debug)]
pub enum DiagnosticsError {
    Storage(StorageError),
    Io(std::io::Error),
}

impl fmt::Display for DiagnosticsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(source) => write!(f, "storage diagnostics error: {source}"),
            Self::Io(source) => write!(f, "io diagnostics error: {source}"),
        }
    }
}

impl std::error::Error for DiagnosticsError {}

impl From<StorageError> for DiagnosticsError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

impl From<std::io::Error> for DiagnosticsError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

pub fn build_status_payload(
    app_started_at: DateTime<Utc>,
    pools: &ConnectionWorkerPools,
    storage: &StorageFacade,
    queue_pool: &QueuePool,
) -> Result<PayloadMap, DiagnosticsError> {
    let mut payload = PayloadMap::new();
    payload.insert(
        "application".to_owned(),
        application_status(app_started_at, Utc::now()),
    );
    payload.insert("memory".to_owned(), memory_stats());
    payload.insert("pools".to_owned(), pool_stats(pools));
    payload.insert("storage".to_owned(), storage_summary(storage)?);
    payload.insert("queues".to_owned(), queue_pool_stats(queue_pool));
    Ok(payload)
}

pub fn application_status(app_started_at: DateTime<Utc>, now: DateTime<Utc>) -> Value {
    let uptime_seconds = (now - app_started_at).num_seconds().max(0);
    Value::Map(vec![
        (
            Value::String("name".into()),
            Value::String(env!("CARGO_PKG_NAME").into()),
        ),
        (
            Value::String("version".into()),
            Value::String(env!("CARGO_PKG_VERSION").into()),
        ),
        (
            Value::String("build_date_utc".into()),
            Value::String(env!("OVERHOP_BUILD_DATE_UTC").into()),
        ),
        (
            Value::String("started_at".into()),
            Value::String(app_started_at.to_rfc3339().into()),
        ),
        (
            Value::String("now".into()),
            Value::String(now.to_rfc3339().into()),
        ),
        (
            Value::String("uptime_seconds".into()),
            Value::Integer(uptime_seconds.into()),
        ),
        (
            Value::String("pid".into()),
            Value::Integer((std::process::id() as i64).into()),
        ),
    ])
}

pub fn memory_stats() -> Value {
    let mut vm_rss_kb: Option<i64> = None;
    let mut vm_size_kb: Option<i64> = None;
    let mut vm_peak_kb: Option<i64> = None;

    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                vm_rss_kb = parse_kb_field(line);
            } else if line.starts_with("VmSize:") {
                vm_size_kb = parse_kb_field(line);
            } else if line.starts_with("VmPeak:") {
                vm_peak_kb = parse_kb_field(line);
            }
        }
    }

    Value::Map(vec![
        (
            Value::String("source".into()),
            Value::String("/proc/self/status".into()),
        ),
        (
            Value::String("available".into()),
            Value::Boolean(vm_rss_kb.is_some() || vm_size_kb.is_some() || vm_peak_kb.is_some()),
        ),
        (
            Value::String("vm_rss_kb".into()),
            vm_rss_kb.map_or(Value::Nil, |v| Value::Integer(v.into())),
        ),
        (
            Value::String("vm_size_kb".into()),
            vm_size_kb.map_or(Value::Nil, |v| Value::Integer(v.into())),
        ),
        (
            Value::String("vm_peak_kb".into()),
            vm_peak_kb.map_or(Value::Nil, |v| Value::Integer(v.into())),
        ),
    ])
}

pub fn pool_stats(pools: &ConnectionWorkerPools) -> Value {
    let anonymous = pools.anonymous.maintenance_snapshot();
    let workers = pools.workers.active_connections();
    let mut subscriptions_total = 0_i64;

    for (worker_id, _) in &workers {
        if let Some(snapshot) = pools.workers.snapshot(*worker_id) {
            subscriptions_total += snapshot.metadata.subscriptions.len() as i64;
        }
    }

    let helloed_anonymous = anonymous
        .iter()
        .filter(|entry| entry.helloed_at.is_some())
        .count() as i64;
    let ident_challenged = anonymous
        .iter()
        .filter(|entry| entry.ident_reply_deadline_at.is_some())
        .count() as i64;

    Value::Map(vec![
        (
            Value::String("anonymous_total".into()),
            Value::Integer((anonymous.len() as i64).into()),
        ),
        (
            Value::String("anonymous_helloed".into()),
            Value::Integer(helloed_anonymous.into()),
        ),
        (
            Value::String("anonymous_ident_challenged".into()),
            Value::Integer(ident_challenged.into()),
        ),
        (
            Value::String("workers_total".into()),
            Value::Integer((workers.len() as i64).into()),
        ),
        (
            Value::String("worker_subscriptions_total".into()),
            Value::Integer(subscriptions_total.into()),
        ),
    ])
}

pub fn storage_summary(storage: &StorageFacade) -> Result<Value, DiagnosticsError> {
    let persisted_queues = storage.load_queues()?;
    let data_path = storage.data_path();
    let path_exists = data_path.exists();
    let total_size_bytes = if path_exists {
        directory_size_bytes(data_path)?
    } else {
        0
    };

    Ok(Value::Map(vec![
        (
            Value::String("engine".into()),
            Value::String(storage.engine().as_str().into()),
        ),
        (
            Value::String("data_path".into()),
            Value::String(data_path.display().to_string().into()),
        ),
        (
            Value::String("path_exists".into()),
            Value::Boolean(path_exists),
        ),
        (
            Value::String("persisted_queues_count".into()),
            Value::Integer((persisted_queues.len() as i64).into()),
        ),
        (
            Value::String("data_size_bytes".into()),
            Value::Integer((total_size_bytes as i64).into()),
        ),
    ]))
}

pub fn queue_pool_stats(queue_pool: &QueuePool) -> Value {
    let snapshot = queue_pool.snapshot();
    let total = snapshot.queues.len() as i64;
    let paused = snapshot
        .queues
        .iter()
        .filter(|queue| queue.is_paused())
        .count() as i64;
    let active = total - paused;

    Value::Map(vec![
        (
            Value::String("bootstrapped".into()),
            Value::Boolean(snapshot.bootstrapped),
        ),
        (
            Value::String("total".into()),
            Value::Integer(total.into()),
        ),
        (
            Value::String("active".into()),
            Value::Integer(active.into()),
        ),
        (
            Value::String("paused".into()),
            Value::Integer(paused.into()),
        ),
    ])
}

fn parse_kb_field(line: &str) -> Option<i64> {
    line.split_whitespace().nth(1)?.parse::<i64>().ok()
}

fn directory_size_bytes(path: &Path) -> Result<u64, std::io::Error> {
    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        } else if metadata.is_dir() {
            total = total.saturating_add(directory_size_bytes(&entry.path())?);
        }
    }
    Ok(total)
}
