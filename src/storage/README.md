# Storage Module (`src/storage`)

## Core Objective

Provide a pluggable storage facade so business/domain modules depend on stable storage APIs rather than a concrete storage engine.

## Core Ideas

- Storage engine selection is explicit in app config via `storage.engine`.
- Current concrete engine is `sled`, but the module is structured for future engine additions.
- Storage initialization is fail-fast and happens before TCP server startup.

## Core Concepts

- `StorageFacade`
  - owns active storage backend
  - stores resolved data path and active engine metadata
  - exposes queue/job persistence and retrieval-oriented backend-agnostic operations
- `StorageBackend`
  - trait boundary for engine-specific implementations
- `src/storage/sled_backend.rs`
  - concrete `SledStorage` backend based on `sled::Db`
- `src/storage/facade.rs`
  - facade orchestration and engine selection
- `src/storage/backend.rs`
  - storage backend trait contract

## Immutable Keying Strategy

- Versioned namespace:
  - queues: `v1:q:<queue-name>`
  - jobs (primary): `v1:j:<job-uuid>`
  - jobs queue-time index: `v1:j_qt:<execution-start-ms>:<queue-name>:<job-uuid>`
  - jobs status value key: `v1:status:<job-uuid>`
  - jobs status FIFO index: `v1:status_fifo:<status>:<created-at-ms-be>:<job-uuid-bytes>`
  - queue-status counters: `v1:q_status:<queue-name>:<status>` (u64 big-endian count)
- Queue records are scanned by prefix (`v1:q:`) for full queue-state restoration.
- Job-by-UUID lookup is direct by exact key (`v1:j:<uuid>`).
- Queue-time index sorts by execution start first, then queue name, then job uuid.
- Queue-time index is intended for future range/limit/offset style retrieval by earliest execution time.
- Status key stores mutable status value and is optimized for frequent updates.
- Status FIFO index allows deterministic status-scoped processing order by `created_at` ascending (FIFO for `waiting` jobs).
- Queue-status counters are updated atomically on job insert, status transition, and removal.
- Numeric components used for ordered scans are encoded to big-endian sortable bytes for sled iterator correctness.
- Key prefixes are immutable and versioned to allow future keyspace migrations without breaking existing data.

## Most Relevant Features

- Configurable storage path (`storage.path`) with `~/` and `$HOME` expansion.
- Configurable sled options:
  - `storage.sled.cache_capacity` (optional)
  - `storage.sled.mode` (`low_space` or `high_throughput`, optional)
- Queue persistence supports full-set replacement (`replace_queues`) for deterministic persist-first/reload flows.
- Job persistence supports upsert by UUID (`upsert_job_record`) for enqueue flows.
- Job retrieval supports status FIFO scans and queue+status paginated listing over persisted records.
- Significant initialization events are logged from this module.
- Initialization errors are surfaced to startup flow and terminate application startup.
