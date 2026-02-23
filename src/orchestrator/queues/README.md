# Orchestrator Queues Module (`src/orchestrator/queues`)

## Core Objective

Provide foundational queue registry abstractions for queue orchestration with unique naming, queue-level defaults, and lifecycle state.

## Core Ideas

- Queue is an autonomous serializable struct.
- Queue pool enforces unique queue names.
- Queue state (active/paused) is persisted and recoverable.
- Bootstrapping completion is explicitly marked by external orchestrator logic.

## Core Concepts

- `Queue`
  - `id` (UUID assigned on queue creation)
  - `name` (unique string)
  - `config`
    - `concurrency_limit` (optional)
    - `allow_job_overrides` (default `true`)
  - `state` (`active` / `paused`)
- `QueuePool`
- queue registration, lookup, listing
- queue removal with system-queue protection (`_` prefix)
  - pause/resume controls
  - JSON snapshot serialization/deserialization
  - reconstruction from stored queues
  - explicit `mark_bootstrapped()` state
- `PersistentQueuePool` (`persistent.rs`)
  - bootstraps queue pool from persistence
  - creates `_system` queue on first run
  - enforces persist-first/reload-after mutation flow

## Most Relevant Features

- Unique queue registration guard.
- System queues (name starts with `_`) cannot be removed.
- Queue creation returns persistent queue UUID for wire/API callers.
- Serializable queue and pool snapshot structures for storage compatibility.
- Pause/resume state persistence through JSON snapshots.
- Headless reconstruction helper for storage-driven bootstrapping flows.
- Persisted bootstrap preloads queues into memory before runtime networking startup.
