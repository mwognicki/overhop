# Jobs Orchestration Module (`src/orchestrator/jobs`)

## Core Objective

Provide a base jobs pool abstraction for queue-aware job creation with strict enqueue validation and immediate persistence hooks.

## Core Ideas

- A job can only be enqueued into an already existing queue.
- System queues (`_` prefix) cannot accept enqueued jobs.
- Job payload is optional but must be JSON-serializable.
- Each new job is assigned a random UUID and a composed job ID (`<queue-name>:<uuid>`).
- Every accepted enqueue operation triggers immediate persistence through a storage backend interface.

## Core Concepts

- `JobsPool`
  - in-memory jobs registry
  - enqueue validation and lifecycle defaults
  - immediate call into `JobsStorageBackend`
  - reserved persistence flow mode toggle for future store-first strategy
- `NewJobOptions`
  - optional payload (`serde_json::Value`)
  - optional `scheduled_at` (past timestamps are allowed)
  - optional `max_attempts` (`u32`, must be `>= 1` if provided)
  - optional `retry_interval_ms` (`u64`, must be `> 0` if provided)
- `Job`
  - immutable UUID and derived `job_id`
  - default status `new`
  - execution start timestamp from `scheduled_at` or immediate `Utc::now()`
  - `runtime.attempts_so_far` counter initialized to `0`

## Most Relevant Features

- Queue existence is enforced at enqueue time.
- System queue enqueue is rejected at enqueue time.
- Enqueue returns generated job UUID on success.
- Storage failure causes enqueue rollback to keep in-memory pool consistent with persistence state.
