# Jobs Orchestration Module (`src/orchestrator/jobs`)

## Core Objective

Provide a transient jobs staging pool abstraction for queue-aware job creation with strict enqueue validation and event-driven persistence handoff.

## Core Ideas

- A job can only be enqueued into an already existing queue.
- System queues (`_` prefix) cannot accept enqueued jobs.
- Job payload is optional but must be JSON-serializable.
- Each new job is assigned a random UUID and a composed job ID (`<queue-name>:<uuid>`).
- Every accepted enqueue operation stages the job in memory and hands it off through events for persistence.

## Core Concepts

- `JobsPool`
  - temporary in-memory jobs staging registry
  - enqueue validation and lifecycle defaults
  - explicit removal API used after successful persistence
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
  - status progression handled by heartbeat-driven runtime updater:
    - `new` -> `waiting` or `delayed`
    - `delayed` -> `waiting` once execution time is reached

## Most Relevant Features

- Queue existence is enforced at enqueue time.
- System queue enqueue is rejected at enqueue time.
- Enqueue stages and returns generated job UUID on success.
- Persist-and-evict behavior is handled by event listeners (outside this module) to keep memory bounded.
- Shutdown flow performs best-effort transient pool draining and delays exit briefly after full drain.
