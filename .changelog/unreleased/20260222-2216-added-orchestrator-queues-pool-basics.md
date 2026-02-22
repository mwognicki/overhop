### Added
- Added a base queue orchestration module with a queue pool that stores registered queues with unique names.
- Queue model now supports optional default per-queue config (`concurrency_limit`, `allow_job_overrides` with default `true`).
- Added queue lifecycle state controls (`active`/`paused`) with serializable state persistence.
- Added headless reconstruction helpers from serialized JSON snapshots and explicit bootstrap state marking.

### Architecture
- Introduced `src/orchestrator/queues` as an autonomous queue registry foundation separated from transport and runtime bootstrap logic.
