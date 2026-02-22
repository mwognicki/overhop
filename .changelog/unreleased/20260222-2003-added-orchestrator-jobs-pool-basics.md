### Added
- Added `src/orchestrator/jobs` with a base `JobsPool` that enforces queue existence when enqueuing new jobs.
- Added `NewJobOptions` support for optional JSON payload, scheduled execution timestamp, max attempts, and positive retry interval.
- Added generated job identity semantics (`<queue-name>:<uuid>`) with default `new` status and immediate-or-scheduled execution start timestamp.
- Added immediate persistence backend hook (`JobsStorageBackend`) with rollback-on-persist-failure behavior.

### Architecture
- Introduced a reserved persistence flow mode marker to prepare future store-first/refresh-pool orchestration without changing job API shape now.
