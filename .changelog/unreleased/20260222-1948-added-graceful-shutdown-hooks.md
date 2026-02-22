### Added
- Added a dedicated shutdown hooks module that listens for `SIGINT` and `SIGTERM` to trigger graceful shutdown flow.
- Runtime now performs coordinated shutdown: stop accepting new listener executions, stop heartbeat, best-effort listener drain wait, and terminate TCP server.

### Changed
- Event emitter now supports shutdown mode (`begin_shutdown`) and best-effort idle waiting (`wait_for_idle`), preventing new listener starts during shutdown.

### Architecture
- Added explicit shutdown orchestration path connecting signal hooks, event lifecycle control, heartbeat termination, and TCP server teardown.
