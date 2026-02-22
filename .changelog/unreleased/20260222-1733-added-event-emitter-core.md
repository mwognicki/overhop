### Added
- Added a Node-inspired event emitter module with per-event listener registration, event emission, and sync/async listener channels.
- Synchronous listener failures and panics now propagate as emitter errors and can terminate runtime via `emit_or_exit`.
- Asynchronous listener failures and panics are isolated to background execution and do not fail the emitter call.

### Architecture
- Introduced `src/events/mod.rs` as a detached pub/sub module so domain code can depend on emitter facade APIs without coupling to execution details.
