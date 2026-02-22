### Added
- Added a dedicated heartbeat module that emits `on-heartbeat` events at configurable intervals between 100ms and 1000ms (default 1000ms).
- Heartbeat now exposes a public immutable initiation timestamp and includes initiation metadata in emitted payloads.
- Added startup logging support for initial heartbeat metadata via a JSON payload helper.

### Architecture
- Introduced `src/heartbeat/mod.rs` as an isolated module built on top of the event emitter facade, with independent lifecycle controls.
