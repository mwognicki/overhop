### Architecture
- Extracted non-entrypoint helpers from `src/main.rs` into dedicated modules (`src/wire/session/runtime.rs`, `src/orchestrator/jobs/mod.rs`, `src/utils/runtime.rs`, `src/utils/startup_banner.rs`, and `src/config/mod.rs`) so `main.rs` contains only startup/runtime orchestration.

### Changed
- Added shared JSON-to-wire conversion helpers to `src/wire/codec/mod.rs` and updated runtime wiring to use module-owned helper APIs.
