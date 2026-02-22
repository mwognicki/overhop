### Changed
- Heartbeat is now bound directly to the config facade via `Heartbeat::from_app_config(...)`, so interval sourcing comes from `AppConfig` without manual field mapping in runtime bootstrap.

### Architecture
- Added explicit config-to-heartbeat conversion path to keep heartbeat interval wiring centralized in module-level APIs.
