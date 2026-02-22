### Changed
- Wire codec max payload/frame bound is now configurable through `wire.max_envelope_size_bytes` in app config.
- Runtime now initializes codec from `AppConfig` and logs the effective max envelope size.

### Architecture
- Added bounded codec configuration model with enforced range `65536..=33554432` bytes (default `8388608`).
