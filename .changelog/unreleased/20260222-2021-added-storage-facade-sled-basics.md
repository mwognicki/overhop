### Added
- Added a new pluggable storage facade module (`src/storage`) with a current `sled` backend implementation behind trait abstractions.
- Added storage configuration surface under `storage` with explicit engine selection, configurable data path, and optional sled settings (`cache_capacity`, `mode`).
- Added fail-fast storage initialization before TCP server startup, including startup logging from the storage module.

### Changed
- Added default runtime storage configuration using `~/.overhop/data` and documented CLI overrides in config templates.
