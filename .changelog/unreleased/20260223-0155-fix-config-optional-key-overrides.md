### Fixed
- Fixed argv override handling for optional keys missing in config files (e.g. `--storage.self_debug_path`).
- Config file loading now merges file values over built-in defaults before applying overrides.
