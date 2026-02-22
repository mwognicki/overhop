### Changed
- Configuration loading now discovers `config.toml` in this order: binary directory, `$HOME/.overhop/config.toml`, then `/etc/overhop/config.toml`.
- Config file naming was normalized to `config.toml` (`config/config.toml` and `config/config.template.toml` in-repo).

### Architecture
- Config module now provides discovery-based loading (`load_with_discovery`) so runtime bootstrap remains detached from hardcoded file paths in `main.rs`.
