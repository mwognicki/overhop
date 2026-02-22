### Added
- Added a dedicated configuration module that loads application config from TOML and supports generic dotted-key argv overrides (`--section.key value`).
- Added runtime config file `config/overhop.toml` and template file `config/overhop.template.toml` with logging and heartbeat settings.

### Changed
- Main runtime now loads config through `src/config/mod.rs` and applies logging behavior from config.

### Architecture
- Introduced a detached config facade so domain/business code consumes typed settings instead of parsing raw argv or TOML directly.
