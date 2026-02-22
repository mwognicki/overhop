# Config Module (`src/config`)

## Core Objective

Provide a centralized application configuration facade that loads defaults from TOML and applies generic argv overrides without hardcoded CLI flags.

## Core Ideas

- TOML is the default source of truth for all runtime settings.
- Any existing TOML key can be overridden by argv using dotted path notation.
- Parsing is schema-aware by TOML key type, not by hardcoded argument list.

## Core Concepts

- `AppConfig`: root strongly-typed configuration.
- `logging` section:
  - `level`
  - `human_friendly`
- `heartbeat` section:
  - `interval_ms`
- `server` section:
  - `host`
  - `port`
  - `tls_enabled` (currently must remain `false` until TLS transport is implemented)
- `wire` section:
  - `max_envelope_size_bytes`
  - `session.unhelloed_max_lifetime_seconds` (default `10`)
  - `session.helloed_unregistered_max_lifetime_seconds` (default `10`)
  - `session.ident_register_timeout_seconds` (default `2`)
- `storage` section:
  - `engine` (currently `sled`)
  - `path` (default `~/.overhop/data`, supports `~/` and `$HOME/` prefixes)
  - `sled.cache_capacity` (optional)
  - `sled.mode` (optional: `low_space` or `high_throughput`)
- `load_from_toml_with_args(path, args)`: loads TOML, applies argv overrides, deserializes config.
- `load_with_discovery(args)`: discovers `config.toml` using runtime path precedence, then applies argv overrides.

## Most Relevant Features

- Generic argv override shape: `--section.key value`
- Override value type inferred from existing TOML key type.
- Unknown dotted paths are rejected.
- Type mismatches are rejected with explicit error messages.
- Config discovery order:
  - `<binary-dir>/config.toml`
  - `$HOME/.overhop/config.toml`
  - `/etc/overhop/config.toml`
- Keeps configuration concerns detached from domain/business logic.

## Runtime Files

- Default runtime config template in repo: `config/config.toml`
- Template config: `config/config.template.toml`
