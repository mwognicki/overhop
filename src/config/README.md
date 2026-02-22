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
- `load_from_toml_with_args(path, args)`: loads TOML, applies argv overrides, deserializes config.

## Most Relevant Features

- Generic argv override shape: `--section.key value`
- Override value type inferred from existing TOML key type.
- Unknown dotted paths are rejected.
- Type mismatches are rejected with explicit error messages.
- Keeps configuration concerns detached from domain/business logic.

## Runtime Files

- Default runtime config: `config/overhop.toml`
- Template config: `config/overhop.template.toml`
