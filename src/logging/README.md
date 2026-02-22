# Logging Module (`src/logging`)

## Core Objective

Provide a lightweight, pluggable logging facade that keeps domain/business code decoupled from output formatting and backend transport details.

## Core Ideas

- Business code talks only to `Logger` and log-level APIs.
- Output and transport details stay behind `LogSink`.
- Logging behavior is configured centrally via `LoggerConfig`.

## Core Concepts

- `LogLevel`: `ERROR`, `WARN`, `INFO`, `DEBUG`, `VERBOSE`
- `LoggerConfig`:
  - `min_level` (default: `DEBUG`)
  - `human_friendly` (default: `false`)
- `LogSink` trait: extension point for alternate sinks/backends
- `StdoutSink`: default sink implementation

## Most Relevant Features

- Full UTC datetime at the leftmost part of each log line.
- Optional context tag (for module/function call-site metadata).
- Optional JSON payload (`serde_json::Value`) alongside message text.
- Optional colorful level rendering in human-friendly mode.
- Level-based filtering with predictable default behavior.
- Config-friendly level parsing via string values (`error`, `warn`, `info`, `debug`, `verbose`).

## Extension Direction

- Add sinks for files, structured streams, or external collectors by implementing `LogSink`.
- Keep any future formatting or transport changes inside this module so domain code remains unchanged.
