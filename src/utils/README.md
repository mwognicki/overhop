# Utils Module (`src/utils`)

## Core Objective

Provide small reusable helpers that are cross-cutting and not tied to a single domain module.

## Current Scope

- `timing`: scoped execution-time measurement helper with debug-level logging.
- `runtime`: POSIX runtime guard used by startup bootstrap.
- `startup_banner`: decorative startup banner output helper.

## Most Relevant Features

- Supports named timers for code sections.
- Logs elapsed execution time with human-friendly formatting while still measured in milliseconds.
- Exposes closure wrapper style (`measure_execution`) and scope-style timer (`ScopedExecutionTimer`).
- Provides explicit POSIX-only runtime guard (`ensure_posix_or_exit`).
- Centralizes startup banner output (`print_startup_banner`) outside `main.rs`.
