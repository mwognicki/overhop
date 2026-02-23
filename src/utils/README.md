# Utils Module (`src/utils`)

## Core Objective

Provide small reusable helpers that are cross-cutting and not tied to a single domain module.

## Current Scope

- `timing`: scoped execution-time measurement helper with debug-level logging.

## Most Relevant Features

- Supports named timers for code sections.
- Logs elapsed execution time with human-friendly formatting while still measured in milliseconds.
- Exposes closure wrapper style (`measure_execution`) and scope-style timer (`ScopedExecutionTimer`).
