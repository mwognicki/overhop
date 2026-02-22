# Diagnostics Module (`src/diagnostics`)

## Core Objective

Provide runtime diagnostics snapshots as a dedicated module, decoupled from wire handlers and business logic.

## Core Concepts

- Category-level functions:
  - application status
  - memory stats
  - connection/worker pool stats
  - storage summary
  - queue pool stats
- Aggregation function:
  - composes category outputs into one payload for wire/API responses.

## Most Relevant Features

- Keeps diagnostics assembly out of `main.rs` protocol handling.
- Surfaces storage and queue-pool visibility through stable summary shapes.
- Uses best-effort POSIX-friendly memory source (`/proc/self/status` when available).
