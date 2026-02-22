# Heartbeat Module (`src/heartbeat`)

## Core Objective

Provide a lightweight heartbeat service that emits regular `on-heartbeat` events through the event emitter, with strict interval bounds and stable initialization metadata.

## Core Ideas

- Heartbeat is a dedicated module with isolated lifecycle (`start`/`stop`).
- Emission is event-driven through `EventEmitter`, not direct callbacks.
- Initialization timestamp is stable and publicly available for listener-side diff calculations.

## Core Concepts

- `HEARTBEAT_EVENT`: canonical event name (`on-heartbeat`)
- `HeartbeatConfig`:
  - `interval_ms` in range `100..=1000`
  - default `1000`
- `Heartbeat`:
  - public immutable initiation timestamp: `initiated_at`
  - `from_app_config(...)` binds heartbeat setup directly to `AppConfig`
  - `start()` launches periodic emitter loop
  - `stop()` shuts down worker loop
- `initial_metadata_payload()`: JSON metadata useful for startup logging

## Most Relevant Features

- Interval validation with explicit error for out-of-range values.
- Periodic event emission includes:
  - `initiated_at`
  - `emitted_at`
  - `interval_ms`
- Runtime-critical listener failures propagate via emitter semantics (`emit_or_exit`).
- Background thread lifecycle managed with stop signal and join behavior.

## Listener Guidance

- Use `initiated_at` from payload with current timestamp to calculate elapsed-time diffs.
- This supports listener-side logic that should execute only when elapsed diff exceeds one second.
