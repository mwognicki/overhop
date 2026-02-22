# Events Module (`src/events`)

## Core Objective

Provide a Node-inspired event emitter facade for internal pub/sub flow, with explicit separation between runtime-critical synchronous listeners and isolated asynchronous listeners.

## Core Ideas

- Register listeners per event name.
- Emit events with optional JSON payload.
- Choose sync vs async listeners based on failure semantics.

## Core Concepts

- `EventEmitter`: main facade used by domain code
- `Event`: `{ name, payload }`
- `on(...)`: register synchronous listener (`Fn(&Event) -> Result<(), String>`)
- `on_async(...)`: register asynchronous listener (`Fn(Event) -> Result<(), String>`)
- `emit(...)`: run sync listeners first, then dispatch async listeners
- `emit_or_exit(...)`: runtime-enforced exit path for sync failures
- `begin_shutdown()`: prevents new listener execution starts
- `wait_for_idle(timeout)`: best-effort wait for running listeners to finish

## Most Relevant Features

- Per-event listener registries for sync and async channels.
- Synchronous errors are propagated as `EmitError` and can terminate runtime.
- Panic best-effort recovery for listeners via `catch_unwind`.
- Asynchronous listener failures are isolated and reported without failing the emitter call.
- Payload support via optional `serde_json::Value`.
- Shutdown-aware listener lifecycle control for graceful teardown.

## Extension Direction

- Add listener lifecycle APIs (`once`, `off`, max listeners) without changing domain-facing emit flow.
- Swap async execution strategy (threads -> task runtime/queue workers) behind module internals.
