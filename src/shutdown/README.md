# Shutdown Module (`src/shutdown`)

## Core Objective

Provide process-level shutdown hooks that capture termination signals and trigger graceful application shutdown flow.

## Core Ideas

- Signal handling is isolated from domain logic.
- Runtime loop polls a shared triggered flag and performs coordinated teardown.
- Graceful shutdown favors best-effort draining over hard blocking.

## Core Concepts

- `ShutdownHooks::install()`: registers signal hooks.
- `is_triggered()`: indicates whether shutdown signal was received.
- Unix signals currently handled:
  - `SIGINT`
  - `SIGTERM`

## Most Relevant Features

- Supports graceful transition from running state to shutdown state.
- Integrates with event-emitter shutdown mode (`begin_shutdown` + `wait_for_idle`).
- Allows heartbeat and server teardown sequencing in runtime bootstrap.
