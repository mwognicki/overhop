# Orchestrator Module (`src/orchestrator`)

## Core Objective

Host queue orchestration building blocks as modular components separated from transport and runtime bootstrap concerns.

## Current Scope

- `queues`: queue registry/pool abstractions with queue lifecycle state and bootstrap tracking.

## Extension Direction

- Add job scheduling, dispatching, retry, and worker-assignment orchestration submodules.
- Keep each orchestration concern in dedicated files/modules for maintainability.
