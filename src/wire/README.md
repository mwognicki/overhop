# Wire Module (`src/wire`)

## Core Objective

Provide transport-level wire protocol building blocks while keeping framing/serialization concerns detached from the future message orchestrator.

## Core Ideas

- Split wire implementation into focused submodules.
- Start with a strict codec layer before building protocol orchestration.
- Keep frame and payload validation centralized.

## Current Scope

- `codec`: MessagePack framing and value validation with strict protocol limits.

## Extension Direction

- Add wire orchestrator and message-type handlers as dedicated submodules/crates.
- Keep codec stable as a reusable primitive for inbound/outbound protocol handling.
