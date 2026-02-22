# Wire Module (`src/wire`)

## Core Objective

Provide transport-level wire protocol building blocks while keeping framing/serialization concerns detached from the future message orchestrator.

## Core Ideas

- Split wire implementation into focused submodules.
- Start with strict codec and envelope layers before building protocol orchestration.
- Keep frame, payload, and fixed envelope validation centralized.

## Current Scope

- `codec`: MessagePack framing and value validation with strict protocol limits.
- `envelope`: fixed wire envelope abstraction (`v/t/rid/p`) with unknown-field-tolerant parsing.
- `handshake`: minimal HELLO/HI protocol agreement exchange.
- `PROTOCOL.md`: implementation-tracking wire protocol draft/spec.

## Extension Direction

- Add wire orchestrator and message-type handlers as dedicated submodules/crates.
- Keep codec+envelope stable as reusable primitives for inbound/outbound protocol handling.
