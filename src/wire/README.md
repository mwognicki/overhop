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
- `session`: anonymous session ordering + REGISTER flow validation/actions.
  - includes IDENT challenge frame support for stale helloed/unregistered connections
  - includes worker `PING/PONG` request-response helpers
  - includes worker `QUEUE/LSQUEUE` query message parsing helpers
  - includes worker `ADDQUEUE` queue-creation message parsing helpers
  - includes worker `RMQUEUE` queue-removal message parsing helpers
  - includes worker `PAUSE/RESUME` queue-state message parsing helpers
  - includes worker `ENQUEUE` job-creation message parsing helpers
  - includes worker `JOB` lookup message parsing helpers
  - includes worker `RMJOB` removal message parsing helpers
  - includes worker `LSJOB` paginated listing message parsing helpers
  - includes worker `SUBSCRIBE/UNSUBSCRIBE` subscription message parsing helpers
  - includes worker `CREDIT` subscription credit increment parsing helpers
  - includes worker `STATUS` diagnostics snapshot message parsing helpers
- `PROTOCOL.md`: implementation-tracking wire protocol draft/spec.

## Extension Direction

- Add wire orchestrator and message-type handlers as dedicated submodules/crates.
- Keep codec+envelope stable as reusable primitives for inbound/outbound protocol handling.
