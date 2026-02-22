# Overhop Wire Protocol (Draft)

## Status

Draft, implementation-backed, version `2`.

## Framing

Each wire frame is:

`| u32_be length (4B) | payload (length) |`

Rules:

- `length` is the payload size in bytes.
- `length == 0` is a protocol error.
- `length > max_envelope_size_bytes` is a protocol error.
- Current configurable payload limit range: `65536..=33554432` bytes.
- Current default payload limit: `8388608` bytes (8 MiB).

## Payload Encoding

Payload is MessagePack-encoded map.

Allowed value types (recursive):

- signed int64-range integers
- UTF-8 strings
- bool
- nil
- arrays
- maps with UTF-8 string keys
- binary (`bin`)

Rejected types:

- float (`f32` / `f64`)
- MessagePack extension values (`ext`)

## Base Envelope

Each payload map must include:

- `v` (`int`): protocol version, fixed `2`
- `t` (`int`): message type
- `rid` (`string`): request id
- `p` (`map`): type-specific payload

Unknown envelope fields are ignored.

## Message Type Conventions

- Client -> server messages use `t <= 100`.
- Server -> client messages use `t > 100`.

Generic server responses currently reserved:

- `OK` = `t=101` with optional payload (default empty map)
- `ERR` = `t=102` with standard payload:
  - `code` (`string`, required)
  - `msg` (`string`, optional)
  - `details` (optional)

## Request ID Rules

- Client -> server requests: `rid` is required and non-empty.
- Server -> client response: should echo request `rid`.
- Server pushes initiated first by server use `rid="0"`.

## Implemented Handshake

### HELLO (client -> server)

- Message type: `t=1`
- Payload: currently ignored (empty map recommended)
- Effect: server stores anonymous connection `helloed_at` timestamp.

### HI (server -> client)

- Message type: `t=103`
- Request id: echoes HELLO `rid`
- Payload: empty map

This HELLO/HI exchange is a protocol agreement sanity check between nodes.

## Implemented Registration Flow

### REGISTER (client -> server, anonymous only)

- Message type: `t=2`
- Allowed only after successful `HELLO -> HI`.
- Payload: must be an empty map.
- On success:
  - anonymous connection is promoted to worker pool
  - server responds with generic `OK` (`t=101`) and payload:
    - `wid` (`string`, UUID worker id)
- On failure:
  - server responds with generic `ERR` (`t=102`) and closes anonymous connection

## Current Anonymous Message Order Constraints

For a new TCP connection (while still anonymous):

1. First client message must be `HELLO` (`t=1`).
2. After `HELLO`, the only allowed client message is `REGISTER` (`t=2`).

Any order violation is treated as protocol violation.
