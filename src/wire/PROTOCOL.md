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

### IDENT (server -> client, anonymous challenge)

- Message type: `t=104`
- Request id: server push (`rid="0"`)
- Payload:
  - `register_timeout_seconds` (`int`)
  - `reply_deadline` (`string`, RFC3339)

IDENT is sent to HELLOed-but-unregistered anonymous connections once their
`helloed_unregistered_max_lifetime_seconds` threshold is reached.

Only allowed follow-up from client is `REGISTER`, and it must arrive before
`reply_deadline`.

## Current Anonymous Message Order Constraints

For a new TCP connection (while still anonymous):

1. First client message must be `HELLO` (`t=1`).
2. After `HELLO`, the only allowed client message is `REGISTER` (`t=2`).
3. If IDENT is issued, `REGISTER` must be received before IDENT deadline.

Any order violation is treated as protocol violation.

## Implemented Worker Ping Flow

### PING (worker client -> server)

- Message type: `t=3`
- Allowed only for registered workers (not anonymous connections).
- Payload: must be an empty map.

### PONG (server -> worker client)

- Message type: `t=105`
- Request id: echoes `PING` request id
- Payload:
  - `server_time` (`string`, RFC3339 timestamp)

### Ordering/Blocking Constraint

`PING -> PONG` exchange is handled as blocking request-response. No other wire
message is processed for that worker connection until `PONG` is produced.

## Implemented Worker Queue Query Flow

### GQUEUE (worker client -> server)

- Message type: `t=4`
- Allowed only for registered workers.
- Payload:
  - `q` (`string`): queue name to query

Response (`OK`, `t=101`):

- if queue is found:
  - payload contains full queue metadata
- if queue is not found:
  - payload is empty map

### LQUEUES (worker client -> server)

- Message type: `t=5`
- Allowed only for registered workers.
- Payload: must be empty map.

Response (`OK`, `t=101`):

- payload contains all queues metadata under `queues` array
- no pagination/cap applied (full list always returned)

## Implemented Worker Subscription Flow

### SUBSCRIBE (worker client -> server)

- Message type: `t=6`
- Allowed only for registered workers.
- Payload:
  - `q` (`string`, required): queue name
  - `credits` (`int`, optional): non-negative starting credits (default `0`)

Response:

- success: `OK` (`t=101`) with payload containing:
  - `sid` (`string`, UUID subscription id)
- failure: `ERR` (`t=102`) with standard error payload

Constraints:

- queue must exist
- worker cannot subscribe more than once to same queue
- worker can subscribe to multiple different queues

### UNSUBSCRIBE (worker client -> server)

- Message type: `t=7`
- Allowed only for registered workers.
- Payload:
  - `sid` (`string`, UUID, required): subscription id

Response:

- success: empty `OK` payload
- failure: `ERR` (`t=102`) with standard error payload

## Heartbeat-Driven Connection Purge

A heartbeat listener enforces anonymous lifecycle with effective cadence equal
to the minimum of:

- `wire.session.unhelloed_max_lifetime_seconds`
- `wire.session.helloed_unregistered_max_lifetime_seconds`
- `wire.session.ident_register_timeout_seconds`

With current defaults, this cadence is 2 seconds.

- unhelloed connection max age:
  - controlled by `wire.session.unhelloed_max_lifetime_seconds` (default `10`)
  - timed-out connections are terminated
- helloed but unregistered connection max age:
  - controlled by `wire.session.helloed_unregistered_max_lifetime_seconds` (default `10`)
  - once exceeded, server sends IDENT challenge
- IDENT register reply timeout:
  - controlled by `wire.session.ident_register_timeout_seconds` (default `2`)
  - missing deadline causes ERR + termination
