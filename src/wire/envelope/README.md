# Wire Envelope Module (`src/wire/envelope`)

## Core Objective

Define the fixed wire-level envelope abstraction shared by all message types, independent from concrete payload specs.

## Envelope Shape

Every frame payload map must expose:

- `v`: protocol version (`int`, fixed `2`)
- `t`: message type (`int`)
- `rid`: request id (`string`)
- `p`: payload (`map`)

Unknown fields are ignored.

## Core Concepts

- `WireEnvelope`: typed abstraction over the fixed envelope header + payload map.
- `from_raw(...)` / `into_raw(...)`: conversion to and from codec map representation.
- Directional request-id checks:
  - client -> server: `rid` required
  - server push: `rid == "0"`
  - server response: echoes request `rid`
- `not_implemented_error(...)`: helper for `ERR(code="NOT_IMPLEMENTED")` response envelope.
