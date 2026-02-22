# Wire Codec Module (`src/wire/codec`)

## Core Objective

Implement strict binary framing and MessagePack serialization for `MessageEnvelope` payloads.

## Core Ideas

- Frame format is fixed: `u32_be_length + payload`.
- Payloads are MessagePack maps with strict recursive type validation.
- Invalid or oversized frames/payloads are protocol errors.

## Core Concepts

- Max encoded payload: `8 MiB`.
- Allowed value types (recursive):
  - int (`<= i64`)
  - string (UTF-8)
  - bool
  - nil
  - array
  - map (string keys)
  - bin
- Rejected types:
  - floats
  - extension types

## Most Relevant Features

- Outbound and inbound size enforcement.
- Length-prefixed frame encoder/decoder with big-endian `u32` header.
- Explicit protocol violations for `length == 0` and `length > 8 MiB`.
- Top-level envelope constrained to map with string keys.
