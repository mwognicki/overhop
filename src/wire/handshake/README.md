# Wire Handshake Module (`src/wire/handshake`)

## Core Objective

Handle minimal handshake-level message exchange used for protocol compatibility checks.

## Current Scope

- `HELLO` (`t=1`) inbound client message handling.
- `HI` (`t=103`) outbound server response generation with empty payload.

## Most Relevant Features

- Decodes/validates inbound frame through wire codec + envelope checks.
- Requires valid client request id semantics.
- Produces `HI` response frame with echoed `rid` for `HELLO`.
- Ignores non-HELLO messages (no response in this module).
