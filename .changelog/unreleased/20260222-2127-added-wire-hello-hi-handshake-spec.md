### Added
- Added basic wire handshake module implementing `HELLO` (client `t=1`) -> `HI` (server `t=103`) with echoed request id and empty payload.
- Integrated handshake handling into anonymous connection processing loop and persisted anonymous `helloed_at` timestamp on successful HELLO.
- Added initial wire protocol spec document (`src/wire/PROTOCOL.md`) covering framing, payload constraints, generic message conventions, and HELLO/HI flow.

### Docs
- Added module README for `src/wire/handshake` and updated wire module overview documentation.
