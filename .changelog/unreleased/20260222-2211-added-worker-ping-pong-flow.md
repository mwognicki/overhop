### Added
- Added worker-only wire `PING` (`t=3`) -> `PONG` (`t=105`) request-response flow.
- PING now requires empty payload and PONG now returns current server time in payload.
- Added worker message processing loop that handles PING as blocking request-response and rejects unsupported worker message types with ERR + termination.

### Docs
- Extended wire protocol draft and wire/session module docs with PING/PONG semantics and constraints.
