### Added
- Added heartbeat-driven anonymous connection lifecycle enforcement (effective every 5 seconds):
  - unhelloed connection max lifetime purge
  - helloed-but-unregistered timeout handling with IDENT challenge
  - IDENT reply timeout enforcement with ERR+termination
- Added IDENT server challenge support (`t=104`) with `register_timeout_seconds` and `reply_deadline` payload fields.
- Added wire session timeout configuration keys under `wire.session.*` with defaults:
  - `unhelloed_max_lifetime_seconds = 10`
  - `helloed_unregistered_max_lifetime_seconds = 10`
  - `ident_register_timeout_seconds = 2`

### Changed
- REGISTER handling now rejects late REGISTER replies after IDENT timeout and returns protocol ERR.
- Anonymous pool metadata now tracks optional IDENT reply deadline timestamps.
- Expanded wire protocol draft docs with IDENT and timeout-driven sequencing rules.
