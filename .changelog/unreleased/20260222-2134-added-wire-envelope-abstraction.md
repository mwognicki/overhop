### Added
- Added a base wire envelope abstraction module that models the fixed envelope fields `v`, `t`, `rid`, and `p`.
- Added conversion between raw codec maps and typed envelope representation, ignoring unknown envelope fields.
- Added request-id direction validators for client request, server response echoing, and server push (`rid = "0"`).
- Added helper for unknown message type handling via `ERR(code="NOT_IMPLEMENTED")` envelope construction.

### Architecture
- Introduced `src/wire/envelope` as protocol-agnostic fixed header abstraction before concrete message-type modules.
