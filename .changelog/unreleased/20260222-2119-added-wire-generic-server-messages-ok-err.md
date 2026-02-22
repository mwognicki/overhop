### Added
- Added generic server-to-client wire envelope message types: `OK` (`t=101`) and `ERR` (`t=102`).
- Added `WireEnvelope::ok(...)` helper with optional payload defaulting to empty map.
- Added `WireEnvelope::err(...)` helper with standard error payload fields (`code`, optional `msg`, optional `details`).
- Added server message type validation helpers for `t > 100` and generic message type constraints.
