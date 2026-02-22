### Added
- Implemented worker `CREDIT` (`t=8`) client-to-server wire message for registered workers.
- Added session validation for `CREDIT` payload requiring `sid` (UUID) and positive `credits` increment.
- Wired runtime handling to increase credits only on subscriptions owned by requesting worker, with standard `OK/ERR` responses.
- Updated wire protocol and module documentation for the CREDIT flow.
