### Added
- Added dedicated `diagnostics` module with separate category functions for application status, memory stats, pool stats, storage summary, and queue pool stats.
- Implemented worker `STATUS` (`t=10`) wire message (client -> server, registered workers only) with diagnostics snapshot response.

### Changed
- Worker session grammar now validates `STATUS` payload as empty map.
- Worker protocol runtime now returns `ERR` when diagnostics collection fails.
