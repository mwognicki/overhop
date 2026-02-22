### Changed
- Server now accepts and tracks persistent full-duplex TCP connections via dedicated connection handles.
- Runtime loop now polls and registers new persistent connections and gracefully shuts down all active connections during shutdown.
- Added `server.tls_enabled` config flag, currently enforced as `false` (TLS not yet implemented).

### Architecture
- Extended server facade with persistent connection registry and explicit connection lifecycle APIs (`try_accept_persistent`, `drop_connection`, `shutdown_all_connections`).
