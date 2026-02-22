### Added
- Added a dedicated `server` module with a non-blocking TCP listener facade.
- Added configurable server host and port (`server.host`, `server.port`) with defaults `0.0.0.0:9876`.
- Added startup INFO logging with app name/version and bound address metadata.

### Architecture
- Added `TcpServer::from_app_config(...)` to keep server binding directly coupled to typed config and detached from domain logic.
