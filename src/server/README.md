# Server Module (`src/server`)

## Core Objective

Provide a dedicated TCP server facade that binds and exposes a non-blocking listener using configuration-driven host and port values.

## Core Ideas

- Server startup and socket binding are isolated from domain logic.
- Runtime uses typed config (`AppConfig.server`) to build server state.
- Non-blocking mode is enforced as part of startup behavior.

## Core Concepts

- `ServerConfig`:
  - `host` (default: `0.0.0.0`)
  - `port` (default: `9876`)
- `TcpServer::bind(&ServerConfig)`: binds listener and sets non-blocking mode.
- `TcpServer::from_app_config(&AppConfig)`: binds server directly from config module values.
- `local_addr()`: returns effective bound socket address.

## Most Relevant Features

- Default host/port constants for predictable startup behavior.
- Non-blocking TCP listener via `set_nonblocking(true)`.
- Explicit startup errors for bind and non-blocking setup failures.
- Startup metadata can be logged by the application layer (app name, version, bind address).

## Extension Direction

- Add connection accept loop and protocol framing in this module.
- Keep transport concerns detached from orchestration/business modules.
