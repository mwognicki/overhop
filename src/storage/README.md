# Storage Module (`src/storage`)

## Core Objective

Provide a pluggable storage facade so business/domain modules depend on stable storage APIs rather than a concrete storage engine.

## Core Ideas

- Storage engine selection is explicit in app config via `storage.engine`.
- Current concrete engine is `sled`, but the module is structured for future engine additions.
- Storage initialization is fail-fast and happens before TCP server startup.

## Core Concepts

- `StorageFacade`
  - owns active storage backend
  - stores resolved data path and active engine metadata
  - exposes backend-agnostic lifecycle operation (`flush`)
- `StorageBackend`
  - trait boundary for engine-specific implementations
- `SledStorage`
  - current concrete backend based on `sled::Db`

## Most Relevant Features

- Configurable storage path (`storage.path`) with `~/` and `$HOME` expansion.
- Configurable sled options:
  - `storage.sled.cache_capacity` (optional)
  - `storage.sled.mode` (`low_space` or `high_throughput`, optional)
- Significant initialization events are logged from this module.
- Initialization errors are surfaced to startup flow and terminate application startup.
