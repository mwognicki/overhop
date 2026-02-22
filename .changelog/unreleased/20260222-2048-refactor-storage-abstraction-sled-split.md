### Architecture
- Refactored storage module to separate abstraction/facade layers from sled concrete implementation.
- Moved sled-specific logic into `src/storage/sled_backend.rs` while keeping backend traits and facade orchestration in dedicated files.
- Preserved existing storage facade API and immutable keyspace behavior while improving module boundaries.
