### Changed
- Added immutable/versioned storage keyspace strategy for sled (`v1:q:<queue-name>` and `v1:j:<job-uuid>`).
- Extended storage facade with queue persistence APIs (`load_queues`, `replace_queues`) and job-by-UUID retrieval path.
- Added persistent queue bootstrap flow that initializes `_system` on first run and preloads queue state from persistence.
- Queue mutation flow now supports persist-first then in-memory reload semantics via `PersistentQueuePool`.

### Reliability
- Application shutdown now explicitly persists and flushes queue state before exit.
