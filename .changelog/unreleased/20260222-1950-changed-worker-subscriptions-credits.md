### Changed
- Extended worker metadata with queue `Subscription` entries (subscription UUID, queue name, and credits defaulting to `0`).
- Added worker-pool APIs to subscribe/unsubscribe workers and to add/subtract credits per subscription.
- Worker subscription now validates queue existence against the queues pool and rejects duplicate queue subscriptions per worker.

### Architecture
- Evolved connection/worker pools toward queue-aware worker state while keeping queue orchestration ownership in `src/orchestrator/queues`.
