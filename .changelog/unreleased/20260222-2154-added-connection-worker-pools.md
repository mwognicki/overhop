### Added
- Added a dedicated pools module with anonymous connections pool and workers pool abstractions.
- Anonymous connection metadata now tracks immutable `connected_at` and optional `helloed_at` timestamps.
- Added promotion flow from anonymous connection to worker with immutable worker UUID and promotion timestamp plus mutable `last_seen_at`.
- Added stubbed termination APIs for anonymous and worker pools accepting optional reason.

### Architecture
- Introduced `ConnectionWorkerPools` coordinator facade to enforce promotion transition semantics (anonymous -> worker).
