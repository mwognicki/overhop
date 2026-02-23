# Overhop Roadmap

## Disclaimer

This roadmap is mutable and may change frequently while the project remains experimental.
Until the best repeatable workflow for LLM-assisted development is established, planning is intentionally adapted through trial-and-error and ongoing observations.

## Implemented Scope (Current)

The table below groups features already implemented in the application, based on module-level `README.md` files in the repository.

| Category | Modules | Implemented Features |
| --- | --- | --- |
| Runtime Foundations | `src/config`, `src/server`, `src/shutdown`, `src/utils` | Typed TOML+argv config loading with discovery and defaults, non-blocking persistent TCP server startup, signal-driven graceful shutdown hooks, timing utility for measured execution logs. |
| Observability | `src/logging`, `src/diagnostics` | Pluggable logger facade with structured payload support and log levels (`ERROR..VERBOSE`), human-friendly formatting mode, diagnostics payload builders for app/memory/pools/storage/queues status. |
| Eventing & Heartbeat | `src/events`, `src/heartbeat` | Event emitter with sync/async listeners and shutdown-aware lifecycle, heartbeat module with bounded interval config, initiated timestamp metadata, periodic maintenance/event dispatch integration. |
| Connection & Worker Lifecycle | `src/pools` | Anonymous connection pool, promotion to worker pool, immutable worker identity/timestamps, subscription and credits lifecycle APIs, termination helpers with best-effort socket shutdown. |
| Queue Orchestration | `src/orchestrator/queues` | Queue registry with uniqueness and name validation, system-queue protections, pause/resume state, persistent bootstrap/reload flow, serializable queue snapshots and persisted queue UUIDs. |
| Job Orchestration | `src/orchestrator/jobs` | Queue-aware enqueue validation, transient staging pool, generated `jid` format (`<queue>:<uuid>`), retries metadata, event-driven persist-and-evict flow, shutdown drain behavior. |
| Storage & Persistence | `src/storage` | Pluggable storage facade with sled backend, versioned immutable keying strategy, queue/job persistence, status FIFO index, queue+status paginated retrieval, persisted queue-status counters updated on insert/transition/delete. |
| Wire Core | `src/wire/codec`, `src/wire/envelope`, `src/wire/handshake` | MessagePack framing with strict type/size validation, fixed envelope contract (`v/t/rid/p`) and generic `OK`/`ERR`, HELLO/HI handshake path with strict request-id semantics. |
| Wire Session Grammar | `src/wire/session`, `src/wire/PROTOCOL.md` | Anonymous sequencing and REGISTER flow, worker grammar for queue/job/subscription/status operations, protocol violation handling, IDENT/PONG builders, message parsing/validation for implemented message types. |
| Self-Debug Workflow | `src/self_debug` | In-process self-debug client mode (`--self-debug`), isolated storage path and cleanup controls, decoded IN/OUT wire logging with message names, scripted protocol verification path (including enqueue/job/list/remove/stats checks). |

## Notes

- This document tracks delivered functionality only.
- Planned/future work should be added incrementally as research direction stabilizes.
