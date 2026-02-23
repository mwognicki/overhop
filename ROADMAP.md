# Overhop Roadmap

## Disclaimer

This roadmap is mutable and may change frequently while the project remains experimental.
Until the best repeatable workflow for LLM-assisted development is established, planning is intentionally adapted through trial-and-error and ongoing observations.

## Implemented Scope (Current)

### Runtime Foundations

| Feature | Comments | Status |
| --- | --- | --- |
| Typed TOML+argv config loading with discovery and defaults | `src/config` | ✅ |
| Non-blocking persistent TCP server startup | `src/server` | ✅ |
| Signal-driven graceful shutdown hooks | `src/shutdown` | ✅ |
| Timing utility for measured execution logs | `src/utils` | ✅ |

### Observability

| Feature | Comments | Status |
| --- | --- | --- |
| Pluggable logger facade with structured payload support and log levels (`ERROR..VERBOSE`) | `src/logging` | ✅ |
| Human-friendly formatting mode | `src/logging` | ✅ |
| Diagnostics payload builders for app/memory/pools/storage/queues status | `src/diagnostics` | ✅ |

### Eventing & Heartbeat

| Feature | Comments | Status |
| --- | --- | --- |
| Event emitter with sync/async listeners and shutdown-aware lifecycle | `src/events` | ✅ |
| Heartbeat module with bounded interval config | `src/heartbeat` | ✅ |
| Initiated timestamp metadata | `src/heartbeat` | ✅ |
| Periodic maintenance/event dispatch integration | `src/heartbeat`, `src/main.rs` | ✅ |

### Connection & Worker Lifecycle

| Feature | Comments | Status |
| --- | --- | --- |
| Anonymous connection pool | `src/pools` | ✅ |
| Promotion to worker pool | `src/pools` | ✅ |
| Immutable worker identity/timestamps | `src/pools` | ✅ |
| Subscription and credits lifecycle APIs | `src/pools` | ✅ |
| Termination helpers with best-effort socket shutdown | `src/pools` | ✅ |

### Queue Orchestration

| Feature | Comments | Status |
| --- | --- | --- |
| Queue registry with uniqueness and name validation | `src/orchestrator/queues` | ✅ |
| System-queue protections | `src/orchestrator/queues` | ✅ |
| Pause/resume state | `src/orchestrator/queues` | ✅ |
| Persistent bootstrap/reload flow | `src/orchestrator/queues/persistent.rs` | ✅ |
| Serializable queue snapshots and persisted queue UUIDs | `src/orchestrator/queues` | ✅ |

### Job Orchestration

| Feature | Comments | Status |
| --- | --- | --- |
| Queue-aware enqueue validation | `src/orchestrator/jobs` | ✅ |
| Transient staging pool | `src/orchestrator/jobs` | ✅ |
| Generated `jid` format (`<queue>:<uuid>`) | `src/orchestrator/jobs` | ✅ |
| Retries metadata | `src/orchestrator/jobs` | ✅ |
| Event-driven persist-and-evict flow | `src/orchestrator/jobs`, `src/main.rs` | ✅ |
| Shutdown drain behavior | `src/orchestrator/jobs`, `src/main.rs` | ✅ |

### Storage & Persistence

| Feature | Comments | Status |
| --- | --- | --- |
| Pluggable storage facade with sled backend | `src/storage` | ✅ |
| Versioned immutable keying strategy | `src/storage` | ✅ |
| Queue/job persistence | `src/storage` | ✅ |
| Status FIFO index | `src/storage` | ✅ |
| Queue+status paginated retrieval | `src/storage` | ✅ |
| Persisted queue-status counters updated on insert/transition/delete | `src/storage` | ✅ |

### Wire Core

| Feature | Comments | Status |
| --- | --- | --- |
| MessagePack framing with strict type/size validation | `src/wire/codec` | ✅ |
| Fixed envelope contract (`v/t/rid/p`) and generic `OK`/`ERR` | `src/wire/envelope` | ✅ |
| HELLO/HI handshake path with strict request-id semantics | `src/wire/handshake` | ✅ |

### Wire Session Grammar

| Feature | Comments | Status |
| --- | --- | --- |
| Anonymous sequencing and REGISTER flow | `src/wire/session` | ✅ |
| Worker grammar for queue/job/subscription/status operations | `src/wire/session`, `src/wire/PROTOCOL.md` | ✅ |
| Protocol violation handling | `src/wire/session` | ✅ |
| IDENT/PONG builders | `src/wire/session` | ✅ |
| Message parsing/validation for implemented message types | `src/wire/session` | ✅ |

### Self-Debug Workflow

| Feature | Comments | Status |
| --- | --- | --- |
| In-process self-debug client mode (`--self-debug`) | `src/self_debug` | ✅ |
| Isolated storage path and cleanup controls | `src/self_debug`, `src/config` | ✅ |
| Decoded IN/OUT wire logging with message names | `src/self_debug` | ✅ |
| Scripted protocol verification path (enqueue/job/list/remove/stats checks) | `src/self_debug` | ✅ |

## Notes

- This document tracks delivered functionality only.
- Planned/future work should be added incrementally as research direction stabilizes.
