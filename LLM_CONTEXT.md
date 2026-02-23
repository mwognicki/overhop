# LLM Context Entry Point

This file defines repository-specific working rules for LLMs collaborating on Overhop.

## Core Rules

1. After each implementation task, ask whether the user wants the changes committed.
2. If a commit is requested, propose a clear, human-readable commit message before committing.
3. After each implementation, evaluate whether `LLM_CONTEXT.md` should be extended with newly learned, reusable repository context.
4. Always update `LLM_CONTEXT.md` when the user explicitly asks to do so.
5. RustRover is the primary IDE workflow. Do not download crates manually; only add or modify dependencies in `Cargo.toml` and let the IDE/tooling resolve them.
6. Logical modules should generally be designed to support independent development and extension, detached from `main.rs` where practical.
7. Follow commit-coupled changelog policy:
   - For meaningful commits, include at least one `.changelog/unreleased/*.md` fragment.
   - Use readable Conventional Commit messages.
   - Keep fragment entries concise and release-note oriented.
8. Use branch-based development flow:
   - Create and work on `feat/*`, `quickfix/*`, or `chore/*` branches.
   - Avoid direct pushes to `main`.
   - Prefer merging through PRs from topic branch to `main`.
   - Prefer generating PR body from `.changelog/unreleased` fragments via `scripts/generate_pr_body_from_changelog.sh` (or optional workflow `.github/workflows/pr-body-from-changelog.yml`).
9. Logging is implemented as a pluggable facade in `src/logging/mod.rs`:
   - Keep domain/business code coupled only to `Logger` API.
   - Keep output/backend details behind the `LogSink` abstraction.
10. Event pub/sub is implemented as a dedicated facade in `src/events/mod.rs`:
   - Use sync listeners when failures must impact runtime flow.
   - Use async listeners for isolated background reactions.
11. Module-level docs policy:
   - Create `README.md` inside each new logical module directory.
   - When a module changes meaningfully, update its module `README.md` in the same change so docs stay synchronized with code.
12. Application configuration lives in `src/config/mod.rs`:
   - Keep TOML as the default config source.
   - If `config.toml` is not found in discovery locations, fall back to built-in defaults and continue startup.
   - Merge discovered TOML over built-in defaults before applying argv overrides so optional config keys remain overrideable.
   - Keep argv overrides generic using dotted key paths (`--section.key value`) without hardcoded flag mappings.
   - Keep config discovery precedence: `<binary-dir>/config.toml`, then `$HOME/.overhop/config.toml`, then `/etc/overhop/config.toml`.
   - Keep wire session timeout controls configurable under `wire.session.*` keys.
   - Heartbeat maintenance cadence for anonymous wire lifecycle should equal min of wire session timeout controls.
13. Heartbeat is implemented in `src/heartbeat/mod.rs`:
   - Keep interval bounds strict (`100..=1000` ms, default `1000`).
   - Preserve stable initiation timestamp semantics for listener-side diff calculations.
14. TCP server startup is implemented in `src/server/mod.rs`:
   - Keep listener non-blocking.
   - Keep host/port sourced from `AppConfig.server` (default `0.0.0.0:9876`).
   - Keep accepted connections persistent and full-duplex.
   - TLS is not enabled yet; `server.tls_enabled` must remain `false` until TLS transport is implemented.
15. Graceful shutdown flow is coordinated via `src/shutdown/mod.rs` and `src/events/mod.rs`:
   - Handle `SIGINT`/`SIGTERM` through shutdown hooks.
   - On shutdown, prevent new listener starts and use best-effort listener drain waiting.
16. Wire protocol codec lives in `src/wire/codec/mod.rs`:
   - Preserve strict frame format (`u32_be length + payload`) and configurable payload bound.
   - Keep configured payload bound constrained to `65536..=33554432` bytes (default `8388608`).
   - Keep recursive type validation strict (no floats, no MessagePack extension values, map keys must be UTF-8 strings).
17. Base wire envelope abstraction lives in `src/wire/envelope/mod.rs`:
   - Keep fixed envelope fields (`v`, `t`, `rid`, `p`) mandatory and version fixed at `2`.
   - Keep unknown envelope fields ignored during parsing.
   - Reserve generic server-to-client message types `OK` (`t=101`) and `ERR` (`t=102`), with `ERR` carrying standard payload (`code`, optional `msg`, optional `details`).
18. Wire handshake basics live in `src/wire/handshake/mod.rs`:
   - HELLO (`t=1`) from client must produce HI (`t=103`) from server with echoed `rid` and empty payload.
   - Successful HELLO handling must update anonymous connection `helloed_at` timestamp.
19. Anonymous wire session sequencing lives in `src/wire/session/mod.rs`:
   - HELLO must be the first message on anonymous connections.
   - After HELLO, only REGISTER (`t=2`, empty payload) is currently allowed.
   - Successful REGISTER must promote anonymous connection to worker and respond with generic OK containing `wid`.
   - Stale HELLOed-but-unregistered connections must receive IDENT (`t=104`) with REGISTER reply deadline metadata.
   - Registered workers may use PING (`t=3`, empty payload) and must receive PONG (`t=105`) with current server time.
   - PING/PONG should remain blocking at connection level (respond with PONG before processing any subsequent worker message).
   - Registered workers may use QUEUE (`t=4`, payload `q`) and LSQUEUE (`t=5`, empty payload) to query queue metadata via OK responses.
   - Registered workers may use ADDQUEUE (`t=9`, payload `name` + optional `config`) to create persisted queues and receive assigned `qid`.
   - Registered workers may use SUBSCRIBE (`t=6`, payload `q` plus optional non-negative `credits`) and UNSUBSCRIBE (`t=7`, payload `sid`) for queue subscription lifecycle.
   - Registered workers may use CREDIT (`t=8`, payload `sid` + positive `credits`) to increase credits on their own subscriptions only.
   - Registered workers may use RMQUEUE (`t=11`, payload `q`) to remove non-system queues only (`_`-prefixed queues are protected).
   - Registered workers may use PAUSE (`t=12`, payload `q`) and RESUME (`t=13`, payload `q`) for non-system queue state transitions.
   - Registered workers may use ENQUEUE (`t=14`) to push jobs to non-system queues, with optional `job_payload`, `scheduled_at`, `max_attempts`, and `retry_interval_ms`.
   - Registered workers may use JOB (`t=15`, payload `jid`) to fetch persisted job records by id; system-queue jobs remain inaccessible.
20. Connection/worker pools are implemented in `src/pools/mod.rs`:
   - New TCP connections must enter anonymous pool with `connected_at` and optional `helloed_at`.
   - Anonymous metadata should track optional IDENT reply deadline timestamp when IDENT challenge is issued.
   - Promotion must remove from anonymous pool and create worker with immutable UUID/promoted timestamp and mutable `last_seen_at`.
   - Worker metadata must support queue subscriptions identified by subscription UUID, with per-subscription credits counter defaulting to `0`.
   - Queue subscription mutations must validate queue existence against queue pool before attaching worker subscriptions.
21. Queue orchestration queue pool base abstractions live in `src/orchestrator/queues/mod.rs`:
   - Queue names must remain unique in pool registration.
   - Queue names must match `[A-Za-z0-9_-]` with the first character alphanumeric.
   - Queue state pause/resume and bootstrap status must be serializable/restorable.
22. Jobs orchestration base abstractions live in `src/orchestrator/jobs/mod.rs`:
   - New jobs must target an already registered queue by name.
   - New jobs must not target system queues (`_` prefix).
   - Job IDs must follow `<queue-name>:<job-uuid>` while enqueue API returns the generated job UUID.
   - Default new-job status is `new`, and execution start timestamp must be `scheduled_at` or immediate time.
   - Job runtime metadata must include `attempts_so_far`, initialized as `0` on job creation.
   - Jobs must be stamped with immutable `created_at` at transient staging time and persisted with the job record.
   - Optional retry interval must be strictly positive when provided.
   - Jobs pool is transient staging only; enqueue stages in memory, emits persistence event, and successful persistence must evict staged job from pool.
   - Shutdown should best-effort flush remaining staged jobs to persistence before exit, and pause briefly (100 ms) after pool drain.
   - Heartbeat-driven status progression should promote persisted jobs from `new` to `waiting`/`delayed`, and from `delayed` to `waiting` once execution time is reached.
   - Status progression must skip jobs targeting paused queues.
23. Storage facade is implemented in `src/storage/mod.rs`:
   - Keep engine selection explicit via `AppConfig.storage.engine` (currently only `sled`).
   - Keep storage initialization fail-fast before TCP server startup.
   - Keep default data path rooted at `~/.overhop/data` with `~/` and `$HOME/` expansion support.
   - Keep engine-specific details behind storage backend abstractions so domain logic stays engine-agnostic.
   - Keep immutable sled keyspace prefixes versioned (`v1:q:` for queues, `v1:j:` primary jobs, `v1:j_qt:` queue-time job index, `v1:status:` job status key, `v1:status_fifo:` status FIFO index ordered by `created_at`).
   - Keep ordered numeric key fragments encoded in big-endian sortable form for sled iterator correctness.
   - Keep storage abstraction and concrete engine implementations separated into dedicated files/modules.
24. Persistent queue bootstrap flow is implemented in `src/orchestrator/queues/persistent.rs`:
   - On first run, persist and preload `_system` queue.
   - Queue pool mutations must persist first, then reload in-memory pool from persistence.
   - On shutdown, queue state should be explicitly persisted/flushed before process exit.
25. Diagnostics are implemented in `src/diagnostics/mod.rs`:
   - Keep diagnostics categories split into dedicated functions (application, memory, pools, storage, queues).
   - Keep wire-facing diagnostics payload assembly centralized in diagnostics module, not inside protocol handlers.
26. Worker STATUS wire flow:
   - Registered workers may use STATUS (`t=10`, empty payload) to request runtime diagnostics snapshot.
   - STATUS success response should return diagnostics categories under generic OK payload; failures should return ERR.
27. Self-debug mode is implemented in `src/self_debug/mod.rs`:
   - Runtime flag `--self-debug` enables in-process self-debug client flow against local TCP server.
   - Self-debug mode must force logger minimum level to `VERBOSE`, regardless of config/argv logging values.
   - Self-debug mode must use isolated storage path (configured via `storage.self_debug_path` or derived as `<storage.path>-self-debug`).
   - Self-debug mode must never reuse regular runtime storage path.
   - On self-debug completion (success or failure), remove self-debug storage artifacts by default, then propagate run result.
   - Runtime flag `--self-debug-keep-artifacts` may be used to skip artifact cleanup for debugging sessions.
   - Keep self-debug output human-readable with clear IN/OUT separation, bold message type names, and decoded envelope JSON payloads.
   - Keep self-debug logic isolated from core domain and protocol handler paths.
28. Runtime platform and startup cosmetics:
   - Overhop runtime target is POSIX (`unix`) only.
   - Startup must print the hardcoded decorative banner verbatim before subsystem initialization.
   - Startup banner footer should include app/version/build-date metadata, short description, and MIT liability disclaimer text.
29. Shared utility helpers live in `src/utils`:
   - Prefer `utils::timing::measure_execution` or `ScopedExecutionTimer` for named execution-time debug logs of startup/module operations.

## Intent

Keep collaboration predictable, modular, and maintainable for both humans and LLMs over time.
