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
20. Connection/worker pools are implemented in `src/pools/mod.rs`:
   - New TCP connections must enter anonymous pool with `connected_at` and optional `helloed_at`.
   - Anonymous metadata should track optional IDENT reply deadline timestamp when IDENT challenge is issued.
   - Promotion must remove from anonymous pool and create worker with immutable UUID/promoted timestamp and mutable `last_seen_at`.
   - Worker metadata must support queue subscriptions identified by subscription UUID, with per-subscription credits counter defaulting to `0`.
   - Queue subscription mutations must validate queue existence against queue pool before attaching worker subscriptions.
21. Queue orchestration queue pool base abstractions live in `src/orchestrator/queues/mod.rs`:
   - Queue names must remain unique in pool registration.
   - Queue state pause/resume and bootstrap status must be serializable/restorable.
22. Jobs orchestration base abstractions live in `src/orchestrator/jobs/mod.rs`:
   - New jobs must target an already registered queue by name.
   - Job IDs must follow `<queue-name>:<job-uuid>` while enqueue API returns the generated job UUID.
   - Default new-job status is `new`, and execution start timestamp must be `scheduled_at` or immediate time.
   - Optional retry interval must be strictly positive when provided.
   - Successful enqueue must trigger immediate persistence through storage backend integration points.
23. Storage facade is implemented in `src/storage/mod.rs`:
   - Keep engine selection explicit via `AppConfig.storage.engine` (currently only `sled`).
   - Keep storage initialization fail-fast before TCP server startup.
   - Keep default data path rooted at `~/.overhop/data` with `~/` and `$HOME/` expansion support.
   - Keep engine-specific details behind storage backend abstractions so domain logic stays engine-agnostic.
   - Keep immutable sled keyspace prefixes versioned (`v1:q:` for queues, `v1:j:` for jobs).
   - Keep storage abstraction and concrete engine implementations separated into dedicated files/modules.
24. Persistent queue bootstrap flow is implemented in `src/orchestrator/queues/persistent.rs`:
   - On first run, persist and preload `_system` queue.
   - Queue pool mutations must persist first, then reload in-memory pool from persistence.
   - On shutdown, queue state should be explicitly persisted/flushed before process exit.
25. Runtime platform and startup cosmetics:
   - Overhop runtime target is POSIX (`unix`) only.
   - Startup must print the hardcoded decorative banner verbatim before subsystem initialization.
   - Startup banner footer should include app/version/build-date metadata, short description, and MIT liability disclaimer text.

## Intent

Keep collaboration predictable, modular, and maintainable for both humans and LLMs over time.
