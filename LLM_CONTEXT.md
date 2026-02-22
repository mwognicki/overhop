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

## Intent

Keep collaboration predictable, modular, and maintainable for both humans and LLMs over time.
