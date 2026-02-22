# LLM Context Entry Point

This file defines repository-specific working rules for LLMs collaborating on Overhop.

## Core Rules

1. After each implementation task, ask whether the user wants the changes committed.
2. If a commit is requested, propose a clear, human-readable commit message before committing.
3. After each implementation, evaluate whether `LLM_CONTEXT.md` should be extended with newly learned, reusable repository context.
4. Always update `LLM_CONTEXT.md` when the user explicitly asks to do so.
5. RustRover is the primary IDE workflow. Do not download crates manually; only add or modify dependencies in `Cargo.toml` and let the IDE/tooling resolve them.
6. Logical modules should generally be designed to support independent development and extension, detached from `main.rs` where practical.

## Intent

Keep collaboration predictable, modular, and maintainable for both humans and LLMs over time.
