### Changed
- Extended PR test workflow with sequential self-debug checks after `cargo test`.
- CI now runs one self-debug pass with `--self-debug-keep-artifacts` and one without it, both with explicit worker-safe storage paths.
- Workflow verifies expected artifact behavior (kept in first run, cleaned in second run).
