### Changed
- Suppressed Rust compiler warnings for PR self-debug CI runs by setting `RUSTFLAGS=-Awarnings` on self-debug `cargo run` commands.
- Error handling remains unchanged; build/runtime errors still fail the workflow.
