### Added
- Added runtime self-debug mode enabled with `--self-debug`.
- Implemented dedicated `src/self_debug` module that acts as an in-process Overhop client against local TCP server.
- Self-debug mode now prints colorful, decoded wire IN/OUT envelopes with full inline JSON payloads.

### Changed
- Main startup now pre-processes runtime-only flags before config argument parsing.
- Updated runtime config examples to document `--self-debug` usage.
