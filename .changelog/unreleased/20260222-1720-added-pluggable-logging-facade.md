### Added
- Added a lightweight, pluggable logging facade module with standard levels plus `VERBOSE`, default `DEBUG` threshold, optional human-friendly colored output, and optional JSON payload support.

### Architecture
- Introduced sink abstraction (`LogSink`) so domain/business code depends on logger facade API instead of output implementation details.
