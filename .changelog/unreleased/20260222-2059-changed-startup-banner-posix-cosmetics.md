### Changed
- Startup now prints a hardcoded decorative banner verbatim before subsystem initialization.
- Added startup footer lines for app/version/build date, short runtime description, and MIT liability disclaimer notice.
- Removed sample greeting output and its unit test from `main.rs`.
- Added POSIX-only runtime guard for startup.

### Build
- Added `build.rs` to inject UTC compilation timestamp into runtime banner metadata.
