### Changed
- Added dedicated `storage.self_debug_path` config support to isolate self-debug runs from regular runtime storage.
- Self-debug mode now defaults to derived isolated path (`<storage.path>-self-debug`) when explicit self-debug path is not configured.
- Self-debug artifacts are now removed after run completion (success or failure), and run result is propagated after cleanup.
