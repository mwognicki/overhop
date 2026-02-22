# Changelog Fragments

Every meaningful commit must include at least one changelog fragment in `.changelog/unreleased/`.

## Fragment naming

Use this file name format:

`YYYYMMDD-HHMM-<type>-<slug>.md`

Example:

`20260222-1710-arch-worker-pool-skeleton.md`

## Allowed types

- `added`
- `changed`
- `fixed`
- `arch`
- `protocol`
- `llm`
- `docs`

## Fragment content

Use a single bullet line, concise but specific:

`- Added worker pool registry skeleton with explicit capacity tracking.`

If needed, add 1-2 more bullets. Keep it short and release-note focused.

## Release flow

1. Keep adding fragments per commit during development.
2. Optional helper: `scripts/new_changelog_fragment.sh <type> <slug> [message]`.
3. Run `scripts/release_changelog.sh <version> <YYYY-MM-DD>` to roll fragments into `CHANGELOG.md`.
4. Commit the release update.
