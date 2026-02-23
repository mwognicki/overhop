# Self Debug Module (`src/self_debug`)

## Core Objective

Run an in-process wire protocol self-check by acting as a local Overhop client against the running TCP server.

## Core Concepts

- Starts only when runtime flag `--self-debug` is present.
- Optional runtime flag `--self-debug-keep-artifacts` disables post-run artifact cleanup.
- Uses dedicated self-debug storage path, never the regular runtime path.
- Forces application logging level to `VERBOSE` while self-debug mode is active.
- Uses decoded wire envelopes for all console output.
- Prints clear directional logs (`OUT`/`IN`) with colorful formatting, bold message type names, and inline JSON payloads.

## Most Relevant Features

- Exercises currently implemented protocol flow (`HELLO`, `REGISTER`, queue ops including `RMQUEUE`, subscriptions, `CREDIT`, `STATUS`, `PING`).
- Leaves one additional self-debug queue persisted at the end of a run and applies `PAUSE` -> `RESUME` on that last created queue to validate persisted queue state flow across sessions.
- Keeps self-debug logic isolated from domain/business modules.
- Cleans self-debug storage artifacts after run completion (success or error) by default, then returns/propagates run result.
- Preserves production startup path when mode is not enabled.
