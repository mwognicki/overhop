# Self Debug Module (`src/self_debug`)

## Core Objective

Run an in-process wire protocol self-check by acting as a local Overhop client against the running TCP server.

## Core Concepts

- Starts only when runtime flag `--self-debug` is present.
- Uses decoded wire envelopes for all console output.
- Prints clear directional logs (`OUT`/`IN`) with colorful formatting and inline JSON payloads.

## Most Relevant Features

- Exercises currently implemented protocol flow (`HELLO`, `REGISTER`, queue ops, subscriptions, `CREDIT`, `STATUS`, `PING`).
- Keeps self-debug logic isolated from domain/business modules.
- Preserves production startup path when mode is not enabled.
