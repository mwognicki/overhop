# Pools Module (`src/pools`)

## Core Objective

Provide base in-memory pool abstractions for anonymous TCP connections and promoted workers, including lifecycle metadata and termination stubs.

## Core Ideas

- New accepted TCP connections enter the anonymous connections pool.
- Anonymous connections can be promoted into the workers pool.
- Promotion removes the connection from anonymous pool and creates immutable worker identity metadata.

## Core Concepts

- `AnonymousConnectionsPool`
  - stores `connected_at`
  - stores optional `helloed_at`
- `WorkersPool`
  - immutable `worker_id` (UUID)
  - immutable `promoted_at`
  - mutable `last_seen_at` (defaults to `promoted_at`)
  - metadata placeholder for queue capabilities
- `ConnectionWorkerPools`
  - coordinator facade for register/promotion/termination operations

## Most Relevant Features

- Promotion semantics: anonymous -> worker (single ownership transition).
- Optional termination reason accepted for both pools (stubbed for future wire-instruction handling).
- Best-effort socket shutdown on termination.
