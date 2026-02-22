# Wire Session Module (`src/wire/session`)

## Core Objective

Enforce anonymous-client message ordering rules and produce protocol-level responses for early session lifecycle flows.

## Current Scope

- Enforces first-message and post-HELLO constraints for anonymous connections.
- Supports REGISTER request validation and transition signaling.
- Builds protocol violation `ERR` response frames.
- Builds IDENT challenge frames (`t=104`) for delayed unregistered anonymous clients.
- Supports worker-side PING request validation and PONG response frame construction.
- Supports worker queue query requests (`GQUEUE`, `LQUEUES`) parsing/validation.
- Supports worker subscription flow requests (`SUBSCRIBE`, `UNSUBSCRIBE`) parsing/validation.

## Most Relevant Features

- First anonymous message must be `HELLO`.
- After successful `HELLO`, only `REGISTER` is currently allowed.
- `REGISTER` payload must be empty map.
- IDENT challenge payload includes register timeout and reply deadline metadata.
- Worker `PING` payload must be empty map, and `PONG` includes server timestamp.
- Worker `GQUEUE` payload must include queue name under `q`.
- Worker `LQUEUES` payload must be empty and returns full queue list metadata.
- Worker `SUBSCRIBE` payload must include queue name under `q`, with optional non-negative `credits`.
- Worker `UNSUBSCRIBE` payload must include `sid` as UUID string.
- Exposes action-oriented result so runtime can apply pool transitions (e.g., anonymous -> worker promotion).
