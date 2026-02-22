# Wire Session Module (`src/wire/session`)

## Core Objective

Enforce anonymous-client message ordering rules and produce protocol-level responses for early session lifecycle flows.

## Current Scope

- Enforces first-message and post-HELLO constraints for anonymous connections.
- Supports REGISTER request validation and transition signaling.
- Builds protocol violation `ERR` response frames.

## Most Relevant Features

- First anonymous message must be `HELLO`.
- After successful `HELLO`, only `REGISTER` is currently allowed.
- `REGISTER` payload must be empty map.
- Exposes action-oriented result so runtime can apply pool transitions (e.g., anonymous -> worker promotion).
