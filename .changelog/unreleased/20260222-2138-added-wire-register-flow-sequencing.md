### Added
- Added anonymous wire session module with strict message ordering (`HELLO` first, then `REGISTER` only).
- Added basic `REGISTER` client flow (`t=2`, empty payload) with anonymous-to-worker promotion and generic `OK` response carrying `wid`.
- Added protocol violation `ERR` responses for invalid anonymous message order/payload.

### Changed
- Server anonymous connection loop now evaluates HELLO/REGISTER protocol frames and stores `helloed_at` on successful HELLO.
- Extended wire protocol documentation with REGISTER flow and anonymous sequencing constraints.
