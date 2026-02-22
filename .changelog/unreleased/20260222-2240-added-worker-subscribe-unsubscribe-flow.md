### Added
- Implemented worker `SUBSCRIBE` (`t=6`) and `UNSUBSCRIBE` (`t=7`) wire flows.
- Added protocol/session parsing for subscription payload validation (`q`, optional non-negative `credits`, `sid` UUID).
- Wired runtime handling to worker pools with standard `OK/ERR` responses (`sid` returned on successful subscribe).
- Updated wire/session docs and protocol spec with subscription grammar and constraints.
