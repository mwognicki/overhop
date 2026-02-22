### Added
- Implemented worker `ADDQUEUE` (`t=9`) wire message with queue creation payload (`name`, optional `config`) and `qid` response.
- Wired `ADDQUEUE` to persistent queue creation flow so queue mutations are persisted and then reloaded.

### Changed
- Renamed worker queue query wire grammar from `GQUEUE` to `QUEUE` and from `LQUEUES` to `LSQUEUE` (message IDs unchanged: `t=4`, `t=5`).
- Extended queue model with immutable UUID (`id`) persisted in storage and surfaced in queue metadata payloads.
