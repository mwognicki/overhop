### Added
- Added worker-side `GQUEUE` (`t=4`) query flow with queue-name payload (`q`) and `OK` responses:
  - empty payload when queue is not found
  - full queue metadata payload when queue exists
- Added worker-side `LQUEUES` (`t=5`) flow returning full queues metadata list under `OK` response payload.
- Extended worker wire session parser/actions to validate and route `GQUEUE/LQUEUES` requests.

### Changed
- Worker runtime message loop now serves queue metadata queries from in-memory queue pool snapshots.
