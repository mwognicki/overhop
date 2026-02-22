### Changed
- Updated PR self-debug workflow to run the second self-debug pass against artifacts produced by the first pass.
- CI now validates artifact reusability/openability across sequential self-debug runs.
- Kept cleanup verification as a secondary check (second run without keep flag should remove artifacts).
