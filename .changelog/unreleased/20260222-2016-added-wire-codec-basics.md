### Added
- Added a dedicated wire codec module implementing MessagePack-based `MessageEnvelope` framing with strict 8 MiB payload bounds.
- Added frame codec format support: `u32_be length (4B) + payload` with protocol errors for zero-length and oversized frames.
- Added recursive value validation rules for allowed types only (int<=int64, string, bool, nil, array, map with string keys, bin) and explicit rejection of floats and extension types.

### Architecture
- Introduced `src/wire/codec` as an isolated serialization/framing layer to keep future wire orchestrator implementation detached from low-level codec logic.
