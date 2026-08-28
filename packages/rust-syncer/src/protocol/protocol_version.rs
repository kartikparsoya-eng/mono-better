//! Port of `packages/zero-protocol/src/protocol-version.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

/// Current protocol version. Must match `packages/zero-protocol/src/protocol-version.ts`.
pub const PROTOCOL_VERSION: u32 = 51;

/// Minimum supported protocol version.
pub const MIN_SERVER_SUPPORTED_SYNC_PROTOCOL: u32 = 30;
