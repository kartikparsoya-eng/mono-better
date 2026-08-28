//! Port of `packages/zero-protocol/src/version.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

/// A CVR version (cookie). Always a string like "00" or "0123abc".
pub type Version = String;
/// A nullable version (for base cookies — null before first request).
pub type NullableVersion = Option<String>;
