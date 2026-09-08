//! Port of `packages/zero-protocol/src/error-kind-enum.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};

// ErrorKind values are PascalCase strings (e.g. "VersionNotSupported").
// No rename needed — Rust variant names match the TS string values exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorKind {
    AuthInvalidated,
    ClientNotFound,
    InvalidConnectionRequest,
    InvalidConnectionRequestBaseCookie,
    InvalidConnectionRequestLastMutationID,
    InvalidConnectionRequestClientDeleted,
    InvalidMessage,
    InvalidPush,
    PushFailed,
    MutationFailed,
    MutationRateLimited,
    Rebalance,
    Rehome,
    TransformFailed,
    Unauthorized,
    VersionNotSupported,
    SchemaVersionNotSupported,
    ServerOverloaded,
    Internal,
}
