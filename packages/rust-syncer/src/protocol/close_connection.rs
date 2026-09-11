//! Port of `packages/zero-protocol/src/close-connection.ts`.

use serde_json::Value;

/// Port of TS `closeConnectionBodySchema = v.array(v.unknown())`
/// (close-connection.ts:3) — the body must be an ARRAY. Rust previously ignored
/// it.
pub type CloseConnectionBody = Vec<Value>;
