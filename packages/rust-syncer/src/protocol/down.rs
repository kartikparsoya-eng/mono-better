//! Port of `packages/zero-protocol/src/down.ts` — serde
//! equivalents of the valita schemas.

use serde::Serialize;
use serde_json::Value;

/// Serialize a downstream message as a JSON tuple `["type", body]`.
pub fn downstream_message(msg_type: &str, body: &impl Serialize) -> Value {
    serde_json::json!([msg_type, body])
}
