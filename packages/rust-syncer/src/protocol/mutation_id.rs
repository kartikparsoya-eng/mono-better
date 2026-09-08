//! Port of `packages/zero-protocol/src/mutation-id.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};

// TS `mutationIDSchema.id` is `v.number()` — a JS number (f64), not an i64
// (M13 R1). `Eq` is gone with it: f64 is not `Eq`, and TS compares these as JS
// numbers anyway. `deny_unknown_fields` mirrors strict `v.object` (M13 R3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationID {
    pub id: crate::protocol::JsNumber,
    #[serde(rename = "clientID")]
    pub client_id: String,
}
