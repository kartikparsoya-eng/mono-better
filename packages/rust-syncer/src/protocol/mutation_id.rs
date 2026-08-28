//! Port of `packages/zero-protocol/src/mutation-id.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationID {
    pub id: i64,
    #[serde(rename = "clientID")]
    pub client_id: String,
}
