//! Port of `packages/zero-protocol/src/mutations-patch.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use super::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum MutationPatchOp {
    #[serde(rename = "put")]
    Put { mutation: Value },
    #[serde(rename = "del")]
    Del { id: MutationID },
}

pub type MutationsPatch = Vec<MutationPatchOp>;
