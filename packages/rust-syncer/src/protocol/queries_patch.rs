//! Port of `packages/zero-protocol/src/queries-patch.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum QueriesPutOp {
    #[serde(rename = "put")]
    Put {
        hash: String,
        ttl: Option<i64>,
        /// Present in upstream (client→server) patches.
        ast: Option<Value>,
        name: Option<String>,
        args: Option<Vec<Value>>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum QueriesDelOp {
    #[serde(rename = "del")]
    Del { hash: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum QueriesClearOp {
    #[serde(rename = "clear")]
    Clear,
}

/// Patch op for queries (downstream — no ast/name/args).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueriesPatchOp {
    Put {
        op: String, // "put"
        hash: String,
        ttl: Option<i64>,
    },
    Del {
        op: String, // "del"
        hash: String,
    },
    Clear {
        op: String, // "clear"
    },
}

pub type QueriesPatch = Vec<QueriesPatchOp>;
pub type UpQueriesPatch = Vec<Value>; // Upstream patches have ast/name/args — use raw JSON
