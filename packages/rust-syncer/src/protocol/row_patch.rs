//! Port of `packages/zero-protocol/src/row-patch.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RowPatchOp {
    Put {
        op: String, // "put"
        table_name: String,
        value: Value, // row
    },
    Update {
        op: String, // "update"
        table_name: String,
        id: Value, // primaryKeyValueRecord
        merge: Option<Value>,
        constrain: Option<Vec<String>>,
    },
    Del {
        op: String, // "del"
        table_name: String,
        id: Value,
    },
    Clear {
        op: String, // "clear"
    },
}

pub type RowsPatch = Vec<RowPatchOp>;
