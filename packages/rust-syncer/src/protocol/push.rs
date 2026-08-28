//! Port of `packages/zero-protocol/src/push.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

// pushBodySchema uses clientGroupID and requestID (capital ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushBody {
    #[serde(rename = "clientGroupID")]
    pub client_group_id: String,
    pub mutations: Vec<Value>,
    #[serde(rename = "pushVersion")]
    pub push_version: i64,
    #[serde(rename = "schemaVersion")]
    pub schema_version: Option<i64>,
    pub timestamp: i64,
    #[serde(rename = "requestID")]
    pub request_id: String,
    pub auth: Option<String>,
    pub traceparent: Option<String>,
}

// ackMutationResponsesSchema uses clientID (capital ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckMutationResponsesBody {
    pub id: i64,
    #[serde(rename = "clientID")]
    pub client_id: String,
}
