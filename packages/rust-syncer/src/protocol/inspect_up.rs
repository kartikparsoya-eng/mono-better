//! Port of `packages/zero-protocol/src/inspect-up.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

// inspectQueriesUpBodySchema uses clientID (capital ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum InspectUpBody {
    #[serde(rename = "queries")]
    Queries {
        id: String,
        #[serde(rename = "clientID")]
        client_id: Option<String>,
    },
    #[serde(rename = "metrics")]
    Metrics { id: String },
    #[serde(rename = "version")]
    Version { id: String },
    #[serde(rename = "authenticate")]
    Authenticate { id: String, value: String },
    #[serde(rename = "analyze-query")]
    AnalyzeQuery {
        id: String,
        value: Option<Value>,
        options: Option<AnalyzeQueryOptions>,
        ast: Option<Value>,
        name: Option<String>,
        args: Option<Vec<Value>>,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyzeQueryOptions {
    pub vended_rows: Option<bool>,
    pub synced_rows: Option<bool>,
    pub join_plans: Option<bool>,
}
