//! Port of `packages/zero-protocol/src/push.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use serde::{Deserialize, Serialize};
use serde_json::Value;

// pushBodySchema uses clientGroupID and requestID (capital ID)
//
// `pushVersion`/`timestamp` are `v.number()` in TS (push.ts) — a JS number, i.e.
// an f64. They were `i64` here, so TS served `1E5`, `1.5`, `-0`, `1e309` and
// `1e-330` while rust answered `InvalidMessage` and CLOSED the connection.
// `1E5` is an ordinary way to write a timestamp, so this disconnected real
// clients (M13 R1).
//
// `deny_unknown_fields`: valita `v.object` REJECTS unknown keys, where serde
// ignores them by default — TS closes the connection on an extra field while
// rust served it (M13 R3). That divergence is a live hazard during a client
// rollout that adds a field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushBody {
    #[serde(rename = "clientGroupID")]
    pub client_group_id: String,
    pub mutations: Vec<Value>,
    #[serde(rename = "pushVersion")]
    pub push_version: crate::protocol::JsNumber,
    #[serde(
        rename = "schemaVersion",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub schema_version: Option<crate::protocol::JsNumber>,
    pub timestamp: crate::protocol::JsNumber,
    #[serde(rename = "requestID")]
    pub request_id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub auth: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub traceparent: Option<String>,
}

// ackMutationResponsesSchema uses clientID (capital ID).
// Body is `mutationIDSchema` (push.ts:96), whose `id` is `v.number()` — an f64,
// not an i64 (M13 R1). `deny_unknown_fields` mirrors strict `v.object` (R3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckMutationResponsesBody {
    pub id: crate::protocol::JsNumber,
    #[serde(rename = "clientID")]
    pub client_id: String,
}
