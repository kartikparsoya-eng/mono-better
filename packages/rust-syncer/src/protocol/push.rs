//! Port of `packages/zero-protocol/src/push.ts` — serde
//! equivalents of the valita schemas.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::mutation_id::passthrough::MutationID as PassthroughMutationID;
use super::optional_no_null;

// pushBodySchema uses clientGroupID and requestID (capital ID)
//
// `pushVersion`/`timestamp` are `v.number()` in TS (push.ts) — a JS number, i.e.
// an f64. They were `i64` here, so TS served `1E5`, `1.5`, `-0`, `1e309` and
// `1e-330` while rust answered `InvalidMessage` and CLOSED the connection.
// `1E5` is an ordinary way to write a timestamp, so this disconnected real
// clients.
//
// `deny_unknown_fields`: valita `v.object` REJECTS unknown keys, where serde
// ignores them by default — TS closes the connection on an extra field while
// rust served it. That divergence is a live hazard during a client
// rollout that adds a field.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushBody {
    #[serde(rename = "clientGroupID")]
    pub client_group_id: String,
    /// `v.array(mutationSchema)` (push.ts:8): each entry validated against
    /// the strict `Mutation` twin, kept as JSON for the relay.
    #[serde(deserialize_with = "crate::protocol::mutation::strict_mutations")]
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
// not an i64. `deny_unknown_fields` mirrors strict `v.object`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckMutationResponsesBody {
    pub id: crate::protocol::JsNumber,
    #[serde(rename = "clientID")]
    pub client_id: String,
}

/// `pushErrorSchema` (push.ts:26-81) — every member `@deprecated`: push
/// errors are `['error', {…}]` messages now. Still a member of
/// `queryResponseSchema`'s error detection (`apiErrorFromResult`,
/// custom/fetch.ts:475), so parsed in valita `passthrough` mode: no
/// `deny_unknown_fields`, and the passthrough `mutationIDSchema`. Told apart
/// by the `error` literal.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "error")]
pub enum PushError {
    #[serde(rename = "unsupportedPushVersion")]
    UnsupportedPushVersion {
        #[serde(rename = "mutationIDs", default, deserialize_with = "optional_no_null")]
        mutation_ids: Option<Vec<PassthroughMutationID>>,
    },
    #[serde(rename = "unsupportedSchemaVersion")]
    UnsupportedSchemaVersion {
        #[serde(rename = "mutationIDs", default, deserialize_with = "optional_no_null")]
        mutation_ids: Option<Vec<PassthroughMutationID>>,
    },
    #[serde(rename = "http")]
    Http {
        status: crate::protocol::JsNumber,
        details: String,
        #[serde(rename = "mutationIDs", default, deserialize_with = "optional_no_null")]
        mutation_ids: Option<Vec<PassthroughMutationID>>,
    },
    #[serde(rename = "zeroPusher")]
    ZeroPusher {
        details: String,
        #[serde(rename = "mutationIDs", default, deserialize_with = "optional_no_null")]
        mutation_ids: Option<Vec<PassthroughMutationID>>,
    },
}
