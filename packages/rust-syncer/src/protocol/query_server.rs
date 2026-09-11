//! Port of `zero-protocol/src/query-server.ts` — the API server's reply to a
//! `['transform', …]` request as zero-cache parses it: `fetchFromAPIServer`
//! is handed `queryResponseSchema` (custom-queries/transform-query.ts:200-201)
//! and parses in valita `passthrough` mode (custom/fetch.ts:260), so unknown
//! keys are kept and no object here carries `deny_unknown_fields`.

use serde::{Deserialize, Deserializer};
use serde_json::Value;

use super::custom_queries::{ErroredQuery, TransformResponseMessage, TransformedQuery};
use super::error::TransformFailedBody;

/// `queryResultSchema` (query-server.ts:9-12): `transformedQuery | erroredQuery`.
/// valita tries the members in order; so does `untagged`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum QueryResult {
    Transformed(TransformedQuery),
    Errored(ErroredQuery),
}

/// `queryResponseBodySchema` (query-server.ts:15).
pub type QueryResponseBody = Vec<QueryResult>;

/// `v.literal('QueryResponse')` (query-server.ts:19).
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum QueryResponseKind {
    #[serde(rename = "QueryResponse")]
    QueryResponse,
}

/// `querySuccessSchema` (query-server.ts:18-22).
#[derive(Debug, Clone, Deserialize)]
pub struct QuerySuccess {
    pub kind: QueryResponseKind,
    /// `v.string().nullable().optional()`: absent, `null`, or a string. TS
    /// reads present-but-`null` as server-validated with no user
    /// (transform-query.ts:216-221), so absent and `null` stay distinct.
    #[serde(default, rename = "userID", deserialize_with = "nullable_optional")]
    pub user_id: Option<Option<String>>,
    pub queries: QueryResponseBody,
}

/// `.nullable().optional()` tri-state: the outer `Option` is presence (serde
/// `default` fills absent), the inner one is `null`.
fn nullable_optional<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Option<String>>, D::Error> {
    Option::<String>::deserialize(d).map(Some)
}

/// `queryResponseSchema` (query-server.ts:25-30):
/// `querySuccess | transformFailedBody | transformResponseMessage` (the last
/// for backwards compatibility).
#[derive(Debug, Clone)]
pub enum QueryResponse {
    Success(QuerySuccess),
    Failed(TransformFailedBody),
    Legacy(TransformResponseMessage),
}

impl<'de> Deserialize<'de> for QueryResponse {
    /// Resolve the member the way valita does — by the discriminating shape
    /// (`'kind' in response`, transform-query.ts:211; an array otherwise) —
    /// then parse the whole value as that member, so the error names the
    /// field that failed instead of "no variant matched".
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        let value = Value::deserialize(d)?;
        let parsed = if value.get("kind").is_some() {
            if value.get("kind").and_then(Value::as_str) == Some("QueryResponse") {
                serde_json::from_value(value).map(QueryResponse::Success)
            } else {
                serde_json::from_value(value).map(QueryResponse::Failed)
            }
        } else {
            serde_json::from_value(value).map(QueryResponse::Legacy)
        };
        parsed.map_err(D::Error::custom)
    }
}
