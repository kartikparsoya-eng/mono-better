//! Port of `zero-protocol/src/query-server.ts` — the API server's reply to a
//! `['transform', …]` request as zero-cache parses it: `fetchFromAPIServer`
//! is handed `queryResponseSchema` (custom-queries/transform-query.ts:200-201)
//! and parses in valita `passthrough` mode (custom/fetch.ts:260), so unknown
//! keys are kept and no object here carries `deny_unknown_fields`.

use serde::{Deserialize, Deserializer};
use serde_json::Value;

use super::custom_queries::{ErroredQuery, TransformResponseMessage, TransformedQuery};
use super::error::TransformFailedBody;
use super::nullable_optional;

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
    /// (`transformResponse.userID`, custom-queries/transform-query.ts:216-221), so
    /// absent and `null` stay distinct.
    #[serde(default, rename = "userID", deserialize_with = "nullable_optional")]
    pub user_id: Option<Option<String>>,
    pub queries: QueryResponseBody,
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
        use super::valita::{deepest_union_issue, deserialize_at, encode_nested};
        use serde::de::Error as _;
        let value = Value::deserialize(d)?;
        // Each member is tried on the value in schema order; the failure
        // reported is the deepest one (`getDeepestUnionParseError`).
        let mut failures = Vec::new();
        match deserialize_at::<QuerySuccess>(&value, &[], &value) {
            Ok(v) => return Ok(QueryResponse::Success(v)),
            Err(issue) => failures.push(issue),
        }
        match deserialize_at::<TransformFailedBody>(&value, &[], &value) {
            Ok(v) => return Ok(QueryResponse::Failed(v)),
            Err(issue) => failures.push(issue),
        }
        match deserialize_at::<TransformResponseMessage>(&value, &[], &value) {
            Ok(v) => return Ok(QueryResponse::Legacy(v)),
            Err(issue) => failures.push(issue),
        }
        Err(D::Error::custom(encode_nested(&deepest_union_issue(
            failures,
        ))))
    }
}
