//! Port of `zero-protocol/src/custom-queries.ts` — the query-transform
//! response schemas zero-cache reads back from the API server. Parsed in
//! valita `passthrough` mode (`fetchFromAPIServer`, custom/fetch.ts:260):
//! unknown keys are kept, so no `deny_unknown_fields`; every known field is
//! checked, and `.optional()` is still absent-or-value, never `null`.
//!
//! The request side (`transformRequestBodySchema`) is built, never parsed, by
//! the syncer (`transform_query.rs` `transform`).

use serde::{Deserialize, Deserializer};
use serde_json::Value;

use super::ast::passthrough;
use super::error::TransformFailedBody;
use super::optional_no_null;
use super::query_server::QueryResult;

/// `transformedQuerySchema` (custom-queries.ts:15-19). The AST is validated
/// against `astSchema` and kept as JSON, as TS keeps the parsed object.
#[derive(Debug, Clone, Deserialize)]
pub struct TransformedQuery {
    pub id: String,
    pub name: String,
    #[serde(deserialize_with = "passthrough_ast")]
    pub ast: Value,
}

/// `astSchema` in passthrough mode for a field the syncer keeps as JSON:
/// validate, then hand the JSON back unchanged.
fn passthrough_ast<'de, D: Deserializer<'de>>(d: D) -> Result<Value, D::Error> {
    let value = Value::deserialize(d)?;
    super::validate_nested::<passthrough::Ast, D::Error>(&value, &[])?;
    Ok(value)
}

/// `appErroredQuerySchema` (custom-queries.ts:21-28), minus the `error`
/// literal that [`ErroredQuery`] carries as the union's tag.
#[derive(Debug, Clone, Deserialize)]
pub struct AppErroredQuery {
    pub id: String,
    pub name: String,
    /// optional for backwards compatibility
    #[serde(default, deserialize_with = "optional_no_null")]
    pub message: Option<String>,
    /// `jsonSchema.optional()`: absent or any JSON value, `null` included.
    #[serde(default)]
    pub details: Option<Value>,
}

/// `parseErroredQuerySchema` (custom-queries.ts:29-35), tag as above.
#[derive(Debug, Clone, Deserialize)]
pub struct ParseErroredQuery {
    pub id: String,
    pub name: String,
    pub message: String,
    #[serde(default)]
    pub details: Option<Value>,
}

/// `erroredQuerySchema` (custom-queries.ts:36-39): `app | parse`, told apart
/// by the `error` literal.
#[derive(Debug, Clone)]
pub enum ErroredQuery {
    App(AppErroredQuery),
    Parse(ParseErroredQuery),
}
crate::tagged_union!(ErroredQuery, "error", ["app" => App(AppErroredQuery), "parse" => Parse(ParseErroredQuery)]);

/// `transformResponseBodySchema` (custom-queries.ts:42-44): an array of the
/// `transformedQuery | erroredQuery` union, which query-server.ts names
/// `queryResultSchema`.
pub type TransformResponseBody = Vec<QueryResult>;

/// `v.literal('transformed')` (custom-queries.ts:65).
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum TransformedTag {
    #[serde(rename = "transformed")]
    Transformed,
}

/// `v.literal('transformFailed')` (custom-queries.ts:61).
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum TransformFailedTag {
    #[serde(rename = "transformFailed")]
    TransformFailed,
}

/// `transformResponseMessageSchema` (custom-queries.ts:60-71): the legacy
/// (pre-`QueryResponse`) API-server reply,
/// `['transformed', body] | ['transformFailed', transformFailedBody]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum TransformResponseMessage {
    Transformed(TransformedTag, TransformResponseBody),
    TransformFailed(TransformFailedTag, TransformFailedBody),
}
