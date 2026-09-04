//! Port of `packages/zero-protocol/src/connect.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use super::*;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectedBody {
    pub wsid: String,
    pub timestamp: Option<i64>,
    /// The server's app id, so a direct-mutation client can build the
    /// mutate-endpoint `appID` / `schema` params identically to zero-cache.
    #[serde(rename = "appID", skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    /// The server's shard number (pairs with `app_id` to form the upstream
    /// schema `{appID}_{shardNum}`).
    #[serde(rename = "shardNum", skip_serializing_if = "Option::is_none")]
    pub shard_num: Option<u32>,
}

// NOTE: TS `ConnectedMessage` / `PongMessage` / error messages are TUPLE type
// aliases (`['connected', ConnectedBody]`, zero-protocol connect.ts / pong.ts),
// not classes. The faithful ports are the free-fn builders below
// (`connected_message` / `pong_message` / `error_message`); an early-port layer
// of `struct {Connected,Pong,Error}Message` wrappers (which serialized as
// objects, not tuples — a wire-shape drift) was DEAD code and has been removed.

// initConnectionBodySchema uses userPushURL/userQueryURL (capital URL)
// valita `v.object` rejects unknown keys (M13 R3).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct InitConnectionBody {
    pub desired_queries_patch: UpQueriesPatch,
    // Every `.optional()` below is absent-or-value, never an explicit `null`
    // (M13 R4) — valita `.optional()` does not admit null, serde's `Option`
    // does.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub client_schema: Option<Value>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub deleted: Option<DeleteClientsBody>,
    #[serde(
        rename = "userPushURL",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub user_push_url: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub user_push_headers: Option<serde_json::Map<String, Value>>,
    #[serde(
        rename = "userQueryURL",
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub user_query_url: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub user_query_headers: Option<serde_json::Map<String, Value>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub active_clients: Option<Vec<String>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    pub traceparent: Option<String>,
}

/// The init connection message tuple `["initConnection", body]`.
pub type InitConnectionMessage = (String, InitConnectionBody);

/// Create a `["connected", {wsid, timestamp, appID, shardNum}]` message.
pub fn connected_message(wsid: &str, app_id: &str, shard_num: u32) -> Value {
    downstream_message(
        "connected",
        &ConnectedBody {
            wsid: wsid.to_string(),
            timestamp: Some(now_ms()),
            app_id: Some(app_id.to_string()),
            shard_num: Some(shard_num),
        },
    )
}

/// Decoded sec-websocket-protocol header contents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecProtocols {
    /// `initConnectionMessage` from the header, or `None`.
    #[serde(rename = "initConnectionMessage")]
    pub init_connection_message: Option<InitConnectionMessage>,
    /// Auth token from the header, or `None`.
    #[serde(rename = "authToken")]
    pub auth_token: Option<String>,
}

/// Decode the `sec-websocket-protocol` header value.
///
/// TS: `decodeSecProtocols` — `decodeURIComponent` → `atob` → UTF-8 decode → `JSON.parse`.
/// Rust: `urlencoding::decode` → `base64::decode` → UTF-8 → `serde_json::from_slice`.
pub fn decode_sec_protocols(header: &str) -> Result<SecProtocols, DecodeError> {
    // 1. URL-decode
    let decoded = urlencoding::decode(header).map_err(|e| DecodeError::UrlDecode(e.to_string()))?;
    // 2. Base64-decode
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(decoded.as_bytes())
        .map_err(|e| DecodeError::Base64(e.to_string()))?;
    // 3. JSON parse (from UTF-8 bytes)
    serde_json::from_slice(&bytes).map_err(DecodeError::Json)
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("URL decode failed: {0}")]
    UrlDecode(String),
    #[error("Base64 decode failed: {0}")]
    Base64(String),
    #[error("JSON parse failed: {0}")]
    Json(#[from] serde_json::Error),
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
