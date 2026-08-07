//! Zero protocol message types — Rust serde equivalents of the TypeScript
//! valita schemas in `packages/zero-protocol/src/`.
//!
//! Wire format: all messages are JSON tuples `["messageType", bodyObject]`.
//! We use untagged enums + `#[serde(tag = "op")]` to match the TS union types.

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// ─── Protocol version ──────────────────────────────────────────────────────

/// Current protocol version. Must match `packages/zero-protocol/src/protocol-version.ts`.
pub const PROTOCOL_VERSION: u32 = 51;

/// Minimum supported protocol version.
pub const MIN_SERVER_SUPPORTED_SYNC_PROTOCOL: u32 = 30;

// ─── Version ───────────────────────────────────────────────────────────────

/// A CVR version (cookie). Always a string like "00" or "0123abc".
pub type Version = String;
/// A nullable version (for base cookies — null before first request).
pub type NullableVersion = Option<String>;

// ─── Error kinds, origins, reasons ─────────────────────────────────────────

// ErrorKind values are PascalCase strings (e.g. "VersionNotSupported").
// No rename needed — Rust variant names match the TS string values exactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorKind {
    AuthInvalidated,
    ClientNotFound,
    InvalidConnectionRequest,
    InvalidConnectionRequestBaseCookie,
    InvalidConnectionRequestLastMutationID,
    InvalidConnectionRequestClientDeleted,
    InvalidMessage,
    InvalidPush,
    PushFailed,
    MutationFailed,
    MutationRateLimited,
    Rebalance,
    Rehome,
    TransformFailed,
    Unauthorized,
    VersionNotSupported,
    SchemaVersionNotSupported,
    ServerOverloaded,
    Internal,
}

// ErrorOrigin values are lowercase/mixed ("client", "server", "zeroCache").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ErrorOrigin {
    Client,
    Server,
    ZeroCache,
}

// ErrorReason values are lowercase/mixed ("database", "parse", "oooMutation", etc.).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorReason {
    #[serde(rename = "database")]
    Database,
    #[serde(rename = "parse")]
    Parse,
    #[serde(rename = "oooMutation")]
    OutOfOrderMutation,
    #[serde(rename = "unsupportedPushVersion")]
    UnsupportedPushVersion,
    #[serde(rename = "internal")]
    Internal,
    #[serde(rename = "http")]
    Http,
    #[serde(rename = "timeout")]
    Timeout,
}

// ─── Mutation ID ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationID {
    pub id: i64,
    #[serde(rename = "clientID")]
    pub client_id: String,
}

// ─── Error body ────────────────────────────────────────────────────────────

/// Basic error body (no backoff, no push/transform details).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BasicErrorBody {
    pub kind: ErrorKind,
    pub message: String,
    /// Optional for backwards compatibility.
    pub origin: Option<ErrorOrigin>,
}

/// Backoff error body (Rebalance, Rehome, ServerOverloaded).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackoffBody {
    pub kind: ErrorKind,
    pub message: String,
    pub min_backoff_ms: Option<i64>,
    pub max_backoff_ms: Option<i64>,
    pub reconnect_params: Option<serde_json::Map<String, Value>>,
    pub origin: Option<ErrorOrigin>,
}

/// PushFailed error with server origin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushFailedServerBody {
    pub kind: ErrorKind,
    pub details: Option<Value>,
    #[serde(rename = "mutationIDs")]
    pub mutation_ids: Vec<MutationID>,
    pub message: String,
    pub origin: ErrorOrigin,
    pub reason: ErrorReason,
}

/// PushFailed error with ZeroCache origin + HTTP reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushFailedHttpBody {
    pub kind: ErrorKind,
    pub details: Option<Value>,
    #[serde(rename = "mutationIDs")]
    pub mutation_ids: Vec<MutationID>,
    pub message: String,
    pub origin: ErrorOrigin,
    pub reason: ErrorReason,
    pub status: i64,
    pub body_preview: Option<String>,
}

/// PushFailed error with ZeroCache origin + non-HTTP reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushFailedZeroCacheBody {
    pub kind: ErrorKind,
    pub details: Option<Value>,
    #[serde(rename = "mutationIDs")]
    pub mutation_ids: Vec<MutationID>,
    pub message: String,
    pub origin: ErrorOrigin,
    pub reason: ErrorReason,
}

/// TransformFailed error with server origin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformFailedServerBody {
    pub kind: ErrorKind,
    pub details: Option<Value>,
    #[serde(rename = "queryIDs")]
    pub query_ids: Vec<String>,
    pub message: String,
    pub origin: ErrorOrigin,
    pub reason: ErrorReason,
}

/// TransformFailed error with ZeroCache origin + HTTP reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformFailedHttpBody {
    pub kind: ErrorKind,
    pub details: Option<Value>,
    #[serde(rename = "queryIDs")]
    pub query_ids: Vec<String>,
    pub message: String,
    pub origin: ErrorOrigin,
    pub reason: ErrorReason,
    pub status: i64,
    pub body_preview: Option<String>,
}

/// TransformFailed error with ZeroCache origin + non-HTTP reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformFailedZeroCacheBody {
    pub kind: ErrorKind,
    pub details: Option<Value>,
    #[serde(rename = "queryIDs")]
    pub query_ids: Vec<String>,
    pub message: String,
    pub origin: ErrorOrigin,
    pub reason: ErrorReason,
}

/// The full error body union. Matches `errorBodySchema` in error.ts.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ErrorBody {
    Basic(BasicErrorBody),
    Backoff(BackoffBody),
    PushFailedServer(PushFailedServerBody),
    PushFailedHttp(PushFailedHttpBody),
    PushFailedZeroCache(PushFailedZeroCacheBody),
    TransformFailedServer(TransformFailedServerBody),
    TransformFailedHttp(TransformFailedHttpBody),
    TransformFailedZeroCache(TransformFailedZeroCacheBody),
}

impl ErrorBody {
    pub fn kind(&self) -> &ErrorKind {
        match self {
            ErrorBody::Basic(b) => &b.kind,
            ErrorBody::Backoff(b) => &b.kind,
            ErrorBody::PushFailedServer(b) => &b.kind,
            ErrorBody::PushFailedHttp(b) => &b.kind,
            ErrorBody::PushFailedZeroCache(b) => &b.kind,
            ErrorBody::TransformFailedServer(b) => &b.kind,
            ErrorBody::TransformFailedHttp(b) => &b.kind,
            ErrorBody::TransformFailedZeroCache(b) => &b.kind,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            ErrorBody::Basic(b) => &b.message,
            ErrorBody::Backoff(b) => &b.message,
            ErrorBody::PushFailedServer(b) => &b.message,
            ErrorBody::PushFailedHttp(b) => &b.message,
            ErrorBody::PushFailedZeroCache(b) => &b.message,
            ErrorBody::TransformFailedServer(b) => &b.message,
            ErrorBody::TransformFailedHttp(b) => &b.message,
            ErrorBody::TransformFailedZeroCache(b) => &b.message,
        }
    }
}

/// Convenience constructors for common errors.
impl ErrorBody {
    pub fn version_not_supported(message: impl Into<String>) -> Self {
        ErrorBody::Basic(BasicErrorBody {
            kind: ErrorKind::VersionNotSupported,
            message: message.into(),
            origin: Some(ErrorOrigin::ZeroCache),
        })
    }

    pub fn invalid_message(message: impl Into<String>) -> Self {
        ErrorBody::Basic(BasicErrorBody {
            kind: ErrorKind::InvalidMessage,
            message: message.into(),
            origin: Some(ErrorOrigin::ZeroCache),
        })
    }

    pub fn invalid_push(message: impl Into<String>) -> Self {
        ErrorBody::Basic(BasicErrorBody {
            kind: ErrorKind::InvalidPush,
            message: message.into(),
            origin: Some(ErrorOrigin::ZeroCache),
        })
    }

    pub fn client_not_found(message: impl Into<String>) -> Self {
        ErrorBody::Basic(BasicErrorBody {
            kind: ErrorKind::ClientNotFound,
            message: message.into(),
            origin: Some(ErrorOrigin::ZeroCache),
        })
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        ErrorBody::Basic(BasicErrorBody {
            kind: ErrorKind::Unauthorized,
            message: message.into(),
            origin: Some(ErrorOrigin::ZeroCache),
        })
    }

    pub fn internal(message: impl Into<String>) -> Self {
        ErrorBody::Basic(BasicErrorBody {
            kind: ErrorKind::Internal,
            message: message.into(),
            origin: Some(ErrorOrigin::ZeroCache),
        })
    }

    pub fn rehome(message: impl Into<String>) -> Self {
        ErrorBody::Backoff(BackoffBody {
            kind: ErrorKind::Rehome,
            message: message.into(),
            min_backoff_ms: None,
            max_backoff_ms: None,
            reconnect_params: None,
            origin: Some(ErrorOrigin::ZeroCache),
        })
    }
}

// ─── Queries patch ─────────────────────────────────────────────────────────

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

// ─── Row patch ─────────────────────────────────────────────────────────────

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

// ─── Mutations patch ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum MutationPatchOp {
    #[serde(rename = "put")]
    Put { mutation: Value },
    #[serde(rename = "del")]
    Del { id: MutationID },
}

pub type MutationsPatch = Vec<MutationPatchOp>;

// ─── Connected ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectedBody {
    pub wsid: String,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectedMessage {
    #[serde(rename = "0")]
    pub msg_type: String, // "connected"
    #[serde(rename = "1")]
    pub body: ConnectedBody,
}

impl ConnectedMessage {
    pub fn new(wsid: String) -> Self {
        Self {
            msg_type: "connected".to_string(),
            body: ConnectedBody {
                wsid,
                timestamp: Some(now_ms()),
            },
        }
    }
}

// ─── Pong ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongBody {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PongMessage {
    #[serde(rename = "0")]
    pub msg_type: String, // "pong"
    #[serde(rename = "1")]
    pub body: PongBody,
}

impl PongMessage {
    pub fn new() -> Self {
        Self {
            msg_type: "pong".to_string(),
            body: PongBody {},
        }
    }
}

// ─── Error message ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMessage {
    #[serde(rename = "0")]
    pub msg_type: String, // "error"
    #[serde(rename = "1")]
    pub body: ErrorBody,
}

impl ErrorMessage {
    pub fn new(body: ErrorBody) -> Self {
        Self {
            msg_type: "error".to_string(),
            body,
        }
    }
}

// ─── Poke messages ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PokeStartBody {
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    pub base_cookie: NullableVersion,
    pub schema_versions: Option<SchemaVersions>,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaVersions {
    pub min_supported_version: i64,
    pub max_supported_version: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PokePartBody {
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    #[serde(rename = "lastMutationIDChanges")]
    pub last_mutation_id_changes: Option<serde_json::Map<String, Value>>,
    #[serde(rename = "desiredQueriesPatches")]
    pub desired_queries_patches: Option<serde_json::Map<String, Value>>,
    #[serde(rename = "gotQueriesPatch")]
    pub got_queries_patch: Option<Value>,
    #[serde(rename = "rowsPatch")]
    pub rows_patch: Option<RowsPatch>,
    #[serde(rename = "mutationsPatch")]
    pub mutations_patch: Option<MutationsPatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PokeEndBody {
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    pub cookie: Version,
    pub cancel: Option<bool>,
}

// ─── Delete clients ────────────────────────────────────────────────────────

// deleteClientsBodySchema uses clientIDs/clientGroupIDs (capital IDs)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeleteClientsBody {
    #[serde(rename = "clientIDs")]
    pub client_ids: Option<Vec<String>>,
    #[serde(rename = "clientGroupIDs")]
    pub client_group_ids: Option<Vec<String>>,
}

// ─── Init connection body ──────────────────────────────────────────────────

// initConnectionBodySchema uses userPushURL/userQueryURL (capital URL)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitConnectionBody {
    pub desired_queries_patch: UpQueriesPatch,
    pub client_schema: Option<Value>,
    pub deleted: Option<DeleteClientsBody>,
    #[serde(rename = "userPushURL")]
    pub user_push_url: Option<String>,
    pub user_push_headers: Option<serde_json::Map<String, Value>>,
    #[serde(rename = "userQueryURL")]
    pub user_query_url: Option<String>,
    pub user_query_headers: Option<serde_json::Map<String, Value>>,
    pub active_clients: Option<Vec<String>>,
    pub traceparent: Option<String>,
}

/// The init connection message tuple `["initConnection", body]`.
pub type InitConnectionMessage = (String, InitConnectionBody);

// ─── Change desired queries ────────────────────────────────────────────────

// changeDesiredQueriesBodySchema uses desiredQueriesPatch
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeDesiredQueriesBody {
    pub desired_queries_patch: UpQueriesPatch,
    pub traceparent: Option<String>,
}

// ─── Update auth ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateAuthBody {
    pub auth: String,
}

// ─── Push body ─────────────────────────────────────────────────────────────

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

// ─── Ack mutation responses ────────────────────────────────────────────────

// ackMutationResponsesSchema uses clientID (capital ID)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckMutationResponsesBody {
    pub id: i64,
    #[serde(rename = "clientID")]
    pub client_id: String,
}

// ─── Inspect up ────────────────────────────────────────────────────────────

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

// ─── Upstream message union ────────────────────────────────────────────────
//
// All upstream messages are `["messageType", body]` tuples.
// We deserialize the tag first, then the body.

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Upstream {
    /// `["initConnection", body]` — body is parsed separately because it may
    /// arrive in the sec-websocket-protocol header.
    InitConnection(Value),
    /// `["ping", {}]`
    Ping,
    /// `["deleteClients", body]`
    DeleteClients(DeleteClientsBody),
    /// `["changeDesiredQueries", body]`
    ChangeDesiredQueries(ChangeDesiredQueriesBody),
    /// `["pull", body]` — not supported by Zero
    Pull(Value),
    /// `["updateAuth", body]`
    UpdateAuth(UpdateAuthBody),
    /// `["push", body]`
    Push(PushBody),
    /// `["closeConnection", body]` — deprecated, no-op
    CloseConnection,
    /// `["inspect", body]`
    Inspect(InspectUpBody),
    /// `["ackMutationResponses", body]`
    AckMutationResponses(AckMutationResponsesBody),
}

/// Parse an upstream message from a JSON array `["type", body]`.
pub fn parse_upstream(text: &str) -> Result<Upstream, serde_json::Error> {
    let arr: Vec<Value> = serde_json::from_str(text)?;
    if arr.len() < 2 {
        return Err(serde::de::Error::custom("message must be a tuple [type, body]"));
    }
    let msg_type = arr[0].as_str().ok_or_else(|| {
        serde::de::Error::custom("message type must be a string")
    })?;
    let body = &arr[1];

    let result = match msg_type {
        "initConnection" => Upstream::InitConnection(body.clone()),
        "ping" => Upstream::Ping,
        "deleteClients" => Upstream::DeleteClients(serde_json::from_value::<DeleteClientsBody>(body.clone())?),
        "changeDesiredQueries" => Upstream::ChangeDesiredQueries(serde_json::from_value::<ChangeDesiredQueriesBody>(body.clone())?),
        "pull" => Upstream::Pull(body.clone()),
        "updateAuth" => Upstream::UpdateAuth(serde_json::from_value::<UpdateAuthBody>(body.clone())?),
        "push" => Upstream::Push(serde_json::from_value::<PushBody>(body.clone())?),
        "closeConnection" => Upstream::CloseConnection,
        "inspect" => Upstream::Inspect(serde_json::from_value::<InspectUpBody>(body.clone())?),
        "ackMutationResponses" => Upstream::AckMutationResponses(serde_json::from_value::<AckMutationResponsesBody>(body.clone())?),
        other => {
            return Err(serde::de::Error::custom(format!(
                "unknown message type: {other}"
            )))
        }
    };
    Ok(result)
}

// ─── Downstream message helpers ────────────────────────────────────────────
//
// Downstream messages are serialized as `["type", body]` tuples.
// We provide constructors that produce `serde_json::Value` for the WS sink.

/// Serialize a downstream message as a JSON tuple `["type", body]`.
pub fn downstream_message(msg_type: &str, body: &impl Serialize) -> Value {
    serde_json::json!([msg_type, body])
}

/// Create a `["connected", {wsid, timestamp}]` message.
pub fn connected_message(wsid: &str) -> Value {
    downstream_message("connected", &ConnectedBody {
        wsid: wsid.to_string(),
        timestamp: Some(now_ms()),
    })
}

/// Create a `["pong", {}]` message.
pub fn pong_message() -> Value {
    downstream_message("pong", &PongBody {})
}

/// Create a `["error", body]` message.
pub fn error_message(body: &ErrorBody) -> Value {
    downstream_message("error", body)
}

// ─── Sec-websocket-protocol encoding/decoding ──────────────────────────────

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
    let decoded = urlencoding::decode(header)
        .map_err(|e| DecodeError::UrlDecode(e.to_string()))?;
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

// ─── Utility ───────────────────────────────────────────────────────────────

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
