//! Port of `packages/zero-protocol/src/error.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use super::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

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

    pub fn basic(kind: ErrorKind, message: String) -> Self {
        ErrorBody::Basic(BasicErrorBody {
            kind,
            message,
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

/// Create a `["error", body]` message.
pub fn error_message(body: &ErrorBody) -> Value {
    downstream_message("error", body)
}
