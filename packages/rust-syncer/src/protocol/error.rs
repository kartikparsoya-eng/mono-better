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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<ErrorOrigin>,
}

/// Backoff error body (Rebalance, Rehome, ServerOverloaded).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackoffBody {
    pub kind: ErrorKind,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_backoff_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_backoff_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_params: Option<serde_json::Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<ErrorOrigin>,
}

/// PushFailed error with server origin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushFailedServerBody {
    pub kind: ErrorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    #[serde(rename = "mutationIDs")]
    pub mutation_ids: Vec<MutationID>,
    pub message: String,
    pub origin: ErrorOrigin,
    pub reason: ErrorReason,
    pub status: crate::protocol::JsNumber,
    #[serde(
        rename = "bodyPreview",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub body_preview: Option<String>,
}

/// PushFailed error with ZeroCache origin + non-HTTP reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushFailedZeroCacheBody {
    pub kind: ErrorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    #[serde(rename = "queryIDs")]
    pub query_ids: Vec<String>,
    pub message: String,
    pub origin: ErrorOrigin,
    pub reason: ErrorReason,
    pub status: crate::protocol::JsNumber,
    #[serde(
        rename = "bodyPreview",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub body_preview: Option<String>,
}

/// TransformFailed error with ZeroCache origin + non-HTTP reason.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformFailedZeroCacheBody {
    pub kind: ErrorKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
    #[serde(rename = "queryIDs")]
    pub query_ids: Vec<String>,
    pub message: String,
    pub origin: ErrorOrigin,
    pub reason: ErrorReason,
}

/// The full error body union. Matches `errorBodySchema` in error.ts.
///
/// Serialization is untagged (each member is a flat object). Deserialization
/// is NOT serde's first-struct-that-fits: it mirrors the valita union, which is
/// discriminated by `kind` (basic / backoff / PushFailed / TransformFailed)
/// and, for the two failure families, by `origin` + `reason` — see
/// `impl Deserialize`. (Untagged first-fit bound every Backoff body to `Basic`
/// and every http PushFailed body to `PushFailedServer`, silently dropping
/// `minBackoffMs` / `status` / `bodyPreview`; pinned by
/// `error_body_wire_parity_against_ts`.)
#[derive(Debug, Clone, Serialize)]
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

/// Members of TS `basicErrorKindSchema` (error.ts:8-23).
const BASIC_ERROR_KINDS: &[ErrorKind] = &[
    ErrorKind::AuthInvalidated,
    ErrorKind::ClientNotFound,
    ErrorKind::InvalidConnectionRequest,
    ErrorKind::InvalidConnectionRequestBaseCookie,
    ErrorKind::InvalidConnectionRequestLastMutationID,
    ErrorKind::InvalidConnectionRequestClientDeleted,
    ErrorKind::InvalidMessage,
    ErrorKind::InvalidPush,
    ErrorKind::MutationRateLimited,
    ErrorKind::MutationFailed,
    ErrorKind::Unauthorized,
    ErrorKind::VersionNotSupported,
    ErrorKind::SchemaVersionNotSupported,
    ErrorKind::Internal,
];

/// Members of TS `backoffErrorKindSchema` (error.ts:32-36).
const BACKOFF_ERROR_KINDS: &[ErrorKind] = &[
    ErrorKind::Rebalance,
    ErrorKind::Rehome,
    ErrorKind::ServerOverloaded,
];

/// `reason` literal unions of the `origin: server` members (error.ts:78-84,
/// 115-119) and of the non-http `origin: zeroCache` members (error.ts:94-98,
/// 129-133).
const PUSH_FAILED_SERVER_REASONS: &[ErrorReason] = &[
    ErrorReason::Database,
    ErrorReason::Parse,
    ErrorReason::OutOfOrderMutation,
    ErrorReason::UnsupportedPushVersion,
    ErrorReason::Internal,
];
const TRANSFORM_FAILED_SERVER_REASONS: &[ErrorReason] = &[
    ErrorReason::Database,
    ErrorReason::Parse,
    ErrorReason::Internal,
];
const ZERO_CACHE_REASONS: &[ErrorReason] = &[
    ErrorReason::Timeout,
    ErrorReason::Parse,
    ErrorReason::Internal,
];

impl<'de> Deserialize<'de> for ErrorBody {
    /// Port of the `errorBodySchema` union (error.ts:137-142) — resolve the
    /// member the way valita does (by the discriminating literals), then
    /// parse the whole object as that member so every required field is
    /// still enforced.
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        let value = Value::deserialize(deserializer)?;
        let field = |name: &str| value.get(name).cloned().unwrap_or(Value::Null);
        let kind: ErrorKind = serde_json::from_value(field("kind"))
            .map_err(|e| D::Error::custom(format!("error body kind: {e}")))?;
        let origin: Option<ErrorOrigin> = serde_json::from_value(field("origin"))
            .map_err(|e| D::Error::custom(format!("error body origin: {e}")))?;
        let reason: Option<ErrorReason> = serde_json::from_value(field("reason"))
            .map_err(|e| D::Error::custom(format!("error body reason: {e}")))?;
        let member = |body: Result<ErrorBody, serde_json::Error>| {
            body.map_err(|e| D::Error::custom(format!("error body ({kind:?}): {e}")))
        };
        let no_member = || {
            D::Error::custom(format!(
                "error body ({kind:?}) matches no errorBodySchema member: origin {origin:?}, reason {reason:?}"
            ))
        };
        match &kind {
            ErrorKind::PushFailed => match (origin.as_ref(), reason.as_ref()) {
                (Some(ErrorOrigin::Server), Some(r)) if PUSH_FAILED_SERVER_REASONS.contains(r) => {
                    member(serde_json::from_value(value).map(ErrorBody::PushFailedServer))
                }
                (Some(ErrorOrigin::ZeroCache), Some(ErrorReason::Http)) => {
                    member(serde_json::from_value(value).map(ErrorBody::PushFailedHttp))
                }
                (Some(ErrorOrigin::ZeroCache), Some(r)) if ZERO_CACHE_REASONS.contains(r) => {
                    member(serde_json::from_value(value).map(ErrorBody::PushFailedZeroCache))
                }
                _ => Err(no_member()),
            },
            ErrorKind::TransformFailed => match (origin.as_ref(), reason.as_ref()) {
                (Some(ErrorOrigin::Server), Some(r))
                    if TRANSFORM_FAILED_SERVER_REASONS.contains(r) =>
                {
                    member(serde_json::from_value(value).map(ErrorBody::TransformFailedServer))
                }
                (Some(ErrorOrigin::ZeroCache), Some(ErrorReason::Http)) => {
                    member(serde_json::from_value(value).map(ErrorBody::TransformFailedHttp))
                }
                (Some(ErrorOrigin::ZeroCache), Some(r)) if ZERO_CACHE_REASONS.contains(r) => {
                    member(serde_json::from_value(value).map(ErrorBody::TransformFailedZeroCache))
                }
                _ => Err(no_member()),
            },
            k if BACKOFF_ERROR_KINDS.contains(k) => {
                // backoffBodySchema: `origin` is `literal(ZeroCache).optional()`.
                if matches!(
                    origin,
                    Some(ErrorOrigin::Server) | Some(ErrorOrigin::Client)
                ) {
                    return Err(no_member());
                }
                member(serde_json::from_value(value).map(ErrorBody::Backoff))
            }
            k if BASIC_ERROR_KINDS.contains(k) => {
                // basicErrorBodySchema: `origin` is `literalUnion(Server, ZeroCache).optional()`.
                if matches!(origin, Some(ErrorOrigin::Client)) {
                    return Err(no_member());
                }
                member(serde_json::from_value(value).map(ErrorBody::Basic))
            }
            _ => Err(no_member()),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn variant_name(body: &ErrorBody) -> &'static str {
        match body {
            ErrorBody::Basic(_) => "Basic",
            ErrorBody::Backoff(_) => "Backoff",
            ErrorBody::PushFailedServer(_) => "PushFailedServer",
            ErrorBody::PushFailedHttp(_) => "PushFailedHttp",
            ErrorBody::PushFailedZeroCache(_) => "PushFailedZeroCache",
            ErrorBody::TransformFailedServer(_) => "TransformFailedServer",
            ErrorBody::TransformFailedHttp(_) => "TransformFailedHttp",
            ErrorBody::TransformFailedZeroCache(_) => "TransformFailedZeroCache",
        }
    }

    /// Layer-2 wire-shape differential against `errorBodySchema` (error.ts):
    /// every TS-valid body (generated + valita-validated by
    /// `generate-error-wire-fixture.mjs`) must (1) bind to the mirrored union
    /// member — the TS union is discriminated by `kind`, then `origin`/`reason`,
    /// NOT by first-struct-that-fits — and (2) re-serialize to JSON-equal
    /// output: same field names, same enum strings, and optional fields ABSENT
    /// (not `null`) when unset, because valita `.optional()` rejects `null` and
    /// zero-client answers an unparseable frame with an InvalidMessage
    /// disconnect.
    #[test]
    fn error_body_wire_parity_against_ts() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/agentic/parity/error-wire-fixture.json"
        );
        let bytes = std::fs::read(path).expect("read error-wire-fixture.json");
        let cases: Value = serde_json::from_slice(&bytes).expect("fixture is valid JSON");
        let cases = cases.as_array().expect("fixture is an array");
        assert!(cases.len() >= 16, "fixture must cover every union member");
        for case in cases {
            let variant = case["variant"].as_str().expect("variant");
            let body = &case["body"];
            let parsed: ErrorBody = serde_json::from_value(body.clone())
                .unwrap_or_else(|e| panic!("{variant}: TS-valid body must parse: {e}\n{body}"));
            assert_eq!(
                variant_name(&parsed),
                variant,
                "{variant}: body bound to the wrong union member\n{body}"
            );
            let round = serde_json::to_value(&parsed).expect("serialize");
            assert_eq!(
                &round, body,
                "{variant}: re-serialized body diverges from TS"
            );
        }
    }

    /// The constructors used on the serving path must emit TS-parseable bodies:
    /// no `null` for unset optionals (Rehome is the shed/close path, I-4).
    #[test]
    fn constructors_omit_unset_optional_fields() {
        let rehome = serde_json::to_value(ErrorBody::rehome("shed")).unwrap();
        assert_eq!(
            rehome,
            serde_json::json!({"kind": "Rehome", "message": "shed", "origin": "zeroCache"})
        );
        let basic = ErrorBody::Basic(BasicErrorBody {
            kind: ErrorKind::Internal,
            message: "x".into(),
            origin: None,
        });
        assert_eq!(
            serde_json::to_value(basic).unwrap(),
            serde_json::json!({"kind": "Internal", "message": "x"})
        );
    }
}
