//! Ported TS tests for zero-protocol.
//!
//! Ports:
//! - `packages/zero-protocol/src/connect.test.ts` — sec-protocol encode/decode round-trip
//! - `packages/zero-protocol/src/error.test.ts` — ProtocolError properties
//! - `packages/zero-protocol/src/protocol-version.test.ts` — version constants

use base64::Engine as _;
use rust_syncer::protocol::*;

// ─── protocol-version.test.ts ──────────────────────────────────────────────

#[test]
fn test_protocol_version_is_51() {
    // From protocol-version.test.ts:
    // "If this test fails upstream or downstream schema has changed such that
    // old code will not understand the new schema, bump the PROTOCOL_VERSION"
    assert_eq!(PROTOCOL_VERSION, 51);
}

#[test]
fn test_min_supported_is_less_than_protocol_version() {
    // From protocol-version.ts: assert(MIN < PROTOCOL_VERSION)
    assert!(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL < PROTOCOL_VERSION);
}

// ─── connect.test.ts: encode/decode round-trip ─────────────────────────────

/// Port of `encodeSecProtocols` from connect.ts.
/// Used to test round-trip: encode → decode → compare.
fn encode_sec_protocols(
    init_connection_message: Option<&InitConnectionMessage>,
    auth_token: Option<&str>,
) -> String {
    let protocols = serde_json::json!({
        "initConnectionMessage": init_connection_message,
        "authToken": auth_token,
    });
    let json = serde_json::to_string(&protocols).unwrap();
    let bytes = json.as_bytes();
    let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    urlencoding::encode(&b64).to_string()
}

#[test]
fn test_sec_protocols_round_trip_empty() {
    // encode(null, null) → decode → (null, null)
    let encoded = encode_sec_protocols(None, None);
    let decoded = decode_sec_protocols(&encoded).unwrap();
    assert!(decoded.init_connection_message.is_none());
    assert!(decoded.auth_token.is_none());
}

#[test]
fn test_sec_protocols_round_trip_with_auth() {
    let encoded = encode_sec_protocols(None, Some("mytoken"));
    let decoded = decode_sec_protocols(&encoded).unwrap();
    assert!(decoded.init_connection_message.is_none());
    assert_eq!(decoded.auth_token, Some("mytoken".to_string()));
}

#[test]
fn test_sec_protocols_round_trip_with_init_connection() {
    let init_msg: InitConnectionMessage = (
        "initConnection".to_string(),
        InitConnectionBody {
            desired_queries_patch: vec![serde_json::json!({"op": "del", "hash": "abc"})],
            client_schema: None,
            deleted: None,
            user_push_url: None,
            user_push_headers: None,
            user_query_url: None,
            user_query_headers: None,
            active_clients: None,
            traceparent: None,
        },
    );
    let encoded = encode_sec_protocols(Some(&init_msg), Some("auth-token"));
    let decoded = decode_sec_protocols(&encoded).unwrap();
    assert!(decoded.init_connection_message.is_some());
    let (msg_type, _body) = decoded.init_connection_message.unwrap();
    assert_eq!(msg_type, "initConnection");
    assert_eq!(decoded.auth_token, Some("auth-token".to_string()));
}

#[test]
fn test_sec_protocols_round_trip_unicode() {
    // Test with unicode characters in auth token
    let encoded = encode_sec_protocols(None, Some("héllo→wörld"));
    let decoded = decode_sec_protocols(&encoded).unwrap();
    assert_eq!(decoded.auth_token, Some("héllo→wörld".to_string()));
}

#[test]
fn test_sec_protocols_round_trip_large_hash() {
    // Port of "encodeSecProtocol with too much data" — tests that large
    // strings don't cause stack overflow (TS: String.fromCharCode spread issue)
    let large_string = "\u{0}".repeat(1 << 20); // 1MB
    let init_msg: InitConnectionMessage = (
        "initConnection".to_string(),
        InitConnectionBody {
            desired_queries_patch: vec![serde_json::json!({"op": "del", "hash": &large_string})],
            client_schema: None,
            deleted: None,
            user_push_url: None,
            user_push_headers: None,
            user_query_url: None,
            user_query_headers: None,
            active_clients: None,
            traceparent: None,
        },
    );
    // Should not panic/overflow
    let encoded = encode_sec_protocols(Some(&init_msg), Some("authToken"));
    let decoded = decode_sec_protocols(&encoded).unwrap();
    assert!(decoded.init_connection_message.is_some());
}

#[test]
fn test_sec_protocols_round_trip_random() {
    // Port of the fast-check property test — try multiple combinations
    let test_cases = vec![
        (None, None),
        (None, Some("")),
        (None, Some("token-abc-123")),
        (None, Some("unicode: 日本語")),
    ];

    for (init_msg, auth) in &test_cases {
        let encoded = encode_sec_protocols(*init_msg, *auth);
        let decoded = decode_sec_protocols(&encoded).unwrap();
        assert_eq!(
            decoded.init_connection_message.is_some(),
            init_msg.is_some()
        );
        assert_eq!(decoded.auth_token.as_deref(), *auth);
    }
}

// ─── error.test.ts: ProtocolError properties ───────────────────────────────

#[test]
fn test_error_body_basic_properties() {
    // Port: "exposes error body and metadata"
    let body = ErrorBody::Basic(BasicErrorBody {
        kind: ErrorKind::InvalidPush,
        message: "invalid push".to_string(),
        origin: Some(ErrorOrigin::Server),
    });

    assert_eq!(body.kind(), &ErrorKind::InvalidPush);
    assert_eq!(body.message(), "invalid push");
}

#[test]
fn test_error_body_unauthorized() {
    let body = ErrorBody::unauthorized("unauthorized");
    assert_eq!(body.kind(), &ErrorKind::Unauthorized);
    assert_eq!(body.message(), "unauthorized");
}

#[test]
fn test_error_body_serialization_matches_ts() {
    // Verify serialized JSON matches the TS wire format exactly
    let body = ErrorBody::Basic(BasicErrorBody {
        kind: ErrorKind::InvalidPush,
        message: "invalid push".to_string(),
        origin: Some(ErrorOrigin::Server),
    });
    let json = serde_json::to_value(&body).unwrap();
    assert_eq!(json["kind"], "InvalidPush");
    assert_eq!(json["message"], "invalid push");
    assert_eq!(json["origin"], "server");
}

#[test]
fn test_error_body_origin_zero_cache() {
    let body = ErrorBody::internal("internal error");
    let json = serde_json::to_value(&body).unwrap();
    assert_eq!(json["origin"], "zeroCache");
}

#[test]
fn test_backoff_error_body_serialization() {
    let body = ErrorBody::Backoff(BackoffBody {
        kind: ErrorKind::Rehome,
        message: "Reconnect required".to_string(),
        min_backoff_ms: Some(1000),
        max_backoff_ms: Some(30000),
        reconnect_params: Some(serde_json::Map::from_iter(vec![(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        )])),
        origin: Some(ErrorOrigin::ZeroCache),
    });
    let json = serde_json::to_value(&body).unwrap();
    assert_eq!(json["kind"], "Rehome");
    assert_eq!(json["minBackoffMs"], 1000);
    assert_eq!(json["maxBackoffMs"], 30000);
    assert_eq!(json["reconnectParams"]["key"], "value");
    assert_eq!(json["origin"], "zeroCache");
}

#[test]
fn test_error_body_deserialization_round_trip() {
    // Deserialize a TS-serialized error body
    let json = r#"{"kind":"ClientNotFound","message":"Client not found","origin":"zeroCache"}"#;
    let body: ErrorBody = serde_json::from_str(json).unwrap();
    assert_eq!(body.kind(), &ErrorKind::ClientNotFound);
    assert_eq!(body.message(), "Client not found");
}

#[test]
fn test_error_body_deserialization_no_origin() {
    // origin is optional for backwards compatibility
    let json = r#"{"kind":"Internal","message":"oops"}"#;
    let body: ErrorBody = serde_json::from_str(json).unwrap();
    assert_eq!(body.kind(), &ErrorKind::Internal);
    assert_eq!(body.message(), "oops");
}

#[test]
fn test_error_message_serialization() {
    let body = ErrorBody::invalid_message("parse failed");
    let msg = error_message(&body);
    assert_eq!(msg[0], "error");
    assert_eq!(msg[1]["kind"], "InvalidMessage");
    assert_eq!(msg[1]["message"], "parse failed");
    assert_eq!(msg[1]["origin"], "zeroCache");
}

// ─── Error kind serialization ──────────────────────────────────────────────

#[test]
fn test_error_kind_all_variants_serialize() {
    // Verify all ErrorKind variants serialize to their exact TS string values
    let kinds = vec![
        (ErrorKind::AuthInvalidated, "AuthInvalidated"),
        (ErrorKind::ClientNotFound, "ClientNotFound"),
        (
            ErrorKind::InvalidConnectionRequest,
            "InvalidConnectionRequest",
        ),
        (
            ErrorKind::InvalidConnectionRequestBaseCookie,
            "InvalidConnectionRequestBaseCookie",
        ),
        (
            ErrorKind::InvalidConnectionRequestLastMutationID,
            "InvalidConnectionRequestLastMutationID",
        ),
        (
            ErrorKind::InvalidConnectionRequestClientDeleted,
            "InvalidConnectionRequestClientDeleted",
        ),
        (ErrorKind::InvalidMessage, "InvalidMessage"),
        (ErrorKind::InvalidPush, "InvalidPush"),
        (ErrorKind::PushFailed, "PushFailed"),
        (ErrorKind::MutationFailed, "MutationFailed"),
        (ErrorKind::MutationRateLimited, "MutationRateLimited"),
        (ErrorKind::Rebalance, "Rebalance"),
        (ErrorKind::Rehome, "Rehome"),
        (ErrorKind::TransformFailed, "TransformFailed"),
        (ErrorKind::Unauthorized, "Unauthorized"),
        (ErrorKind::VersionNotSupported, "VersionNotSupported"),
        (
            ErrorKind::SchemaVersionNotSupported,
            "SchemaVersionNotSupported",
        ),
        (ErrorKind::ServerOverloaded, "ServerOverloaded"),
        (ErrorKind::Internal, "Internal"),
    ];

    for (kind, expected) in kinds {
        let json = serde_json::to_value(&kind).unwrap();
        assert_eq!(json, expected, "ErrorKind serialization mismatch");
    }
}

#[test]
fn test_error_origin_all_variants_serialize() {
    assert_eq!(
        serde_json::to_value(&ErrorOrigin::Client).unwrap(),
        "client"
    );
    assert_eq!(
        serde_json::to_value(&ErrorOrigin::Server).unwrap(),
        "server"
    );
    assert_eq!(
        serde_json::to_value(&ErrorOrigin::ZeroCache).unwrap(),
        "zeroCache"
    );
}

#[test]
fn test_error_reason_all_variants_serialize() {
    let reasons = vec![
        (ErrorReason::Database, "database"),
        (ErrorReason::Parse, "parse"),
        (ErrorReason::OutOfOrderMutation, "oooMutation"),
        (
            ErrorReason::UnsupportedPushVersion,
            "unsupportedPushVersion",
        ),
        (ErrorReason::Internal, "internal"),
        (ErrorReason::Http, "http"),
        (ErrorReason::Timeout, "timeout"),
    ];

    for (reason, expected) in reasons {
        let json = serde_json::to_value(&reason).unwrap();
        assert_eq!(json, expected, "ErrorReason serialization mismatch");
    }
}

// ─── Downstream message constructors ───────────────────────────────────────

#[test]
fn test_connected_message_format() {
    let msg = connected_message("ws-123", "zero", 0);
    assert_eq!(msg[0], "connected");
    assert_eq!(msg[1]["wsid"], "ws-123");
    assert!(msg[1]["timestamp"].is_number());
    assert_eq!(msg[1]["appID"], "zero");
    assert_eq!(msg[1]["shardNum"], 0);
}

#[test]
fn test_pong_message_format() {
    let msg = pong_message();
    assert_eq!(msg[0], "pong");
    assert!(msg[1].as_object().unwrap().is_empty());
}

#[test]
fn test_connected_message_serializes_to_valid_json() {
    let msg = connected_message("test-ws", "zero", 0);
    let text = serde_json::to_string(&msg).unwrap();
    // Should be a JSON array: ["connected", {"wsid": "...", "timestamp": ..., ...}]
    assert!(text.starts_with(r#"["connected",{"wsid":"test-ws","timestamp":"#));
}

// ─── Upstream message parsing ──────────────────────────────────────────────

#[test]
fn test_parse_all_upstream_message_types() {
    // Port: verify all 10 upstream message types parse correctly
    let test_cases = vec![
        (r#"["ping",{}]"#, "ping"),
        (r#"["closeConnection",[]]"#, "closeConnection"),
        (r#"["deleteClients",{"clientIDs":["c1"]}]"#, "deleteClients"),
        (
            r#"["changeDesiredQueries",{"desiredQueriesPatch":[]}]"#,
            "changeDesiredQueries",
        ),
        (r#"["updateAuth",{"auth":"token"}]"#, "updateAuth"),
        (
            r#"["ackMutationResponses",{"id":1,"clientID":"c1"}]"#,
            "ackMutationResponses",
        ),
        (
            r#"["initConnection",{"desiredQueriesPatch":[]}]"#,
            "initConnection",
        ),
        (
            r#"["pull",{"clientGroupID":"g1","cookie":null,"requestID":"r1"}]"#,
            "pull",
        ),
    ];

    for (json, expected_type) in test_cases {
        let result = parse_upstream(json);
        assert!(
            result.is_ok(),
            "Failed to parse {expected_type}: {:?}",
            result.err()
        );
    }
}

#[test]
fn test_parse_push_message() {
    let msg = r#"["push",{"clientGroupID":"cg1","mutations":[],"pushVersion":1,"timestamp":123,"requestID":"req1"}]"#;
    let result = parse_upstream(msg).unwrap();
    match result {
        Upstream::Push(body) => {
            assert_eq!(body.client_group_id, "cg1");
            assert_eq!(body.push_version, 1);
            assert_eq!(body.timestamp, 123);
            assert_eq!(body.request_id, "req1");
            assert!(body.mutations.is_empty());
        }
        _ => panic!("expected Push"),
    }
}

#[test]
fn test_parse_inspect_queries() {
    let msg = r#"["inspect",{"op":"queries","id":"req1","clientID":"c1"}]"#;
    let result = parse_upstream(msg).unwrap();
    match result {
        Upstream::Inspect(InspectUpBody::Queries { id, client_id }) => {
            assert_eq!(id, "req1");
            assert_eq!(client_id, Some("c1".to_string()));
        }
        _ => panic!("expected Inspect::Queries"),
    }
}

#[test]
fn test_parse_inspect_metrics() {
    let msg = r#"["inspect",{"op":"metrics","id":"req1"}]"#;
    let result = parse_upstream(msg).unwrap();
    match result {
        Upstream::Inspect(InspectUpBody::Metrics { id }) => {
            assert_eq!(id, "req1");
        }
        _ => panic!("expected Inspect::Metrics"),
    }
}

#[test]
fn test_parse_inspect_version() {
    let msg = r#"["inspect",{"op":"version","id":"req1"}]"#;
    let result = parse_upstream(msg).unwrap();
    match result {
        Upstream::Inspect(InspectUpBody::Version { id }) => {
            assert_eq!(id, "req1");
        }
        _ => panic!("expected Inspect::Version"),
    }
}

#[test]
fn test_parse_inspect_authenticate() {
    let msg = r#"["inspect",{"op":"authenticate","id":"req1","value":"password123"}]"#;
    let result = parse_upstream(msg).unwrap();
    match result {
        Upstream::Inspect(InspectUpBody::Authenticate { id, value }) => {
            assert_eq!(id, "req1");
            assert_eq!(value, "password123");
        }
        _ => panic!("expected Inspect::Authenticate"),
    }
}

#[test]
fn test_parse_unknown_message_type_errors() {
    let msg = r#"["unknown",{}]"#;
    assert!(parse_upstream(msg).is_err());
}

#[test]
fn test_parse_invalid_json_errors() {
    assert!(parse_upstream("not json").is_err());
    assert!(parse_upstream(r#"{}"#).is_err()); // not an array
    assert!(parse_upstream(r#"[]"#).is_err()); // empty array
    assert!(parse_upstream(r#"[123]"#).is_err()); // type not a string
}

// ─── Connect params ────────────────────────────────────────────────────────

use rust_syncer::connect_params::{extract_protocol_version, get_connect_params};

#[test]
fn test_extract_protocol_version_from_path() {
    assert_eq!(extract_protocol_version("/sync/v51/connect"), Some(51));
    assert_eq!(extract_protocol_version("/sync/v30/connect"), Some(30));
    assert_eq!(extract_protocol_version("/sync/connect"), None);
    assert_eq!(extract_protocol_version("/v51/connect"), Some(51));
    assert_eq!(extract_protocol_version("/sync/v0/connect"), Some(0));
}

fn make_sec_protocol(auth: Option<&str>) -> String {
    let json = serde_json::json!({
        "initConnectionMessage": null,
        "authToken": auth,
    });
    let b64 = base64::engine::general_purpose::STANDARD.encode(json.to_string());
    urlencoding::encode(&b64).to_string()
}

#[test]
fn test_connect_params_full() {
    let sec = make_sec_protocol(Some("testtoken"));
    let url = "http://localhost/sync/v51/connect?clientID=c1&clientGroupID=cg1&ts=1234567890&lmid=42&wsid=ws-1&userID=u1&debugPerf=true";

    let params = get_connect_params(
        51,
        url,
        Some(&sec),
        Some("cookie=abc"),
        Some("https://example.com"),
    )
    .unwrap();

    assert_eq!(params.protocol_version, 51);
    assert_eq!(params.client_id, "c1");
    assert_eq!(params.client_group_id, "cg1");
    assert_eq!(params.timestamp, 1234567890);
    assert_eq!(params.lm_id, 42);
    assert_eq!(params.ws_id, "ws-1");
    assert_eq!(params.user_id, Some("u1".to_string()));
    assert!(params.debug_perf);
    assert_eq!(params.auth, Some("testtoken".to_string()));
    assert_eq!(params.http_cookie, Some("cookie=abc".to_string()));
    assert_eq!(params.origin, Some("https://example.com".to_string()));
}

#[test]
fn test_connect_params_missing_required_field() {
    let sec = make_sec_protocol(None);
    // Missing clientID
    let url = "http://localhost/sync/v51/connect?clientGroupID=cg1&ts=123&lmid=42";
    assert!(get_connect_params(51, url, Some(&sec), None, None).is_err());
}

#[test]
fn test_connect_params_optional_defaults() {
    let sec = make_sec_protocol(None);
    let url = "http://localhost/sync/v51/connect?clientID=c1&clientGroupID=cg1&ts=123&lmid=42";

    let params = get_connect_params(51, url, Some(&sec), None, None).unwrap();

    assert_eq!(params.ws_id, "");
    assert_eq!(params.user_id, None);
    assert!(!params.debug_perf);
    assert_eq!(params.profile_id, None);
    assert_eq!(params.base_cookie, None);
}

#[test]
fn test_connect_params_missing_sec_protocol() {
    let url = "http://localhost/sync/v51/connect?clientID=c1&clientGroupID=cg1&ts=123&lmid=42";
    assert!(get_connect_params(51, url, None, None, None).is_err());
}

#[test]
fn test_connect_params_invalid_integer() {
    let sec = make_sec_protocol(None);
    let url = "http://localhost/sync/v51/connect?clientID=c1&clientGroupID=cg1&ts=abc&lmid=42";
    assert!(get_connect_params(51, url, Some(&sec), None, None).is_err());
}

#[test]
fn test_connect_params_debug_perf_false_by_default() {
    let sec = make_sec_protocol(None);
    let url = "http://localhost/sync/v51/connect?clientID=c1&clientGroupID=cg1&ts=123&lmid=42&debugPerf=false";
    let params = get_connect_params(51, url, Some(&sec), None, None).unwrap();
    assert!(!params.debug_perf);
}

#[test]
fn test_connect_params_with_optional_fields() {
    let sec = make_sec_protocol(None);
    let url = "http://localhost/sync/v51/connect?clientID=c1&clientGroupID=cg1&ts=123&lmid=42&profileID=p1&baseCookie=bc1&wsid=ws-2";
    let params = get_connect_params(51, url, Some(&sec), None, None).unwrap();
    assert_eq!(params.profile_id, Some("p1".to_string()));
    assert_eq!(params.base_cookie, Some("bc1".to_string()));
    assert_eq!(params.ws_id, "ws-2");
}

// ─── TS test port status ────────────────────────────────────────────────────
//
// PORTED (unit-testable Rust equivalents now exist):
// - connection.test.ts log-level classification → `connection.rs` tests
//   (ClientNotFound/TransformFailed → warn, compressed-socket-close → warn,
//   internal → error, protocol → info).
// - syncer-ws-message-handler.test.ts push routing / deleteClients /
//   ackMutationResponses forwarding → `tests/phase2_test.rs`.
// - inspect-handler.test.ts auth gate + version → `router.rs`
//   `inspect_auth_gate_then_version`.
// - JWT auth validation (secret/jwk/jwks, sub match, opaque, fail-closed) →
//   `auth.rs` tests.
//
// STILL PENDING — these depend on subsystems intentionally left as placeholders
// or on live infrastructure (tracked with the PG test harness):
// - connection-context-manager.test.ts: background-connection selection +
//   maintenance planning (revalidation/retransform deadlines, defer, stale
//   revision). The Rust ConnContextManager is a placeholder (auth state lives on
//   the CG thread); these need the full CCM port before they can be ported.
// - client-handler.test.ts poke non-interleaving / ensureSafeJSON: exercise
//   `rust-cvr`'s ClientHandler (covered by that crate's own suite).
// - view-syncer.pg.test.ts / syncer.test.ts: large integration tests needing a
//   live Postgres + replica (the planned PG harness); e.g. connection hijacking
//   prevention + ref counting are integration-level.
