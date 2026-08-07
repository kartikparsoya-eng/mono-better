#[cfg(test)]
mod tests {
    use rust_syncer::protocol::*;
    use base64::Engine as _;

    // ─── Protocol version constants ────────────────────────────────────────

    #[test]
    fn test_protocol_version_matches_ts() {
        assert_eq!(PROTOCOL_VERSION, 51);
        assert_eq!(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL, 30);
    }

    // ─── Error body serialization ──────────────────────────────────────────

    #[test]
    fn test_error_body_serialization() {
        let error = ErrorBody::version_not_supported("bad version");
        let json = serde_json::to_value(&error).unwrap();
        assert_eq!(json["kind"], "VersionNotSupported");
        assert_eq!(json["message"], "bad version");
        assert_eq!(json["origin"], "zeroCache");
    }

    #[test]
    fn test_error_message_serialization() {
        let error = ErrorBody::invalid_message("parse failed");
        let msg = error_message(&error);
        assert_eq!(msg[0], "error");
        assert_eq!(msg[1]["kind"], "InvalidMessage");
    }

    // ─── Connected message ─────────────────────────────────────────────────

    #[test]
    fn test_connected_message() {
        let msg = connected_message("ws-123");
        assert_eq!(msg[0], "connected");
        assert_eq!(msg[1]["wsid"], "ws-123");
        assert!(msg[1]["timestamp"].is_number());
    }

    // ─── Pong message ──────────────────────────────────────────────────────

    #[test]
    fn test_pong_message() {
        let msg = pong_message();
        assert_eq!(msg[0], "pong");
        assert!(msg[1].as_object().unwrap().is_empty());
    }

    // ─── Upstream message parsing ──────────────────────────────────────────

    #[test]
    fn test_parse_ping() {
        let msg = r#"["ping",{}]"#;
        let result = parse_upstream(msg).unwrap();
        assert!(matches!(result, Upstream::Ping));
    }

    #[test]
    fn test_parse_close_connection() {
        let msg = r#"["closeConnection",[]]"#;
        let result = parse_upstream(msg).unwrap();
        assert!(matches!(result, Upstream::CloseConnection));
    }

    #[test]
    fn test_parse_delete_clients() {
        let msg = r#"["deleteClients",{"clientIDs":["c1","c2"]}]"#;
        let result = parse_upstream(msg).unwrap();
        match result {
            Upstream::DeleteClients(body) => {
                assert_eq!(body.client_ids, Some(vec!["c1".to_string(), "c2".to_string()]));
            }
            _ => panic!("expected DeleteClients"),
        }
    }

    #[test]
    fn test_parse_change_desired_queries() {
        let msg = r#"["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"abc123"}]}]"#;
        let result = parse_upstream(msg).unwrap();
        match result {
            Upstream::ChangeDesiredQueries(body) => {
                assert_eq!(body.desired_queries_patch.len(), 1);
            }
            _ => panic!("expected ChangeDesiredQueries"),
        }
    }

    #[test]
    fn test_parse_update_auth() {
        let msg = r#"["updateAuth",{"auth":"token123"}]"#;
        let result = parse_upstream(msg).unwrap();
        match result {
            Upstream::UpdateAuth(body) => {
                assert_eq!(body.auth, "token123");
            }
            _ => panic!("expected UpdateAuth"),
        }
    }

    #[test]
    fn test_parse_push() {
        let msg = r#"["push",{"clientGroupID":"cg1","mutations":[],"pushVersion":1,"timestamp":123,"requestID":"req1"}]"#;
        let result = parse_upstream(msg).unwrap();
        match result {
            Upstream::Push(body) => {
                assert_eq!(body.client_group_id, "cg1");
                assert_eq!(body.push_version, 1);
                assert_eq!(body.timestamp, 123);
                assert_eq!(body.request_id, "req1");
            }
            _ => panic!("expected Push"),
        }
    }

    #[test]
    fn test_parse_ack_mutation_responses() {
        let msg = r#"["ackMutationResponses",{"id":42,"clientID":"c1"}]"#;
        let result = parse_upstream(msg).unwrap();
        match result {
            Upstream::AckMutationResponses(body) => {
                assert_eq!(body.id, 42);
                assert_eq!(body.client_id, "c1");
            }
            _ => panic!("expected AckMutationResponses"),
        }
    }

    #[test]
    fn test_parse_init_connection() {
        let msg = r#"["initConnection",{"desiredQueriesPatch":[]}]"#;
        let result = parse_upstream(msg).unwrap();
        assert!(matches!(result, Upstream::InitConnection(_)));
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
    fn test_parse_unknown_message_type() {
        let msg = r#"["unknown",{}]"#;
        assert!(parse_upstream(msg).is_err());
    }

    #[test]
    fn test_parse_invalid_json() {
        let msg = "not json";
        assert!(parse_upstream(msg).is_err());
    }

    // ─── Sec-websocket-protocol decoding ───────────────────────────────────

    #[test]
    fn test_decode_sec_protocols_empty() {
        // Encode an empty SecProtocols object
        let json = r#"{"initConnectionMessage":null,"authToken":null}"#;
        let encoded = urlencoding::encode(
            &base64::engine::general_purpose::STANDARD.encode(json.as_bytes())
        ).to_string();

        let result = decode_sec_protocols(&encoded).unwrap();
        assert!(result.init_connection_message.is_none());
        assert!(result.auth_token.is_none());
    }

    #[test]
    fn test_decode_sec_protocols_with_auth() {
        let json = r#"{"initConnectionMessage":null,"authToken":"mytoken"}"#;
        let encoded = urlencoding::encode(
            &base64::engine::general_purpose::STANDARD.encode(json.as_bytes())
        ).to_string();

        let result = decode_sec_protocols(&encoded).unwrap();
        assert!(result.init_connection_message.is_none());
        assert_eq!(result.auth_token, Some("mytoken".to_string()));
    }

    #[test]
    fn test_decode_sec_protocols_with_init_connection() {
        let json = r#"{"initConnectionMessage":["initConnection",{"desiredQueriesPatch":[]}],"authToken":null}"#;
        let encoded = urlencoding::encode(
            &base64::engine::general_purpose::STANDARD.encode(json.as_bytes())
        ).to_string();

        let result = decode_sec_protocols(&encoded).unwrap();
        assert!(result.init_connection_message.is_some());
        let (msg_type, _body) = result.init_connection_message.unwrap();
        assert_eq!(msg_type, "initConnection");
        assert!(result.auth_token.is_none());
    }

    #[test]
    fn test_decode_sec_protocols_with_unicode() {
        // Test with unicode in the JSON (e.g., auth token with unicode chars)
        let json = r#"{"initConnectionMessage":null,"authToken":"héllo→wörld"}"#;
        let encoded = urlencoding::encode(
            &base64::engine::general_purpose::STANDARD.encode(json.as_bytes())
        ).to_string();

        let result = decode_sec_protocols(&encoded).unwrap();
        assert_eq!(result.auth_token, Some("héllo→wörld".to_string()));
    }

    // ─── Connect params parsing ────────────────────────────────────────────

    use rust_syncer::connect_params::{get_connect_params, extract_protocol_version};

    #[test]
    fn test_extract_protocol_version() {
        assert_eq!(extract_protocol_version("/sync/v51/connect"), Some(51));
        assert_eq!(extract_protocol_version("/sync/v30/connect"), Some(30));
        assert_eq!(extract_protocol_version("/sync/connect"), None);
        assert_eq!(extract_protocol_version("/v51/connect"), Some(51));
    }

    #[test]
    fn test_get_connect_params_full() {
        // Build sec-websocket-protocol header
        let sec_json = r#"{"initConnectionMessage":null,"authToken":"testtoken"}"#;
        let sec_encoded = urlencoding::encode(
            &base64::engine::general_purpose::STANDARD.encode(sec_json.as_bytes())
        ).to_string();

        let url = format!(
            "http://localhost/sync/v51/connect?clientID=c1&clientGroupID=cg1&ts=1234567890&lmid=42&wsid=ws-1&userID=u1&debugPerf=true"
        );

        let params = get_connect_params(
            51,
            &url,
            Some(&sec_encoded),
            Some("cookie=abc"),
            Some("https://example.com"),
        ).unwrap();

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
    fn test_get_connect_params_missing_required() {
        let sec_json = r#"{"initConnectionMessage":null,"authToken":null}"#;
        let sec_encoded = urlencoding::encode(
            &base64::engine::general_purpose::STANDARD.encode(sec_json.as_bytes())
        ).to_string();

        // Missing clientID
        let url = "http://localhost/sync/v51/connect?clientGroupID=cg1&ts=123&lmid=42";

        let result = get_connect_params(51, url, Some(&sec_encoded), None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_connect_params_optional_defaults() {
        let sec_json = r#"{"initConnectionMessage":null,"authToken":null}"#;
        let sec_encoded = urlencoding::encode(
            &base64::engine::general_purpose::STANDARD.encode(sec_json.as_bytes())
        ).to_string();

        // Only required params
        let url = "http://localhost/sync/v51/connect?clientID=c1&clientGroupID=cg1&ts=123&lmid=42";

        let params = get_connect_params(51, url, Some(&sec_encoded), None, None).unwrap();

        assert_eq!(params.ws_id, ""); // Default to empty
        assert_eq!(params.user_id, None);
        assert!(!params.debug_perf);
        assert_eq!(params.profile_id, None);
        assert_eq!(params.base_cookie, None);
    }

    #[test]
    fn test_get_connect_params_missing_sec_protocol() {
        let url = "http://localhost/sync/v51/connect?clientID=c1&clientGroupID=cg1&ts=123&lmid=42";
        let result = get_connect_params(51, url, None, None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_get_connect_params_invalid_integer() {
        let sec_json = r#"{"initConnectionMessage":null,"authToken":null}"#;
        let sec_encoded = urlencoding::encode(
            &base64::engine::general_purpose::STANDARD.encode(sec_json.as_bytes())
        ).to_string();

        // ts is not a number
        let url = "http://localhost/sync/v51/connect?clientID=c1&clientGroupID=cg1&ts=abc&lmid=42";
        let result = get_connect_params(51, url, Some(&sec_encoded), None, None);
        assert!(result.is_err());
    }
}
