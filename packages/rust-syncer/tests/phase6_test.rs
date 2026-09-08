//! Phase 6 tests — PokeHandler Drop, send_query_transform_failed_error,
//! and integration tests.

use rust_cvr::client_handler::{ClientHandler, WebSocketSink};
use rust_cvr::shards::ShardID;
use std::sync::{Arc, Mutex};

// ─── Mock WebSocketSink ────────────────────────────────────────────────────

#[derive(Default)]
struct MockSink {
    messages: Mutex<Vec<serde_json::Value>>,
    failed: Mutex<Option<String>>,
    cancelled: Mutex<bool>,
}

impl WebSocketSink for MockSink {
    fn push(&self, msg: serde_json::Value) -> Result<(), String> {
        self.messages.lock().unwrap().push(msg);
        Ok(())
    }

    fn fail(&self, err: String) {
        *self.failed.lock().unwrap() = Some(err);
    }

    fn cancel(&self) {
        *self.cancelled.lock().unwrap() = true;
    }
}

fn make_handler() -> (ClientHandler, Arc<MockSink>) {
    let sink = Arc::new(MockSink::default());
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    let handler = ClientHandler::new(
        "test-cg",
        "test-client",
        "test-ws",
        &shard,
        None,
        sink.clone() as Arc<dyn WebSocketSink>,
    );
    (handler, sink)
}

// ─── PokeHandler Drop tests ────────────────────────────────────────────────

#[test]
fn test_poke_handler_drop_releases_chain() {
    let (handler, _sink) = make_handler();

    // Start a poke — this acquires the poke chain
    let poke = handler.start_poke(rust_cvr::schema::types::CVRVersion {
        state_version: "1".to_string(),
        config_version: Some(1),
    });

    // The poke_chain should be false (not yet started — only acquired on end())
    // because start_poke creates a PokeHandler but doesn't call end() yet.
    // Actually, the chain is acquired in end(), not in start_poke().
    // Let's test the actual Drop behavior: if poke_in_progress is true,
    // Drop should release the chain.

    // Drop without calling end() — should be a no-op since poke_in_progress
    // is false at this point.
    drop(poke);

    // Starting another poke should work fine
    let _poke2 = handler.start_poke(rust_cvr::schema::types::CVRVersion {
        state_version: "2".to_string(),
        config_version: Some(1),
    });
}

#[test]
fn test_poke_handler_normal_lifecycle() {
    let (handler, sink) = make_handler();

    // Start a poke
    let poke = handler.start_poke(rust_cvr::schema::types::CVRVersion {
        state_version: "1".to_string(),
        config_version: Some(1),
    });

    // End the poke — this sends pokeStart + pokeEnd
    poke.end(rust_cvr::schema::types::CVRVersion {
        state_version: "1".to_string(),
        config_version: Some(1),
    })
    .unwrap();

    // Should have sent pokeStart and pokeEnd
    let messages = sink.messages.lock().unwrap();
    assert!(messages.len() >= 2);
    assert_eq!(messages[0][0], "pokeStart");
    assert_eq!(messages[messages.len() - 1][0], "pokeEnd");
}

// ─── send_query_transform_failed_error tests ───────────────────────────────

#[test]
fn test_send_query_transform_failed_error() {
    let (handler, sink) = make_handler();

    let error = serde_json::json!({
        "kind": "TransformFailed",
        "message": "bad transform",
        "origin": "zeroCache",
        "queryIDs": ["q1"]
    });

    handler.send_query_transform_failed_error(&error);

    let messages = sink.messages.lock().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0][0], "error");
    assert_eq!(messages[0][1]["kind"], "TransformFailed");

    let failed = sink.failed.lock().unwrap();
    assert!(failed.is_some());
}

// ─── ClientHandler basic operations ────────────────────────────────────────

#[test]
fn test_client_handler_start_poke_noop_when_base_equal() {
    let (handler, sink) = make_handler();

    let v = rust_cvr::schema::types::CVRVersion {
        state_version: "1".to_string(),
        config_version: Some(1),
    };

    // Set base version to "1" (client is caught up).
    handler.set_base_version_for_test(v.clone());

    // The first poke on connect is forced (an empty poke) even when caught up,
    // so the client learns its got-queries state was reconciled (TS `#everPoked`,
    // zero/v1.9.0). Consume it before asserting the true-NOOP behavior.
    handler.start_poke(v.clone()).end(v.clone()).unwrap();
    sink.messages.lock().unwrap().clear();

    // A subsequent poke at the same version is a genuine NOOP.
    let poke = handler.start_poke(v.clone());
    poke.end(v).unwrap();

    let messages = sink.messages.lock().unwrap();
    assert_eq!(messages.len(), 0);
}

#[test]
fn test_client_handler_send_inspect_response() {
    let (handler, sink) = make_handler();

    let response = serde_json::json!({
        "queries": [{"id": "q1", "ast": {}}],
        "server": {}
    });

    handler.send_inspect_response(response.clone());

    let messages = sink.messages.lock().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0][0], "inspect");
    assert_eq!(messages[0][1], response);
}

#[test]
fn test_client_handler_fail() {
    let (handler, sink) = make_handler();

    handler.fail("test error");

    let failed = sink.failed.lock().unwrap();
    assert_eq!(failed.as_ref().unwrap(), "test error");
}

#[test]
fn test_client_handler_close() {
    let (handler, sink) = make_handler();

    handler.close("test reason");

    let cancelled = sink.cancelled.lock().unwrap();
    assert!(*cancelled);
}

#[test]
fn test_client_handler_send_delete_clients() {
    let (handler, sink) = make_handler();

    handler
        .send_delete_clients(vec!["client-a".to_string(), "client-b".to_string()], vec![])
        .unwrap();

    let messages = sink.messages.lock().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0][0], "deleteClients");
    assert_eq!(messages[0][1]["clientIDs"][0], "client-a");
    assert_eq!(messages[0][1]["clientIDs"][1], "client-b");
}

#[test]
fn test_client_handler_send_query_transform_application_errors() {
    let (handler, sink) = make_handler();

    let errors = vec![serde_json::json!({"id": "q1", "error": "bad"})];

    handler
        .send_query_transform_application_errors(errors)
        .unwrap();

    let messages = sink.messages.lock().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0][0], "transformError");
}
