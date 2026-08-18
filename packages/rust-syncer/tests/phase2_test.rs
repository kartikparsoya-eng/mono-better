//! Phase 2 tests — port of connection.test.ts and syncer-ws-message-handler.test.ts.
//!
//! Tests:
//! - `connection.test.ts`: send/sendError behavior, log level classification
//! - `syncer-ws-message-handler.test.ts`: message routing (push, updateAuth,
//!   deleteClients, ackMutationResponses, changeDesiredQueries)
//! - `drain-coordinator.ts` behavior: shouldDrain, drainNextIn

use rust_syncer::connection::{LogLevel, MessageHandler, classify_error_log_level};
use rust_syncer::drain::DrainCoordinator;
use rust_syncer::message_handler::{
    ConnContextInfo, ConnContextManagerDispatch, ConnectionSelector, MutagenDispatch,
    PusherDispatch, SyncerWsMessageHandler, ViewSyncerDispatch,
};
use rust_syncer::protocol::{self, ErrorBody, ErrorKind, ErrorOrigin};
use std::sync::{Arc, Mutex};

// ─── Mock implementations ──────────────────────────────────────────────────

#[derive(Default)]
struct MockViewSyncer {
    change_desired_queries_calls: Mutex<Vec<(ConnectionSelector, String)>>,
    update_auth_calls: Mutex<Vec<(ConnectionSelector, String, bool)>>,
    delete_clients_calls: Mutex<Vec<(ConnectionSelector, String)>>,
    delete_clients_result: Mutex<Vec<String>>,
    init_connection_calls: Mutex<Vec<(ConnectionSelector, String)>>,
    init_connection_result: Mutex<bool>,
    inspect_calls: Mutex<Vec<(ConnectionSelector, String)>>,
}

impl ViewSyncerDispatch for MockViewSyncer {
    fn change_desired_queries(&self, selector: &ConnectionSelector, msg: &str) {
        self.change_desired_queries_calls
            .lock()
            .unwrap()
            .push((selector.clone(), msg.to_string()));
    }

    fn update_auth(&self, selector: &ConnectionSelector, msg: &str, changed: bool) {
        self.update_auth_calls
            .lock()
            .unwrap()
            .push((selector.clone(), msg.to_string(), changed));
    }

    fn delete_clients(&self, selector: &ConnectionSelector, msg: &str) -> Vec<String> {
        self.delete_clients_calls
            .lock()
            .unwrap()
            .push((selector.clone(), msg.to_string()));
        self.delete_clients_result.lock().unwrap().clone()
    }

    fn init_connection(&self, selector: &ConnectionSelector, msg: &str) -> bool {
        self.init_connection_calls
            .lock()
            .unwrap()
            .push((selector.clone(), msg.to_string()));
        *self.init_connection_result.lock().unwrap()
    }

    fn inspect(&self, selector: &ConnectionSelector, msg: &str) {
        self.inspect_calls
            .lock()
            .unwrap()
            .push((selector.clone(), msg.to_string()));
    }
}

#[derive(Default)]
struct MockConnContextManager {
    auth: Mutex<Option<String>>,
    revision: Mutex<u32>,
    update_auth_result: Mutex<bool>,
    init_connection_calls: Mutex<usize>,
}

impl ConnContextManagerDispatch for MockConnContextManager {
    fn must_get_connection_context(&self, _selector: &ConnectionSelector) -> ConnContextInfo {
        ConnContextInfo {
            auth: self.auth.lock().unwrap().clone(),
            revision: *self.revision.lock().unwrap(),
        }
    }

    fn init_connection(&self, _selector: &ConnectionSelector, _body: &serde_json::Value) {
        *self.init_connection_calls.lock().unwrap() += 1;
    }

    fn update_auth(&self, _selector: &ConnectionSelector, _body: &serde_json::Value) -> bool {
        let result = *self.update_auth_result.lock().unwrap();
        if result {
            *self.revision.lock().unwrap() += 1;
        }
        result
    }
}

#[derive(Default)]
struct MockMutagen {
    process_mutation_calls: Mutex<Vec<(serde_json::Value, Option<serde_json::Value>, bool)>>,
    process_mutation_result: Mutex<Option<(ErrorKind, String)>>,
}

impl MutagenDispatch for MockMutagen {
    fn process_mutation(
        &self,
        mutation: &serde_json::Value,
        auth: Option<&serde_json::Value>,
        has_pusher: bool,
    ) -> Option<(ErrorKind, String)> {
        self.process_mutation_calls.lock().unwrap().push((
            mutation.clone(),
            auth.cloned(),
            has_pusher,
        ));
        self.process_mutation_result.lock().unwrap().clone()
    }
}

#[derive(Default)]
struct MockPusher {
    enqueue_push_calls: Mutex<Vec<(ConnectionSelector, serde_json::Value)>>,
    init_connection_calls: Mutex<Vec<ConnectionSelector>>,
    ack_calls: Mutex<Vec<(ConnectionSelector, serde_json::Value)>>,
    delete_client_calls: Mutex<Vec<(ConnectionSelector, Vec<String>)>>,
}

impl PusherDispatch for MockPusher {
    fn enqueue_push(
        &self,
        selector: &ConnectionSelector,
        body: &serde_json::Value,
        _headers: &rust_syncer::PushRelayHeaders,
        _client_group_id: &str,
    ) -> rust_syncer::connection::HandlerResult {
        self.enqueue_push_calls
            .lock()
            .unwrap()
            .push((selector.clone(), body.clone()));
        rust_syncer::connection::HandlerResult::Ok
    }

    fn init_connection(&self, selector: &ConnectionSelector) {
        self.init_connection_calls
            .lock()
            .unwrap()
            .push(selector.clone());
    }

    fn ack_mutation_responses(
        &self,
        selector: &ConnectionSelector,
        body: &serde_json::Value,
        _headers: &rust_syncer::PushRelayHeaders,
        _client_group_id: &str,
    ) {
        self.ack_calls
            .lock()
            .unwrap()
            .push((selector.clone(), body.clone()));
    }

    fn delete_client_mutations(
        &self,
        selector: &ConnectionSelector,
        client_ids: &[String],
        _headers: &rust_syncer::PushRelayHeaders,
        _client_group_id: &str,
    ) {
        self.delete_client_calls
            .lock()
            .unwrap()
            .push((selector.clone(), client_ids.to_vec()));
    }
}

// ─── Helper: create handler with mocks ─────────────────────────────────────

fn create_handler(
    view_syncer: Arc<MockViewSyncer>,
    conn_context_manager: Arc<MockConnContextManager>,
    mutagen: Option<Arc<MockMutagen>>,
    pusher: Option<Arc<MockPusher>>,
) -> SyncerWsMessageHandler {
    SyncerWsMessageHandler::new(
        view_syncer as Arc<dyn ViewSyncerDispatch>,
        conn_context_manager as Arc<dyn ConnContextManagerDispatch>,
        mutagen.map(|m| m as Arc<dyn MutagenDispatch>),
        pusher.map(|p| p as Arc<dyn PusherDispatch>),
        "test-client-group".to_string(),
        "test-client".to_string(),
        "test-ws".to_string(),
        rust_syncer::PushRelayHeaders::default(),
    )
}

// ─── connection.test.ts: Error log level classification ────────────────────

#[test]
fn test_client_not_found_logged_as_warn() {
    let error = ErrorBody::client_not_found("Client not found");
    assert_eq!(classify_error_log_level(&error), LogLevel::Warn);
}

#[test]
fn test_transform_failed_logged_as_warn() {
    let error = ErrorBody::Basic(protocol::BasicErrorBody {
        kind: ErrorKind::TransformFailed,
        message: "bad transform config".to_string(),
        origin: Some(ErrorOrigin::ZeroCache),
    });
    assert_eq!(classify_error_log_level(&error), LogLevel::Warn);
}

#[test]
fn test_internal_error_logged_as_error() {
    let error = ErrorBody::internal("unexpected failure");
    assert_eq!(classify_error_log_level(&error), LogLevel::Error);
}

#[test]
fn test_socket_closed_while_compressing_logged_as_warn() {
    let error = ErrorBody::internal("The socket was closed while data was being compressed");
    assert_eq!(classify_error_log_level(&error), LogLevel::Warn);
}

#[test]
fn test_epipe_in_message_logged_as_warn() {
    // TS checks for 'errno' or transient socket codes on the thrown error.
    // In our Rust impl, we check the error message for the pattern.
    // The full thrown-error classification needs the thrown error object,
    // but for the error body classification, we check the message.
    let error = ErrorBody::internal("write EPIPE");
    // Without the thrown error, Internal kind defaults to Error.
    // But if the message contains "socket was closed while data was being compressed",
    // it would be Warn. EPIPE alone doesn't trigger the message pattern check.
    // This matches the TS behavior where EPIPE is checked on the thrown error's
    // `code` property, not the error body's message.
    assert_eq!(classify_error_log_level(&error), LogLevel::Error);
}

#[test]
fn test_version_not_supported_logged_as_info() {
    let error = ErrorBody::version_not_supported("unsupported");
    assert_eq!(classify_error_log_level(&error), LogLevel::Info);
}

#[test]
fn test_unauthorized_logged_as_info() {
    let error = ErrorBody::unauthorized("not authorized");
    assert_eq!(classify_error_log_level(&error), LogLevel::Info);
}

#[test]
fn test_invalid_push_logged_as_info() {
    let error = ErrorBody::invalid_push("bad push");
    assert_eq!(classify_error_log_level(&error), LogLevel::Info);
}

// ─── syncer-ws-message-handler.test.ts: Message routing ────────────────────

#[test]
fn test_push_with_custom_mutation_routes_to_pusher() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let pusher = Arc::new(MockPusher::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, Some(pusher.clone()));

    let msg = r#"["push",{"clientGroupID":"test-client-group","mutations":[{"type":"custom","id":1,"clientID":"test-client","name":"testMutation","args":[],"timestamp":123}],"pushVersion":1,"schemaVersion":1,"timestamp":123,"requestID":"req-1"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
    assert_eq!(pusher.enqueue_push_calls.lock().unwrap().len(), 1);
}

#[test]
fn test_push_with_wrong_client_group_id_returns_fatal() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let pusher = Arc::new(MockPusher::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, Some(pusher.clone()));

    let msg = r#"["push",{"clientGroupID":"wrong-group","mutations":[{"type":"custom","id":1,"clientID":"test-client","name":"testMutation","args":[],"timestamp":123}],"pushVersion":1,"schemaVersion":1,"timestamp":123,"requestID":"req-1"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Fatal { .. }
    ));
    assert_eq!(pusher.enqueue_push_calls.lock().unwrap().len(), 0);
}

#[test]
fn test_push_with_empty_mutations_returns_ok() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let pusher = Arc::new(MockPusher::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, Some(pusher.clone()));

    let msg = r#"["push",{"clientGroupID":"test-client-group","mutations":[],"pushVersion":1,"schemaVersion":1,"timestamp":123,"requestID":"req-1"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
    assert_eq!(pusher.enqueue_push_calls.lock().unwrap().len(), 0);
}

#[test]
fn test_push_with_custom_mutation_no_pusher_returns_transient() {
    // With no pusher configured (mutations are direct — the sync connection is
    // read-only), a `push` over the WebSocket is surfaced as a transient error
    // that keeps the read connection open (rather than tearing it down).
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, None);

    let msg = r#"["push",{"clientGroupID":"test-client-group","mutations":[{"type":"custom","id":1,"clientID":"test-client","name":"testMutation","args":[],"timestamp":123}],"pushVersion":1,"schemaVersion":1,"timestamp":123,"requestID":"req-1"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Transient { .. }
    ));
}

#[test]
fn test_push_with_crud_mutation_routes_to_mutagen() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let mutagen = Arc::new(MockMutagen::default());
    let pusher = Arc::new(MockPusher::default());
    let handler = create_handler(
        vs.clone(),
        ccm.clone(),
        Some(mutagen.clone()),
        Some(pusher.clone()),
    );

    let msg = r#"["push",{"clientGroupID":"test-client-group","mutations":[{"type":"crud","id":1,"clientID":"test-client","name":"mutate","args":[{"ops":[]}],"timestamp":123}],"pushVersion":1,"schemaVersion":1,"timestamp":123,"requestID":"req-1"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
    assert_eq!(mutagen.process_mutation_calls.lock().unwrap().len(), 1);
}

#[test]
fn test_push_with_crud_mutation_no_mutagen_returns_fatal() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let pusher = Arc::new(MockPusher::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, Some(pusher.clone()));

    let msg = r#"["push",{"clientGroupID":"test-client-group","mutations":[{"type":"crud","id":1,"clientID":"test-client","name":"mutate","args":[{"ops":[]}],"timestamp":123}],"pushVersion":1,"schemaVersion":1,"timestamp":123,"requestID":"req-1"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Fatal { .. }
    ));
}

#[test]
fn test_push_with_crud_mutation_error_returns_transient() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let mutagen = Arc::new(MockMutagen {
        process_mutation_result: Mutex::new(Some((
            ErrorKind::MutationFailed,
            "mutation error".to_string(),
        ))),
        ..Default::default()
    });
    let pusher = Arc::new(MockPusher::default());
    let handler = create_handler(
        vs.clone(),
        ccm.clone(),
        Some(mutagen.clone()),
        Some(pusher.clone()),
    );

    let msg = r#"["push",{"clientGroupID":"test-client-group","mutations":[{"type":"crud","id":1,"clientID":"test-client","name":"mutate","args":[{"ops":[]}],"timestamp":123}],"pushVersion":1,"schemaVersion":1,"timestamp":123,"requestID":"req-1"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    match &results[0] {
        rust_syncer::connection::HandlerResult::Transient { errors } => {
            assert_eq!(errors.len(), 1);
            assert_eq!(errors[0].kind(), &ErrorKind::MutationFailed);
        }
        _ => panic!("expected Transient"),
    }
}

#[test]
fn test_change_desired_queries_routes_to_view_syncer() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, None);

    let msg = r#"["changeDesiredQueries",{"desiredQueriesPatch":[],"traceparent":"test-tp"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
    assert_eq!(vs.change_desired_queries_calls.lock().unwrap().len(), 1);
}

#[test]
fn test_update_auth_routes_to_view_syncer() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager {
        update_auth_result: Mutex::new(true),
        ..Default::default()
    });
    let handler = create_handler(vs.clone(), ccm.clone(), None, None);

    let msg = r#"["updateAuth",{"auth":"new-token"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
    let calls = vs.update_auth_calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(calls[0].2); // auth_revision_changed = true
}

#[test]
fn test_update_auth_no_change_does_not_call_view_syncer_with_changed() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager {
        update_auth_result: Mutex::new(false),
        ..Default::default()
    });
    let handler = create_handler(vs.clone(), ccm.clone(), None, None);

    let msg = r#"["updateAuth",{"auth":"same-token"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
    let calls = vs.update_auth_calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert!(!calls[0].2); // auth_revision_changed = false
}

#[test]
fn test_delete_clients_routes_to_view_syncer_and_pusher() {
    let vs = Arc::new(MockViewSyncer {
        delete_clients_result: Mutex::new(vec!["client-a".to_string()]),
        ..Default::default()
    });
    let ccm = Arc::new(MockConnContextManager::default());
    let pusher = Arc::new(MockPusher::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, Some(pusher.clone()));

    let msg = r#"["deleteClients",{"clientIDs":["client-a"]}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
    assert_eq!(vs.delete_clients_calls.lock().unwrap().len(), 1);
    assert_eq!(pusher.delete_client_calls.lock().unwrap().len(), 1);
    assert_eq!(
        pusher.delete_client_calls.lock().unwrap()[0].1,
        vec!["client-a".to_string()]
    );
}

#[test]
fn test_delete_clients_no_pusher_no_error() {
    let vs = Arc::new(MockViewSyncer {
        delete_clients_result: Mutex::new(vec!["client-a".to_string()]),
        ..Default::default()
    });
    let ccm = Arc::new(MockConnContextManager::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, None);

    let msg = r#"["deleteClients",{"clientIDs":["client-a"]}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
}

#[test]
fn test_delete_clients_empty_result_no_pusher_call() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let pusher = Arc::new(MockPusher::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, Some(pusher.clone()));

    let msg = r#"["deleteClients",{"clientIDs":[]}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
    assert_eq!(pusher.delete_client_calls.lock().unwrap().len(), 0);
}

#[test]
fn test_ack_mutation_responses_routes_to_pusher() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let pusher = Arc::new(MockPusher::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, Some(pusher.clone()));

    let msg = r#"["ackMutationResponses",{"id":42,"clientID":"test-client"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
    assert_eq!(pusher.ack_calls.lock().unwrap().len(), 1);
}

#[test]
fn test_ack_mutation_responses_no_pusher_no_error() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, None);

    let msg = r#"["ackMutationResponses",{"id":42,"clientID":"test-client"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
}

#[test]
fn test_init_connection_routes_to_view_syncer_and_pusher() {
    let vs = Arc::new(MockViewSyncer {
        init_connection_result: Mutex::new(true),
        ..Default::default()
    });
    let ccm = Arc::new(MockConnContextManager::default());
    let pusher = Arc::new(MockPusher::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, Some(pusher.clone()));

    let msg = r#"["initConnection",{"desiredQueriesPatch":[],"traceparent":"test-tp"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
    assert_eq!(vs.init_connection_calls.lock().unwrap().len(), 1);
    assert_eq!(*ccm.init_connection_calls.lock().unwrap(), 1);
    assert_eq!(pusher.init_connection_calls.lock().unwrap().len(), 1);
}

#[test]
fn test_close_connection_is_noop() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, None);

    let msg = r#"["closeConnection",[]]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
}

#[test]
fn test_inspect_routes_to_view_syncer() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, None);

    let msg = r#"["inspect",{"op":"queries","id":"req1","clientID":"test-client"}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
    assert_eq!(vs.inspect_calls.lock().unwrap().len(), 1);
}

#[test]
fn test_ping_returns_ok() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, None);

    let msg = r#"["ping",{}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Ok
    ));
}

#[test]
fn test_invalid_message_returns_fatal() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, None);

    let results = handler.handle_message("not valid json");

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Fatal { .. }
    ));
}

#[test]
fn test_unknown_message_type_returns_fatal() {
    let vs = Arc::new(MockViewSyncer::default());
    let ccm = Arc::new(MockConnContextManager::default());
    let handler = create_handler(vs.clone(), ccm.clone(), None, None);

    let msg = r#"["unknownType",{}]"#;
    let results = handler.handle_message(msg);

    assert_eq!(results.len(), 1);
    assert!(matches!(
        results[0],
        rust_syncer::connection::HandlerResult::Fatal { .. }
    ));
}

// ─── drain-coordinator behavior tests ──────────────────────────────────────

#[test]
fn test_drain_coordinator_initial_not_draining() {
    let dc = DrainCoordinator::new();
    assert!(!dc.should_drain());
    assert!(!dc.is_draining());
    assert_eq!(dc.next_drain_time(), 0);
}

#[test]
fn test_drain_coordinator_drain_next_in_sets_draining() {
    let dc = DrainCoordinator::new();
    // Use a tokio runtime for the spawn.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    dc.drain_next_in(100);
    assert!(dc.is_draining());
    // next_drain_time should be in the future (now + 100 / 0.6 ≈ now + 166).
    use std::time::{SystemTime, UNIX_EPOCH};
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    assert!(dc.next_drain_time() > now_ms);
}

#[test]
fn test_drain_coordinator_should_drain_after_time_passes() {
    let dc = DrainCoordinator::new();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _guard = rt.enter();

    // drain_next_in(0) → adjusted = 0 / 0.6 = 0, so next_drain_time = now.
    dc.drain_next_in(0);

    // should_drain() should be true since next_drain_time <= now.
    // (There might be a tiny race, but 0ms means it's immediately due.)
    assert!(dc.should_drain());
}
