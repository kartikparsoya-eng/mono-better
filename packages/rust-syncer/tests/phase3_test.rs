//! Phase 3 tests — port of connection-context-manager.test.ts.
//!
//! Tests the full connection context state machine:
//! - Provisional → Validated state transitions
//! - Group auth pinning (first validated userID binds the group)
//! - Background connection selection (sticky, newest fallback)
//! - Auth resolution (opaque token changes, JWT rotation)
//! - Maintenance planning (revalidation, retransform deadlines, defer)
//! - Stale revision handling
//! - Fetch context configuration (query/push URLs, headers, cookies)

use rust_syncer::connection_context::*;
use std::sync::{Arc, Mutex};

// ─── Helpers ───────────────────────────────────────────────────────────────

fn selector(client_id: &str, ws_id: &str) -> ConnectionSelector {
    ConnectionSelector {
        client_id: client_id.to_string(),
        ws_id: ws_id.to_string(),
    }
}

fn make_params(client_id: &str, ws_id: &str) -> ConnectParamsForRegistration {
    ConnectParamsForRegistration {
        client_id: client_id.to_string(),
        ws_id: ws_id.to_string(),
        user_id: Some(format!("user-{}", client_id)),
        profile_id: None,
        base_cookie: None,
        protocol_version: 0,
        http_cookie: Some(format!("cookie-{}", ws_id)),
        origin: Some(format!("origin-{}", ws_id)),
    }
}

fn make_params_with_user(
    client_id: &str,
    ws_id: &str,
    user_id: Option<&str>,
) -> ConnectParamsForRegistration {
    let mut p = make_params(client_id, ws_id);
    p.user_id = user_id.map(|s| s.to_string());
    p
}

fn register(
    manager: &mut ConnectionContextManager,
    client_id: &str,
    ws_id: &str,
) -> ConnectionContext {
    manager.register_connection(
        &selector(client_id, ws_id),
        &make_params(client_id, ws_id),
        Some(Auth::Opaque {
            raw: format!("token-{}", ws_id),
        }),
    )
}

fn register_with_user(
    manager: &mut ConnectionContextManager,
    client_id: &str,
    ws_id: &str,
    user_id: &str,
) -> ConnectionContext {
    manager.register_connection(
        &selector(client_id, ws_id),
        &make_params_with_user(client_id, ws_id, Some(user_id)),
        Some(Auth::Opaque {
            raw: format!("token-{}", ws_id),
        }),
    )
}

fn register_logged_out(
    manager: &mut ConnectionContextManager,
    client_id: &str,
    ws_id: &str,
) -> ConnectionContext {
    manager.register_connection(
        &selector(client_id, ws_id),
        &make_params_with_user(client_id, ws_id, None),
        None,
    )
}

fn init_connection(
    manager: &mut ConnectionContextManager,
    client_id: &str,
    ws_id: &str,
    body: InitConnectionBody,
) -> ConnectionContext {
    manager
        .init_connection(&selector(client_id, ws_id), &body)
        .unwrap()
}

fn validate(
    manager: &mut ConnectionContextManager,
    client_id: &str,
    ws_id: &str,
) -> Result<Option<ValidationResult>, CCMError> {
    let rev = manager
        .must_get_connection_context(&selector(client_id, ws_id))
        .unwrap()
        .revision;
    manager.validate_connection(
        &selector(client_id, ws_id),
        rev,
        &ConnectionValidation::ClientFallback,
    )
}

fn validate_with(
    manager: &mut ConnectionContextManager,
    client_id: &str,
    ws_id: &str,
    validation: ConnectionValidation,
) -> Result<Option<ValidationResult>, CCMError> {
    let rev = manager
        .must_get_connection_context(&selector(client_id, ws_id))
        .unwrap()
        .revision;
    manager.validate_connection(&selector(client_id, ws_id), rev, &validation)
}

fn validate_with_rev(
    manager: &mut ConnectionContextManager,
    client_id: &str,
    ws_id: &str,
    revision: u32,
) -> Option<ValidationResult> {
    manager
        .validate_connection(
            &selector(client_id, ws_id),
            revision,
            &ConnectionValidation::ClientFallback,
        )
        .unwrap()
}

fn make_now_fn() -> (Arc<Mutex<i64>>, Box<dyn Fn() -> i64 + Send + Sync>) {
    let now = Arc::new(Mutex::new(1000i64));
    let now_clone = now.clone();
    let f = Box::new(move || *now_clone.lock().unwrap());
    (now, f)
}

fn set_now(now: &Arc<Mutex<i64>>, val: i64) {
    *now.lock().unwrap() = val;
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[test]
fn test_register_provisional_apply_init_replace() {
    let mut manager = ConnectionContextManager::new(None, None, None, None, None, None);

    let conn = register(&mut manager, "c1", "ws1");
    assert_eq!(conn.client_id, "c1");
    assert_eq!(conn.ws_id, "ws1");
    assert_eq!(conn.revision, 0);
    assert_eq!(conn.state, ConnectionState::Provisional);
    assert_eq!(conn.user.id, Some("user-c1".to_string()));
    assert_eq!(conn.revalidate_at, None);
    assert_eq!(
        conn.query_context.header_options.origin,
        Some("origin-ws1".to_string())
    );
    assert_eq!(conn.query_context.header_options.cookie, None);

    // Apply init metadata
    let init = init_connection(
        &mut manager,
        "c1",
        "ws1",
        InitConnectionBody {
            user_query_url: Some("https://api.example/query".to_string()),
            user_query_headers: Some([("foo".to_string(), "bar".to_string())].into()),
            user_push_url: Some("https://api.example/push".to_string()),
            user_push_headers: Some([("baz".to_string(), "qux".to_string())].into()),
        },
    );
    assert_eq!(init.revision, 1);
    assert_eq!(init.state, ConnectionState::Provisional);
    assert_eq!(
        init.query_context.url,
        Some("https://api.example/query".to_string())
    );
    assert_eq!(
        init.query_context
            .header_options
            .custom_headers
            .as_ref()
            .unwrap()
            .get("foo"),
        Some(&"bar".to_string())
    );
    assert_eq!(
        init.mutate_context.url,
        Some("https://api.example/push".to_string())
    );
    assert_eq!(
        init.mutate_context
            .header_options
            .custom_headers
            .as_ref()
            .unwrap()
            .get("baz"),
        Some(&"qux".to_string())
    );

    // Replace with new ws
    let conn2 = register(&mut manager, "c1", "ws2");
    assert_eq!(conn2.ws_id, "ws2");
    assert_eq!(conn2.state, ConnectionState::Provisional);
    assert_eq!(conn2.query_context.url, None);

    // Old ws is gone
    assert!(
        manager
            .get_connection_context(&selector("c1", "ws1"))
            .is_none()
    );
    assert!(
        manager
            .get_connection_context(&selector("c1", "ws2"))
            .is_some()
    );
}

#[test]
fn test_binds_first_validated_user_id_from_client() {
    let mut manager = ConnectionContextManager::new(None, None, None, None, None, None);
    register_with_user(&mut manager, "c1", "ws1", "user-1");

    let result = validate(&mut manager, "c1", "ws1").unwrap().unwrap();
    assert_eq!(result.connection.state, ConnectionState::Validated);
    assert_eq!(result.connection.user.id, Some("user-1".to_string()));
    assert_eq!(result.connection.revalidate_at, None); // no revalidate interval configured

    let group = result.group;
    assert_eq!(
        group.pinned_user,
        Some(UserState {
            id: Some("user-1".to_string())
        })
    );
    assert_eq!(group.background_connection, Some(selector("c1", "ws1")));
}

#[test]
fn test_pins_logged_out_client_group_to_null_user_id() {
    let mut manager = ConnectionContextManager::new(None, None, None, None, None, None);
    register_logged_out(&mut manager, "c1", "ws1");
    register_with_user(&mut manager, "c2", "ws2", "user-2");

    let result = validate_with(
        &mut manager,
        "c1",
        "ws1",
        ConnectionValidation::ServerValidated {
            validated_user_id: None,
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(result.connection.state, ConnectionState::Validated);
    assert_eq!(result.connection.user.id, None);
    assert_eq!(result.group.pinned_user, Some(UserState { id: None }));

    // c2 with user-2 should be rejected
    let err = manager
        .validate_connection(
            &selector("c2", "ws2"),
            manager
                .must_get_connection_context(&selector("c2", "ws2"))
                .unwrap()
                .revision,
            &ConnectionValidation::ClientFallback,
        )
        .unwrap_err();
    assert!(matches!(err, CCMError::Unauthorized(_)));
}

#[test]
fn test_rejects_mismatched_validated_user_ids() {
    let mut manager = ConnectionContextManager::new(None, None, None, None, None, None);
    register_with_user(&mut manager, "c1", "ws1", "user-1");

    let err = validate_with(
        &mut manager,
        "c1",
        "ws1",
        ConnectionValidation::ServerValidated {
            validated_user_id: Some("user-2".to_string()),
        },
    )
    .unwrap_err();
    assert!(matches!(err, CCMError::Unauthorized(_)));

    // Connection should remain provisional
    let conn = manager
        .get_connection_context(&selector("c1", "ws1"))
        .unwrap();
    assert_eq!(conn.state, ConnectionState::Provisional);

    // Group should not be pinned
    assert_eq!(manager.get_group_state().pinned_user, None);
}

#[test]
fn test_rejects_mismatched_user_ids_and_keeps_provisional() {
    let mut manager = ConnectionContextManager::new(None, None, None, None, None, None);
    register_with_user(&mut manager, "c1", "ws1", "user-1");
    register_with_user(&mut manager, "c2", "ws2", "user-2");
    validate(&mut manager, "c1", "ws1").unwrap();

    let err = validate(&mut manager, "c2", "ws2").unwrap_err();
    assert!(matches!(err, CCMError::Unauthorized(_)));

    let conn = manager
        .get_connection_context(&selector("c2", "ws2"))
        .unwrap();
    assert_eq!(conn.state, ConnectionState::Provisional);
    assert_eq!(
        manager.get_group_state().pinned_user,
        Some(UserState {
            id: Some("user-1".to_string())
        })
    );
}

#[test]
fn test_keeps_user_id_pinned_after_all_connections_removed() {
    let mut manager = ConnectionContextManager::new(None, None, None, None, None, None);
    register_with_user(&mut manager, "c1", "ws1", "user-1");
    validate(&mut manager, "c1", "ws1").unwrap();

    manager.close_connection(&selector("c1", "ws1"));
    register_with_user(&mut manager, "c2", "ws2", "user-2");

    assert_eq!(
        manager.get_group_state().pinned_user,
        Some(UserState {
            id: Some("user-1".to_string())
        })
    );
    assert_eq!(manager.get_group_state().background_connection, None);

    let err = validate(&mut manager, "c2", "ws2").unwrap_err();
    assert!(matches!(err, CCMError::Unauthorized(_)));
}

#[test]
fn test_allows_multiple_validated_connections_matching_user_ids() {
    let mut manager = ConnectionContextManager::new(None, None, None, None, None, None);
    register_with_user(&mut manager, "c1", "ws1", "user-1");
    register_with_user(&mut manager, "c2", "ws2", "user-1");
    register_with_user(&mut manager, "c3", "ws3", "user-1");

    validate(&mut manager, "c1", "ws1").unwrap();
    validate(&mut manager, "c2", "ws2").unwrap();
    validate(&mut manager, "c3", "ws3").unwrap();

    assert_eq!(
        manager.get_group_state().pinned_user,
        Some(UserState {
            id: Some("user-1".to_string())
        })
    );
    for (cid, wid) in [("c1", "ws1"), ("c2", "ws2"), ("c3", "ws3")] {
        assert_eq!(
            manager
                .get_connection_context(&selector(cid, wid))
                .unwrap()
                .state,
            ConnectionState::Validated
        );
    }
}

#[test]
fn test_returned_contexts_stay_stable_across_updates() {
    let (_now, now_fn) = make_now_fn();
    let mut manager =
        ConnectionContextManager::new(Some(5), Some(10), None, None, None, Some(now_fn));

    let registered = register_with_user(&mut manager, "c1", "ws1", "user-1");
    assert_eq!(registered.revision, 0);
    assert_eq!(registered.state, ConnectionState::Provisional);
    assert_eq!(registered.query_context.url, None);

    let initialized = init_connection(
        &mut manager,
        "c1",
        "ws1",
        InitConnectionBody {
            user_query_url: Some("https://api.example/query".to_string()),
            user_query_headers: Some([("foo".to_string(), "bar".to_string())].into()),
            ..Default::default()
        },
    );
    assert_eq!(initialized.revision, 1);
    assert_eq!(
        initialized.query_context.url,
        Some("https://api.example/query".to_string())
    );

    let validated = validate(&mut manager, "c1", "ws1")
        .unwrap()
        .unwrap()
        .connection;
    assert_eq!(validated.revision, 1);
    assert_eq!(validated.state, ConnectionState::Validated);
    assert_eq!(validated.revalidate_at, Some(6000)); // now(1000) + 5s

    let background = manager.must_get_background_connection_context().unwrap();
    assert_eq!(background.revision, 1);
    assert_eq!(background.state, ConnectionState::Validated);

    let updated = manager
        .update_auth(
            &selector("c1", "ws1"),
            &UpdateAuthBody {
                auth: Some("token-ws1-next".to_string()),
            },
        )
        .unwrap();
    assert_eq!(updated.revision, 2);
    assert_eq!(updated.state, ConnectionState::Provisional);
    assert_eq!(updated.revalidate_at, None);
    assert_eq!(
        updated.auth,
        Some(Auth::Opaque {
            raw: "token-ws1-next".to_string()
        })
    );
    assert!(manager.get_background_connection_context().is_none());
}

#[test]
fn test_returned_group_contexts_stay_stable() {
    let (now, now_fn) = make_now_fn();
    let mut manager =
        ConnectionContextManager::new(Some(5), Some(2), None, None, None, Some(now_fn));

    register_with_user(&mut manager, "c1", "ws1", "user-1");
    register_with_user(&mut manager, "c2", "ws2", "user-1");
    validate(&mut manager, "c1", "ws1").unwrap();
    manager.set_shared_retransform_ready(true);

    let initial = manager.get_group_state().clone();
    assert_eq!(
        initial.pinned_user,
        Some(UserState {
            id: Some("user-1".to_string())
        })
    );
    assert_eq!(initial.background_connection, Some(selector("c1", "ws1")));
    assert_eq!(initial.retransform_at, Some(3000)); // now(1000) + 2s

    set_now(&now, 2000);
    let bg = manager.must_get_background_connection_context().unwrap();
    manager.mark_background_retransform_success(&selector(&bg.client_id, &bg.ws_id), bg.revision);
    assert_eq!(manager.get_group_state().retransform_at, Some(4000)); // now(2000) + 2s

    manager.defer_maintenance(MaintenanceKind::Revalidate);
    assert_eq!(
        manager.get_group_state().maintenance_not_before_at,
        Some(7000)
    ); // now(2000) + 5s

    validate(&mut manager, "c2", "ws2").unwrap();
    manager.close_connection(&selector("c1", "ws1"));

    let final_state = manager.get_group_state();
    assert_eq!(
        final_state.background_connection,
        Some(selector("c2", "ws2"))
    );
    assert_eq!(final_state.maintenance_not_before_at, Some(7000));
    assert_eq!(final_state.retransform_at, Some(4000));
}

#[test]
fn test_does_not_demote_when_auth_unchanged_by_value() {
    let (_now, now_fn) = make_now_fn();
    let mut manager =
        ConnectionContextManager::new(Some(5), Some(10), None, None, None, Some(now_fn));

    register_with_user(&mut manager, "c1", "ws1", "user-1");
    validate(&mut manager, "c1", "ws1").unwrap();
    manager.set_shared_retransform_ready(true);

    let prev_auth = manager
        .must_get_connection_context(&selector("c1", "ws1"))
        .unwrap()
        .auth
        .clone();

    let updated = manager
        .update_auth(
            &selector("c1", "ws1"),
            &UpdateAuthBody {
                auth: Some("token-ws1".to_string()),
            },
        )
        .unwrap();

    // Same token value → no demotion
    assert_eq!(updated.state, ConnectionState::Validated);
    assert_eq!(updated.revalidate_at, Some(6000));
    assert_eq!(updated.auth, prev_auth);
    assert_eq!(manager.get_group_state().retransform_at, Some(11000));

    let bg = manager.get_background_connection_context();
    assert!(bg.is_some());
    assert_eq!(bg.unwrap().client_id, "c1");
}

#[test]
fn test_demotes_only_connection_whose_auth_changes() {
    let (_now, now_fn) = make_now_fn();
    let mut manager =
        ConnectionContextManager::new(Some(5), Some(10), None, None, None, Some(now_fn));

    register_with_user(&mut manager, "c1", "ws1", "user-1");
    register_with_user(&mut manager, "c2", "ws2", "user-1");
    validate(&mut manager, "c1", "ws1").unwrap();
    validate(&mut manager, "c2", "ws2").unwrap();
    manager.set_shared_retransform_ready(true);

    let updated = manager
        .update_auth(
            &selector("c2", "ws2"),
            &UpdateAuthBody {
                auth: Some("token-ws2-new".to_string()),
            },
        )
        .unwrap();

    assert_eq!(updated.state, ConnectionState::Provisional);
    assert_eq!(updated.revalidate_at, None);
    assert_eq!(updated.revision, 1);

    // c1 should remain validated
    let c1 = manager
        .get_connection_context(&selector("c1", "ws1"))
        .unwrap();
    assert_eq!(c1.state, ConnectionState::Validated);
    assert_eq!(c1.revalidate_at, Some(6000));

    // Background should be c1
    let bg = manager.get_background_connection_context().unwrap();
    assert_eq!(bg.client_id, "c1");

    assert_eq!(manager.get_group_state().retransform_at, Some(11000));
}

#[test]
fn test_background_sticky_until_disappears_then_promotes_newest() {
    let mut manager = ConnectionContextManager::new(None, None, None, None, None, None);
    register_with_user(&mut manager, "c1", "ws1", "user-1");
    register_with_user(&mut manager, "c2", "ws2", "user-1");
    register_with_user(&mut manager, "c3", "ws3", "user-1");
    validate(&mut manager, "c1", "ws1").unwrap();
    validate(&mut manager, "c2", "ws2").unwrap();
    validate(&mut manager, "c3", "ws3").unwrap();

    // c1 is background (first validated)
    let bg = manager.get_background_connection_context().unwrap();
    assert_eq!(bg.client_id, "c1");

    // Close c1 → should promote c3 (newest by insertion order)
    manager.close_connection(&selector("c1", "ws1"));
    let bg = manager.get_background_connection_context().unwrap();
    assert_eq!(bg.client_id, "c3");
}

#[test]
fn test_stale_websocket_operations_are_no_ops() {
    let mut manager = ConnectionContextManager::new(None, None, None, None, None, None);
    register(&mut manager, "c1", "ws1");
    register(&mut manager, "c1", "ws2"); // replaces ws1

    // ws1 is gone — validate should return None
    assert!(validate_with_rev(&mut manager, "c1", "ws1", 0).is_none());

    // close should return None
    assert!(manager.close_connection(&selector("c1", "ws1")).is_none());

    // mustGet should error
    assert!(
        manager
            .must_get_connection_context(&selector("c1", "ws1"))
            .is_err()
    );

    // updateAuth should error
    assert!(
        manager
            .update_auth(&selector("c1", "ws1"), &UpdateAuthBody { auth: None })
            .is_err()
    );

    // ws2 is still there
    let conn = manager
        .get_connection_context(&selector("c1", "ws2"))
        .unwrap();
    assert_eq!(conn.state, ConnectionState::Provisional);
}

#[test]
fn test_stores_normalized_fetch_context() {
    let query_config = FetchConfig {
        url: Some(vec!["https://default.example/query".to_string()]),
        api_key: Some("query-api-key".to_string()),
        allowed_client_headers: Some(vec!["x-query-header".to_string()]),
        allowed_request_headers: None,
        forward_cookies: true,
    };
    let push_config = FetchConfig {
        url: Some(vec!["https://default.example/push".to_string()]),
        api_key: Some("push-api-key".to_string()),
        allowed_client_headers: Some(vec!["x-push-header".to_string()]),
        allowed_request_headers: None,
        forward_cookies: false,
    };

    let mut manager = ConnectionContextManager::new(
        None,
        None,
        Some(query_config),
        Some(push_config),
        None,
        None,
    );

    register_with_user(&mut manager, "c1", "ws1", "user-1");

    let init = init_connection(
        &mut manager,
        "c1",
        "ws1",
        InitConnectionBody {
            user_query_url: Some("https://user.example/query".to_string()),
            user_query_headers: Some(
                [("x-query-header".to_string(), "query-value".to_string())].into(),
            ),
            user_push_url: Some("https://user.example/push".to_string()),
            user_push_headers: Some(
                [("x-push-header".to_string(), "push-value".to_string())].into(),
            ),
        },
    );

    assert_eq!(init.revision, 1);
    assert_eq!(
        init.query_context.url,
        Some("https://user.example/query".to_string())
    );
    assert_eq!(
        init.query_context.header_options.api_key,
        Some("query-api-key".to_string())
    );
    assert_eq!(
        init.query_context
            .header_options
            .custom_headers
            .unwrap()
            .get("x-query-header"),
        Some(&"query-value".to_string())
    );
    assert_eq!(
        init.query_context.header_options.allowed_client_headers,
        Some(vec!["x-query-header".to_string()])
    );
    assert_eq!(
        init.query_context.header_options.cookie,
        Some("cookie-ws1".to_string())
    );
    assert_eq!(
        init.query_context.header_options.origin,
        Some("origin-ws1".to_string())
    );

    assert_eq!(
        init.mutate_context.url,
        Some("https://user.example/push".to_string())
    );
    assert_eq!(
        init.mutate_context.header_options.api_key,
        Some("push-api-key".to_string())
    );
    assert_eq!(
        init.mutate_context.header_options.cookie,
        None // forwardCookies = false
    );
}

#[test]
fn test_ignores_stale_revision_validation_and_failure() {
    let mut manager = ConnectionContextManager::new(None, None, None, None, None, None);
    let registered = register(&mut manager, "c1", "ws1");
    let revised = init_connection(
        &mut manager,
        "c1",
        "ws1",
        InitConnectionBody {
            user_query_url: Some("https://api.example/query".to_string()),
            ..Default::default()
        },
    );

    // Validate with old revision → None
    assert!(validate_with_rev(&mut manager, "c1", "ws1", registered.revision).is_none());

    // Fail with old revision → None
    assert!(
        manager
            .fail_connection(&selector("c1", "ws1"), registered.revision)
            .is_none()
    );

    // Connection should still be there with revised state
    let conn = manager
        .get_connection_context(&selector("c1", "ws1"))
        .unwrap();
    assert_eq!(conn.revision, revised.revision);
    assert_eq!(conn.state, ConnectionState::Provisional);
}

#[test]
fn test_plans_maintenance_with_revalidation_and_retransform() {
    let (now, now_fn) = make_now_fn();
    let mut manager =
        ConnectionContextManager::new(Some(5), Some(2), None, None, None, Some(now_fn));

    register_with_user(&mut manager, "c1", "ws1", "user-1");
    register_with_user(&mut manager, "c2", "ws2", "user-1");
    register_with_user(&mut manager, "c3", "ws3", "user-1");
    validate(&mut manager, "c2", "ws2").unwrap(); // insertion_order 2
    validate(&mut manager, "c1", "ws1").unwrap(); // insertion_order 1
    manager.set_shared_retransform_ready(true);

    // now=1000, revalidate_at=6000, retransform_at=3000
    let plan = manager.plan_maintenance();
    assert_eq!(plan.due_revalidations.len(), 0);
    assert!(!plan.due_retransform);
    assert_eq!(plan.earliest_deadline_at, Some(3000));

    set_now(&now, 3000);
    let plan = manager.plan_maintenance();
    assert!(plan.due_retransform);
    assert_eq!(plan.earliest_deadline_at, Some(3000));

    // Mark retransform success → resets to now(3000) + 2s = 5000
    let bg = manager.must_get_background_connection_context().unwrap();
    manager.mark_background_retransform_success(&selector(&bg.client_id, &bg.ws_id), bg.revision);
    let plan = manager.plan_maintenance();
    assert!(!plan.due_retransform);
    assert_eq!(plan.earliest_deadline_at, Some(5000));

    set_now(&now, 5000);
    let plan = manager.plan_maintenance();
    assert!(plan.due_retransform);
    assert_eq!(plan.earliest_deadline_at, Some(5000));

    // Mark retransform success again → now(5000) + 2s = 7000
    // But c1/c2.revalidate_at=6000, so earliest = min(7000, 6000) = 6000
    let bg = manager.must_get_background_connection_context().unwrap();
    manager.mark_background_retransform_success(&selector(&bg.client_id, &bg.ws_id), bg.revision);
    let plan = manager.plan_maintenance();
    assert_eq!(plan.earliest_deadline_at, Some(6000));

    set_now(&now, 6000);
    // Now revalidation is due (revalidate_at=6000, now=6000)
    let plan = manager.plan_maintenance();
    assert_eq!(plan.due_revalidations.len(), 2);
    // Sorted by insertion_order ascending: c1(1) before c2(2)
    assert_eq!(plan.due_revalidations[0].client_id, "c1");
    assert_eq!(plan.due_revalidations[1].client_id, "c2");
    assert!(!plan.due_retransform);
    assert_eq!(plan.earliest_deadline_at, Some(6000));

    // Re-validate to push revalidate_at forward
    validate(&mut manager, "c2", "ws2").unwrap();
    validate(&mut manager, "c1", "ws1").unwrap();

    let plan = manager.plan_maintenance();
    assert_eq!(plan.due_revalidations.len(), 0);
    // revalidate_at now 11000, retransform_at still 7000
    assert_eq!(plan.earliest_deadline_at, Some(7000));
}

#[test]
fn test_revalidation_does_not_reset_retransform_but_success_does() {
    let (now, now_fn) = make_now_fn();
    let mut manager =
        ConnectionContextManager::new(Some(5), Some(2), None, None, None, Some(now_fn));

    register_with_user(&mut manager, "c1", "ws1", "user-1");
    validate(&mut manager, "c1", "ws1").unwrap();
    manager.set_shared_retransform_ready(true);

    // revalidate_at=6000, retransform_at=3000
    assert_eq!(
        manager
            .get_connection_context(&selector("c1", "ws1"))
            .unwrap()
            .revalidate_at,
        Some(6000)
    );
    assert_eq!(manager.get_group_state().retransform_at, Some(3000));

    set_now(&now, 1500);
    validate(&mut manager, "c1", "ws1").unwrap();

    // revalidate_at=6500 (1500+5000), retransform_at unchanged (3000)
    assert_eq!(
        manager
            .get_connection_context(&selector("c1", "ws1"))
            .unwrap()
            .revalidate_at,
        Some(6500)
    );
    assert_eq!(manager.get_group_state().retransform_at, Some(3000));

    set_now(&now, 2000);
    let bg = manager.must_get_background_connection_context().unwrap();
    manager.mark_background_retransform_success(&selector(&bg.client_id, &bg.ws_id), bg.revision);

    // retransform_at=4000 (2000+2000)
    assert_eq!(manager.get_group_state().retransform_at, Some(4000));
}

#[test]
fn test_defers_all_scheduled_maintenance() {
    let (now, now_fn) = make_now_fn();
    let mut manager =
        ConnectionContextManager::new(Some(5), Some(2), None, None, None, Some(now_fn));

    register_with_user(&mut manager, "c1", "ws1", "user-1");
    register_with_user(&mut manager, "c2", "ws2", "user-1");
    validate(&mut manager, "c1", "ws1").unwrap();
    validate(&mut manager, "c2", "ws2").unwrap();
    manager.set_shared_retransform_ready(true);

    set_now(&now, 3000);
    manager.defer_maintenance(MaintenanceKind::Retransform);
    assert_eq!(
        manager.get_group_state().maintenance_not_before_at,
        Some(5000)
    );

    let plan = manager.plan_maintenance();
    assert_eq!(plan.due_revalidations.len(), 0);
    assert!(!plan.due_retransform);
    assert_eq!(plan.earliest_deadline_at, Some(5000));

    set_now(&now, 6000);
    manager.defer_maintenance(MaintenanceKind::Revalidate);
    assert_eq!(
        manager.get_group_state().maintenance_not_before_at,
        Some(11000)
    );

    let plan = manager.plan_maintenance();
    assert_eq!(plan.earliest_deadline_at, Some(11000));

    set_now(&now, 11000);
    let plan = manager.plan_maintenance();
    assert_eq!(plan.due_revalidations.len(), 2);
    assert!(plan.due_retransform);
    // earliestDeadlineAt is min of all deadlines = 3000 (original retransform)
    assert_eq!(plan.earliest_deadline_at, Some(3000));
}

#[test]
fn test_does_not_schedule_retransform_until_ready() {
    let (now, now_fn) = make_now_fn();
    let mut manager =
        ConnectionContextManager::new(Some(5), Some(2), None, None, None, Some(now_fn));

    register_with_user(&mut manager, "c1", "ws1", "user-1");
    validate(&mut manager, "c1", "ws1").unwrap();

    assert_eq!(manager.get_group_state().retransform_at, None);
    let plan = manager.plan_maintenance();
    assert_eq!(plan.earliest_deadline_at, Some(6000));

    manager.set_shared_retransform_ready(true);
    assert_eq!(manager.get_group_state().retransform_at, Some(3000));
    let plan = manager.plan_maintenance();
    assert_eq!(plan.earliest_deadline_at, Some(3000));

    set_now(&now, 3000);
    let plan = manager.plan_maintenance();
    assert!(plan.due_retransform);

    manager.set_shared_retransform_ready(false);
    assert_eq!(manager.get_group_state().retransform_at, None);
    let plan = manager.plan_maintenance();
    assert_eq!(plan.earliest_deadline_at, Some(6000));
}

#[test]
fn test_auth_equals() {
    let opaque1 = Auth::Opaque {
        raw: "token1".to_string(),
    };
    let opaque2 = Auth::Opaque {
        raw: "token1".to_string(),
    };
    let opaque3 = Auth::Opaque {
        raw: "token2".to_string(),
    };
    let jwt1 = Auth::Jwt {
        raw: "jwt1".to_string(),
        decoded: JwtPayload {
            sub: Some("u1".to_string()),
            iat: Some(1),
        },
    };
    let jwt2 = Auth::Jwt {
        raw: "jwt1".to_string(),
        decoded: JwtPayload {
            sub: Some("u1".to_string()),
            iat: Some(1),
        },
    };

    assert!(auth_equals(Some(&opaque1), Some(&opaque2))); // same value, different object
    assert!(!auth_equals(Some(&opaque1), Some(&opaque3))); // different raw
    assert!(!auth_equals(Some(&opaque1), Some(&jwt1))); // different type
    assert!(auth_equals(Some(&jwt1), Some(&jwt2))); // same JWT
    assert!(auth_equals(None, None));
    assert!(!auth_equals(Some(&opaque1), None));
    assert!(!auth_equals(None, Some(&opaque1)));
}

#[test]
fn test_resolve_auth_opaque_unchanged() {
    let prev = Auth::Opaque {
        raw: "token1".to_string(),
    };
    let result = resolve_auth(Some(&prev), Some("user-1"), Some("token1"), None).unwrap();
    assert_eq!(result, Some(prev));
}

#[test]
fn test_resolve_auth_opaque_changed() {
    let prev = Auth::Opaque {
        raw: "token1".to_string(),
    };
    let result = resolve_auth(Some(&prev), Some("user-1"), Some("token2"), None).unwrap();
    assert_eq!(
        result,
        Some(Auth::Opaque {
            raw: "token2".to_string()
        })
    );
}

#[test]
fn test_resolve_auth_no_previous() {
    let result = resolve_auth(None, Some("user-1"), Some("token1"), None).unwrap();
    assert_eq!(
        result,
        Some(Auth::Opaque {
            raw: "token1".to_string()
        })
    );
}

#[test]
fn test_resolve_auth_clearing_when_authenticated() {
    let prev = Auth::Opaque {
        raw: "token1".to_string(),
    };
    let err = resolve_auth(Some(&prev), Some("user-1"), None, None).unwrap_err();
    assert!(matches!(err, CCMError::Unauthorized(_)));
}

#[test]
fn test_resolve_auth_clearing_when_unauthenticated() {
    let result = resolve_auth(None, None, None, None).unwrap();
    assert_eq!(result, None);
}

#[test]
fn test_resolve_auth_empty_string_is_no_auth() {
    let result = resolve_auth(None, None, Some(""), None).unwrap();
    assert_eq!(result, None);
}

#[test]
fn test_resolve_auth_user_id_required_for_auth() {
    let err = resolve_auth(None, None, Some("token1"), None).unwrap_err();
    assert!(matches!(err, CCMError::Unauthorized(_)));
}
