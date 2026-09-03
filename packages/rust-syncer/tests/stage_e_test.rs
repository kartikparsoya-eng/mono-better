//! Stage E — end-to-end data-path test.
//!
//! Builds a real SQLite table with rows, drives the full syncer hot path
//! (`SyncEngine::config_and_hydrate`: config-driven desired-query tracking +
//! query-driven hydrate against the real `TableSource`), and asserts that
//! actual ROW data — not just query patches — reaches the client sink as poke
//! frames. This is the first test exercising real engine rows through the
//! syncer, proving `IvmPipelines` → `rust-cvr` → poke end to end.
//!
//! Uses `init_from_connection` (hydrate needs only a plain SQLite connection —
//! the snapshotter/wal2 machinery is only required for `advance`).

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use rusqlite::Connection;
use rust_cvr::client_handler::WebSocketSink;
use rust_cvr::cvr::DesiredQuerySpec;
use rust_cvr::shards::ShardID;
use rust_syncer::services::view_syncer::pipeline_driver::IvmPipelines;
use rust_syncer::services::view_syncer::view_syncer::{
    CustomQueryTransformMode, ViewSyncerService as SyncEngine, empty_cvr,
};
use rust_syncer::ws_sink::{DirectWebSocketSink, WsCommand};

#[test]
fn hydrate_real_rows_produces_row_pokes() {
    // A real SQLite table with two rows.
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "issue" (
            "id"    "text|NOT_NULL",
            "title" "text",
            "_0_version" "text",
            PRIMARY KEY ("id")
        );
        INSERT INTO "issue" ("id", "title", "_0_version") VALUES
            ('i1', 'first issue', '01'),
            ('i2', 'second issue', '01');
        "#,
    )
    .unwrap();

    // Derive the table specs from the real replica schema (Part 1). This
    // includes `_0_version`, which the ChangeProcessor reads as the row version.
    let specs = rust_syncer::compute_zql_specs(&conn, None).unwrap();
    let shared_conn: SharedConnAlias = Rc::new(RefCell::new(conn));

    // Build the pipelines from that connection (no snapshotter needed to hydrate).
    let mut pipelines = IvmPipelines::new();
    pipelines.init_from_connection(specs, shared_conn).unwrap();

    let mut engine = SyncEngine::new(pipelines);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

    // A fresh CVR + a desired query for the whole `issue` table.
    let cvr = empty_cvr("cg1", "01");
    let puts = vec![DesiredQuerySpec {
        hash: "q_issue".to_string(),
        ast: Some(serde_json::json!({"table": "issue"})),
        name: None,
        args: None,
        ttl: None,
    }];

    // ANYONE_CAN: client AST queries are always transformed (permissions:None
    // = empty config = deny-all per TS view-syncer.ts:1549 `?? {tables: {}}`).
    let anyone_can = serde_json::json!({
        "tables": {"issue": {"row": {"select": [["allow", {"type": "and", "conditions": []}]]}}}
    });
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result_cvr = rt
        .block_on(engine.config_and_hydrate(
            cvr,
            "client1",
            &["ws1".to_string()],
            &shard,
            puts,
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            Some(&anyone_can),
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "01".to_string(),
            0,
            0,
            0,
        ))
        .unwrap();

    // The row-set-signature provider (task 13) persisted a signature for the
    // hydrated query — the engine XOR-accumulates a per-row unit as `q_issue`
    // hydrates its two rows, and the updater's flush wrote it into the CVR.
    let sig = result_cvr
        .queries
        .get("q_issue")
        .and_then(|q| q.base().row_set_signature.clone());
    assert!(
        sig.is_some(),
        "expected a persisted row_set_signature for the hydrated query"
    );

    // Collect all poke frames and assert the two real rows arrived as row
    // patches inside a pokePart.
    let mut saw_i1 = false;
    let mut saw_i2 = false;
    let mut saw_poke_part = false;
    let mut frames = 0;
    while let Ok(WsCommand::Send { msg: v, .. }) = rx.try_recv() {
        frames += 1;
        if v[0] == "pokePart" {
            saw_poke_part = true;
            let s = serde_json::to_string(&v).unwrap();
            if s.contains("\"i1\"") && s.contains("first issue") {
                saw_i1 = true;
            }
            if s.contains("\"i2\"") && s.contains("second issue") {
                saw_i2 = true;
            }
        }
    }

    assert!(frames > 0, "expected poke frames");
    assert!(saw_poke_part, "expected a pokePart with row data");
    assert!(saw_i1, "expected row i1 in a poke");
    assert!(saw_i2, "expected row i2 in a poke");
}

/// The internal `lmids` query (created by `ensure_client`) hydrates a real
/// `{app}_{shard}.clients` row and its `lastMutationID` reaches the client as a
/// `lastMutationIDChanges` poke — the mechanism by which a client learns its
/// mutations have been applied. This exercises the full-desired-set pipeline
/// sync (internal queries are hydrated from the CVR, not just client puts).
#[test]
fn lmids_internal_query_produces_last_mutation_id_changes() {
    // The upstream schema for shard app/0 is `app_0`; the internal `lmids`
    // query reads `app_0.clients` filtered by `clientGroupID = <cvr.id>`.
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "app_0.clients" (
            "clientGroupID"  "text|NOT_NULL",
            "clientID"       "text|NOT_NULL",
            "lastMutationID" "int8|NOT_NULL",
            "userID"         "text",
            "_0_version"     "text",
            PRIMARY KEY ("clientGroupID", "clientID")
        );
        CREATE TABLE "app_0.mutations" (
            "clientGroupID"  "text|NOT_NULL",
            "clientID"       "text|NOT_NULL",
            "mutationID"     "int8|NOT_NULL",
            "result"         "json|NOT_NULL",
            "_0_version"     "text",
            PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
        );
        INSERT INTO "app_0.clients"
            ("clientGroupID", "clientID", "lastMutationID", "userID", "_0_version")
            VALUES ('cg1', 'client1', 5, NULL, '01');
        "#,
    )
    .unwrap();

    let specs = rust_syncer::compute_zql_specs(&conn, None).unwrap();
    let shared_conn: SharedConnAlias = Rc::new(RefCell::new(conn));

    let mut pipelines = IvmPipelines::new();
    pipelines.init_from_connection(specs, shared_conn).unwrap();

    let mut engine = SyncEngine::new(pipelines);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

    // No client queries at all — only the internal `lmids` / `mutationResults`
    // queries that `ensure_client` creates. They must still hydrate.
    let cvr = empty_cvr("cg1", "01");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result_cvr = rt
        .block_on(engine.config_and_hydrate(
            cvr,
            "client1",
            &["ws1".to_string()],
            &shard,
            Vec::new(),
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "01".to_string(),
            0,
            0,
            0,
        ))
        .unwrap();

    // The internal queries exist in the CVR but produce NO got-query patch.
    assert!(result_cvr.queries.contains_key("lmids"));
    assert!(result_cvr.queries.contains_key("mutationResults"));

    // The client received a poke carrying lastMutationIDChanges.client1 == 5.
    let mut saw_lmid = false;
    while let Ok(WsCommand::Send { msg: v, .. }) = rx.try_recv() {
        if v[0] == "pokePart" {
            let changes = &v[1]["lastMutationIDChanges"];
            if changes.get("client1").and_then(|n| n.as_i64()) == Some(5) {
                saw_lmid = true;
            }
        }
    }
    assert!(
        saw_lmid,
        "expected lastMutationIDChanges.client1 == 5 in a pokePart"
    );
}

/// Initial hydration with rows in MULTIPLE queries (two queries over two tables):
/// each query hydrates its own rows and BOTH sets reach the client in the same
/// version-ready poke, with the CVR carrying a persisted row-set signature per
/// query. Ports view-syncer.pg.test.ts "initial hydration, rows in multiple
/// queries" to the in-memory harness.
#[test]
fn hydrate_multiple_queries_pokes_rows_from_each() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "issue" (
            "id"    "text|NOT_NULL",
            "title" "text",
            "_0_version" "text",
            PRIMARY KEY ("id")
        );
        CREATE TABLE "label" (
            "id"   "text|NOT_NULL",
            "name" "text",
            "_0_version" "text",
            PRIMARY KEY ("id")
        );
        INSERT INTO "issue" ("id", "title", "_0_version") VALUES
            ('i1', 'first issue', '01'),
            ('i2', 'second issue', '01');
        INSERT INTO "label" ("id", "name", "_0_version") VALUES
            ('l1', 'bug', '01');
        "#,
    )
    .unwrap();

    let specs = rust_syncer::compute_zql_specs(&conn, None).unwrap();
    let shared_conn: SharedConnAlias = Rc::new(RefCell::new(conn));

    let mut pipelines = IvmPipelines::new();
    pipelines.init_from_connection(specs, shared_conn).unwrap();

    let mut engine = SyncEngine::new(pipelines);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

    // Two desired queries over two different tables.
    let cvr = empty_cvr("cg1", "01");
    let puts = vec![
        DesiredQuerySpec {
            hash: "q_issue".to_string(),
            ast: Some(serde_json::json!({"table": "issue"})),
            name: None,
            args: None,
            ttl: None,
        },
        DesiredQuerySpec {
            hash: "q_label".to_string(),
            ast: Some(serde_json::json!({"table": "label"})),
            name: None,
            args: None,
            ttl: None,
        },
    ];

    // ANYONE_CAN for both queried tables (see hydrate_real_rows comment).
    let anyone_can = serde_json::json!({
        "tables": {
            "issue": {"row": {"select": [["allow", {"type": "and", "conditions": []}]]}},
            "label": {"row": {"select": [["allow", {"type": "and", "conditions": []}]]}}
        }
    });
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result_cvr = rt
        .block_on(engine.config_and_hydrate(
            cvr,
            "client1",
            &["ws1".to_string()],
            &shard,
            puts,
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            Some(&anyone_can),
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "01".to_string(),
            0,
            0,
            0,
        ))
        .unwrap();

    // Both queries hydrated and each persisted a row-set signature.
    for q in ["q_issue", "q_label"] {
        assert!(
            result_cvr
                .queries
                .get(q)
                .and_then(|r| r.base().row_set_signature.clone())
                .is_some(),
            "expected a persisted row_set_signature for {q}"
        );
    }

    // Rows from BOTH queries reached the client as poke row patches.
    let (mut saw_i1, mut saw_i2, mut saw_l1) = (false, false, false);
    while let Ok(WsCommand::Send { msg: v, .. }) = rx.try_recv() {
        if v[0] == "pokePart" {
            let s = serde_json::to_string(&v).unwrap();
            if s.contains("first issue") {
                saw_i1 = true;
            }
            if s.contains("second issue") {
                saw_i2 = true;
            }
            if s.contains("\"l1\"") && s.contains("bug") {
                saw_l1 = true;
            }
        }
    }
    assert!(saw_i1 && saw_i2, "expected both issue rows in pokes");
    assert!(saw_l1, "expected the label row in pokes");
}

/// Initial hydration of a CUSTOM (name-based) query: the query is resolved via
/// the transform path (seeded in the cache to avoid a network call), then
/// hydrates real rows that reach the client as poke row patches. Exercises the
/// whole custom-query pipeline end-to-end (custom_specs → transform → executed →
/// hydrate → poke), which unit tests only cover piecewise. Ports
/// view-syncer.pg.test.ts "initial hydration of a custom query".
#[test]
fn hydrate_custom_query_resolves_via_transform_and_pokes_rows() {
    use rust_syncer::custom_queries::transform_query::{CustomQueryContext, TransformedQuery};

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "issue" (
            "id"    "text|NOT_NULL",
            "title" "text",
            "_0_version" "text",
            PRIMARY KEY ("id")
        );
        INSERT INTO "issue" ("id", "title", "_0_version") VALUES
            ('i1', 'custom-hydrated issue', '01');
        "#,
    )
    .unwrap();

    let specs = rust_syncer::compute_zql_specs(&conn, None).unwrap();
    let shared_conn: SharedConnAlias = Rc::new(RefCell::new(conn));
    let mut pipelines = IvmPipelines::new();
    pipelines.init_from_connection(specs, shared_conn).unwrap();

    let mut engine = SyncEngine::new(pipelines);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);

    // The connection's custom-query context (bogus URL — the cache seed proves no
    // network call happens) and the seeded transform result for our query id.
    let url = "http://127.0.0.1:1/custom-hydrate-test";
    let ctx = CustomQueryContext {
        url: url.to_string(),
        allowed_urls: vec![url.to_string()],
        ..CustomQueryContext::default()
    };
    rust_syncer::custom_queries::transform_query::seed_transform_cache_for_test(
        &ctx,
        "custom_q",
        &TransformedQuery {
            id: "custom_q".to_string(),
            ast: serde_json::json!({"table": "issue"}),
            hash: "thash1".to_string(),
        },
    );

    // A desired CUSTOM query (name + args, no inline AST) → resolved via transform.
    let cvr = empty_cvr("cg1", "01");
    let puts = vec![DesiredQuerySpec {
        hash: "custom_q".to_string(),
        ast: None,
        name: Some("myIssues".to_string()),
        args: Some(vec![]),
        ttl: None,
    }];

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result_cvr = rt
        .block_on(engine.config_and_hydrate(
            cvr,
            "client1",
            &["ws1".to_string()],
            &shard,
            puts,
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            Some(&ctx),
            "00".to_string(),
            "01".to_string(),
            0,
            0,
            0,
        ))
        .unwrap();

    // The custom query is now tracked in the CVR and hydrated its row.
    assert!(
        result_cvr.queries.contains_key("custom_q"),
        "custom query should be recorded in the CVR"
    );

    let mut saw_row = false;
    while let Ok(WsCommand::Send { msg: v, .. }) = rx.try_recv() {
        if v[0] == "pokePart" {
            let s = serde_json::to_string(&v).unwrap();
            if s.contains("\"i1\"") && s.contains("custom-hydrated issue") {
                saw_row = true;
            }
        }
    }
    assert!(
        saw_row,
        "expected the custom query's hydrated row in a pokePart"
    );
}

/// Partial-success custom-query transform: the API server returns one healthy
/// query and one errored query in the same batch. The healthy query must still
/// hydrate its rows (the sibling's error does not take the batch down). Ports
/// view-syncer.pg.test.ts "some individual queries fail". Uses a minimal
/// one-shot TCP HTTP mock (no network dependency) as the transform endpoint.
#[test]
fn partial_success_transform_hydrates_healthy_query() {
    // One-shot HTTP mock returning `{queries:[{ok, ast}, {err, error}]}`.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf); // drain request (we don't parse it)
            let body = r#"{"queries":[{"id":"custom_ok","ast":{"table":"issue"}},{"id":"custom_err","error":{"message":"boom"}}]}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        }
    });

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "issue" (
            "id"    "text|NOT_NULL",
            "title" "text",
            "_0_version" "text",
            PRIMARY KEY ("id")
        );
        INSERT INTO "issue" ("id", "title", "_0_version") VALUES
            ('i1', 'healthy issue', '01');
        "#,
    )
    .unwrap();

    let specs = rust_syncer::compute_zql_specs(&conn, None).unwrap();
    let shared_conn: SharedConnAlias = Rc::new(RefCell::new(conn));
    let mut pipelines = IvmPipelines::new();
    pipelines.init_from_connection(specs, shared_conn).unwrap();

    let mut engine = SyncEngine::new(pipelines);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);
    // A sibling socket of the SAME client group that never desired the erroring
    // query. TS routes `transformError` by the query's CVR clientState
    // (`#sendQueryTransformErrorToClients` → `getAffectedClientIDs`,
    // view-syncer.ts:1728), so the sibling is poked (got-del is group-wide)
    // but must NOT receive the error frame (frame-capture #4, 2026-09-03).
    let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink2: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx2));
    engine.register_client("client2", "ws2", "cg1", &shard, None, sink2);

    let ctx = rust_syncer::custom_queries::transform_query::CustomQueryContext {
        url: format!("http://{addr}/transform"),
        allowed_urls: vec![format!("http://{addr}/transform")],
        ..rust_syncer::custom_queries::transform_query::CustomQueryContext::default()
    };

    // Two custom queries — both uncached, so they batch into the one mock request.
    let cvr = empty_cvr("cg1", "01");
    let mk = |hash: &str, name: &str| DesiredQuerySpec {
        hash: hash.to_string(),
        ast: None,
        name: Some(name.to_string()),
        args: Some(vec![]),
        ttl: None,
    };
    let puts = vec![mk("custom_ok", "healthy"), mk("custom_err", "broken")];

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result_cvr = rt
        .block_on(engine.config_and_hydrate(
            cvr,
            "client1",
            &["ws1".to_string(), "ws2".to_string()],
            &shard,
            puts,
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            None,
            &serde_json::json!({}),
            Some(&ctx),
            "00".to_string(),
            "01".to_string(),
            0,
            0,
            0,
        ))
        .unwrap();

    // The healthy query is tracked and hydrated; the errored one did not take the
    // batch down.
    assert!(
        result_cvr.queries.contains_key("custom_ok"),
        "healthy custom query should be recorded"
    );

    // TS removes the errored query from the CVR in the same pass
    // (`removeQueriesQueryIds` = expired ∪ erroredQueryIDs, view-syncer.ts:2062-2067
    // → `#trackRemoved` deletes it, :742-757).
    assert!(
        !result_cvr.queries.contains_key("custom_err"),
        "the errored custom query must be removed from the CVR (TS #trackRemoved)"
    );
    let mut frames: Vec<serde_json::Value> = Vec::new();
    while let Ok(WsCommand::Send { msg: v, .. }) = rx.try_recv() {
        frames.push(v);
    }
    let mut sibling: Vec<serde_json::Value> = Vec::new();
    while let Ok(WsCommand::Send { msg: v, .. }) = rx2.try_recv() {
        sibling.push(v);
    }
    assert!(
        !sibling.iter().any(|v| v[0] == "transformError"),
        "TS #sendQueryTransformErrorToClients delivers the transformError only to clients in the \
         query's clientState; the sibling never desired custom_err. sibling frames={sibling:?}"
    );
    assert_eq!(
        frames.iter().filter(|v| v[0] == "transformError").count(),
        1,
        "the desiring client gets exactly one transformError frame"
    );
    let saw_healthy_row = frames.iter().any(|v| {
        v[0] == "pokePart" && {
            let s = serde_json::to_string(v).unwrap();
            s.contains("\"i1\"") && s.contains("healthy issue")
        }
    });
    assert!(
        saw_healthy_row,
        "the healthy query's row must still hydrate despite the sibling's transform error"
    );
    // The client is told the query errored (`transformError`) and then, in the
    // sync poke, that the server dropped it (got-`del`) — the exact frame
    // sequence TS emits (captured per client on the xyne ART sandbox:
    // transformError → pokeStart → pokePart{gotQueriesPatch:[del]} → pokeEnd).
    // Before this port rust sent the transformError and no del, so the client
    // kept the query as pending/got forever.
    let err_idx = frames
        .iter()
        .position(|v| v[0] == "transformError")
        .expect("a transformError frame for custom_err");
    let del_idx = frames.iter().position(|v| {
        v[0] == "pokePart"
            && v[1]
                .get("gotQueriesPatch")
                .and_then(|g| g.as_array())
                .is_some_and(|g| {
                    g.iter()
                        .any(|e| e["op"] == "del" && e["hash"] == "custom_err")
                })
    });
    let del_idx = del_idx.unwrap_or_else(|| {
        panic!("expected a gotQueriesPatch del for custom_err after the transformError; frames={frames:?}")
    });
    assert!(
        del_idx > err_idx,
        "TS sends the transformError before the poke carrying the got-del"
    );
    let got_puts_for_err = frames
        .iter()
        .filter(|v| v[0] == "pokePart")
        .filter_map(|v| {
            v[1].get("gotQueriesPatch")
                .and_then(|g| g.as_array())
                .cloned()
        })
        .flatten()
        .filter(|e| e["op"] == "put" && e["hash"] == "custom_err")
        .count();
    assert_eq!(
        got_puts_for_err, 0,
        "an errored query is never reported as got"
    );
}

/// A transform failure during one connection's config pass fails ONLY that
/// connection — a sibling connection is untouched. Ports view-syncer.pg.test.ts
/// "transform ... fails only that connection" (#20/#21): the whole-batch failed
/// error is delivered to `get_clients(poke_ws_ids)`, which is just the failing
/// client. Uses a one-shot TCP mock returning HTTP 401.
#[test]
fn transform_failure_fails_only_the_offending_connection() {
    // Mock transform endpoint that returns 401 for the (single) request.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        use std::io::{Read, Write};
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 8192];
            let _ = stream.read(&mut buf);
            let body = r#"{"kind":"Unauthorized","message":"nope"}"#;
            let resp = format!(
                "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        }
    });

    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "issue" (
            "id" "text|NOT_NULL", "title" "text", "_0_version" "text",
            PRIMARY KEY ("id")
        );
        INSERT INTO "issue" ("id","title","_0_version") VALUES ('i1','x','01');
        "#,
    )
    .unwrap();
    let specs = rust_syncer::compute_zql_specs(&conn, None).unwrap();
    let shared_conn: SharedConnAlias = Rc::new(RefCell::new(conn));
    let mut pipelines = IvmPipelines::new();
    pipelines.init_from_connection(specs, shared_conn).unwrap();
    let mut engine = SyncEngine::new(pipelines);

    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    // Two clients, each with its own sink.
    let (tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let (tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    engine.register_client(
        "clientA",
        "wsA",
        "cg1",
        &shard,
        None,
        Arc::new(DirectWebSocketSink::new(tx_a)),
    );
    engine.register_client(
        "clientB",
        "wsB",
        "cg1",
        &shard,
        None,
        Arc::new(DirectWebSocketSink::new(tx_b)),
    );

    let ctx = rust_syncer::custom_queries::transform_query::CustomQueryContext {
        url: format!("http://{addr}/transform"),
        allowed_urls: vec![format!("http://{addr}/transform")],
        ..rust_syncer::custom_queries::transform_query::CustomQueryContext::default()
    };
    // Only client A's config pass runs (poke_ws_ids = [wsA]); its transform fails.
    let cvr = empty_cvr("cg1", "01");
    let puts = vec![DesiredQuerySpec {
        hash: "cq".to_string(),
        ast: None,
        name: Some("q".to_string()),
        args: Some(vec![]),
        ttl: None,
    }];
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(engine.config_and_hydrate(
        cvr,
        "clientA",
        &["wsA".to_string()],
        &shard,
        puts,
        Vec::new(),
        false,
        None,
        CustomQueryTransformMode::All,
        None,
        &serde_json::json!({}),
        Some(&ctx),
        "00".to_string(),
        "01".to_string(),
        0,
        0,
        0,
    ))
    .unwrap();

    // Client A received an error frame (its connection was failed).
    let mut a_got_error = false;
    while let Ok(WsCommand::Send { msg: v, .. }) = rx_a.try_recv() {
        if v[0] == "error" {
            a_got_error = true;
        }
    }
    assert!(a_got_error, "the offending connection (A) must be failed");

    // Client B received NOTHING — the failure did not leak to the sibling.
    assert!(
        rx_b.try_recv().is_err(),
        "a sibling connection (B) must be untouched by A's transform failure"
    );
}

// `SharedConn` is `Rc<RefCell<rusqlite::Connection>>`; alias locally for clarity.
type SharedConnAlias = Rc<RefCell<Connection>>;

mod common;

/// PG-gated (`TEST_CVR_PG_URI`): the catch-up scan that closes a hydrate pass
/// must not replay the got-`put` this very pass just tracked. TS
/// `#catchupClients(lc, cvr, finalVersion, addQueries ids, pokers)`
/// (view-syncer.ts:2350-2356) bounds the config-patch scan at the PRE-hydrate
/// CVR version; bounding it at the final version made every hydrate poke carry
/// the new query's got-`put` TWICE (every pass of the xyne ART frame capture;
/// TS once). Needs the real store: without PG the catch-up scan is empty and
/// the duplicate cannot appear, which is why the store-less tests never saw it.
#[test]
fn pg_catchup_after_hydrate_does_not_replay_the_got_put_just_poked() {
    let Some(uri) = common::pg_uri() else {
        eprintln!(
            "SKIP pg_catchup_after_hydrate_does_not_replay_the_got_put_just_poked: TEST_CVR_PG_URI not set"
        );
        return;
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let schema = "stage_e_catchup_window";
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&uri)
            .await
            .expect("connect TEST_CVR_PG_URI");
        sqlx::raw_sql(&common::cvr_ddl(schema))
            .execute(&pool)
            .await
            .expect("create CVR schema");

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE "issue" (
                "id"    "text|NOT_NULL",
                "title" "text",
                "_0_version" "text",
                PRIMARY KEY ("id")
            );
            INSERT INTO "issue" ("id", "title", "_0_version") VALUES
                ('i1', 'first issue', '01'),
                ('i2', 'second issue', '01');
            "#,
        )
        .unwrap();
        let specs = rust_syncer::compute_zql_specs(&conn, None).unwrap();
        let shared_conn: SharedConnAlias = Rc::new(RefCell::new(conn));
        let mut pipelines = IvmPipelines::new();
        pipelines.init_from_connection(specs, shared_conn).unwrap();
        let mut engine = SyncEngine::new(pipelines);
        engine
            .set_cvr_store(
                pool.clone(),
                schema.to_string(),
                "cg1".to_string(),
                "task-0".to_string(),
            )
            .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
        let shard = ShardID {
            app_id: "app".to_string(),
            shard_num: 0,
        };
        engine.register_client("client1", "ws1", "cg1", &shard, None, sink);
        let anyone_can = serde_json::json!({
            "tables": {"issue": {"row": {"select": [["allow", {"type": "and", "conditions": []}]]}}}
        });
        let put = |hash: &str, ast: serde_json::Value| DesiredQuerySpec {
            hash: hash.to_string(),
            ast: Some(ast),
            name: None,
            args: None,
            ttl: None,
        };
        // Pass 1: the first query is hydrated and its got-put poked + flushed.
        let cvr1 = engine
            .config_and_hydrate(
                empty_cvr("cg1", "01"),
                "client1",
                &["ws1".to_string()],
                &shard,
                vec![put("q_all", serde_json::json!({"table": "issue"}))],
                Vec::new(),
                false,
                None,
                CustomQueryTransformMode::All,
                Some(&anyone_can),
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "01".to_string(),
                0,
                0,
                0,
            )
            .await
            .unwrap();
        while rx.try_recv().is_ok() {}
        // Pass 2: a second query. Its got-put must be poked exactly once, and
        // pass 1's got-put (already delivered) must not be replayed.
        let _cvr2 = engine
            .config_and_hydrate(
                cvr1,
                "client1",
                &["ws1".to_string()],
                &shard,
                vec![put(
                    "q_desc",
                    serde_json::json!({"table": "issue", "orderBy": [["id", "desc"]]}),
                )],
                Vec::new(),
                false,
                None,
                CustomQueryTransformMode::All,
                Some(&anyone_can),
                &serde_json::json!({}),
                None,
                "00".to_string(),
                "01".to_string(),
                0,
                0,
                0,
            )
            .await
            .unwrap();
        let mut got: Vec<(String, String)> = Vec::new();
        while let Ok(WsCommand::Send { msg: v, .. }) = rx.try_recv() {
            if v[0] == "pokePart"
                && let Some(gq) = v[1].get("gotQueriesPatch").and_then(|g| g.as_array())
            {
                for e in gq {
                    got.push((
                        e["op"].as_str().unwrap_or("").to_string(),
                        e["hash"].as_str().unwrap_or("").to_string(),
                    ));
                }
            }
        }
        let q_desc_puts = got
            .iter()
            .filter(|(op, h)| op == "put" && h == "q_desc")
            .count();
        assert_eq!(
            q_desc_puts, 1,
            "pass 2 must poke q_desc's got-put exactly once (TS); got={got:?}"
        );
        assert!(
            !got.iter().any(|(_, h)| h == "q_all"),
            "pass 2 must not replay pass 1's got-put for q_all; got={got:?}"
        );
    });
}
