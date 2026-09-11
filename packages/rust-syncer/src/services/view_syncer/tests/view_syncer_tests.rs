//! Unit tests for `view_syncer.rs`.
//!
//! Kept out of line so the production file stays reviewable. Declared with
//! `#[path]` from `view_syncer.rs` under `#[cfg(test)]`, so `use super::*` sees
//! the same private items an inline `mod tests` would.

use super::*;
use crate::protocol::PROTOCOL_VERSION;
use crate::workers::syncer_ws_message_handler::{ConnContextManagerDispatch, ConnectionSelector};
use crate::ws_sink::{DirectWebSocketSink, WsCommand};
use rust_cvr::schema::types::version_from_string;

/// `fail_if_current` fails only the client's CURRENT socket: a matching
/// ws_id gets the error frame + close (TS `#failDownstream`); a stale ws_id
/// or unknown client is dropped (the reconnected socket re-pushes on its own).
#[test]
fn connection_sinks_deliver_only_to_current_socket() {
    use crate::protocol::{ErrorBody, ErrorKind};
    let sinks = ConnectionSinks::new();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    sinks.insert_for_test("cA", "ws1", DirectWebSocketSink::new(tx));
    let err = ErrorBody::basic(ErrorKind::PushFailed, "boom".to_string());

    // Stale ws_id → dropped, nothing delivered.
    assert!(!sinks.fail_if_current("cA", "ws0", &err));
    // Unknown client → dropped.
    assert!(!sinks.fail_if_current("cZ", "ws1", &err));
    assert!(rx.try_recv().is_err(), "no frame for stale/unknown targets");

    // Current ws_id → FAILED: error frame + close (TS `downstream.fail` →
    // `closeWithError`), never a bare Send that leaves the socket open.
    assert!(sinks.fail_if_current("cA", "ws1", &err));
    match rx.try_recv() {
        Ok(WsCommand::Fail(e)) => assert_eq!(crate::protocol::error_message(&e)[0], "error"),
        _ => panic!("expected WsCommand::Fail (error + close) on the current socket"),
    }
}

/// Coalescing parity with TS notifier.ts: the newer notification's fields
/// win, but the merged upstream commit time keeps the OLDEST value (it
/// bounds the lag of everything the merge subsumed).
#[test]
fn merge_notifications_keeps_newest_fields_and_oldest_commit_time() {
    let m = merge_notifications(
        serde_json::json!({"state":"version-ready","watermark":"05","upstreamCommitTimeMs":100.0}),
        serde_json::json!({"state":"version-ready","watermark":"09","upstreamCommitTimeMs":80.0}),
    );
    assert_eq!(m.get("watermark").unwrap(), "09");
    assert_eq!(m.get("upstreamCommitTimeMs").unwrap().as_f64(), Some(80.0));

    // A notification missing the commit time inherits the older one's.
    let m = merge_notifications(
        serde_json::json!({"watermark":"05","upstreamCommitTimeMs":50.0}),
        serde_json::json!({"watermark":"09"}),
    );
    assert_eq!(m.get("watermark").unwrap(), "09");
    assert_eq!(m.get("upstreamCommitTimeMs").unwrap().as_f64(), Some(50.0));

    // Fields present only on the older notification survive the merge.
    let m = merge_notifications(
        serde_json::json!({"state":"version-ready","watermark":"05"}),
        serde_json::json!({"watermark":"09"}),
    );
    assert_eq!(m.get("state").unwrap(), "version-ready");
    assert_eq!(m.get("watermark").unwrap(), "09");
}

#[test]
fn non_empty_client_cookie_against_empty_cvr_is_client_not_found() {
    let client = Some(version_from_string("01"));
    let error = check_client_and_cvr_versions(&client, &EMPTY_CVR_VERSION).unwrap_err();
    assert_eq!(error.kind(), &crate::protocol::ErrorKind::ClientNotFound);
    assert_eq!(error.message(), "Client not found");
}

#[test]
fn client_cookie_ahead_of_non_empty_cvr_is_invalid_base_cookie() {
    let client = Some(version_from_string("02"));
    // "01:00" parses to configVersion Some(0); versionString renders it as
    // the bare "01" (configVersion 0 is falsy in TS), so the error message
    // reads "01", not "01:00". See version_string's falsy-zero contract.
    let cvr = version_from_string("01:00");
    let error = check_client_and_cvr_versions(&client, &cvr).unwrap_err();
    assert_eq!(
        error.kind(),
        &crate::protocol::ErrorKind::InvalidConnectionRequestBaseCookie
    );
    assert_eq!(error.message(), "CVR is at version 01");
}

#[test]
fn client_cookie_at_or_behind_cvr_is_accepted() {
    let cvr = version_from_string("02");
    assert!(check_client_and_cvr_versions(&Some(cvr.clone()), &cvr).is_ok());
    assert!(check_client_and_cvr_versions(&Some(version_from_string("01")), &cvr).is_ok());
    assert!(check_client_and_cvr_versions(&None, &cvr).is_ok());
}

/// Wrap a test service in the shared cell + self-handle the live dispatch
/// needs (L9 Stage 3d) — the test twin of `cg_event_loop`'s setup.
fn shared(state: ViewSyncerService) -> Rc<RefCell<ViewSyncerService>> {
    let rc = Rc::new(RefCell::new(state));
    let weak = Rc::downgrade(&rc);
    rc.borrow_mut().self_handle = Some(weak);
    rc
}

struct TestFactory {
    handle: tokio::runtime::Handle,
}
impl CGServicesFactory for TestFactory {
    fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
        None
    }
    fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
        None
    }
    fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
        SyncEngineConfig {
            initialization_error: None,
            tables: Vec::new(),
            full_tables: Vec::new(),
            replica_path: None, // in-memory (no PG, no replica)
            app_id: "zero".to_string(),
            replica_version: "00".to_string(),
            shard: ShardID {
                app_id: "zero".to_string(),
                shard_num: 0,
            },
            cvr_pg: None,
            permissions: None,
            permissions_hash: None,
            revalidate_interval_ms: None,
            query_config: None,
            enable_query_covering: true,
            enable_query_planner: true,
            priority_op_running_yield_threshold_ms: 2.5,
            normal_yield_threshold_ms: 10.0,
            tokio_handle: self.handle.clone(),
            admin_password: None,
            server_version: "test".to_string(),
            metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
        }
    }
}

/// Factory that points a CG at a real on-disk replica (with a
/// `zero.permissions` row) and seeds an initial permissions hash — for the
/// hot-reload test.
struct PermsReloadFactory {
    handle: tokio::runtime::Handle,
    replica_path: String,
    initial_hash: Option<String>,
    initial_permissions: Option<serde_json::Value>,
}
impl CGServicesFactory for PermsReloadFactory {
    fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
        None
    }
    fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
        None
    }
    fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
        SyncEngineConfig {
            initialization_error: None,
            tables: Vec::new(),
            full_tables: Vec::new(),
            replica_path: Some(self.replica_path.clone()),
            app_id: "zero".to_string(),
            replica_version: "00".to_string(),
            shard: ShardID {
                app_id: "zero".to_string(),
                shard_num: 0,
            },
            cvr_pg: None,
            permissions: self.initial_permissions.clone(),
            permissions_hash: self.initial_hash.clone(),
            revalidate_interval_ms: None,
            query_config: None,
            enable_query_covering: true,
            enable_query_planner: true,
            priority_op_running_yield_threshold_ms: 2.5,
            normal_yield_threshold_ms: 10.0,
            tokio_handle: self.handle.clone(),
            admin_password: None,
            server_version: "test".to_string(),
            metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
        }
    }
}

/// `maybe_reload_permissions` is a no-op while the deployed hash is
/// unchanged, and on a redeploy (new hash) it swaps in the new compiled
/// permissions, remembers the new hash, and bumps the reload metric. This is
/// the CG-thread half of the TS `reloadPermissionsIfChanged` hot-reload.
/// TS parity: a replica notification never reloads permissions. TS
/// `#advancePipelines` (view-syncer.ts:2567-2640) does not touch them; they
/// are re-read through the pinned snapshot at the transform sites via
/// `PipelineDriver.currentPermissions()` (:1933). Before this port the
/// notification path opened a fresh replica connection and reset the
/// pipelines on a hash change — this test FAILS on that code (hash flips to
/// h2 and permissionReloads becomes 1 during the notification).
#[test]
fn notification_does_not_reload_permissions_ts_reads_them_at_transform_time() {
    use rusqlite::Connection;
    let db_path = "/tmp/rust-syncer-perms-notification-test.db";
    for suffix in ["", "-wal", "-wal2", "-shm"] {
        let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
    }
    {
        let conn = Connection::open(db_path).unwrap();
        conn.execute_batch(r#"CREATE TABLE "zero.permissions" (permissions TEXT, hash TEXT);"#)
            .unwrap();
        conn.execute(
            r#"INSERT INTO "zero.permissions" (permissions, hash) VALUES (?1, 'h1')"#,
            rusqlite::params![r#"{"tables":{}}"#],
        )
        .unwrap();
    }
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(PermsReloadFactory {
        handle: rt.handle().clone(),
        replica_path: db_path.to_string(),
        initial_hash: Some("h1".to_string()),
        initial_permissions: Some(serde_json::json!({"tables": {}})),
    });
    let global = Arc::new(Mutex::new(HashMap::new()));
    let count = Arc::new(AtomicU64::new(0));
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        global,
        count,
    );
    // The deployed doc changes on disk (hash h1 → h2) ...
    {
        let conn = Connection::open(db_path).unwrap();
        conn.execute(
            r#"UPDATE "zero.permissions" SET permissions = ?1, hash = 'h2'"#,
            rusqlite::params![r#"{"tables":{"issue":{"row":{"select":[]}}}}"#],
        )
        .unwrap();
    }
    // ... and a replica notification arrives for a group with a loaded CVR.
    state.cvr = Some(empty_cvr("cg1", "00"));
    rt.block_on(state.on_notification(serde_json::json!({"state": "version-ready"})));
    assert_eq!(
        state.permissions_hash.as_deref(),
        Some("h1"),
        "a notification must not reload permissions (TS reads them at transform time)"
    );
    assert_eq!(state.permissions, Some(serde_json::json!({"tables": {}})));
    assert_eq!(state.metrics.snapshot()["permissionReloads"], 0);
    for suffix in ["", "-wal", "-wal2", "-shm"] {
        let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
    }
}

/// TS `#pipelines.reset(clientSchema)` → `#initAndResetCommon` re-runs
/// `checkClientSchema` against the RE-READ replica schema on every reset
/// (pipeline-driver.ts:343-369); the thrown ProtocolError fails the view
/// syncer, so a schema change that drops a table the client declares ends
/// every connection with `SchemaVersionNotSupported`. Before this port rust
/// only checked at initConnection: the reset rebuilt an empty pipeline and
/// the client silently kept syncing nothing.
#[test]
fn reset_rechecks_the_client_schema_against_the_reread_replica() {
    use rusqlite::Connection;
    let db_path = format!("/tmp/rust-syncer-reset-schema-{}.db", std::process::id());
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
    }
    {
        let conn = Connection::open(&db_path).unwrap();
        let _ = conn.pragma_update(None, "journal_mode", "wal2");
        let _ = conn.pragma_update(None, "journal_mode", "wal");
        conn.execute_batch(
                r#"
                CREATE TABLE "_zero.replicationConfig" (lock TEXT PRIMARY KEY DEFAULT 'singleton',
                    replicaVersion TEXT NOT NULL, publications TEXT NOT NULL);
                CREATE TABLE "_zero.replicationState" (lock TEXT PRIMARY KEY DEFAULT 'singleton',
                    stateVersion TEXT NOT NULL);
                CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL, "table" TEXT NOT NULL,
                    "rowKey" TEXT NOT NULL, "op" TEXT NOT NULL, "pos" INTEGER NOT NULL,
                    PRIMARY KEY ("stateVersion","pos"));
                INSERT INTO "_zero.replicationConfig" VALUES ('singleton','00','[]');
                INSERT INTO "_zero.replicationState"  VALUES ('singleton','01');
                CREATE TABLE "issue" ("id" "text|NOT_NULL", "title" "text", "_0_version" "text",
                    PRIMARY KEY ("id"));
                CREATE TABLE "zero.permissions" (permissions TEXT, hash TEXT);
                "#,
            )
            .unwrap();
    }
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(PermsReloadFactory {
        handle: rt.handle().clone(),
        replica_path: db_path.clone(),
        initial_hash: None,
        initial_permissions: Some(serde_json::json!({"tables": {}})),
    });
    let global = Arc::new(Mutex::new(HashMap::new()));
    let count = Arc::new(AtomicU64::new(0));
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        global,
        count,
    );
    let (tx, mut drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    let mut cvr = empty_cvr("cg1", "00");
    cvr.client_schema = Some(serde_json::json!({
        "tables": {"issue": {"columns": {"id": {"type": "string"}}, "primaryKey": ["id"]}}
    }));
    // The replica schema changes underneath the CG: the declared table is gone.
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(r#"DROP TABLE "issue";"#).unwrap();
    }
    assert!(!state.terminal, "the engine must be live before the reset");
    rt.block_on(state.reset_pipelines_and_rehydrate(cvr, "schema change"));
    let errors = error_bodies(&mut drx);
    let err = errors.last().cloned().unwrap_or_else(|| {
        panic!("the reset must fail the connection with the TS error body; got {errors:?}")
    });
    assert_eq!(err["kind"], "SchemaVersionNotSupported");
    assert_eq!(
        err["message"],
        "The \"issue\" table does not exist or is not one of the replicated tables: ."
    );
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
    }
}

/// Replica for the post-reset tests: the `issue` table the CVR below
/// declares, with one row.
fn reset_replica_db(tag: &str) -> String {
    use rusqlite::Connection;
    let db_path = format!("/tmp/rust-syncer-reset-{tag}-{}.db", std::process::id());
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
    }
    let conn = Connection::open(&db_path).unwrap();
    let _ = conn.pragma_update(None, "journal_mode", "wal2");
    let _ = conn.pragma_update(None, "journal_mode", "wal");
    conn.execute_batch(
        r#"
            CREATE TABLE "_zero.replicationConfig" (lock TEXT PRIMARY KEY DEFAULT 'singleton',
                replicaVersion TEXT NOT NULL, publications TEXT NOT NULL);
            CREATE TABLE "_zero.replicationState" (lock TEXT PRIMARY KEY DEFAULT 'singleton',
                stateVersion TEXT NOT NULL);
            CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL, "table" TEXT NOT NULL,
                "rowKey" TEXT NOT NULL, "op" TEXT NOT NULL, "pos" INTEGER NOT NULL,
                PRIMARY KEY ("stateVersion","pos"));
            INSERT INTO "_zero.replicationConfig" VALUES ('singleton','00','[]');
            INSERT INTO "_zero.replicationState"  VALUES ('singleton','01');
            CREATE TABLE "issue" ("id" "text|NOT_NULL", "title" "text", "_0_version" "text",
                PRIMARY KEY ("id"));
            INSERT INTO "issue" VALUES ('i1','one','01');
            CREATE TABLE "zero.permissions" (permissions TEXT, hash TEXT);
            "#,
    )
    .unwrap();
    db_path
}

fn reset_replica_cleanup(db_path: &str) {
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
    }
}

fn reset_state(rt: &tokio::runtime::Runtime, db_path: &str) -> ViewSyncerService {
    let factory: Arc<dyn CGServicesFactory> = Arc::new(PermsReloadFactory {
        handle: rt.handle().clone(),
        replica_path: db_path.to_string(),
        initial_hash: None,
        initial_permissions: Some(serde_json::json!({"tables": {}})),
    });
    ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(0)),
    )
}

/// A CVR carrying one gotten query over `issue`, desired by client `c1`
/// (a query no client desires is unreferenced and would be dropped by the
/// pass itself, which is not what a reset re-hydrates).
fn reset_cvr() -> CVR {
    let mut cvr = empty_cvr("cg1", "00");
    cvr.client_schema = Some(serde_json::json!({
        "tables": {"issue": {"columns": {"id": {"type": "string"}}, "primaryKey": ["id"]}}
    }));
    for client in ["c1", "c2"] {
        cvr.clients.insert(
            client.to_string(),
            rust_cvr::schema::types::ClientRecord {
                id: client.to_string(),
                desired_query_ids: vec!["q1".to_string()],
            },
        );
    }
    let version = cvr.version.clone();
    let mut client_state = std::collections::BTreeMap::new();
    for client in ["c1", "c2"] {
        client_state.insert(
            client.to_string(),
            rust_cvr::schema::types::ClientState {
                inactivated_at: None,
                ttl: 1_000,
                version: version.clone(),
            },
        );
    }
    cvr.queries.insert(
        "q1".to_string(),
        QueryRecord::Client(rust_cvr::schema::types::ClientQueryRecord {
            base: rust_cvr::schema::types::BaseQueryRecord {
                id: "q1".to_string(),
                transformation_hash: None,
                transformation_version: None,
                row_set_signature: None,
            },
            ast: serde_json::json!({"table": "issue"}),
            client_state,
            patch_version: Some(version),
        }),
    );
    cvr
}

/// TS's post-reset rehydrate resolves its context with
/// `mustGetBackgroundConnectionContext()` (view-syncer.ts:1500-1501,
/// :1913-1914), which THROWS `ProtocolErrorWithLevel(InvalidConnectionRequest,
/// 'warn')` when no validated connection exists
/// (connection-context-manager.ts:555-565). The throw escapes the
/// `#stateChanges` loop; `run()`'s catch logs `stopping view-syncer <id>:
/// <String(e)>` at warn (:617-622) and `#cleanup(e)` ends the group. A CG
/// whose last client just dropped (reap pending) is notified and takes
/// exactly this path. Rust used to rehydrate it with an empty auth instead
/// (d8c00a28f) and keep serving. Non-vacuous: restore that branch and the
/// group stays live with its query rebuilt.
#[test]
fn reset_with_no_validated_connection_stops_the_group_like_ts() {
    use super::engine_tests::{capture_logs, captured};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db_path = reset_replica_db("noconn");
    let mut state = reset_state(&rt, &db_path);
    assert!(
        state.registered_ws.is_empty()
            && lock_unpoisoned(&state.ccm)
                .get_background_connection_context()
                .is_none(),
        "the fixture must have no validated connection"
    );
    state.pipelines_synced = true;
    let logs = {
        let (buf, _guard) = capture_logs(tracing::Level::DEBUG);
        rt.block_on(state.reset_pipelines_and_rehydrate(reset_cvr(), "advancement-timeout"));
        captured(&buf)
    };
    reset_replica_cleanup(&db_path);
    assert!(
        state.terminal,
        "TS's mustGetBackgroundConnectionContext throw ends the group; rust kept it live"
    );
    assert_eq!(
        state.query_count(),
        0,
        "the throw precedes every addQuery: nothing is rebuilt"
    );
    assert!(
        state.reset_pass_contexts.is_empty(),
        "no rehydrate pass may run without a background connection"
    );
    let line = "stopping view-syncer cg1: ProtocolError: No validated connection is \
                    available for shared query work.";
    assert_eq!(
        logs.lines()
            .filter(|l| l.contains("WARN") && l.contains(line))
            .count(),
        1,
        "TS run() logs the escaped error at getLogLevel(e) = warn (view-syncer.ts:617-622); got:\n{logs}"
    );
}

/// With validated connections, TS rebuilds in ONE pass —
/// `#hydrateUnchangedQueries` then `#syncQueryPipelineSet('missing')`,
/// view-syncer.ts:592-606 — using the BACKGROUND connection's context for
/// both (:1500-1501, :1913-1914) and poking `#getClients()`. Rust looped
/// once per registered client, each with that client's own context.
/// Non-vacuous: restore the per-client loop and two contexts are recorded.
#[test]
fn reset_runs_one_pass_with_the_background_connection_context_like_ts() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db_path = reset_replica_db("bg");
    let mut state = reset_state(&rt, &db_path);
    let (tx1, _rx1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let (tx2, _rx2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx1),
    ));
    rt.block_on(state.on_new_connection(
        pinned_params("c2", "ws2", "user-1"),
        DirectWebSocketSink::new(tx2),
    ));
    validate_test_connection(&rt, &mut state, "c1", "ws1");
    validate_test_connection(&rt, &mut state, "c2", "ws2");
    let background = lock_unpoisoned(&state.ccm)
        .get_background_connection_context()
        .map(|c| c.client_id)
        .expect("two validated connections must yield a background connection");
    state.pipelines_synced = true;
    rt.block_on(state.reset_pipelines_and_rehydrate(reset_cvr(), "advancement-timeout"));
    reset_replica_cleanup(&db_path);
    assert!(
        !state.terminal,
        "a reset with a background connection must not fail the group"
    );
    assert!(
        state.pipelines_synced,
        "TS sets #pipelinesSynced = true after the single pass (view-syncer.ts:606)"
    );
    assert_eq!(
        state.query_count(),
        1,
        "the CVR's query is back in the pipelines"
    );
    assert_eq!(
        state.reset_pass_contexts,
        vec![background],
        "exactly ONE pass, run with the background connection's context"
    );
}

/// TS parity (view-syncer.ts:1933): the transform site re-reads permissions
/// through the pipeline's pinned snapshot (`currentPermissions()`) and uses
/// the reloaded doc for THIS pass. The engine starts with a stale allow-all
/// doc (hash h0) while the replica carries a deny-all doc (hash h1): the
/// config pass must hydrate ZERO rows and end on h1. Before this port the
/// pass used the permissions it was handed and served both rows — this test
/// FAILS on that code.
#[test]
fn transform_rereads_permissions_through_the_snapshot_at_use_time() {
    use rusqlite::Connection;
    let db_path = format!("/tmp/rust-syncer-perms-usetime-{}.db", std::process::id());
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
    }
    {
        let conn = Connection::open(&db_path).unwrap();
        let _ = conn.pragma_update(None, "journal_mode", "wal2");
        let _ = conn.pragma_update(None, "journal_mode", "wal");
        conn.execute_batch(
                r#"
                CREATE TABLE "_zero.replicationConfig" (lock TEXT PRIMARY KEY DEFAULT 'singleton',
                    replicaVersion TEXT NOT NULL, publications TEXT NOT NULL);
                CREATE TABLE "_zero.replicationState" (lock TEXT PRIMARY KEY DEFAULT 'singleton',
                    stateVersion TEXT NOT NULL);
                CREATE TABLE "_zero.changeLog2" ("stateVersion" TEXT NOT NULL, "table" TEXT NOT NULL,
                    "rowKey" TEXT NOT NULL, "op" TEXT NOT NULL, "pos" INTEGER NOT NULL,
                    PRIMARY KEY ("stateVersion","pos"));
                INSERT INTO "_zero.replicationConfig" VALUES ('singleton','replica-1','[]');
                INSERT INTO "_zero.replicationState"  VALUES ('singleton','01');
                CREATE TABLE "issue" (
                    "id"    "text|NOT_NULL",
                    "title" "text",
                    "_0_version" "text",
                    PRIMARY KEY ("id")
                );
                INSERT INTO "issue" ("id", "title", "_0_version") VALUES
                    ('i1', 'first issue', '01'),
                    ('i2', 'second issue', '01');
                CREATE TABLE "app.permissions" (permissions TEXT, hash TEXT);
                INSERT INTO "app.permissions" (permissions, hash)
                    VALUES ('{"tables":{"issue":{"row":{"select":[]}}}}', 'h1');
                "#,
            )
            .unwrap();
    }
    let specs = crate::compute_table_specs_from_path(&db_path).unwrap();
    let mut pipelines = IvmPipelines::new();
    pipelines.init(specs, Some(&db_path), "app").unwrap();
    let mut engine = ViewSyncerService::new(pipelines);
    engine.app_id = "app".to_string();
    let allow_all = serde_json::json!({
        "tables": {"issue": {"row": {"select": [["allow", {"type": "and", "conditions": []}]]}}}
    });
    engine.permissions = Some(allow_all.clone());
    engine.permissions_hash = Some("h0".to_string());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<crate::ws_sink::WsCommand>();
    let sink: Arc<dyn rust_cvr::client_handler::WebSocketSink> =
        Arc::new(crate::ws_sink::DirectWebSocketSink::new(tx));
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    engine.register_client("client1", "ws1", "cg1", &shard, None, sink);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result_cvr = rt
        .block_on(engine.config_and_hydrate(
            empty_cvr("cg1", "01"),
            "client1",
            &["ws1".to_string()],
            &shard,
            vec![DesiredQuerySpec {
                hash: "q_issue".to_string(),
                ast: Some(serde_json::json!({"table": "issue"})),
                name: None,
                args: None,
                ttl: None,
            }],
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            Some(&allow_all),
            &serde_json::json!({}),
            None,
            "00".to_string(),
            "01".to_string(),
            0,
            0,
            0,
        ))
        .unwrap();
    assert!(result_cvr.queries.contains_key("q_issue"));
    assert_eq!(
        engine.permissions_hash.as_deref(),
        Some("h1"),
        "the transform site must have reloaded the replica's permissions (h0 → h1)"
    );
    assert_eq!(engine.metrics.snapshot()["permissionReloads"], 1);
    let mut rows = 0usize;
    while let Ok(Some(v)) = rx.try_recv().map(|c| c.frame_value()) {
        if v[0] == "pokePart" {
            rows += v[1]
                .get("rowsPatch")
                .and_then(|r| r.as_array())
                .map(|r| r.len())
                .unwrap_or(0);
        }
    }
    assert_eq!(
        rows, 0,
        "deny-all permissions read at use time must hydrate no rows (stale allow-all was handed in)"
    );
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{db_path}{suffix}"));
    }
}

fn test_params(client_id: &str, ws_id: &str) -> ConnectParams {
    ConnectParams {
        protocol_version: PROTOCOL_VERSION,
        client_id: client_id.to_string(),
        client_group_id: "cg1".to_string(),
        profile_id: None,
        base_cookie: None,
        timestamp: 0,
        lm_id: 0,
        ws_id: ws_id.to_string(),
        debug_perf: false,
        auth: None,
        user_id: None,
        init_connection_msg: None,
        http_cookie: None,
        origin: None,
        request_headers: Default::default(),
    }
}

fn seed_test_client_schema(state: &mut ViewSyncerService) {
    let mut cvr = empty_cvr(&state.cg_id, &state.replica_version);
    cvr.client_schema = Some(serde_json::json!({"tables": {}}));
    state.cvr = Some(cvr);
}

/// I-8 Step 2 golden: `custom_query_context_from` maps the
/// ConnectionContextManager's live `ConnectionContext` (the single owner of
/// url/headers/auth/userID) onto the transform's `CustomQueryContext`, exactly
/// as the deleted `client_query_ctx` map did. Drives a real
/// register + initConnection through the CCM, then asserts every TS-config-
/// derived field survives the mapping (url/auth/api_key/cookie/origin/
/// allowed_urls/userID + the allowlist-filtered client headers).
///
/// NON-VACUOUS: drop any field in `custom_query_context_from` (e.g. the
/// `auth` or `cookie` mapping) and the corresponding assert fails.
#[test]
fn configured_query_context_matches_typescript_defaults_and_header_filtering() {
    use crate::services::view_syncer::connection_context_manager::{
        Auth, ConnectionContextManager,
    };
    let config = FetchConfig {
        url: Some(vec!["https://api.example/query".to_string()]),
        api_key: Some("secret".to_string()),
        allowed_client_headers: Some(vec!["X-Request-ID".to_string()]),
        allowed_request_headers: None,
        forward_cookies: true,
    };
    let mut ccm = ConnectionContextManager::new(None, None, Some(config), None, None, None);
    let selector = CcmConnectionSelector {
        client_id: "c1".to_string(),
        ws_id: "w1".to_string(),
    };
    let reg = ConnectParamsForRegistration {
        client_id: "c1".to_string(),
        ws_id: "w1".to_string(),
        user_id: Some("u1".to_string()),
        profile_id: None,
        base_cookie: None,
        protocol_version: 1,
        http_cookie: Some("session=1".to_string()),
        origin: Some("https://app.example".to_string()),
        request_headers: Vec::new(),
    };
    // An authenticated connection (TS requires a userID alongside a token).
    ccm.register_connection(
        &selector,
        &reg,
        Some(Auth::Opaque {
            raw: "jwt".to_string(),
        }),
    );
    // initConnection carries client headers; only the allowlisted one survives.
    ccm.init_connection(
        &selector,
        &InitConnectionBody {
            user_query_url: None,
            user_query_headers: Some(std::collections::HashMap::from([
                ("x-request-id".to_string(), "allowed".to_string()),
                ("authorization".to_string(), "blocked".to_string()),
            ])),
            user_push_url: None,
            user_push_headers: None,
        },
    )
    .unwrap();

    let ctx = ccm.must_get_connection_context(&selector).unwrap();
    let context = custom_query_context_from(&ctx).unwrap();
    assert_eq!(context.url, "https://api.example/query");
    assert_eq!(context.auth.as_deref(), Some("jwt"));
    assert_eq!(context.api_key.as_deref(), Some("secret"));
    assert_eq!(context.cookie.as_deref(), Some("session=1"));
    assert_eq!(context.origin.as_deref(), Some("https://app.example"));
    assert_eq!(context.allowed_urls, vec!["https://api.example/query"]);
    assert_eq!(context.user_id.as_deref(), Some("u1"));
    // initConnection header filtering: allowlisted survives, others dropped.
    assert_eq!(
        context.client_headers,
        vec![("x-request-id".to_string(), "allowed".to_string())]
    );
    // The composed outgoing set carries them all (TS fetchFromAPIServer).
    let composed = context.composed_headers();
    assert!(composed.contains(&("X-Api-Key".to_string(), "secret".to_string())));
    assert!(composed.contains(&("Cookie".to_string(), "session=1".to_string())));
    assert!(composed.contains(&("Origin".to_string(), "https://app.example".to_string())));
}

#[test]
fn forwards_allowlisted_incoming_request_headers() {
    // Port of #6144: only headers on `allowed_request_headers` (case-
    // insensitive) are forwarded from the incoming request to the query API.
    // Read back from the CCM via `custom_query_context_from` (I-8 Step 2).
    use crate::services::view_syncer::connection_context_manager::ConnectionContextManager;
    let config = FetchConfig {
        url: Some(vec!["https://api.example/query".to_string()]),
        api_key: None,
        allowed_client_headers: None,
        allowed_request_headers: Some(vec!["X-Forwarded-For".to_string(), "x-tenant".to_string()]),
        forward_cookies: false,
    };
    let mut ccm = ConnectionContextManager::new(None, None, Some(config), None, None, None);
    let selector = CcmConnectionSelector {
        client_id: "c1".to_string(),
        ws_id: "w1".to_string(),
    };
    let reg = ConnectParamsForRegistration {
        client_id: "c1".to_string(),
        ws_id: "w1".to_string(),
        user_id: None,
        profile_id: None,
        base_cookie: None,
        protocol_version: 1,
        http_cookie: None,
        origin: None,
        request_headers: vec![
            ("x-forwarded-for".to_string(), "203.0.113.7".to_string()),
            ("x-tenant".to_string(), "acme".to_string()),
            ("authorization".to_string(), "secret".to_string()),
        ],
    };
    ccm.register_connection(&selector, &reg, None);

    let ctx = ccm.must_get_connection_context(&selector).unwrap();
    let context = custom_query_context_from(&ctx).unwrap();
    // Allowlisted (case-insensitive) headers forwarded; others dropped.
    assert!(
        context
            .request_headers
            .contains(&("x-forwarded-for".to_string(), "203.0.113.7".to_string()))
    );
    assert!(
        context
            .request_headers
            .contains(&("x-tenant".to_string(), "acme".to_string()))
    );
    assert!(
        !context
            .request_headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("authorization"))
    );
}

fn authed_params(client_id: &str, ws_id: &str, token: &str) -> ConnectParams {
    let mut p = test_params(client_id, ws_id);
    p.auth = Some(token.to_string());
    p
}

/// A minimal decodable JWT (`header.payload.sig`) whose payload is `{sub}`.
/// Only the payload is read by `decode_jwt_claims`; the signature is unused
/// by the pin pre-check.
fn fake_jwt(sub: &str) -> String {
    use base64::Engine;
    let payload = serde_json::json!({ "sub": sub }).to_string();
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
    format!("hdr.{b64}.sig")
}

pub(super) fn pinned_params(client_id: &str, ws_id: &str, user_id: &str) -> ConnectParams {
    let mut p = authed_params(client_id, ws_id, &fake_jwt(user_id));
    p.user_id = Some(user_id.to_string());
    p
}

/// The raw auth token the ConnectionContextManager holds for a connection.
/// Replaces the deleted `client_raw_auth` map in tests — the CCM is now the
/// single owner of per-connection auth (I-8).
fn ccm_raw_auth(state: &ViewSyncerService, client_id: &str, ws_id: &str) -> Option<String> {
    lock_unpoisoned(&state.ccm)
        .get_connection_context(&CcmConnectionSelector {
            client_id: client_id.to_string(),
            ws_id: ws_id.to_string(),
        })
        .and_then(|c| c.auth)
        .map(|a| a.raw().to_string())
}

/// AuthValidator whose verdict flips with a shared flag — to simulate a
/// token that later expires / is revoked.
struct ToggleAuthValidator {
    valid: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait::async_trait]
impl AuthValidator for ToggleAuthValidator {
    async fn validate_auth(
        &self,
        _cg: &str,
        _cid: &str,
        _uid: Option<&str>,
        _auth: Option<&str>,
    ) -> Result<(), crate::protocol::ErrorBody> {
        if self.valid.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(crate::protocol::ErrorBody::unauthorized("token expired"))
        }
    }
}

/// Factory with a configurable periodic auth-maintenance interval (no PG,
/// in-memory sources).
struct RevalidateFactory {
    handle: tokio::runtime::Handle,
    revalidate_interval_ms: Option<i64>,
    /// When set, the CCM gets a `FetchConfig` allowing exactly this
    /// `ZERO_QUERY_URL` (TS: a configured transformer), so a connection's
    /// `userQueryURL` at that address passes the request-time allow check.
    query_url: Option<String>,
}
impl CGServicesFactory for RevalidateFactory {
    fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
        None
    }
    fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
        None
    }
    fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
        SyncEngineConfig {
            initialization_error: None,
            tables: Vec::new(),
            full_tables: Vec::new(),
            replica_path: None,
            app_id: "zero".to_string(),
            replica_version: "00".to_string(),
            shard: ShardID {
                app_id: "zero".to_string(),
                shard_num: 0,
            },
            cvr_pg: None,
            permissions: None,
            permissions_hash: None,
            revalidate_interval_ms: self.revalidate_interval_ms,
            query_config: self.query_url.as_ref().map(|u| FetchConfig {
                url: Some(vec![u.clone()]),
                api_key: None,
                allowed_client_headers: None,
                allowed_request_headers: None,
                forward_cookies: false,
            }),
            enable_query_covering: true,
            enable_query_planner: true,
            priority_op_running_yield_threshold_ms: 2.5,
            normal_yield_threshold_ms: 10.0,
            tokio_handle: self.handle.clone(),
            admin_password: None,
            server_version: "test".to_string(),
            metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
        }
    }
}

/// Test double: records the pusher hooks the view-syncer wires.
struct RecordingHookPusher {
    validate_hook: Mutex<Option<crate::workers::syncer_ws_message_handler::ValidateHook>>,
}
impl PusherDispatch for RecordingHookPusher {
    fn enqueue_push(
        &self,
        _selector: &ConnectionSelector,
        _body: &serde_json::Value,
        _headers: &crate::workers::syncer_ws_message_handler::PushRelayHeaders,
        _client_group_id: &str,
    ) -> crate::workers::connection::HandlerResult {
        crate::workers::connection::HandlerResult::Ok
    }
    fn init_connection(&self, _s: &ConnectionSelector) {}
    fn ack_mutation_responses(
        &self,
        _selector: &ConnectionSelector,
        _body: &serde_json::Value,
        _headers: &crate::workers::syncer_ws_message_handler::PushRelayHeaders,
        _client_group_id: &str,
    ) {
    }
    fn delete_client_mutations(
        &self,
        _selector: &ConnectionSelector,
        _client_ids: &[String],
        _headers: &crate::workers::syncer_ws_message_handler::PushRelayHeaders,
        _client_group_id: &str,
    ) {
    }
    // Explicit empty body: this test dispatch has no connection-context
    // owner. `set_auth_fail_hook` is a REQUIRED trait method so the choice
    // cannot be inherited silently.
    fn set_auth_fail_hook(&self, _hook: crate::workers::syncer_ws_message_handler::AuthFailHook) {}

    fn set_validate_hook(&self, hook: crate::workers::syncer_ws_message_handler::ValidateHook) {
        *self.validate_hook.lock().unwrap() = Some(hook);
    }
}
struct RecordingHookFactory {
    handle: tokio::runtime::Handle,
    pusher: Arc<RecordingHookPusher>,
}
impl CGServicesFactory for RecordingHookFactory {
    fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
        None
    }
    fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
        Some(self.pusher.clone())
    }
    fn create_sync_engine_config(&self, cg: &str) -> SyncEngineConfig {
        RevalidateFactory {
            handle: self.handle.clone(),
            revalidate_interval_ms: None,
            query_url: None,
        }
        .create_sync_engine_config(cg)
    }
}

/// TS `#processPush` (pusher.ts:545-556) → `#connContextManager
/// .validateConnection(connCtx, revision, validation)`: the view-syncer
/// must hand the pusher a validate path into THIS group's CCM, and that
/// path must (a) mark a matching `server-validated` connection Validated
/// and (b) reject a mismatched userID with `Unauthorized` (TS
/// connection-context-manager.ts:420). Without the wiring the pusher's
/// hook stays unset: a successful push never validates the connection and
/// the background retransform finds none ("No validated connection is
/// available for shared query work", prod 14/day).
#[test]
fn successful_push_validates_connection_through_the_ccm() {
    use crate::services::view_syncer::connection_context_manager::ConnectionState;
    let rt = tokio::runtime::Runtime::new().unwrap();
    let pusher = Arc::new(RecordingHookPusher {
        validate_hook: Mutex::new(None),
    });
    let factory: Arc<dyn CGServicesFactory> = Arc::new(RecordingHookFactory {
        handle: rt.handle().clone(),
        pusher: pusher.clone(),
    });
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(ToggleAuthValidator {
            valid: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        }),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(0)),
    );
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    let hook = pusher.validate_hook.lock().unwrap().clone().expect(
        "view-syncer must wire the pusher's validate hook (TS #processPush → validateConnection)",
    );
    let sel = ConnectionSelector {
        client_id: "c1".to_string(),
        ws_id: "ws1".to_string(),
    };
    let ccm_sel = CcmConnectionSelector {
        client_id: "c1".to_string(),
        ws_id: "ws1".to_string(),
    };
    let revision = lock_unpoisoned(&state.ccm)
        .get_connection_context(&ccm_sel)
        .expect("connection registered in the CCM")
        .revision;
    hook(
        &sel,
        revision,
        ConnectionValidation::ServerValidated {
            validated_user_id: Some("user-1".to_string()),
        },
    )
    .expect("a matching server userID validates the connection");
    let ctx = lock_unpoisoned(&state.ccm)
        .get_connection_context(&ccm_sel)
        .expect("still registered");
    assert_eq!(ctx.state, ConnectionState::Validated);
    let err = hook(
        &sel,
        revision,
        ConnectionValidation::ServerValidated {
            validated_user_id: Some("someone-else".to_string()),
        },
    )
    .expect_err(
        "a mismatched server userID must be rejected (TS: validateConnection throws Unauthorized)",
    );
    assert!(matches!(err, CCMError::Unauthorized(_)), "got {err:?}");
}

/// Drive the initConnection-time `#validateConnection` (view-syncer.ts:942)
/// for a test connection.
///
/// TS `registerConnection` leaves a connection UNVALIDATED
/// (connection-context-manager.ts:267 sets `revalidateAt: undefined`); it is
/// `initConnection` that validates and thereby arms revalidation. Rust used
/// to record a `client-fallback` validation at socket accept, which is why
/// these tests could assert on maintenance straight after
/// `on_new_connection`. Now that the ordering matches TS, they have to
/// validate explicitly.
///
/// Only the validation half is driven, not the whole config/hydrate pass:
/// the barebones test factory's config pass fails and tears the connection
/// down, which would mask exactly what these tests assert.
pub(super) fn validate_test_connection(
    rt: &tokio::runtime::Runtime,
    state: &mut ViewSyncerService,
    client_id: &str,
    ws_id: &str,
) {
    let selector = CcmConnectionSelector {
        client_id: client_id.to_string(),
        ws_id: ws_id.to_string(),
    };
    let Ok(ctx) = lock_unpoisoned(&state.ccm).must_get_connection_context(&selector) else {
        return;
    };
    let _ = rt.block_on(state.validate_connection(&ctx));
    state.schedule_auth_maintenance();
}

pub(super) fn revalidate_state(
    rt: &tokio::runtime::Runtime,
    interval_ms: Option<i64>,
    valid: Arc<std::sync::atomic::AtomicBool>,
) -> ViewSyncerService {
    revalidate_state_with_query_url(rt, interval_ms, valid, None)
}

/// `revalidate_state` with a configured `ZERO_QUERY_URL` allow-list entry.
pub(super) fn revalidate_state_with_query_url(
    rt: &tokio::runtime::Runtime,
    interval_ms: Option<i64>,
    valid: Arc<std::sync::atomic::AtomicBool>,
    query_url: Option<String>,
) -> ViewSyncerService {
    let factory: Arc<dyn CGServicesFactory> = Arc::new(RevalidateFactory {
        handle: rt.handle().clone(),
        revalidate_interval_ms: interval_ms,
        query_url,
    });
    ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(ToggleAuthValidator { valid }),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(0)),
    )
}

/// F-CVR-STORE-8 interval state machine — ports of TS
/// `#startTTLClockInterval` / `#stopTTLClockInterval` /
/// `#updateTTLClockInCVRWithoutLock` (view-syncer.ts:1091-1119). The
/// interval must be OFF until a flush arms it (TS arms only in
/// `#flushUpdater`'s `if (flushed)`), arm ~TTL_CLOCK_INTERVAL out, and the
/// no-CVR guard must keep the update call a no-op.
/// Port-parity for `#checkForThrashing` (view-syncer.ts:2121-2148): three
/// transformation replacements INSIDE the 60s window reach the warn
/// threshold; a replacement OUTSIDE the window starts a FRESH record
/// (count back to 1 — TS deletes + reinserts `{count: 1}`), and records
/// are per-query. Pins the window-reset branch: a port that only ever
/// increments would pass the in-window assertions but fail the reset one.
/// TS `ClientHandler.fail(e)` → `wrapWithProtocolError(e)`
/// (types/error-with-level.ts): a failure the view-syncer cannot serve
/// through — here the CVR store's Postgres is unreachable — reaches the
/// client as `{kind: Internal, message: getErrorMessage(e), origin:
/// ZeroCache}` with the UNDERLYING error text. Rust used to send a
/// `Rehome` with a fixed label ("Unable to load the client view state" /
/// "Client view synchronization failed"): a different kind (zero-client
/// reconnects immediately on Rehome, backs off on Internal) and no
/// diagnostic for the app.
#[test]
fn store_failure_fails_clients_with_internal_like_ts_wrap_with_protocol_error() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, None, valid);
    // sqlx pools spawn their reaper on construction → needs the runtime context.
    let pool = rt.block_on(async {
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_millis(300))
            .connect_lazy("postgres://nobody:nobody@127.0.0.1:1/nowhere")
            .expect("lazy pool")
    });
    state
        .set_cvr_store(
            pool,
            "cvr".to_string(),
            "cg1".to_string(),
            "task-0".to_string(),
        )
        .expect("set store");
    state.cvr_pg = true; // production wiring flips this after set_cvr_store (new_with_accepting)
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    // The replication notification runs `ensure_cvr` → `load_cvr` through
    // the store (TS `#runInLockWithCVR` → `#cvrStore.load()` in the
    // #stateChanges loop) — the first PG round trip, which fails here.
    let logs = {
        use super::engine_tests::{capture_logs, captured};
        let (buf, _guard) = capture_logs(tracing::Level::DEBUG);
        rt.block_on(state.on_notification(serde_json::json!({"state": "version-ready"})));
        captured(&buf)
    };
    // LEVEL: a PG failure is a plain `Error` in TS — the raw-throw branch of
    // `getLogLevel` — so `#cleanup(err)` → `client.fail(err)` logs at ERROR
    // and `sendError` classifies the wrapped-ProtocolError frame line at
    // WARN (`cvr_store_error_thrown`). NON-VACUOUS (2026-09-08): with the
    // `None` every caller passed before, both came out WARN.
    let at = |level: &str, msg: &str| {
        logs.lines()
            .filter(|l| l.contains(level) && l.contains(msg))
            .count()
    };
    assert_eq!(
        at("ERROR", "view-syncer closing connection with error"),
        1,
        "TS `ClientHandler.fail` logs a raw store error at ERROR; got:\n{logs}"
    );
    assert_eq!(
        at("WARN", "Sending error on WebSocket"),
        1,
        "the frame line reads the WRAPPED ProtocolError → WARN; got:\n{logs}"
    );
    let mut errors: Vec<serde_json::Value> = Vec::new();
    while let Ok(cmd) = rx.try_recv() {
        match cmd {
            WsCommand::Send { msg, .. } if msg[0] == "error" => errors.push(msg[1].clone()),
            WsCommand::Fail(e) => errors.push(crate::protocol::error_message(&e)[1].clone()),
            _ => {}
        }
    }
    let e = errors
        .last()
        .cloned()
        .expect("an unreachable CVR store must fail the connection with an error frame");
    assert_eq!(
        e["kind"], "Internal",
        "TS wrapWithProtocolError → kind Internal; got {e}"
    );
    assert_eq!(
        e["origin"], "zeroCache",
        "TS wrapWithProtocolError sets origin ZeroCache; got {e}"
    );
    let msg = e["message"].as_str().unwrap_or_default();
    assert!(
        !msg.is_empty()
            && msg != "Unable to load the client view state"
            && msg != "Client view synchronization failed",
        "TS sends getErrorMessage(e) — the underlying error text, not a fixed label; got {msg:?}"
    );
}

/// TS `OwnershipError` is a `ProtocolErrorWithLevel(.., 'info')`
/// (cvr-store.ts:1382-1400): when another instance takes the CVR,
/// `#cleanup(err)` → `client.fail(err)` logs
/// `view-syncer closing connection with error` at INFO and `sendError`
/// (the `thrown instanceof ProtocolErrorWithLevel` branch) logs
/// `Sending error on WebSocket` at INFO. An ownership hand-off is routine,
/// not a pager event.
///
/// NON-VACUOUS (2026-09-08): `cvr_store_error_thrown` returning
/// `WithLevel(Warn)` — or the `None` every caller passed before it existed —
/// makes both counts 0 and the WARN count 2.
#[test]
fn ownership_transfer_fails_clients_at_info_like_ts_ownership_error() {
    use super::engine_tests::{capture_logs, captured};
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, None, valid);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    while rx.try_recv().is_ok() {}

    let error = CVRStoreError::OwnershipError {
        owner: Some("other-task".to_string()),
        granted_at: 1_000.0,
        last_connect_time: 0.0,
    };
    let message = error.to_string();
    let logs = {
        let (buf, _guard) = capture_logs(tracing::Level::DEBUG);
        state.fail_group_with_error(
            cvr_store_error_body(&error),
            Some(cvr_store_error_thrown(&error, &message)),
        );
        captured(&buf)
    };
    let at = |level: &str, msg: &str| {
        logs.lines()
            .filter(|l| l.contains(level) && l.contains(msg))
            .count()
    };
    assert_eq!(
        at("INFO", "view-syncer closing connection with error"),
        1,
        "OwnershipError is level 'info' in TS (cvr-store.ts:1400); got:\n{logs}"
    );
    // TS `String(e)` = `${e.name}: ${e.message}`, and `OwnershipError`
    // overrides `name` (cvr-store.ts:1383). Non-vacuous: render the bare
    // message and this reads 0.
    assert_eq!(
        at(
            "INFO",
            "view-syncer closing connection with error: OwnershipError: CVR ownership was \
                 transferred to other-task at 1970-01-01T00:00:01.000Z (last connect time: \
                 1970-01-01T00:00:00.000Z)"
        ),
        1,
        "the fail line must print TS's `String(e)`; got:\n{logs}"
    );
    assert_eq!(
        at("INFO", "Sending error on WebSocket"),
        1,
        "sendError takes the ProtocolErrorWithLevel branch → its own 'info'; got:\n{logs}"
    );
    assert_eq!(
        at("WARN", "closing connection with error") + at("WARN", "Sending error on WebSocket"),
        0,
        "nothing about an ownership hand-off is WARN in TS; got:\n{logs}"
    );
    let frame = loop {
        match rx.try_recv() {
            Ok(WsCommand::Send { msg, .. }) if msg[0] == "error" => break msg[1].clone(),
            Ok(_) => continue,
            Err(_) => panic!("the client must receive the Rehome frame"),
        }
    };
    assert_eq!(frame["kind"], "Rehome");
    assert_eq!(
        frame["maxBackoffMs"], 0,
        "TS OwnershipError sets maxBackoffMs: 0"
    );
}

/// A `ClientNotFound` from the CVR *load* on a client-initiated lock op must
/// fail ONLY the requesting client. TS: `deleteClients` runs inside
/// `#runInLockForClient`; `CVRStore.load` throws inside `#runInLockWithCVR`
/// (view-syncer.ts:489-493) BEFORE the callback runs, so `client` is still
/// undefined in the catch and the error is RETHROWN (view-syncer.ts:1249)
/// -> `Connection.#handleMessage`'s catch -> `#closeWithThrown` closes that
/// ONE socket (workers/connection.ts:229-230); every other client of the
/// group keeps its stream. Rust used to `fail_group_with_error` here, so a
/// purged CVR ("Client has been purged due to inactivity", cvr-store.ts:423)
/// surfaced by ONE client's `deleteClients` wiped every other client of the
/// group. Non-vacuous: restoring `fail_group_with_error` makes clientB
/// receive the error frame and the group go terminal.
#[test]
fn client_not_found_at_load_fails_only_the_requesting_client() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, None, valid);
    let (tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let (tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("clientA", "wsA", "user-1"),
        DirectWebSocketSink::new(tx_a),
    ));
    rt.block_on(state.on_new_connection(
        pinned_params("clientB", "wsB", "user-1"),
        DirectWebSocketSink::new(tx_b),
    ));
    // `deleteClients` is honored only from a socket that has sent
    // initConnection (TS `#runInLockForClient`, view-syncer.ts:1216):
    // register both handlers the TS way (view-syncer.ts:914).
    assert!(state.init_connection("clientA", "wsA"));
    assert!(state.init_connection("clientB", "wsB"));

    // The next store load answers with TS's purge verdict (cvr-store.ts:423).
    state.cvr = None;
    state.cvr_pg = true;
    force_load_error(CVRStoreError::ClientNotFound(
        "Client has been purged due to inactivity".to_string(),
    ));

    // Log-level parity (2026-09-08): TS logs a load failure ONCE, from
    // `sendError` at the thrown error's level — `warn` for
    // `ClientNotFoundError` (cvr-store.ts:1362, connection.ts:428). Rust
    // additionally logged a rust-only `unable to load CVR` line at ERROR,
    // which turned a routine purged-client reconnect into a paging-level
    // event ("0 ERROR" is the prod health signal). Capture ERROR-and-above:
    // it must stay empty. Revert the `tracing::debug!` back to
    // `tracing::error!` in `apply_client_deletions` → fails.
    let errors_logged = {
        use std::sync::{Arc, Mutex};
        #[derive(Clone)]
        struct CapWriter(Arc<Mutex<Vec<u8>>>);
        struct CapGuard(Arc<Mutex<Vec<u8>>>);
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapWriter {
            type Writer = CapGuard;
            fn make_writer(&'a self) -> CapGuard {
                CapGuard(self.0.clone())
            }
        }
        impl std::io::Write for CapGuard {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(CapWriter(buf.clone()))
            .with_ansi(false)
            .with_max_level(tracing::Level::ERROR)
            .finish();
        crate::ensure_permissive_global_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            rt.block_on(state.apply_client_deletions(
                "clientA",
                None,
                &["clientC".to_string()],
                &[],
            ));
        });
        String::from_utf8(buf.lock().unwrap().clone()).unwrap()
    };
    assert!(
        errors_logged.is_empty(),
        "a purged client is a WARN-level event in TS (ClientNotFoundError → 'warn'); \
             rust logged at ERROR:\n{errors_logged}"
    );

    let mut errors_a: Vec<serde_json::Value> = Vec::new();
    while let Ok(cmd) = rx_a.try_recv() {
        match cmd {
            WsCommand::Send { msg, .. } if msg[0] == "error" => errors_a.push(msg[1].clone()),
            WsCommand::Fail(e) => errors_a.push(crate::protocol::error_message(&e)[1].clone()),
            _ => {}
        }
    }
    let mut errors_b: Vec<serde_json::Value> = Vec::new();
    while let Ok(cmd) = rx_b.try_recv() {
        match cmd {
            WsCommand::Send { msg, .. } if msg[0] == "error" => errors_b.push(msg[1].clone()),
            WsCommand::Fail(e) => errors_b.push(crate::protocol::error_message(&e)[1].clone()),
            _ => {}
        }
    }
    assert_eq!(
        errors_a.len(),
        1,
        "the requesting client must be failed exactly once; got {errors_a:?}"
    );
    assert_eq!(errors_a[0]["kind"], "ClientNotFound");
    assert_eq!(
        errors_a[0]["message"], "Client has been purged due to inactivity",
        "the store message reaches the client verbatim (cvr-store.ts:424)"
    );
    assert!(
        errors_b.is_empty(),
        "TS keeps serving the group's other clients; clientB received {errors_b:?}"
    );
    assert!(
        !state.terminal,
        "the group must stay alive for its other clients (TS never reaches #cleanup here)"
    );
    assert!(
        state.connections.contains_key("clientB"),
        "clientB's connection must survive the requesting client's failure"
    );
}

/// TS `wrapWithProtocolError` returns a ProtocolError UNCHANGED
/// (error-with-level.ts:33-35) and EVERY error `CVRStore` raises is one
/// (cvr-store.ts:1354-1420). Rust stringified them all into `fail_group`, so
/// an ownership transfer — what a rolling restart does to every client group
/// when the new task takes the CVR — reached the client as `{kind: Internal}`.
/// The zero-client BACKS OFF on Internal and reconnects immediately on
/// Rehome (`maxBackoffMs: 0`), so every client of that group waited out a
/// backoff TS never imposes. Table-driven: a new variant that forgets its arm
/// falls into the Internal fallback and fails its row.
#[test]
fn cvr_store_errors_reach_the_client_with_their_ts_kind() {
    let cases: Vec<(CVRStoreError, serde_json::Value)> = vec![
        (
            CVRStoreError::OwnershipError {
                owner: Some("task-B".to_string()),
                granted_at: 1_757_243_985_123.0,
                last_connect_time: 1_757_243_000_000.0,
            },
            serde_json::json!({
                "kind": "Rehome",
                "message": "CVR ownership was transferred to task-B at \
                            2025-09-07T11:19:45.123Z (last connect time: \
                            2025-09-07T11:03:20.000Z)",
                "maxBackoffMs": 0,
                "origin": "zeroCache",
            }),
        ),
        (
            CVRStoreError::ConcurrentModification {
                expected: "01".to_string(),
                actual: "02".to_string(),
            },
            serde_json::json!({
                "kind": "Rehome",
                "message": "CVR has been concurrently modified. Expected 01, got 02",
                "origin": "zeroCache",
            }),
        ),
        (
            CVRStoreError::InvalidClientSchema("bad".to_string()),
            serde_json::json!({
                "kind": "SchemaVersionNotSupported",
                "message": "Could not parse clientSchema stored in CVR: bad",
                "origin": "zeroCache",
            }),
        ),
        (
            CVRStoreError::ClientNotFound("Client has been purged due to inactivity".to_string()),
            serde_json::json!({
                "kind": "ClientNotFound",
                "message": "Client has been purged due to inactivity",
                "origin": "zeroCache",
            }),
        ),
        // NOT a TS ProtocolError → `wrapWithProtocolError` wraps it.
        (
            CVRStoreError::RowsVersionBehind {
                cvr_version: "03".to_string(),
                rows_version: None,
            },
            serde_json::json!({
                "kind": "Internal",
                "message": "Rows version behind: cvr=03, rows=None",
                "origin": "zeroCache",
            }),
        ),
    ];
    for (error, want) in cases {
        let got = serde_json::to_value(cvr_store_error_body(&error)).unwrap();
        assert_eq!(got, want, "wire body for {error:?}");
    }
}

/// `String(e)` prints `${e.name}: ${e.message}`; the cvr-store classes that
/// override `name` (cvr-store.ts:1368, 1383, 1406, 1438) print it, the ones
/// that do not print `ProtocolError`, and a third `:` in a version string
/// is a `TypeError` (schema/types.ts:339). Non-vacuous: collapse the names
/// to 'ProtocolError'/'Error' and the rows fail.
#[test]
fn cvr_store_error_thrown_prints_the_ts_error_name() {
    let cases: Vec<(CVRStoreError, &str)> = vec![
        (
            CVRStoreError::ClientNotFound("purged".to_string()),
            "ProtocolError: purged",
        ),
        (
            CVRStoreError::ConcurrentModification {
                expected: "01".to_string(),
                actual: "02".to_string(),
            },
            "ConcurrentModificationException: CVR has been concurrently modified. Expected 01, got 02",
        ),
        (
            CVRStoreError::OwnershipError {
                owner: Some("task-B".to_string()),
                granted_at: 0.0,
                last_connect_time: 0.0,
            },
            "OwnershipError: CVR ownership was transferred to task-B at 1970-01-01T00:00:00.000Z (last connect time: 1970-01-01T00:00:00.000Z)",
        ),
        (
            CVRStoreError::InvalidClientSchema("bad".to_string()),
            "InvalidClientSchemaError: Could not parse clientSchema stored in CVR: bad",
        ),
        (
            CVRStoreError::RowsVersionBehind {
                cvr_version: "03".to_string(),
                rows_version: None,
            },
            "RowsVersionBehindError: Rows version behind: cvr=03, rows=None",
        ),
        (
            CVRStoreError::VersionParse(rust_cvr::schema::types::VersionError::TooManyParts(
                "a:b:c".to_string(),
            )),
            "TypeError: Invalid version string in CVR data: invalid version string \"a:b:c\": \
                 more than one ':' separator",
        ),
    ];
    for (error, want) in cases {
        let message = error.to_string();
        let body = cvr_store_error_body(&error);
        let got = cvr_store_error_thrown(&error, &message).js_string(body.message());
        assert_eq!(got, want, "String(e) for {error:?}");
    }
}

/// The same, at the live seam: a load that loses ownership must reach EVERY
/// client of the group as TS's Rehome body. This is the run-loop path, where
/// TS's throw exits `#stateChanges` and `#cleanup(err)` fails every client
/// (view-syncer.ts:2820-2826), so the group scope is 1:1 — only the KIND was
/// wrong. Non-vacuous: restoring `fail_group(&e.to_string())` makes both
/// clients receive `{kind: Internal}`.
#[test]
fn ownership_loss_at_load_rehomes_every_client() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, None, valid);
    let (tx_a, mut rx_a) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let (tx_b, mut rx_b) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("clientA", "wsA", "user-1"),
        DirectWebSocketSink::new(tx_a),
    ));
    rt.block_on(state.on_new_connection(
        pinned_params("clientB", "wsB", "user-1"),
        DirectWebSocketSink::new(tx_b),
    ));
    state.cvr = None;
    state.cvr_pg = true;
    force_load_error(CVRStoreError::OwnershipError {
        owner: Some("task-B".to_string()),
        granted_at: 1_757_243_985_123.0,
        last_connect_time: 1_757_243_000_000.0,
    });

    rt.block_on(state.on_notification(serde_json::json!({"state": "version-ready"})));

    for (who, rx) in [("clientA", &mut rx_a), ("clientB", &mut rx_b)] {
        let mut errors: Vec<serde_json::Value> = Vec::new();
        while let Ok(cmd) = rx.try_recv() {
            match cmd {
                WsCommand::Send { msg, .. } if msg[0] == "error" => errors.push(msg[1].clone()),
                WsCommand::Fail(e) => errors.push(crate::protocol::error_message(&e)[1].clone()),
                _ => {}
            }
        }
        let e = errors
            .last()
            .cloned()
            .unwrap_or_else(|| panic!("{who} must be failed when the CVR is owned elsewhere"));
        assert_eq!(e["kind"], "Rehome", "{who} got {e}");
        assert_eq!(
            e["maxBackoffMs"], 0,
            "{who} must reconnect NOW, not back off: {e}"
        );
        assert_eq!(
            e["message"],
            "CVR ownership was transferred to task-B at 2025-09-07T11:19:45.123Z \
                 (last connect time: 2025-09-07T11:03:20.000Z)",
            "{who} message must be TS's verbatim"
        );
    }
}

#[test]
fn check_for_thrashing_window_and_threshold() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, None, valid);

    state.check_for_thrashing("q1");
    assert_eq!(state.query_replacements.get("q1").unwrap().count, 1);
    state.check_for_thrashing("q1");
    state.check_for_thrashing("q1");
    assert_eq!(
        state.query_replacements.get("q1").unwrap().count,
        3,
        "3 in-window replacements must reach the TS THRASH_THRESHOLD"
    );

    // Age the record past THRASH_WINDOW_MS (60s): the next replacement
    // must reset, not increment to 4.
    state.query_replacements.get_mut("q1").unwrap().window_start = now_ms() - 60_001;
    state.check_for_thrashing("q1");
    let rec = state.query_replacements.get("q1").unwrap();
    assert_eq!(
        rec.count, 1,
        "outside-window replacement starts a fresh count"
    );
    assert!(now_ms() - rec.window_start < 60_000, "fresh window start");

    // Records are per-query (TS keys #queryReplacements by queryID).
    state.check_for_thrashing("q2");
    assert_eq!(state.query_replacements.get("q2").unwrap().count, 1);
    assert_eq!(state.query_replacements.get("q1").unwrap().count, 1);
}

#[test]
fn ttl_clock_interval_state_machine() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, None, valid);

    // Not running until a flush starts it (a read-only CG never ticks).
    assert!(state.ttl_clock_interval.is_none());
    assert!(state.next_ttl_clock_delay().is_none());

    let before = now_ms();
    state.start_ttl_clock_interval();
    let deadline = state.ttl_clock_interval.expect("armed");
    assert!(
        deadline >= before + TTL_CLOCK_INTERVAL && deadline <= now_ms() + TTL_CLOCK_INTERVAL,
        "deadline must be TTL_CLOCK_INTERVAL (60s) out"
    );
    assert!(state.next_ttl_clock_delay().is_some());

    // Restart replaces the deadline (TS stop-then-set).
    state.start_ttl_clock_interval();
    assert!(state.ttl_clock_interval.expect("re-armed") >= deadline);

    state.stop_ttl_clock_interval();
    assert!(state.ttl_clock_interval.is_none());

    // No loaded CVR (TS `#ttlClock !== undefined` guard): update is a no-op
    // and must not advance the in-memory clock bookkeeping.
    let base_before = state.ttl_clock_base;
    state.update_ttl_clock_in_cvr_without_lock();
    assert_eq!(state.ttl_clock_base, base_before);
}

/// Periodic revalidation must CLOSE a connection whose token no longer
/// validates (expired/revoked). Security core of TS `#runAuthMaintenance`'s
/// `dueRevalidations` → `#validateConnection` failure path.
#[test]
fn periodic_revalidation_closes_expired_connection() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    // Interval 0 → the CCM marks the connection due immediately, so the
    // manually-fired tick below actually has revalidation work (the plan-
    // driven tick honors `revalidate_at`; an early fire is a no-op).
    let mut state = revalidate_state(&rt, Some(0), valid.clone());

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    // A userID-bearing (JWT) connection — the case revalidation applies to.
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    // TS validates at initConnection, not at socket accept
    // (connection-context-manager.ts:267 registers with
    // `revalidateAt: undefined`); arm revalidation the way TS does.
    validate_test_connection(&rt, &mut state, "c1", "ws1");
    assert_eq!(state.registered_ws.len(), 1);

    // Arming happened on connect (interval set + a token present).
    assert!(state.next_auth_maintenance_at.is_some());

    // The token expires; the next maintenance tick must drop the connection.
    valid.store(false, Ordering::SeqCst);
    rt.block_on(state.run_auth_maintenance());

    assert_eq!(
        state.registered_ws.len(),
        0,
        "expired connection must be closed"
    );
    assert!(
        ccm_raw_auth(&state, "c1", "ws1").is_none(),
        "closed connection's auth must be gone from the CCM"
    );
    assert_eq!(state.metrics.snapshot()["authRevalidationFailures"], 1);
    // No authed connection remains → disarmed.
    assert!(state.next_auth_maintenance_at.is_none());
}

/// initConnection with NO profileID must default the CVR profileID to
/// `cg{clientGroupID}` — TS view-syncer.ts:862
/// (`connCtx.profileID ?? `cg${this.id}``). Ported from
/// view-syncer.pg.test.ts "initConnectionMessage with no profileID sets a
/// default profileID based on the client group ID".
#[test]
fn absent_profile_id_defaults_to_cg_client_group_id() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    // authed_params → test_params, which sets profile_id = None,
    // client_group_id = "cg1".
    rt.block_on(state.on_new_connection(
        authed_params("c1", "ws1", "tok-c1"),
        DirectWebSocketSink::new(tx),
    ));

    assert_eq!(
        state.client_profile_ids.get("c1").map(String::as_str),
        Some("cgcg1"),
        "absent profileID must default to cg{{clientGroupID}}"
    );
}

/// A profileID supplied in the connection URL is used verbatim (the default
/// only applies when absent).
#[test]
fn present_profile_id_is_used_verbatim() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let mut params = authed_params("c1", "ws1", "tok-c1");
    params.profile_id = Some("p-explicit".to_string());
    rt.block_on(state.on_new_connection(params, DirectWebSocketSink::new(tx)));

    assert_eq!(
        state.client_profile_ids.get("c1").map(String::as_str),
        Some("p-explicit"),
        "explicit profileID must be used verbatim, not defaulted"
    );
}

/// A `deleteClients` frame arriving on a SUPERSEDED (old) wsID must be
/// ignored — the stale-frame guard drops it, so the targeted client is not
/// deleted. Ports view-syncer.pg.test.ts "ignores deleteClients from old
/// wsID".
#[test]
fn delete_clients_from_stale_ws_id_is_ignored() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let cell = shared(revalidate_state(&rt, Some(300_000), valid));

    // Connect "foo" on ws1, then reconnect "foo" on ws2 (supersedes ws1).
    let (tx1, _d1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let _ = rt.block_on(cell.borrow_mut().on_new_connection(
        authed_params("foo", "ws1", "tok"),
        DirectWebSocketSink::new(tx1),
    ));
    let (tx2, _d2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let _ = rt.block_on(cell.borrow_mut().on_new_connection(
        authed_params("foo", "ws2", "tok"),
        DirectWebSocketSink::new(tx2),
    ));
    assert_eq!(
        cell.borrow().registered_ws.get("foo").map(String::as_str),
        Some("ws2"),
        "reconnect should supersede ws1 with ws2"
    );

    // deleteClients targeting "foo" arrives on the STALE ws1 → must be dropped.
    rt.block_on(on_inbound(
        &cell,
        "foo".into(),
        "ws1".into(),
        r#"["deleteClients",{"clientIDs":["foo"]}]"#.to_string(),
    ));

    // The stale frame was ignored: "foo" is still registered (on ws2), not
    // deleted.
    assert_eq!(
        cell.borrow().registered_ws.get("foo").map(String::as_str),
        Some("ws2"),
        "deleteClients from a stale wsID must not delete the client"
    );
}

/// `activeClients` GC: any CVR client absent from the active set is selected
/// for removal (its queries are then inactivated + TTL-expired). Ports the
/// selection core of view-syncer.pg.test.ts "activeClients inactivates queries
/// from inactive clients". The inactivate→expire chain itself is covered by
/// `delete_clients_removes_client_and_acks` +
/// `expired_query_is_removed_after_ttl_elapses`.
#[test]
fn active_clients_gc_selects_clients_not_in_set() {
    let cvr_clients = vec![
        "clientA".to_string(),
        "clientB".to_string(),
        "clientC".to_string(),
    ];
    // activeClients = [A, B] → C is inactive and must be removed.
    let del = clients_to_delete(
        &cvr_clients,
        Some(&["clientA".to_string(), "clientB".to_string()]),
        &[],
    );
    assert_eq!(del, vec!["clientC".to_string()]);
}

/// activeClients GC unions with explicit deletions, without duplicating a
/// client already selected by the GC.
#[test]
fn active_clients_gc_unions_explicit_deletions() {
    let cvr_clients = vec![
        "clientA".to_string(),
        "clientB".to_string(),
        "clientC".to_string(),
    ];
    // active = [A]; explicit delete of B and C (C also GC-selected → no dup).
    let del = clients_to_delete(
        &cvr_clients,
        Some(&["clientA".to_string()]),
        &["clientB".to_string(), "clientC".to_string()],
    );
    assert_eq!(del, vec!["clientB".to_string(), "clientC".to_string()]);
}

/// No `activeClients` → no GC; only explicit deletions are removed.
#[test]
fn no_active_clients_means_only_explicit_deletions() {
    let cvr_clients = vec!["clientA".to_string(), "clientB".to_string()];
    assert!(clients_to_delete(&cvr_clients, None, &[]).is_empty());
    assert_eq!(
        clients_to_delete(&cvr_clients, None, &["clientB".to_string()]),
        vec!["clientB".to_string()]
    );
}

/// A CVR written by a NEWER replica than the one we serve must produce the
/// exact TS ClientNotFound message (view-syncer.pg.test.ts "sends reset for
/// CVR from older replica version up"). This drives the client to wipe local
/// state and re-sync fresh rather than reconnect elsewhere.
#[test]
fn older_replica_error_matches_ts_message() {
    let mut cvr = empty_cvr("cg1", "01");
    // A synced CVR (state_version != "00") from replica "101" > our "01".
    cvr.version.state_version = "07".to_string();
    cvr.replica_version = Some("101".to_string());
    assert_eq!(
        older_replica_error(&cvr, "01").as_deref(),
        Some("Cannot sync from older replica: CVR=101, DB=01"),
    );
}

/// No error when the replica is the same/older, or the CVR is brand new
/// (state_version "00", never synced — exempt even if its replica is newer).
#[test]
fn older_replica_error_none_when_not_older() {
    // Same replica version → safe.
    let mut same = empty_cvr("cg1", "01");
    same.version.state_version = "07".to_string();
    same.replica_version = Some("01".to_string());
    assert!(older_replica_error(&same, "01").is_none());

    // Brand-new CVR (state_version "00") is exempt even from a newer replica.
    let mut fresh = empty_cvr("cg1", "01");
    fresh.replica_version = Some("101".to_string());
    assert_eq!(fresh.version.state_version, "00");
    assert!(older_replica_error(&fresh, "01").is_none());
}

/// updateAuth with a refreshed OPAQUE token must re-transform (opaque tokens
/// carry no claims, so the change must be detected on the raw token, not
/// decoded claims). Ported from view-syncer.pg.test.ts "retransforms custom
/// queries when opaque auth refreshes". `auth_changes` increments only on the
/// re-transform path, so it is the signal that a re-transform was triggered.
#[test]
fn update_auth_opaque_token_change_retransforms() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    // Opaque token (not a JWT) WITH a userID — the CCM records the token so the
    // unchanged-check compares against a REAL previous opaque auth (this is what
    // makes the raw-vs-decoded distinction non-vacuous: two opaque tokens both
    // decode to `{}`, so a decoded-claims comparison would falsely skip). The
    // group pins to `user-1`; the opaque refresh must NOT be closed by the pin.
    let mut params = authed_params("c1", "ws1", "opaque-token-1");
    params.user_id = Some("user-1".to_string());
    rt.block_on(state.on_new_connection(params, DirectWebSocketSink::new(tx)));
    assert_eq!(state.metrics.snapshot()["authChanges"], 0);

    // Refresh to a DIFFERENT opaque token → must re-transform. `auth_changes`
    // is incremented on the re-transform path (before the config/hydrate),
    // so it is the robust signal that the change was detected — regardless of
    // what the barebones test factory's config/hydrate then does downstream.
    rt.block_on(state.handle_update_auth("c1", "opaque-token-2"));
    assert_eq!(
        state.metrics.snapshot()["authChanges"],
        1,
        "opaque token refresh must trigger a re-transform"
    );
}

/// `RevalidateFactory` with a real table, so a desired-queries pass actually
/// hydrates (the init/sync flag flips only after a successful sync).
struct IssueTableFactory {
    handle: tokio::runtime::Handle,
}
impl CGServicesFactory for IssueTableFactory {
    fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
        None
    }
    fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
        None
    }
    fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
        SyncEngineConfig {
            initialization_error: None,
            tables: vec![issue_table_spec()],
            full_tables: vec![issue_full_table_spec()],
            replica_path: None,
            app_id: "zero".to_string(),
            replica_version: "00".to_string(),
            shard: ShardID {
                app_id: "zero".to_string(),
                shard_num: 0,
            },
            cvr_pg: None,
            permissions: None,
            permissions_hash: None,
            revalidate_interval_ms: None,
            query_config: None,
            enable_query_covering: true,
            enable_query_planner: true,
            priority_op_running_yield_threshold_ms: 2.5,
            normal_yield_threshold_ms: 10.0,
            tokio_handle: self.handle.clone(),
            admin_password: None,
            server_version: "test".to_string(),
            metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
        }
    }
}

/// TS `#runInLockWithCVR` records `zero.sync.lock-wait-time` when the lock
/// is granted (view-syncer.ts:459-461). Rust's lock is the CG message queue:
/// a frame enqueued 40 ms ago records a wait of at least 40 ms when
/// `dispatch_cg_message` picks it up. NON-VACUOUS: before 2026-09-09 no
/// site recorded the instrument — the seam stays `None`.
#[test]
fn lock_wait_time_is_recorded_when_the_cg_dequeues_a_frame() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(IssueTableFactory {
        handle: rt.handle().clone(),
    });
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(ToggleAuthValidator { valid }),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(0)),
    );
    seed_test_client_schema(&mut state);
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    let state_rc = Rc::new(RefCell::new(state));
    let (_cg_tx, mut cg_rx) = tokio::sync::mpsc::unbounded_channel::<CGMessage>();
    let mut stashed = std::collections::VecDeque::new();
    *crate::metrics::LAST_LOCK_WAIT_MS.lock().unwrap() = None;
    let enqueued_at = std::time::Instant::now() - std::time::Duration::from_millis(40);
    rt.block_on(dispatch_cg_message(
        &state_rc,
        &mut cg_rx,
        &mut stashed,
        CGMessage::Inbound {
            client_id: Arc::from("c1"),
            ws_id: Arc::from("ws1"),
            text: r#"["changeDesiredQueries",{"desiredQueriesPatch":[]}]"#.to_string(),
            enqueued_at,
        },
    ));
    let recorded = crate::metrics::LAST_LOCK_WAIT_MS.lock().unwrap().take();
    let recorded = recorded.expect("dequeuing a frame records zero.sync.lock-wait-time");
    assert!(
        recorded >= 40.0,
        "lock wait covers the queue time, got {recorded} ms"
    );
}

/// TS counts `zero.sync.hydration` once per `#addAndRemoveQueries` batch
/// (view-syncer.ts:2093 guard → :2300 `hydrations.add(1)`), never per
/// config pass. Rust counted every `handle_desired_queries` pass, so the
/// `already caught up` no-op passes inflated it: 720 vs TS 396 on an
/// identical 6-connection replay (2026-09-09 collector diff).
///
/// NON-VACUOUS: restore `self.metrics.record_hydration(elapsed_ms)` in
/// `handle_desired_queries` and the no-op pass reads 2, the batch 3.
#[test]
fn hydration_counter_counts_add_batches_not_config_passes_like_ts() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(IssueTableFactory {
        handle: rt.handle().clone(),
    });
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(ToggleAuthValidator { valid }),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(0)),
    );
    seed_test_client_schema(&mut state);
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    let patch = |hashes: &[&str]| {
        let puts: Vec<serde_json::Value> = hashes
            .iter()
            .map(|h| serde_json::json!({"op": "put", "hash": h, "ast": {"table": "issue"}}))
            .collect();
        serde_json::json!({"desiredQueriesPatch": puts})
    };

    // initConnection with one query: one batch → 1.
    assert!(rt.block_on(state.handle_desired_queries(
        "c1",
        &patch(&["q1"]),
        ConfigPassOrigin::InitConnection,
        CustomQueryTransformMode::All,
    )));
    assert_eq!(state.metrics.snapshot()["hydrations"], 1, "one add batch");
    // The same desired set again: nothing to add or remove, so TS never
    // enters `#addAndRemoveQueries` (`already caught up`) → still 1.
    assert!(rt.block_on(state.handle_desired_queries(
        "c1",
        &patch(&["q1"]),
        ConfigPassOrigin::ChangeDesiredQueries,
        CustomQueryTransformMode::Missing,
    )));
    assert_eq!(
        state.metrics.snapshot()["hydrations"],
        1,
        "a no-op config pass is not a hydration"
    );
    // Two new queries in one patch: ONE batch, not two.
    assert!(rt.block_on(state.handle_desired_queries(
        "c1",
        &patch(&["q2", "q3"]),
        ConfigPassOrigin::ChangeDesiredQueries,
        CustomQueryTransformMode::Missing,
    )));
    assert_eq!(
        state.metrics.snapshot()["hydrations"],
        2,
        "a two-query batch counts once"
    );
}

/// NON-VACUOUS (log parity, 2026-09-08): TS logs `init pipelines@…` ONLY on
/// the run loop's `!#pipelinesSynced` path (view-syncer.ts:568-606) — once
/// per pipeline (re)init — never on a later `changeDesiredQueries`
/// (`#syncQueryPipelineSet('missing')`, :644). The GKE sandbox log showed rust
/// printing it on EVERY query-set change (34 lines / 10 min for one client),
/// which reads as a pipeline reset. Revert the `if !self.pipelines_synced`
/// gate around the line in `handle_desired_queries` → the count is 2 → fails.
#[test]
fn init_pipelines_is_logged_once_per_pipeline_init_not_per_query_set_change() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(IssueTableFactory {
        handle: rt.handle().clone(),
    });
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(ToggleAuthValidator { valid }),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(0)),
    );
    seed_test_client_schema(&mut state);
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    let put = |hash: &str| {
        serde_json::json!({"desiredQueriesPatch": [
            {"op": "put", "hash": hash, "ast": {"table": "issue"}}
        ]})
    };

    let logged = {
        use std::sync::{Arc, Mutex};
        #[derive(Clone)]
        struct CapWriter(Arc<Mutex<Vec<u8>>>);
        struct CapGuard(Arc<Mutex<Vec<u8>>>);
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapWriter {
            type Writer = CapGuard;
            fn make_writer(&'a self) -> CapGuard {
                CapGuard(self.0.clone())
            }
        }
        impl std::io::Write for CapGuard {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(CapWriter(buf.clone()))
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        crate::ensure_permissive_global_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            // initConnection → first sync → pipelines (re)init → the line.
            let accepted = rt.block_on(state.handle_desired_queries(
                "c1",
                &put("q1"),
                ConfigPassOrigin::InitConnection,
                CustomQueryTransformMode::All,
            ));
            assert!(accepted, "initConnection pass must be accepted");
            // changeDesiredQueries → `'missing'` sync → no line.
            let accepted = rt.block_on(state.handle_desired_queries(
                "c1",
                &put("q2"),
                ConfigPassOrigin::ChangeDesiredQueries,
                CustomQueryTransformMode::Missing,
            ));
            assert!(accepted, "changeDesiredQueries pass must be accepted");
        });
        String::from_utf8(buf.lock().unwrap().clone()).unwrap()
    };
    assert!(
        state.pipelines_synced,
        "the passes must have synced the pipeline set"
    );
    let cvr = state.cvr.as_ref().expect("cvr");
    assert!(
        cvr.queries.contains_key("q1") && cvr.queries.contains_key("q2"),
        "both passes must have run; cvr queries: {:?}",
        cvr.queries.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        logged.matches("init pipelines@").count(),
        1,
        "`init pipelines@` once per pipeline init (TS view-syncer.ts:590); got:\n{logged}"
    );
}

/// TS `#runInLockForClient` catches a hydrate throw and fails the REQUESTING
/// CONNECTION only — `closing connection with error` + `failConnection` +
/// `client.fail(e)` (view-syncer.ts:1236-1249). `#addQueryImpl` logs
/// `query-pipeline-hydrate-failed` and RETHROWS (pipeline-driver.ts:794-812),
/// so the throw lands in that catch, never in the run loop's `#cleanup(err)`
/// — the only TS path that fails every client of a group.
///
/// Rust called `fail_group`, so ONE client's unhydratable query evicted every
/// client of the group and terminated the CG thread. Found by the G44 runtime
/// log differential (2026-09-08): the same replay produced 47
/// `terminating after fatal synchronization error` on rust against 47 per-query
/// `query hydration failed` on TS, triggered by a filter value SQLite cannot
/// parse reaching the planner's inlined probe SQL.
///
/// NON-VACUOUS: restore `self.fail_group(&e.to_string())` and the sibling
/// assertions fail — c2 receives an error frame and the group goes terminal.
#[test]
fn hydrate_failure_fails_only_the_requesting_connection_like_ts() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);
    seed_test_client_schema(&mut state);

    let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx1),
    ));
    rt.block_on(state.on_new_connection(
        pinned_params("c2", "ws2", "user-1"),
        DirectWebSocketSink::new(tx2),
    ));
    // Both sockets complete initConnection (TS view-syncer.ts:914) so both are
    // real clients of the group; only c1 then asks for the failing query.
    assert!(state.init_connection("c1", "ws1"));
    assert!(state.init_connection("c2", "ws2"));
    let _ = error_bodies(&mut rx1);
    let _ = error_bodies(&mut rx2);

    // The next hydrate answers with the production failure text.
    force_hydrate_error(JsError::new(
        "SqliteError",
        "probe SQL contains NUL byte: SELECT \"_0_version\",\"boardId\" FROM \"stages\"",
    ));
    use super::engine_tests::{capture_logs, captured};
    let (logs, accepted) = {
        let (buf, _guard) = capture_logs(tracing::Level::DEBUG);
        let accepted = rt.block_on(state.handle_desired_queries(
            "c1",
            &serde_json::json!({
                "desiredQueriesPatch": [
                    {"op": "put", "hash": "q-unhydratable", "ast": {"table": "issue"}}
                ]
            }),
            ConfigPassOrigin::ChangeDesiredQueries,
            CustomQueryTransformMode::Missing,
        ));
        (captured(&buf), accepted)
    };
    assert!(
        !accepted,
        "a failed hydrate must not report an accepted pass"
    );

    // TS emits THREE lines for one failed client op, at two different levels,
    // and the split is the point (see `Connection::fail`):
    //   1. `closing connection with error`  @ getLogLevel(e)      = ERROR
    //      (`#runInLockForClient`'s catch, view-syncer.ts:1243)
    //   2. `view-syncer closing connection with error: <e>` @ same = ERROR
    //      (`ClientHandler.fail`, client-handler.ts:176)
    //   3. `Sending error on WebSocket`     @ the WRAPPED level   = WARN
    //      (`sendError`, connection.ts:429 — `#closeWithThrown` receives
    //      `wrapWithProtocolError(e)`, so `isProtocolError` → warn)
    // Rust emitted only 1 and 3: measured 47 against TS's 92 on the
    // 2026-09-08 G44 runtime log differential.
    let line_at = |level: &str, msg: &str| {
        logs.lines()
            .filter(|l| l.contains(level) && l.contains(msg))
            .count()
    };
    assert_eq!(
        line_at("ERROR", "closing connection with error"),
        2,
        "both TS `closing connection with error` lines must be ERROR \
             (view-syncer.ts:1243 and client-handler.ts:176); got:\n{logs}"
    );
    assert_eq!(
        line_at(
            "ERROR",
            "view-syncer closing connection with error: SqliteError: probe SQL contains NUL byte"
        ),
        1,
        "TS `ClientHandler.fail` logs `String(e)` — the RAW hydrate error with its \
             class name (`db.prepare` throws better-sqlite3's SqliteError, \
             sqlite-cost-model.ts:78), not the wrapped body; got:\n{logs}"
    );
    assert_eq!(
        line_at("WARN", "Sending error on WebSocket"),
        1,
        "the frame's level comes from the WRAPPED ProtocolError, so it stays \
             WARN while the two lines above are ERROR; got:\n{logs}"
    );

    // The requesting connection is failed, exactly as `client.fail(e)` does.
    let c1_errors = error_bodies(&mut rx1);
    assert!(
        !c1_errors.is_empty(),
        "the requesting connection must receive the error frame"
    );
    assert!(
        !state.connections.contains_key("c1"),
        "the requesting connection must be torn down"
    );

    // The sibling keeps serving: TS never touches it.
    assert!(
        error_bodies(&mut rx2).is_empty(),
        "a sibling connection must NOT be failed by another client's bad query"
    );
    assert!(
        state.connections.contains_key("c2"),
        "the sibling connection must stay open"
    );
    assert!(
        !state.terminal,
        "the view-syncer must keep running (TS fails the client, not the group)"
    );
    assert!(
        state.accepting.load(Ordering::SeqCst),
        "the group must keep accepting new connections"
    );
}

/// NON-VACUOUS (fix, 2026-09-02): `updateAuth` and the background retransform
/// carry an EMPTY desired-queries body, and both MUST still run the
/// config/hydrate pass — re-transforming every query under the refreshed
/// credential is their whole purpose (TS `updateAuth` -> `#handleConfigUpdate`
/// with `'all'`, view-syncer.ts:1019-1031; `#runBackgroundRetransform` ->
/// `#syncQueryPipelineSet('all')`, view-syncer.ts:2670).
///
/// The early return that skips a no-op pass must therefore key off
/// `forces_config_pass()`, NOT `is_init_connection()`. Narrow it to
/// `is_init` and both paths become silent no-ops — which the
/// `forced_retransform_outcomes` seam cannot catch, because it short-circuits
/// before `handle_desired_queries` is ever called.
#[test]
fn empty_body_config_passes_still_run_for_update_auth_and_retransform() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);
    seed_test_client_schema(&mut state);

    let empty = serde_json::json!({});
    for origin in [
        ConfigPassOrigin::UpdateAuth,
        ConfigPassOrigin::BackgroundRetransform,
    ] {
        let (tx, _d) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        rt.block_on(state.on_new_connection(
            pinned_params("c1", "ws1", "user-1"),
            DirectWebSocketSink::new(tx),
        ));
        // `updateAuth` runs inside `#runInLockForClient`, which honors only
        // a socket that has sent initConnection (view-syncer.ts:914/:1216):
        // register the client the TS way first.
        rt.block_on(state.handle_desired_queries(
            "c1",
            &empty,
            ConfigPassOrigin::InitConnection,
            CustomQueryTransformMode::All,
        ));
        let before = state.config_pass_runs;
        rt.block_on(state.handle_desired_queries(
            "c1",
            &empty,
            origin,
            CustomQueryTransformMode::All,
        ));
        assert_eq!(
            state.config_pass_runs - before,
            1,
            "{origin:?} must run the config/hydrate pass on an empty body"
        );
    }

    // The contrast that makes the gate meaningful: a `changeDesiredQueries`
    // with nothing to change IS a genuine no-op (TS's only entry point that
    // exists solely to carry a query change).
    let (tx, _d) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    let before = state.config_pass_runs;
    rt.block_on(state.handle_desired_queries(
        "c1",
        &empty,
        ConfigPassOrigin::ChangeDesiredQueries,
        CustomQueryTransformMode::Missing,
    ));
    assert_eq!(
        state.config_pass_runs - before,
        0,
        "an empty changeDesiredQueries must stay a no-op"
    );
}

/// NON-VACUOUS (fix, 2026-09-02): `initConnection` validates the connection
/// against the query API server BEFORE any data is sent. TS:
/// ```text
/// // Validate auth before sending any data is sent to this connection.
/// // the #handleConfigUpdate call below will also transform
/// // queries, but that may hit the transform cache so do not rely on
/// // it for validation. ...
/// if (!(await this.#validateConnection(connCtx))) { return; }
/// ```
/// (view-syncer.ts:936-944). Rust did NOT probe here — it recorded a
/// `client-fallback` validation at socket accept and went straight to the
/// config pass, so a token that is cryptographically valid but revoked at the
/// app layer was served data. Delete the `if is_init { ... validate ... }`
/// block in `handle_desired_queries` and the `false` assert below fails
/// (the pass is accepted despite the failing probe).
#[test]
fn init_connection_validates_before_serving_any_data() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);
    seed_test_client_schema(&mut state);

    let (tx, mut drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    // Record a query URL outside the allow-list, the way the handler's
    // `ccm.initConnection` dispatch does. The probe then fails
    // SYNCHRONOUSLY (no network) with a `TransformFailed` quoting the URL —
    // standing in for a token the API server rejects.
    let selector = CcmConnectionSelector {
        client_id: "c1".to_string(),
        ws_id: "ws1".to_string(),
    };
    lock_unpoisoned(&state.ccm)
        .init_connection(
            &selector,
            &InitConnectionBody {
                user_query_url: Some("https://revoked.example/query".to_string()),
                user_query_headers: None,
                user_push_url: None,
                user_push_headers: None,
            },
        )
        .unwrap();

    let accepted = rt.block_on(state.handle_desired_queries(
        "c1",
        &serde_json::json!({"desiredQueriesPatch": []}),
        ConfigPassOrigin::InitConnection,
        CustomQueryTransformMode::All,
    ));
    assert!(
        !accepted,
        "initConnection must NOT proceed to the config/hydrate pass when the \
             connection fails validation (TS returns; view-syncer.ts:942)"
    );

    let err = std::iter::from_fn(|| drx.try_recv().ok()).find_map(|command| match command {
        WsCommand::Send { msg, .. }
            if msg.get(0).and_then(serde_json::Value::as_str) == Some("error") =>
        {
            msg.get(1).cloned()
        }
        _ => None,
    });
    let err = err.expect("a failed validation must reach the client as an error frame");
    assert_eq!(err["kind"], "TransformFailed");
    assert!(
        !state.registered_ws.contains_key("c1"),
        "a connection that failed validation must be closed, not left serving"
    );
}

/// NON-VACUOUS (fix, 2026-09-02): `updateAuth` validates the connection ONLY
/// when pipelines are not yet synced. TS:
/// ```text
/// // If pipelines are not yet synced, there is no transform request that
/// // can absorb validation, so validate immediately.
/// if (!this.#pipelinesSynced) {
///   if (!(await this.#validateConnection(connCtx))) return;
/// }
/// ```
/// (view-syncer.ts:1009-1015). Once synced, the `'all'` re-transform carries
/// the validation and TS deliberately does NOT validate here — validating
/// anyway records a CLIENT-asserted identity instead of the API server's
/// (the hazard view-syncer.ts:1988-1990 calls out).
///
/// Before the fix rust validated on EVERY updateAuth. Revert the
/// `!self.pipelines_synced` gate → the second half's `== 1` assert fails
/// (the counter reaches 2).
/// Drain every error frame a test sink received, as `["error", body]`
/// bodies (a plain `Send`, a `Fail`, or a `FailWithCode`).
fn error_bodies(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<WsCommand>,
) -> Vec<serde_json::Value> {
    std::iter::from_fn(|| rx.try_recv().ok())
        .filter_map(|command| match command {
            WsCommand::Send { msg, .. }
                if msg.get(0).and_then(serde_json::Value::as_str) == Some("error") =>
            {
                msg.get(1).cloned()
            }
            WsCommand::Fail(e) | WsCommand::FailWithCode { error: e, .. } => {
                Some(crate::protocol::error_message(&e)[1].clone())
            }
            _ => None,
        })
        .collect()
}

/// Connect `c1`/`ws1` as `user-1` with `userQueryURL` pointing at `url`.
fn connect_with_query_url(
    rt: &tokio::runtime::Runtime,
    state: &mut ViewSyncerService,
    url: String,
) -> tokio::sync::mpsc::UnboundedReceiver<WsCommand> {
    let (tx, drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    let selector = CcmConnectionSelector {
        client_id: "c1".to_string(),
        ws_id: "ws1".to_string(),
    };
    lock_unpoisoned(&state.ccm)
        .init_connection(
            &selector,
            &InitConnectionBody {
                user_query_url: Some(url),
                user_query_headers: None,
                user_push_url: None,
                user_push_headers: None,
            },
        )
        .unwrap();
    drx
}

/// TS `#validateConnection` (view-syncer.ts:2749-2767) records the API
/// server's `userID` from the `/query` validation response through
/// `connContextManager.validateConnection`; a userID that differs from the
/// connection's is `Unauthorized` (connection-context-manager.ts:420) →
/// `#failMaintenanceConnection`: the client receives the Unauthorized
/// error frame, is closed, and initConnection does NOT proceed. Rust
/// recorded `client-fallback` unconditionally, so a server answering for a
/// DIFFERENT user was silently accepted.
#[test]
fn init_connection_rejects_a_server_user_id_that_does_not_match() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    // Every probe/transform answers for a DIFFERENT user than the JWT's.
    let url = crate::custom_queries::transform_query::test_support::spawn_http_stub_with(8, |_| {
        (
            "200 OK",
            r#"{"kind":"QueryResponse","userID":"someone-else","queries":[]}"#.to_string(),
        )
    });
    let mut state = revalidate_state_with_query_url(&rt, Some(300_000), valid, Some(url.clone()));
    seed_test_client_schema(&mut state);
    let mut drx = connect_with_query_url(&rt, &mut state, url);
    let accepted = rt.block_on(state.handle_desired_queries(
        "c1",
        &serde_json::json!({"desiredQueriesPatch": []}),
        ConfigPassOrigin::InitConnection,
        CustomQueryTransformMode::All,
    ));
    assert!(
        !accepted,
        "a server userID mismatch must reject initConnection (TS validateConnection throws Unauthorized)"
    );
    let errors = error_bodies(&mut drx);
    let err = errors
        .last()
        .cloned()
        .expect("the mismatch must reach the client as an error frame");
    assert_eq!(err["kind"], "Unauthorized", "got {err}");
    assert_eq!(
        err["message"],
        "Connection userID does not match validated server userID."
    );
    assert!(
        !state.registered_ws.contains_key("c1"),
        "the rejected connection must be closed"
    );
}

/// TS `#syncQueryPipelineSet` (view-syncer.ts:1984-1992): after an UNCACHED
/// custom-query transform the connection is validated with the `userID`
/// the API server returned IN THE TRANSFORM RESPONSE. A mismatch is
/// `Unauthorized` and throws: that client is failed and none of the batch
/// is applied. Pins the transform-response path, not the `/query` probe:
/// init validates fine (the server says `user-1`), then a desired-queries
/// change transforms against a server that now answers for someone else.
#[test]
fn desired_queries_transform_rejects_a_server_user_id_that_does_not_match() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    // The empty validation probe answers for user-1 (validates fine); a REAL
    // transform answers for someone else.
    let url = crate::custom_queries::transform_query::test_support::spawn_http_stub_with(
        8,
        |req| {
            if req.contains(r#"["transform",[]]"#) {
                (
                    "200 OK",
                    r#"{"kind":"QueryResponse","userID":"user-1","queries":[]}"#.to_string(),
                )
            } else {
                (
                    "200 OK",
                    r#"{"kind":"QueryResponse","userID":"someone-else","queries":[{"id":"h1","ast":{"table":"issue"}}]}"#.to_string(),
                )
            }
        },
    );
    let mut state = revalidate_state_with_query_url(&rt, Some(300_000), valid, Some(url.clone()));
    seed_test_client_schema(&mut state);
    let mut drx = connect_with_query_url(&rt, &mut state, url);
    let accepted = rt.block_on(state.handle_desired_queries(
        "c1",
        &serde_json::json!({"desiredQueriesPatch": []}),
        ConfigPassOrigin::InitConnection,
        CustomQueryTransformMode::All,
    ));
    assert!(
        accepted,
        "a matching server userID validates initConnection"
    );
    let _ = error_bodies(&mut drx);
    rt.block_on(state.handle_desired_queries(
        "c1",
        &serde_json::json!({"desiredQueriesPatch": [
            {"op": "put", "hash": "h1", "name": "myQuery", "args": []}
        ]}),
        ConfigPassOrigin::ChangeDesiredQueries,
        CustomQueryTransformMode::All,
    ));
    let errors = error_bodies(&mut drx);
    let err = errors
            .iter()
            .find(|e| e["kind"] == "Unauthorized")
            .cloned()
            .unwrap_or_else(|| panic!("the transform-response userID mismatch must fail the client with Unauthorized; got {errors:?}"));
    assert_eq!(
        err["message"],
        "Connection userID does not match validated server userID."
    );
    assert!(
        !state.registered_ws.contains_key("c1"),
        "the rejected connection must be closed"
    );
}

#[test]
fn update_auth_validates_only_when_pipelines_are_not_synced() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let mut params = authed_params("c1", "ws1", "opaque-token-1");
    params.user_id = Some("user-1".to_string());
    rt.block_on(state.on_new_connection(params, DirectWebSocketSink::new(tx)));
    // `updateAuth` runs inside `#runInLockForClient`, which honors only a
    // socket that has sent initConnection (view-syncer.ts:914/:1216) —
    // register the handler the TS way (the prefix only: the barebones
    // factory's config pass would tear the connection down).
    assert!(state.init_connection("c1", "ws1"));

    // Pipelines NOT synced (a connection that has not completed its first
    // sync): updateAuth must validate immediately.
    state.pipelines_synced = false;
    let before = state.validate_connection_runs;
    rt.block_on(state.handle_update_auth("c1", "opaque-token-2"));
    assert_eq!(
        state.validate_connection_runs - before,
        1,
        "with pipelines unsynced, updateAuth must validate immediately \
             (TS view-syncer.ts:1011)"
    );

    // Pipelines synced (steady state): the `'all'` re-transform absorbs the
    // validation, so updateAuth must NOT validate here.
    //
    // A FRESH connection is required: the barebones test factory's
    // config/hydrate fails, which tears the first connection down — without
    // re-registering, `handle_update_auth` would bail at the
    // `registered_ws` lookup and this half would pass vacuously.
    let (tx2, _drx2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let mut params2 = authed_params("c2", "ws2", "opaque-token-1");
    params2.user_id = Some("user-1".to_string());
    rt.block_on(state.on_new_connection(params2, DirectWebSocketSink::new(tx2)));
    assert!(
        state.registered_ws.contains_key("c2"),
        "the second half needs a live connection or it asserts nothing"
    );
    assert!(state.init_connection("c2", "ws2"));
    state.pipelines_synced = true;
    let before = state.validate_connection_runs;
    rt.block_on(state.handle_update_auth("c2", "opaque-token-3"));
    assert_eq!(
        state.validate_connection_runs - before,
        0,
        "with pipelines synced, TS does NOT validate in updateAuth — the \
             re-transform carries it (view-syncer.ts:1009-1015)"
    );
}

/// updateAuth with the SAME opaque token is a no-op (no re-transform) — the
/// raw-token comparison must treat an unchanged token as unchanged.
#[test]
fn update_auth_same_opaque_token_skips_retransform() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    // Opaque token WITH a userID — `resolve_auth` requires a userID whenever a
    // token is present (auth.ts:79-85), so the ConnectionContextManager holds
    // the token and the unchanged-check can compare against it. A pinned group
    // must NOT close an opaque refresh (opaque tokens carry no `sub`).
    let mut params = authed_params("c1", "ws1", "opaque-token-1");
    params.user_id = Some("user-1".to_string());
    rt.block_on(state.on_new_connection(params, DirectWebSocketSink::new(tx)));

    rt.block_on(state.handle_update_auth("c1", "opaque-token-1"));
    assert_eq!(
        state.metrics.snapshot()["authChanges"],
        0,
        "an unchanged opaque token must NOT trigger a re-transform"
    );
    // Passing for the RIGHT reason: the connection SURVIVES (an unchanged skip,
    // not a pin-mismatch close). Before the opaque sub-pin fix, a pinned group
    // closed the connection here — which also read authChanges==0, masking the
    // divergence.
    assert_eq!(
        state.registered_ws.len(),
        1,
        "an unchanged opaque refresh must keep the connection open"
    );
}

/// I-8: the message handler's connection-context dispatch is backed by the
/// ported CCM (`CcmDispatchAdapter`), NOT the `auth:None` placeholder — so the
/// handler's live reads (mutagen-CRUD auth + relayed-push auth) see the single
/// owner's real per-connection auth. Pins that the adapter surfaces the CCM's
/// auth + revision, and returns `None` for an unknown connection.
///
/// NON-VACUOUS: the old `PlaceholderConnContextManager` returned `auth:None`
/// unconditionally — this asserts a NON-None token, so wiring the placeholder
/// (or breaking the adapter's `auth` mapping) fails the first assert.
#[test]
fn ccm_dispatch_adapter_surfaces_real_connection_auth() {
    use crate::services::view_syncer::connection_context_manager::{
        Auth, ConnectionContextManager,
    };
    let ccm = Arc::new(Mutex::new(ConnectionContextManager::new(
        None, None, None, None, None, None,
    )));
    let reg = ConnectParamsForRegistration {
        client_id: "c1".to_string(),
        ws_id: "ws1".to_string(),
        user_id: Some("user-1".to_string()),
        profile_id: None,
        base_cookie: None,
        protocol_version: 1,
        http_cookie: None,
        origin: None,
        request_headers: Vec::new(),
    };
    lock_unpoisoned(&ccm).register_connection(
        &CcmConnectionSelector {
            client_id: "c1".to_string(),
            ws_id: "ws1".to_string(),
        },
        &reg,
        Some(Auth::Opaque {
            raw: "the-token".to_string(),
        }),
    );

    let adapter = CcmDispatchAdapter::new(ccm);
    let info = adapter
        .must_get_connection_context(&ConnectionSelector {
            client_id: "c1".to_string(),
            ws_id: "ws1".to_string(),
        })
        .expect("registered connection must resolve");
    assert_eq!(
        info.auth.as_deref(),
        Some("the-token"),
        "adapter must surface the CCM's real auth, not the placeholder None"
    );

    // Unknown connection → the TS `mustGetConnectionContext` THROW
    // (InvalidConnectionRequest), NOT a defaulted `auth: None`. The old
    // "safe default" here is exactly what relayed Authorization-less
    // pushes in prod (2026-08-29 "No token provided" 401s).
    let missing = adapter
        .must_get_connection_context(&ConnectionSelector {
            client_id: "nope".to_string(),
            ws_id: "ws1".to_string(),
        })
        .expect_err("missing connection context must be an error, never a default");
    assert_eq!(
        *missing.kind(),
        crate::protocol::ErrorKind::InvalidConnectionRequest,
        "must mirror the TS mustGetConnectionContext ProtocolError kind"
    );
}

/// Regression (push-relay 401, prod incident 2026-08-27): the token forwarded
/// on relayed custom-mutation pushes MUST track `updateAuth`. Rust once
/// snapshotted the connect-time token and never refreshed it → the API server
/// 401'd every mutation on any session longer than the token TTL.
///
/// After the I-8 push-relay flip there is NO parallel auth cell: every relay
/// fills `PushRelayHeaders.auth` fresh from `mustGetConnectionContext(selector)
/// .auth` (handler `relay_headers_for` / router deleteClients cleanup / the
/// `CcmDispatchAdapter`), so the forwarded token is whatever the CCM — the
/// single owner — currently holds. This asserts exactly that value across an
/// `updateAuth`.
///
/// NON-VACUOUS: `updateAuth` (via the CCM) stores the new token; the relay
/// reads the CCM at use time, so a broken adapter/mapping or a CCM that failed
/// to refresh keeps forwarding the connect-time token and the second assert
/// fails. (The `updateAuth` re-transform is exercised via the CCM directly
/// because the storeless harness tears the connection down on a full
/// `handle_update_auth` re-hydrate — a harness limitation, not the relay path.)
#[test]
fn update_auth_refreshes_the_forwarded_push_relay_token() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));

    // The value the relay forwards == the CCM's current auth (what the handler's
    // `relay_headers_for` / the router deleteClients cleanup read).
    assert_eq!(
        ccm_raw_auth(&state, "c1", "ws1").as_deref(),
        Some(fake_jwt("user-1").as_str()),
        "initial forwarded push token is the connect-time token"
    );

    // A refreshed token (same user, newer iat) flows through `updateAuth` into
    // the CCM; the relay then forwards the NEW token on subsequent pushes.
    let token2 = {
        use base64::Engine;
        let payload = serde_json::json!({"sub": "user-1", "iat": 2}).to_string();
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
        format!("hdr.{b64}.sig")
    };
    let _ = lock_unpoisoned(&state.ccm).update_auth(
        &CcmConnectionSelector {
            client_id: "c1".to_string(),
            ws_id: "ws1".to_string(),
        },
        &UpdateAuthBody {
            auth: Some(token2.clone()),
        },
    );
    assert_eq!(
        ccm_raw_auth(&state, "c1", "ws1").as_deref(),
        Some(token2.as_str()),
        "updateAuth must refresh the token the relay forwards \
             (a stale snapshot is what caused the API-server 401 storm)"
    );
}

/// The ConnectionContextManager owns per-connection auth: `registerConnection`
/// seeds the connect-time token, `updateAuth` refreshes it, and
/// `closeConnection` drops the entry (no leaked auth — the bug-2 soil). Pins
/// that the seeded auth equals the connect token and survives a token refresh.
///
/// NON-VACUOUS: registering with `auth: None` fails the seeded-auth assert;
/// skipping the `close_connection` on teardown fails the final `is_err`.
#[test]
fn connection_context_manager_tracks_register_update_and_close() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    // Authed, user-pinned connection (`resolve_auth` requires a userID when a
    // token is present — TS auth.ts:79-85).
    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));

    let selector = CcmConnectionSelector {
        client_id: "c1".to_string(),
        ws_id: "ws1".to_string(),
    };
    assert!(
        state
            .ccm
            .lock()
            .unwrap()
            .must_get_connection_context(&selector)
            .is_ok(),
        "on_new_connection must register the connection in the CCM"
    );

    // The connect-time auth is seeded from the connect token.
    let seeded = state
        .ccm
        .lock()
        .unwrap()
        .must_get_connection_context(&selector)
        .unwrap()
        .auth
        .map(|a| a.raw().to_string());
    assert!(
        seeded.is_some(),
        "connect-time auth must be seeded at register"
    );
    assert_eq!(
        seeded.as_deref(),
        Some(fake_jwt("user-1").as_str()),
        "seeded CCM auth must equal the connect token"
    );

    // A refreshed token for the SAME user (distinct raw) flows through
    // `updateAuth`. We call it directly rather than the full
    // `handle_update_auth`, whose no-PG re-transform (`ensure_cvr`
    // ClientNotFound) tears the connection down — that teardown is asserted
    // below.
    let token2 = {
        use base64::Engine;
        let payload = serde_json::json!({"sub": "user-1", "iat": 2}).to_string();
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
        format!("hdr.{b64}.sig")
    };
    let _ = state.ccm.lock().unwrap().update_auth(
        &selector,
        &UpdateAuthBody {
            auth: Some(token2.clone()),
        },
    );
    let raw = state
        .ccm
        .lock()
        .unwrap()
        .must_get_connection_context(&selector)
        .expect("still registered")
        .auth
        .map(|a| a.raw().to_string());
    assert_eq!(
        raw.as_deref(),
        Some(token2.as_str()),
        "updateAuth must refresh the CCM's auth token"
    );

    // A teardown drops the connection from the CCM (no leaked auth).
    state.delete_client_due_to_disconnect("c1", "ws1");
    assert!(
        state
            .ccm
            .lock()
            .unwrap()
            .must_get_connection_context(&selector)
            .is_err(),
        "delete_client_due_to_disconnect must drop the connection from the CCM"
    );
}

/// The permission `authData` is read from the ConnectionContextManager at use
/// time (TS `mustGetConnectionContext(selector).auth?.raw`, decoded), not from
/// a separate cache. For a JWT connection the CCM-derived claims must carry the
/// token's `sub` and equal the decoded connect token — read-permission
/// evaluation is unchanged.
///
/// NON-VACUOUS: a CCM returning no auth yields `{}` instead of
/// `{sub:"user-1"}`, failing the assert.
#[test]
fn authdata_reads_from_connection_context_manager() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));

    let via_ccm = state
        .ccm
        .lock()
        .unwrap()
        .must_get_connection_context(&CcmConnectionSelector {
            client_id: "c1".to_string(),
            ws_id: "ws1".to_string(),
        })
        .unwrap()
        .auth
        .map(|a| crate::auth::jwt::decode_jwt_claims(a.raw()))
        .unwrap_or_else(|| serde_json::json!({}));
    assert_eq!(
        via_ccm.get("sub").and_then(|v| v.as_str()),
        Some("user-1"),
        "authData from the CCM must carry the JWT sub"
    );
    let token = ccm_raw_auth(&state, "c1", "ws1").unwrap();
    assert_eq!(
        via_ccm,
        crate::auth::jwt::decode_jwt_claims(&token),
        "CCM authData must equal the decoded connect token"
    );
}

/// A still-valid token survives the tick and the deadline is re-armed for the
/// next interval.
#[test]
fn periodic_revalidation_keeps_valid_connection_and_rearms() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    // Interval 0 → due immediately at the manual tick (see the expired test).
    let mut state = revalidate_state(&rt, Some(0), valid);

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    // A userID-bearing (JWT) connection — the case revalidation applies to;
    // `resolve_auth` requires a userID with a token (auth.ts:79-85), so the
    // ConnectionContextManager holds its auth.
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    // TS validates at initConnection, not at socket accept
    // (connection-context-manager.ts:267 registers with
    // `revalidateAt: undefined`); arm revalidation the way TS does.
    validate_test_connection(&rt, &mut state, "c1", "ws1");
    seed_test_client_schema(&mut state);
    let armed_before = state.next_auth_maintenance_at;
    assert!(armed_before.is_some());

    rt.block_on(state.run_auth_maintenance());

    assert_eq!(
        state.registered_ws.len(),
        1,
        "valid connection must survive"
    );
    assert_eq!(state.metrics.snapshot()["authRevalidations"], 1);
    // Re-armed (still a token present).
    assert!(state.next_auth_maintenance_at.is_some());
}

/// Auth maintenance reads the token from the ConnectionContextManager — the
/// single owner of per-connection auth. Arming + revalidation are driven
/// entirely from the CCM (there is no separate auth map anymore).
///
/// NON-VACUOUS: if arming/revalidation did not read the CCM, the connection's
/// auth would be invisible → no arm, no revalidation, and both asserts fail.
#[test]
fn auth_maintenance_reads_token_from_the_connection_context_manager() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    // Interval 0 → due immediately at the manual tick (see the expired test).
    let mut state = revalidate_state(&rt, Some(0), valid);

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    // TS validates at initConnection, not at socket accept
    // (connection-context-manager.ts:267 registers with
    // `revalidateAt: undefined`); arm revalidation the way TS does.
    validate_test_connection(&rt, &mut state, "c1", "ws1");
    seed_test_client_schema(&mut state);

    state.next_auth_maintenance_at = None;
    state.schedule_auth_maintenance();
    assert!(
        state.next_auth_maintenance_at.is_some(),
        "schedule_auth_maintenance must see the connection's auth via the CCM"
    );

    rt.block_on(state.run_auth_maintenance());
    assert_eq!(
        state.registered_ws.len(),
        1,
        "the (valid) connection must be revalidated and survive"
    );
    assert_eq!(state.metrics.snapshot()["authRevalidations"], 1);
}

/// With the feature disabled (interval None) no deadline is ever armed. An
/// UNTOKENED (cookie/anonymous) connection, however, IS scheduled: TS
/// `validateConnection` stamps `revalidateAt` on every validated connection
/// regardless of token presence, and maintenance re-validates it via the
/// server-side probe (`#validateConnection` → transformer.validate).
///
/// NON-VACUOUS for the plan-driven migration: the previous interval-driven
/// arm skipped untokened connections entirely, so the `is_some` assert below
/// fails against the old code.
#[test]
fn periodic_revalidation_disabled_never_arms_but_unauthed_is_scheduled() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));

    // Disabled: interval None.
    let mut disabled = revalidate_state(&rt, None, valid.clone());
    let (tx, _d) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(disabled.on_new_connection(
        authed_params("c1", "ws1", "tok"),
        DirectWebSocketSink::new(tx),
    ));
    // TS validates at initConnection, not at socket accept
    // (connection-context-manager.ts:267 registers with
    // `revalidateAt: undefined`); arm revalidation the way TS does.
    validate_test_connection(&rt, &mut disabled, "c1", "ws1");
    assert!(disabled.next_auth_maintenance_at.is_none());
    assert!(disabled.next_auth_maintenance_delay().is_none());

    // Enabled + no token → still validated, still scheduled (TS parity).
    let mut unauthed = revalidate_state(&rt, Some(300_000), valid);
    let (tx2, _d2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(
        unauthed.on_new_connection(test_params("c2", "ws2"), DirectWebSocketSink::new(tx2)),
    );
    validate_test_connection(&rt, &mut unauthed, "c2", "ws2");
    assert!(
        unauthed.next_auth_maintenance_at.is_some(),
        "a validated cookie connection gets a revalidate deadline (TS \
             validateConnection stamps revalidateAt unconditionally)"
    );
}

/// The tick executes the CCM's PLAN, not a flat re-check of every
/// connection: with a 300s revalidate interval, a tick fired right after
/// connect finds nothing due and touches nothing.
///
/// NON-VACUOUS: the previous interval-driven tick revalidated every tokened
/// connection on ANY tick, so `authRevalidations == 0` fails against it.
#[test]
fn maintenance_honors_ccm_revalidate_deadlines() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    // TS validates at initConnection, not at socket accept
    // (connection-context-manager.ts:267 registers with
    // `revalidateAt: undefined`); arm revalidation the way TS does.
    validate_test_connection(&rt, &mut state, "c1", "ws1");
    seed_test_client_schema(&mut state);
    assert!(state.next_auth_maintenance_at.is_some());

    // Fire the tick 300s EARLY: the connection's `revalidate_at` is not due,
    // so no revalidation work runs and the connection is untouched.
    rt.block_on(state.run_auth_maintenance());
    assert_eq!(state.registered_ws.len(), 1);
    assert_eq!(state.metrics.snapshot()["authRevalidations"], 0);
    // Still armed for the real deadline.
    assert!(state.next_auth_maintenance_at.is_some());
}

/// Single-user pin: once a group is pinned to user-1, an `updateAuth` bearing
/// a validly-formed token for a DIFFERENT user (user-2) must be REJECTED and
/// the connection closed — a group cannot be re-scoped to another user
/// mid-connection. Port of `pickToken`'s "pinned to a single user" rule.
#[test]
fn update_auth_rejects_cross_user_token() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    assert_eq!(state.pinned_user_id.as_deref(), Some("user-1"));
    assert_eq!(state.registered_ws.len(), 1);

    // updateAuth with a token for user-2 → rejected, connection closed.
    rt.block_on(state.handle_update_auth("c1", &fake_jwt("user-2")));
    assert_eq!(
        state.registered_ws.len(),
        0,
        "cross-user updateAuth must close the connection"
    );
    assert_eq!(state.metrics.snapshot()["authRevalidationFailures"], 1);
}

/// The pin allows an `updateAuth` whose token stays on the SAME user.
#[test]
fn update_auth_accepts_same_user_token() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let mut state = revalidate_state(&rt, Some(300_000), valid);

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));

    // Same-user token → not rejected (connection stays open).
    rt.block_on(state.handle_update_auth("c1", &fake_jwt("user-1")));
    assert_eq!(
        state.registered_ws.len(),
        1,
        "same-user updateAuth must keep the connection open"
    );
}

/// The CG event loop: a new connection registers a client with the
/// SyncEngine; a notification with no CVR is graceful; a disconnect
/// unregisters the client. Runs on the test thread (not a tokio worker), so
/// the sink's `blocking_send` is legal.
///
/// Note: `on_new_connection` (the SERIAL CG-thread path) must NOT emit
/// `connected` — that message is sent on the accept task
/// (`handle_connection`, TS `syncer.ts#handleConnection`) so the connect-ack
/// is never queued behind an in-flight `config_and_hydrate`. This test pins
/// that: registration happens here, `connected` does not.
#[test]
fn cg_state_connection_lifecycle_and_notification() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let global = Arc::new(Mutex::new(HashMap::new()));
    let count = Arc::new(AtomicU64::new(0));
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        global,
        count,
    );

    let (tx, mut drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink = DirectWebSocketSink::new(tx);
    rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));

    // The CG-thread path registers the client but does NOT emit `connected`
    // (that is the accept task's job — see `handle_connection`).
    let mut connected = false;
    while let Ok(cmd) = drx.try_recv() {
        if let Some(v) = cmd.frame_value()
            && v[0] == "connected"
        {
            connected = true;
        }
    }
    assert!(
        !connected,
        "on_new_connection (CG thread) must NOT send `connected`; the accept \
             task does (decoupling the ack from config_and_hydrate)"
    );
    assert_eq!(state.registered_ws.len(), 1);
    assert_eq!(state.connections.len(), 1);

    // Notification with no loaded CVR (no PG) is a graceful no-op.
    rt.block_on(state.on_notification(serde_json::json!({"state": "version-ready"})));

    // Disconnect unregisters the client.
    state.delete_client_due_to_disconnect("c1", "ws1");
    assert_eq!(state.registered_ws.len(), 0);
    assert_eq!(state.connections.len(), 0);
}

/// L7 cancel-during-hydrate teardown completeness (view-syncer.ts:916 —
/// closes its GAP). TS returns `downstream` synchronously from
/// `initConnection` so a close arriving DURING hydrate can still cancel the
/// subscription "even if #runInLockForClient() has not had a chance to run."
/// In rust that close reaches the serial CG thread as a `ConnectionClosed`
/// enqueued AFTER `NewConnection` (FIFO), so it is processed once the blocked
/// hydrate releases — that the serial channel DOES process a message queued
/// behind a blocked hydrate is the other half, proven by
/// `connected_ack_is_decoupled_from_a_blocked_cg_hydrate`. This test pins
/// THIS half: when that close finally runs, `delete_client_due_to_disconnect` must fully
/// tear the client down. No per-client state (auth, raw auth, query ctx, push
/// headers, profile id, base version, sink registration) may leak — a leak
/// would let a reconnecting client or a later relayed push read stale auth
/// (the bug-2 class this framework exists to kill).
///
/// NON-VACUOUS: delete any single `self.<map>.remove(client_id)` line from
/// `delete_client_due_to_disconnect` and the matching `is_empty()` assertion fails.
#[test]
fn a_close_fully_tears_down_all_per_client_state() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let count = Arc::new(AtomicU64::new(0));
    let mut state = ViewSyncerService::new_test(
        "cg-teardown",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        Arc::new(Mutex::new(HashMap::new())),
        count,
    );

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink = DirectWebSocketSink::new(tx);
    rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));

    // Simulate a fully-hydrated client: seed the remaining per-client state
    // the way a completed initConnection would (connections/registered_ws are
    // already set by on_new_connection; the per-connection auth/query context
    // lives in the ConnectionContextManager, registered by on_new_connection).
    state
        .client_profile_ids
        .insert("c1".into(), "profile-1".into());
    let sel = CcmConnectionSelector {
        client_id: "c1".to_string(),
        ws_id: "ws1".to_string(),
    };
    assert!(
        lock_unpoisoned(&state.ccm)
            .get_connection_context(&sel)
            .is_some()
            && !state.connections.is_empty()
    );

    // The mid-hydrate cancel, delivered to the serial thread after the
    // hydrate as `ConnectionClosed`.
    state.delete_client_due_to_disconnect("c1", "ws1");

    assert!(state.connections.is_empty(), "connections leaked");
    assert!(state.registered_ws.is_empty(), "registered_ws leaked");
    assert!(
        lock_unpoisoned(&state.ccm)
            .get_connection_context(&sel)
            .is_none(),
        "ConnectionContextManager leaked — stale per-connection auth/query \
             context survives close"
    );
    assert!(
        state.client_push_headers.is_empty(),
        "client_push_headers leaked — stale relay headers survive close"
    );
    assert!(
        state.client_profile_ids.is_empty(),
        "client_profile_ids leaked"
    );
    assert!(
        state.client_base_versions.is_empty(),
        "client_base_versions leaked"
    );
}

/// Port fidelity for the `#expiredQueriesTimer` / `#scheduleExpireEviction` /
/// `#stopExpireTimer` trio (view-syncer.ts:278/1394/773). A config update
/// arms the eviction timer for an inactive TTL query; the LAST client
/// disconnecting must STOP it (TS `#deleteClientDueToDisconnect`,
/// view-syncer.ts:767) so an idle group with no clients runs zero eviction —
/// matching TS, which clears the timer on last disconnect and never re-arms
/// it until a client reconnects. Non-vacuous: reverting the
/// `stop_expire_timer()` call at the last-disconnect branch leaves the timer
/// armed and the `is_none()` assertions below fail.
#[test]
fn last_disconnect_stops_the_eviction_timer() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let count = Arc::new(AtomicU64::new(0));
    let mut state = ViewSyncerService::new_test(
        "cg-expire",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        Arc::new(Mutex::new(HashMap::new())),
        count,
    );

    let (tx, _drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink = DirectWebSocketSink::new(tx);
    rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));
    assert!(!state.connections.is_empty());

    // Build a CVR whose only query is inactive for its only client with a
    // 1s TTL, so `next_eviction_time` is Some — then arm the timer exactly
    // as `#handleConfigUpdate`'s tail does (view-syncer.ts:1390).
    let version = CVRVersion {
        state_version: "00".to_string(),
        config_version: None,
    };
    let mut client_state = std::collections::BTreeMap::new();
    client_state.insert(
        "c1".to_string(),
        rust_cvr::schema::types::ClientState {
            inactivated_at: Some(0),
            ttl: 1_000,
            version: version.clone(),
        },
    );
    let query = QueryRecord::Client(rust_cvr::schema::types::ClientQueryRecord {
        base: rust_cvr::schema::types::BaseQueryRecord {
            id: "q1".to_string(),
            transformation_hash: None,
            transformation_version: None,
            row_set_signature: None,
        },
        ast: serde_json::json!({"table": "users"}),
        client_state,
        patch_version: None,
    });
    let mut queries = std::collections::BTreeMap::new();
    queries.insert("q1".to_string(), query);
    let cvr = CVR {
        id: "cg-expire".to_string(),
        version,
        last_active: 0,
        ttl_clock: 0,
        replica_version: Some("v1".to_string()),
        clients: std::collections::BTreeMap::new(),
        queries,
        client_schema: None,
        profile_id: None,
    };
    state.cvr = Some(cvr.clone());
    state.schedule_expire_eviction(&cvr);
    assert!(
        state.expired_queries_timer.is_some(),
        "a config update with an inactive TTL query must arm the eviction timer"
    );
    assert!(state.next_expiry_delay().is_some());

    // Last client disconnects → TS `#stopExpireTimer` (view-syncer.ts:767).
    state.delete_client_due_to_disconnect("c1", "ws1");
    assert!(state.connections.is_empty());
    assert!(
        state.expired_queries_timer.is_none(),
        "last disconnect must stop the eviction timer (TS #stopExpireTimer, \
             view-syncer.ts:767): an idle group with no clients runs 0 evictions"
    );
    assert!(state.next_expiry_delay().is_none());
}

/// A same-clientID supersede (CGMessage::CloseConnection → close_connection)
/// must close the replaced socket FRAME-LESS, matching TS
/// (view-syncer.ts:913 `client.close("replaced by wsID: …")` →
/// `downstream.cancel()`). It must NOT send an `["error", …]` frame — a
/// Rehome there tells the superseded client to reconnect elsewhere even
/// though the SAME client already reconnected. Non-vacuous: reverting
/// `close_connection` to `close_with_error(rehome(...))` makes an error frame
/// appear and the assertion fails. (Caught by the G49 ownership differential:
/// rust=Rehome, TS=none, 2026-08-28.)
#[test]
fn supersede_close_is_frameless_like_ts() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let count = Arc::new(AtomicU64::new(0));
    let mut state = ViewSyncerService::new_test(
        "cg-supersede",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        Arc::new(Mutex::new(HashMap::new())),
        count,
    );

    let (tx, mut drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink = DirectWebSocketSink::new(tx);
    rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));
    assert_eq!(
        state.registered_ws.get("c1").map(String::as_str),
        Some("ws1")
    );
    // Drain any connect-time commands queued on this sink.
    while drx.try_recv().is_ok() {}

    // Supersede: the CG receives CloseConnection for the still-registered ws.
    state.close_connection("c1", "ws1");

    let mut saw_error_frame = false;
    let mut saw_close = false;
    while let Ok(cmd) = drx.try_recv() {
        match cmd {
            WsCommand::Fail(_) | WsCommand::FailWithCode { .. } => saw_error_frame = true,
            WsCommand::Send { msg, .. } => {
                if msg.get(0).and_then(|v| v.as_str()) == Some("error") {
                    saw_error_frame = true;
                }
            }
            // A poke part is neither an error frame nor a close. Named
            // rather than swept into a wildcard so a future variant has to
            // make this same decision explicitly.
            WsCommand::SendPokePart { .. } => {}
            WsCommand::Close(_) | WsCommand::CloseWithCode { .. } => saw_close = true,
        }
    }
    assert!(
        !saw_error_frame,
        "supersede must NOT send an error frame — TS closes frame-less \
             (view-syncer.ts:913); a Rehome here is a spurious reconnect signal"
    );
    assert!(
        saw_close,
        "supersede must still close the superseded socket"
    );
}

#[test]
fn idle_shutdown_requires_both_keepalive_expiry_and_zero_admissions() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let count = Arc::new(AtomicU64::new(0));
    let mut state = ViewSyncerService::new_test(
        "cg-idle",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        Arc::new(Mutex::new(HashMap::new())),
        count.clone(),
    );

    // Empty is not enough: the TS-compatible keepalive protects a recently
    // disconnected group from reconnect thrash.
    state.keepalive_until = now_ms() + 60_000;
    assert!(!state.idle_shutdown_due());
    assert!(state.next_idle_shutdown_delay().is_some());

    // Expiry makes the empty group eligible.
    state.keepalive_until = now_ms() - 1;
    assert!(state.idle_shutdown_due());

    // A connection admitted by the router but not installed on the CG
    // thread yet must keep the group alive as well.
    count.store(1, Ordering::Relaxed);
    assert!(!state.idle_shutdown_due());
    assert!(state.next_idle_shutdown_delay().is_none());
}

/// The first connection binds the client group's userID; a later connection
/// with a different userID is rejected, while the same userID is allowed.
#[test]
fn group_pins_user_id_on_first_connection() {
    let mut group = GroupAuthState::default();
    // First connection binds the pin.
    assert!(check_and_pin_user(&mut group, "user-1").is_ok());
    assert_eq!(group.pinned_user_id.as_deref(), Some("user-1"));
    // Same user → allowed, pin unchanged.
    assert!(check_and_pin_user(&mut group, "user-1").is_ok());
    assert_eq!(group.pinned_user_id.as_deref(), Some("user-1"));
    // Different user → rejected, pin unchanged.
    assert!(check_and_pin_user(&mut group, "user-2").is_err());
    assert_eq!(group.pinned_user_id.as_deref(), Some("user-1"));
}

/// When a client reconnects (same clientID, new wsID) the superseded
/// connection's socket must be failed/closed and unregistered — otherwise the
/// old ws_id keeps receiving pokes and its socket lingers open.
#[test]
fn reconnect_closes_superseded_connection() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let global = Arc::new(Mutex::new(HashMap::new()));
    let count = Arc::new(AtomicU64::new(2));
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        global,
        count,
    );

    // First connection: client c1 on ws1.
    let (tx1, mut drx1) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(test_params("c1", "ws1"), DirectWebSocketSink::new(tx1)));
    while drx1.try_recv().is_ok() {} // drain ws1's `connected` frame

    // Reconnect: same client c1 on a NEW ws2.
    let (tx2, _drx2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(test_params("c1", "ws2"), DirectWebSocketSink::new(tx2)));

    // The superseded ws1 socket is closed FRAME-LESS — TS syncer.ts:649
    // `existing.close(`replaced by ${params.wsID}`)` → ws close, no error
    // frame (an `error` frame here was the G49-class divergence).
    let mut ws1_closed = false;
    let mut ws1_errored = false;
    while let Ok(cmd) = drx1.try_recv() {
        match cmd {
            WsCommand::Close(_) | WsCommand::CloseWithCode { .. } => ws1_closed = true,
            WsCommand::Fail(_) | WsCommand::FailWithCode { .. } => ws1_errored = true,
            WsCommand::Send { msg, .. } if msg[0] == "error" => ws1_errored = true,
            _ => {}
        }
    }
    assert!(ws1_closed, "the superseded ws1 connection must be closed");
    assert!(
        !ws1_errored,
        "the supersede close carries no error frame (TS syncer.ts:649)"
    );

    // The mapping now points at ws2, with exactly one registered client.
    assert_eq!(
        state.registered_ws.get("c1").map(String::as_str),
        Some("ws2")
    );
    assert_eq!(state.registered_ws.len(), 1);

    // The delayed close event from ws1 must not tear down ws2.
    state.delete_client_due_to_disconnect("c1", "ws1");
    assert_eq!(
        state.registered_ws.get("c1").map(String::as_str),
        Some("ws2")
    );
    assert!(state.connections.contains_key("c1"));
}

/// Drain closes every known connection the way TS `#cleanup()` does
/// (view-syncer.ts:2810-2824): `client.close(`closed clientGroupID=${id}`)`
/// → `Connection.close` → `ws.close()` — NO error frame, no status. Every TS
/// drain path lands here: `Syncer.drain()` → `vs.stop()` (workers/syncer.ts:
/// 746) → `#stateChanges.cancel()` → the run loop ends normally.
///
/// NON-VACUOUS (2026-09-08): until this commit rust sent `["error", Rehome
/// "Reconnect required"]` here and the test asserted exactly that, citing a
/// `#cleanup` `client.fail(...)` that does not exist on this path (see the
/// `shutdown` doc). Restore `close_with_error(rehome(..))` and the
/// no-error-frame assertion fails.
#[test]
fn shutdown_closes_connections_frameless_like_ts_cleanup() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let global = Arc::new(Mutex::new(HashMap::new()));
    let count = Arc::new(AtomicU64::new(0));
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        global,
        count,
    );

    let (tx, mut drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink = DirectWebSocketSink::new(tx);
    rt.block_on(state.on_new_connection(test_params("c1", "ws1"), sink));
    // Discard the accept-path frames (`connected`, …) so only the drain's
    // output is judged.
    while drx.try_recv().is_ok() {}

    state.shutdown();

    let mut error_frames = 0;
    let mut close_reason: Option<String> = None;
    while let Ok(cmd) = drx.try_recv() {
        match cmd {
            WsCommand::Send { msg, .. } => {
                if msg[0] == "error" {
                    error_frames += 1;
                }
            }
            WsCommand::Fail(_) | WsCommand::FailWithCode { .. } => error_frames += 1,
            // A poke part is not an error frame (see the twin note in the
            // supersede test).
            WsCommand::SendPokePart { .. } => {}
            WsCommand::Close(reason) => close_reason = Some(reason),
            WsCommand::CloseWithCode { reason, .. } => close_reason = Some(reason),
        }
    }
    assert_eq!(
        error_frames, 0,
        "TS `#cleanup()` closes a drained client with NO error frame"
    );
    assert_eq!(
        close_reason.as_deref(),
        Some("closed clientGroupID=cg1"),
        "TS `client.close(`closed clientGroupID=${{id}}`)` (view-syncer.ts:2822)"
    );
    assert_eq!(state.connections.len(), 0);
    assert_eq!(state.registered_ws.len(), 0);
    assert!(!state.accepting.load(Ordering::SeqCst));

    // Cleanup is deliberately idempotent: shutdown can arrive after a
    // terminal failure or an idle-expiry path.
    state.shutdown();
    assert_eq!(state.connection_count.load(Ordering::Relaxed), 0);
}

/// TS `#runInLockWithCVR` (view-syncer.ts:464-478): an op that reaches a
/// view-syncer whose `#stateChanges` is no longer active — "a backlog of
/// tasks queued on the lock, or ... a client connects before the ViewSyncer
/// has been deleted from the ServiceRunner" — throws
/// `ProtocolErrorWithLevel(Rehome "Reconnect required", 'info')`; for an
/// `initConnection` that is `.catch(e => newClient.fail(e))` (:964), so the
/// client gets the Rehome frame and a no-status close. Rust's mailbox is
/// that lock queue: a `NewConnection` the router sent (and counted) before
/// it observed `accepting == false` sits behind the drain `Shutdown`.
///
/// NON-VACUOUS (2026-09-08): before the post-loop drain in `cg_event_loop`
/// the queued admission was dropped with the receiver, its sink with it,
/// and the writer task ended on a closed channel — the socket closed with
/// NO frame. Remove the `while let Ok(msg) = rx.try_recv()` block and the
/// sink's channel yields `Disconnected` instead of the Rehome.
#[test]
fn queued_connection_behind_shutdown_is_rehomed_like_ts_run_in_lock_with_cvr() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
        jwk: None,
        secret: None,
        jwks_url: None,
        issuer: None,
        audience: None,
    });
    let ctx = crate::workers::cg_executor::CgTaskContext {
        services_factory: factory,
        auth_validator: validator,
        connections: Arc::new(Mutex::new(HashMap::new())),
        cvr_pool: None,
        serving_lag_registry: Arc::new(crate::workers::syncer::ServingLagRegistry::new()),
    };
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<CGMessage>();
    // The router counted the admission before sending it (`get_or_create_cg`).
    let connection_count = Arc::new(AtomicU64::new(1));
    let accepting = Arc::new(AtomicBool::new(true));
    let (ws_tx, mut ws_rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();

    // Order is the point: the stop lands first, the admission is queued
    // behind it.
    tx.send(CGMessage::Shutdown).unwrap();
    tx.send(CGMessage::NewConnection {
        params: Box::new(test_params("c1", "ws1")),
        sink: DirectWebSocketSink::new(ws_tx),
        enqueued_at: std::time::Instant::now(),
    })
    .unwrap();
    drop(tx);

    rt.block_on(cg_event_loop(
        "cg1",
        rx,
        connection_count.clone(),
        accepting.clone(),
        ctx,
        None,
    ));

    match ws_rx.try_recv() {
        Ok(WsCommand::FailWithCode { error, code }) => {
            assert!(matches!(error.kind(), crate::protocol::ErrorKind::Rehome));
            assert_eq!(error.message(), "Reconnect required");
            assert_eq!(code, None, "TS `ws.close()` with no status");
        }
        Ok(_) => panic!("expected the Rehome error frame first"),
        Err(_) => {
            panic!("the queued admission must be answered, not dropped with the receiver")
        }
    }
    assert_eq!(
        connection_count.load(Ordering::Relaxed),
        0,
        "the rejected admission is un-counted"
    );
    assert!(!accepting.load(Ordering::SeqCst));
}

/// I-1 observable (INVENTIONS.md): a connection arriving for a group whose
/// handle has stopped accepting — the TS "client connects before the
/// ViewSyncer has been deleted from the ServiceRunner" race that TS answers
/// with a Rehome (view-syncer.ts:464-478) — is admitted to a FRESH group and
/// served. Contract: never left hanging.
///
/// NON-VACUOUS: drop the `!handle.accepting` branch of `get_or_create_cg`
/// (always return the existing handle) and the two handles share one
/// channel.
#[test]
fn connection_after_a_groups_shutdown_is_admitted_to_a_fresh_group_not_rehomed() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
        jwk: None,
        secret: None,
        jwks_url: None,
        issuer: None,
        audience: None,
    });
    let router = Arc::new(crate::workers::syncer::Syncer::new_with_limit(
        factory,
        validator,
        Arc::new(crate::metrics::Metrics::default()),
        4,
    ));

    let first = router.get_or_create_cg("cg1").unwrap();
    // The group stops (idle expiry / drain) — its handle flips first.
    first.accepting.store(false, Ordering::SeqCst);

    let second = router.get_or_create_cg("cg1").unwrap();
    assert!(
        !first.tx.same_channel(&second.tx),
        "a stopped group's handle must be replaced, not handed back"
    );
    assert!(
        second.accepting.load(Ordering::SeqCst),
        "the replacement is a live group"
    );
    assert_eq!(router.cg_count(), 1, "the stale handle is gone");
    rt.block_on(router.shutdown());
}

/// `broadcast_notification` fans out to every CG thread. With none
/// registered it is a no-op returning 0 (the global-commit path is exercised
/// end-to-end by the replica/PG harness).
#[test]
fn broadcast_notification_with_no_cgs_returns_zero() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
        jwk: None,
        secret: None,
        jwks_url: None,
        issuer: None,
        audience: None,
    });
    let router = crate::workers::syncer::Syncer::new(
        factory,
        validator,
        Arc::new(crate::metrics::Metrics::default()),
    );
    assert_eq!(
        router.broadcast_notification(serde_json::json!({"state": "version-ready"})),
        0
    );
}

#[test]
fn client_group_creation_is_single_owner_and_bounded() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
        jwk: None,
        secret: None,
        jwks_url: None,
        issuer: None,
        audience: None,
    });
    let router = Arc::new(crate::workers::syncer::Syncer::new_with_limit(
        factory,
        validator,
        Arc::new(crate::metrics::Metrics::default()),
        1,
    ));

    std::thread::scope(|scope| {
        for _ in 0..8 {
            let router = router.clone();
            scope.spawn(move || {
                router.get_or_create_cg("cg1").unwrap();
            });
        }
    });
    assert_eq!(router.cg_count(), 1, "one CG id must have one owner thread");
    assert!(
        router.get_or_create_cg("cg2").is_err(),
        "an active CG must not be evicted past the configured limit"
    );

    let cg1 = router.cg_handles.get("cg1").unwrap();
    cg1.connection_count.store(0, Ordering::Relaxed);
    drop(cg1);
    assert!(router.get_or_create_cg("cg2").is_ok());
    assert_eq!(
        router.cg_count(),
        1,
        "an idle CG is evicted for the new group"
    );
    rt.block_on(router.shutdown());
}

/// At the client-group cap with no idle CG to evict, a new group's
/// connection is REHOMED (retryable, load-shed), not hard-rejected with
/// `ServerOverloaded`. Mirrors TS's drain/rehome load-shedding and avoids the
/// reject→retry storm the old cap behavior caused near saturation.
#[test]
fn overflow_rehomes_instead_of_server_overloaded() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
        jwk: None,
        secret: None,
        jwks_url: None,
        issuer: None,
        audience: None,
    });
    // Cap of 1: the first group fills the only slot.
    let router = Arc::new(crate::workers::syncer::Syncer::new_with_limit(
        factory,
        validator,
        Arc::new(crate::metrics::Metrics::default()),
        1,
    ));

    let make_ctx = |cgid: &str, cid: &str, ws: &str| {
        let mut params = test_params(cid, ws);
        params.client_group_id = cgid.to_string();
        let (up_tx, up_rx) = tokio::sync::mpsc::channel::<String>(8);
        let (sink_tx, sink_rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        (
            ConnectionContext {
                params,
                sink: DirectWebSocketSink::new(sink_tx),
                upstream_rx: up_rx,
            },
            up_tx,
            sink_rx,
        )
    };

    // 1. First group takes the single slot. Keep its upstream sender alive so
    //    the connection stays active (connection_count == 1) and the CG is
    //    NOT idle/evictable.
    let (ctx1, _keep_alive1, _sink1) = make_ctx("cgA", "cA", "wsA");
    rt.block_on(router.create_connection(ctx1));
    assert_eq!(router.cg_count(), 1);

    // 2. A second, distinct group at cap with no idle CG -> its sink must get
    //    a Rehome error, never ServerOverloaded.
    let (ctx2, _keep_alive2, mut sink2) = make_ctx("cgB", "cB", "wsB");
    rt.block_on(router.create_connection(ctx2));

    let mut saw_rehome = false;
    let mut saw_overloaded = false;
    while let Ok(cmd) = sink2.try_recv() {
        // A connect-time rejection closes 3000 (TS syncer.ts `ws.close(3000, ...)`).
        let body = match cmd {
            WsCommand::Fail(body) => body,
            WsCommand::FailWithCode { error, code } => {
                assert_eq!(code, Some(3000), "connect-time reject must close 3000");
                error
            }
            _ => continue,
        };
        match body.kind() {
            crate::protocol::ErrorKind::Rehome => saw_rehome = true,
            crate::protocol::ErrorKind::ServerOverloaded => saw_overloaded = true,
            _ => {}
        }
    }
    assert!(saw_rehome, "overflow must Rehome (load-shed)");
    assert!(
        !saw_overloaded,
        "overflow must NOT ServerOverloaded — that reject was the storm cause"
    );
    // The overflow group was not admitted.
    assert_eq!(router.cg_count(), 1);

    rt.block_on(router.shutdown());
}

/// A Pusher whose `init_connection` blocks the CG thread on the first call,
/// simulating a long synchronous `config_and_hydrate`. (The seam moved with
/// the L9 Stage 3d un-interception: `pusher.initConnection` now fires from
/// the handler's `initConnection` arm ON the CG task, inside the same
/// dispatch that runs the config/hydrate pass — the old injectable
/// placeholder-CCM call is gone.) Signals `entered` when it reaches the
/// block and holds until the test flips `release`.
struct BlockingPusher {
    entered: Arc<AtomicBool>,
    release: Arc<(Mutex<bool>, std::sync::Condvar)>,
    blocked_once: AtomicBool,
}
impl PusherDispatch for BlockingPusher {
    // Explicit empty bodies: this test dispatch has no connection-context
    // owner. Both hooks are REQUIRED trait methods so the choice cannot be
    // inherited silently (see the trait docs).
    fn set_auth_fail_hook(&self, _hook: crate::workers::syncer_ws_message_handler::AuthFailHook) {}

    fn set_validate_hook(&self, _hook: crate::workers::syncer_ws_message_handler::ValidateHook) {}

    fn enqueue_push(
        &self,
        _selector: &ConnectionSelector,
        _body: &serde_json::Value,
        _headers: &crate::workers::syncer_ws_message_handler::PushRelayHeaders,
        _client_group_id: &str,
    ) -> crate::workers::connection::HandlerResult {
        crate::workers::connection::HandlerResult::Ok
    }
    fn init_connection(&self, _s: &ConnectionSelector) {
        // Only the first (blocker) connection holds the thread.
        if self.blocked_once.swap(true, Ordering::SeqCst) {
            return;
        }
        self.entered.store(true, Ordering::SeqCst);
        let (m, cv) = &*self.release;
        let mut released = m.lock().unwrap();
        while !*released {
            released = cv.wait(released).unwrap();
        }
    }
    fn ack_mutation_responses(
        &self,
        _selector: &ConnectionSelector,
        _body: &serde_json::Value,
        _headers: &crate::workers::syncer_ws_message_handler::PushRelayHeaders,
        _client_group_id: &str,
    ) {
    }
    fn delete_client_mutations(
        &self,
        _selector: &ConnectionSelector,
        _client_ids: &[String],
        _headers: &crate::workers::syncer_ws_message_handler::PushRelayHeaders,
        _client_group_id: &str,
    ) {
    }
}

struct BlockingPusherFactory {
    handle: tokio::runtime::Handle,
    pusher: Arc<BlockingPusher>,
}
impl CGServicesFactory for BlockingPusherFactory {
    fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
        None
    }
    fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
        Some(self.pusher.clone())
    }
    fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
        SyncEngineConfig {
            initialization_error: None,
            tables: vec![issue_table_spec()],
            full_tables: vec![issue_full_table_spec()],
            replica_path: None,
            app_id: "zero".to_string(),
            replica_version: "00".to_string(),
            shard: ShardID {
                app_id: "zero".to_string(),
                shard_num: 0,
            },
            cvr_pg: None,
            permissions: None,
            permissions_hash: None,
            revalidate_interval_ms: None,
            query_config: None,
            enable_query_covering: true,
            enable_query_planner: true,
            priority_op_running_yield_threshold_ms: 2.5,
            normal_yield_threshold_ms: 10.0,
            tokio_handle: self.handle.clone(),
            admin_password: None,
            server_version: "test".to_string(),
            metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
        }
    }
}

/// Regression (connect-ack decoupling, prod incident 2026-08-27): the
/// `connected` message MUST be emitted on the accept task
/// (`handle_connection`), NOT on the serial CG thread. When a client's CG
/// thread is blocked in an in-flight `config_and_hydrate` (here simulated by
/// a blocking `PusherDispatch::init_connection`, which the handler's
/// initConnection arm fires ON the CG task), a SECOND client
/// on the SAME group must still receive `connected` immediately — otherwise
/// its 10s connect timeout fires, it disconnects, the idle CG is reaped, and
/// the reconnect pays a full cold re-hydrate (the thrash we observed).
///
/// NON-VACUOUS: before the fix, `connected` was sent by `Connection::init()`
/// inside `on_new_connection` on the CG thread, so client B's ack was queued
/// behind the blocked hydrate and this `try_recv` finds nothing → the assert
/// fails. (Verified by reverting the `handle_connection` emission.)
#[test]
fn connected_ack_is_decoupled_from_a_blocked_cg_hydrate() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let pusher = Arc::new(BlockingPusher {
        entered: entered.clone(),
        release: release.clone(),
        blocked_once: AtomicBool::new(false),
    });
    let factory: Arc<dyn CGServicesFactory> = Arc::new(BlockingPusherFactory {
        handle: rt.handle().clone(),
        pusher,
    });
    let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
        jwk: None,
        secret: None,
        jwks_url: None,
        issuer: None,
        audience: None,
    });
    let router = Arc::new(crate::workers::syncer::Syncer::new_with_limit(
        factory,
        validator,
        Arc::new(crate::metrics::Metrics::default()),
        10,
    ));

    let make_ctx = |cid: &str, ws: &str, init: bool| {
        let mut params = test_params(cid, ws);
        params.client_group_id = "cgX".to_string();
        if init {
            // clientSchema so the new group's init is ACCEPTED — the
            // blocking seam (`pusher.initConnection`) only fires on an
            // accepted config pass (TS: after the ViewSyncer stream starts).
            params.init_connection_msg = Some(
                serde_json::from_value(serde_json::json!([
                    "initConnection",
                    {"desiredQueriesPatch": [], "clientSchema": {"tables": {}}}
                ]))
                .unwrap(),
            );
        }
        let (up_tx, up_rx) = tokio::sync::mpsc::channel::<String>(8);
        let (sink_tx, sink_rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        (
            ConnectionContext {
                params,
                sink: DirectWebSocketSink::new(sink_tx),
                upstream_rx: up_rx,
            },
            up_tx,
            sink_rx,
        )
    };

    // Blocker A: its `initConnection` drives the CG thread into the blocking
    // `pusher.init_connection` (fired by the handler after the accepted
    // config pass), holding the thread like a long hydrate.
    let (ctx_a, _keep_a, _sink_a) = make_ctx("cA", "wsA", true);
    rt.block_on(router.create_connection(ctx_a));

    // Wait until the CG thread is actually blocked.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !entered.load(Ordering::SeqCst) {
        assert!(
            std::time::Instant::now() < deadline,
            "blocker never reached the CG-thread init_connection"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    // Client B on the SAME group. Its `connected` must arrive from the accept
    // task even though the CG thread is blocked on A.
    let (ctx_b, _keep_b, mut sink_b) = make_ctx("cB", "wsB", false);
    rt.block_on(router.create_connection(ctx_b));

    let mut saw_connected = false;
    while let Ok(cmd) = sink_b.try_recv() {
        if let Some(msg) = cmd.frame_value()
            && msg.get(0).and_then(|v| v.as_str()) == Some("connected")
        {
            saw_connected = true;
        }
    }

    // Release the blocked CG thread FIRST so shutdown is clean regardless of
    // the assertion outcome.
    {
        let (m, cv) = &*release;
        *m.lock().unwrap() = true;
        cv.notify_all();
    }
    assert!(
        saw_connected,
        "client B must receive `connected` from the accept task while the CG \
             thread is mid-hydrate (connect-ack must not be serialized behind it)"
    );

    rt.block_on(router.shutdown());
}

/// `place_cg` chooses the executor hosting the FEWEST groups, ignoring the
/// cg_id hash except to break ties among equally-loaded executors. Proves the
/// least-loaded contract directly: a heavily-loaded executor is avoided even
/// when the hash would otherwise select it.
#[test]
fn place_cg_picks_least_loaded_executor() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
        jwk: None,
        secret: None,
        jwks_url: None,
        issuer: None,
        audience: None,
    });
    let router = crate::workers::syncer::Syncer::new_sharded(
        factory,
        validator,
        Arc::new(crate::metrics::Metrics::default()),
        100,
        3,
        None,
        ConnectionSinks::new(),
        ShardID {
            app_id: "zero".to_string(),
            shard_num: 0,
        },
    );

    // Fake handles (no real CG task) let us load specific executors without
    // spawning groups — place_cg only reads `executor_idx`, never the channel.
    let dummy = |executor_idx: usize| {
        let (tx, _rx) = mpsc::unbounded_channel::<CGMessage>();
        CGHandle {
            tx,
            connection_count: Arc::new(AtomicU64::new(1)),
            accepting: Arc::new(AtomicBool::new(true)),
            executor_idx,
        }
    };

    // Empty router: no load anywhere, so every executor ties and the pick is
    // the deterministic hash tie-break (spreads a cold system).
    assert_eq!(
        router.place_cg("some-cg"),
        shard_for("some-cg", 3),
        "on an empty router placement falls back to the hash tie-break"
    );

    // Load executor 0 with two groups and executor 1 with one; executor 2 is
    // empty, so it MUST be chosen regardless of the cg_id hash.
    router.cg_handles.insert("a".to_string(), dummy(0));
    router.cg_handles.insert("b".to_string(), dummy(0));
    router.cg_handles.insert("c".to_string(), dummy(1));
    for cg in ["x", "y", "z", "hash-would-pick-0", "another"] {
        assert_eq!(
            router.place_cg(cg),
            2,
            "least-loaded executor (2) must win for {cg} despite the hash"
        );
    }

    rt.block_on(router.shutdown());
}

/// End-to-end, placing many real groups spreads them evenly across executors:
/// because placement is serialized and each placed group is registered before
/// the next placement, least-loaded degenerates to round-robin and keeps the
/// per-executor group counts within 1 of each other.
#[test]
fn placement_balances_groups_across_executors() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let validator: Arc<dyn AuthValidator> = Arc::new(crate::auth::jwt::JwtAuthValidator {
        jwk: None,
        secret: None,
        jwks_url: None,
        issuer: None,
        audience: None,
    });
    let k = 4;
    let n = 40;
    let router = crate::workers::syncer::Syncer::new_sharded(
        factory,
        validator,
        Arc::new(crate::metrics::Metrics::default()),
        200,
        k,
        None,
        ConnectionSinks::new(),
        ShardID {
            app_id: "zero".to_string(),
            shard_num: 0,
        },
    );

    for i in 0..n {
        router.get_or_create_cg(&format!("cg{i}")).unwrap();
    }

    let mut counts = vec![0usize; k];
    for entry in router.cg_handles.iter() {
        counts[entry.executor_idx] += 1;
    }
    let max = *counts.iter().max().unwrap();
    let min = *counts.iter().min().unwrap();
    assert!(
        max - min <= 1,
        "expected round-robin-balanced placement, got {counts:?}"
    );
    assert_eq!(counts.iter().sum::<usize>(), n, "every group placed once");

    rt.block_on(router.shutdown());
}

/// L9 Stage 3d regression: a piggybacked `initConnection` is dispatched
/// through the SAME path as a socket frame (Connection → handler →
/// ViewSyncerDispatch), and the handler's `connContextManager.initConnection`
/// dispatch — now the SINGLE recording site — lands the body's
/// `userQueryURL` in the real CCM. Fails if the piggyback bypasses the
/// handler or the CCM recording is dropped/duplicated elsewhere.
#[test]
fn init_connection_fires_ccm_init_side_effect() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let global = Arc::new(Mutex::new(HashMap::new()));
    let count = Arc::new(AtomicU64::new(0));
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        global,
        count,
    );
    seed_test_client_schema(&mut state);
    let cell = shared(state);

    let (tx, mut drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink = DirectWebSocketSink::new(tx);
    let mut params = test_params("c1", "ws1");
    // Piggyback an initConnection with an empty desired-queries patch and a
    // custom-query URL (recorded only via the handler's ccm dispatch).
    params.init_connection_msg = Some(
        serde_json::from_value(serde_json::json!([
            "initConnection",
            {"desiredQueriesPatch": [], "userQueryURL": "https://api.example.com/z"}
        ]))
        .unwrap(),
    );
    let piggyback = rt.block_on(cell.borrow_mut().on_new_connection(params, sink));
    let (client_id, ws_id, text) =
        piggyback.expect("on_new_connection must hand back the piggybacked initConnection");
    rt.block_on(on_inbound(&cell, client_id, ws_id, text));

    // The recorded `userQueryURL` is observed through the initConnection-time
    // `#validateConnection` probe (view-syncer.ts:942) rather than by reading
    // the CCM field: with no allow-list configured, the probe fails the URL
    // check SYNCHRONOUSLY (no network) and the failure message quotes the URL
    // it tried. That is a stronger assertion than the field — it proves the
    // recorded URL is the one actually used for the API-server round trip.
    let err = std::iter::from_fn(|| drx.try_recv().ok()).find_map(|command| match command {
        WsCommand::Send { msg, .. }
            if msg.get(0).and_then(serde_json::Value::as_str) == Some("error") =>
        {
            msg.get(1).cloned()
        }
        _ => None,
    });
    let err = err.expect("initConnection must probe the recorded userQueryURL");
    assert_eq!(err["kind"], "TransformFailed");
    assert!(
        err["message"]
            .as_str()
            .unwrap_or_default()
            .contains("https://api.example.com/z"),
        "userQueryURL must be recorded through the handler's ccm.initConnection \
             dispatch and used for the validation probe; got {err:?}"
    );
}

#[test]
fn new_client_group_rejects_init_without_client_schema() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(1)),
    );
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    rt.block_on(state.on_new_connection(test_params("c1", "ws1"), DirectWebSocketSink::new(tx)));
    rt.block_on(state.handle_desired_queries(
        "c1",
        &serde_json::json!({"desiredQueriesPatch": []}),
        ConfigPassOrigin::InitConnection,
        CustomQueryTransformMode::All,
    ));

    let error = std::iter::from_fn(|| rx.try_recv().ok()).find_map(|command| match command {
        WsCommand::Send { msg: value, .. }
            if value.get(0).and_then(serde_json::Value::as_str) == Some("error") =>
        {
            value.get(1).cloned()
        }
        _ => None,
    });
    let error = error.expect("missing schema must close with a protocol error");
    assert_eq!(error["kind"], "InvalidConnectionRequest");
    assert!(
        error["message"]
            .as_str()
            .is_some_and(|message| message.contains("must include client schema"))
    );
}

/// G36 garbage-cookie / overlarge-configversion: a baseCookie that fails
/// `versionFromString` (TS schema/types.ts, called from the ClientHandler
/// constructor's `cookieToVersion`) FAILS the connection with a fatal
/// `Internal` error (`wrapWithProtocolError`, types/error-with-level.ts) —
/// it must NOT be silently treated as "no base version". Covers both G36
/// shapes: a non-Lexi stateVersion and a configVersion above 2^53.
///
/// The `connected`-before-`error` ordering TS guarantees is now structural:
/// `handle_connection` (accept task) sends `connected` BEFORE dispatching
/// `NewConnection` to the CG thread, and this `Internal` error is emitted
/// later on the CG thread by `init_connection` — when the initConnection
/// MESSAGE is handled (the ClientHandler constructor, view-syncer.ts:903-910),
/// not at accept. This unit test drives the CG thread directly, so it
/// asserts only that half (nothing at accept, the `Internal` close at init);
/// the ordering is covered by `handle_connection`'s accept-task emission.
#[test]
fn malformed_base_cookie_closes_with_internal_error() {
    for bad_cookie in ["!!notlexi!!", "00:b100000000000"] {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
            handle: rt.handle().clone(),
        });
        let mut state = ViewSyncerService::new_test(
            "cg1",
            &factory,
            Arc::new(crate::auth::jwt::JwtAuthValidator {
                jwk: None,
                secret: None,
                jwks_url: None,
                issuer: None,
                audience: None,
            }),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(AtomicU64::new(1)),
        );
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let mut params = test_params("c1", "ws1");
        params.base_cookie = Some(bad_cookie.to_string());
        let accept_logs = {
            use super::engine_tests::{capture_logs, captured};
            let (buf, _guard) = capture_logs(tracing::Level::DEBUG);
            rt.block_on(state.on_new_connection(params, DirectWebSocketSink::new(tx)));
            captured(&buf)
        };
        assert!(
            drain_sends(&mut rx).iter().all(|f| f[0] != "error"),
            "[{bad_cookie}] the cookie is parsed at initConnection, not at accept"
        );
        // TS emits NOTHING at accept for a bad cookie (it is not parsed
        // until `new ClientHandler` at initConnection); rust's accept-time
        // fallback is bookkeeping and must not be a WARN with no TS twin.
        assert!(
            !accept_logs
                .lines()
                .any(|l| l.contains("WARN") && l.contains("malformed base cookie")),
            "[{bad_cookie}] rust-only WARN at accept; got:\n{accept_logs}"
        );
        let logs = {
            use super::engine_tests::{capture_logs, captured};
            let (buf, _guard) = capture_logs(tracing::Level::DEBUG);
            rt.block_on(state.handle_desired_queries(
                "c1",
                &serde_json::json!({"clientSchema": {"tables": {}}}),
                ConfigPassOrigin::InitConnection,
                CustomQueryTransformMode::All,
            ));
            captured(&buf)
        };
        // LEVEL: TS throws a RAW `TypeError`/`Error` from `versionFromString`
        // (schema/types.ts:333/338) → `Connection.#closeWithThrown(e)` →
        // `sendError(.., thrown=e)` → `getLogLevel(plain Error)` = 'error'.
        // NON-VACUOUS (2026-09-08): the bodiless `close_with_error` this site
        // used classified `Internal` with no thrown → 'info'.
        assert_eq!(
            logs.lines()
                .filter(|l| l.contains("ERROR") && l.contains("Sending error on WebSocket"))
                .count(),
            1,
            "[{bad_cookie}] a malformed cookie is a raw throw in TS → ERROR; got:\n{logs}"
        );

        let mut saw_connected = false;
        let mut error = None;
        while let Ok(command) = rx.try_recv() {
            if let Some(value) = command.frame_value() {
                match value.get(0).and_then(serde_json::Value::as_str) {
                    Some("connected") => saw_connected = true,
                    Some("error") => error = value.get(1).cloned(),
                    _ => {}
                }
            }
        }
        // `connected` is emitted on the accept task, NOT on this CG-thread
        // path, so the CG thread must not send it here.
        assert!(
            !saw_connected,
            "[{bad_cookie}] on_new_connection must not send `connected` (accept task does)"
        );
        let error =
            error.unwrap_or_else(|| panic!("[{bad_cookie}] must close with a protocol error"));
        assert_eq!(error["kind"], "Internal", "[{bad_cookie}] {error}");
        assert!(
            !state.connections.contains_key("c1"),
            "[{bad_cookie}] connection must be torn down"
        );
    }
}

/// The inspector protocol gates every op behind an `authenticate` that
/// matches the configured admin password; `version` then returns the
/// configured server version.
#[test]
fn inspect_auth_gate_then_version() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let global = Arc::new(Mutex::new(HashMap::new()));
    let count = Arc::new(AtomicU64::new(0));
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        global,
        count,
    );
    // Configure an admin password for the inspector.
    state.admin_password = Some("s3cret".to_string());
    state.server_version = "9.9.9".to_string();
    let cell = shared(state);

    let (tx, mut drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink = DirectWebSocketSink::new(tx);
    let _ = rt.block_on(
        cell.borrow_mut()
            .on_new_connection(test_params("c1", "ws1"), sink),
    );
    // `inspect` runs inside `#runInLockForClient` (view-syncer.ts:2640):
    // the socket must have sent initConnection.
    assert!(cell.borrow_mut().init_connection("c1", "ws1"));

    let drain =
        |drx: &mut tokio::sync::mpsc::UnboundedReceiver<WsCommand>| -> Vec<serde_json::Value> {
            let mut v = Vec::new();
            while let Ok(Some(m)) = drx.try_recv().map(|c| c.frame_value()) {
                v.push(m);
            }
            v
        };
    let _ = drain(&mut drx); // discard the `connected` frame

    // 1) `version` before authenticating → challenge (authenticated:false).
    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        r#"["inspect",{"op":"version","id":"1"}]"#.to_string(),
    ));
    let frames = drain(&mut drx);
    let last = frames.last().unwrap();
    assert_eq!(last[0], "inspect");
    assert_eq!(last[1]["op"], "authenticated");
    assert_eq!(last[1]["value"], false);

    // 2) authenticate with the wrong password → false.
    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        r#"["inspect",{"op":"authenticate","id":"2","value":"nope"}]"#.to_string(),
    ));
    assert_eq!(drain(&mut drx).last().unwrap()[1]["value"], false);
    assert!(!cell.borrow().inspector_authenticated);

    // 3) authenticate with the right password → true.
    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        r#"["inspect",{"op":"authenticate","id":"3","value":"s3cret"}]"#.to_string(),
    ));
    assert_eq!(drain(&mut drx).last().unwrap()[1]["value"], true);
    assert!(cell.borrow().inspector_authenticated);

    // 4) `version` now returns the configured server version.
    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        r#"["inspect",{"op":"version","id":"4"}]"#.to_string(),
    ));
    let last = drain(&mut drx).into_iter().next_back().unwrap();
    assert_eq!(last[1]["op"], "version");
    assert_eq!(last[1]["value"], "9.9.9");
}

/// A ViewSyncerService pre-authenticated to the inspector, with a live connection.
/// Returns the state, runtime, and the sink's receive channel.
fn inspect_test_state() -> (
    Rc<RefCell<ViewSyncerService>>,
    tokio::runtime::Runtime,
    tokio::sync::mpsc::UnboundedReceiver<WsCommand>,
) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TestFactory {
        handle: rt.handle().clone(),
    });
    let mut state = ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(0)),
    );
    state.admin_password = Some("s3cret".to_string());
    state.inspector_authenticated = true;
    let cell = shared(state);
    let (tx, drx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink = DirectWebSocketSink::new(tx);
    let _ = rt.block_on(
        cell.borrow_mut()
            .on_new_connection(test_params("c1", "ws1"), sink),
    );
    // `inspect` runs inside `#runInLockForClient` (view-syncer.ts:2640):
    // the socket must have sent initConnection.
    assert!(cell.borrow_mut().init_connection("c1", "ws1"));
    (cell, rt, drx)
}

fn last_inspect_frame(
    drx: &mut tokio::sync::mpsc::UnboundedReceiver<WsCommand>,
) -> serde_json::Value {
    let mut last = serde_json::Value::Null;
    while let Ok(Some(m)) = drx.try_recv().map(|c| c.frame_value()) {
        last = m;
    }
    last
}

// NOTE: the `queries` inspector rows are produced by the SQL port
// `CVRStore::inspect_queries` (rust-cvr); `inspect_queries_value` enriches
// each row with the per-query `metrics` (via the InspectorDelegate +
// `metrics_for_protocol`) and the `getASTForQuery` AST fallback, 1:1 with
// TS inspect-handler.ts:63-70. The row-shape / TTL-filter / got-flag /
// rowCount / client-filter coverage lives in rust-cvr tests/inspect_pg_test.rs
// (PG-gated), against the real desires/queries/rows tables.

/// The `metrics` op returns the InspectorDelegate's real global aggregate
/// digests (TS `getMetricsJSON()`), not a hardcoded empty pair. NON-VACUOUS:
/// a `query-materialization-server` metric seeded into the delegate shows up
/// as `[1000, 12, 1]` in the frame; reverting the op to the old hardcoded
/// `[1000]` (or not feeding the delegate) makes the exact-array assert fail.
#[test]
fn inspect_metrics_returns_delegate_global_aggregates() {
    use rust_ivm::query::metrics_delegate::Metric;
    let (cell, rt, mut drx) = inspect_test_state();
    // Seed one materialization sample into this CG's delegate.
    cell.borrow().inspector_delegate().borrow_mut().add_metric(
        Metric::QueryMaterializationServer,
        12.0,
        "q1",
    );
    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        r#"["inspect",{"op":"metrics","id":"m1"}]"#.to_string(),
    ));
    let frame = last_inspect_frame(&mut drx);
    assert_eq!(frame[0], "inspect");
    assert_eq!(frame[1]["op"], "metrics");
    assert_eq!(frame[1]["id"], "m1");
    let value = &frame[1]["value"];
    assert!(
        value.is_object(),
        "metrics value must be a record, not an array"
    );
    // The seeded materialization point flows through to the global digest.
    assert_eq!(
        value["query-materialization-server"],
        serde_json::json!([1000, 12, 1]),
        "seeded materialization metric must appear in the global aggregate"
    );
    // No update samples → the update digest is empty `[1000]`.
    assert_eq!(value["query-update-server"], serde_json::json!([1000]));
}

#[test]
fn inspect_unsupported_and_unknown_ops_answer_with_error_op() {
    let (cell, rt, mut drx) = inspect_test_state();

    // analyze-query IS ported, but a request with no AST must answer with
    // `{op:"error"}` (AST required) — NOT a success frame carrying an
    // `{error}` payload, which would fail the client's
    // `analyzeQueryResultSchema`. (Port of the TS `throw new Error('AST is
    // required...')`, inspect-handler.ts:131, surfaced through the error op.)
    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        r#"["inspect",{"op":"analyze-query","id":"a1"}]"#.to_string(),
    ));
    let frame = last_inspect_frame(&mut drx);
    assert_eq!(frame[1]["op"], "error");
    assert_eq!(frame[1]["id"], "a1");
    assert!(
        frame[1]["value"]
            .as_str()
            .unwrap()
            .contains("AST is required"),
        "error value must be the AST-required message; got {:?}",
        frame[1]["value"]
    );

    // Unknown op (TS `unreachable` throw → catch) → error op, not silence.
    // Driven through handle_inspect directly: protocol validation upstream
    // (parse_upstream_array, mirroring the TS valita layer) rejects unknown
    // ops before dispatch, so this covers the defensive arm.
    rt.block_on(
        cell.borrow_mut()
            .handle_inspect("c1", &serde_json::json!({"op": "bogus", "id": "b1"})),
    );
    let frame = last_inspect_frame(&mut drx);
    assert_eq!(frame[1]["op"], "error");
    assert_eq!(frame[1]["id"], "b1");
    assert!(frame[1]["value"].is_string());
}

// ─── ViewSyncerService mock-factory harness ───────────────────────────────────────
//
// Drives ViewSyncerService (the fused port of TS view-syncer.ts +
// syncer-ws-message-handler.ts dispatch) directly on the test thread, with mock dispatch
// services (Noop* above), an in-memory replica carrying a real `issue`
// table spec, and a channel-backed DirectWebSocketSink standing in for the
// client socket. Models the TS view-syncer.pg.test.ts `connect()` +
// `nextPoke()` pattern.

/// The `fullTables` twin of `issue_table_spec()` (what `listTables` would
/// report for it), so `check_client_schema` sees a synced replica.
fn issue_full_table_spec() -> crate::db::specs::LiteTableSpec {
    use crate::db::specs::{LiteColumnSpec, LiteTableSpec};
    LiteTableSpec {
        name: "issue".to_string(),
        columns: vec![
            (
                "id".to_string(),
                LiteColumnSpec {
                    pos: 1,
                    data_type: "text|NOT_NULL".to_string(),
                    not_null: false,
                },
            ),
            (
                "title".to_string(),
                LiteColumnSpec {
                    pos: 2,
                    data_type: "text".to_string(),
                    not_null: false,
                },
            ),
        ],
        primary_key: None,
    }
}

fn issue_table_spec() -> crate::services::view_syncer::pipeline_driver::IvmTableSpec {
    use crate::services::view_syncer::pipeline_driver::{IvmColumnSchema, IvmTableSpec};
    IvmTableSpec {
        table: "issue".to_string(),
        column_order: Vec::new(),
        columns: HashMap::from([
            (
                "id".to_string(),
                IvmColumnSchema {
                    r#type: "string".to_string(),
                    optional: false,
                },
            ),
            (
                "title".to_string(),
                IvmColumnSchema {
                    r#type: "string".to_string(),
                    optional: true,
                },
            ),
        ]),
        primary_key: vec!["id".to_string()],
        unique_keys: None,
        all_potential_primary_keys: vec![vec!["id".to_string()]],
        min_row_version: None,
    }
}

/// Factory whose in-memory engine has a REAL `issue` table spec, so
/// desired-query puts against it hydrate instead of failing the group.
struct TablesFactory {
    handle: tokio::runtime::Handle,
}
impl CGServicesFactory for TablesFactory {
    fn create_mutagen(&self, _cg: &str) -> Option<Arc<dyn MutagenDispatch>> {
        None
    }
    fn create_pusher(&self, _cg: &str) -> Option<Arc<dyn PusherDispatch>> {
        None
    }
    fn create_sync_engine_config(&self, _cg: &str) -> SyncEngineConfig {
        SyncEngineConfig {
            initialization_error: None,
            tables: vec![issue_table_spec()],
            full_tables: vec![issue_full_table_spec()],
            replica_path: None, // in-memory sources
            app_id: "zero".to_string(),
            replica_version: "00".to_string(),
            shard: ShardID {
                app_id: "zero".to_string(),
                shard_num: 0,
            },
            cvr_pg: None,
            permissions: None,
            permissions_hash: None,
            revalidate_interval_ms: None,
            query_config: None,
            enable_query_covering: true,
            enable_query_planner: true,
            priority_op_running_yield_threshold_ms: 2.5,
            normal_yield_threshold_ms: 10.0,
            tokio_handle: self.handle.clone(),
            admin_password: None,
            server_version: "test".to_string(),
            metrics: std::sync::Arc::new(crate::metrics::Metrics::default()),
        }
    }
}

/// A ViewSyncerService over the `issue`-table factory, plus its runtime.
fn tables_state(rt: &tokio::runtime::Runtime) -> ViewSyncerService {
    let factory: Arc<dyn CGServicesFactory> = Arc::new(TablesFactory {
        handle: rt.handle().clone(),
    });
    ViewSyncerService::new_test(
        "cg1",
        &factory,
        Arc::new(crate::auth::jwt::JwtAuthValidator {
            jwk: None,
            secret: None,
            jwks_url: None,
            issuer: None,
            audience: None,
        }),
        Arc::new(Mutex::new(HashMap::new())),
        Arc::new(AtomicU64::new(1)),
    )
}

/// Drain every queued `Send` frame off the sink channel.
fn drain_sends(rx: &mut tokio::sync::mpsc::UnboundedReceiver<WsCommand>) -> Vec<serde_json::Value> {
    std::iter::from_fn(|| rx.try_recv().ok())
        // `frame_value` so a `SendPokePart` yields its frame: matching
        // `Send { msg }` here silently dropped every poke part.
        .filter_map(|command| command.frame_value())
        .collect()
}

/// Connect `c1`/`ws1` on the CG-thread path and drain any queued frames so
/// the returned rx starts clean for the caller's own assertions.
///
/// Note: `on_new_connection` no longer emits `connected` (that is the accept
/// task's job — see `handle_connection`), so this helper does not expect it.
fn connect_c1(
    rt: &tokio::runtime::Runtime,
    cell: &Rc<RefCell<ViewSyncerService>>,
) -> tokio::sync::mpsc::UnboundedReceiver<WsCommand> {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let _ = rt.block_on(
        cell.borrow_mut()
            .on_new_connection(test_params("c1", "ws1"), DirectWebSocketSink::new(tx)),
    );
    let frames = drain_sends(&mut rx);
    assert!(
        !frames.iter().any(|f| f[0] == "connected"),
        "on_new_connection (CG thread) must not emit `connected`; the accept task does"
    );
    rx
}

const INIT_CONNECTION_HASH1: &str = r#"["initConnection",{"clientSchema":{"tables":{}},"desiredQueriesPatch":[{"op":"put","hash":"query-hash1","ast":{"table":"issue"}}]}]"#;
const CHANGE_DESIRED_HASH1: &str = r#"["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"query-hash1","ast":{"table":"issue"}}]}]"#;

/// TS puts the poke-target `ClientHandler` in `#clients` when the
/// initConnection MESSAGE arrives (`initConnection`, view-syncer.ts:903-914)
/// and bumps `#activeClients` there (:888) — never at socket accept. Until
/// then the socket is not a client: `#getClients()` cannot return it, and
/// every other message from it is dropped by `#runInLockForClient`'s wsID
/// gate (:1216-1221, `mismatched wsID`) because it has no handler. Rust
/// registered the handler in `on_new_connection`, so an accepted socket whose
/// baseCookie matched the CVR version was an advance-poke target before its
/// initConnection, and its pre-init changeDesiredQueries ran a full config
/// pass. Non-vacuous: on the pre-fix code the first assertion fails.
#[test]
fn socket_becomes_a_client_only_on_init_connection() {
    use super::engine_tests::{capture_logs, captured};
    let (buf, _guard) = capture_logs(tracing::Level::DEBUG);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let cell = shared(tables_state(&rt));
    let mut rx = connect_c1(&rt, &cell);
    {
        let state = cell.borrow();
        assert!(
            state.get_clients(&["ws1".to_string()]).is_empty(),
            "an accepted socket is not in #clients until initConnection (view-syncer.ts:914)"
        );
        assert!(
            !state.active_client_pv.contains_key("ws1"),
            "#activeClients moves at initConnection (view-syncer.ts:888)"
        );
    }
    // A changeDesiredQueries BEFORE initConnection: dropped — no poke, no
    // CVR change (TS `#runInLockForClient` returns on the wsID mismatch).
    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        CHANGE_DESIRED_HASH1.to_string(),
    ));
    let frames = drain_sends(&mut rx);
    assert!(
        frames.iter().all(|f| f[0] != "pokeStart"),
        "a pre-init changeDesiredQueries must not poke: {frames:?}"
    );
    assert!(
        captured(&buf).contains("mismatched wsID"),
        "TS view-syncer.ts:1217; got:\n{}",
        captured(&buf)
    );
    assert!(
        cell.borrow()
            .cvr
            .as_ref()
            .is_none_or(|c| !c.clients.contains_key("c1")),
        "a dropped message must not record the client in the CVR"
    );
    // initConnection: the handler exists, the gauge moved, pokes flow.
    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        INIT_CONNECTION_HASH1.to_string(),
    ));
    let frames = drain_sends(&mut rx);
    assert!(
        frames.iter().any(|f| f[0] == "pokeStart"),
        "initConnection must poke: {frames:?}"
    );
    let state = cell.borrow();
    assert_eq!(state.get_clients(&["ws1".to_string()]).len(), 1);
    assert!(state.active_client_pv.contains_key("ws1"));
    assert!(
        state
            .cvr
            .as_ref()
            .is_some_and(|c| c.clients.contains_key("c1"))
    );
}

/// TS `initConnection` re-bases the TTL clock when the first client joins an
/// idle ViewSyncer (`if (this.#clients.size === 0) this.#ttlClockBase = now`,
/// view-syncer.ts:893-899): TTLs count CONNECTED time only, so the idle gap
/// between the last disconnect and the next initConnection must not advance
/// the clock. Rust re-based only on CVR load, so a reconnect inside the
/// keepalive window (CVR still loaded) charged the whole idle gap to every
/// query's TTL. Non-vacuous: on the pre-fix code the clock jumps by the
/// simulated 100 s gap.
#[test]
fn idle_gap_before_the_next_init_connection_does_not_advance_the_ttl_clock() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let cell = shared(tables_state(&rt));
    let mut rx = connect_c1(&rt, &cell);
    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        INIT_CONNECTION_HASH1.to_string(),
    ));
    drain_sends(&mut rx);
    cell.borrow_mut()
        .delete_client_due_to_disconnect("c1", "ws1");
    // The last disconnect flushed the clock at `now`; pretend that was 100 s
    // ago.
    let ttl_before = {
        let mut state = cell.borrow_mut();
        assert!(
            state.cvr.is_some(),
            "the CVR stays loaded through the keepalive window"
        );
        state.ttl_clock_base -= 100_000;
        state.ttl_clock
    };
    let (tx, mut rx2) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let _ = rt.block_on(
        cell.borrow_mut()
            .on_new_connection(test_params("c1", "ws2"), DirectWebSocketSink::new(tx)),
    );
    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws2".into(),
        INIT_CONNECTION_HASH1.to_string(),
    ));
    drain_sends(&mut rx2);
    let advanced = cell.borrow().ttl_clock - ttl_before;
    assert!(
        advanced < 50_000,
        "the idle gap must not be charged to the TTL clock (view-syncer.ts:893-899); advanced {advanced} ms"
    );
}

/// Port of TS connection.ts `#handleMessage` ping fast-path, driven through
/// the CG dispatch (`on_inbound` → Connection): `["ping",{}]` answers
/// exactly `["pong",{}]` and nothing else.
#[test]
fn on_inbound_ping_answers_pong() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let cell = shared(tables_state(&rt));
    let mut rx = connect_c1(&rt, &cell);

    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        r#"["ping",{}]"#.to_string(),
    ));
    let frames = drain_sends(&mut rx);
    assert_eq!(frames, vec![serde_json::json!(["pong", {}])]);
    assert!(
        cell.borrow().connections.contains_key("c1"),
        "ping must not close the connection"
    );
}

/// Port of TS connection.ts `#handleMessage` parse/valita catch: malformed
/// JSON and an unknown message tag both fail `upstreamSchema` → the exact
/// InvalidMessage error frame, then the connection is torn down.
#[test]
fn on_inbound_malformed_message_closes_with_invalid_message() {
    for bad in ["{not json", r#"["definitelyNotAThing",{}]"#] {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let cell = shared(tables_state(&rt));
        let mut rx = connect_c1(&rt, &cell);

        rt.block_on(on_inbound(
            &cell,
            "c1".into(),
            "ws1".into(),
            bad.to_string(),
        ));
        let frames = drain_sends(&mut rx);
        let error = frames
            .iter()
            .find(|f| f[0] == "error")
            .unwrap_or_else(|| panic!("[{bad}] expected an error frame"));
        assert_eq!(error[1]["kind"], "InvalidMessage", "[{bad}]");
        assert!(
            !cell.borrow().connections.contains_key("c1"),
            "[{bad}] the connection must be closed"
        );
        assert!(
            !cell.borrow().registered_ws.contains_key("c1"),
            "[{bad}] the client must be unregistered"
        );
    }
}

/// Port of TS view-syncer.pg.test.ts "initial hydration" (first poke):
/// an initConnection with a put patch pokes the desired-queries config —
/// `pokeStart {pokeID:"00:01", baseCookie:null}` → `pokePart` whose
/// `desiredQueriesPatches` carries the client's `{op:"put",
/// hash:"query-hash1"}` → `pokeEnd {cookie:"00:01"}` — and records the
/// client + the internal `lmids` query in the CVR (TS EXPECTED_LMIDS_AST).
#[test]
fn init_connection_pokes_desired_queries_patch() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let cell = shared(tables_state(&rt));
    let mut rx = connect_c1(&rt, &cell);

    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        INIT_CONNECTION_HASH1.to_string(),
    ));
    let frames = drain_sends(&mut rx);

    let poke_start = frames
        .iter()
        .find(|f| f[0] == "pokeStart")
        .expect("expected a pokeStart");
    assert_eq!(poke_start[1]["pokeID"], "00:01");
    assert!(
        poke_start[1]["baseCookie"].is_null(),
        "first poke must be from a null baseCookie: {poke_start}"
    );

    let desired = frames
        .iter()
        .filter(|f| f[0] == "pokePart")
        .find_map(|f| f[1].get("desiredQueriesPatches").cloned())
        .expect("expected a pokePart with desiredQueriesPatches");
    let c1_patch = desired["c1"]
        .as_array()
        .expect("desiredQueriesPatches keyed by clientID");
    assert!(
        c1_patch
            .iter()
            .any(|op| op["op"] == "put" && op["hash"] == "query-hash1"),
        "expected the put for query-hash1, got {c1_patch:?}"
    );

    let poke_end = frames
        .iter()
        .find(|f| f[0] == "pokeEnd")
        .expect("expected a pokeEnd");
    assert_eq!(poke_end[1]["cookie"], "00:01");

    // The hydrate pass follows as a SECOND poke: with no replica advance the
    // stateVersion is unchanged ("00"), so the got-queries update bumps the
    // minor (config) version — TS `CVRQueryDrivenUpdater.trackQueries`
    // bumps the minor version when hydrating at an unchanged stateVersion
    // (in TS's pg test the same got patch instead rides stateVersion "01"
    // after `version-ready`).
    let got = frames
        .iter()
        .filter(|f| f[0] == "pokePart")
        .find_map(|f| f[1].get("gotQueriesPatch").cloned())
        .expect("expected a pokePart with gotQueriesPatch");
    assert!(
        got.as_array()
            .unwrap()
            .iter()
            .any(|op| op["op"] == "put" && op["hash"] == "query-hash1"),
        "expected got put for query-hash1, got {got:?}"
    );

    // CVR state after config + hydrate (TS "responds to changeDesiredQueries
    // patch" asserts the same records via CVRStore.load).
    let state = cell.borrow();
    let cvr = state.cvr.as_ref().expect("CVR loaded");
    assert_eq!(
        cvr.clients.get("c1").map(|c| c.desired_query_ids.clone()),
        Some(vec!["query-hash1".to_string()])
    );
    assert!(
        cvr.queries.contains_key("lmids"),
        "the internal lmids query must be recorded"
    );
    assert_eq!(cvr.version.state_version, "00");
    // 1 = the desired-queries config bump; 2 = the same-state hydrate bump.
    assert_eq!(cvr.version.config_version, Some(2));
}

/// Port of TS view-syncer.pg.test.ts "responds to changeDesiredQueries
/// patch": a `changeDesiredQueries` with `[put query-hash2, del
/// query-hash1]` bumps the config version to 2, pokes both ops to the
/// client, leaves `desiredQueryIDs = [query-hash2]`, and keeps the deleted
/// query-hash1 record with the client's state INACTIVATED (TTL grace), not
/// erased.
#[test]
fn change_desired_queries_pokes_put_and_del_and_updates_cvr() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let cell = shared(tables_state(&rt));
    let mut rx = connect_c1(&rt, &cell);
    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        INIT_CONNECTION_HASH1.to_string(),
    ));
    let _ = drain_sends(&mut rx);

    rt.block_on(on_inbound(
            &cell,
            "c1".into(),
            "ws1".into(),
            r#"["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"query-hash2","ast":{"table":"issue"}},{"op":"del","hash":"query-hash1"}]}]"#
                .to_string(),
        ));
    let frames = drain_sends(&mut rx);

    // After the init's two version bumps (config "00:01" + hydrate "00:02")
    // this change's config poke is "00:03".
    let poke_start = frames
        .iter()
        .find(|f| f[0] == "pokeStart")
        .expect("expected a pokeStart");
    assert_eq!(poke_start[1]["pokeID"], "00:03");
    let c1_ops: Vec<serde_json::Value> = frames
        .iter()
        .filter(|f| f[0] == "pokePart")
        .filter_map(|f| f[1]["desiredQueriesPatches"]["c1"].as_array().cloned())
        .flatten()
        .collect();
    assert!(
        c1_ops
            .iter()
            .any(|op| op["op"] == "put" && op["hash"] == "query-hash2"),
        "expected the put for query-hash2, got {c1_ops:?}"
    );
    assert!(
        c1_ops
            .iter()
            .any(|op| op["op"] == "del" && op["hash"] == "query-hash1"),
        "expected the del for query-hash1, got {c1_ops:?}"
    );
    let poke_end = frames
        .iter()
        .find(|f| f[0] == "pokeEnd")
        .expect("expected a pokeEnd");
    assert_eq!(poke_end[1]["cookie"], "00:03");

    let state = cell.borrow();
    let cvr = state.cvr.as_ref().expect("CVR loaded");
    // "00:03" = the put/del config bump; "00:04" = the same-state hydrate
    // bump for the newly-got query-hash2 (see the init test).
    assert_eq!(cvr.version.config_version, Some(4));
    assert_eq!(
        cvr.clients.get("c1").map(|c| c.desired_query_ids.clone()),
        Some(vec!["query-hash2".to_string()])
    );
    // TS keeps the deleted query record with `clientState.foo.inactivatedAt`
    // set (the TTL grace window), rather than deleting it outright.
    let hash1 = cvr
        .queries
        .get("query-hash1")
        .expect("query-hash1 must survive the del (inactivated, not erased)");
    assert!(
        hash1
            .client_state()
            .and_then(|cs| cs.get("c1"))
            .is_some_and(|cs| cs.inactivated_at.is_some()),
        "query-hash1 must be inactivated for c1"
    );
}

/// Port of TS view-syncer.pg.test.ts "responds to changeDesiredQueries
/// patch" (the old-wsid arm): a changeDesiredQueries arriving on a stale
/// wsID is IGNORED — no poke, no CVR change.
#[test]
fn change_desired_queries_from_stale_ws_is_ignored() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let cell = shared(tables_state(&rt));
    let mut rx = connect_c1(&rt, &cell);
    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        INIT_CONNECTION_HASH1.to_string(),
    ));
    let _ = drain_sends(&mut rx);

    rt.block_on(on_inbound(
            &cell,
            "c1".into(),
            "old-wsid".into(),
            r#"["changeDesiredQueries",{"desiredQueriesPatch":[{"op":"put","hash":"query-hash-1234567890","ast":{"table":"issue"}}]}]"#
                .to_string(),
        ));
    assert!(
        drain_sends(&mut rx).is_empty(),
        "a stale-wsID frame must produce no output"
    );
    let state = cell.borrow();
    let cvr = state.cvr.as_ref().unwrap();
    assert_eq!(
        cvr.clients.get("c1").map(|c| c.desired_query_ids.clone()),
        Some(vec!["query-hash1".to_string()]),
        "the stale frame must not change the desired set"
    );
    // Still at the init's config+hydrate version (see the init test): the
    // stale frame must not bump it further.
    assert_eq!(cvr.version.config_version, Some(2));
}

// The protocol-version gate is TS `Connection.init()` (connection.ts); its
// exact `VersionNotSupported` message is pinned 1:1 by connection.rs
// `init_out_of_range_closes_with_exact_version_not_supported_message`. The
// prod gate is applied on the accept path (`ws_server::accept_connection`)
// with the byte-identical message, since Rust builds `Connection` on the CG
// thread and `on_new_connection` never sees an unvalidated version.

/// Port of `pickToken`'s pinned-user rule through the WIRE dispatch
/// (`["updateAuth", …]` → `handle_update_auth`): a validly-formed token for
/// a different user gets the exact Unauthorized error body and the
/// connection is closed.
#[test]
fn update_auth_cross_user_via_wire_gets_exact_unauthorized_error() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let cell = shared(revalidate_state(&rt, Some(300_000), valid));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let _ = rt.block_on(cell.borrow_mut().on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    let _ = drain_sends(&mut rx);

    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        format!(r#"["updateAuth",{{"auth":"{}"}}]"#, fake_jwt("user-2")),
    ));
    let frames = drain_sends(&mut rx);
    let error = frames
        .iter()
        .find(|f| f[0] == "error")
        .expect("cross-user updateAuth must error");
    assert_eq!(error[1]["kind"], "Unauthorized");
    assert_eq!(
        error[1]["message"],
        "The user id in the new token does not match the previous token. \
             Client groups are pinned to a single user."
    );
    assert!(
        cell.borrow().registered_ws.is_empty(),
        "connection must be closed"
    );
}

/// TS `updateAuth` with an empty/absent token is a no-op: no error, no
/// re-transform, connection stays registered.
#[test]
fn update_auth_empty_token_is_a_noop() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let valid = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let cell = shared(revalidate_state(&rt, Some(300_000), valid));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let _ = rt.block_on(cell.borrow_mut().on_new_connection(
        pinned_params("c1", "ws1", "user-1"),
        DirectWebSocketSink::new(tx),
    ));
    let _ = drain_sends(&mut rx);

    rt.block_on(on_inbound(
        &cell,
        "c1".into(),
        "ws1".into(),
        r#"["updateAuth",{"auth":""}]"#.to_string(),
    ));
    assert!(drain_sends(&mut rx).is_empty(), "no output for empty auth");
    assert_eq!(
        cell.borrow().registered_ws.len(),
        1,
        "connection must survive"
    );
    assert_eq!(cell.borrow().metrics.snapshot()["authChanges"], 0);
}
