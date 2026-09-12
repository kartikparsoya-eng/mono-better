//! Port of the `restartAfter` drift family in TS `view-syncer.pg.test.ts`
//! (lines 4625-5290): a view-syncer hydrates against a SQLite replica and
//! persists the CVR (with each query's `rowSetSignature`) in Postgres; the
//! service is then STOPPED, the replica and/or the stored CVR are mutated
//! underneath it, and a FRESH service starts on the same CVR + replica. The
//! restart must detect a drifted row set (`#hydrateUnchangedQueries`,
//! view-syncer.ts:1659-1673), re-execute only the drifted query, poke the
//! row diff with a bumped cookie, persist the corrected signature — and do
//! NONE of that when the signature matches, is absent (legacy), or is `"0"`.
//!
//! Gated on `TEST_CVR_PG_URI` like `pg_harness.rs`; skipped (and green)
//! without it. Each test owns a PG schema and a replica file.
//!
//! Not ported here: `same-hash rehydrate during deleteClients forces a
//! version bump` (pinned at the unit level by
//! `same_hash_rehydration_forces_bump_matches_ts_guard`), and the two
//! custom-query cases (`custom (named) query diffs rows`, `transformationHash
//! change bypasses the drift check`) — they need the API-server transformer,
//! which this harness does not stub; the hash-change branch is pinned by
//! `changed_transformation_hash_rehydrates_query`.
mod common;
use common::{cvr_ddl, pg_uri};

use std::sync::Arc;
use std::time::{Duration, Instant};

use rusqlite::Connection;
use rust_cvr::client_handler::WebSocketSink;
use rust_cvr::cvr::{CVR, DesiredQuerySpec};
use rust_cvr::row_set_signature::{format_signature, row_id_signature_unit};
use rust_cvr::schema::types::{RowID, version_string};
use rust_cvr::shards::ShardID;
use rust_syncer::services::view_syncer::pipeline_driver::IvmPipelines;
use rust_syncer::services::view_syncer::view_syncer::{
    CustomQueryTransformMode, FlushTimes, ViewSyncerService as SyncEngine,
    empty_cvr as empty_engine_cvr,
};
use rust_syncer::ws_sink::{DirectWebSocketSink, WsCommand};
use serde_json::{Value, json};

const CG: &str = "cg1";
const CLIENT: &str = "client1";
const REPLICA_VERSION: &str = "replica-1";

/// One test's world: a SQLite replica file, a PG schema, a Tokio runtime.
struct World {
    uri: String,
    schema: String,
    db_path: String,
    rt: tokio::runtime::Runtime,
    engines_started: usize,
}

impl World {
    /// TS `setup()` for these tests: the replica carries `issues` (rows 1 and
    /// 2 — TS `pruneIssues('3','4','5')` leaves exactly those) and `users`,
    /// plus the internal tables the syncer's own queries read.
    fn new(schema: &str) -> Option<World> {
        let uri = pg_uri()?;
        let db_path = format!(
            "/tmp/rust-syncer-pg-restart-{schema}-{}.db",
            std::process::id()
        );
        let w = World {
            uri,
            schema: schema.to_string(),
            db_path,
            rt: tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
            engines_started: 0,
        };
        w.cleanup_sqlite();
        w.rt.block_on(async {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .connect(&w.uri)
                .await
                .expect("connect");
            sqlx::raw_sql(&format!(r#"DROP SCHEMA IF EXISTS "{}" CASCADE;"#, w.schema))
                .execute(&pool)
                .await
                .unwrap();
            sqlx::raw_sql(&cvr_ddl(&w.schema))
                .execute(&pool)
                .await
                .unwrap();
        });
        let conn = Connection::open(&w.db_path).unwrap();
        let _ = conn.pragma_update(None, "journal_mode", "wal2");
        conn.execute_batch(
            r#"
            CREATE TABLE "_zero.replicationConfig" (
                lock TEXT PRIMARY KEY DEFAULT 'singleton',
                replicaVersion TEXT NOT NULL,
                publications TEXT NOT NULL
            );
            CREATE TABLE "_zero.replicationState" (
                lock TEXT PRIMARY KEY DEFAULT 'singleton',
                stateVersion TEXT NOT NULL
            );
            CREATE TABLE "_zero.changeLog2" (
                "stateVersion" TEXT NOT NULL,
                "table"        TEXT NOT NULL,
                "rowKey"       TEXT NOT NULL,
                "op"           TEXT NOT NULL,
                "pos"          INTEGER NOT NULL,
                PRIMARY KEY ("stateVersion", "pos")
            );
            CREATE TABLE "app_0.clients" (
                "clientGroupID"  TEXT,
                "clientID"       TEXT,
                "lastMutationID" INTEGER,
                "userID"         TEXT,
                _0_version       TEXT NOT NULL,
                PRIMARY KEY ("clientGroupID", "clientID")
            );
            CREATE TABLE "app_0.mutations" (
                "clientGroupID"  TEXT,
                "clientID"       TEXT,
                "mutationID"     INTEGER,
                "result"         TEXT,
                _0_version       TEXT NOT NULL,
                PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
            );
            CREATE TABLE issues (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                "_0_version" TEXT NOT NULL
            );
            CREATE TABLE users (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                "_0_version" TEXT NOT NULL
            );
            INSERT INTO "_zero.replicationConfig" (lock, replicaVersion, publications)
                VALUES ('singleton', 'replica-1', '[]');
            INSERT INTO "_zero.replicationState" (lock, stateVersion)
                VALUES ('singleton', '01');
            INSERT INTO issues (id, title, "_0_version") VALUES ('1', 'parent issue foo', '01');
            INSERT INTO issues (id, title, "_0_version") VALUES ('2', 'parent issue bar', '01');
            INSERT INTO users (id, name, "_0_version") VALUES ('100', 'Alice', '01');
            INSERT INTO users (id, name, "_0_version") VALUES ('101', 'Bob', '01');
            "#,
        )
        .unwrap();
        Some(w)
    }

    fn cleanup_sqlite(&self) {
        for suffix in ["", "-wal", "-wal2", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.db_path));
        }
    }

    fn replica(&self) -> Connection {
        Connection::open(&self.db_path).unwrap()
    }

    fn state_version(&self) -> String {
        self.replica()
            .query_row(
                "SELECT stateVersion FROM \"_zero.replicationState\"",
                [],
                |r| r.get(0),
            )
            .unwrap()
    }

    /// TS `restartViewSyncer`: a fresh service (fresh pipelines) over the SAME
    /// replica file and CVR schema, with a new task id so the CVR ownership
    /// hand-off is the real one.
    fn start_engine(&mut self) -> SyncEngine {
        self.engines_started += 1;
        let specs = rust_syncer::compute_table_specs_from_path(&self.db_path).unwrap();
        let mut pipelines = IvmPipelines::new();
        pipelines.init(specs, Some(&self.db_path), "app").unwrap();
        let mut engine = SyncEngine::new(pipelines);
        let handle = self.rt.handle().clone();
        let pool = {
            let _g = handle.enter();
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(5)
                .connect_lazy(&self.uri)
                .unwrap()
        };
        engine.set_tokio_handle(handle);
        engine
            .set_cvr_store(
                pool,
                self.schema.clone(),
                CG.to_string(),
                format!("task-{}", self.engines_started),
            )
            .unwrap();
        engine
    }

    /// TS `loadStoredSig`.
    fn stored_sig(&self, hash: &str) -> Option<String> {
        self.rt.block_on(async {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .connect(&self.uri)
                .await
                .unwrap();
            let row: Option<(Option<String>,)> = sqlx::query_as(&format!(
                r#"SELECT "rowSetSignature" FROM "{}".queries
                    WHERE "clientGroupID" = $1 AND "queryHash" = $2"#,
                self.schema
            ))
            .bind(CG)
            .bind(hash)
            .fetch_optional(&pool)
            .await
            .unwrap();
            row.and_then(|(sig,)| sig)
        })
    }

    /// The TS tests' `UPDATE queries SET "rowSetSignature" = …` tamper.
    fn set_stored_sig(&self, hash: &str, sig: Option<&str>) {
        self.rt.block_on(async {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .connect(&self.uri)
                .await
                .unwrap();
            sqlx::query(&format!(
                r#"UPDATE "{}".queries SET "rowSetSignature" = $1
                    WHERE "clientGroupID" = $2 AND "queryHash" = $3"#,
                self.schema
            ))
            .bind(sig)
            .bind(CG)
            .bind(hash)
            .execute(&pool)
            .await
            .unwrap();
        });
    }

    /// TS `vi.waitFor(() => expect(await loadStoredSig(h)).toEqual(sig))`:
    /// the corrected signature is written by the flush, so poll for it.
    fn wait_stored_sig(&self, hash: &str, want: Option<&str>) -> Option<String> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let got = self.stored_sig(hash);
            if got.as_deref() == want || Instant::now() > deadline {
                return got;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn drop_schema(&self) {
        self.rt.block_on(async {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .connect(&self.uri)
                .await
                .unwrap();
            sqlx::raw_sql(&format!(
                r#"DROP SCHEMA IF EXISTS "{}" CASCADE;"#,
                self.schema
            ))
            .execute(&pool)
            .await
            .unwrap();
        });
        self.cleanup_sqlite();
    }
}

/// TS `expectedIssuesSig`: XOR-fold of `rowIDSignatureUnit` over the row ids.
fn expected_sig(table: &str, ids: &[&str]) -> String {
    let sig = ids.iter().fold(0u64, |acc, id| {
        let mut key = serde_json::Map::new();
        key.insert("id".to_string(), Value::String(id.to_string()));
        acc ^ row_id_signature_unit(&RowID {
            schema: String::new(),
            table: Arc::from(table),
            row_key: key,
        })
    });
    format_signature(sig)
}

fn anyone_can() -> Value {
    let allow = json!([["allow", {"type": "and", "conditions": []}]]);
    json!({"tables": {
        "issues": {"row": {"select": allow}},
        "users": {"row": {"select": allow}},
    }})
}

fn put(hash: &str, table: &str) -> DesiredQuerySpec {
    DesiredQuerySpec {
        hash: hash.to_string(),
        ast: Some(json!({"table": table})),
        name: None,
        args: None,
        ttl: None,
    }
}

fn times() -> FlushTimes {
    FlushTimes {
        last_connect_time: 0,
        last_active: 0,
        ttl_clock: 0,
    }
}

/// TS `connect(...)` + draining its pokes: register the client (with the
/// reconnect cookie when given) and run the connect-time sync.
fn connect(
    w: &World,
    engine: &mut SyncEngine,
    cvr: CVR,
    ws_id: &str,
    base_cookie: Option<&str>,
    puts: Vec<DesiredQuerySpec>,
) -> (CVR, Vec<Value>) {
    let shard = ShardID {
        app_id: "app".to_string(),
        shard_num: 0,
    };
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
    let sink: Arc<dyn WebSocketSink> = Arc::new(DirectWebSocketSink::new(tx));
    engine.register_client(CLIENT, ws_id, CG, &shard, base_cookie, sink);
    let perms = anyone_can();
    let cvr =
        w.rt.block_on(engine.config_and_hydrate(
            cvr,
            CLIENT,
            &[ws_id.to_string()],
            &shard,
            puts,
            Vec::new(),
            false,
            None,
            CustomQueryTransformMode::All,
            Some(&perms),
            &json!({}),
            None,
            w.state_version(),
            REPLICA_VERSION.to_string(),
            times(),
        ))
        .expect("config_and_hydrate");
    let mut frames = Vec::new();
    while let Ok(cmd) = rx.try_recv() {
        if let Some(f) = cmd.frame_value() {
            frames.push(f);
        }
    }
    (cvr, frames)
}

/// TS `rowOpsFor(poke, table)`: the put ids and del ids for one table across
/// every `pokePart` in the frames.
fn row_ops(frames: &[Value], table: &str) -> (Vec<String>, Vec<String>) {
    let (mut puts, mut dels) = (Vec::new(), Vec::new());
    for f in frames {
        if f[0] != "pokePart" {
            continue;
        }
        for p in f[1]["rowsPatch"].as_array().into_iter().flatten() {
            if p["tableName"] != table {
                continue;
            }
            match p["op"].as_str() {
                Some("put") => puts.push(p["value"]["id"].as_str().unwrap().to_string()),
                Some("del") => dels.push(p["id"]["id"].as_str().unwrap().to_string()),
                _ => {}
            }
        }
    }
    (puts, dels)
}

/// TS `drainUntilRowsPatchOrQuiet(queue) !== undefined`.
fn has_rows_patch(frames: &[Value]) -> bool {
    frames
        .iter()
        .any(|f| f[0] == "pokePart" && f[1]["rowsPatch"].as_array().is_some_and(|a| !a.is_empty()))
}

/// TS `cookieOf(poke)`: the last `pokeEnd` cookie.
fn cookie_of(frames: &[Value]) -> Option<String> {
    frames
        .iter()
        .rev()
        .find(|f| f[0] == "pokeEnd")
        .and_then(|f| f[1]["cookie"].as_str().map(str::to_string))
}

/// TS `restartAfter({mutate, queriesPatch, baseCookie})`: stop the running
/// service, apply `mutate`, start a fresh one, load the persisted CVR and
/// reconnect on `ws2` with the given cookie.
fn restart_after(
    w: &mut World,
    engine: SyncEngine,
    mutate: impl FnOnce(&World),
    base_cookie: Option<&str>,
    puts: Vec<DesiredQuerySpec>,
) -> (SyncEngine, CVR, Vec<Value>) {
    drop(engine);
    mutate(w);
    let mut fresh = w.start_engine();
    let loaded =
        w.rt.block_on(fresh.load_cvr(0.0))
            .unwrap()
            .expect("the stopped service persisted its CVR");
    let (cvr, frames) = connect(w, &mut fresh, loaded, "ws2", base_cookie, puts);
    (fresh, cvr, frames)
}

/// TS `deleteIssue` / `insertIssue`: direct replica writes with NO
/// stateVersion bump — the row set a re-execution sees at the same version.
fn delete_issue(w: &World, id: &str) {
    w.replica()
        .execute("DELETE FROM issues WHERE id = ?1", [id])
        .unwrap();
}

fn insert_issue(w: &World, id: &str, title: &str) {
    w.replica()
        .execute(
            "INSERT INTO issues (id, title, \"_0_version\") VALUES (?1, ?2, '01')",
            [id, title],
        )
        .unwrap();
}

/// Port of TS `rowSetSignature persisted by end-to-end hydrate and advance`
/// (view-syncer.pg.test.ts:4664): the stored signature is the XOR of the
/// hydrated rows' units, an advance that deletes a row removes its unit and
/// an update that keeps the row (row-version bump only) leaves it alone.
#[test]
fn pg_row_set_signature_persisted_by_hydrate_and_advance() {
    let Some(mut w) = World::new("cvr_sig_persisted") else {
        eprintln!(
            "SKIP pg_row_set_signature_persisted_by_hydrate_and_advance: TEST_CVR_PG_URI not set"
        );
        return;
    };
    {
        let conn = w.replica();
        conn.execute_batch(
            r#"INSERT INTO issues (id, title, "_0_version") VALUES ('3', 'three', '01');
               INSERT INTO issues (id, title, "_0_version") VALUES ('4', 'four', '01');"#,
        )
        .unwrap();
    }
    let mut engine = w.start_engine();
    let (cvr, _) = connect(
        &w,
        &mut engine,
        empty_engine_cvr(CG, REPLICA_VERSION),
        "ws1",
        None,
        vec![put("query-hash1", "issues")],
    );
    assert_eq!(
        w.wait_stored_sig(
            "query-hash1",
            Some(&expected_sig("issues", &["1", "2", "3", "4"]))
        ),
        Some(expected_sig("issues", &["1", "2", "3", "4"])),
        "hydrate persists the XOR of the four rows' units"
    );

    // Advance: delete issue 3 (leaves the query), update issue 4 (stays in the
    // query, row-version bump only — must not change the signature).
    w.replica()
        .execute_batch(
            r#"
            BEGIN;
            DELETE FROM issues WHERE id = '3';
            UPDATE issues SET title = 'edited bar', "_0_version" = '02' WHERE id = '4';
            INSERT INTO "_zero.changeLog2" ("stateVersion", "table", "rowKey", "op", "pos")
                VALUES ('02', 'issues', '{"id":"3"}', 'd', 0),
                       ('02', 'issues', '{"id":"4"}', 's', 1);
            UPDATE "_zero.replicationState" SET stateVersion = '02';
            COMMIT;
            "#,
        )
        .unwrap();
    let advanced =
        w.rt.block_on(engine.advance_and_sync(cvr, REPLICA_VERSION.to_string(), &[], times()))
            .expect("advance");
    assert!(advanced.reset_reason.is_none(), "advance must not reset");
    assert_eq!(
        w.wait_stored_sig(
            "query-hash1",
            Some(&expected_sig("issues", &["1", "2", "4"]))
        ),
        Some(expected_sig("issues", &["1", "2", "4"])),
        "the deleted row's unit leaves the signature; the updated row's stays"
    );
    drop(engine);
    w.drop_schema();
}

/// Port of TS `rowSetSignature drift on rehydration triggers re-execution and
/// CVR correction` (view-syncer.pg.test.ts:4703): a stored signature that
/// does not match the re-hydrated row set (a prior non-deterministic
/// hydration) is corrected on restart.
#[test]
fn pg_row_set_signature_drift_on_rehydration_corrects_cvr() {
    let Some(mut w) = World::new("cvr_sig_drift_corrects") else {
        eprintln!(
            "SKIP pg_row_set_signature_drift_on_rehydration_corrects_cvr: TEST_CVR_PG_URI not set"
        );
        return;
    };
    let mut engine = w.start_engine();
    let (_cvr, _) = connect(
        &w,
        &mut engine,
        empty_engine_cvr(CG, REPLICA_VERSION),
        "ws1",
        None,
        vec![put("query-hash1", "issues")],
    );
    let correct = expected_sig("issues", &["1", "2"]);
    assert_eq!(
        w.wait_stored_sig("query-hash1", Some(&correct)),
        Some(correct.clone())
    );

    const BOGUS_SIG: &str = "deadbeefcafebabe";
    let (engine, _, _) = restart_after(
        &mut w,
        engine,
        |w| {
            w.set_stored_sig("query-hash1", Some(BOGUS_SIG));
            assert_eq!(w.stored_sig("query-hash1").as_deref(), Some(BOGUS_SIG));
        },
        None,
        vec![put("query-hash1", "issues")],
    );
    assert_eq!(
        w.wait_stored_sig("query-hash1", Some(&correct)),
        Some(correct),
        "drift detection must re-execute the query and flush the corrected signature"
    );
    drop(engine);
    w.drop_schema();
}

/// Port of TS `rowSetSignature drift diffs rows: del A, put C, keep B; version
/// is bumped` (view-syncer.pg.test.ts:4752): with the replica mutated
/// underneath a stopped service (no stateVersion bump), the restart's
/// re-execution pokes exactly the diff — A retracted, C added, B untouched —
/// past the pre-drift cookie, and persists the corrected signature.
#[test]
fn pg_row_set_signature_drift_diffs_rows_and_bumps_version() {
    let Some(mut w) = World::new("cvr_sig_drift_diff") else {
        eprintln!(
            "SKIP pg_row_set_signature_drift_diffs_rows_and_bumps_version: TEST_CVR_PG_URI not set"
        );
        return;
    };
    let mut engine = w.start_engine();
    let (_cvr, frames) = connect(
        &w,
        &mut engine,
        empty_engine_cvr(CG, REPLICA_VERSION),
        "ws1",
        None,
        vec![put("query-hash1", "issues")],
    );
    let phase1_cookie = cookie_of(&frames).expect("phase 1 poke cookie");
    assert_eq!(
        w.wait_stored_sig("query-hash1", Some(&expected_sig("issues", &["1", "2"]))),
        Some(expected_sig("issues", &["1", "2"]))
    );

    let (engine, _, frames) = restart_after(
        &mut w,
        engine,
        |w| {
            delete_issue(w, "1");
            insert_issue(w, "6", "row C");
        },
        Some(&phase1_cookie),
        vec![put("query-hash1", "issues")],
    );
    assert!(
        has_rows_patch(&frames),
        "drift must poke a row diff; frames={frames:?}"
    );
    let end_cookie = cookie_of(&frames).expect("pokeEnd cookie");
    // (1) Version is bumped past the pre-drift cookie.
    assert!(
        end_cookie > phase1_cookie,
        "cookie {end_cookie} must be past the pre-drift cookie {phase1_cookie}"
    );
    // (2) Diff semantics: A retracted, C added, B not re-emitted.
    let (puts, dels) = row_ops(&frames, "issues");
    assert_eq!(dels, vec!["1"], "A is retracted");
    assert_eq!(puts, vec!["6"], "C is added");
    assert!(
        !puts.contains(&"2".to_string()) && !dels.contains(&"2".to_string()),
        "B untouched"
    );
    // Corrected signature is persisted.
    assert_eq!(
        w.wait_stored_sig("query-hash1", Some(&expected_sig("issues", &["2", "6"]))),
        Some(expected_sig("issues", &["2", "6"]))
    );
    drop(engine);
    w.drop_schema();
}

/// Port of TS `rowSetSignature match on rehydration: no re-execution, no
/// row-diff poke` (view-syncer.pg.test.ts:4919): nothing changed between the
/// phases, so the restart's re-hydration matches the stored signature — no
/// drift, no row-diff poke, signature untouched.
#[test]
fn pg_row_set_signature_match_on_rehydration_no_reexecution() {
    let Some(mut w) = World::new("cvr_sig_match") else {
        eprintln!(
            "SKIP pg_row_set_signature_match_on_rehydration_no_reexecution: TEST_CVR_PG_URI not set"
        );
        return;
    };
    let mut engine = w.start_engine();
    let (_cvr, frames) = connect(
        &w,
        &mut engine,
        empty_engine_cvr(CG, REPLICA_VERSION),
        "ws1",
        None,
        vec![put("query-hash1", "issues")],
    );
    let phase1_cookie = cookie_of(&frames).expect("phase 1 poke cookie");
    let expected = expected_sig("issues", &["1", "2"]);
    assert_eq!(
        w.wait_stored_sig("query-hash1", Some(&expected)),
        Some(expected.clone())
    );

    let (engine, _, frames) = restart_after(
        &mut w,
        engine,
        |_| {},
        Some(&phase1_cookie),
        vec![put("query-hash1", "issues")],
    );
    assert!(
        !has_rows_patch(&frames),
        "no drift → no row-diff poke; frames={frames:?}"
    );
    assert_eq!(
        w.stored_sig("query-hash1"),
        Some(expected),
        "no re-execution ran, so nothing rewrote the signature"
    );
    drop(engine);
    w.drop_schema();
}

/// Port of TS `rowSetSignature drift: only the drifted query is re-executed`
/// (view-syncer.pg.test.ts:4966): query A (issues) drifts, query B (users) is
/// untouched — the poke carries only A's diff, only A's signature changes.
#[test]
fn pg_row_set_signature_drift_only_drifted_query_reexecuted() {
    let Some(mut w) = World::new("cvr_sig_drift_only") else {
        eprintln!(
            "SKIP pg_row_set_signature_drift_only_drifted_query_reexecuted: TEST_CVR_PG_URI not set"
        );
        return;
    };
    let mut engine = w.start_engine();
    let (_cvr, frames) = connect(
        &w,
        &mut engine,
        empty_engine_cvr(CG, REPLICA_VERSION),
        "ws1",
        None,
        vec![put("query-hash-A", "issues"), put("query-hash-B", "users")],
    );
    let phase1_cookie = cookie_of(&frames).expect("phase 1 poke cookie");
    let sig_a = expected_sig("issues", &["1", "2"]);
    assert_eq!(w.wait_stored_sig("query-hash-A", Some(&sig_a)), Some(sig_a));
    let sig_b = w.wait_stored_sig(
        "query-hash-B",
        Some(&expected_sig("users", &["100", "101"])),
    );
    assert!(sig_b.is_some(), "B's signature is persisted");

    let (engine, _, frames) = restart_after(
        &mut w,
        engine,
        |w| {
            delete_issue(w, "1");
            insert_issue(w, "6", "row C");
        },
        Some(&phase1_cookie),
        vec![put("query-hash-A", "issues"), put("query-hash-B", "users")],
    );
    assert!(
        has_rows_patch(&frames),
        "A drifted → a row-diff poke; frames={frames:?}"
    );
    let (issue_puts, issue_dels) = row_ops(&frames, "issues");
    let (user_puts, user_dels) = row_ops(&frames, "users");
    assert_eq!(issue_dels, vec!["1"]);
    assert_eq!(issue_puts, vec!["6"]);
    assert!(
        user_puts.is_empty() && user_dels.is_empty(),
        "B did not drift: no users rows in the poke"
    );
    assert_eq!(
        w.wait_stored_sig("query-hash-A", Some(&expected_sig("issues", &["2", "6"]))),
        Some(expected_sig("issues", &["2", "6"]))
    );
    assert_eq!(
        w.stored_sig("query-hash-B"),
        sig_b,
        "B's signature is unchanged"
    );
    drop(engine);
    w.drop_schema();
}

/// Port of TS `rowSetSignature absent (legacy): drift check is skipped, no
/// forced re-execution` (view-syncer.pg.test.ts:5036): a query record
/// written before signatures existed has none stored; the restart must NOT
/// treat that as drift (or every pre-feature client would get every row
/// re-sent), even though the replica changed underneath. The signature is
/// initialised by the flush's pass over the queries, not by a re-execution.
#[test]
fn pg_row_set_signature_absent_legacy_skips_drift_check() {
    let Some(mut w) = World::new("cvr_sig_legacy_null") else {
        eprintln!(
            "SKIP pg_row_set_signature_absent_legacy_skips_drift_check: TEST_CVR_PG_URI not set"
        );
        return;
    };
    let mut engine = w.start_engine();
    let (_cvr, frames) = connect(
        &w,
        &mut engine,
        empty_engine_cvr(CG, REPLICA_VERSION),
        "ws1",
        None,
        vec![put("query-hash1", "issues")],
    );
    let phase1_cookie = cookie_of(&frames).expect("phase 1 poke cookie");
    assert_eq!(
        w.wait_stored_sig("query-hash1", Some(&expected_sig("issues", &["1", "2"]))),
        Some(expected_sig("issues", &["1", "2"]))
    );

    let (engine, _, frames) = restart_after(
        &mut w,
        engine,
        |w| {
            w.set_stored_sig("query-hash1", None);
            assert_eq!(w.stored_sig("query-hash1"), None);
            delete_issue(w, "1");
            insert_issue(w, "6", "would be drift if detection fired");
        },
        Some(&phase1_cookie),
        vec![put("query-hash1", "issues")],
    );
    assert!(
        !has_rows_patch(&frames),
        "a legacy null signature must not force a re-execution; frames={frames:?}"
    );
    assert_eq!(
        w.wait_stored_sig("query-hash1", Some(&expected_sig("issues", &["2", "6"]))),
        Some(expected_sig("issues", &["2", "6"])),
        "the signature is initialised from the re-hydrated rows on this cycle"
    );
    drop(engine);
    w.drop_schema();
}

/// Port of TS `rowSetSignature: emptied-by-advance query rehydrates to "0"
/// without drift` (view-syncer.pg.test.ts:5141): an advance that removes
/// every row XORs each unit back out, so the CVR persists `"0"`; a restart
/// re-hydrates zero rows (candidate 0) and must read `"0"` as a match.
#[test]
fn pg_row_set_signature_emptied_by_advance_rehydrates_to_zero() {
    let Some(mut w) = World::new("cvr_sig_emptied") else {
        eprintln!(
            "SKIP pg_row_set_signature_emptied_by_advance_rehydrates_to_zero: TEST_CVR_PG_URI not set"
        );
        return;
    };
    let mut engine = w.start_engine();
    let (cvr, _) = connect(
        &w,
        &mut engine,
        empty_engine_cvr(CG, REPLICA_VERSION),
        "ws1",
        None,
        vec![put("query-hash1", "issues")],
    );
    assert_eq!(
        w.wait_stored_sig("query-hash1", Some(&expected_sig("issues", &["1", "2"]))),
        Some(expected_sig("issues", &["1", "2"]))
    );
    w.replica()
        .execute_batch(
            r#"
            BEGIN;
            DELETE FROM issues WHERE id IN ('1', '2');
            INSERT INTO "_zero.changeLog2" ("stateVersion", "table", "rowKey", "op", "pos")
                VALUES ('02', 'issues', '{"id":"1"}', 'd', 0),
                       ('02', 'issues', '{"id":"2"}', 'd', 1);
            UPDATE "_zero.replicationState" SET stateVersion = '02';
            COMMIT;
            "#,
        )
        .unwrap();
    let advanced =
        w.rt.block_on(engine.advance_and_sync(cvr, REPLICA_VERSION.to_string(), &[], times()))
            .expect("advance");
    assert!(advanced.reset_reason.is_none(), "advance must not reset");
    let phase1_cookie = version_string(&advanced.cvr.version);
    assert_eq!(
        w.wait_stored_sig("query-hash1", Some("0")),
        Some("0".to_string()),
        "removing every row XORs the signature back to 0"
    );

    let (engine, _, frames) = restart_after(
        &mut w,
        engine,
        |_| {},
        Some(&phase1_cookie),
        vec![put("query-hash1", "issues")],
    );
    assert!(
        !has_rows_patch(&frames),
        "stored \"0\" matches an empty re-hydration: no drift; frames={frames:?}"
    );
    assert_eq!(w.stored_sig("query-hash1"), Some("0".to_string()));
    drop(engine);
    w.drop_schema();
}
