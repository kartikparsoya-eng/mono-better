//! End-to-end tests for the `analyzeQuery` subsystem over a real lite replica.
//!
//! These build a throwaway SQLite replica file (the same shape the replicator
//! produces: syncable tables with `_0_version`, a UNIQUE row-key index), then
//! drive `analyze_query` — the port of TS `analyzeQuery` (analyze.ts) that the
//! `analyze-query` inspector op calls on a blocking thread.
//!
//! NON-VACUOUS (A1, applyPermissions): the SAME 3-row query returns 0 rows under
//! a deny-all permissions config and all 3 under ANYONE_CAN. Reverting the
//! read-permission transform in `run_ast` (dropping `transform_and_hash_query`)
//! makes the deny-all case return 3 rows and the assertion fails.

use rusqlite::Connection;
use serde_json::json;

/// Create a minimal replica file with a `users` table (3 rows) shaped like a
/// real Zero replica: `text|NOT_NULL` lite types, an explicit UNIQUE row-key
/// index on `id`, and the `_0_version` row-version column. Returns the path.
fn build_users_replica(path: &str) {
    cleanup(path);
    let conn = Connection::open(path).unwrap();
    // The snapshotter requires the replica be in wal2 mode (matches production).
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
        INSERT INTO "_zero.replicationConfig" (lock, replicaVersion, publications)
            VALUES ('singleton', 'v1', '[]');
        INSERT INTO "_zero.replicationState" (lock, stateVersion)
            VALUES ('singleton', 'v1');

        CREATE TABLE "users" (
            "id"          "text|NOT_NULL",
            "name"        "text|NOT_NULL",
            "_0_version"  "text"
        );
        CREATE UNIQUE INDEX "u_id" ON "users" ("id");
        INSERT INTO "users" ("id", "name", "_0_version") VALUES
            ('u1', 'Alice', '01'),
            ('u2', 'Bob',   '01'),
            ('u3', 'Carol', '01');
        "#,
    )
    .unwrap();
}

fn cleanup(path: &str) {
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}

fn tmp_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!(
            "rust_syncer_analyze_{}_{}.db",
            tag,
            std::process::id()
        ))
        .to_string_lossy()
        .to_string()
}

/// ANYONE_CAN: a single always-true select rule (`{and, conditions:[]}`).
fn anyone_can(table: &str) -> serde_json::Value {
    json!({
        "tables": { table: { "row": { "select": [["allow", {"type":"and","conditions":[]}]] } } }
    })
}

fn users_ast() -> String {
    // A plain scan of all users, ordered by the primary key.
    json!({"table": "users", "orderBy": [["id", "asc"]]}).to_string()
}

#[test]
fn analyze_applies_read_permissions() {
    let path = tmp_path("perms");
    build_users_replica(&path);
    let ast = users_ast();

    // No permissions passed → no transform → the full table (baseline).
    let none = rust_syncer::services::analyze::analyze_query(
        &path, "app", &ast, true, false, None, None, false,
    )
    .expect("analyze without permissions");
    assert_eq!(
        none.synced_row_count, 3,
        "without permissions the analysis sees all 3 rows"
    );

    // ANYONE_CAN → an always-true rule → still all 3 rows.
    let allow = rust_syncer::services::analyze::analyze_query(
        &path,
        "app",
        &ast,
        true,
        false,
        Some(anyone_can("users")),
        None,
        false,
    )
    .expect("analyze with ANYONE_CAN");
    assert_eq!(
        allow.synced_row_count, 3,
        "ANYONE_CAN permits all 3 rows; got {allow:?}"
    );

    // Deny-all (no select rule for `users`) → the read-authorizer rewrites the
    // WHERE to an always-false empty-OR → 0 rows. This is the security-critical
    // assertion the applyPermissions wiring exists to satisfy.
    let deny = rust_syncer::services::analyze::analyze_query(
        &path,
        "app",
        &ast,
        true,
        false,
        Some(rust_syncer::deny_all_permissions()),
        None,
        false,
    )
    .expect("analyze with deny-all");
    assert_eq!(
        deny.synced_row_count, 0,
        "deny-all permissions must filter EVERY row; got {} (rows={:?})",
        deny.synced_row_count, deny.synced_rows
    );

    // applyPermissions with no auth pushes the TS no-auth warning (runAst:77).
    assert!(
        deny.warnings
            .iter()
            .any(|w| w.contains("No auth data provided")),
        "applyPermissions without auth warns about NULL auth-data comparison; got {:?}",
        deny.warnings
    );

    cleanup(&path);
}

/// A4: the requesting connection's decoded JWT claims (`authData`) bind the
/// permission rules' static parameters. NON-VACUOUS: a rule `name = authData.name`
/// returns only the matching row when auth is present, and 0 rows (+ the no-auth
/// warning) when auth is absent. If A4's auth threading is reverted (auth forced
/// to `None`), the authed case returns 0 instead of 1 and the assertion fails.
#[test]
fn analyze_binds_auth_data_into_permission_rules() {
    let path = tmp_path("auth");
    build_users_replica(&path);
    let ast = users_ast();

    // A select rule: a user may read a `users` row only when its `name` equals
    // the caller's `authData.name`.
    let perms = json!({
        "tables": { "users": { "row": { "select": [[
            "allow",
            {"type":"simple","op":"=",
             "left":{"type":"column","name":"name"},
             "right":{"type":"static","anchor":"authData","field":"name"}}
        ]]}}}
    });

    // Authed as Alice → the static binds to "Alice" → exactly Alice's row.
    let authed = rust_syncer::services::analyze::analyze_query(
        &path,
        "app",
        &ast,
        true,
        false,
        Some(perms.clone()),
        Some(json!({"name": "Alice"})),
        false,
    )
    .expect("analyze authed");
    assert_eq!(
        authed.synced_row_count, 1,
        "auth 'Alice' permits exactly the one matching row; got {} (rows={:?})",
        authed.synced_row_count, authed.synced_rows
    );

    // No auth → the static resolves to NULL → `name = NULL` matches nothing.
    let anon = rust_syncer::services::analyze::analyze_query(
        &path,
        "app",
        &ast,
        true,
        false,
        Some(perms),
        None,
        false,
    )
    .expect("analyze anon");
    assert_eq!(
        anon.synced_row_count, 0,
        "without auth the rule compares against NULL and returns 0 rows; got {}",
        anon.synced_row_count
    );
    assert!(
        anon.warnings
            .iter()
            .any(|w| w.contains("No auth data provided")),
        "anon analyze warns about NULL auth-data; got {:?}",
        anon.warnings
    );

    cleanup(&path);
}

/// A5: after analyze, `sqlitePlans` carries a plan for EVERY vended SQL in
/// `readRowCountsByQuery` — the explainQueries fallback (analyze.ts:112-119)
/// fills any query SQLite prepared but did not populate a scanstatus EXPLAIN
/// for. This is the TS post-condition: no vended query is left without a plan.
/// NON-VACUOUS on builds without SQLITE_ENABLE_STMT_SCANSTATUS (where run_ast
/// captures no plans and the fallback is the ONLY source): reverting the
/// fallback leaves sqlitePlans empty while readRowCountsByQuery is non-empty.
#[test]
fn analyze_fills_sqlite_plans_for_every_vended_query() {
    let path = tmp_path("plans");
    build_users_replica(&path);
    let ast = users_ast();

    let result = rust_syncer::services::analyze::analyze_query(
        &path, "app", &ast, true, false, None, None, false,
    )
    .expect("analyze");

    let read_counts = result
        .read_row_counts_by_query
        .clone()
        .expect("readRowCountsByQuery present");
    // The users scan vended rows, so there is at least one SQL to plan.
    let all_sqls: Vec<String> = read_counts
        .values()
        .flat_map(|by_sql| by_sql.keys().cloned())
        .collect();
    assert!(
        !all_sqls.is_empty(),
        "the users scan vended at least one SQL; got {read_counts:?}"
    );

    let plans = result.sqlite_plans.clone().unwrap_or_default();
    for sql in &all_sqls {
        assert!(
            plans.contains_key(sql),
            "every vended SQL must have a sqlitePlan (explainQueries fallback); \
             missing plan for {sql:?}; plans={:?}",
            plans.keys().collect::<Vec<_>>()
        );
    }

    cleanup(&path);
}

/// B6: when permissions are applied, `afterPermissions` renders the transformed
/// query back to ZQL (`ast.table + astToZQL(ast)`); with no permissions it is
/// absent. NON-VACUOUS: reverting the afterPermissions wiring in run_ast leaves
/// the field `None` even under applyPermissions and the is_some assertion fails.
#[test]
fn analyze_populates_after_permissions() {
    let path = tmp_path("after");
    build_users_replica(&path);
    let ast = users_ast();

    // ANYONE_CAN → the transformed WHERE is always-true (renders to nothing),
    // leaving the orderBy — so afterPermissions is "users.orderBy('id', 'asc')".
    let allow = rust_syncer::services::analyze::analyze_query(
        &path,
        "app",
        &ast,
        true,
        false,
        Some(anyone_can("users")),
        None,
        false,
    )
    .expect("analyze with ANYONE_CAN");
    let after = allow
        .after_permissions
        .as_deref()
        .expect("afterPermissions is set when permissions are applied");
    assert!(
        after.starts_with("users") && after.contains(".orderBy('id', 'asc')"),
        "afterPermissions renders the transformed query as ZQL; got {after:?}"
    );

    // No permissions applied → afterPermissions omitted (TS leaves it undefined).
    let none = rust_syncer::services::analyze::analyze_query(
        &path, "app", &ast, true, false, None, None, false,
    )
    .expect("analyze without permissions");
    assert!(
        none.after_permissions.is_none(),
        "afterPermissions is absent when permissions are not applied; got {:?}",
        none.after_permissions
    );

    cleanup(&path);
}

/// Build a 2-table replica (`issue` + `comment`, FK `comment.issueId`) so an
/// EXISTS query produces a flippable semi-join for the planner to enumerate.
fn build_issues_comments_replica(path: &str) {
    cleanup(path);
    let conn = Connection::open(path).unwrap();
    let _ = conn.pragma_update(None, "journal_mode", "wal2");
    conn.execute_batch(
        r#"
        CREATE TABLE "_zero.replicationConfig" (
            lock TEXT PRIMARY KEY DEFAULT 'singleton',
            replicaVersion TEXT NOT NULL, publications TEXT NOT NULL);
        CREATE TABLE "_zero.replicationState" (
            lock TEXT PRIMARY KEY DEFAULT 'singleton', stateVersion TEXT NOT NULL);
        INSERT INTO "_zero.replicationConfig" VALUES ('singleton', 'v1', '[]');
        INSERT INTO "_zero.replicationState" VALUES ('singleton', 'v1');

        CREATE TABLE "issue" ("id" "text|NOT_NULL", "_0_version" "text");
        CREATE UNIQUE INDEX "i_id" ON "issue" ("id");
        INSERT INTO "issue" VALUES ('i1','01'),('i2','01'),('i3','01');

        CREATE TABLE "comment" (
            "id" "text|NOT_NULL", "issueId" "text|NOT_NULL", "_0_version" "text");
        CREATE UNIQUE INDEX "c_id" ON "comment" ("id");
        INSERT INTO "comment" VALUES ('c1','i1','01'),('c2','i1','01'),('c3','i2','01');
        "#,
    )
    .unwrap();
}

/// `issue.whereExists('comments')` — an EXISTS over the `comment` relationship,
/// which the planner models as a flippable semi-join.
fn exists_ast() -> String {
    json!({
        "table": "issue",
        "where": {
            "type": "correlatedSubquery",
            "op": "EXISTS",
            "related": {
                "correlation": {"parentField": ["id"], "childField": ["issueId"]},
                "subquery": {"table": "comment", "alias": "zsubq_comments"}
            }
        },
        "orderBy": [["id", "asc"]]
    })
    .to_string()
}

/// B7: with `joinPlans` enabled, `join_plans` carries the planner's decision
/// trace (attempt-start / plan-complete / best-plan-selected / node-cost) for a
/// query with a flippable join; with `joinPlans` disabled it is absent.
/// NON-VACUOUS: reverting the planner instrumentation (or the run_ast wiring)
/// leaves `join_plans` empty/None and the assertions fail.
#[test]
fn analyze_join_plans_emits_planner_events() {
    let path = tmp_path("joinplans");
    build_issues_comments_replica(&path);
    let ast = exists_ast();

    // joinPlans=false → the field is omitted.
    let off = rust_syncer::services::analyze::analyze_query(
        &path, "app", &ast, true, false, None, None, false,
    )
    .expect("analyze joinPlans off");
    assert!(
        off.join_plans.is_none(),
        "joinPlans disabled ⇒ join_plans is absent; got {:?}",
        off.join_plans
    );

    // joinPlans=true → a non-empty event stream with the graph-level events.
    let on = rust_syncer::services::analyze::analyze_query(
        &path, "app", &ast, true, false, None, None, true,
    )
    .expect("analyze joinPlans on");
    let events = on
        .join_plans
        .expect("joinPlans enabled ⇒ join_plans present");
    assert!(!events.is_empty(), "the planner emitted at least one event");

    let types: std::collections::HashSet<&str> = events
        .iter()
        .filter_map(|e| e.get("type").and_then(|v| v.as_str()))
        .collect();
    for expected in [
        "attempt-start",
        "plan-complete",
        "best-plan-selected",
        "node-cost",
    ] {
        assert!(
            types.contains(expected),
            "join_plans must contain a {expected:?} event; got types {types:?}"
        );
    }

    // The exhaustive planner enumerates 2^1 = 2 flip attempts for one flippable
    // join, so there are exactly two `attempt-start` events (patterns 0 and 1).
    let attempts = events
        .iter()
        .filter(|e| e.get("type").and_then(|v| v.as_str()) == Some("attempt-start"))
        .count();
    assert_eq!(attempts, 2, "one flippable join ⇒ 2 flip-pattern attempts");

    // node-cost events are stamped with the current attempt number.
    let stamped = events.iter().any(|e| {
        e.get("type").and_then(|v| v.as_str()) == Some("node-cost")
            && e.get("attemptNumber").is_some()
    });
    assert!(stamped, "node-cost events are stamped with attemptNumber");

    cleanup(&path);
}
