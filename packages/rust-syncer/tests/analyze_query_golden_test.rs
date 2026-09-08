//! Full `AnalyzeQueryResult` TS-golden parity test.
//!
//! Drives the REAL TypeScript `analyzeQuery` (via `tests/ts_golden_analyze.mts`
//! under `npx tsx`) AND the Rust `analyze_query` port over the SAME SQLite
//! replica, and asserts the two `AnalyzeQueryResult`s are field-for-field equal
//! (excluding the nondeterministic `start`/`end`/`elapsed` timings). This pins
//! TS parity for the WHOLE result shape at once — synced rows (incl.
//! `_0_version`), the vended-SQL keys, row counts, db scans, and the sqlite
//! plan text — not merely the component goldens (hashOfAST / astToZQL / TDigest)
//! that were already byte-exact.
//!
//! Requires `node`/`npx tsx` + an installed workspace (node_modules). When tsx
//! cannot be invoked (a minimal CI without `pnpm install`), the test SKIPS with
//! a clear message rather than failing — mirroring the PG-gated tests. When it
//! DOES run it is non-vacuous: a divergence in the Rust port (a different SQL
//! string, a missing row-version column, a wrong row count) fails the
//! `assert_eq!` on the normalized results.

use std::process::Command;

use serde_json::{Value, json};

fn cleanup(path: &str) {
    for suffix in ["", "-wal", "-wal2", "-shm", "-journal"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}

fn tmp_path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!(
            "rs_analyze_golden_{}_{}.db",
            tag,
            std::process::id()
        ))
        .to_string_lossy()
        .to_string()
}

fn harness_path() -> String {
    format!("{}/tests/ts_golden_analyze.mts", env!("CARGO_MANIFEST_DIR"))
}

fn repo_root() -> String {
    format!("{}/../..", env!("CARGO_MANIFEST_DIR"))
}

/// Build the golden replica via the TS harness (a `zqlite` `Database`). This is
/// the SHARED replica both readers open: crucially, zqlite runs `PRAGMA
/// optimize` on dispose, so the replica carries the `sqlite_stat1`/`sqlite_stat4`
/// tables TS's `createSQLiteCostModel` reads — matching a production replica
/// (the replicator ANALYZEs). A rusqlite-built replica lacks `sqlite_stat4`
/// (the wal2-sqlite build has no `SQLITE_ENABLE_STAT4`), so TS's cost model
/// would throw. Returns `false` when tsx cannot be invoked (skip).
fn build_ts_replica(path: &str, fixture: &str) -> bool {
    cleanup(path);
    let out = Command::new("npx")
        .current_dir(repo_root())
        .args(["tsx", &harness_path(), "build", path, fixture])
        .output();
    match out {
        Ok(o) if o.status.success() => true,
        Ok(o) => {
            let stderr = String::from_utf8_lossy(&o.stderr);
            if stderr.contains("Cannot find")
                || stderr.contains("not found")
                || stderr.contains("ENOENT")
            {
                eprintln!("SKIP: tsx harness unavailable (build):\n{stderr}");
                false
            } else {
                panic!("TS replica build failed:\n{stderr}");
            }
        }
        Err(e) => {
            eprintln!("SKIP: cannot invoke `npx tsx` ({e}); TS golden not run");
            false
        }
    }
}

/// Strip the nondeterministic timing fields so two runs are comparable.
fn normalize(mut v: Value) -> Value {
    if let Value::Object(map) = &mut v {
        map.remove("start");
        map.remove("end");
        map.remove("elapsed");
    }
    v
}

/// Run the TS `analyzeQuery` golden over `replica_path`, returning its
/// `AnalyzeQueryResult` JSON — or `None` when tsx cannot be invoked (skip).
#[allow(clippy::too_many_arguments)]
fn ts_golden(
    replica_path: &str,
    ast_json: &str,
    synced: bool,
    vended: bool,
    perms: &str,
    auth: &str,
    join_plans: bool,
    client_schema: &str,
) -> Option<Value> {
    let out = Command::new("npx")
        .current_dir(repo_root())
        .args([
            "tsx",
            &harness_path(),
            "analyze",
            replica_path,
            ast_json,
            if synced { "1" } else { "0" },
            if vended { "1" } else { "0" },
            perms,
            auth,
            if join_plans { "1" } else { "0" },
            client_schema,
        ])
        .output();
    let out = match out {
        Ok(o) => o,
        Err(e) => {
            eprintln!("SKIP: cannot invoke `npx tsx` ({e}); TS golden not run");
            return None;
        }
    };
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        // A missing toolchain / node_modules surfaces here — skip, don't fail.
        if stderr.contains("Cannot find")
            || stderr.contains("not found")
            || stderr.contains("ENOENT")
        {
            eprintln!("SKIP: tsx harness unavailable:\n{stderr}");
            return None;
        }
        panic!(
            "TS golden harness failed:\nstdout={}\nstderr={stderr}",
            String::from_utf8_lossy(&out.stdout)
        );
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    Some(
        serde_json::from_str(stdout.trim())
            .unwrap_or_else(|e| panic!("TS golden emitted non-JSON: {e}\n---\n{stdout}\n---")),
    )
}

fn rust_analyze(
    replica_path: &str,
    ast_json: &str,
    synced: bool,
    vended: bool,
    perms: Option<Value>,
    auth: Option<Value>,
    join_plans: bool,
) -> Value {
    let result = rust_syncer::services::analyze::analyze_query(
        replica_path,
        "app",
        ast_json,
        synced,
        vended,
        perms,
        auth,
        join_plans,
    )
    .expect("rust analyze_query");
    serde_json::to_value(&result).unwrap()
}

/// A plain single-table scan: the Rust `analyze_query` output equals the TS
/// `analyzeQuery` output field-for-field (rows incl. `_0_version`, the exact
/// vended SQL key, readRowCount, dbScansByQuery, and the sqlite plan text).
#[test]
fn golden_users_scan_matches_ts() {
    let path = tmp_path("users");
    if !build_ts_replica(&path, "users") {
        cleanup(&path);
        return; // skipped (no tsx)
    }
    let ast = json!({"table": "users", "orderBy": [["id", "asc"]]}).to_string();
    let client_schema = json!({
        "tables": {"users": {
            "columns": {"id": {"type": "string"}, "name": {"type": "string"}},
            "primaryKey": ["id"],
        }}
    })
    .to_string();

    let Some(ts) = ts_golden(&path, &ast, true, false, "", "", false, &client_schema) else {
        cleanup(&path);
        return; // skipped (no tsx)
    };
    let rust = rust_analyze(&path, &ast, true, false, None, None, false);

    assert_eq!(
        normalize(rust),
        normalize(ts),
        "rust analyze_query must match the TS analyzeQuery golden byte-for-byte \
         (minus timings)"
    );
    cleanup(&path);
}

/// An EXISTS/semi-join query: the two-table scan, the flip-planner-driven vended
/// SQLs, and the row set must match TS.
#[test]
fn golden_exists_join_matches_ts() {
    let path = tmp_path("exists");
    if !build_ts_replica(&path, "issues_comments") {
        cleanup(&path);
        return; // skipped (no tsx)
    }
    let ast = json!({
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
    .to_string();
    let client_schema = json!({
        "tables": {
            "issue": {"columns": {"id": {"type": "string"}}, "primaryKey": ["id"]},
            "comment": {
                "columns": {"id": {"type": "string"}, "issueId": {"type": "string"}},
                "primaryKey": ["id"],
            }
        }
    })
    .to_string();

    let Some(ts) = ts_golden(&path, &ast, true, false, "", "", false, &client_schema) else {
        cleanup(&path);
        return;
    };
    let rust = rust_analyze(&path, &ast, true, false, None, None, false);

    assert_eq!(
        normalize(rust),
        normalize(ts),
        "EXISTS-join golden mismatch"
    );
    cleanup(&path);
}
