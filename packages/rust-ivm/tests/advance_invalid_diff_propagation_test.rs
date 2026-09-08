//! Invalid-diff propagation and WAL2 snapshot regression gates.
//!
//! A stale-snapshot diff (the pinned prev/curr BEGIN CONCURRENT read view slips
//! forward under wal2 frame recycling) trips `check_valid`'s prev-version guard
//! in src/snapshotter/diff.rs (`Diff is no longer valid. prev db has advanced
//! past X.`) → `DiffError::InvalidDiff`.
//!
//! The bug that dropped ~5×/2h of live client groups was NOT the propagation
//! semantics — it was the SNAPSHOT OPEN. `Snapshot::create` silently fell back
//! to a read-only connection on rw-open failure; under wal2 a read-only handle
//! cannot write the -shm read-mark, so the checkpointer recycled frames under
//! the pinned snapshot → the prev/curr read view slipped → InvalidDiff fired far
//! more often than it ever does in TS (which has no read-only fallback).
//!
//! The fix removes the read-only fallback: retry the read-write open and never
//! serve an unmarked wal2 snapshot. If validation still detects a moved
//! snapshot, Rust must propagate InvalidDiff exactly like TypeScript.

use std::collections::{HashMap, HashSet};

use rusqlite::Connection;
use rust_ivm::engine::Engine;
use rust_ivm::snapshotter::spec::{ColumnSchema, LiteAndZqlSpec, TableSpec};
use rust_ivm::snapshotter::{DiffError, Snapshotter};

fn clean_db(path: &str) {
    for p in [
        path.to_string(),
        format!("{path}-wal"),
        format!("{path}-wal2"),
        format!("{path}-shm"),
    ] {
        let _ = std::fs::remove_file(p);
    }
}

/// Build a wal2 replica with the zero replication tables + one syncable table
/// (`users`), plus a changeLog2 entry at stateVersion "v2" while the replica's
/// pinned stateVersion stays "v1".
///
/// When the snapshotter pins at "v1" (prev == curr == "v1") and iterates the
/// diff, `read_changelog` returns the "v2" SET entry (v2 > prev "v1"), and
/// `check_valid` sees the entry's `state_version "v2" > curr_version "v1"` — the
/// exact stale-diff condition ("Diff is no longer valid. curr db has advanced
/// past v1") that yields `DiffError::InvalidDiff`. The `users` row also carries
/// `_0_version = "v3"` so the symmetric prev-version guard would trip too; either
/// way the diff is InvalidDiff. This is deterministic and needs no concurrent
/// checkpoint/writer (which the bundled non-wal2 SQLite would serialize against
/// the pinned read tx anyway).
fn create_stale_diff_replica(path: &str) {
    clean_db(path);
    let conn = Connection::open(path).unwrap();
    // wal2 if the fork supports it; plain wal otherwise (guard logic is identical).
    let _ = conn.pragma_update(None, "journal_mode", "wal2");
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS "_zero.replicationConfig" (
            lock TEXT PRIMARY KEY DEFAULT 'singleton',
            replicaVersion TEXT NOT NULL,
            publications TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS "_zero.replicationState" (
            lock TEXT PRIMARY KEY DEFAULT 'singleton',
            stateVersion TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS "_zero.changeLog2" (
            "stateVersion" TEXT NOT NULL,
            "table"        TEXT NOT NULL,
            "rowKey"       TEXT NOT NULL,
            "op"           TEXT NOT NULL,
            "pos"          INTEGER NOT NULL,
            PRIMARY KEY ("stateVersion", "pos")
        );
        CREATE TABLE IF NOT EXISTS users (
            id         TEXT PRIMARY KEY,
            name       TEXT NOT NULL,
            _0_version TEXT NOT NULL
        );

        INSERT OR REPLACE INTO "_zero.replicationConfig" (lock, replicaVersion, publications)
            VALUES ('singleton', 'v1', '[]');
        INSERT OR REPLACE INTO "_zero.replicationState" (lock, stateVersion)
            VALUES ('singleton', 'v1');

        -- Row carries _0_version "v3" > the pinned version "v1".
        INSERT INTO users (id, name, _0_version) VALUES ('u1', 'Alice', 'v3');

        -- A change-log SET at stateVersion "v2" (> the pinned "v1") — diffing it
        -- trips check_valid's stale-diff guard → DiffError::InvalidDiff.
        INSERT INTO "_zero.changeLog2" ("stateVersion","table","rowKey","op","pos")
            VALUES ('v2','users','{"id":"u1"}','s',0);
        "#,
    )
    .unwrap();
    drop(conn);
}

fn users_spec() -> LiteAndZqlSpec {
    let mut columns = HashMap::new();
    for c in ["id", "name", "_0_version"] {
        columns.insert(
            c.to_string(),
            ColumnSchema {
                r#type: "TEXT".to_string(),
                optional: false,
            },
        );
    }
    LiteAndZqlSpec {
        table_spec: TableSpec {
            name: "users".to_string(),
            columns: columns.clone(),
            unique_keys: vec![vec!["id".to_string()]],
            min_row_version: None,
        },
        zql_spec: columns,
    }
}

/// A stale prev/curr snapshot propagates InvalidDiff exactly like TS.
#[test]
fn stale_snapshot_propagates_invalid_diff() {
    let db_path = "/tmp/rust-ivm-invalid-diff-propagation.db";
    create_stale_diff_replica(db_path);

    let mut snapshotter = Snapshotter::new(db_path, "", None);
    snapshotter.init().expect("init snapshotter");
    assert_eq!(
        snapshotter.current_version().unwrap(),
        "v1",
        "snapshot must pin at v1 (the version the changelog entry / row _0_version exceed)"
    );

    let syncable: HashMap<String, LiteAndZqlSpec> =
        HashMap::from([("users".to_string(), users_spec())]);
    let all_tables: HashSet<String> = HashSet::from(["users".to_string()]);

    let mut engine = Engine::new(HashMap::new());

    let result = engine.advance_to_head_stream(
        &mut snapshotter,
        &syncable,
        &all_tables,
        |_v, _n| {},
        |_rc| {},
    );

    match result {
        Err(DiffError::InvalidDiff(e)) => assert!(
            e.msg.contains("no longer valid"),
            "InvalidDiff must identify the moved snapshot, got {}",
            e.msg
        ),
        Ok(res) => panic!(
            "stale snapshot must not become reset/success: aborted={}, reason={:?}",
            res.aborted, res.reset_reason
        ),
        Err(e) => panic!("expected InvalidDiff, got {e:?}"),
    }

    snapshotter.destroy();
    clean_db(db_path);
}

/// Under wal2, the pinned snapshot connection must be READ-WRITE (able
/// to register the -shm read-mark). The old code silently fell back to a
/// read-only connection on rw-open failure, which cannot write -shm — the
/// checkpointer then recycles frames under the pinned snapshot → torn read → the
/// InvalidDiff teardown this whole test guards. Assert the connection is
/// writable (proves it is NOT a silently-degraded read-only handle).
#[test]
fn snapshot_connection_is_read_write_under_wal2() {
    let db_path = "/tmp/rust-ivm-stale-diff-rwcheck.db";
    create_stale_diff_replica(db_path);

    let mut snapshotter = Snapshotter::new(db_path, "", None);
    snapshotter.init().expect("init snapshotter");

    let conn = snapshotter.current_conn().expect("current conn");
    // A read-only SQLite connection rejects writes with SQLITE_READONLY. A
    // successful write to a scratch temp table proves the handle is RW (and thus
    // able to write the wal2 -shm read-mark). Temp tables don't touch the
    // pinned snapshot's read view.
    let write_ok = {
        let c = conn.borrow();
        c.execute_batch("CREATE TEMP TABLE rw_probe (x INTEGER); INSERT INTO rw_probe VALUES (1);")
    };
    assert!(
        write_ok.is_ok(),
        "snapshot connection must be READ-WRITE under wal2 (a silent read-only \
         fallback cannot write the -shm read-mark → torn reads); write failed: {:?}",
        write_ok.err()
    );

    snapshotter.destroy();
    clean_db(db_path);
}
