// tests/teardown_gate_test.rs — single-owner teardown integrity gate.
//
// Validates that repeated hydrate → advance → destroy cycles on the Rust
// snapshotter do NOT corrupt replica.db. This is the regression gate for the
// ART-reported SQLITE_CORRUPT on replica.db WAL2 during destroy().
//
// Root cause of the original corruption: TWO runtimes (TS Snapshotter + Rust
// engine) held connections to the same replica.db WAL2 file. When TS
// Snapshotter.destroy() ran `PRAGMA optimize` on close while the Rust engine
// still had an active read transaction, the WAL2 state could be left
// inconsistent. The single-owner fix gives the Rust engine the ONLY connection
// to replica.db; TS never opens one. This test confirms that fix.
//
// The test creates a replica.db with a replication-state row + a simple table,
// then runs N cycles of: create snapshotter → init → advance_without_diff →
// destroy. After all cycles, it opens a fresh connection and runs
// PRAGMA integrity_check.

use rusqlite::Connection;
use rust_ivm::snapshotter::Snapshotter;

fn create_test_replica(dir: &str) -> String {
    let db_path = format!("{}/teardown-gate-replica.db", dir);

    let conn = Connection::open(&db_path).expect("open replica");
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
        INSERT OR REPLACE INTO "_zero.replicationConfig" (lock, replicaVersion, publications)
            VALUES ('singleton', '1', '[]');
        INSERT OR REPLACE INTO "_zero.replicationState" (lock, stateVersion)
            VALUES ('singleton', '1');

        CREATE TABLE IF NOT EXISTS issues (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            _0_version TEXT NOT NULL
        );
        INSERT INTO issues (id, title, _0_version) VALUES ('i1', 'issue-1', '1');
        "#,
    )
    .expect("create schema");

    db_path
}

#[test]
fn teardown_integrity_after_repeated_cycles() {
    let dir = std::env::temp_dir()
        .join(format!("teardown-gate-{}", std::process::id()))
        .to_string_lossy()
        .to_string();
    let _ = std::fs::create_dir_all(&dir);
    let db_path = create_test_replica(&dir);

    const CYCLES: usize = 20;

    for i in 0..CYCLES {
        let mut snap = Snapshotter::new(&db_path, "test-app", None);
        snap.init().expect("init snapshotter");

        let v1 = snap.advance_without_diff().expect("advance_without_diff");
        assert!(!v1.is_empty(), "cycle {}: version empty", i);

        snap.destroy();
        assert!(snap.destroyed(), "cycle {}: not destroyed", i);
    }

    let conn = Connection::open(&db_path).expect("open for integrity check");
    let result: String = conn
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .expect("integrity_check");
    assert_eq!(
        result, "ok",
        "replica.db corrupted after {CYCLES} teardown cycles: {result}"
    );

    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn advance_without_diff_returns_version() {
    let dir = std::env::temp_dir()
        .join(format!("teardown-awd-{}", std::process::id()))
        .to_string_lossy()
        .to_string();
    let _ = std::fs::create_dir_all(&dir);
    let db_path = create_test_replica(&dir);

    let mut snap = Snapshotter::new(&db_path, "test-app", None);
    snap.init().expect("init");

    let _v0 = snap.current_version().expect("current version").to_string();
    let v1 = snap.advance_without_diff().expect("advance").to_string();
    assert!(!v1.is_empty());

    snap.destroy();
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_dir_all(&dir);
}
