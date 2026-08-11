//! Snapshotter stale-pin regression suite — the WAL checkpoint-starvation
//! class, encoded as tests.
//!
//! Line-by-line audit result (TS snapshotter.ts vs src/snapshotter/): the
//! leapfrog itself is a faithful port — BOTH sides hold two pinned read
//! transactions (`curr` at the last-advanced version, `prev` one advance
//! older), and marks move only when `advance*()` is called. TS's mark lifetime
//! is bounded by its synchronous lifecycle (every replication `version-ready`
//! advances; `destroy()` closes inline). The Rust engine's marks move only
//! when actor jobs ARRIVE — so a wedged JS-side await upstream freezes the
//! pinned read-marks while the replicator keeps writing, and the wal2
//! checkpointer can never copy past them: unbounded WAL growth from a
//! healthy-looking, epoll-idle process (the observed prod wedge).
//!
//! These tests pin:
//! 1. the REPRO — a stale pinned snapshot blocks checkpoint copying;
//! 2. `head_version()` — reads the moving head from under a pinned snapshot;
//! 3. `repin_at_head()` — the self-heal: releases the stale marks, re-pins at
//!    head, checkpointing proceeds, and the snapshotter remains functional
//!    (subsequent advances work);
//! 4. `destroy()` — releases every mark.
//!
//! Runs on plain WAL (the `non-wal2-test-support` feature): `BEGIN` + read
//! pins a read-mark with the same checkpoint-blocking semantics as wal2's
//! `BEGIN CONCURRENT`.

use rusqlite::Connection;
use rust_ivm::snapshotter::Snapshotter;

fn create_replica(name: &str) -> String {
    let dir = std::env::temp_dir()
        .join(format!("stale-pin-{}-{}", name, std::process::id()))
        .to_string_lossy()
        .to_string();
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = format!("{dir}/replica.db");

    let conn = Connection::open(&db_path).unwrap();
    conn.execute_batch(
        r#"
        PRAGMA journal_mode=wal;
        CREATE TABLE "_zero.replicationState" (
            lock TEXT PRIMARY KEY DEFAULT 'singleton',
            stateVersion TEXT NOT NULL
        );
        INSERT INTO "_zero.replicationState" (lock, stateVersion) VALUES ('singleton', '01');
        CREATE TABLE issues (id INTEGER PRIMARY KEY, title TEXT NOT NULL);
        "#,
    )
    .unwrap();
    // Start with a fully-checkpointed (empty) WAL.
    checkpoint_passive(&conn);
    db_path
}

/// Simulate the replicator: bump the head stateVersion and write rows (WAL
/// frames past every already-pinned read-mark).
fn write_head(conn: &Connection, version: &str, rows: std::ops::Range<i64>) {
    conn.execute(
        "UPDATE \"_zero.replicationState\" SET stateVersion = ?",
        [version],
    )
    .unwrap();
    for i in rows {
        conn.execute(
            "INSERT INTO issues (id, title) VALUES (?, ?)",
            rusqlite::params![i, format!("t{i}")],
        )
        .unwrap();
    }
}

/// (busy, log_frames, checkpointed_frames). `checkpointed < log` means a
/// reader's mark is pinning frames the checkpointer cannot copy — the
/// starvation condition (on wal2 this is what makes the WAL grow unbounded).
fn checkpoint_passive(conn: &Connection) -> (i64, i64, i64) {
    conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |r| {
        Ok((r.get(0)?, r.get(1)?, r.get(2)?))
    })
    .unwrap()
}

#[test]
fn stale_pinned_snapshot_starves_checkpoint_and_repin_heals_it() {
    let db_path = create_replica("heal");

    let mut snap = Snapshotter::new(&db_path, "test-app", None);
    snap.init().unwrap();
    assert_eq!(snap.current_version().unwrap(), "01");

    // Replicator advances while the engine receives no advance calls (the
    // wedged-caller scenario).
    let writer = Connection::open(&db_path).unwrap();
    write_head(&writer, "02", 0..300);

    // REPRO: the pinned mark at "01" blocks the checkpointer from copying the
    // new frames. This is the wedge — on wal2 the file then grows at the
    // replica write rate for as long as the pin lives.
    let (_busy, log, checkpointed) = checkpoint_passive(&writer);
    assert!(
        checkpointed < log,
        "expected stale pin to starve checkpoint copying (log={log}, checkpointed={checkpointed})"
    );

    // The guard's condition probe: head has moved from under the pin.
    assert_eq!(snap.current_version().unwrap(), "01");
    assert_eq!(snap.head_version().unwrap(), "02");

    // Self-heal: roll the pins forward.
    let (old, new) = snap.repin_at_head().unwrap();
    assert_eq!(old, "01");
    assert_eq!(new, "02");
    assert_eq!(snap.current_version().unwrap(), "02");

    // The checkpointer can now copy everything the pin was blocking.
    let (_busy, log, checkpointed) = checkpoint_passive(&writer);
    assert_eq!(
        checkpointed, log,
        "repin_at_head must release the stale mark so checkpointing proceeds"
    );

    // The snapshotter must remain fully functional after a repin: the normal
    // leapfrog advance still works and tracks the head.
    write_head(&writer, "03", 300..320);
    let v = snap.advance_without_diff().unwrap().to_string();
    assert_eq!(v, "03");

    snap.destroy();
    let _ = std::fs::remove_file(&db_path);
}

#[test]
fn leapfrog_advances_bound_the_pin_and_destroy_releases_everything() {
    let db_path = create_replica("advance");

    let mut snap = Snapshotter::new(&db_path, "test-app", None);
    snap.init().unwrap();

    let writer = Connection::open(&db_path).unwrap();

    // Healthy cadence: each advance rolls the OLDER mark forward (TS leapfrog
    // parity — `prev` stays pinned one advance behind `curr`), so the
    // checkpointer is never starved by more than one advance window.
    for (i, v) in ["02", "03", "04"].iter().enumerate() {
        let base = (i as i64 + 1) * 100;
        write_head(&writer, v, base..base + 50);
        let got = snap.advance_without_diff().unwrap().to_string();
        assert_eq!(&got, v);
    }
    // curr@04, prev@03: copying may lag by at most the prev window, never
    // unboundedly. Advance once more with no new writes: prev rotates to 04.
    let (_busy, log, checkpointed) = checkpoint_passive(&writer);
    assert!(
        checkpointed >= log - 2 || checkpointed > 0,
        "healthy leapfrog should keep checkpointing moving (log={log}, checkpointed={checkpointed})"
    );

    // destroy() releases BOTH marks; a TRUNCATE checkpoint (needs no readers
    // in the WAL at all) must fully succeed.
    snap.destroy();
    let (busy, log, checkpointed) = writer
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
            ))
        })
        .unwrap();
    assert_eq!(busy, 0, "destroy must release every read-mark");
    assert_eq!(log, checkpointed);

    let _ = std::fs::remove_file(&db_path);
}

/// A quiet replica (head == pinned) is NOT the starvation condition — there is
/// nothing past the mark for the checkpointer to copy. The guard's probe
/// (`head_version` vs `current_version`) distinguishes exactly this, so an
/// idle CG on an idle system is never reset.
#[test]
fn quiet_replica_is_not_flagged_as_stale() {
    let db_path = create_replica("quiet");

    let mut snap = Snapshotter::new(&db_path, "test-app", None);
    snap.init().unwrap();

    assert_eq!(
        snap.head_version().unwrap(),
        snap.current_version().unwrap(),
        "quiet replica: head == pinned — guard must not trigger"
    );

    snap.destroy();
    let _ = std::fs::remove_file(&db_path);
}

/// A FAILED repin (ROLLBACK succeeded, re-BEGIN failed on a busy replica)
/// leaves a connection in autocommit while `version` bookkeeping still claims
/// the old pin. `reset_to_head` must tolerate that txn-less state — otherwise
/// the failure is PERMANENT: every later advance/repin dies on the same
/// "cannot rollback - no transaction is active" and the snapshotter can never
/// pin again (the guard poisons on repin failure; this is what lets its
/// next-tick retry actually heal).
#[test]
fn repin_heals_a_txn_less_connection_from_a_prior_failed_repin() {
    let db_path = create_replica("txnless");

    let mut snap = Snapshotter::new(&db_path, "test-app", None);
    snap.init().unwrap();
    assert_eq!(snap.current_version().unwrap(), "01");

    // Induce the half-repinned state out-of-band: the conn is in autocommit,
    // bookkeeping still says "01".
    snap.current_conn()
        .unwrap()
        .borrow()
        .execute_batch("ROLLBACK")
        .unwrap();
    assert_eq!(snap.current_version().unwrap(), "01");

    let writer = Connection::open(&db_path).unwrap();
    write_head(&writer, "02", 0..50);

    // The retry must succeed despite the missing transaction, and pin at head.
    let (old, new) = snap.repin_at_head().unwrap();
    assert_eq!(old, "01");
    assert_eq!(new, "02");
    assert_eq!(snap.current_version().unwrap(), "02");

    // The healed pin is a REAL pin again: new frames past it cannot be fully
    // checkpointed (a txn-less conn would not block anything).
    write_head(&writer, "03", 50..80);
    let (_busy, log, checkpointed) = checkpoint_passive(&writer);
    assert!(
        checkpointed < log,
        "healed pin must hold a real read-mark (log={log}, checkpointed={checkpointed})"
    );

    // And the normal leapfrog advance still works on top of it.
    let v = snap.advance_without_diff().unwrap().to_string();
    assert_eq!(v, "03");

    snap.destroy();
    let _ = std::fs::remove_file(&db_path);
}

/// The guard's two-strike decision core (`StalePinTracker`) — the exact
/// state machine the napi guard runs every tick. Window ZERO makes every
/// observation "a full window elapsed" so the strike logic itself is under
/// test, not the clock.
#[test]
fn stale_pin_tracker_two_strike_semantics() {
    use rust_ivm::snapshotter::{StalePinAction, StalePinTracker};
    let head = |v: &str| {
        let v = v.to_string();
        move || Ok::<String, String>(v)
    };

    let mut t = StalePinTracker::new(std::time::Duration::ZERO);

    // First sight of a pin only initializes tracking — never a strike.
    assert_eq!(t.observe("01", head("02")).unwrap(), StalePinAction::None);
    // Same frozen pin + head ahead: strike 1 (warn), then strike 2 (repin).
    assert_eq!(t.observe("01", head("02")).unwrap(), StalePinAction::Warn);
    assert_eq!(t.observe("01", head("02")).unwrap(), StalePinAction::Repin);
    // Repin resets the strike: the SAME pin observed again re-arms from Warn
    // (models a failed repin retrying next tick), not straight to Repin.
    assert_eq!(t.observe("01", head("02")).unwrap(), StalePinAction::Warn);

    // An advance moving the pin between strikes DISARMS — the long-cold-
    // hydrate laggard and the healthy-race cases.
    let mut t = StalePinTracker::new(std::time::Duration::ZERO);
    assert_eq!(t.observe("01", head("02")).unwrap(), StalePinAction::None);
    assert_eq!(t.observe("01", head("02")).unwrap(), StalePinAction::Warn);
    assert_eq!(t.observe("02", head("03")).unwrap(), StalePinAction::None);
    assert_eq!(t.observe("02", head("03")).unwrap(), StalePinAction::Warn);
    assert_eq!(t.observe("02", head("03")).unwrap(), StalePinAction::Repin);

    // Quiet replica (head == pinned) never strikes AND disarms a prior
    // suspect: an idle CG is untouched no matter how long its pin sits.
    let mut t = StalePinTracker::new(std::time::Duration::ZERO);
    assert_eq!(t.observe("01", head("02")).unwrap(), StalePinAction::None);
    assert_eq!(t.observe("01", head("02")).unwrap(), StalePinAction::Warn);
    assert_eq!(t.observe("01", head("01")).unwrap(), StalePinAction::None);
    assert_eq!(t.observe("01", head("02")).unwrap(), StalePinAction::Warn);

    // A window that has NOT elapsed never reads the head at all.
    let mut t = StalePinTracker::new(std::time::Duration::from_secs(3600));
    assert_eq!(
        t.observe("01", || -> Result<String, String> {
            panic!("head must not be read inside the window")
        })
        .unwrap(),
        StalePinAction::None
    );
    assert_eq!(
        t.observe("01", || -> Result<String, String> {
            panic!("head must not be read inside the window")
        })
        .unwrap(),
        StalePinAction::None
    );

    // A head-read error propagates without corrupting strike state.
    let mut t = StalePinTracker::new(std::time::Duration::ZERO);
    assert_eq!(t.observe("01", head("02")).unwrap(), StalePinAction::None);
    assert!(
        t.observe("01", || Err::<String, String>("io".into()))
            .is_err()
    );
    assert_eq!(t.observe("01", head("02")).unwrap(), StalePinAction::Warn);
}
