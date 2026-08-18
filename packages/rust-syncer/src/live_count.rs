//! Live-instance census for leak hunting in the Rust syncer. Each tracked
//! long-lived type increments its counter on construction and decrements in
//! `Drop`; `snapshot()` renders the process-wide totals. Used (env-gated) via
//! the HTTP `/census` endpoint to watch which counter climbs during a load run
//! — a client-group count that never returns to zero under cold-client churn is
//! the leak signal (the CG owns the `SyncEngine`, which pins the IVM graph and
//! the CVR store connections). Counters are process-global; the interesting
//! signal is the DELTA over time (or the residual after all clients disconnect),
//! not the absolute value.
//!
//! Idiom mirrors `rust_ivm::live_count`: `AtomicI64` statics, `inc`/`dec`
//! helpers, and a Clone-safe RAII [`Guard`] that inc's on construction and dec's
//! on `Drop`. For **Clone** types the guard is embedded in the type's Arc'd
//! shared inner state (one guard per logical instance), so cloning a handle does
//! NOT bump the count — the census tracks logical instances, not clones, and
//! returns to baseline exactly when the last clone drops.
use std::sync::atomic::{AtomicI64, Ordering};

/// Per-client-group state (`router::CgState`), one per hosted client group.
/// THE most important census for a leak hunt: a CgState owns the `SyncEngine`
/// (and thus the IVM pipeline graph + CVR store handle), so a CG that never
/// drops pins everything below it. A nonzero residual after all clients
/// disconnect and idle-shutdown fires means a CG task is being retained.
pub static CLIENT_GROUP: AtomicI64 = AtomicI64::new(0);
/// `sync_engine::SyncEngine` — one per CgState. Should track CLIENT_GROUP; a
/// divergence means an engine escaped its owning CG.
pub static SYNC_ENGINE: AtomicI64 = AtomicI64::new(0);
/// `connection::Connection` — one per live client socket on a CG.
pub static CONNECTION: AtomicI64 = AtomicI64::new(0);
/// `message_handler::SyncerWsMessageHandler` — one per accepted connection.
pub static WS_MESSAGE_HANDLER: AtomicI64 = AtomicI64::new(0);
/// `push_relay::HttpRelayPusher` — one per CG that has a push relay configured.
pub static PUSHER: AtomicI64 = AtomicI64::new(0);

pub fn inc(c: &AtomicI64) {
    c.fetch_add(1, Ordering::Relaxed);
}

pub fn dec(c: &AtomicI64) {
    c.fetch_sub(1, Ordering::Relaxed);
}

/// RAII census guard: increments its counter on construction and decrements it
/// on `Drop`. Embed as a field on the tracked type — construction bumps the
/// count, and the automatic `Drop` returns it, so the census can never leak on
/// its own (unlike a manual inc/dec that a `?`/early-return can bypass).
///
/// For Clone types, place the guard inside the Arc'd shared inner state so all
/// clones share the single guard: the count tracks logical instances and only
/// dec's when the last clone drops.
#[derive(Debug)]
pub struct Guard {
    counter: &'static AtomicI64,
}

impl Guard {
    pub fn new(counter: &'static AtomicI64) -> Self {
        inc(counter);
        Guard { counter }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        dec(self.counter);
    }
}

/// Emit a `force_capture` backtrace to stderr when `RUST_SYNCER_DROP_BACKTRACE=1`.
/// Called from the drop path of a long-lived type (e.g. `CgState`) to attribute
/// *who* tore it down — steady-state runs don't pay the capture cost unless the
/// env var is set. Mirrors the `RUST_IVM_DROP_BACKTRACE` capture in the
/// snapshotter.
pub fn drop_backtrace(context: &str) {
    if std::env::var("RUST_SYNCER_DROP_BACKTRACE").as_deref() == Ok("1") {
        eprintln!(
            "[syncer] {context} drop backtrace:\n{}",
            std::backtrace::Backtrace::force_capture()
        );
    }
}

pub fn snapshot() -> String {
    format!(
        "cg={} engine={} conn={} wsmh={} pusher={}",
        CLIENT_GROUP.load(Ordering::Relaxed),
        SYNC_ENGINE.load(Ordering::Relaxed),
        CONNECTION.load(Ordering::Relaxed),
        WS_MESSAGE_HANDLER.load(Ordering::Relaxed),
        PUSHER.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn guard_returns_counter_to_baseline_on_drop() {
        // Use a dedicated static so this test is insensitive to other counters.
        static T: AtomicI64 = AtomicI64::new(0);
        let base = T.load(Ordering::Relaxed);
        {
            let _g = Guard::new(&T);
            assert_eq!(T.load(Ordering::Relaxed), base + 1);
            // A clone-shared guard would live in an Arc; a second logical guard
            // bumps the count again, and each dec's independently on drop.
            {
                let _g2 = Guard::new(&T);
                assert_eq!(T.load(Ordering::Relaxed), base + 2);
            }
            assert_eq!(T.load(Ordering::Relaxed), base + 1);
        }
        assert_eq!(T.load(Ordering::Relaxed), base, "guard leaked on drop");
    }

    #[test]
    fn snapshot_renders_all_counters() {
        let s = snapshot();
        assert!(s.contains("cg="));
        assert!(s.contains("engine="));
        assert!(s.contains("conn="));
        assert!(s.contains("wsmh="));
        assert!(s.contains("pusher="));
    }
}
