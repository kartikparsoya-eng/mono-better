//! Live-instance census for leak hunting in the CVR stack. Each tracked type
//! increments its counter on construction and decrements in `Drop`;
//! `snapshot()` renders the process-wide totals. Used (env-gated) to show which
//! long-lived CVR structs survive a syncer teardown — a nonzero census after a
//! client group is dropped means a handle (or spawned task) is retaining the
//! store/cache, which in turn pins the PG pool and defers flush completion.
//! Counters are process-global across client groups; the interesting signal is
//! the DELTA logged across teardown, not the absolute value.
//!
//! Mirrors `packages/rust-ivm/src/live_count.rs`. The `Guard` RAII pattern is
//! preferred over manual `inc`/`dec` so the census can never leak from the
//! instrumentation itself: for `Clone` types the guard lives inside the Arc'd
//! shared inner state so the count tracks logical instances, not handle-clones.
use std::sync::atomic::{AtomicI64, Ordering};

/// The `CVRStoreHandle` — the single atomic PG writer per client group.
pub static CVR_STORE: AtomicI64 = AtomicI64::new(0);
/// The `RowRecordCache` — write-through/write-back adapter for `cvr.rows`.
/// `Clone`; the guard lives in its Arc'd shared state so clones don't count.
pub static ROW_RECORD_CACHE: AtomicI64 = AtomicI64::new(0);
/// The per-connection `ClientHandler`.
pub static CLIENT_HANDLER: AtomicI64 = AtomicI64::new(0);
/// The transient per-poke `PokeHandler` (should return to 0 between pokes).
pub static POKE_HANDLER: AtomicI64 = AtomicI64::new(0);
/// The transient per-advance `CVRQueryDrivenUpdater` (should be 0 at rest).
pub static QUERY_DRIVEN_UPDATER: AtomicI64 = AtomicI64::new(0);
/// The transient per-advance `CVRConfigDrivenUpdater` (should be 0 at rest).
pub static CONFIG_DRIVEN_UPDATER: AtomicI64 = AtomicI64::new(0);

// NOTE: unlike `rust-ivm`'s live_count (which exposes free `inc`/`dec`
// functions and calls them from each operator's ctor/Drop), rust-cvr tracks
// exclusively through the `Guard` RAII type below — so the manual `inc`/`dec`
// helpers had zero callers here and were removed (they inlined the same two
// `fetch_add`/`fetch_sub` the Guard already performs).

/// RAII census guard: inc on construction, dec on Drop. Embed ONE in each
/// long-lived struct. For `Clone` types, place the guard inside the Arc'd
/// shared inner state so all handle-clones share a single guard and the
/// count tracks logical instances, not clones.
pub struct Guard(&'static AtomicI64);

impl Guard {
    pub fn new(counter: &'static AtomicI64) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Guard(counter)
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub fn snapshot() -> String {
    format!(
        "cvr_store={} row_record_cache={} client_handler={} poke_handler={} query_updater={} config_updater={}",
        CVR_STORE.load(Ordering::Relaxed),
        ROW_RECORD_CACHE.load(Ordering::Relaxed),
        CLIENT_HANDLER.load(Ordering::Relaxed),
        POKE_HANDLER.load(Ordering::Relaxed),
        QUERY_DRIVEN_UPDATER.load(Ordering::Relaxed),
        CONFIG_DRIVEN_UPDATER.load(Ordering::Relaxed),
    )
}

/// Print a captured backtrace when `RUST_CVR_DROP_BACKTRACE=1`, to name who
/// triggered a leak-suspect drop/teardown. Gated so prod pays nothing.
pub fn drop_backtrace(context: &str) {
    if std::env::var("RUST_CVR_DROP_BACKTRACE").as_deref() == Ok("1") {
        eprintln!(
            "[cvr] {context} drop backtrace:\n{}",
            std::backtrace::Backtrace::force_capture()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_returns_counter_to_baseline() {
        // Use a dedicated static so parallel tests can't perturb the assertion.
        static COUNTER: AtomicI64 = AtomicI64::new(0);
        let start = COUNTER.load(Ordering::Relaxed);
        {
            let _g = Guard::new(&COUNTER);
            assert_eq!(COUNTER.load(Ordering::Relaxed), start + 1);
            // A raw clone of the *guard* is intentionally impossible (no Clone
            // impl): that is what forces the Arc-shared-guard pattern for
            // `Clone` types and prevents the census from going negative.
        }
        assert_eq!(
            COUNTER.load(Ordering::Relaxed),
            start,
            "census must return to baseline after Guard drop (leak-free)"
        );
    }
}
