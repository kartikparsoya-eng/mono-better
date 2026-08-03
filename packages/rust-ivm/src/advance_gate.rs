//! Per-fetch economic advancement budget (TS parity, point 2).
//!
//! The engine's advance loop already re-checks the economic budget BEFORE each
//! change (engine/mod.rs advance_to_head_stream). TS additionally re-checks it
//! "whenever a row is fetched from a TableSource during push processing"
//! (pipeline-driver.ts #shouldAdvanceYieldMaybeAbortAdvance, docstring point 2),
//! so a single fat change — e.g. one correlated-EXISTS re-fetch returning a huge
//! set — is abandoned mid-fetch instead of grinding to the end of the change.
//!
//! The Rust IVM push is INFALLIBLE (operators return `Vec<Change>`, not
//! `Result`), so we can't throw a `ResetPipelinesSignal` from deep inside a
//! fetch the way TS does. Instead: a thread-local gate (advance runs single-
//! threaded on the actor thread, so a thread-local is the exact right scope —
//! worker threads and the hydrate path see `None` and are unaffected) is armed
//! for the duration of an advance. The row-read loop (`LazyRowsIter::next`)
//! calls `should_stop_fetch()` between rows; when the budget is blown it returns
//! `None` — a normal short-input end-of-stream (no Take/Cap guard trips, exactly
//! as when a table genuinely has fewer rows than a LIMIT) — and sets the gate's
//! `tripped` flag. The advance loop checks `tripped()` right after the change's
//! push and returns `advancement-timeout`, which rehydrates (discarding the
//! truncated push, so the early stream end is harmless).

use std::cell::RefCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Minimum advancement time before an abort is considered (ms). MUST match
/// engine::MIN_ADVANCEMENT_TIME_LIMIT_MS (the per-change check) so the per-row
/// and per-change arms trip on the same threshold.
pub const MIN_ADVANCEMENT_TIME_LIMIT_MS: f64 = 50.0;

/// Shared, thread-safe economic budget for one in-flight advance. `start`,
/// `budget_ms` and `num_changes` are fixed for the advance; only `pos` (progress)
/// advances. Evaluated by both the per-change check (via `over_budget`) and the
/// per-row fetch check (`should_stop_fetch`).
pub struct AdvanceGate {
    start: Instant,
    budget_ms: f64,
    num_changes: usize,
    pos: AtomicUsize,
    tripped: AtomicBool,
    excluded_nanos: AtomicU64,
}

impl AdvanceGate {
    /// Create a gate sharing the advance's own start instant (so per-row and
    /// per-change elapsed agree).
    pub fn new(start: Instant, budget_ms: f64, num_changes: usize) -> Arc<Self> {
        Arc::new(Self {
            start,
            budget_ms,
            num_changes,
            pos: AtomicUsize::new(0),
            tripped: AtomicBool::new(false),
            excluded_nanos: AtomicU64::new(0),
        })
    }

    /// Exclude time spent synchronously delivering rows across the NAPI
    /// boundary. TS pauses its timer while the consumer is yielded.
    pub fn exclude(&self, duration: Duration) {
        let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
        self.excluded_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    fn elapsed(&self) -> Duration {
        self.start.elapsed().saturating_sub(Duration::from_nanos(
            self.excluded_nanos.load(Ordering::Relaxed),
        ))
    }

    /// Update progress (the number of changes emitted so far).
    pub fn set_pos(&self, pos: usize) {
        self.pos.store(pos, Ordering::Relaxed);
    }

    /// Whether the budget has been blown at any point this advance.
    pub fn tripped(&self) -> bool {
        self.tripped.load(Ordering::Relaxed)
    }

    /// The exact TS/per-change formula. Latches `tripped` once it fires.
    pub fn over_budget(&self) -> bool {
        if self.tripped.load(Ordering::Relaxed) {
            return true;
        }
        let elapsed = self.elapsed_ms();
        if elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS
            && (elapsed > self.budget_ms
                || (elapsed > self.budget_ms / 2.0
                    && self.pos.load(Ordering::Relaxed) <= self.num_changes / 2))
        {
            self.tripped.store(true, Ordering::Relaxed);
            return true;
        }
        false
    }

    pub fn elapsed_ms(&self) -> f64 {
        self.elapsed().as_secs_f64() * 1000.0
    }

    pub fn budget_ms(&self) -> f64 {
        self.budget_ms
    }
}

thread_local! {
    static ADVANCE_GATE: RefCell<Option<Arc<AdvanceGate>>> = const { RefCell::new(None) };
}

/// RAII guard: clears the thread-local gate on drop — including on a panic
/// unwind (e.g. a HardError row-read panic), so a subsequent hydrate on this
/// thread can NEVER inherit a stale advance budget and wrongly truncate.
#[must_use = "dropping the guard immediately disarms the gate"]
pub struct GateGuard(());

impl Drop for GateGuard {
    fn drop(&mut self) {
        ADVANCE_GATE.with(|g| *g.borrow_mut() = None);
    }
}

/// Arm the per-fetch gate on THIS (actor) thread; the returned guard disarms it
/// when dropped (scope exit or panic).
pub fn arm(gate: Arc<AdvanceGate>) -> GateGuard {
    ADVANCE_GATE.with(|g| *g.borrow_mut() = Some(gate));
    GateGuard(())
}

/// Called from the row-read loop between rows. `true` → the in-flight advance
/// has blown its economic budget mid-fetch; stop producing rows. Cheap: a
/// thread-local read; the caller throttles how often it asks.
pub fn should_stop_fetch() -> bool {
    ADVANCE_GATE.with(|g| g.borrow().as_ref().is_some_and(|g| g.over_budget()))
}

/// Exclude a synchronous row-delivery wait from the currently armed advance.
/// This is a no-op outside production advance.
pub fn exclude_current(duration: Duration) {
    ADVANCE_GATE.with(|g| {
        if let Some(gate) = g.borrow().as_ref() {
            gate.exclude(duration);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// A gate whose start is `ms` in the past (so elapsed ≈ ms), at `pos/num`.
    fn gate(ms: u64, budget: f64, num: usize, pos: usize) -> Arc<AdvanceGate> {
        let start = Instant::now()
            .checked_sub(Duration::from_millis(ms))
            .unwrap_or_else(Instant::now);
        let g = AdvanceGate::new(start, budget, num);
        g.set_pos(pos);
        g
    }

    #[test]
    fn under_min_floor_never_trips() {
        // 10ms < 50ms floor — no abort even with a tiny budget.
        assert!(!gate(10, 1.0, 4, 0).over_budget());
    }

    #[test]
    fn over_full_budget_trips_regardless_of_progress() {
        // 200ms > budget 100 — trips even though pos (3) is past half.
        assert!(gate(200, 100.0, 4, 3).over_budget());
    }

    #[test]
    fn half_budget_trips_when_behind() {
        // 60ms > 50 floor, > budget/2 (50), pos 0 ≤ 4/2 → trip.
        assert!(gate(60, 100.0, 4, 0).over_budget());
    }

    #[test]
    fn half_budget_no_trip_when_ahead() {
        // 60ms > budget/2 but pos 3 > 4/2 (half rule off) and < budget 100 → no trip.
        assert!(!gate(60, 100.0, 4, 3).over_budget());
    }

    #[test]
    fn tripped_latches_across_pos_changes() {
        let g = gate(200, 100.0, 4, 0);
        assert!(g.over_budget());
        assert!(g.tripped());
        g.set_pos(3); // even "rewinding" progress stays tripped
        assert!(g.over_budget());
    }

    #[test]
    fn delivery_wait_is_excluded_from_budget() {
        let g = gate(200, 100.0, 4, 0);
        g.exclude(Duration::from_millis(175));
        assert!(g.elapsed_ms() < 50.0);
        assert!(!g.over_budget());
    }

    #[test]
    fn should_stop_fetch_is_false_when_unarmed() {
        // No gate armed on this thread → hydrate/worker fetches never stop.
        assert!(!should_stop_fetch());
    }
}
