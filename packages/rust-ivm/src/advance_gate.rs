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

/// Minimum advancement time before an abort is considered (ms).
///
/// SINGLE source of truth (review #7): `engine::mod` imports this exact value
/// for its per-change check, so the per-row and per-change economic arms can
/// never trip on different thresholds (a TS-parity hazard when it was two
/// independent `const`s).
pub const MIN_ADVANCEMENT_TIME_LIMIT_MS: f64 = 50.0;

/// Absolute wall-clock ceiling for one advance, measured WITHOUT the
/// delivery-time exclusion. The economic clock pauses while rows are being
/// delivered downstream (`exclude`), which TS does not do — its budget keeps
/// ticking through consumer/flush work. Under a slow consumer the Rust advance
/// could therefore hold the previous WAL snapshot open indefinitely, the very
/// resource the advancement-timeout reset exists to bound. This ceiling
/// restores an absolute bound regardless of exclusions; it ignores the
/// late-finish exception for the same reason. 60s matches the query liveness
/// ceiling.
pub const ADVANCE_WALL_CLOCK_CEILING_MS: f64 = 60_000.0;

// Smarter load-shedding tunables — byte-identical to TS pipeline-driver.ts
// (#6206). We project the total cost of pushing the change backlog and bail if
// it materially exceeds a fresh hydrate, catch a single pathological change, and
// let an already-mostly-done advance finish.
const MIN_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES: usize = 8;
const PROJECTED_ADVANCEMENT_SAMPLE_FRACTION: f64 = 0.25;
const MAX_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES: usize = 50;
const MIN_PROJECTED_ADVANCEMENT_SAMPLE_MS: f64 = 5.0;
const MIN_PROJECTED_ADVANCEMENT_CHANGES: usize = 16;
const PROJECTED_ADVANCEMENT_RESET_MULTIPLIER: f64 = 1.5;
const LATE_ADVANCEMENT_FINISH_PROGRESS: f64 = 0.8;

/// Which arm of the economic budget tripped. Distinct reasons so the engine can
/// emit the same diagnostic messages TS does. Mirrors TS pipeline-driver.ts
/// `#shouldAdvanceYieldMaybeAbortAdvance`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AdvanceReset {
    /// One source change alone exceeded the hydration budget.
    SlowCurrentChange { current_change_ms: f64 },
    /// The whole batch projects to cost more than a rehydrate.
    Projected { projected_ms: f64 },
    /// The original economic time-limit arm.
    Timeout,
    /// The absolute wall-clock bound (including excluded delivery time) was
    /// exceeded — the advance has held the previous WAL snapshot too long.
    WallClockCeiling { wall_ms: f64 },
}

/// Project total advancement time from the elapsed cost of the processed prefix.
/// `None` until at least one change has been processed. TS
/// `projectedAdvancementTimeMs`.
fn projected_advancement_time_ms(
    elapsed_ms: f64,
    processed_changes: usize,
    num_changes: usize,
) -> Option<f64> {
    if processed_changes == 0 || num_changes == 0 {
        return None;
    }
    Some((elapsed_ms / processed_changes as f64) * num_changes as f64)
}

/// The advancement time budget: the total original hydration time (floored at 1
/// to avoid a zero budget). TS `advancementResetTimeLimitMs`.
fn advancement_reset_time_limit_ms(total_hydration_time_ms: f64) -> f64 {
    total_hydration_time_ms.max(1.0)
}

/// How many changes must be sampled before the projection is trusted, scaled by
/// batch size. TS `minProjectedAdvancementSampleChanges`.
fn min_projected_advancement_sample_changes(num_changes: usize) -> usize {
    let scaled = (num_changes as f64 * PROJECTED_ADVANCEMENT_SAMPLE_FRACTION).ceil() as usize;
    MIN_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES
        .max(MAX_PROJECTED_ADVANCEMENT_SAMPLE_CHANGES.min(scaled))
}

/// Whether the projected batch cost warrants a reset. TS
/// `shouldResetProjectedAdvancement`.
fn should_reset_projected_advancement(
    elapsed_ms: f64,
    projected_total_ms: Option<f64>,
    processed_changes: usize,
    num_changes: usize,
    total_hydration_time_ms: f64,
) -> bool {
    let Some(projected) = projected_total_ms else {
        return false;
    };
    if num_changes < MIN_PROJECTED_ADVANCEMENT_CHANGES
        || processed_changes < min_projected_advancement_sample_changes(num_changes)
        || elapsed_ms < MIN_PROJECTED_ADVANCEMENT_SAMPLE_MS
    {
        return false;
    }
    projected
        > advancement_reset_time_limit_ms(total_hydration_time_ms)
            * PROJECTED_ADVANCEMENT_RESET_MULTIPLIER
}

/// Whether the advance is far enough along that it should finish rather than
/// reset. TS `shouldFinishLateAdvancement`.
fn should_finish_late_advancement(processed_changes: usize, num_changes: usize) -> bool {
    num_changes > 0
        && processed_changes as f64 / num_changes as f64 >= LATE_ADVANCEMENT_FINISH_PROGRESS
}

/// Whether the CURRENT single change alone has exceeded the hydration budget.
/// This always resets — the late-finish exception does not apply. TS
/// `shouldResetSlowCurrentChange`.
fn should_reset_slow_current_change(
    current_change_elapsed_ms: f64,
    total_hydration_time_ms: f64,
) -> bool {
    current_change_elapsed_ms > MIN_ADVANCEMENT_TIME_LIMIT_MS
        && current_change_elapsed_ms > advancement_reset_time_limit_ms(total_hydration_time_ms)
}

/// Shared, thread-safe economic budget for one in-flight advance. `start`,
/// `budget_ms` and `num_changes` are fixed for the advance; `pos` (progress) and
/// `current_change_start` advance. Evaluated by both the per-change check (via
/// `advance_reset`/`over_budget`) and the per-row fetch check
/// (`should_stop_fetch`).
pub struct AdvanceGate {
    start: Instant,
    budget_ms: f64,
    num_changes: usize,
    pos: AtomicUsize,
    tripped: AtomicBool,
    /// Which arm latched `tripped` (0 = none, 1 = slow-current-change,
    /// 2 = projected, 3 = timeout, 4 = wall-clock ceiling) and the triggering
    /// measurement (ms, f64 bits) — so the mid-fetch reset message can
    /// attribute the arm instead of collapsing all three into a generic
    /// "timed out mid-fetch".
    tripped_arm: AtomicUsize,
    tripped_value_bits: AtomicU64,
    excluded_nanos: AtomicU64,
    /// Elapsed-ms at which the current change's push began; `active` gates it
    /// (a change boundary clears it so the slow-current-change arm only applies
    /// mid-push). TS `AdvanceContext.currentChangeStartMs`.
    current_change_start_bits: AtomicU64,
    current_change_active: AtomicBool,
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
            tripped_arm: AtomicUsize::new(0),
            tripped_value_bits: AtomicU64::new(0),
            excluded_nanos: AtomicU64::new(0),
            current_change_start_bits: AtomicU64::new(0),
            current_change_active: AtomicBool::new(false),
        })
    }

    /// Mark the start (elapsed-ms) of the current change's push so the
    /// slow-current-change arm can measure it. TS sets `currentChangeStartMs` at
    /// the top of each change's processing.
    pub fn set_current_change_start(&self, elapsed_ms: f64) {
        // Publish the bits BEFORE the flag (Release) and read the flag with
        // Acquire below: a cross-thread reader that observes `active == true`
        // is then guaranteed to see THIS change's start, never a previous
        // change's smaller value (which would inflate the measured duration
        // and fire a spurious slow-current-change reset). Today every accessor
        // runs on the single actor thread, but the struct is Arc-shared and
        // atomic-typed — pay the (free on x86/aarch64 loads) barrier rather
        // than advertise a thread-safety the code doesn't have.
        self.current_change_start_bits
            .store(elapsed_ms.to_bits(), Ordering::Relaxed);
        self.current_change_active.store(true, Ordering::Release);
    }

    /// Clear the current-change marker at a change boundary (TS resets
    /// `currentChangeStartMs = undefined` after each change).
    pub fn clear_current_change(&self) {
        self.current_change_active.store(false, Ordering::Release);
    }

    fn current_change_start_ms(&self) -> Option<f64> {
        if self.current_change_active.load(Ordering::Acquire) {
            Some(f64::from_bits(
                self.current_change_start_bits.load(Ordering::Relaxed),
            ))
        } else {
            None
        }
    }

    /// Exclude consumer time from the economic budget.
    ///
    /// DIVERGENCE (2026-09-04, unregistered): this was justified by the NAPI
    /// boundary's synchronous row delivery, and that boundary was deleted in
    /// a5e502ad9. What the sole caller now excludes is ALL time between two
    /// pulls of `AdvanceStream` — the CVR `received()`/poke work as well as a
    /// yielded time slice. TS stops its `TimeSliceTimer` ONLY inside
    /// `timer.yieldProcess()` (view-syncer.ts `TimeSliceTimer.#stopLap`); the
    /// `#processChanges` consumer work runs with the timer STILL RUNNING, and
    /// that is the `elapsed` its budget arms compare against
    /// (pipeline-driver.ts:1102). rust therefore measures a smaller `elapsed`
    /// than TS for identical work and sheds LESS eagerly. Narrowing this to the
    /// yield await alone changes load-shedding under pressure, so it needs its
    /// own change plus an ART gate rather than a comment fix.
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

    /// The smarter load-shedding decision (TS #6206
    /// `#shouldAdvanceYieldMaybeAbortAdvance`): three arms evaluated in order —
    /// (1) a single slow change (always resets), (2) the projected batch cost
    /// (skipped once the advance is late enough to finish), (3) the original
    /// economic time limit (also skipped when late enough to finish). Returns the
    /// arm that tripped, or `None` to keep advancing.
    pub fn advance_reset(&self) -> Option<AdvanceReset> {
        let elapsed = self.elapsed_ms();
        let pos = self.pos.load(Ordering::Relaxed);
        let num = self.num_changes;
        let budget = self.budget_ms;

        // Arm 0: absolute wall-clock bound, exclusion-free — no late-finish
        // exception (see ADVANCE_WALL_CLOCK_CEILING_MS).
        let wall_ms = self.raw_elapsed_ms();
        if wall_ms > ADVANCE_WALL_CLOCK_CEILING_MS {
            return Some(AdvanceReset::WallClockCeiling { wall_ms });
        }

        // Arm 1: the current change alone blew the budget. No late-finish
        // exception — a single pathological push always resets.
        if let Some(cc_start) = self.current_change_start_ms() {
            let cc_elapsed = elapsed - cc_start;
            if should_reset_slow_current_change(cc_elapsed, budget) {
                return Some(AdvanceReset::SlowCurrentChange {
                    current_change_ms: cc_elapsed,
                });
            }
        }

        let should_finish = should_finish_late_advancement(pos, num);

        // Arm 2: the batch projects to cost more than a rehydrate.
        if !should_finish {
            let projected = projected_advancement_time_ms(elapsed, pos, num);
            if should_reset_projected_advancement(elapsed, projected, pos, num, budget) {
                return Some(AdvanceReset::Projected {
                    projected_ms: projected.unwrap_or(0.0),
                });
            }
        }

        // Arm 3: the original economic time-limit model, now also gated by the
        // late-finish exception.
        if !should_finish
            && elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS
            && (elapsed > budget || (elapsed > budget / 2.0 && pos <= num / 2))
        {
            return Some(AdvanceReset::Timeout);
        }

        None
    }

    /// Whether the advance has blown its budget (any arm). Latches `tripped`
    /// once it fires — used by the per-row fetch check.
    pub fn over_budget(&self) -> bool {
        if self.tripped.load(Ordering::Relaxed) {
            return true;
        }
        if let Some(reset) = self.advance_reset() {
            let (arm, value) = match reset {
                AdvanceReset::SlowCurrentChange { current_change_ms } => (1, current_change_ms),
                AdvanceReset::Projected { projected_ms } => (2, projected_ms),
                AdvanceReset::Timeout => (3, self.elapsed_ms()),
                AdvanceReset::WallClockCeiling { wall_ms } => (4, wall_ms),
            };
            self.tripped_arm.store(arm, Ordering::Relaxed);
            self.tripped_value_bits
                .store(value.to_bits(), Ordering::Relaxed);
            self.tripped.store(true, Ordering::Release);
            return true;
        }
        false
    }

    /// The arm that latched `tripped` mid-fetch, reconstructed for the reset
    /// message. `None` when nothing tripped.
    pub fn tripped_reset(&self) -> Option<AdvanceReset> {
        if !self.tripped.load(Ordering::Acquire) {
            return None;
        }
        let value = f64::from_bits(self.tripped_value_bits.load(Ordering::Relaxed));
        Some(match self.tripped_arm.load(Ordering::Relaxed) {
            1 => AdvanceReset::SlowCurrentChange {
                current_change_ms: value,
            },
            2 => AdvanceReset::Projected {
                projected_ms: value,
            },
            4 => AdvanceReset::WallClockCeiling { wall_ms: value },
            _ => AdvanceReset::Timeout,
        })
    }

    pub fn elapsed_ms(&self) -> f64 {
        self.elapsed().as_secs_f64() * 1000.0
    }

    /// Wall-clock elapsed WITHOUT the delivery-time exclusion (arm 0).
    pub fn raw_elapsed_ms(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
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

/// Whether a gate is armed on THIS thread. Rust-only (no TS twin): the gate is
/// a thread-local invention, and this is the observability hook the
/// `AdvanceStream` tests use to pin that a stream suspended at a `Yield`
/// leaves the shard thread DISARMED (a neighbouring client group's fetch must
/// never read a suspended advance's budget).
pub fn is_armed() -> bool {
    ADVANCE_GATE.with(|g| g.borrow().is_some())
}

/// Called from the row-read loop between rows. `true` → the in-flight advance
/// has blown its economic budget mid-fetch; stop producing rows. Cheap: a
/// thread-local read; the caller throttles how often it asks.
pub fn should_stop_fetch() -> bool {
    ADVANCE_GATE.with(|g| g.borrow().as_ref().is_some_and(|g| g.over_budget()))
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
    fn yield_wait_is_excluded_from_budget() {
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

    #[test]
    fn projected_batch_cost_trips() {
        // 50ms to process 25 of 100 changes → projected ~200ms. Budget 10ms, so
        // ~200 > 10*1.5=15 → reset. num≥16, processed≥ceil(100*.25)=25, elapsed≥5.
        let g = gate(50, 10.0, 100, 25);
        match g.advance_reset() {
            Some(AdvanceReset::Projected { projected_ms }) => {
                assert!(
                    projected_ms >= 200.0,
                    "projected {projected_ms} should be ≈200"
                );
            }
            other => panic!("expected Projected reset, got {other:?}"),
        }
    }

    #[test]
    fn projected_reset_needs_enough_samples() {
        // Same projection ratio, but only 5 of 100 processed (< 25 sample floor)
        // and elapsed 6ms < old-model floor → no reset yet.
        let g = gate(6, 10.0, 100, 5);
        assert_eq!(g.advance_reset(), None);
    }

    #[test]
    fn projected_reset_disabled_for_small_batches() {
        // num=8 < MIN_PROJECTED_ADVANCEMENT_CHANGES (16): projection never applies,
        // and elapsed 20ms < 50 floor → the old arm doesn't fire either.
        let g = gate(20, 5.0, 8, 4);
        assert_eq!(g.advance_reset(), None);
    }

    #[test]
    fn late_advancement_finishes_instead_of_resetting() {
        // 85 of 100 done (≥0.8) → let it finish even though elapsed (200ms) far
        // exceeds the budget (10ms). Neither the projected nor the old arm fires.
        let g = gate(200, 10.0, 100, 85);
        assert_eq!(g.advance_reset(), None);
    }

    #[test]
    fn slow_current_change_trips_regardless_of_late_finish() {
        // Even when 85% done (late-finish), a single change that alone took
        // 60ms (> 50 floor and > budget 10ms) always resets.
        let g = gate(200, 10.0, 100, 85);
        g.set_current_change_start(g.elapsed_ms() - 60.0);
        match g.advance_reset() {
            Some(AdvanceReset::SlowCurrentChange { current_change_ms }) => {
                assert!(
                    (60.0..70.0).contains(&current_change_ms),
                    "current change {current_change_ms} should be ≈60",
                );
            }
            other => panic!("expected SlowCurrentChange reset, got {other:?}"),
        }
    }

    #[test]
    fn cleared_current_change_disables_slow_arm() {
        let g = gate(200, 10.0, 100, 85);
        g.set_current_change_start(g.elapsed_ms() - 60.0);
        g.clear_current_change();
        // Slow arm gone; late-finish suppresses the other arms → no reset.
        assert_eq!(g.advance_reset(), None);
    }
}
