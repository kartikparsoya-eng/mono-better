//! Drain coordinator — port of `drain-coordinator.ts` (~76 LOC).
//!
//! Wired into production via `ConnectionRouter::drain` (router.rs), which
//! `main.rs` runs on SIGTERM: client groups are rehomed one at a time with
//! this module's pacing instead of all at once, so a deploy does not stampede
//! the receiving servers with simultaneous reconnect+rehydrate storms. SIGINT
//! (dev ctrl-C) still shuts down immediately.
//!
//! Two drain types (TS semantics):
//! 1. Elective drain: a view-syncer checks `should_drain()` before processing
//!    a replication event; if true it exits its run loop and calls
//!    `drain_next_in()` itself.
//! 2. Force drain: the syncer waits for `force_drain_timeout()`, picks a
//!    view-syncer, stops it, and re-arms via `drain_next_in()` — necessary
//!    for draining servers with no work (nothing to electively drain on).
//!
//! `TARGET_UTILIZATION = 0.6` — drain interval is divided by 0.6 to give the
//! receiving server breathing room.
//! `FORCE_DRAIN_PADDING = 2` ms — extra padding on force drain timeout.

use std::sync::atomic::{AtomicI64, Ordering};

/// The target (additional) utilization to impose on the server that
/// receives the drained connections.
const TARGET_UTILIZATION: f64 = 0.6;

/// Extra padding on force drain timeout (ms).
const FORCE_DRAIN_PADDING: i64 = 2;

/// Drain coordinator — manages elective and forced drains.
///
/// The TS version keeps a resolver + `setTimeout` that `drainNextIn` pushes
/// forward (`clearTimeout` + re-arm). Here both the elective deadline
/// (`next_drain_time`) and the force deadline (`force_drain_at`) are plain
/// wall-clock atomics that `force_drain_timeout()` sleeps against. Sleeping
/// on a stored deadline (instead of a `Notify` armed by a spawned timer)
/// cannot lose a wakeup that fires between arming and awaiting, and needs no
/// runtime to be live at `drain_next_in` time.
pub struct DrainCoordinator {
    /// When the next drain should happen (ms since epoch). 0 = not draining.
    next_drain_time: AtomicI64,
    /// When the force-drain timeout fires (ms since epoch). 0 = not armed.
    force_drain_at: AtomicI64,
}

impl DrainCoordinator {
    pub fn new() -> Self {
        Self {
            next_drain_time: AtomicI64::new(0),
            force_drain_at: AtomicI64::new(0),
        }
    }

    /// Whether a drain should happen now.
    ///
    /// Port of `DrainCoordinator.shouldDrain()`.
    pub fn should_drain(&self) -> bool {
        let ndt = self.next_drain_time.load(Ordering::SeqCst);
        ndt != 0 && ndt <= now_ms()
    }

    /// Start draining. Sets `next_drain_time` and arms (or pushes forward) the
    /// force drain timeout.
    ///
    /// Port of `DrainCoordinator.drainNextIn()`.
    pub fn drain_next_in(&self, interval_ms: u64) {
        // Increase the timeout to give the receiving server space.
        let adjusted = (interval_ms as f64 / TARGET_UTILIZATION) as i64;
        let now = now_ms();

        // TS `drainNextIn` asserts `nextDrainTime <= now` ("should only be called
        // if shouldDrain()"). The router's forced-drain loop upholds this by
        // construction, so we keep it a `debug_assert` (catches a caller logic
        // error in tests/dev) rather than a production panic the original port
        // deliberately avoided.
        debug_assert!(
            self.next_drain_time.load(Ordering::SeqCst) <= now,
            "drain_next_in() should only be called when should_drain() is true"
        );
        self.next_drain_time.store(now + adjusted, Ordering::SeqCst);

        // Push the force-drain deadline forward (TS clearTimeout + setTimeout).
        self.force_drain_at
            .store(now + adjusted + FORCE_DRAIN_PADDING, Ordering::SeqCst);
    }

    /// Wait for the force drain timeout to fire, then disarm it.
    ///
    /// Port of `DrainCoordinator.forceDrainTimeout` (a promise getter). If the
    /// deadline is pushed forward by a concurrent `drain_next_in`, the sleep
    /// re-checks and keeps waiting for the new deadline.
    pub async fn force_drain_timeout(&self) {
        loop {
            let at = self.force_drain_at.load(Ordering::SeqCst);
            if at == 0 {
                // Not armed yet — poll until `drain_next_in` arms it.
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                continue;
            }
            let now = now_ms();
            if now >= at {
                self.force_drain_at.store(0, Ordering::SeqCst);
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis((at - now) as u64)).await;
        }
    }

    /// Whether draining has been initiated.
    ///
    /// Port of `DrainCoordinator.draining` promise.
    pub fn is_draining(&self) -> bool {
        self.next_drain_time.load(Ordering::SeqCst) != 0
    }

    /// Get the next drain time (for testing).
    pub fn next_drain_time(&self) -> i64 {
        self.next_drain_time.load(Ordering::SeqCst)
    }
}

impl Default for DrainCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_next_in_applies_target_utilization() {
        let c = DrainCoordinator::new();
        assert!(!c.is_draining());
        let before = now_ms();
        c.drain_next_in(600);
        let after = now_ms();
        // 600ms / 0.6 = 1000ms adjusted interval.
        let ndt = c.next_drain_time();
        assert!(ndt >= before + 1000 && ndt <= after + 1000);
        assert!(c.is_draining());
        assert!(!c.should_drain());
    }

    #[test]
    fn drain_next_in_zero_should_drain_immediately() {
        let c = DrainCoordinator::new();
        c.drain_next_in(0);
        assert!(c.should_drain());
    }

    #[tokio::test]
    async fn force_drain_timeout_fires_after_padded_interval() {
        let c = DrainCoordinator::new();
        c.drain_next_in(0);
        // Deadline is now + 0/0.6 + 2ms padding — resolves ~immediately.
        tokio::time::timeout(std::time::Duration::from_secs(2), c.force_drain_timeout())
            .await
            .expect("force drain timeout should fire");
        // Disarmed after firing: the next wait blocks until re-armed.
        assert_eq!(c.force_drain_at.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn force_drain_timeout_rearms_per_interval() {
        let c = DrainCoordinator::new();
        c.drain_next_in(0);
        c.force_drain_timeout().await;
        // Re-arm with a real interval; the next timeout waits ~interval/0.6.
        c.drain_next_in(30);
        let start = std::time::Instant::now();
        tokio::time::timeout(std::time::Duration::from_secs(2), c.force_drain_timeout())
            .await
            .expect("re-armed force drain timeout should fire");
        // 30ms / 0.6 = 50ms adjusted; allow generous scheduling slop above.
        assert!(start.elapsed() >= std::time::Duration::from_millis(50));
    }
}
