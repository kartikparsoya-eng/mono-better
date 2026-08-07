//! Drain coordinator — port of `drain-coordinator.ts` (~76 LOC).
//!
//! Two drain types:
//! 1. Elective drain: ViewSyncer checks `should_drain()` before processing
//!    a replication event. If true, exits its run loop and calls
//!    `drain_next_in()`.
//! 2. Force drain: Syncer picks a random ViewSyncer, calls `stop()`,
//!    waits for `force_drain_timeout`, repeats.
//!
//! `TARGET_UTILIZATION = 0.6` — drain interval is divided by 0.6 to give the
//! receiving server breathing room.
//! `FORCE_DRAIN_PADDING = 2` ms — extra padding on force drain timeout.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// The target (additional) utilization to impose on the server that
/// receives the drained connections.
const TARGET_UTILIZATION: f64 = 0.6;

/// Extra padding on force drain timeout (ms).
const FORCE_DRAIN_PADDING: u64 = 2;

/// Drain coordinator — manages elective and forced drains.
///
/// In the TS code, this uses resolvers (promises) and setTimeout.
/// In Rust, we use atomics for `next_drain_time` and a `Notify` for
/// the force drain timeout. The CG thread checks `should_drain()` and
/// calls `drain_next_in()`. The main thread awaits `force_drain_timeout()`.
pub struct DrainCoordinator {
    /// When the next drain should happen (ms since epoch).
    /// 0 = not draining.
    next_drain_time: AtomicI64,
    /// Whether draining has been initiated.
    draining: Arc<Notify>,
    /// Notified when the force drain timeout fires.
    force_drain_notify: Arc<Notify>,
    /// Whether the force drain timeout is armed.
    force_drain_armed: std::sync::Mutex<bool>,
}

impl DrainCoordinator {
    pub fn new() -> Self {
        Self {
            next_drain_time: AtomicI64::new(0),
            draining: Arc::new(Notify::new()),
            force_drain_notify: Arc::new(Notify::new()),
            force_drain_armed: std::sync::Mutex::new(false),
        }
    }

    /// Whether a drain should happen now.
    ///
    /// Port of `DrainCoordinator.shouldDrain()`.
    pub fn should_drain(&self) -> bool {
        let ndt = self.next_drain_time.load(Ordering::SeqCst);
        ndt != 0 && ndt <= now_ms()
    }

    /// Start draining. Sets `next_drain_time` and arms the force drain timeout.
    ///
    /// Port of `DrainCoordinator.drainNextIn()`.
    pub fn drain_next_in(&self, interval_ms: u64) {
        // Mark as draining.
        self.draining.notify_waiters();

        // Increase the timeout to give the receiving server space.
        let adjusted = (interval_ms as f64 / TARGET_UTILIZATION) as i64;
        let now = now_ms();

        // Assert: next_drain_time should be <= now (should_drain() was true).
        // In Rust we just set it without asserting.
        self.next_drain_time
            .store(now + adjusted, Ordering::SeqCst);

        // Arm the force drain timeout.
        *self.force_drain_armed.lock().unwrap() = true;
        let notify = self.force_drain_notify.clone();
        let timeout = (adjusted + FORCE_DRAIN_PADDING as i64) as u64;

        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(timeout)).await;
            notify.notify_waiters();
        });
    }

    /// Wait for the force drain timeout to fire.
    ///
    /// Port of `DrainCoordinator.forceDrainTimeout` (a promise getter).
    pub async fn force_drain_timeout(&self) {
        // If not armed, wait until it is.
        loop {
            let armed = *self.force_drain_armed.lock().unwrap();
            if armed {
                break;
            }
            // Wait briefly and retry.
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        self.force_drain_notify.notified().await;
        *self.force_drain_armed.lock().unwrap() = false;
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
