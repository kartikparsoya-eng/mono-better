//! Stream-scoped, generation-tagged credit gate for streaming backpressure.
//!
//! ## Why
//! The streaming hydrate/advance producer runs on the engine actor thread and
//! hands rows to JS across a ThreadsafeFunction. A sync TSFN callback cannot
//! `await`, and blocking the JS main thread would deadlock the very consumer we
//! wait on — so the producer must not run arbitrarily far ahead of the
//! consumer. Before crossing the boundary with `n` rows the producer
//! [`acquire`](StreamCreditGate::acquire)s `n` credits; the JS consumer
//! [`grant`](StreamCreditGate::grant)s them back as it drains rows out of its
//! bounded queue. When credit hits zero the producer PARKS until the consumer
//! catches up (or the stream is cancelled).
//!
//! ## Why generations (not a bare counter)
//! A single shared counter cannot tell a late grant belonging to a FINISHED
//! stream apart from the next one — a stale hydration grant could inflate a
//! subsequent advance's budget and defeat the bound. Every stream gets a fresh
//! `generation`; grants/acquires are tagged with it, and anything tagged with a
//! non-current generation is ignored. Credit is also capped at the stream's
//! capacity so replayed/duplicate grants can never exceed it.
//!
//! ## Cancellation & the watchdog
//! `acquire` observes the SAME [`CancellationToken`] the job watchdog flips on
//! its abort deadline (and that `cancel()`/consumer-abort flips). So a parked
//! producer is unparked uniformly by: the consumer draining (grant), the
//! consumer going away ([`cancel_current`](StreamCreditGate::cancel_current) /
//! [`close`](StreamCreditGate::close)), or the watchdog aborting a truly-stuck
//! job. Credit-wait therefore inherits the existing blocking-TSFN watchdog
//! semantics — it introduces no new false-kill path — and the short poll below
//! is only a belt-and-suspenders fallback; close/cancel are the primary unpark.
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use crate::engine::CancellationToken;

/// Fallback re-check interval while parked. Primary unpark is a `notify_all`
/// from `grant`/`close`/`cancel_current`; this only bounds the worst-case
/// latency if a notification is ever missed.
const POLL: Duration = Duration::from_millis(50);

struct Inner {
    /// Current stream generation. `0` means "no active stream".
    generation: u64,
    /// Credits available to the current generation.
    credit: i64,
    /// Ceiling for the current generation; `credit` never exceeds it.
    capacity: i64,
    /// The current generation has ended (stream done or consumer gone).
    closed: bool,
}

/// See the module docs. Cheap to share behind an `Arc`; all methods take `&self`.
pub struct StreamCreditGate {
    inner: Mutex<Inner>,
    cv: Condvar,
}

impl StreamCreditGate {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                generation: 0,
                credit: 0,
                capacity: 0,
                closed: true,
            }),
            cv: Condvar::new(),
        }
    }

    /// Begin a new stream generation with `capacity` credits (min 1). Returns
    /// the new generation id to tag subsequent `acquire`/`grant`/`close` calls.
    /// Prefer [`StreamCreditGuard::begin`] so the generation is always closed.
    pub fn begin(&self, capacity: i64) -> u64 {
        let cap = capacity.max(1);
        let mut g = self.inner.lock().unwrap();
        g.generation = g.generation.wrapping_add(1);
        if g.generation == 0 {
            g.generation = 1; // never hand out the "no active stream" sentinel
        }
        g.capacity = cap;
        g.credit = cap;
        g.closed = false;
        self.cv.notify_all();
        g.generation
    }

    /// Block until `permits` credits are available for `generation`, then
    /// consume them and return `true`. Returns `false` WITHOUT consuming if the
    /// producer should stop: the generation was superseded, the stream was
    /// closed/cancelled, or `cancel` was flipped (watchdog/consumer abort).
    ///
    /// `permits` is clamped to the generation's capacity so a batch larger than
    /// capacity can never deadlock (it simply waits for a full window).
    #[must_use]
    pub fn acquire(&self, generation: u64, permits: i64, cancel: &CancellationToken) -> bool {
        let mut g = self.inner.lock().unwrap();
        loop {
            if g.generation != generation || g.closed || cancel.is_cancelled() {
                return false;
            }
            let want = permits.clamp(0, g.capacity);
            if g.credit >= want {
                g.credit -= want;
                return true;
            }
            let (ng, _timeout) = self.cv.wait_timeout(g, POLL).unwrap();
            g = ng;
        }
    }

    /// Return `permits` credits to `generation` (the consumer drained that many
    /// rows). Stale (non-current generation) or post-close grants are ignored;
    /// credit is capped at capacity so it can never exceed the bound.
    pub fn grant(&self, generation: u64, permits: i64) {
        if permits <= 0 {
            return;
        }
        let mut g = self.inner.lock().unwrap();
        if g.generation != generation || g.closed {
            return;
        }
        g.credit = (g.credit + permits).min(g.capacity);
        self.cv.notify_all();
    }

    /// Close `generation` (stream finished). Idempotent; ignores a stale id.
    pub fn close(&self, generation: u64) {
        let mut g = self.inner.lock().unwrap();
        if g.generation == generation && !g.closed {
            g.closed = true;
            self.cv.notify_all();
        }
    }

    /// Close whatever generation is current, regardless of id — the out-of-band
    /// consumer-abort path (mirrors `Engine::cancel`). Wakes a parked producer.
    pub fn cancel_current(&self) {
        let mut g = self.inner.lock().unwrap();
        if !g.closed {
            g.closed = true;
            self.cv.notify_all();
        }
    }

    /// The current generation (`0` if none active).
    pub fn current_generation(&self) -> u64 {
        self.inner.lock().unwrap().generation
    }

    #[cfg(test)]
    fn credit_snapshot(&self) -> i64 {
        self.inner.lock().unwrap().credit
    }
}

impl Default for StreamCreditGate {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII owner of a stream generation. Constructing it `begin`s a generation;
/// dropping it `close`s that generation — so EVERY native exit path (normal
/// completion, TSFN failure, cancellation, panic-unwind, reset) wakes any
/// parked waiter and prevents a leaked, forever-open generation.
pub struct StreamCreditGuard {
    gate: std::sync::Arc<StreamCreditGate>,
    generation: u64,
}

impl StreamCreditGuard {
    pub fn begin(gate: std::sync::Arc<StreamCreditGate>, capacity: i64) -> Self {
        let generation = gate.begin(capacity);
        Self { gate, generation }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn gate(&self) -> &StreamCreditGate {
        &self.gate
    }
}

impl Drop for StreamCreditGuard {
    fn drop(&mut self) {
        self.gate.close(self.generation);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn tok() -> CancellationToken {
        CancellationToken::new()
    }

    #[test]
    fn parks_exactly_at_capacity_then_grant_unparks() {
        let gate = Arc::new(StreamCreditGate::new());
        let gen_id = gate.begin(2);
        let c = tok();
        assert!(gate.acquire(gen_id, 1, &c)); // 2 -> 1
        assert!(gate.acquire(gen_id, 1, &c)); // 1 -> 0
        let (g2, c2) = (gate.clone(), c.clone());
        let h = thread::spawn(move || g2.acquire(gen_id, 1, &c2));
        thread::sleep(Duration::from_millis(100));
        assert!(!h.is_finished(), "producer must park at zero credit");
        gate.grant(gen_id, 1);
        assert!(h.join().unwrap(), "grant unparks and the acquire succeeds");
    }

    #[test]
    fn one_grant_permits_exactly_one_row() {
        let gate = Arc::new(StreamCreditGate::new());
        let gen_id = gate.begin(1);
        let c = tok();
        assert!(gate.acquire(gen_id, 1, &c)); // drain to 0
        let (a, b) = (gate.clone(), gate.clone());
        let (ca, cb) = (c.clone(), c.clone());
        let ha = thread::spawn(move || a.acquire(gen_id, 1, &ca));
        let hb = thread::spawn(move || b.acquire(gen_id, 1, &cb));
        thread::sleep(Duration::from_millis(80));
        gate.grant(gen_id, 1); // exactly one credit for two waiters
        thread::sleep(Duration::from_millis(150));
        let done = [ha.is_finished(), hb.is_finished()];
        assert_eq!(
            done.iter().filter(|x| **x).count(),
            1,
            "exactly one producer proceeds on a single grant",
        );
        gate.grant(gen_id, 1); // release the other so we can join cleanly
        assert!(ha.join().unwrap());
        assert!(hb.join().unwrap());
    }

    #[test]
    fn stale_grant_ignored_and_credit_capped() {
        let gate = StreamCreditGate::new();
        let g1 = gate.begin(4);
        gate.close(g1);
        let g2 = gate.begin(4); // fresh generation: credit == 4
        gate.grant(g1, 100); // stale generation → ignored
        assert_eq!(gate.credit_snapshot(), 4, "stale grant must not credit g2");
        gate.grant(g2, 100); // current gen but over capacity → capped
        assert_eq!(gate.credit_snapshot(), 4, "credit must be capped at capacity");
    }

    #[test]
    fn cancel_token_unparks_zero_credit_producer() {
        let gate = Arc::new(StreamCreditGate::new());
        let gen_id = gate.begin(1);
        let c = tok();
        assert!(gate.acquire(gen_id, 1, &c)); // drain to 0
        let (g2, c2) = (gate.clone(), c.clone());
        let h = thread::spawn(move || g2.acquire(gen_id, 1, &c2));
        thread::sleep(Duration::from_millis(80));
        assert!(!h.is_finished());
        c.cancel(); // watchdog / consumer abort flips the shared token
        assert!(
            !h.join().unwrap(),
            "cancel returns false so the producer stops",
        );
    }

    #[test]
    fn close_unparks_and_superseded_generation_stops() {
        let gate = Arc::new(StreamCreditGate::new());
        let gen_id = gate.begin(1);
        let c = tok();
        assert!(gate.acquire(gen_id, 1, &c));
        let (g2, c2) = (gate.clone(), c.clone());
        let h = thread::spawn(move || g2.acquire(gen_id, 1, &c2));
        thread::sleep(Duration::from_millis(80));
        gate.cancel_current(); // consumer gone
        assert!(!h.join().unwrap());
        // A later generation makes the old-gen acquire return false at once.
        let _g3 = gate.begin(1);
        assert!(!gate.acquire(gen_id, 1, &c), "superseded generation stops");
    }

    #[test]
    fn guard_closes_generation_on_drop() {
        let gate = Arc::new(StreamCreditGate::new());
        let c = tok();
        let gen_id = {
            let guard = StreamCreditGuard::begin(gate.clone(), 1);
            let g = guard.generation();
            assert_eq!(gate.current_generation(), g);
            g
        }; // guard dropped here → close(gen)
        assert!(
            !gate.acquire(gen_id, 1, &c),
            "dropping the guard closes the generation",
        );
    }

    #[test]
    fn batch_larger_than_capacity_does_not_deadlock() {
        let gate = Arc::new(StreamCreditGate::new());
        let gen_id = gate.begin(2); // capacity 2
        let c = tok();
        // Acquiring 5 (> capacity) clamps to 2 and succeeds against a full window.
        assert!(gate.acquire(gen_id, 5, &c), "over-capacity acquire clamps, no deadlock");
        assert_eq!(gate.credit_snapshot(), 0);
    }
}
