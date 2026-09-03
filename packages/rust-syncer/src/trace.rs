//! Env-gated event trace harness for debugging the syncer's connection/advance
//! lifecycle.
//!
//! Enable with `SYNCER_TRACE=1`. Zero cost when off (one cached bool check).
//! Instrument key lifecycle events with:
//!     crate::trace::note("conn-open", &format!("cg={cg} client={id}"));
//! and read the flow as a top-to-bottom log of connection open/close, hydrate
//! start/end (with elapsed), advance start/end, and poke.
//!
//! Intended for correlating a climbing `/census` counter with the event that
//! failed to tear something down (e.g. an advance that never completes, or a
//! connection whose close never fires). Not part of the production path.

use std::sync::OnceLock;

static ENABLED: OnceLock<bool> = OnceLock::new();

#[inline]
pub fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var("SYNCER_TRACE").is_ok())
}

/// Log a free-form lifecycle event. `op` is a short event tag (e.g.
/// `"conn-open"`, `"hydrate-end"`); `msg` carries the context (ids, elapsed).
#[inline]
pub fn note(op: &str, msg: &str) {
    if enabled() {
        eprintln!("[syncer-trace] {op:16} {msg}");
    }
}

/// CPU time consumed by the calling thread so far, in milliseconds
/// (`CLOCK_THREAD_CPUTIME_ID`). Trace-only: paired with a wall-clock lap it
/// separates IVM work from time the shard thread spent waiting for a core —
/// with hundreds of shard threads pinned to a few cores, a long hydrate can
/// show a wall time several times its CPU time. Other tasks that run on this
/// thread during a yield are charged to it too, so read it alongside
/// `yielded_ms`.
pub fn thread_cpu_ms() -> f64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: clock_gettime writes a timespec into the pointer we own; the
    // clock id is a constant supported on Linux and macOS.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        return f64::NAN;
    }
    ts.tv_sec as f64 * 1000.0 + ts.tv_nsec as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    #[test]
    fn thread_cpu_ms_advances_with_cpu_work_and_not_with_sleep() {
        let before = super::thread_cpu_ms();
        assert!(!before.is_nan(), "CLOCK_THREAD_CPUTIME_ID unsupported");
        let start = std::time::Instant::now();
        let mut x = 0u64;
        while start.elapsed() < std::time::Duration::from_millis(30) {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
        let after_work = super::thread_cpu_ms();
        std::thread::sleep(std::time::Duration::from_millis(30));
        let after_sleep = super::thread_cpu_ms();
        assert!(
            after_work - before >= 10.0,
            "busy loop must consume CPU ({x})"
        );
        assert!(
            after_sleep - after_work < 10.0,
            "sleeping must not consume CPU"
        );
    }
}
