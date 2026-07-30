//! Cross-thread SQLite interrupt + job-scoped watchdog (DESIGN §1a, N1/N2).
//!
//! The composability decision: every connection — the actor's today, every
//! pooled worker's later — is opened with [`install_interrupt`] so it carries
//! a `Send + Sync` [`rusqlite::InterruptHandle`]. A cancel/timeout from any
//! thread calls `.interrupt()` to abort a query running on that connection
//! in-flight (returns `SQLITE_INTERRUPT`), closing the "cancel only checked
//! *between* rows" wedge where one runaway SQLite query parks the actor
//! thread uninterruptibly.
//!
//! [`JobWatchdog`] is a single monitor thread owning a registry of
//! `(deadline, InterruptHandle[], CancellationToken)` entries — one per
//! in-flight `EngineHandle::call`. On deadline it flips the token AND
//! `.interrupt()`s every handle; past a hard bound it logs a stuck-actor
//! signal (the actor is wedged past recovery; the caller will surface it).
//! A single thread — not thread-per-job — is the doc's explicit choice (§1a
//! seam 3): the same loop serves serial jobs (one handle) today and parallel
//! jobs (N handles) in Phase 1+ verbatim.
//!
//! This module is deliberately connection-generic and job-scoped (seams 1–3)
//! so Phase 1's worker pool reuses it without rework. Nothing here touches the
//! engine graph; the interrupt handle is the only graph-agnostic abort path.

use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use rusqlite::Connection;

/// Install a cross-thread interrupt handle on a connection.
///
/// Call this at EVERY connection open (actor connection today, every pooled
/// worker connection in Phase 1+). The returned handle is `Send + Sync`; hold
/// it on the actor and call `.interrupt()` from any thread to abort a query
/// running on `conn` in-flight. Never special-case "the actor's connection" —
/// seam 1 is connection-generic by construction.
///
/// `get_interrupt_handle` is infallible (it allocates a small handle object);
/// there is nothing to recover from.
pub fn install_interrupt(conn: &Connection) -> rusqlite::InterruptHandle {
    conn.get_interrupt_handle()
}

/// A registered job under watchdog supervision.
///
/// One entry per in-flight `EngineHandle::call`. At `warn_at` the monitor logs
/// a slow-job signal (NO abort — a legitimate cold hydrate under load can take
/// 43–144s, so warning is harmless but aborting would cause a reset-storm). At
/// `abort_at` the monitor flips `cancel` and `.interrupt()`s every handle —
/// this is the genuinely-stuck bound, a last resort well above any legit
/// operation and well after the view-syncer's graceful advancement-timeout
/// (which calls `cancel()` first). The abort must NOT compete with the graceful
/// path; it exists solely for the wedge where cancel-between-rows never
/// reaches a runaway query (N1).
pub(crate) struct WatchEntry {
    warn_at: Instant,
    abort_at: Instant,
    cancel: CancellationToken,
    /// Shared handle-bag: the monitor reads from it under the lock when firing.
    /// `InterruptHandle` is not `Clone`, so we share the Vec via Arc<Mutex<_>>
    /// rather than cloning handles out per job. The actor's persistent handles
    /// live in one shared bag reused across all jobs; a parallel job in Phase
    /// 1+ passes a per-job bag with the worker handles.
    handles: Arc<Mutex<Vec<rusqlite::InterruptHandle>>>,
    /// Monotonic id assigned at registration; used to unregister on return.
    id: u64,
}

/// Shared state between the monitor thread and registrants.
struct WatchState {
    /// Pending entries, sorted by deadline (the monitor sleeps to the nearest).
    entries: Vec<WatchEntry>,
    /// Next monotonic registration id.
    next_id: u64,
    /// Set to true on drop of the outer [`JobWatchdog`] to stop the monitor.
    shutdown: bool,
}

/// A job-scoped watchdog: a single monitor thread + a deadline registry.
///
/// Built once per engine actor (cheap; one background thread). `call` registers
/// the job's interrupt handles + a deadline before sending the job, and
/// unregisters on return (the [`WatchGuard`] does the unregister on drop, so
/// even a panic can't leak an entry). On deadline the monitor flips the cancel
/// token and `.interrupt()`s the handles; past a hard bound it logs a
/// stuck-actor signal.
///
/// Thread model: exactly one monitor thread. Registrants are the actor's JS
/// callers (via `EngineHandle::call`); they register/unregister under the
/// shared `Mutex` and `Condvar`. The monitor sleeps to the nearest deadline
/// and is woken on registration/unregister/shutdown so it recomputes.
pub struct JobWatchdog {
    inner: Arc<(Mutex<WatchState>, Condvar)>,
    monitor: Option<thread::JoinHandle<()>>,
}

impl Drop for JobWatchdog {
    fn drop(&mut self) {
        {
            let (lock, cv) = &*self.inner;
            let mut s = lock.lock().unwrap();
            s.shutdown = true;
            cv.notify_all();
        }
        if let Some(h) = self.monitor.take() {
            let _ = h.join();
        }
    }
}

impl Default for JobWatchdog {
    fn default() -> Self {
        Self::new()
    }
}

impl JobWatchdog {
    /// Create a new watchdog and start its monitor thread.
    pub fn new() -> Self {
        let inner = Arc::new((
            Mutex::new(WatchState {
                entries: Vec::new(),
                next_id: 1,
                shutdown: false,
            }),
            Condvar::new(),
        ));
        let monitor_inner = inner.clone();
        let monitor = thread::Builder::new()
            .name("rust-ivm-watchdog".into())
            .spawn(move || monitor_loop(monitor_inner))
            .expect("spawn rust-ivm watchdog thread");
        JobWatchdog {
            inner,
            monitor: Some(monitor),
        }
    }

    /// Register a job for watchdog supervision. Returns a guard that
    /// unregisters on drop — keep it alive for the duration of the job.
    ///
    /// `warn_at` is when the monitor logs a slow-job signal (NO abort — a legit
    /// cold hydrate can take 43–144s under load, so we only warn at this bound).
    /// `abort_at` is when the monitor flips `cancel` + `.interrupt()`s the
    /// handles — the genuinely-stuck last-resort bound, well above any legit
    /// op and well after the view-syncer's graceful advancement-timeout. A job
    /// with an empty handle-bag still benefits from the cancel-token flip at
    /// `abort_at`. Typical: `warn_at = now + warn_timeout`,
    /// `abort_at = now + abort_timeout` with `abort_timeout > warn_timeout`.
    pub fn register(
        &self,
        warn_at: Instant,
        abort_at: Instant,
        cancel: CancellationToken,
        handles: Arc<Mutex<Vec<rusqlite::InterruptHandle>>>,
    ) -> WatchGuard {
        let (lock, cv) = &*self.inner;
        let id = {
            let mut s = lock.lock().unwrap();
            let id = s.next_id;
            s.next_id += 1;
            s.entries.push(WatchEntry {
                warn_at,
                abort_at,
                cancel,
                handles,
                id,
            });
            id
        };
        cv.notify_all();
        WatchGuard {
            watchdog: self.inner.clone(),
            id,
        }
    }
}

/// RAII unregister for a registered job. Dropping this removes the entry so
/// the monitor stops supervising the job (it returned in time).
pub struct WatchGuard {
    watchdog: Arc<(Mutex<WatchState>, Condvar)>,
    id: u64,
}

impl Drop for WatchGuard {
    fn drop(&mut self) {
        let (lock, cv) = &*self.watchdog;
        {
            let mut s = lock.lock().unwrap();
            s.entries.retain(|e| e.id != self.id);
        }
        cv.notify_all();
    }
}

fn monitor_loop(inner: Arc<(Mutex<WatchState>, Condvar)>) {
    let (lock, cv) = &*inner;
    // Two independent signals per job: `warned` (logged the slow-job signal)
    // and `aborted` (fired the cancel+interrupt). A job can be warned without
    // being aborted (the common case — slow but legit); abort only fires at
    // the hard bound, well after the warn.
    let mut warned: Vec<u64> = Vec::new();
    let mut aborted: Vec<u64> = Vec::new();
    loop {
        let now = Instant::now();
        let sleep_until = {
            let s = lock.lock().unwrap();
            if s.shutdown {
                return;
            }
            // WARN: log a slow-job signal for entries past `warn_at` that we
            // haven't warned yet. This is NON-ABORTING — a legit cold hydrate
            // under load can take 43–144s, so warning is the only action at
            // this bound. Aborting here would cause a reset-storm.
            for e in s.entries.iter() {
                if now >= e.warn_at && !warned.contains(&e.id) {
                    eprintln!(
                        "[rust-ivm-watchdog] slow-job signal: job {} past warn bound {:?} ago (not aborting — legit hydrates can take minutes)",
                        e.id,
                        now.saturating_duration_since(e.warn_at),
                    );
                    warned.push(e.id);
                }
            }
            // ABORT: at the hard bound, flip cancel + interrupt every handle.
            // This is the genuinely-stuck last resort — well above any legit
            // op and well after the view-syncer's graceful advancement-timeout
            // (which calls cancel() first). It exists solely for the N1 wedge
            // where cancel-between-rows never reaches a runaway query.
            let mut due: Vec<usize> = Vec::new();
            for (i, e) in s.entries.iter().enumerate() {
                if now >= e.abort_at && !aborted.contains(&e.id) {
                    due.push(i);
                }
            }
            for &i in &due {
                let e = &s.entries[i];
                e.cancel.cancel();
                // Interrupt every handle in the shared bag. The bag is locked
                // briefly here; the actor never holds this lock during a query
                // (it only pushes at connection open), so this can't wedge.
                let handles = e.handles.lock().unwrap();
                for h in handles.iter() {
                    h.interrupt();
                }
                drop(handles);
                aborted.push(e.id);
                eprintln!(
                    "[rust-ivm-watchdog] stuck-actor abort: job {} overran abort bound {:?} ago — cancel flipped + handles interrupted",
                    e.id,
                    now.saturating_duration_since(e.abort_at),
                );
            }
            // Sleep to the nearest pending action (warn or abort) across all
            // entries — the monitor wakes early on register/unregister/shutdown.

            s.entries
                .iter()
                .filter_map(|e| {
                    if !warned.contains(&e.id) && now < e.warn_at {
                        Some(e.warn_at)
                    } else if !aborted.contains(&e.id) && now < e.abort_at {
                        Some(e.abort_at)
                    } else {
                        None
                    }
                })
                .min()
        };
        match sleep_until {
            // No pending entries → sleep until woken (registration/shutdown).
            None => {
                let s = lock.lock().unwrap();
                if s.shutdown {
                    return;
                }
                let _ = cv.wait_timeout(s, Duration::from_secs(60));
            }
            Some(d) => {
                let sleep_dur = d.saturating_duration_since(now);
                let s = lock.lock().unwrap();
                if s.shutdown {
                    return;
                }
                let _ = cv.wait_timeout(s, sleep_dur.max(Duration::from_millis(1)));
            }
        }
    }
}

// Re-export the token so callers don't need a separate import path. The token
// is the engine's existing `CancellationToken` (Arc<AtomicBool>); we depend on
// it rather than redefining to keep a single source of truth.
pub use crate::engine::CancellationToken;
