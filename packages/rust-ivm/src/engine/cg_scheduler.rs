//! Client-group scheduler — multiplexes many engines onto a bounded thread pool.
//!
//! ## Why
//!
//! Today `EngineHandle::spawn()` (napi/src/lib.rs) creates **one OS thread per
//! client group**, unbounded. At a few hundred CGs per sync worker that is a
//! lot of threads on 8 vCPU, and it does not bound.
//!
//! This replaces thread-per-CG with **K worker threads, CGs assigned by stable
//! hash**. Not to be confused with `engine::worker`, which parallelizes *within*
//! one hydrate; this multiplexes *across* client groups.
//!
//! ## The two constraints that shape the design
//!
//! 1. **`Engine` is `!Send`.** It holds `Rc<RefCell<..>>` operator graphs. So an
//!    engine can never move between threads: it must be *constructed on* its
//!    worker and stay there for life. Assignment is therefore a stable hash,
//!    never work-stealing. This is a hard correctness constraint, not a tuning
//!    choice — a work-stealing scheduler here would be unsound.
//!
//! 2. **Hydrates are long.** The napi watchdog treats 43–144s as *legitimate*
//!    for a cold whale hydrate. Naively co-locating CGs on one thread with
//!    run-to-completion jobs would let one whale starve every co-located CG for
//!    minutes — strictly worse than thread-per-CG, where the OS preempts.
//!
//! Constraint 2 is why this scheduler is only possible *after* the hydrate was
//! made resumable (`Engine::begin_hydrate` / `hydrate_step` / `finish_hydrate`).
//! Work is run in **quanta**: a job produces at most `QUANTUM_ROWS` rows, then
//! yields to the next runnable CG. Fairness comes from the round-robin, not
//! from the OS.
//!
//! ## What this does NOT fix
//!
//! A single blocking `sqlite3_step()` on a huge scan still occupies the worker
//! for its full duration — no userspace scheduler can preempt an FFI call. The
//! quantum bounds *rows*, not *time in one row*. Whale queries with enormous
//! single scans still want isolation; that is what `max_engines_per_worker`
//! and the existing interrupt machinery are for.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;

use crate::engine::{Engine, HydrateCursor, QueryResult, QuerySpec};
use crate::streamer::RowChange;

/// Rows a job may produce before yielding to the next runnable client group.
///
/// Tuning note: too small and per-quantum bookkeeping dominates; too large and
/// tail latency for co-located CGs suffers. 256 keeps the yield check well
/// under a microsecond of overhead per row while capping a co-located CG's
/// wait at 256 rows of someone else's work.
pub const QUANTUM_ROWS: usize = 256;

/// How many worker threads to run, from `RUST_IVM_CG_WORKERS`:
///
/// * **unset or `0`** — *thread-per-client-group* (today's behaviour). The napi
///   layer keeps spawning a dedicated OS thread per engine. Nothing changes.
/// * **`N`** — *pooled*: N worker threads, client groups multiplexed onto them.
/// * **`auto`** — pooled with N = available parallelism.
///
/// Default is 0 so this ships dark, matching the convention already used by
/// `RUST_IVM_READ_LANES`. Flip it per-deployment: a box with few, large client
/// groups wants thread-per-CG; a box with many small ones wants pooling.
pub fn configured_workers() -> usize {
    match std::env::var("RUST_IVM_CG_WORKERS").ok().as_deref() {
        None | Some("") | Some("0") => 0,
        Some("auto") => std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4),
        Some(v) => v.parse().unwrap_or(0),
    }
}

type EngineFn = Box<dyn FnOnce(&mut Engine) + Send>;
type MakeEngine = Box<dyn FnOnce() -> Engine + Send>;
type RowSink = Box<dyn FnMut(&RowChange) + Send>;

enum Msg {
    /// Construct an engine **on the worker thread** — `Engine` is `!Send` and
    /// cannot be built elsewhere and moved in.
    Create {
        cg: String,
        make: MakeEngine,
    },
    /// Run a short closure to completion (init, remove_query, ping).
    Immediate {
        cg: String,
        f: EngineFn,
    },
    /// Begin a resumable hydrate. Rows are delivered to `sink` as produced;
    /// `done` receives the results once every query is drained.
    Hydrate {
        cg: String,
        queries: Vec<QuerySpec>,
        sink: RowSink,
        done: Sender<Vec<QueryResult>>,
    },
    Remove {
        cg: String,
    },
    Shutdown,
}

/// A hydrate suspended between quanta.
struct RunnableJob {
    cg: String,
    cursor: HydrateCursor,
    sink: RowSink,
    done: Sender<Vec<QueryResult>>,
}

/// Stable, migration-free assignment of a client group to a worker.
///
/// FxHash-style multiply-xor: cheap and well-distributed for the short opaque
/// ids used as client-group keys. Deliberately *not* `DefaultHasher`, whose
/// output is randomized per process — that would make assignment differ across
/// restarts, which is fine for correctness but makes debugging a specific CG's
/// placement impossible.
pub fn worker_for(cg: &str, workers: usize) -> usize {
    debug_assert!(workers > 0);
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in cg.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % workers as u64) as usize
}

pub struct CgScheduler {
    workers: Vec<Sender<Msg>>,
    handles: Vec<thread::JoinHandle<()>>,
    /// Permanent client-group -> worker assignment. Written once, on first
    /// sight of a client group, and never changed: an engine is `!Send` and
    /// must stay on the thread that built it.
    assignments: Mutex<HashMap<String, usize>>,
    /// Live engine count per worker, for least-loaded placement.
    loads: Vec<AtomicUsize>,
}

impl CgScheduler {
    /// Spawn `k` worker threads. `k` should track available cores, not client
    /// group count — that is the entire point.
    pub fn new(k: usize) -> CgScheduler {
        assert!(k > 0, "scheduler needs at least one worker");
        let mut workers = Vec::with_capacity(k);
        let mut handles = Vec::with_capacity(k);
        for i in 0..k {
            let (tx, rx) = channel::<Msg>();
            let handle = thread::Builder::new()
                .name(format!("rust-ivm-cg-{i}"))
                .spawn(move || worker_loop(rx))
                .expect("spawn cg worker");
            workers.push(tx);
            handles.push(handle);
        }
        let loads = (0..k).map(|_| AtomicUsize::new(0)).collect();
        CgScheduler {
            workers,
            handles,
            assignments: Mutex::new(HashMap::new()),
            loads,
        }
    }

    /// Build from `RUST_IVM_CG_WORKERS`. Returns `None` when pooling is
    /// disabled, in which case the caller keeps thread-per-client-group.
    pub fn from_env() -> Option<CgScheduler> {
        match configured_workers() {
            0 => None,
            k => Some(CgScheduler::new(k)),
        }
    }

    /// Assign `cg` to a worker, or return its existing assignment.
    ///
    /// **Least-loaded, not hashed.** Hashing is stable across restarts, which
    /// reads well, but at low client-group counts it collides badly: 4 groups
    /// over 12 workers collide about half the time, and two engines sharing a
    /// worker halves the inter-CG parallelism that `scripts/parallelism-test.mjs`
    /// measures. Least-loaded placement spreads perfectly at low N and is
    /// equally stable *within* a process, which is all `!Send` confinement
    /// requires.
    fn assign(&self, cg: &str) -> usize {
        let mut map = self.assignments.lock().expect("assignments poisoned");
        if let Some(&w) = map.get(cg) {
            return w;
        }
        let (idx, _) = self
            .loads
            .iter()
            .enumerate()
            .min_by_key(|(_, l)| l.load(Ordering::Relaxed))
            .expect("at least one worker");
        self.loads[idx].fetch_add(1, Ordering::Relaxed);
        map.insert(cg.to_string(), idx);
        idx
    }

    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    fn send(&self, cg: &str, msg: Msg) {
        let idx = self.assign(cg);
        let _ = self.workers[idx].send(msg);
    }

    pub fn create<F: FnOnce() -> Engine + Send + 'static>(&self, cg: &str, make: F) {
        self.send(
            cg,
            Msg::Create {
                cg: cg.to_string(),
                make: Box::new(make),
            },
        );
    }

    /// Run `f` on the engine and block until it completes.
    pub fn with_engine<R, F>(&self, cg: &str, f: F) -> R
    where
        F: FnOnce(&mut Engine) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = channel();
        self.send(
            cg,
            Msg::Immediate {
                cg: cg.to_string(),
                f: Box::new(move |eng| {
                    let _ = tx.send(f(eng));
                }),
            },
        );
        rx.recv().expect("worker dropped the reply channel")
    }

    /// Start a hydrate. Returns a receiver that yields the results once done.
    /// Rows arrive on `sink`, on the worker thread, as they are produced.
    pub fn hydrate<S: FnMut(&RowChange) + Send + 'static>(
        &self,
        cg: &str,
        queries: Vec<QuerySpec>,
        sink: S,
    ) -> Receiver<Vec<QueryResult>> {
        let (done, rx) = channel();
        self.send(
            cg,
            Msg::Hydrate {
                cg: cg.to_string(),
                queries,
                sink: Box::new(sink),
                done,
            },
        );
        rx
    }

    pub fn remove(&self, cg: &str) {
        self.send(cg, Msg::Remove { cg: cg.to_string() });
        let mut map = self.assignments.lock().expect("assignments poisoned");
        if let Some(w) = map.remove(cg) {
            self.loads[w].fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Which worker a client group is on. `None` if never seen.
    pub fn assigned_worker(&self, cg: &str) -> Option<usize> {
        self.assignments
            .lock()
            .expect("assignments poisoned")
            .get(cg)
            .copied()
    }
}

impl Drop for CgScheduler {
    fn drop(&mut self) {
        for w in &self.workers {
            let _ = w.send(Msg::Shutdown);
        }
        for h in self.handles.drain(..) {
            let _ = h.join();
        }
    }
}

fn worker_loop(rx: Receiver<Msg>) {
    let mut engines: HashMap<String, Engine> = HashMap::new();
    let mut runq: VecDeque<RunnableJob> = VecDeque::new();

    loop {
        // Drain the inbox without blocking, so newly-arrived work joins the
        // round-robin promptly rather than waiting for the queue to empty.
        loop {
            match rx.try_recv() {
                Ok(Msg::Shutdown) => return,
                Ok(msg) => {
                    if !handle_msg(msg, &mut engines, &mut runq) {
                        return;
                    }
                }
                Err(_) => break,
            }
        }

        match runq.pop_front() {
            Some(mut job) => {
                let Some(eng) = engines.get_mut(&job.cg) else {
                    // Engine removed mid-hydrate: drop the job. The consumer
                    // sees a closed channel, which is the same signal it gets
                    // for a cancelled hydrate.
                    continue;
                };
                let mut produced = 0;
                let finished = loop {
                    match eng.hydrate_step(&mut job.cursor) {
                        Some(rc) => {
                            (job.sink)(&rc);
                            produced += 1;
                            if produced >= QUANTUM_ROWS {
                                break false;
                            }
                        }
                        None => break true,
                    }
                };
                if finished {
                    let results = eng.finish_hydrate(job.cursor);
                    let _ = job.done.send(results);
                } else {
                    // Yield: back of the queue, so every other runnable CG on
                    // this worker gets a quantum before this one resumes.
                    runq.push_back(job);
                }
            }
            None => {
                // Nothing runnable — block until work arrives.
                match rx.recv() {
                    Ok(Msg::Shutdown) | Err(_) => return,
                    Ok(msg) => {
                        if !handle_msg(msg, &mut engines, &mut runq) {
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Returns false if the worker should stop.
fn handle_msg(
    msg: Msg,
    engines: &mut HashMap<String, Engine>,
    runq: &mut VecDeque<RunnableJob>,
) -> bool {
    match msg {
        Msg::Create { cg, make } => {
            engines.entry(cg).or_insert_with(make);
        }
        Msg::Immediate { cg, f } => {
            if let Some(eng) = engines.get_mut(&cg) {
                f(eng);
            }
        }
        Msg::Hydrate {
            cg,
            queries,
            sink,
            done,
        } => {
            if let Some(eng) = engines.get_mut(&cg) {
                let cursor = eng.begin_hydrate(&queries);
                runq.push_back(RunnableJob {
                    cg,
                    cursor,
                    sink,
                    done,
                });
            }
        }
        Msg::Remove { cg } => {
            engines.remove(&cg);
            runq.retain(|j| j.cg != cg);
        }
        Msg::Shutdown => return false,
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::ast::Ast;
    use crate::ivm::data::{Row, Value};
    use crate::ivm::schema::ColumnType;
    use crate::ivm::source::{MemorySource, Source};
    use rustc_hash::FxHashMap;
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_engine(rows: usize) -> Engine {
        let mut pks = HashMap::new();
        pks.insert("t".to_string(), vec!["id".to_string()]);
        let mut eng = Engine::new(pks);

        let mut cols = HashMap::new();
        cols.insert("id".to_string(), ColumnType::Number { optional: false });
        let src = Rc::new(RefCell::new(MemorySource::new(
            "t",
            cols,
            vec!["id".to_string()],
        )));
        for i in 0..rows {
            let mut m: FxHashMap<String, Value> = FxHashMap::default();
            m.insert("id".to_string(), Value::F64(i as f64));
            let row: Row = Arc::new(m);
            src.borrow_mut()
                .push(crate::ivm::change::SourceChange::Add { row });
        }
        eng.register_source(src as Rc<RefCell<dyn Source>>);
        eng
    }

    fn spec(id: &str) -> QuerySpec {
        QuerySpec {
            query_id: id.to_string(),
            ast: Ast {
                schema: None,
                table: "t".to_string(),
                alias: None,
                where_clause: None,
                related: Vec::new(),
                limit: None,
                order_by: None,
                start: None,
            },
        }
    }

    #[test]
    fn assignment_is_stable_and_never_migrates() {
        for cg in ["cg-a", "cg-b", "client-group-42", ""] {
            let first = worker_for(cg, 8);
            for _ in 0..100 {
                assert_eq!(worker_for(cg, 8), first, "assignment must not vary");
            }
            assert!(first < 8);
        }
    }

    #[test]
    fn assignment_spreads_across_workers() {
        let mut hits = [0usize; 8];
        for i in 0..2000 {
            hits[worker_for(&format!("cg-{i}"), 8)] += 1;
        }
        // Not asking for perfection, just that no worker is starved or swamped.
        for (i, h) in hits.iter().enumerate() {
            assert!(*h > 100, "worker {i} got only {h} of 2000 client groups");
        }
    }

    #[test]
    fn many_client_groups_share_one_worker_thread() {
        let sched = CgScheduler::new(1);
        let counts: Vec<Arc<AtomicUsize>> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let mut dones = Vec::new();

        for (i, c) in counts.iter().enumerate() {
            let cg = format!("cg-{i}");
            sched.create(&cg, move || make_engine(50));
            let c = c.clone();
            dones.push(sched.hydrate(&cg, vec![spec("q")], move |_rc| {
                c.fetch_add(1, Ordering::Relaxed);
            }));
        }
        for d in dones {
            d.recv().expect("hydrate completed");
        }
        for (i, c) in counts.iter().enumerate() {
            assert_eq!(c.load(Ordering::Relaxed), 50, "cg-{i} produced wrong count");
        }
    }

    /// The property that makes co-location safe at all.
    ///
    /// A large hydrate and a small one share a worker. With run-to-completion
    /// jobs the small one could not emit a single row until the large one
    /// finished. With quantum scheduling it must interleave — so the small
    /// hydrate finishes long before the large one has produced all its rows.
    #[test]
    fn a_large_hydrate_does_not_starve_a_small_one() {
        let sched = CgScheduler::new(1);

        let big_rows = QUANTUM_ROWS * 20;
        sched.create("whale", move || make_engine(big_rows));
        sched.create("minnow", || make_engine(1));

        let whale_progress = Arc::new(AtomicUsize::new(0));
        let wp = whale_progress.clone();
        let whale_done = sched.hydrate("whale", vec![spec("q")], move |_| {
            wp.fetch_add(1, Ordering::Relaxed);
        });

        // Observed whale progress at the moment the minnow's row lands.
        let at_minnow = Arc::new(AtomicUsize::new(usize::MAX));
        let am = at_minnow.clone();
        let wp2 = whale_progress.clone();
        let minnow_done = sched.hydrate("minnow", vec![spec("q")], move |_| {
            am.store(wp2.load(Ordering::Relaxed), Ordering::Relaxed);
        });

        minnow_done.recv().expect("minnow completed");
        let observed = at_minnow.load(Ordering::Relaxed);
        assert!(
            observed < big_rows,
            "minnow only ran after the whale finished ({observed} of {big_rows} rows) \
             — scheduling is run-to-completion, not interleaved"
        );

        whale_done.recv().expect("whale completed");
        assert_eq!(whale_progress.load(Ordering::Relaxed), big_rows);
    }

    /// The property that protects `scripts/parallelism-test.mjs`.
    ///
    /// That benchmark runs 4 engines and measures sequential vs parallel wall
    /// time. If two of those 4 land on the same worker, parallelism halves and
    /// the benchmark regresses — which is exactly what hashing would do about
    /// half the time at these counts. Least-loaded placement must give one
    /// worker each while workers are available.
    #[test]
    fn few_client_groups_never_share_a_worker() {
        let sched = CgScheduler::new(12);
        for i in 0..4 {
            sched.create(&format!("cg-{i}"), || make_engine(1));
        }
        let assigned: std::collections::HashSet<usize> = (0..4)
            .map(|i| sched.assigned_worker(&format!("cg-{i}")).expect("assigned"))
            .collect();
        assert_eq!(
            assigned.len(),
            4,
            "4 client groups over 12 workers must not collide; got {assigned:?}"
        );
    }

    #[test]
    fn assignment_is_permanent_once_made() {
        let sched = CgScheduler::new(4);
        sched.create("cg-x", || make_engine(1));
        let first = sched.assigned_worker("cg-x").expect("assigned");
        for _ in 0..50 {
            sched.with_engine("cg-x", |_| ());
            assert_eq!(sched.assigned_worker("cg-x"), Some(first));
        }
    }

    #[test]
    fn env_toggle_selects_the_mode() {
        // Unset/0 means thread-per-client-group: no pool is built.
        unsafe { std::env::remove_var("RUST_IVM_CG_WORKERS") };
        assert_eq!(configured_workers(), 0);
        assert!(CgScheduler::from_env().is_none());

        unsafe { std::env::set_var("RUST_IVM_CG_WORKERS", "3") };
        assert_eq!(configured_workers(), 3);
        assert_eq!(CgScheduler::from_env().map(|s| s.worker_count()), Some(3));

        unsafe { std::env::set_var("RUST_IVM_CG_WORKERS", "auto") };
        assert!(configured_workers() >= 1);

        unsafe { std::env::remove_var("RUST_IVM_CG_WORKERS") };
    }

    #[test]
    fn thread_count_is_bounded_by_workers_not_client_groups() {
        let sched = CgScheduler::new(2);
        assert_eq!(sched.worker_count(), 2);
        let mut dones = Vec::new();
        for i in 0..32 {
            let cg = format!("cg-{i}");
            sched.create(&cg, || make_engine(3));
            dones.push(sched.hydrate(&cg, vec![spec("q")], |_| {}));
        }
        for d in dones {
            d.recv().expect("all 32 client groups hydrate on 2 threads");
        }
    }
}
