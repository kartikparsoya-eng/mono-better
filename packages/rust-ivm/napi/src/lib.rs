// NAPI addon for Rust IVM — in-process engine for zero-cache.
//
// Architecture: the engine runs on its OWN dedicated OS thread (an actor),
// one per RustIvmEngine instance — i.e. one per client group, since the
// zero-cache RustIVMDriver creates an engine per CG.
// - The engine graph is Rc<RefCell> (!Send), so it is thread-CONFINED to the
//   actor thread and never touched from the JS thread. All access goes through
//   a Job channel (`EngineHandle::call`), which sends a `FnOnce(&mut EngineState)`
//   to the actor and (for sync methods) blocks on a reply channel.
// - The two heavy methods (hydrate / advance) are exposed as ASYNC #[napi]
//   `AsyncTask`s: their `compute()` runs on a libuv worker (off the JS event
//   loop) and relays to the actor, so multiple CGs' engines compute in PARALLEL
//   on separate OS threads instead of serializing on the single JS thread.
//   (Set UV_THREADPOOL_SIZE >= expected concurrent CGs per sync worker.)
// - `cancel()` is out-of-band: it flips a shared CancellationToken directly
//   (Arc<AtomicBool>), so it can interrupt an advance already running on the
//   actor thread without queueing behind it.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Mutex};

use napi::bindgen_prelude::AsyncTask;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Env, Error as NapiError, JsFunction, Status, Task, bindgen_prelude::*};
use napi_derive::napi;
use rust_ivm::credit::{StreamCreditGate, StreamCreditGuard};
use rust_ivm::engine::{CancellationToken, Engine, QuerySpec, ScalarResetError};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::source::{MemorySource, Source};
use rust_ivm::snapshotter::Snapshotter;
use rust_ivm::snapshotter::spec::{ColumnSchema, LiteAndZqlSpec, TableSpec};
use rust_ivm::sqlite::JobWatchdog;
use rust_ivm::sqlite::table_source::TableSource;

/// Watchdog warn/abort bounds for a single `EngineHandle::call`. The warn
/// bound logs a slow-job signal (NON-aborting — a legit cold hydrate under load
/// can take 43–144s, so warning is the only action there). The abort bound
/// flips cancel + `.interrupt()`s the handles — the genuinely-stuck last
/// resort, well above any legit op and well after the view-syncer's graceful
/// advancement-timeout (which calls `cancel()` first).
///
/// Defaults: warn=120s, abort=600s (10 min, ~4x the 144s legit-hydrate ceiling).
/// The abort must NOT compete with the graceful advancement-timeout; it exists
/// solely for the N1 wedge where cancel-between-rows never reaches a runaway
/// query. Override via env for soak/stress tuning:
///   RUST_IVM_WATCHDOG_WARN_MS   (default 120000)
///   RUST_IVM_WATCHDOG_ABORT_MS  (default 600000)
/// Bounded TSFN queue depth for the per-row streaming callbacks. `1` (default)
/// = the actor parks after every row until JS drains it — O(1) rows in flight,
/// but each row's delivery waits on a full event-loop turn, so a busy main
/// thread (CVR flush / poke serialize / WS writes / other CGs) stalls delivery
/// per-row (microbench: 0.5–5ms main-thread bursts inflate per-row 180–750×).
/// Raising to K lets the actor enqueue up to K rows without parking, so it stops
/// blocking per-row on a contended event loop (rows drain K-per-turn). Trade:
/// O(K) buffered NapiRowChanges (bounded); delivery stays incremental. Env
/// `RUST_IVM_TSFN_QUEUE` (local default 1; the production image pins 64).
fn tsfn_queue_depth() -> usize {
    std::env::var("RUST_IVM_TSFN_QUEUE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(1)
}

/// Streaming-backpressure credit window (#3): the max rows the producer may run
/// ahead of the JS consumer's AsyncQueue drain. Matches the driver's AsyncQueue
/// `#maxBuffer` (256) so the credit gate throttles at the same depth the JS
/// buffer would, bounding in-flight rows to ~capacity regardless of TSFN queue
/// depth. Configurable via `RUST_IVM_STREAM_CREDIT`.
fn stream_credit_capacity() -> i64 {
    std::env::var("RUST_IVM_STREAM_CREDIT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(256) as i64
}

/// Rows per TSFN crossing on the streaming paths. Each streaming delivery
/// batches up to this many `NapiRowChange`s into ONE `tsfn.call` (one JS
/// event-loop turnaround), instead of one call per row — under a busy event
/// loop each turnaround costs ms, so per-row delivery made advance/hydrate
/// latency load-dependent. `RUST_IVM_DELIVERY_CHUNK=1` reproduces per-row-ish
/// delivery (chunks of one; header/END still merge into their chunk). Read
/// once per process.
fn delivery_chunk_size() -> usize {
    static SIZE: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *SIZE.get_or_init(|| {
        std::env::var("RUST_IVM_DELIVERY_CHUNK")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(64)
    })
}

/// A producer must exhaust its interruptible credit gate before it can fill the
/// TSFN queue. Otherwise `credit > queue` lets it acquire another permit and
/// block inside an uninterruptible `tsfn.call(Blocking)`, beyond the reach of
/// cancel/watchdog. Clamping preserves the configured bound and makes every
/// park site cancellation-safe.
///
/// Chunked delivery: one TSFN queue slot now holds one CHUNK (≤ `chunk` rows),
/// so the row-credit window is clamped to `queue_depth * chunk` rows — the
/// producer parks on `credit.acquire` (interruptible) strictly before it could
/// block in `tsfn.call`. The chunk itself is clamped to the configured credit
/// window (an `acquire(chunk_len)` larger than capacity would be rejected), so
/// a tiny operator-configured window (e.g. the backpressure tests' 4) simply
/// shrinks the chunk instead of breaking the stream.
///
/// Returns `(chunk_rows, credit_capacity_rows)`; `capacity >= chunk` always.
fn delivery_window(queue_depth: usize) -> (usize, i64) {
    let credit = stream_credit_capacity(); // >= 1 by construction
    let chunk = delivery_chunk_size().min(credit as usize).max(1);
    let capacity = credit.min((queue_depth as i64).saturating_mul(chunk as i64));
    (chunk, capacity)
}

/// Return freed heap memory to the OS after a CG teardown. glibc retains freed
/// arena memory under reconnect churn (RSS climbs and never drops); `malloc_trim`
/// releases it. Rate-limited to ≤1/s so a churn storm doesn't pay the heap walk
/// on every destroy. No-op off glibc (musl/macOS) where the crate isn't linked.
#[cfg(target_env = "gnu")]
fn maybe_malloc_trim() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST_TRIM_SECS: AtomicU64 = AtomicU64::new(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST_TRIM_SECS.load(Ordering::Relaxed);
    if now > last
        && LAST_TRIM_SECS
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        // SAFETY: malloc_trim is a glibc-only, thread-safe housekeeping call.
        unsafe {
            libc::malloc_trim(0);
        }
    }
}

#[cfg(not(target_env = "gnu"))]
fn maybe_malloc_trim() {}

fn watchdog_bounds() -> (std::time::Duration, std::time::Duration) {
    fn env_ms(name: &str, default: u64) -> u64 {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v: &u64| *v > 0)
            .unwrap_or(default)
    }
    let warn = env_ms("RUST_IVM_WATCHDOG_WARN_MS", 120_000);
    let abort = env_ms("RUST_IVM_WATCHDOG_ABORT_MS", 600_000).max(warn);
    (
        std::time::Duration::from_millis(warn),
        std::time::Duration::from_millis(abort),
    )
}

// ---------------------------------------------------------------------------
// Engine actor — a dedicated thread owning the !Send EngineState.
// ---------------------------------------------------------------------------

/// A unit of work executed on the actor thread with exclusive `&mut EngineState`.
/// The actor loop exits (and destroys the engine) when the last `Sender` drops,
/// i.e. when the owning `RustIvmEngine` and any in-flight `AsyncTask`s are gone.
struct Job(Box<dyn FnOnce(&mut EngineState) + Send>);

/// Handle to an engine actor thread. Cheaply cloneable; `Send + Sync`.
#[derive(Clone)]
struct EngineHandle {
    tx: Sender<Job>,
    /// Populated by `init` once the engine exists; read by out-of-band `cancel`.
    cancel_slot: Arc<Mutex<Option<CancellationToken>>>,
    /// Interrupt handles for Snapshotter's live `prev` and `curr` connections.
    snapshot_interrupt_handles: Arc<Mutex<Vec<rusqlite::InterruptHandle>>>,
    /// Single monitor thread + deadline registry (N2). One per actor; the same
    /// monitor supervises each serial engine job. `call` registers each job with
    /// a deadline.
    watchdog: Arc<JobWatchdog>,
    /// Streaming backpressure gate (#3). The streaming producer on the actor
    /// thread `acquire`s credit before crossing the TSFN boundary; the JS
    /// consumer `grant`s it back out-of-band (`grant_stream_credit`) as it
    /// drains rows, and closes it on early exit (`cancel_stream`). Shared here
    /// so the out-of-band napi methods reach it WITHOUT queueing on the actor
    /// (which is parked inside the stream) — the same reason `cancel` uses
    /// `cancel_slot` directly.
    credit: Arc<StreamCreditGate>,
}

impl EngineHandle {
    fn spawn() -> Self {
        let (tx, rx) = channel::<Job>();
        std::thread::Builder::new()
            .name("rust-ivm-engine".into())
            .spawn(move || {
                let mut state = EngineState::default();
                while let Ok(Job(f)) = rx.recv() {
                    f(&mut state);
                    // Deterministic teardown: DestroyTask sets should_exit so the
                    // thread exits now (freeing its stack) rather than lingering
                    // until the last Sender drops at GC time.
                    if state.should_exit {
                        break;
                    }
                }
                if let Some(ref mut eng) = state.engine {
                    eng.destroy();
                }
            })
            .expect("spawn rust-ivm engine actor thread");
        EngineHandle {
            tx,
            cancel_slot: Arc::new(Mutex::new(None)),
            snapshot_interrupt_handles: Arc::new(Mutex::new(Vec::new())),
            watchdog: Arc::new(JobWatchdog::new()),
            credit: Arc::new(StreamCreditGate::new()),
        }
    }

    /// Run `f` on the actor thread and block until it returns. Used by the
    /// lightweight synchronous methods; the two heavy methods use `AsyncTask`
    /// so they do not block the JS event loop.
    ///
    /// N2: registers the job with the watchdog for the duration of the call.
    /// The soft deadline only logs. At the hard deadline the monitor flips the
    /// cancel token and interrupts both live SQLite registries, aborting a query
    /// that cannot reach its between-row cancellation check. The guard
    /// unregisters on return (even on panic).
    fn call<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut EngineState) -> T + Send + 'static,
    {
        let (rtx, rrx) = channel::<std::thread::Result<T>>();
        // A job with no live connection yet (e.g. init) still benefits from the
        // cancel-token flip at the hard deadline.
        let cancel = self.cancel_slot.lock().unwrap().clone().unwrap_or_default();
        let handles = self.snapshot_interrupt_handles.clone();
        let (warn, abort) = watchdog_bounds();
        let now = std::time::Instant::now();
        let _guard = self
            .watchdog
            .register(now + warn, now + abort, cancel.clone(), handles);
        self.tx
            .send(Job(Box::new(move |s| {
                // Contain any panic so it (a) does NOT unwind out of and kill the
                // actor thread, and (b) surfaces to JS as a THROWN error. This
                // matches the TS engine contract: an unexpected error during
                // advance/hydrate (e.g. a source-drift assert "Row already
                // exists"/"Row not found") propagates as a raw Error →
                // #advancePipelines re-throws → the view-syncer tears down and
                // the client reconnects. We must NOT convert these into a
                // ResetPipelinesSignal (that is the in-place-reset path TS
                // reserves for advancement-timeout/schema-change/etc.).
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(s)));
                let _ = rtx.send(r);
            })))
            .map_err(|_| NapiError::from_reason("engine actor thread is gone"))?;
        match rrx.recv() {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(panic)) => Err(NapiError::from_reason(panic_message(&panic))),
            Err(_) => Err(NapiError::from_reason("engine actor dropped the reply")),
        }
    }
}

/// Map a zql type string to the engine's `ColumnType` (same mapping used for
/// source registration and the planner cost model's table specs).
fn to_column_type(zql_type: &str, optional: bool) -> rust_ivm::ivm::schema::ColumnType {
    match zql_type {
        "boolean" => rust_ivm::ivm::schema::ColumnType::Boolean { optional },
        "number" => rust_ivm::ivm::schema::ColumnType::Number { optional },
        "json" => rust_ivm::ivm::schema::ColumnType::Json { optional },
        _ => rust_ivm::ivm::schema::ColumnType::String { optional },
    }
}

/// Derive per-table column lists straight from the replica schema. Fallback
/// for the planner cost model when the engine has a snapshotter but no
/// initialized sources (harness-only path): the probe's SELECT list does not
/// affect the plan, and constraint columns are treated as non-boolean.
fn replica_table_specs(
    conn: &rusqlite::Connection,
) -> std::result::Result<HashMap<String, HashMap<String, rust_ivm::ivm::schema::ColumnType>>, String>
{
    let mut out = HashMap::new();
    let mut tables_stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        .map_err(|e| format!("plan_ast replica tables: {}", e))?;
    let tables: Vec<String> = tables_stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| format!("plan_ast replica tables: {}", e))?
        .filter_map(|r| r.ok())
        .collect();
    for table in tables {
        let mut cols_stmt = conn
            .prepare("SELECT name FROM pragma_table_info(?)")
            .map_err(|e| format!("plan_ast table_info: {}", e))?;
        let cols: HashMap<String, rust_ivm::ivm::schema::ColumnType> = cols_stmt
            .query_map(rusqlite::params![table], |r| r.get::<_, String>(0))
            .map_err(|e| format!("plan_ast table_info: {}", e))?
            .filter_map(|r| r.ok())
            .map(|c| {
                (
                    c,
                    rust_ivm::ivm::schema::ColumnType::String { optional: false },
                )
            })
            .collect();
        out.insert(table, cols);
    }
    Ok(out)
}

/// Extract a human-readable message from a caught panic payload.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "engine job panicked".to_string())
}

/// If a caught advance panic is a `ScalarResetError` (a resolved scalar
/// subquery's value changed mid-advance), return its message. This maps to a
/// -2 reset row with reason `scalar-subquery` — a transparent in-place reset,
/// NOT a teardown — mirroring TS-native's `ResetPipelinesSignal('scalar-subquery')`
/// and Go's `RPC_CODE_SCALAR_RESET` (-32105).
fn scalar_reset_message(payload: &Box<dyn std::any::Any + Send>) -> Option<String> {
    payload
        .downcast_ref::<ScalarResetError>()
        .map(|e| e.to_string())
}

/// Build a `-2` reset row carrying a `ResetPipelinesSignal` reason + message.
/// The view-syncer consumes this as an in-place reset (rehydrate at curr),
/// distinct from a thrown teardown error.
fn make_reset_row(reason: &str, msg: &str) -> NapiRowChange {
    let row_key = serde_json::json!({
        "reason": reason,
        "msg": msg,
    })
    .to_string();
    NapiRowChange {
        change_type: -2,
        query_id: String::new(),
        table: String::new(),
        row_key,
        row: None,
        is_hidden: false,
    }
}

/// End-of-stream barrier sentinel (changeType = -3). Streamed as the final TSFN
/// call after every real row. The driver's streaming consumers skip changeType
/// == -3 (it is not a data row); it exists purely to anchor `drain_barrier`.
fn end_sentinel() -> NapiRowChange {
    NapiRowChange {
        change_type: -3,
        query_id: String::new(),
        table: String::new(),
        row_key: "{}".to_string(),
        row: None,
        is_hidden: false,
    }
}

/// Deliver the FINAL chunk of a stream (its last element is the `-3` END
/// sentinel) and block the actor thread until JS has actually *executed* the
/// callback for it — a true drain barrier merged with the last data delivery.
///
/// WHY: the streaming tasks fire chunks via `tsfn.call(.., Blocking)` and then
/// return from `compute()`, resolving the async-task promise.
/// The driver closes its row queue on that promise (`hydrated.then(deferClose)`).
/// But `call(Blocking)` only blocks until the queue has *space* (the prior item
/// was popped), NOT until its JS callback finished running. So the final chunk's
/// callback could still be in flight when the promise resolves and the queue is
/// closed → the last rows are silently dropped (differential fuzzer seed 308,
/// worst-case on a fat trailing row). Raising the queue depth only perturbs the
/// timing; it does not fix the race.
///
/// `call_with_return_value` invokes `cb` on the main JS thread *after* the JS
/// callback returns. Because the TSFN queue is FIFO and the main thread is
/// single-threaded, the final chunk's callback runs strictly after every prior
/// chunk's callback has completed. We block the actor thread on a channel that the
/// `cb` signals, so `compute()` cannot return (→ promise resolve → queue close)
/// until every row has been delivered to the driver. Bounded by a generous
/// timeout so a torn-down/aborted TSFN can never wedge the actor thread.
/// Returns Err when delivery could NOT be confirmed — a torn-down TSFN or a
/// 30s timeout. Callers MUST propagate it (fail the task → promise rejects →
/// driver tears down/retries). Swallowing it would resolve the promise with
/// rows possibly undelivered — a silent incomplete hydrate/advance, the exact
/// row-drop class this barrier exists to close.
///
/// Delivering the trailing rows + END + ack in ONE crossing (instead of the old
/// separate END call) merges the barrier round-trip with the last data
/// delivery — one JS-loop turnaround, not two.
fn drain_barrier_chunk(
    tsfn: &ThreadsafeFunction<Vec<NapiRowChange>>,
    final_chunk: Vec<NapiRowChange>,
) -> std::result::Result<(), String> {
    debug_assert!(
        matches!(final_chunk.last(), Some(rc) if rc.change_type == -3),
        "final chunk must end with the END sentinel",
    );
    // NOTE: this scope accrues AFTER the engine's perf_trace::report() for the
    // op has already run (report() clears the stats), so it is surfaced by the
    // `perf_trace::report_residual(..)` call right after each drain_barrier
    // call site (same actor thread → same thread-local stats).
    let _t = rust_ivm::perf_trace::scope("deliver.drain");
    let (tx, rx) = std::sync::mpsc::sync_channel::<()>(1);
    let status = tsfn.call_with_return_value(
        Ok(final_chunk),
        ThreadsafeFunctionCallMode::Blocking,
        move |_ret: napi::JsUnknown| {
            let _ = tx.send(());
            Ok(())
        },
    );
    if status != Status::Ok {
        return Err(format!(
            "drain barrier: TSFN closed before the final chunk could be queued \
             (status {:?}) — stream completeness cannot be confirmed",
            status
        ));
    }
    rx.recv_timeout(std::time::Duration::from_secs(30))
        .map_err(|_| {
            "drain barrier: final-chunk callback did not execute within 30s — \
         stream completeness cannot be confirmed (JS thread wedged or TSFN torn \
         down mid-drain)"
                .to_string()
        })
}

// ---------------------------------------------------------------------------
// ChunkSender — per-CHUNK TSFN delivery (collapses per-row boundary crossings)
// ---------------------------------------------------------------------------

/// Accumulates outgoing rows into chunks of `chunk_size` and crosses the TSFN
/// boundary once per CHUNK instead of once per row. Under a contended JS event
/// loop each crossing waits a full loop turnaround (ms), so per-row delivery
/// made advance latency O(rows × loop-latency); per-chunk delivery is 1-2
/// turnarounds per advance regardless of row count.
///
/// Protocol (element order inside chunks is the exact former per-row order):
/// - the advance header (`-1`) is the FIRST element of the first chunk;
/// - a reset (`-2`) is appended and nothing but END follows it;
/// - the END sentinel (`-3`) is appended to the FINAL chunk, which is delivered
///   through [`drain_barrier_chunk`] so the drain-barrier ack round-trip is
///   merged with the last data delivery;
/// - credit is acquired in ONE `acquire(stream_id, data_rows_in_chunk)` call
///   before the chunk crosses (control rows −1/−2/−3 are not credit-gated,
///   matching the JS side which only grants for data rows).
///
/// The engine's between-rows cancellation checks are untouched — they never
/// crossed the boundary.
struct ChunkSender<'a> {
    tsfn: &'a ThreadsafeFunction<Vec<NapiRowChange>>,
    credit: &'a StreamCreditGate,
    stream_id: u64,
    cancel: CancellationToken,
    chunk_size: usize,
    buf: Vec<NapiRowChange>,
    /// Credit-gated (data) rows currently in `buf`.
    data_in_buf: i64,
    /// Set on credit-closed / TSFN failure; drops all further output.
    stopped: bool,
}

impl<'a> ChunkSender<'a> {
    fn new(
        tsfn: &'a ThreadsafeFunction<Vec<NapiRowChange>>,
        credit: &'a StreamCreditGate,
        stream_id: u64,
        cancel: CancellationToken,
        chunk_size: usize,
    ) -> Self {
        ChunkSender {
            tsfn,
            credit,
            stream_id,
            cancel,
            chunk_size: chunk_size.max(1),
            buf: Vec::with_capacity(chunk_size.max(1) + 1),
            data_in_buf: 0,
            stopped: false,
        }
    }

    /// Append a data row; flushes when the chunk is full.
    fn push_data(&mut self, rc: &rust_ivm::streamer::RowChange) {
        if self.stopped {
            return;
        }
        let napi_rc = {
            let _t = rust_ivm::perf_trace::scope("deliver.json");
            row_change_to_napi(rc)
        };
        self.buf.push(napi_rc);
        self.data_in_buf += 1;
        if self.buf.len() >= self.chunk_size {
            self.flush();
        }
    }

    /// Append a control row (header `-1` / reset `-2`) — not credit-gated and
    /// never triggers a mid-stream flush by itself (a reset is always followed
    /// by `finish`, which flushes it with END appended).
    fn push_control(&mut self, rc: NapiRowChange) {
        if self.stopped {
            return;
        }
        self.buf.push(rc);
    }

    /// Mid-stream flush: one credit acquire for the chunk's data rows, then one
    /// TSFN crossing for the whole chunk. On a closed/cancelled gate or a dead
    /// TSFN, flips the cancel token (the engine's between-rows check ends the
    /// op) and drops the buffered rows — the consumer is discarding anyway.
    fn flush(&mut self) {
        if self.stopped || self.buf.is_empty() {
            return;
        }
        let acquired = {
            let _t = rust_ivm::perf_trace::scope("deliver.credit");
            self.credit
                .acquire(self.stream_id, self.data_in_buf, &self.cancel)
        };
        if !acquired {
            self.cancel.cancel();
            self.stopped = true;
            self.buf.clear();
            self.data_in_buf = 0;
            return;
        }
        let chunk = std::mem::replace(&mut self.buf, Vec::with_capacity(self.chunk_size + 1));
        self.data_in_buf = 0;
        let status = {
            let _t = rust_ivm::perf_trace::scope("deliver.tsfn");
            self.tsfn
                .call(Ok(chunk), ThreadsafeFunctionCallMode::Blocking)
        };
        if status != Status::Ok {
            self.cancel.cancel();
            self.stopped = true;
        }
    }

    /// Ack'd flush for ERROR paths: deliver the buffered rows (header +
    /// pre-failure data) and WAIT for the JS callback to process them before
    /// returning. The task's rejection travels a different queue than the
    /// TSFN callbacks, so a fire-and-forget flush lets the rejection overtake
    /// the header on some schedules (CI-observed) — the driver must record the
    /// advanced version from the header BEFORE it sees the error, as the
    /// per-row protocol guaranteed by ordering. Best-effort: barrier failures
    /// are swallowed — the caller is already surfacing an error.
    fn flush_acked(&mut self) {
        if self.stopped || self.cancel.is_cancelled() || self.buf.is_empty() {
            self.buf.clear();
            self.data_in_buf = 0;
            return;
        }
        if self.data_in_buf > 0 {
            let acquired = {
                let _t = rust_ivm::perf_trace::scope("deliver.credit");
                self.credit
                    .acquire(self.stream_id, self.data_in_buf, &self.cancel)
            };
            if !acquired {
                self.cancel.cancel();
                self.stopped = true;
                self.buf.clear();
                self.data_in_buf = 0;
                return;
            }
        }
        self.data_in_buf = 0;
        let _ = drain_barrier_chunk(self.tsfn, std::mem::take(&mut self.buf));
    }

    /// Terminal flush: appends the END sentinel and delivers the final chunk
    /// through the drain barrier (ack merged with the last data delivery).
    /// A cancelled/stopped stream skips the barrier entirely — on early
    /// abandonment the consumer is discarding rows, so there is no last row to
    /// guarantee, and blocking would stall the actor until the 30s timeout.
    fn finish(mut self) -> std::result::Result<(), String> {
        if self.stopped || self.cancel.is_cancelled() {
            return Ok(());
        }
        if self.data_in_buf > 0 {
            let acquired = {
                let _t = rust_ivm::perf_trace::scope("deliver.credit");
                self.credit
                    .acquire(self.stream_id, self.data_in_buf, &self.cancel)
            };
            if !acquired {
                self.cancel.cancel();
                return Ok(());
            }
        }
        self.buf.push(end_sentinel());
        drain_barrier_chunk(self.tsfn, std::mem::take(&mut self.buf))
    }
}

// ---------------------------------------------------------------------------
// NAPI types (cross-boundary value representations)
// ---------------------------------------------------------------------------

#[napi(object)]
pub struct NapiValue {
    pub kind: String,
    pub bool_val: Option<bool>,
    pub f64_val: Option<f64>,
    pub str_val: Option<String>,
    pub json_val: Option<String>,
}

#[napi(object)]
pub struct NapiRowChange {
    pub change_type: i32,
    pub query_id: String,
    pub table: String,
    /// JSON-encoded row key object, e.g. `{"id":"r1"}`.
    /// Using String + JSON.parse on the JS side is ~10x faster than
    /// HashMap<String, NapiValue> (which creates 5 V8 properties per value).
    pub row_key: String,
    /// JSON-encoded row object, or null.
    pub row: Option<String>,
    /// True when this row belongs to a hidden EXISTS/NOT-EXISTS relationship.
    pub is_hidden: bool,
}

#[napi(object)]
pub struct NapiQuerySpec {
    pub query_id: String,
    pub ast_json: String,
}

#[napi(object)]
pub struct NapiTableSpec {
    pub table: String,
    pub columns: HashMap<String, NapiColumnSchema>,
    pub primary_key: Vec<String>,
    pub unique_keys: Option<Vec<Vec<String>>>,
    pub min_row_version: Option<String>,
}

#[napi(object)]
pub struct NapiColumnSchema {
    pub r#type: String,
    pub optional: bool,
}

// ---------------------------------------------------------------------------
// Synchronous pull-iterator — holds a Rust Iterator, JS drains via next()
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// TS AST deserialization adapter
// ---------------------------------------------------------------------------
// The TS AST JSON format differs from the Rust internal types:
// - Tagged unions use { type: "..." } internal tagging
// - CorrelatedSubquery uses correlation: { parentField, childField }
// - OrderPart is a [string, string] tuple
// - Field names are camelCase

#[derive(serde::Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
enum TsCondition {
    Simple {
        op: String,
        left: TsValuePosition,
        right: TsValuePosition,
    },
    And {
        conditions: Vec<TsCondition>,
    },
    Or {
        conditions: Vec<TsCondition>,
    },
    #[serde(rename = "correlatedSubquery")]
    CorrelatedSubquery {
        related: TsCorrelatedSubquery,
        op: String,
        // Tri-state (matches TS `flip?: boolean`): None = planner decides,
        // Some(true) = force flip, Some(false) = force no-flip.
        #[serde(default)]
        flip: Option<bool>,
        #[serde(default)]
        scalar: bool,
    },
}

#[derive(serde::Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
enum TsValuePosition {
    Column {
        name: String,
    },
    Literal {
        value: serde_json::Value,
    },
    Static {
        anchor: String,
        field: serde_json::Value,
    },
}

#[derive(serde::Deserialize, Clone)]
struct TsCorrelation {
    #[serde(rename = "parentField")]
    parent_field: Vec<String>,
    #[serde(rename = "childField")]
    child_field: Vec<String>,
}

#[derive(serde::Deserialize, Clone)]
struct TsCorrelatedSubquery {
    correlation: TsCorrelation,
    subquery: Box<TsAst>,
    system: Option<String>,
    #[serde(default)]
    hidden: bool,
}

#[derive(serde::Deserialize, Default, Clone)]
#[serde(default, rename_all = "camelCase")]
struct TsAst {
    schema: Option<String>,
    table: String,
    alias: Option<String>,
    r#where: Option<TsCondition>,
    related: Vec<TsCorrelatedSubquery>,
    limit: Option<usize>,
    order_by: Option<Vec<(String, String)>>,
    start: Option<TsBound>,
}

#[derive(serde::Deserialize, Clone)]
struct TsBound {
    row: rust_ivm::ivm::data::Row,
    exclusive: bool,
}

fn parse_ts_ast(json: &str) -> std::result::Result<rust_ivm::builder::ast::Ast, String> {
    let ts: TsAst = match serde_json::from_str(json) {
        Ok(v) => v,
        Err(e) => return Err(format!("{}", e)),
    };
    Ok(convert_ast(ts))
}

fn convert_ast(ts: TsAst) -> rust_ivm::builder::ast::Ast {
    rust_ivm::builder::ast::Ast {
        schema: ts.schema,
        table: ts.table,
        alias: ts.alias.clone(),
        where_clause: ts.r#where.map(convert_condition),
        related: ts.related.iter().map(convert_csq).collect(),
        limit: ts.limit,
        order_by: ts.order_by.map(|parts| {
            parts
                .into_iter()
                .map(|(col, dir)| rust_ivm::builder::ast::OrderPart {
                    column: col,
                    direction: dir,
                })
                .collect()
        }),
        start: ts.start.map(|b| rust_ivm::builder::ast::Bound {
            row: b.row,
            exclusive: b.exclusive,
        }),
    }
}

fn convert_condition(c: TsCondition) -> rust_ivm::builder::ast::Condition {
    use rust_ivm::builder::ast::*;
    match c {
        TsCondition::Simple { op, left, right } => Condition::Simple(SimpleCondition {
            op,
            left: convert_value_position(left),
            right: convert_value_position(right),
        }),
        TsCondition::And { conditions } => {
            Condition::And(conditions.into_iter().map(convert_condition).collect())
        }
        TsCondition::Or { conditions } => {
            Condition::Or(conditions.into_iter().map(convert_condition).collect())
        }
        TsCondition::CorrelatedSubquery {
            related,
            op,
            flip,
            scalar,
        } => Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: convert_csq(&related),
            op,
            flip,
            scalar,
            plan_id: None,
        }),
    }
}

fn convert_value_position(vp: TsValuePosition) -> rust_ivm::builder::ast::ValuePosition {
    use rust_ivm::builder::ast::ValuePosition;
    match vp {
        TsValuePosition::Column { name } => ValuePosition::Column { name },
        TsValuePosition::Literal { value } => ValuePosition::Literal {
            value: json_to_value(value),
        },
        TsValuePosition::Static { anchor, field } => {
            let _ = (anchor, field);
            ValuePosition::Literal {
                value: rust_ivm::ivm::data::Value::Null,
            }
        }
    }
}

fn convert_csq(c: &TsCorrelatedSubquery) -> rust_ivm::builder::ast::RelatedSubquery {
    rust_ivm::builder::ast::RelatedSubquery {
        subquery: Box::new(convert_ast((*c.subquery).clone())),
        relationship_name: c.subquery.alias.clone().unwrap_or_default(),
        parent_key: c.correlation.parent_field.clone(),
        child_key: c.correlation.child_field.clone(),
        hidden: c.hidden,
        system: c.system.as_deref().and_then(|s| match s {
            "permissions" => Some(rust_ivm::ivm::schema::System::Permissions),
            "client" => Some(rust_ivm::ivm::schema::System::Client),
            "test" => Some(rust_ivm::ivm::schema::System::Test),
            _ => None,
        }),
    }
}

fn json_to_value(v: serde_json::Value) -> rust_ivm::ivm::data::Value {
    match v {
        serde_json::Value::Null => rust_ivm::ivm::data::Value::Null,
        serde_json::Value::Bool(b) => rust_ivm::ivm::data::Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                // Match TS: integers beyond ±(2^53-1) are unsupported (would
                // silently lose precision as f64).
                if !(-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&i) {
                    panic!("integer {i} is outside of supported bounds");
                }
                rust_ivm::ivm::data::Value::F64(i as f64)
            } else if let Some(f) = n.as_f64() {
                rust_ivm::ivm::data::Value::F64(f)
            } else {
                rust_ivm::ivm::data::Value::Null
            }
        }
        serde_json::Value::String(s) => rust_ivm::ivm::data::Value::Str(s.into()),
        // Arrays/objects (e.g. an `IN [ids]` list literal) -> JSON string,
        // matching replay.rs json_to_rust_value. Previously these fell through
        // to Null, so `col IN [ids]` compiled to `IN json_each('[]')` and
        // matched NOTHING — silently dropping every row of any IN/NOT IN query
        // (ticketsByIds, userVisibleChannelsV3, ...) on the napi/TableSource
        // path (the fixture/MemorySource path was unaffected, hiding it).
        other => rust_ivm::ivm::data::Value::Json(other.to_string().into()),
    }
}

// ---------------------------------------------------------------------------
// NAPI Engine — a JS-thread-confined value
// ---------------------------------------------------------------------------

/// Convert a JSON value to a rusqlite bind value.
fn json_to_rusqlite(v: &serde_json::Value) -> std::result::Result<rusqlite::types::Value, String> {
    Ok(match v {
        serde_json::Value::Null => rusqlite::types::Value::Null,
        serde_json::Value::Bool(b) => rusqlite::types::Value::Integer(*b as i64),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rusqlite::types::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                rusqlite::types::Value::Real(f)
            } else {
                rusqlite::types::Value::Null
            }
        }
        serde_json::Value::String(s) => rusqlite::types::Value::Text(s.clone()),
        _ => return Err(format!("unsupported bind type: {}", v)),
    })
}

/// The engine state held directly on the JS thread.
fn row_to_json(row: &rusqlite::Row, i: usize) -> std::result::Result<serde_json::Value, String> {
    let v = rust_ivm::sqlite::read_value_lossy(row, i).map_err(|e| format!("col {}: {}", i, e))?;
    Ok(match v {
        rusqlite::types::Value::Null => serde_json::Value::Null,
        rusqlite::types::Value::Integer(i) => {
            // JSON numbers cannot carry SQLite i64 values losslessly through
            // `readQuery`. Tag unsafe integers so the TS runner revives them to
            // bigint and the shared `fromSQLiteTypes` path raises the same
            // UnsupportedValueError as PipelineDriver's safeIntegers(true).
            if !(-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&i) {
                serde_json::json!({"__rustIvmSqliteInteger": i.to_string()})
            } else {
                serde_json::Value::Number(i.into())
            }
        }
        rusqlite::types::Value::Real(f) => sqlite_real_to_json(f),
        rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
        rusqlite::types::Value::Blob(b) => {
            serde_json::Value::String(String::from_utf8_lossy(&b).into_owned())
        }
    })
}

fn sqlite_real_to_json(value: f64) -> serde_json::Value {
    serde_json::Number::from_f64(value)
        .map(serde_json::Value::Number)
        .unwrap_or_else(|| {
            let encoded = if value.is_nan() {
                "NaN"
            } else if value.is_sign_negative() {
                "-Infinity"
            } else {
                "Infinity"
            };
            serde_json::json!({"__rustIvmSqliteReal": encoded})
        })
}

fn ast_to_ts_json(ast: &rust_ivm::builder::ast::Ast) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    if let Some(schema) = &ast.schema {
        out.insert("schema".into(), schema.clone().into());
    }
    out.insert("table".into(), ast.table.clone().into());
    if let Some(alias) = &ast.alias {
        out.insert("alias".into(), alias.clone().into());
    }
    if let Some(condition) = &ast.where_clause {
        out.insert("where".into(), condition_to_ts_json(condition));
    }
    if !ast.related.is_empty() {
        out.insert(
            "related".into(),
            ast.related.iter().map(related_to_ts_json).collect(),
        );
    }
    if let Some(limit) = ast.limit {
        out.insert("limit".into(), limit.into());
    }
    if let Some(order) = &ast.order_by {
        out.insert(
            "orderBy".into(),
            order
                .iter()
                .map(|part| serde_json::json!([part.column, part.direction]))
                .collect(),
        );
    }
    if let Some(start) = &ast.start {
        out.insert(
            "start".into(),
            serde_json::json!({
                "row": value_map_to_json_value(&start.row),
                "exclusive": start.exclusive,
            }),
        );
    }
    serde_json::Value::Object(out)
}

fn related_to_ts_json(related: &rust_ivm::builder::ast::RelatedSubquery) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    out.insert("subquery".into(), ast_to_ts_json(&related.subquery));
    out.insert(
        "correlation".into(),
        serde_json::json!({
            "parentField": related.parent_key,
            "childField": related.child_key,
        }),
    );
    serde_json::Value::Object(out)
}

fn condition_to_ts_json(condition: &rust_ivm::builder::ast::Condition) -> serde_json::Value {
    use rust_ivm::builder::ast::{Condition, ValuePosition};
    let position = |value: &ValuePosition| match value {
        ValuePosition::Column { name } => serde_json::json!({"type": "column", "name": name}),
        ValuePosition::Literal { value } => {
            serde_json::json!({"type": "literal", "value": value_to_serde_json(value)})
        }
    };
    match condition {
        Condition::Simple(simple) => serde_json::json!({
            "type": "simple",
            "op": simple.op,
            "left": position(&simple.left),
            "right": position(&simple.right),
        }),
        Condition::And(conditions) => serde_json::json!({
            "type": "and",
            "conditions": conditions.iter().map(condition_to_ts_json).collect::<Vec<_>>(),
        }),
        Condition::Or(conditions) => serde_json::json!({
            "type": "or",
            "conditions": conditions.iter().map(condition_to_ts_json).collect::<Vec<_>>(),
        }),
        Condition::CorrelatedSubquery(csq) => {
            let mut out = serde_json::Map::new();
            out.insert("type".into(), "correlatedSubquery".into());
            out.insert("op".into(), csq.op.clone().into());
            out.insert("related".into(), related_to_ts_json(&csq.related));
            if csq.scalar {
                out.insert("scalar".into(), true.into());
            }
            if let Some(flip) = csq.flip {
                out.insert("flip".into(), flip.into());
            }
            serde_json::Value::Object(out)
        }
    }
}

fn value_map_to_json_value(map: &rust_ivm::ivm::data::Row) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for (key, value) in map.iter() {
        out.insert(key.to_string(), value_to_serde_json(value));
    }
    serde_json::Value::Object(out)
}

struct EngineState {
    engine: Option<Engine>,
    snapshotter: Option<Snapshotter>,
    syncable_tables: HashMap<String, LiteAndZqlSpec>,
    all_table_names: HashSet<String>,
    sources: HashMap<String, std::rc::Rc<RefCell<dyn Source>>>,
    primary_keys: HashMap<String, Vec<String>>,
    /// Set true when a non-scalar panic (e.g. a source-drift assert) was caught
    /// mid-advance. The engine graph may be left half-mutated; the restored
    /// engine must NOT advance again until it is rehydrated. A poisoned engine
    /// forces the next advance to emit a reset (rehydrate) instead of running
    /// on the inconsistent graph — defense-in-depth for the case where a caller
    /// retries advance rather than tearing down after the thrown error. Cleared
    /// by (re)hydrate / reset / init.
    poisoned: bool,
    /// Version-keyed table row-count cache for the planner cost model, shared
    /// across `plan_ast` calls so a connection-init burst of `addQuery`s reuses
    /// one `COUNT(*)` per table rather than re-counting per query. Self-
    /// invalidates on snapshot version change (see `create_snapshot_cost_model_cached`).
    plan_count_cache: rust_ivm::planner::PlanCountCache,
    /// Prepared (sorted) table specs for the scanstatus cost model, built once
    /// per schema and reused across every `plan_ast`. The specs are the full
    /// visible-column map of every syncable table — invariant between init and
    /// reset — so rebuilding them per `addQuery` (as the first cut did) is pure
    /// allocator churn that ratchets resident set upward under the system
    /// allocator. Lazily populated on first plan; `None` until then. Cleared by
    /// `reset` and (via `EngineState::default`) by `init`.
    cost_specs_cache: Option<std::rc::Rc<rust_ivm::sqlite::sqlite_cost_model::PreparedTableSpecs>>,
    /// The whole scanstatus cost model, cached and keyed by snapshot version —
    /// the port of TS `PipelineDriver.#costModels` (a `Map<db, costModel>`; see
    /// `#ensureCostModelExistsIfEnabled`). Within one snapshot version a burst of
    /// `addQuery`s reuses the SAME model, so its `SQLiteStatFanout` fanout cache
    /// stays warm (no per-query stat4 recompute) and the closure isn't rebuilt.
    /// Rebuilt only when the snapshot advances (new version ⇒ new pinned conn),
    /// exactly as TS re-creates the model for a new `db`. Cleared on reset/init.
    cost_model_cache: Option<(String, rust_ivm::planner::ConnectionCostModel)>,
    /// Set by `DestroyTask` after teardown so the actor loop exits promptly and
    /// frees its 2 MB stack — instead of lingering (blocked on `rx.recv()`) until
    /// V8 GCs the owning `RustIvmEngine`. Under reconnect churn, GC-timing-bound
    /// thread lingering would accumulate stacks; this makes teardown deterministic.
    should_exit: bool,
}

impl Default for EngineState {
    fn default() -> Self {
        EngineState {
            engine: None,
            snapshotter: None,
            syncable_tables: HashMap::new(),
            all_table_names: HashSet::new(),
            sources: HashMap::new(),
            primary_keys: HashMap::new(),
            poisoned: false,
            plan_count_cache: std::rc::Rc::new(RefCell::new((String::new(), HashMap::new()))),
            cost_specs_cache: None,
            cost_model_cache: None,
            should_exit: false,
        }
    }
}

#[napi]
pub struct RustIvmEngine {
    handle: EngineHandle,
}

#[napi]
impl RustIvmEngine {
    #[napi(constructor)]
    pub fn new() -> Result<Self> {
        Ok(RustIvmEngine {
            handle: EngineHandle::spawn(),
        })
    }

    /// Initialize ONLY the snapshotter (replica.db connection) so that
    /// `read_query` works before `init` is called. This solves the
    /// chicken-and-egg: the driver needs to read schema from replica.db
    /// (via read_query) to build table specs for `init`, but `init` is
    /// what normally creates the snapshotter.
    ///
    /// Call this FIRST, then `computeZqlSpecs` via `read_query`, then `init`.
    /// `init` will skip snapshotter creation if this already did it.
    #[napi]
    pub fn init_snapshotter(&self, db_path: String, app_id: String) -> Result<()> {
        let snapshot_reg = self.handle.snapshot_interrupt_handles.clone();
        self.handle
            .call(move |state| -> std::result::Result<(), String> {
                if state.snapshotter.is_some() {
                    return Ok(()); // already initialized
                }
                let mut snap = Snapshotter::new(&db_path, &app_id, None);
                snap.set_snapshot_interrupt_registry(snapshot_reg);
                snap.init()
                    .map_err(|e| format!("snapshotter init: {}", e))?;
                eprintln!(
                    "[rust-ivm] snapshotter pre-initialized at version {}",
                    snap.current_version().unwrap_or("?")
                );
                state.snapshotter = Some(snap);
                Ok(())
            })?
            .map_err(NapiError::from_reason)
    }

    /// Initialize the engine with table schemas and optional SQLite db_path.
    /// When db_path is provided, creates TableSource instances backed by SQLite.
    /// When no db_path, creates MemorySource instances (test/dev mode).
    /// If `init_snapshotter` was called first, the existing snapshotter is
    /// reused (its live interrupt registry remains attached).
    #[napi]
    pub fn init(
        &self,
        tables: Vec<NapiTableSpec>,
        db_path: Option<String>,
        app_id: String,
    ) -> Result<()> {
        let cancel_slot = self.handle.cancel_slot.clone();
        let snapshot_interrupt_handles = self.handle.snapshot_interrupt_handles.clone();
        self.handle
            .call(move |state| -> std::result::Result<(), String> {
                // Clear any previous state.
                if let Some(ref mut eng) = state.engine {
                    eng.destroy();
                }
                // Preserve the snapshotter if init_snapshotter was called first.
                let preserved_snap = state.snapshotter.take();
                *state = EngineState::default();
                state.snapshotter = preserved_snap;

                // TS Snapshotter initialization is mandatory. A per-table connection
                // fallback would serve rows outside the pinned snapshot and can mix
                // database versions within one hydrate, so propagate every failure.
                let snapshot_conn = if let Some(ref path) = db_path {
                    if state.snapshotter.is_none() {
                        let mut snap = Snapshotter::new(path, &app_id, None);
                        snap.set_snapshot_interrupt_registry(snapshot_interrupt_handles.clone());
                        snap.init()
                            .map_err(|e| format!("snapshotter init: {}", e))?;
                        eprintln!(
                            "[rust-ivm] snapshotter initialized at version {}",
                            snap.current_version().unwrap_or("?")
                        );
                        state.snapshotter = Some(snap);
                    }
                    Some(
                        state
                            .snapshotter
                            .as_ref()
                            .unwrap()
                            .current_conn()
                            .map_err(|e| format!("snapshotter current connection: {}", e))?,
                    )
                } else {
                    None
                };

                let mut primary_keys = HashMap::new();

                for spec in &tables {
                    let mut columns = HashMap::new();
                    for (col, schema) in &spec.columns {
                        columns
                            .insert(col.clone(), to_column_type(&schema.r#type, schema.optional));
                    }

                    let rc_source: std::rc::Rc<RefCell<dyn Source>> =
                        if let Some(conn) = &snapshot_conn {
                            let table_source = TableSource::new(
                                conn.clone(),
                                &spec.table,
                                columns,
                                spec.primary_key.clone(),
                            );
                            std::rc::Rc::new(RefCell::new(table_source))
                        } else {
                            let source =
                                MemorySource::new(&spec.table, columns, spec.primary_key.clone());
                            std::rc::Rc::new(RefCell::new(source))
                        };
                    state.sources.insert(spec.table.clone(), rc_source);
                    primary_keys.insert(spec.table.clone(), spec.primary_key.clone());

                    // Build syncable table spec for snapshotter diff.
                    let table_spec = TableSpec {
                        name: spec.table.clone(),
                        columns: spec
                            .columns
                            .iter()
                            .map(|(k, v)| {
                                (
                                    k.clone(),
                                    ColumnSchema {
                                        r#type: v.r#type.clone(),
                                        optional: v.optional,
                                    },
                                )
                            })
                            .collect(),
                        unique_keys: spec
                            .unique_keys
                            .clone()
                            .unwrap_or_else(|| vec![spec.primary_key.clone()]),
                        min_row_version: spec.min_row_version.clone(),
                    };
                    let zql_spec: HashMap<String, ColumnSchema> = spec
                        .columns
                        .iter()
                        .map(|(k, v)| {
                            (
                                k.clone(),
                                ColumnSchema {
                                    r#type: v.r#type.clone(),
                                    optional: v.optional,
                                },
                            )
                        })
                        .collect();
                    state.syncable_tables.insert(
                        spec.table.clone(),
                        LiteAndZqlSpec {
                            table_spec,
                            zql_spec,
                        },
                    );
                    state.all_table_names.insert(spec.table.clone());
                }

                let mut eng = Engine::new(primary_keys.clone());
                for source in state.sources.values() {
                    eng.register_source(source.clone());
                }
                for spec in &tables {
                    if let Some(ref mrv) = spec.min_row_version {
                        eng.set_table_spec(&spec.table, Some(mrv.clone()));
                    }
                    // Unique keys (PK plus any unique indexes) drive scalar-subquery
                    // resolution. Default to the primary key when none are provided.
                    let unique_keys = spec
                        .unique_keys
                        .clone()
                        .unwrap_or_else(|| vec![spec.primary_key.clone()]);
                    eng.set_unique_keys(&spec.table, unique_keys);
                }
                // Publish the cancellation token so the JS thread's out-of-band
                // `cancel()` can interrupt an advance running on this actor thread.
                *cancel_slot.lock().unwrap() = Some(eng.cancellation_token());
                state.engine = Some(eng);
                state.primary_keys = primary_keys;

                if let Some(ref path) = db_path {
                    eprintln!("[rust-ivm] sources initialized (db_path={})", path);
                }

                Ok(())
            })?
            .map_err(NapiError::from_reason)
    }

    /// ORACLE/FIXTURE-REPLAY ONLY — not used by the production `RustIVMDriver`.
    /// Add queries and hydrate them on the engine actor, off the JS event loop,
    /// resolving to the FULL row list buffered into one `Vec`. This is
    /// unbounded by design and safe only for the bounded fixture corpora that
    /// `agentic/oracle/*` drives; production hydration must use the credit-gated
    /// `add_queries_streaming_rows` below, which is the only backpressured path.
    #[napi(ts_return_type = "Promise<NapiRowChange[]>")]
    pub fn add_queries_streaming(&self, queries: Vec<NapiQuerySpec>) -> AsyncTask<HydrateTask> {
        AsyncTask::new(HydrateTask {
            handle: self.handle.clone(),
            queries,
        })
    }

    /// ORACLE/FIXTURE-REPLAY ONLY — not used by the production `RustIVMDriver`.
    /// Advance to head, buffering `[header, ...rows]` into one `Vec` (header
    /// changeType=-1; -2 = reset row). Unbounded by design; production advance
    /// must use the credit-gated `advance_to_head_streaming_rows` below.
    #[napi(ts_return_type = "Promise<NapiRowChange[]>")]
    pub fn advance_to_head_streaming(&self) -> AsyncTask<AdvanceTask> {
        AsyncTask::new(AdvanceTask {
            handle: self.handle.clone(),
        })
    }

    /// Add queries and hydrate them, streaming rows in CHUNKS via `on_rows`
    /// (one TSFN crossing per chunk of up to `RUST_IVM_DELIVERY_CHUNK` rows;
    /// the final chunk ends with the `-3` END sentinel). A row-credit gate and
    /// bounded TSFN queue cap in-flight rows independently of result size.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn add_queries_streaming_rows(
        &self,
        env: Env,
        queries: Vec<NapiQuerySpec>,
        #[napi(ts_arg_type = "(err: Error | null, rows: NapiRowChange[]) => void")]
        on_rows: JsFunction,
        // Caller-minted monotonic stream id (#3): the driver passes the same id
        // to `grant_stream_credit`/`cancel_stream` so grants are tagged to this
        // exact stream. See StreamCreditGate.
        stream_id: f64,
    ) -> Result<AsyncTask<HydrateStreamingTask>> {
        let queue_depth = tsfn_queue_depth();
        let (chunk_size, credit_capacity) = delivery_window(queue_depth);
        let tsfn =
            env.create_threadsafe_function(&on_rows, queue_depth, |ctx| Ok(vec![ctx.value]))?;
        Ok(AsyncTask::new(HydrateStreamingTask {
            handle: self.handle.clone(),
            queries,
            tsfn,
            stream_id: stream_id as u64,
            credit_capacity,
            chunk_size,
        }))
    }

    /// Advance to head, streaming rows in CHUNKS via `on_rows`. The header
    /// (changeType=-1) is the first element of the first chunk, change rows
    /// follow, a reset row (changeType=-2) is appended if the engine reported a
    /// reset_reason, and the final chunk ends with the `-3` END sentinel.
    /// Bounded TSFN (queue slots hold chunks, Blocking mode) + row-credit gate
    /// for real backpressure.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn advance_to_head_streaming_rows(
        &self,
        env: Env,
        #[napi(ts_arg_type = "(err: Error | null, rows: NapiRowChange[]) => void")]
        on_rows: JsFunction,
        stream_id: f64,
    ) -> Result<AsyncTask<AdvanceStreamingTask>> {
        let queue_depth = tsfn_queue_depth();
        let (chunk_size, credit_capacity) = delivery_window(queue_depth);
        let tsfn =
            env.create_threadsafe_function(&on_rows, queue_depth, |ctx| Ok(vec![ctx.value]))?;
        Ok(AsyncTask::new(AdvanceStreamingTask {
            handle: self.handle.clone(),
            tsfn,
            stream_id: stream_id as u64,
            credit_capacity,
            chunk_size,
        }))
    }

    #[napi]
    pub fn remove_query(&self, query_id: String) -> Result<()> {
        self.handle.call(move |state| {
            if let Some(ref mut eng) = state.engine {
                eng.remove_query(&query_id);
            }
        })
    }

    /// Scalar-resolved logical AST for public PipelineDriver query metadata.
    #[napi]
    pub fn query_transformed_ast(&self, query_id: String) -> Result<Option<String>> {
        self.handle.call(move |state| {
            state.engine.as_ref().and_then(|engine| {
                engine
                    .transformed_ast(&query_id)
                    .map(|ast| ast_to_ts_json(&ast).to_string())
            })
        })
    }

    /// Sum of successful pipeline hydration times, matching
    /// `PipelineDriver.totalHydrationTimeMs()`.
    #[napi]
    pub fn total_hydration_time_ms(&self) -> Result<f64> {
        self.handle.call(|state| {
            state
                .engine
                .as_ref()
                .map_or(0.0, Engine::total_hydration_time_ms)
        })
    }

    /// Store the driver's pause-aware hydration duration for one query.
    #[napi]
    pub fn set_hydration_time_ms(&self, query_id: String, hydration_time_ms: f64) -> Result<bool> {
        if !hydration_time_ms.is_finite() || hydration_time_ms < 0.0 {
            return Err(NapiError::from_reason(
                "hydration time must be a finite nonnegative number",
            ));
        }
        self.handle.call(move |state| {
            state
                .engine
                .as_mut()
                .is_some_and(|engine| engine.set_hydration_time_ms(&query_id, hydration_time_ms))
        })
    }

    /// Cancel any in-progress advance or hydrate. **Out-of-band**: flips the
    /// shared CancellationToken directly (does NOT queue behind the running job
    /// on the actor thread), so it can actually interrupt an advance in flight.
    ///
    /// N1: ALSO `.interrupt()`s every registered SQLite handle, so a runaway
    /// query that the between-rows cancel check never reaches is hard-aborted
    /// mid-flight (returns SQLITE_INTERRUPT). Without this, one slow SQLite
    /// query wedges the actor thread uninterruptibly until it returns.
    #[napi]
    pub fn cancel(&self) -> Result<()> {
        if let Some(token) = self.handle.cancel_slot.lock().unwrap().as_ref() {
            token.cancel();
        }
        // Hard-abort any in-flight SQLite query on every live snapshot connection.
        let handles = self.handle.snapshot_interrupt_handles.lock().unwrap();
        for h in handles.iter() {
            h.interrupt();
        }
        // A hard cancel also releases a producer parked on stream credit.
        self.handle.credit.cancel_current();
        Ok(())
    }

    /// Grant `permits` streaming-credit back to stream `stream_id` (#3).
    /// **Out-of-band**: mutates the shared credit gate directly WITHOUT queueing
    /// on the actor thread — which is exactly the point, since the actor is
    /// parked inside the stream waiting for these credits. The driver calls this
    /// as it drains rows out of its AsyncQueue. A grant tagged to a finished or
    /// superseded stream is ignored by the gate, and credit is capped at the
    /// window, so a late/duplicate grant can never break the bound.
    #[napi]
    pub fn grant_stream_credit(&self, stream_id: f64, permits: f64) -> Result<()> {
        self.handle.credit.grant(stream_id as u64, permits as i64);
        Ok(())
    }

    /// Close stream `stream_id`'s credit gate (#3). **Out-of-band**. The driver
    /// calls this when the consumer stops early (generator `return`/`throw`) so
    /// the parked producer unparks promptly instead of relying on the fallback
    /// poll. Ignored if `stream_id` is not the current stream.
    #[napi]
    pub fn cancel_stream(&self, stream_id: f64) -> Result<()> {
        self.handle.credit.close(stream_id as u64);
        Ok(())
    }

    /// Reset the engine: clear all pipelines and sources.
    #[napi]
    pub fn reset(&self) -> Result<()> {
        self.handle.call(|state| {
            if let Some(ref mut eng) = state.engine {
                eng.reset();
            }
            state.sources.clear();
            state.primary_keys.clear();
            state.syncable_tables.clear();
            state.all_table_names.clear();
            // Drop cached planner row-counts so post-reset planning recomputes
            // against the fresh snapshot. Version-keyed already, but a reset may
            // re-pin the same version over changed data — clear defensively.
            {
                let mut cache = state.plan_count_cache.borrow_mut();
                cache.0.clear();
                cache.1.clear();
            }
            // Schema may change across reset; drop the prepared cost-model specs
            // and the cached model so the next plan rebuilds both against the new
            // syncable_tables / snapshot.
            state.cost_specs_cache = None;
            state.cost_model_cache = None;
            state.poisoned = false;
        })
    }

    /// Query planner (`#planAstForRust`): plan `ast_json` (TS-shape) with a cost
    /// model backed by the pinned snapshot connection, and return the ordered
    /// `flip` decisions as a JSON array (`true`/`false`/`null`). The TS driver
    /// walks its own AST in the same order (WHERE pre-order then `related`) and
    /// sets `flip` per position. The driver invokes this when the same
    /// `enablePlanner` flag used by PipelineDriver is enabled. Returns `[]` if no
    /// snapshot exists yet.
    ///
    /// Cost model: the scanstatus/stat-fanout model (exact port of TS
    /// `createSQLiteCostModel` — filter-aware, probe SQL prepared against the
    /// snapshot connection). `RUST_IVM_PLANNER_COST_MODEL=count` restores the
    /// legacy filter-blind `COUNT(*)` model (escape hatch).
    #[napi]
    pub fn plan_ast(&self, ast_json: String) -> Result<String> {
        self.handle
            .call(move |state| -> std::result::Result<String, String> {
                let ast_value: serde_json::Value = serde_json::from_str(&ast_json)
                    .map_err(|e| format!("plan_ast parse: {}", e))?;
                let snap = match state.snapshotter.as_ref() {
                    Some(s) => s,
                    None => return Ok("[]".to_string()),
                };
                let (conn, version) = match (snap.current_conn(), snap.current_version()) {
                    (Ok(c), Ok(v)) => (c, v.to_string()),
                    _ => return Ok("[]".to_string()),
                };
                let use_count_model = matches!(
                    std::env::var("RUST_IVM_PLANNER_COST_MODEL").as_deref(),
                    Ok("count")
                );
                let model = if use_count_model {
                    // Legacy model. Reuses row counts across the
                    // connection-init addQuery burst (same snapshot version);
                    // COUNT(*) runs at most once per (table, version).
                    rust_ivm::planner::create_snapshot_cost_model_cached(
                        conn,
                        &version,
                        state.plan_count_cache.clone(),
                    )
                } else {
                    // TS-parity model: scanstatus probe + stat4/stat1 fanout.
                    // Cache the whole model keyed by snapshot version — the port
                    // of TS `PipelineDriver.#costModels` (keyed by db). Within one
                    // version's `addQuery` burst the same model (and its warm
                    // `SQLiteStatFanout` cache) is reused — no per-query stat4
                    // recompute, no closure rebuild. Rebuilt on advance (new
                    // version ⇒ new pinned conn), as TS rebuilds for a new db.
                    let hit = matches!(&state.cost_model_cache, Some((v, _)) if *v == version);
                    if !hit {
                        // Prepared specs mirror TS `tableSpecs.zqlSpec` and are
                        // invariant per schema (syncable_tables changes only at
                        // init/reset), so build+sort them once and reuse across
                        // versions — rebuilding the full column map per plan is
                        // per-call allocator churn.
                        let specs = if state.syncable_tables.is_empty() {
                            // Snapshotter-only harness path: no schema to cache
                            // against (sources absent). Fall back to the replica
                            // schema — the SELECT list does not affect the probe's
                            // plan. Not a prod path.
                            std::rc::Rc::new(
                                rust_ivm::sqlite::sqlite_cost_model::prepare_table_specs(
                                    replica_table_specs(&conn.borrow())?,
                                ),
                            )
                        } else {
                            if state.cost_specs_cache.is_none() {
                                let raw: std::collections::HashMap<
                                    String,
                                    std::collections::HashMap<
                                        String,
                                        rust_ivm::ivm::schema::ColumnType,
                                    >,
                                > = state
                                    .syncable_tables
                                    .iter()
                                    .map(|(table, spec)| {
                                        (
                                            table.clone(),
                                            spec.zql_spec
                                                .iter()
                                                .map(|(col, cs)| {
                                                    (
                                                        col.clone(),
                                                        to_column_type(&cs.r#type, cs.optional),
                                                    )
                                                })
                                                .collect(),
                                        )
                                    })
                                    .collect();
                                state.cost_specs_cache = Some(std::rc::Rc::new(
                                    rust_ivm::sqlite::sqlite_cost_model::prepare_table_specs(raw),
                                ));
                            }
                            state.cost_specs_cache.as_ref().unwrap().clone()
                        };
                        let model =
                            rust_ivm::sqlite::sqlite_cost_model::create_sqlite_cost_model_prepared(
                                conn.clone(),
                                specs,
                            )
                            .map_err(|e| format!("plan_ast cost model: {}", e))?;
                        state.cost_model_cache = Some((version.clone(), model));
                    }
                    state.cost_model_cache.as_ref().unwrap().1.clone()
                };
                // A watchdog interrupt racing the cost-model probe unwinds with
                // a CostProbeInterrupted payload. That is not a planner bug —
                // degrade to "no flips" (the unplanned AST, same contract as
                // the uninitialized-snapshotter path above) instead of letting
                // the panic cross the napi boundary and tear down the CG.
                let planned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    rust_ivm::planner::plan_ast_flips(&ast_value, model)
                }));
                let flips = match planned {
                    Ok(flips) => flips,
                    Err(payload) => {
                        match payload.downcast::<
                            rust_ivm::sqlite::sqlite_cost_model::CostProbeInterrupted,
                        >() {
                            Ok(interrupted) => {
                                eprintln!(
                                    "[rust-ivm] plan_ast: cost-model probe interrupted; \
                                     planning without flips: {}",
                                    interrupted.0
                                );
                                return Ok("[]".to_string());
                            }
                            Err(payload) => std::panic::resume_unwind(payload),
                        }
                    }
                };
                let arr: Vec<serde_json::Value> = flips
                    .iter()
                    .map(|f| match f {
                        Some(b) => serde_json::Value::Bool(*b),
                        None => serde_json::Value::Null,
                    })
                    .collect();
                serde_json::to_string(&arr).map_err(|e| format!("plan_ast serialize: {}", e))
            })?
            .map_err(NapiError::from_reason)
    }

    /// Run a read-only SQL query against the snapshotter's current connection
    /// and return the rows as a JSON array of objects. This is the single-owner
    /// escape hatch: TS never opens its own replica.db connection; all reads go
    /// through the Rust-owned snapshotter connection.
    ///
    /// `params` is an optional JSON array of bind values, e.g. `["schema", "table"]`.
    /// Errors if the snapshotter isn't initialized or the query fails.
    #[napi]
    pub fn read_query(&self, sql: String, params: Option<String>) -> Result<String> {
        self.handle
            .call(move |state| -> std::result::Result<String, String> {
                let snap = state
                    .snapshotter
                    .as_ref()
                    .ok_or_else(|| "Snapshotter not initialized".to_string())?;
                let conn = snap
                    .current_conn()
                    .map_err(|e| format!("No current snapshot: {}", e))?;
                let conn = conn.borrow();
                let mut stmt = conn.prepare(&sql).map_err(|e| format!("prepare: {}", e))?;
                let cols: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

                // Parse bind params from JSON array string.
                let bind_values: Vec<rusqlite::types::Value> = if let Some(ref p) = params {
                    let arr: serde_json::Value =
                        serde_json::from_str(p).map_err(|e| format!("params parse: {}", e))?;
                    match arr {
                        serde_json::Value::Array(a) => {
                            a.iter()
                                .map(json_to_rusqlite)
                                .collect::<std::result::Result<Vec<_>, _>>()?
                        }
                        _ => return Err("params must be a JSON array".to_string()),
                    }
                } else {
                    Vec::new()
                };

                let mut rows: Vec<serde_json::Value> = Vec::new();
                let mut raw_rows = stmt
                    .query(rusqlite::params_from_iter(bind_values.iter()))
                    .map_err(|e| format!("query: {}", e))?;
                while let Some(row) = raw_rows.next().map_err(|e| format!("row: {}", e))? {
                    let mut obj = serde_json::Map::with_capacity(cols.len());
                    for (i, name) in cols.iter().enumerate() {
                        let v = row_to_json(row, i)?;
                        obj.insert(name.clone(), v);
                    }
                    rows.push(serde_json::Value::Object(obj));
                }
                // Propagate a serialization failure — never fall back to an
                // empty "[]", which would silently turn a read error into a
                // no-rows result on the getRow/init/permissions paths.
                serde_json::to_string(&rows).map_err(|e| format!("serialize rows: {}", e))
            })?
            .map_err(NapiError::from_reason)
    }

    /// Advance the Rust snapshotter to head WITHOUT computing a diff
    /// (mirrors TS Snapshotter.advanceWithoutDiff()). Returns the new version.
    ///
    /// Used by the view-syncer's permission-invalidations path and by the
    /// CVR invalidation check. No engine work is triggered.
    #[napi]
    pub fn advance_without_diff(&self) -> Result<String> {
        self.handle
            .call(|state| -> std::result::Result<String, String> {
                let snap = state
                    .snapshotter
                    .as_mut()
                    .ok_or_else(|| "Snapshotter not initialized".to_string())?;
                snap.advance_without_diff()
                    .map_err(|e| format!("advance_without_diff: {}", e))?;
                // Re-point every TableSource at the new curr connection.
                // advance_without_diff swaps prev/curr, so sources that were
                // pointing at the old curr (now prev) would read stale data.
                let curr = snap
                    .current_conn()
                    .map_err(|e| format!("advance_without_diff: {}", e))?;
                for source in state.sources.values() {
                    source.borrow_mut().set_snapshot_db(curr.clone());
                }
                Ok(snap.current_version().unwrap_or_default().to_string())
            })?
            .map_err(NapiError::from_reason)
    }

    /// Read the subscription state (replicaVersion + watermark) from replica.db
    /// via the Rust snapshotter's current connection. Returns JSON
    /// `{"replicaVersion": "...", "watermark": "..."}`.
    ///
    /// Mirrors TS `getSubscriptionState()`. The Rust engine owns replica.db,
    /// so this is the single-owner read path.
    #[napi]
    pub fn get_subscription_state(&self, _app_id: String) -> Result<String> {
        // Mirrors TS getSubscriptionState() — table names are always _zero.*.
        // app_id is accepted for API compatibility but not used in the query.
        let sql = "SELECT c.replicaVersion, s.stateVersion as watermark \
             FROM \"_zero.replicationConfig\" as c \
             JOIN \"_zero.replicationState\" as s ON c.lock = s.lock";
        self.handle
            .call(move |state| -> std::result::Result<String, String> {
                let snap = state
                    .snapshotter
                    .as_ref()
                    .ok_or_else(|| "Snapshotter not initialized".to_string())?;
                let conn = snap
                    .current_conn()
                    .map_err(|e| format!("No current snapshot: {}", e))?;
                let conn = conn.borrow();
                let (replica_version, watermark) = conn
                    .query_row(sql, [], |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                    })
                    .map_err(|e| format!("subscription state: {}", e))?;
                let obj = serde_json::json!({
                    "replicaVersion": replica_version,
                    "watermark": watermark,
                });
                Ok(obj.to_string())
            })?
            .map_err(NapiError::from_reason)
    }

    /// Explicit teardown on client-group drop (rust-ivm-driver.destroy()).
    /// Frees the engine graph, its SQLite reader connections, and the
    /// snapshotter's replica handles NOW rather than waiting for the JS object
    /// to be garbage-collected (which is when the actor thread finally exits).
    /// Matches the TS PipelineDriver.destroy() contract of prompt resource
    /// release. The actor thread itself is cheap (blocked on recv) and still
    /// exits on GC; this just avoids holding SQLite fds + IVM memory per dropped
    /// CG until the next GC cycle.
    ///
    /// ASYNC (Promise): blocks until the actor thread confirms teardown.
    /// This is the single-owner design — no TS connection races with Rust's
    /// because there IS no TS connection. The Rust side drops everything
    /// before the Promise resolves.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn destroy(&self) -> AsyncTask<DestroyTask> {
        // Destroy is actor-queued, while a producer may be parked waiting for
        // credit on that same actor. Cancel out-of-band before queueing it.
        let _ = self.cancel();
        AsyncTask::new(DestroyTask {
            handle: self.handle.clone(),
        })
    }

    #[napi]
    pub fn ping(&self) -> Result<String> {
        Ok("pong".to_string())
    }
}

// The engine actor thread owns EngineState and destroys the engine when the
// last EngineHandle (owner + any in-flight AsyncTask) drops and the channel
// disconnects — so RustIvmEngine needs no explicit Drop.

// ---------------------------------------------------------------------------
// Async tasks — run heavy engine work on the actor thread, off the JS loop.
// ---------------------------------------------------------------------------

/// Hydration task (add_queries_streaming). Resolves to the full row list.
pub struct HydrateTask {
    handle: EngineHandle,
    queries: Vec<NapiQuerySpec>,
}

impl Task for HydrateTask {
    type Output = Vec<NapiRowChange>;
    type JsValue = Vec<NapiRowChange>;

    fn compute(&mut self) -> Result<Self::Output> {
        let queries = std::mem::take(&mut self.queries);
        self.handle
            .call(
                move |state| -> std::result::Result<Vec<NapiRowChange>, String> {
                    // Rehydrate rebuilds pipelines fresh, so any poison is cleared.
                    state.poisoned = false;
                    let eng = state
                        .engine
                        .as_mut()
                        .ok_or_else(|| "Engine not initialized".to_string())?;
                    let mut specs: Vec<QuerySpec> = Vec::with_capacity(queries.len());
                    for q in queries.iter() {
                        let ast: rust_ivm::builder::ast::Ast =
                            parse_ts_ast(&q.ast_json).map_err(|e| {
                                format!("AST parse error for qid={}: {}", q.query_id, e)
                            })?;
                        specs.push(QuerySpec {
                            query_id: q.query_id.clone(),
                            ast,
                        });
                    }
                    let mut rows: Vec<NapiRowChange> = Vec::new();
                    // Single-fetch hydrate: one fetch per pipeline warms operator
                    // state and emits output in the same pass, on the actor's pinned
                    // snapshot connection.
                    eng.add_queries_streaming(&specs, |rc| rows.push(row_change_to_napi(rc)));
                    Ok(rows)
                },
            )?
            .map_err(NapiError::from_reason)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}

/// Advance-to-head task. Resolves to `[header, ...rows]` (see the method doc).
pub struct AdvanceTask {
    handle: EngineHandle,
}

impl Task for AdvanceTask {
    type Output = Vec<NapiRowChange>;
    type JsValue = Vec<NapiRowChange>;

    fn compute(&mut self) -> Result<Self::Output> {
        self.handle
            .call(|state| -> std::result::Result<Vec<NapiRowChange>, String> {
                // A prior panic left the engine possibly half-mutated. Refuse to
                // advance on it; emit a reset so the driver rehydrates first.
                if state.poisoned {
                    state.poisoned = false;
                    return Ok(vec![make_reset_row(
                        "schema-change",
                        "engine reset after a prior advance panic; rehydrating",
                    )]);
                }
                let syncable_tables = state.syncable_tables.clone();
                let all_table_names = state.all_table_names.clone();
                let mut eng = state
                    .engine
                    .take()
                    .ok_or_else(|| "Engine not initialized".to_string())?;
                let mut snapshotter = match state.snapshotter.take() {
                    Some(s) => s,
                    None => {
                        state.engine = Some(eng);
                        return Err("Snapshotter not initialized".to_string());
                    }
                };

                // catch_unwind: an engine panic (e.g. a source-drift assert on
                // the internal `clients` table under reconnect churn) would
                // otherwise cross the napi FFI and SIGABRT the whole syncer,
                // killing every CG on the pod. Mirror TS: emit a -2 reset row so
                // the driver throws ResetPipelinesSignal -> reset + rehydrate.
                let advance = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut header: Option<NapiRowChange> = None;
                    let mut rows: Vec<NapiRowChange> = Vec::new();
                    let result = eng.advance_to_head_stream(
                        &mut snapshotter,
                        &syncable_tables,
                        &all_table_names,
                        |version, num_changes| {
                            let header_key = serde_json::json!({
                                "version": version.to_string(),
                                "numChanges": num_changes as f64,
                                "aborted": false,
                            }).to_string();
                            header = Some(NapiRowChange {
                                change_type: -1,
                                query_id: String::new(),
                                table: String::new(),
                                row_key: header_key,
                                row: None,
                                is_hidden: false,
                            });
                        },
                        |rc| rows.push(row_change_to_napi(rc)),
                    );
                    (result, header, rows)
                }));

                // Restore engine + snapshotter on every path so a follow-up
                // reset()/re-hydrate can run.
                state.engine = Some(eng);
                state.snapshotter = Some(snapshotter);

                match advance {
                    Ok((result, header, mut rows)) => {
                        if let Some(h) = header {
                            rows.insert(0, h);
                        }
                        match result {
                            Ok(advance_result) => {
                                if let Some(reason) = &advance_result.reset_reason {
                                    let reset_key = serde_json::json!({
                                        "reason": reason,
                                        "msg": advance_result.reset_msg.clone().unwrap_or_default(),
                                    }).to_string();
                                    rows.push(NapiRowChange {
                                        change_type: -2,
                                        query_id: String::new(),
                                        table: String::new(),
                                        row_key: reset_key,
                                        row: None,
                                        is_hidden: false,
                                    });
                                }
                                Ok(rows)
                            }
                            Err(e) => Err(format!("advance failed: {}", e)),
                        }
                    }
                    Err(payload) => {
                        // A scalar-subquery value change is a RESET, not a
                        // teardown: emit a -2 reset row with reason
                        // 'scalar-subquery' (TS-native ResetPipelinesSignal /
                        // Go's -32105) → in-place rehydrate at curr.
                        if let Some(msg) = scalar_reset_message(&payload) {
                            Ok(vec![make_reset_row("scalar-subquery", &msg)])
                        } else {
                            // An engine PANIC — a source-drift assert ("Add
                            // duplicate row"/"Remove missing row") OR a Take
                            // boundary assert ("Bound should be set" /
                            // "…BoundNode must be found during fetch").
                            // catch_unwind here keeps it from crossing the napi FFI
                            // and SIGABRT-ing the process, and restores
                            // engine+snapshotter above. We then
                            // surface it as a THROWN error (Err), NOT a -2 reset row —
                            // matching TS, where these asserts throw a raw Error that
                            // #advancePipelines re-throws → view-syncer teardown →
                            // client reconnect. Converting to ResetPipelinesSignal
                            // here would diverge from the TS lifecycle (in-place reset
                            // vs teardown). Only advance_result.reset_reason (above)
                            // maps to a -2 ResetPipelinesSignal, as in TS.
                            //
                            // Mark the engine poisoned: it may be half-mutated, so
                            // the next advance must reset+rehydrate rather than run
                            // on the inconsistent graph (defense-in-depth if the
                            // caller retries instead of tearing down).
                            state.poisoned = true;
                            let msg = panic_message(&payload);
                            eprintln!(
                                "[rust-ivm] advance panicked — surfacing as thrown error (TS teardown parity): {msg}"
                            );
                            Err(format!("engine advance panic: {msg}"))
                        }
                    }
                }
            })?
            .map_err(NapiError::from_reason)
    }

    fn resolve(&mut self, _env: Env, output: Self::Output) -> Result<Self::JsValue> {
        Ok(output)
    }
}

// ---------------------------------------------------------------------------
// Streaming tasks — row-by-row via ThreadsafeFunction
// ---------------------------------------------------------------------------

/// Hydration streaming task. Streams row CHUNKS via TSFN instead of
/// materializing (see ChunkSender for the chunk protocol).
pub struct HydrateStreamingTask {
    handle: EngineHandle,
    queries: Vec<NapiQuerySpec>,
    tsfn: ThreadsafeFunction<Vec<NapiRowChange>>,
    stream_id: u64,
    credit_capacity: i64,
    chunk_size: usize,
}

impl Task for HydrateStreamingTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let queries = std::mem::take(&mut self.queries);
        let tsfn = self.tsfn.clone();
        let credit = self.handle.credit.clone();
        let stream_id = self.stream_id;
        let capacity = self.credit_capacity;
        let chunk_size = self.chunk_size;
        self.handle
            .call(move |state| -> std::result::Result<(), String> {
                // Rehydrate rebuilds pipelines fresh, so any poison is cleared.
                state.poisoned = false;
                let eng = state
                    .engine
                    .as_mut()
                    .ok_or_else(|| "Engine not initialized".to_string())?;
                let mut specs: Vec<QuerySpec> = Vec::with_capacity(queries.len());
                for q in queries.iter() {
                    let ast: rust_ivm::builder::ast::Ast = parse_ts_ast(&q.ast_json)
                        .map_err(|e| format!("AST parse error for qid={}: {}", q.query_id, e))?;
                    specs.push(QuerySpec {
                        query_id: q.query_id.clone(),
                        ast,
                    });
                }
                let cancel = eng.cancellation_token();
                // #3 backpressure: open this stream's credit window. The guard
                // closes it on EVERY exit (return, panic-unwind, cancel) so a
                // parked-nowhere consumer never leaks an open generation.
                let _credit_guard = StreamCreditGuard::begin(credit.clone(), stream_id, capacity);
                // Per-CHUNK delivery: rows accumulate on the actor (no boundary
                // crossing) and cross via one credit acquire + one TSFN call
                // per chunk. The gate-closed path flips cancel; the engine's
                // between-rows cancel check ends the fetch.
                let mut sender =
                    ChunkSender::new(&tsfn, &credit, stream_id, cancel.clone(), chunk_size);
                // Single-fetch hydrate, streaming chunk-by-chunk to JS via the
                // TSFN (blocking backpressure). One fetch per pipeline warms
                // operator state and emits output while the actor graph remains
                // single-writer.
                let checkpoint = eng.source_connection_checkpoint();
                let hydrated = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    eng.add_queries_streaming(&specs, |rc| sender.push_data(rc));
                }));
                if let Err(payload) = hydrated {
                    eng.rollback_source_connections(&checkpoint);
                    std::panic::resume_unwind(payload);
                }
                // Barrier: don't resolve the promise (→ driver closes its row
                // queue) until every streamed row has landed in JS. finish()
                // flushes the trailing rows WITH the END sentinel through the
                // drain barrier (one merged crossing) — see drain_barrier_chunk
                // for the seed-308 last-row-drop it closes. It SKIPS the barrier
                // when cancelled: on early abandonment the consumer is discarding
                // rows, so there is no last row to guarantee — blocking on the
                // barrier would stall this job (and everything queued behind it
                // on the actor) until the barrier's 30s timeout (#3).
                sender.finish()?;
                if !cancel.is_cancelled() {
                    rust_ivm::perf_trace::report_residual("hydrate");
                }
                Ok(())
            })?
            .map_err(NapiError::from_reason)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

/// Advance streaming task. Streams row CHUNKS via TSFN instead of
/// materializing (see ChunkSender for the chunk protocol).
pub struct AdvanceStreamingTask {
    handle: EngineHandle,
    tsfn: ThreadsafeFunction<Vec<NapiRowChange>>,
    stream_id: u64,
    credit_capacity: i64,
    chunk_size: usize,
}

impl Task for AdvanceStreamingTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let tsfn = self.tsfn.clone();
        let credit = self.handle.credit.clone();
        let stream_id = self.stream_id;
        let capacity = self.credit_capacity;
        let chunk_size = self.chunk_size;
        self.handle
            .call(move |state| -> std::result::Result<(), String> {
                // A prior panic left the engine possibly half-mutated. Refuse to
                // advance on it; stream a reset so the driver rehydrates first.
                if state.poisoned {
                    state.poisoned = false;
                    drain_barrier_chunk(
                        &tsfn,
                        vec![
                            make_reset_row(
                                "schema-change",
                                "engine reset after a prior advance panic; rehydrating",
                            ),
                            end_sentinel(),
                        ],
                    )?;
                    rust_ivm::perf_trace::report_residual("advance");
                    return Ok(());
                }
                let syncable_tables = state.syncable_tables.clone();
                let all_table_names = state.all_table_names.clone();
                let mut eng = state
                    .engine
                    .take()
                    .ok_or_else(|| "Engine not initialized".to_string())?;
                let mut snapshotter = match state.snapshotter.take() {
                    Some(s) => s,
                    None => {
                        state.engine = Some(eng);
                        return Err("Snapshotter not initialized".to_string());
                    }
                };

                let cancel = eng.cancellation_token();
                // #3 backpressure window for this advance stream. Closed on
                // every exit by the guard (data rows below are credit-gated;
                // the O(1) header/reset rows are not — extra consumer grants for
                // them are capped at the window, so the bound still holds).
                let _credit_guard =
                    StreamCreditGuard::begin(credit.clone(), stream_id, capacity);

                // Per-CHUNK delivery. The header is the first element of the
                // first chunk; data rows are credit-gated per chunk (one
                // acquire(chunk_len) each); a reset/END lands in the final
                // barriered chunk (see finish below). RefCell: the header and
                // row callbacks both feed the same sender (single actor thread).
                let sender = RefCell::new(ChunkSender::new(
                    &tsfn,
                    &credit,
                    stream_id,
                    cancel.clone(),
                    chunk_size,
                ));
                let advance = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    eng.advance_to_head_stream(
                        &mut snapshotter,
                        &syncable_tables,
                        &all_table_names,
                        |version, num_changes| {
                            let _t = rust_ivm::perf_trace::scope("deliver.header");
                            let header_key = serde_json::json!({
                                "version": version.to_string(),
                                "numChanges": num_changes as f64,
                                "aborted": false,
                            }).to_string();
                            sender.borrow_mut().push_control(NapiRowChange {
                                change_type: -1,
                                query_id: String::new(),
                                table: String::new(),
                                row_key: header_key,
                                row: None,
                                is_hidden: false,
                            });
                        },
                        |rc| sender.borrow_mut().push_data(rc),
                    )
                }));

                state.engine = Some(eng);
                state.snapshotter = Some(snapshotter);

                let mut sender = sender.into_inner();
                match advance {
                    Ok(result) => {
                        match result {
                            Ok(advance_result) => {
                                if let Some(reason) = &advance_result.reset_reason {
                                    let reset_key = serde_json::json!({
                                        "reason": reason,
                                        "msg": advance_result.reset_msg.clone().unwrap_or_default(),
                                    }).to_string();
                                    sender.push_control(NapiRowChange {
                                        change_type: -2,
                                        query_id: String::new(),
                                        table: String::new(),
                                        row_key: reset_key,
                                        row: None,
                                        is_hidden: false,
                                    });
                                }
                                // finish() delivers the final chunk (trailing
                                // rows + optional reset + END) through the
                                // merged drain barrier; it skips the barrier on
                                // early abandonment (#3): a discarding consumer
                                // has no last row to await.
                                sender.finish()?;
                                if !cancel.is_cancelled() {
                                    rust_ivm::perf_trace::report_residual("advance");
                                }
                                Ok(())
                            }
                            Err(e) => {
                                // Deliver what was already produced (header +
                                // pre-failure rows) before failing — per-row
                                // delivery did this implicitly, and the driver's
                                // currentVersion comes from the header even when
                                // the advance errors (matching TS, where the
                                // snapshotter has advanced before the failure).
                                sender.flush_acked();
                                Err(format!("advance failed: {}", e))
                            }
                        }
                    }
                    Err(payload) => {
                        // Scalar-subquery value change → transparent reset:
                        // stream a -2 reset row (reason 'scalar-subquery') and
                        // succeed, rather than throwing a teardown error. The
                        // buffered header/pre-reset rows are flushed first (the
                        // former per-row protocol had already delivered them).
                        if let Some(msg) = scalar_reset_message(&payload) {
                            sender.flush();
                            drain_barrier_chunk(
                                &tsfn,
                                vec![make_reset_row("scalar-subquery", &msg), end_sentinel()],
                            )?;
                            rust_ivm::perf_trace::report_residual("advance");
                            Ok(())
                        } else {
                            // Poison the engine: half-mutated graph must reset+
                            // rehydrate before the next advance (see AdvanceTask).
                            state.poisoned = true;
                            // Deliver the buffered header/pre-panic rows before
                            // surfacing the error (per-row parity; the header
                            // carries the advanced version — see the Err(e) arm).
                            sender.flush_acked();
                            let msg = panic_message(&payload);
                            eprintln!(
                                "[rust-ivm] advance streamed panicked — surfacing as thrown error: {msg}"
                            );
                            Err(format!("engine advance panic: {msg}"))
                        }
                    }
                }
            })?
            .map_err(NapiError::from_reason)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

fn row_change_to_napi(rc: &rust_ivm::streamer::RowChange) -> NapiRowChange {
    NapiRowChange {
        change_type: rc.change_type as i32,
        query_id: rc.query_id.clone(),
        table: rc.table.clone(),
        row_key: value_map_to_json_string(&rc.row_key),
        row: rc.row.as_ref().map(value_map_to_json_string),
        is_hidden: rc.is_hidden,
    }
}

/// Convert a Value map to a JSON object string for efficient cross-boundary
/// transfer. One JSON.parse on the JS side is ~10x faster than creating N
/// NapiValue wrapper objects (each with 5 Option fields = 5 V8 properties).
fn value_map_to_json_string(map: &rust_ivm::ivm::data::Row) -> String {
    let mut obj = serde_json::Map::with_capacity(map.len());
    for (k, v) in map.iter() {
        obj.insert(k.to_string(), value_to_serde_json(v));
    }
    serde_json::to_string(&obj).unwrap_or_else(|_| "{}".to_string())
}

fn value_to_serde_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        // Match `Value::Serialize` (data.rs) AND TS `JSON.stringify`: an
        // integer-valued f64 serializes as a JSON integer ("42", not "42.0");
        // a non-integer as a float; NaN/Infinity (unrepresentable in JSON) as
        // null, exactly as `JSON.stringify(NaN) === "null"`.
        Value::F64(n) => {
            if n.fract() == 0.0 && n.is_finite() && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                serde_json::Value::Number((*n as i64).into())
            } else if let Some(num) = serde_json::Number::from_f64(*n) {
                serde_json::Value::Number(num)
            } else {
                sqlite_real_to_json(*n)
            }
        }
        Value::Str(s) => serde_json::Value::String(s.to_string()),
        // Json is validated at ingest (`sqlite_value_to_ivm`), so `from_str`
        // always succeeds here; the fallback is dead defence.
        Value::Json(j) => {
            serde_json::from_str(j).unwrap_or(serde_json::Value::String(j.to_string()))
        }
    }
}

/// Async task for `destroy()`. Runs the teardown on the actor thread and
/// confirms via the reply channel, so the returned Promise resolves only
/// after all SQLite connections + engine graph are dropped.
///
/// Single-owner design: this is the ONLY teardown path. No TS connection
/// exists to race with.
pub struct DestroyTask {
    handle: EngineHandle,
}

impl Task for DestroyTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        self.handle.call(|state| {
            if let Some(ref mut eng) = state.engine {
                eng.destroy();
            }
            // Release every TableSource BEFORE the snapshotter drops: each
            // source's `db` field holds a clone of the snapshot connection Rc,
            // so with sources alive the Snapshot::drop close is deferred to
            // the last implicit drop — where a close failure is swallowed
            // (observed live: "151 outstanding conn holder(s) at drop" on
            // every CG close). Sources-first makes the snapshot the sole
            // holder, so its explicit close runs and failures are logged.
            state.engine = None;
            state.sources.clear();
            if let Some(ref mut snap) = state.snapshotter {
                snap.destroy();
            }
            *state = EngineState::default();
            // Signal the actor loop to exit after this job so its stack is freed
            // immediately (see EngineState::should_exit). Set AFTER the default
            // reset above (which would otherwise clear it).
            state.should_exit = true;
        })?;
        // Return the CG's just-freed heap to the OS. glibc retains freed arena
        // memory under reconnect churn (profiled: ~2.3GB/worker of [anon]+[heap]
        // held post-soak) — malloc_trim forces it back. Rate-limited (≤1/s) so
        // heavy churn doesn't pay the heap walk on every teardown.
        maybe_malloc_trim();
        Ok(())
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

#[cfg(test)]
mod reset_mapping_tests {
    use super::*;

    /// DECISION LOCK (2026-08-05): the ONLY advance panic that maps to a `-2`
    /// in-place reset is a scalar-subquery value change (TS's own
    /// `ResetPipelinesSignal('scalar-subquery')`). Everything else — source
    /// drift ("Add duplicate row" / "Remove missing row" / "Edit missing old
    /// row") AND the Take boundary asserts ("Bound should be set" /
    /// "…BoundNode must be found during fetch") — stays on the THROW → teardown
    /// path, matching TS EXACTLY (a `-2` reset re-hydrates anyway, so it renews
    /// the reader pin identically to a teardown — no WAL benefit — and diverging
    /// adds masking + loop-safety risk). This asserts a source-drift AND a Take
    /// payload each match the scalar mapper NOT (→ they can only reach the Err
    /// teardown arm). If a future change makes either recoverable, wire a new
    /// mapper and update this test deliberately — never silently.
    #[test]
    fn only_scalar_maps_to_reset_others_stay_on_teardown() {
        let drift: Box<dyn std::any::Any + Send> =
            Box::new(String::from("source drift: Remove missing row from users"));
        let take: Box<dyn std::any::Any + Send> = Box::new(String::from("Bound should be set"));

        assert!(
            scalar_reset_message(&drift).is_none(),
            "source drift must NOT be classified as a scalar-subquery reset",
        );
        assert!(
            scalar_reset_message(&take).is_none(),
            "a Take boundary assert must NOT be classified as a reset — TS tears down",
        );
        // Both still yield a readable message for the thrown teardown Err.
        assert!(
            panic_message(&drift).contains("source drift")
                && panic_message(&take).contains("Bound should be set"),
            "the thrown teardown error must carry the panic message",
        );
    }
}
