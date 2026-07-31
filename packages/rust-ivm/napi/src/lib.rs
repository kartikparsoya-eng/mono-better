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
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};

use napi::bindgen_prelude::AsyncTask;
use napi::{bindgen_prelude::*, Env, Error as NapiError, Status, Task, JsFunction};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode, ErrorStrategy};
use napi_derive::napi;
use rust_ivm::engine::{CancellationToken, Engine, QuerySpec, ScalarResetError};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::source::{MemorySource, Source};
use rust_ivm::snapshotter::spec::{ColumnSchema, LiteAndZqlSpec, TableSpec};
use rust_ivm::snapshotter::Snapshotter;
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::sqlite::{install_interrupt, JobWatchdog};

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
/// Number of connections the frame-pinned read pool co-pins for parallel cold
/// hydrate (read-level parallelism). `0` disables it (serial hydrate). Env
/// `RUST_IVM_READ_LANES` (default 0 — ships dark until the fuzzer + microbench
/// gate in DESIGN-read-parallelism.md is cleared).
fn read_pool_lanes() -> usize {
    std::env::var("RUST_IVM_READ_LANES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
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
    (std::time::Duration::from_millis(warn), std::time::Duration::from_millis(abort))
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
    /// Persistent interrupt handles for every SQLite connection the actor owns
    /// (the actor's own connection today; every pooled worker connection in
    /// Phase 1+). `cancel()` calls `.interrupt()` on each to abort a query
    /// running on that connection in-flight — closing the wedge where cancel
    /// is only checked *between* rows (N1). The handles are `Send + Sync`, so
    /// this registry is the cross-thread hard-abort path; the actor thread is
    /// the only connection *opener*, so only it writes here.
    interrupt_handles: Arc<Mutex<Vec<rusqlite::InterruptHandle>>>,
    /// Single monitor thread + deadline registry (N2). One per actor; the same
    /// monitor supervises serial jobs (one handle) today and parallel jobs
    /// (N handles) in Phase 1+. `call` registers each job with a deadline.
    watchdog: Arc<JobWatchdog>,
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
            interrupt_handles: Arc::new(Mutex::new(Vec::new())),
            watchdog: Arc::new(JobWatchdog::new()),
        }
    }

    /// Run `f` on the actor thread and block until it returns. Used by the
    /// lightweight synchronous methods; the two heavy methods use `AsyncTask`
    /// so they do not block the JS event loop.
    ///
    /// N2: registers the job with the watchdog for the duration of the call.
    /// On the soft deadline the monitor flips the cancel token AND `.interrupt()`s
    /// the actor's persistent SQLite handles — so a runaway query that the
    /// between-rows cancel check never reaches (the current wedge, N1) is
    /// hard-aborted mid-flight. Past the hard bound the monitor logs a
    /// stuck-actor signal. The guard unregisters on return (even on panic).
    fn call<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut EngineState) -> T + Send + 'static,
    {
        let (rtx, rrx) = channel::<std::thread::Result<T>>();
        // Arm the watchdog for this job. The handles are the actor's persistent
        // SQLite connections, shared via Arc<Mutex<_>> (InterruptHandle is not
        // Clone). A job with no live connection yet (e.g. init) still benefits
        // from the cancel-token flip on deadline.
        let cancel = self
            .cancel_slot
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(CancellationToken::new);
        let handles = self.interrupt_handles.clone();
        let (warn, abort) = watchdog_bounds();
        let now = std::time::Instant::now();
        let _guard = self.watchdog.register(
            now + warn,
            now + abort,
            cancel.clone(),
            handles,
        );
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

    /// Queue `f` on the actor thread and return IMMEDIATELY without waiting for
    /// it to run. Used by `destroy()`: a blocking `call` would park the JS event
    /// loop on `recv()` until the actor is free, and if the actor is momentarily
    /// parked inside a streaming `tsfn.call` (waiting on the same event loop to
    /// drain the TSFN), that is a deadlock. Fire-and-forget can never block the
    /// loop; the teardown job runs as soon as the actor finishes its current op.
    fn call_detached<F>(&self, f: F)
    where
        F: FnOnce(&mut EngineState) + Send + 'static,
    {
        // Contain panics so a detached job can't unwind out of the actor thread.
        let _ = self.tx.send(Job(Box::new(move |s| {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(s)));
        })));
    }
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
    }).to_string();
    NapiRowChange {
        change_type: -2,
        query_id: String::new(),
        table: String::new(),
        row_key,
        row: None,
        is_hidden: false,
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

/// A synchronous iterator that JS drains in a loop, exactly like TS drains
/// a generator. The iterator holds a boxed closure that produces the next
/// NapiRowChange (or None when done).
#[napi]
pub struct NapiStreamIterator {
    next_fn: RefCell<Box<dyn FnMut() -> Option<NapiRowChange>>>,
    cancelled: std::sync::atomic::AtomicBool,
}

#[napi]
impl NapiStreamIterator {
    /// Synchronous next — returns the next row, or null when done.
    #[napi]
    pub fn next(&self) -> Option<NapiRowChange> {
        if self.cancelled.load(std::sync::atomic::Ordering::Relaxed) {
            return None;
        }
        (self.next_fn.borrow_mut())()
    }

    /// Cancel the iterator — subsequent next() calls return None.
    #[napi]
    pub fn cancel(&self) {
        self.cancelled.store(true, std::sync::atomic::Ordering::Release);
    }
}

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
    Column { name: String },
    Literal { value: serde_json::Value },
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
            parts.into_iter().map(|(col, dir)| {
                rust_ivm::builder::ast::OrderPart { column: col, direction: dir }
            }).collect()
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
        TsCondition::Simple { op, left, right } => {
            Condition::Simple(SimpleCondition {
                op,
                left: convert_value_position(left),
                right: convert_value_position(right),
            })
        }
        TsCondition::And { conditions } => {
            Condition::And(conditions.into_iter().map(convert_condition).collect())
        }
        TsCondition::Or { conditions } => {
            Condition::Or(conditions.into_iter().map(convert_condition).collect())
        }
        TsCondition::CorrelatedSubquery { related, op, flip, scalar } => {
            Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
                related: convert_csq(&related),
                op,
                flip,
                scalar,
                plan_id: None,
            })
        }
    }
}

fn convert_value_position(vp: TsValuePosition) -> rust_ivm::builder::ast::ValuePosition {
    use rust_ivm::builder::ast::ValuePosition;
    match vp {
        TsValuePosition::Column { name } => ValuePosition::Column { name },
        TsValuePosition::Literal { value } => {
            ValuePosition::Literal { value: json_to_value(value) }
        }
        TsValuePosition::Static { anchor, field } => {
            let _ = (anchor, field);
            ValuePosition::Literal { value: rust_ivm::ivm::data::Value::Null }
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
                if i > 9_007_199_254_740_991 || i < -9_007_199_254_740_991 {
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
    let v = row
        .get::<_, rusqlite::types::Value>(i)
        .map_err(|e| format!("col {}: {}", i, e))?;
    Ok(match v {
        rusqlite::types::Value::Null => serde_json::Value::Null,
        rusqlite::types::Value::Integer(i) => serde_json::Value::Number(i.into()),
        rusqlite::types::Value::Real(f) => serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap_or_else(|| 0.into())),
        rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
        rusqlite::types::Value::Blob(b) => serde_json::Value::String(String::from_utf8_lossy(&b).into_owned()),
    })
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
    pub fn init_snapshotter(
        &self,
        db_path: String,
        app_id: String,
    ) -> Result<()> {
        let reg = self.handle.interrupt_handles.clone();
        self.handle.call(move |state| -> std::result::Result<(), String> {
            if state.snapshotter.is_some() {
                return Ok(()); // already initialized
            }
            let mut snap = Snapshotter::with_read_pool(&db_path, &app_id, None, read_pool_lanes(), Some(reg));
            snap.init().map_err(|e| format!("snapshotter init: {}", e))?;
            eprintln!("[rust-ivm] snapshotter pre-initialized at version {}", snap.current_version().unwrap_or("?"));
            state.snapshotter = Some(snap);
            Ok(())
        })?.map_err(NapiError::from_reason)
    }

    /// Initialize the engine with table schemas and optional SQLite db_path.
    /// When db_path is provided, creates TableSource instances backed by SQLite.
    /// When no db_path, creates MemorySource instances (test/dev mode).
    /// If `init_snapshotter` was called first, the existing snapshotter is
    /// reused (its interrupt handle is still registered below).
    #[napi]
    pub fn init(
        &self,
        tables: Vec<NapiTableSpec>,
        db_path: Option<String>,
        app_id: String,
    ) -> Result<()> {
        let cancel_slot = self.handle.cancel_slot.clone();
        let interrupt_handles = self.handle.interrupt_handles.clone();
        self.handle.call(move |state| -> std::result::Result<(), String> {
        // Clear any previous state.
        if let Some(ref mut eng) = state.engine {
            eng.destroy();
        }
        // Preserve the snapshotter if init_snapshotter was called first.
        let preserved_snap = state.snapshotter.take();
        *state = EngineState::default();
        state.snapshotter = preserved_snap;
        // N1: clear the interrupt-handle registry on (re)init — the old
        // connections are gone (cleared by EngineState::default above). New
        // connections push fresh handles below.
        // But preserve the snapshotter's interrupt handle if it was pre-initialized.
        let preserved_handles: Vec<_> = interrupt_handles.lock().unwrap().drain(..).collect();
        // Re-add handles that belong to the preserved snapshotter (if any).
        // The snapshotter's handle was already taken by init_snapshotter, so
        // we just clear here; init() will re-register it below.
        let _ = preserved_handles;

        let mut primary_keys = HashMap::new();

        for spec in &tables {
            let mut columns = HashMap::new();
            for (col, schema) in &spec.columns {
                let col_type = match schema.r#type.as_str() {
                    "boolean" => rust_ivm::ivm::schema::ColumnType::Boolean { optional: schema.optional },
                    "number" => rust_ivm::ivm::schema::ColumnType::Number { optional: schema.optional },
                    "json" => rust_ivm::ivm::schema::ColumnType::Json { optional: schema.optional },
                    _ => rust_ivm::ivm::schema::ColumnType::String { optional: schema.optional },
                };
                columns.insert(col.clone(), col_type);
            }

            let rc_source: std::rc::Rc<RefCell<dyn Source>> = if db_path.is_some() {
                let path = db_path.as_ref().unwrap();
                let conn = rusqlite::Connection::open_with_flags(
                    path,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                        | rusqlite::OpenFlags::SQLITE_OPEN_URI,
                ).map_err(|e| format!("Failed to open SQLite for {}: {}", spec.table, e))?;
                let _ = conn.busy_timeout(std::time::Duration::from_millis(5000));
                let _ = conn.execute_batch("PRAGMA case_sensitive_like = ON; PRAGMA query_only = ON;");
                // N1: install a cross-thread interrupt handle on this connection
                // and register it so cancel()/watchdog can hard-abort a runaway
                // query in-flight (closing the between-rows-only cancel wedge).
                // The handle is Send+Sync; the actor is the only opener so only
                // it writes the registry, but any thread (cancel/watchdog) reads.
                let handle = install_interrupt(&conn);
                interrupt_handles.lock().unwrap().push(handle);
                let table_source = TableSource::new(
                    std::rc::Rc::new(RefCell::new(conn)),
                    &spec.table,
                    columns,
                    spec.primary_key.clone(),
                );
                std::rc::Rc::new(RefCell::new(table_source))
            } else {
                let source = MemorySource::new(&spec.table, columns, spec.primary_key.clone());
                std::rc::Rc::new(RefCell::new(source))
            };
            state.sources.insert(spec.table.clone(), rc_source);
            primary_keys.insert(spec.table.clone(), spec.primary_key.clone());

            // Build syncable table spec for snapshotter diff.
            let table_spec = TableSpec {
                name: spec.table.clone(),
                columns: spec.columns.iter().map(|(k, v)| {
                    (k.clone(), ColumnSchema {
                        r#type: v.r#type.clone(),
                        optional: v.optional,
                    })
                }).collect(),
                unique_keys: spec.unique_keys.clone().unwrap_or_else(|| vec![spec.primary_key.clone()]),
                min_row_version: spec.min_row_version.clone(),
            };
            let zql_spec: HashMap<String, ColumnSchema> = spec.columns.iter().map(|(k, v)| {
                (k.clone(), ColumnSchema {
                    r#type: v.r#type.clone(),
                    optional: v.optional,
                })
            }).collect();
            state.syncable_tables.insert(spec.table.clone(), LiteAndZqlSpec { table_spec, zql_spec });
            state.all_table_names.insert(spec.table.clone());
        }

        let mut eng = Engine::new(primary_keys.clone());
        for (_, source) in &state.sources {
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
            if state.snapshotter.is_none() {
                let mut snap = Snapshotter::with_read_pool(path, &app_id, None, read_pool_lanes(), Some(interrupt_handles.clone()));
                match snap.init() {
                    Ok(()) => {
                        eprintln!("[rust-ivm] snapshotter initialized at version {}", snap.current_version().unwrap_or("?"));
                        state.snapshotter = Some(snap);
                    }
                    Err(e) => {
                        eprintln!("[rust-ivm] snapshotter init failed (non-fatal): {}", e);
                    }
                }
            }
            // Point every TableSource at the snapshotter's CURR (pinned
            // at head) connection, so all reads share the snapshot the
            // engine advances over.
            if let Some(ref snap) = state.snapshotter {
                if let Ok(curr) = snap.current_conn() {
                    let pool = snap.read_pool();
                    for source in state.sources.values() {
                        let mut src = source.borrow_mut();
                        src.set_snapshot_db(curr.clone());
                        // Read-level parallelism: the source fans its leaf child
                        // reads out across this frame-pinned pool during cold
                        // hydrate (co-pinned at curr's frame). Serial fallback
                        // whenever the pool isn't pinned at the read frame.
                        src.set_read_pool(pool.clone());
                    }
                }
                // The snapshot connection's interrupt handle is deliberately NOT
                // registered for cancel/watchdog. The pin-race fix replaces the
                // snapshot connection on every advance, so a once-registered
                // handle goes stale (a harmless no-op after the old conn closes).
                // It isn't needed anyway: snapshot diff reads run BEGIN CONCURRENT
                // on the local wal2 replica — they return SQLITE_BUSY/error rather
                // than blocking, and advance is already cancelled BETWEEN changes
                // via the cancellation_token + economic abort. Only the source
                // FETCH connections (long correlated-EXISTS scans) are registered
                // above — those genuinely need mid-query hard-abort.
            }
            eprintln!("[rust-ivm] sources initialized (db_path={})", path);
        }

        Ok(())
        })?.map_err(NapiError::from_reason)
    }

    /// Add queries and hydrate them. **Async**: the hydration runs on this
    /// engine's actor thread (off the JS event loop), so hydrations for
    /// different client groups execute in parallel. Resolves to the full row
    /// list (the previous pull-iterator walked an eagerly-built Vec anyway).
    #[napi(ts_return_type = "Promise<NapiRowChange[]>")]
    pub fn add_queries_streaming(&self, queries: Vec<NapiQuerySpec>) -> AsyncTask<HydrateTask> {
        AsyncTask::new(HydrateTask { handle: self.handle.clone(), queries })
    }

    /// Advance to head: Rust derives its own diff from the snapshotter,
    /// pushes through pipelines, streams RowChanges.
    /// The first row from the iterator is a header (changeType=-1) with
    /// version, numChanges, aborted in the row_key field.
    /// Advance to head. **Async**: runs on this engine's actor thread (off the
    /// JS event loop) so advances for different client groups run in parallel.
    /// Resolves to `[header, ...rows]` (header changeType=-1; -2 = reset row).
    #[napi(ts_return_type = "Promise<NapiRowChange[]>")]
    pub fn advance_to_head_streaming(&self) -> AsyncTask<AdvanceTask> {
        AsyncTask::new(AdvanceTask { handle: self.handle.clone() })
    }

    /// Add queries and hydrate them, streaming rows one at a time via `on_row`.
    /// Each RowChange is handed to JS the instant it is produced, with
    /// backpressure via a bounded TSFN (max_queue_size=1, Blocking mode).
    /// The actor thread parks when the queue is full, so at most 1 row is
    /// in flight at any time — O(1) JS objects vs O(result) for the eager path.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn add_queries_streaming_rows(
        &self,
        env: Env,
        queries: Vec<NapiQuerySpec>,
        #[napi(ts_arg_type = "(row: NapiRowChange) => void")]
        on_row: JsFunction,
    ) -> Result<AsyncTask<HydrateStreamingTask>> {
        let tsfn = env.create_threadsafe_function(
            &on_row,
            1, // max_queue_size=1: real backpressure — actor parks when full
            |ctx| Ok(vec![ctx.value]),
        )?;
        Ok(AsyncTask::new(HydrateStreamingTask {
            handle: self.handle.clone(),
            queries,
            tsfn,
        }))
    }

    /// Advance to head, streaming rows one at a time via `on_row`.
    /// Header (changeType=-1) is emitted first, change rows in the middle,
    /// reset row (changeType=-2) last if the engine reported a reset_reason.
    /// Bounded TSFN (max_queue_size=1, Blocking mode) for real backpressure.
    #[napi(ts_return_type = "Promise<void>")]
    pub fn advance_to_head_streaming_rows(
        &self,
        env: Env,
        #[napi(ts_arg_type = "(row: NapiRowChange) => void")]
        on_row: JsFunction,
    ) -> Result<AsyncTask<AdvanceStreamingTask>> {
        let tsfn = env.create_threadsafe_function(
            &on_row,
            1, // max_queue_size=1: real backpressure
            |ctx| Ok(vec![ctx.value]),
        )?;
        Ok(AsyncTask::new(AdvanceStreamingTask {
            handle: self.handle.clone(),
            tsfn,
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
        // Hard-abort any in-flight SQLite query on every actor connection.
        let handles = self.handle.interrupt_handles.lock().unwrap();
        for h in handles.iter() {
            h.interrupt();
        }
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
            state.poisoned = false;
        })
    }

    /// Query planner (`#planAstForRust`): plan `ast_json` (TS-shape) with a cost
    /// model backed by the pinned snapshot connection, and return the ordered
    /// `flip` decisions as a JSON array (`true`/`false`/`null`). The TS driver
    /// walks its own AST in the same order (WHERE pre-order then `related`) and
    /// sets `flip` per position. Reaching parity with zero 1.7's default-on
    /// planner (which is disabled on the Rust single-owner path); ships behind
    /// the driver's `enablePlanner` flag. Returns `[]` if no snapshot yet.
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
                // Reuse row counts across the connection-init addQuery burst
                // (same snapshot version); COUNT(*) runs at most once per
                // (table, version) instead of once per table per addQuery.
                let model = rust_ivm::planner::create_snapshot_cost_model_cached(
                    conn,
                    &version,
                    state.plan_count_cache.clone(),
                );
                let flips = rust_ivm::planner::plan_ast_flips(&ast_value, model);
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
        self.handle.call(move |state| -> std::result::Result<String, String> {
            let snap = state
                .snapshotter
                .as_ref()
                .ok_or_else(|| "Snapshotter not initialized".to_string())?;
            let conn = snap
                .current_conn()
                .map_err(|e| format!("No current snapshot: {}", e))?;
            let conn = conn.borrow();
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| format!("prepare: {}", e))?;
            let cols: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();

            // Parse bind params from JSON array string.
            let bind_values: Vec<rusqlite::types::Value> = if let Some(ref p) = params {
                let arr: serde_json::Value = serde_json::from_str(p)
                    .map_err(|e| format!("params parse: {}", e))?;
                match arr {
                    serde_json::Value::Array(a) => {
                        a.iter().map(json_to_rusqlite).collect::<std::result::Result<Vec<_>, _>>()?
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
            while let Some(row) = raw_rows
                .next()
                .map_err(|e| format!("row: {}", e))?
            {
                let mut obj = serde_json::Map::with_capacity(cols.len());
                for (i, name) in cols.iter().enumerate() {
                    let v = row_to_json(row, i)?;
                    obj.insert(name.clone(), v);
                }
                rows.push(serde_json::Value::Object(obj));
            }
            Ok(serde_json::to_string(&rows).unwrap_or_else(|_| "[]".to_string()))
        })?.map_err(NapiError::from_reason)
    }

    /// Advance the Rust snapshotter to head WITHOUT computing a diff
    /// (mirrors TS Snapshotter.advanceWithoutDiff()). Returns the new version.
    ///
    /// Used by the view-syncer's permission-invalidations path and by the
    /// CVR invalidation check. No engine work is triggered.
    #[napi]
    pub fn advance_without_diff(&self) -> Result<String> {
        self.handle.call(|state| -> std::result::Result<String, String> {
            let snap = state
                .snapshotter
                .as_mut()
                .ok_or_else(|| "Snapshotter not initialized".to_string())?;
            snap.advance_without_diff()
                .map_err(|e| format!("advance_without_diff: {}", e))?;
            // Re-point every TableSource at the new curr connection.
            // advance_without_diff swaps prev/curr, so sources that were
            // pointing at the old curr (now prev) would read stale data.
            let curr = snap.current_conn().map_err(|e| format!("advance_without_diff: {}", e))?;
            for source in state.sources.values() {
                source.borrow_mut().set_snapshot_db(curr.clone());
            }
            Ok(snap.current_version().unwrap_or_default().to_string())
        })?.map_err(NapiError::from_reason)
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
        self.handle.call(move |state| -> std::result::Result<String, String> {
            let snap = state
                .snapshotter
                .as_ref()
                .ok_or_else(|| "Snapshotter not initialized".to_string())?;
            let conn = snap
                .current_conn()
                .map_err(|e| format!("No current snapshot: {}", e))?;
            let conn = conn.borrow();
            let (replica_version, watermark) = conn.query_row(&sql, [], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            }).map_err(|e| format!("subscription state: {}", e))?;
            let obj = serde_json::json!({
                "replicaVersion": replica_version,
                "watermark": watermark,
            });
            Ok(obj.to_string())
        })?.map_err(NapiError::from_reason)
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
            .call(move |state| -> std::result::Result<Vec<NapiRowChange>, String> {
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
                    specs.push(QuerySpec { query_id: q.query_id.clone(), ast });
                }
                let mut rows: Vec<NapiRowChange> = Vec::new();
                // Single-fetch hydrate: one fetch per pipeline warms operator
                // state AND emits output in the same pass, on the actor's pinned
                // snapshot connection. Read-level parallelism lives below the
                // source; the actor graph stays single-writer.
                eng.add_queries_streaming(&specs, |rc| rows.push(row_change_to_napi(rc)));
                Ok(rows)
            })?
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
                            // An engine PANIC (e.g. a source-drift assert: "Add
                            // duplicate row"/"Remove missing row"). catch_unwind here
                            // keeps it from crossing the napi FFI and SIGABRT-ing the
                            // process, and restores engine+snapshotter above. We then
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

/// Hydration streaming task. Streams rows via TSFN instead of materializing.
pub struct HydrateStreamingTask {
    handle: EngineHandle,
    queries: Vec<NapiQuerySpec>,
    tsfn: ThreadsafeFunction<NapiRowChange>,
}

impl Task for HydrateStreamingTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let queries = std::mem::take(&mut self.queries);
        let tsfn = self.tsfn.clone();
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
                    specs.push(QuerySpec { query_id: q.query_id.clone(), ast });
                }
                let cancel = eng.cancellation_token();
                let do_hydrate = |rc: &rust_ivm::streamer::RowChange| {
                    let napi_rc = row_change_to_napi(rc);
                    if tsfn.call(Ok(napi_rc), ThreadsafeFunctionCallMode::Blocking) != Status::Ok {
                        cancel.cancel();
                    }
                };
                // Single-fetch hydrate, streaming row-by-row to JS via the TSFN
                // (blocking backpressure). One fetch per pipeline warms operator
                // state AND emits output; read-level parallelism lives below the
                // source, keeping the actor graph single-writer.
                eng.add_queries_streaming(&specs, do_hydrate);
                Ok(())
            })?
            .map_err(NapiError::from_reason)
    }

    fn resolve(&mut self, _env: Env, _output: Self::Output) -> Result<Self::JsValue> {
        Ok(())
    }
}

/// Advance streaming task. Streams rows via TSFN instead of materializing.
pub struct AdvanceStreamingTask {
    handle: EngineHandle,
    tsfn: ThreadsafeFunction<NapiRowChange>,
}

impl Task for AdvanceStreamingTask {
    type Output = ();
    type JsValue = ();

    fn compute(&mut self) -> Result<Self::Output> {
        let tsfn = self.tsfn.clone();
        self.handle
            .call(move |state| -> std::result::Result<(), String> {
                // A prior panic left the engine possibly half-mutated. Refuse to
                // advance on it; stream a reset so the driver rehydrates first.
                if state.poisoned {
                    state.poisoned = false;
                    let _ = tsfn.call(
                        Ok(make_reset_row(
                            "schema-change",
                            "engine reset after a prior advance panic; rehydrating",
                        )),
                        ThreadsafeFunctionCallMode::Blocking,
                    );
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

                let advance = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
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
                            let _ = tsfn.call(Ok(NapiRowChange {
                                change_type: -1,
                                query_id: String::new(),
                                table: String::new(),
                                row_key: header_key,
                                row: None,
                                is_hidden: false,
                            }), ThreadsafeFunctionCallMode::Blocking);
                        },
                        |rc| {
                            let napi_rc = row_change_to_napi(rc);
                            if tsfn.call(Ok(napi_rc), ThreadsafeFunctionCallMode::Blocking) != Status::Ok {
                                cancel.cancel();
                            }
                        },
                    );
                    result
                }));

                state.engine = Some(eng);
                state.snapshotter = Some(snapshotter);

                match advance {
                    Ok(result) => {
                        match result {
                            Ok(advance_result) => {
                                if let Some(reason) = &advance_result.reset_reason {
                                    let reset_key = serde_json::json!({
                                        "reason": reason,
                                        "msg": advance_result.reset_msg.clone().unwrap_or_default(),
                                    }).to_string();
                                    let _ = tsfn.call(Ok(NapiRowChange {
                                        change_type: -2,
                                        query_id: String::new(),
                                        table: String::new(),
                                        row_key: reset_key,
                                        row: None,
                                        is_hidden: false,
                                    }), ThreadsafeFunctionCallMode::Blocking);
                                }
                                Ok(())
                            }
                            Err(e) => Err(format!("advance failed: {}", e)),
                        }
                    }
                    Err(payload) => {
                        // Scalar-subquery value change → transparent reset:
                        // stream a -2 reset row (reason 'scalar-subquery') and
                        // succeed, rather than throwing a teardown error.
                        if let Some(msg) = scalar_reset_message(&payload) {
                            let _ = tsfn.call(
                                Ok(make_reset_row("scalar-subquery", &msg)),
                                ThreadsafeFunctionCallMode::Blocking,
                            );
                            Ok(())
                        } else {
                            // Poison the engine: half-mutated graph must reset+
                            // rehydrate before the next advance (see AdvanceTask).
                            state.poisoned = true;
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
        row: rc.row.as_ref().map(|r| value_map_to_json_string(r)),
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
        Value::F64(n) => serde_json::Value::Number(serde_json::Number::from_f64(*n).unwrap_or_else(|| 0.into())),
        Value::Str(s) => serde_json::Value::String(s.to_string()),
        Value::Json(j) => serde_json::from_str(j).unwrap_or(serde_json::Value::String(j.to_string())),
    }
}

fn value_to_napi(v: &Value) -> NapiValue {
    match v {
        Value::Null => NapiValue { kind: "null".into(), bool_val: None, f64_val: None, str_val: None, json_val: None },
        Value::Bool(b) => NapiValue { kind: "bool".into(), bool_val: Some(*b), f64_val: None, str_val: None, json_val: None },
        Value::F64(n) => NapiValue { kind: "f64".into(), bool_val: None, f64_val: Some(*n), str_val: None, json_val: None },
        Value::Str(s) => NapiValue { kind: "str".into(), bool_val: None, f64_val: None, str_val: Some(s.to_string()), json_val: None },
        Value::Json(j) => NapiValue { kind: "json".into(), bool_val: None, f64_val: None, str_val: None, json_val: Some(j.to_string()) },
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
