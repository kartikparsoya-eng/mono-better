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
    let mut reset_key = HashMap::new();
    reset_key.insert("reason".to_string(), NapiValue {
        kind: "str".into(), bool_val: None, f64_val: None,
        str_val: Some(reason.to_string()), json_val: None,
    });
    reset_key.insert("msg".to_string(), NapiValue {
        kind: "str".into(), bool_val: None, f64_val: None,
        str_val: Some(msg.to_string()), json_val: None,
    });
    NapiRowChange {
        change_type: -2,
        query_id: String::new(),
        table: String::new(),
        row_key: reset_key,
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
    pub row_key: HashMap<String, NapiValue>,
    pub row: Option<HashMap<String, NapiValue>>,
    /// True when this row belongs to a hidden EXISTS/NOT-EXISTS relationship.
    /// Production consumers ignore it (behaviour matches TS streamNodes); the
    /// differential test harness filters these out.
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
        #[serde(default)]
        flip: bool,
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

/// The engine state held directly on the JS thread.
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
    /// Parallel hydrate flag (DESIGN Phase 1, default on).
    /// When true (default), hydrate dispatches one task per pipeline to a
    /// bounded worker pool; when false, hydrate runs serially. Disabled via
    /// env `RUST_IVM_PARALLEL_HYDRATE=0` or the `set_parallel_hydrate` napi method.
    parallel_hydrate: bool,
    /// Worker pool size for parallel hydrate (default 2). Env
    /// `RUST_IVM_HYDRATE_LANES`.
    hydrate_lanes: usize,
    /// Bounded channel capacity per task for parallel hydrate (default 4). Env
    /// `RUST_IVM_HYDRATE_BOUND`.
    hydrate_bound: usize,
}

impl Default for EngineState {
    fn default() -> Self {
        let parallel_hydrate = std::env::var("RUST_IVM_PARALLEL_HYDRATE")
            .ok()
            .map(|v| {
                v != "0" && !v.eq_ignore_ascii_case("false") && !v.eq_ignore_ascii_case("off")
            })
            .unwrap_or(true);
        let hydrate_lanes = std::env::var("RUST_IVM_HYDRATE_LANES")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v: &usize| *v > 0)
            .unwrap_or(2);
        let hydrate_bound = std::env::var("RUST_IVM_HYDRATE_BOUND")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v: &usize| *v > 0)
            .unwrap_or(4);
        EngineState {
            engine: None,
            snapshotter: None,
            syncable_tables: HashMap::new(),
            all_table_names: HashSet::new(),
            sources: HashMap::new(),
            primary_keys: HashMap::new(),
            poisoned: false,
            parallel_hydrate,
            hydrate_lanes,
            hydrate_bound,
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

    /// Initialize the engine with table schemas and optional SQLite db_path.
    /// When db_path is provided, creates TableSource instances backed by SQLite.
    /// When no db_path, creates MemorySource instances (test/dev mode).
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
        *state = EngineState::default();
        // N1: clear the interrupt-handle registry on (re)init — the old
        // connections are gone (cleared by EngineState::default above). New
        // connections push fresh handles below.
        interrupt_handles.lock().unwrap().clear();

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

        let mut eng = Engine::new(primary_keys.clone(), 1);
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
            let mut snap = Snapshotter::new(path, &app_id, None);
            match snap.init() {
                Ok(()) => {
                    eprintln!("[rust-ivm] snapshotter initialized at version {}", snap.current_version().unwrap_or("?"));
                    // Point every TableSource at the snapshotter's CURR (pinned
                    // at head) connection, so all reads share the snapshot the
                    // engine advances over — instead of each source floating on
                    // its own head-latest connection (the source-drift cause).
                    if let Ok(curr) = snap.current_conn() {
                        for source in state.sources.values() {
                            source.borrow_mut().set_snapshot_db(curr.clone());
                        }
                    }
                    // N1: register the snapshot connection's interrupt handle
                    // with the EngineHandle so cancel()/watchdog can hard-abort
                    // a slow snapshot read mid-flight.
                    if let Some(h) = snap.take_current_interrupt_handle() {
                        interrupt_handles.lock().unwrap().push(h);
                    }
                    state.snapshotter = Some(snap);
                }
                Err(e) => {
                    eprintln!("[rust-ivm] snapshotter init failed (non-fatal): {}", e);
                }
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

    /// Enable/disable parallel hydrate (DESIGN Phase 1, behind a flag, default
    /// off). When enabled, hydrate dispatches one task per pipeline to a bounded
    /// worker pool. Falls back to serial on any failure (S4).
    #[napi]
    pub fn set_parallel_hydrate(&self, enabled: bool, lanes: Option<u32>) -> Result<()> {
        self.handle.call(move |state| {
            state.parallel_hydrate = enabled;
            if let Some(l) = lanes {
                state.hydrate_lanes = l as usize;
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
            state.poisoned = false;
        })
    }

    /// Explicit teardown on client-group drop (rust-ivm-driver.destroy()).
    /// Frees the engine graph, its SQLite reader connections, and the
    /// snapshotter's replica handles NOW rather than waiting for the JS object
    /// to be garbage-collected (which is when the actor thread finally exits).
    /// Matches the TS PipelineDriver.destroy() contract of prompt resource
    /// release. The actor thread itself is cheap (blocked on recv) and still
    /// exits on GC; this just avoids holding SQLite fds + IVM memory per dropped
    /// CG until the next GC cycle.
    #[napi]
    pub fn destroy(&self) -> Result<()> {
        // Fire-and-forget: never block the JS event loop. If the actor is
        // momentarily parked inside a streaming tsfn.call (which only drains
        // when the event loop runs), a blocking destroy would deadlock the loop
        // against itself. The teardown runs as soon as the actor is free; the
        // driver's finally already flips cancel() so the actor stops promptly.
        self.handle.call_detached(|state| {
            if let Some(ref mut eng) = state.engine {
                eng.destroy();
            }
            *state = EngineState::default();
        });
        Ok(())
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
                if state.parallel_hydrate {
                    let result = eng.parallel_add_queries_streaming(
                        &specs,
                        state.hydrate_lanes,
                        state.hydrate_bound,
                        |rc| rows.push(row_change_to_napi(rc)),
                    );
                    if let Err(_e) = result {
                        // Parallel failed — partial rows may have been emitted.
                        // Clear and fall back to serial (S4: one clean reset,
                        // no partial results committed).
                        rows.clear();
                        eng.add_queries_streaming(&specs, |rc| rows.push(row_change_to_napi(rc)));
                    }
                } else {
                    eng.add_queries_streaming(&specs, |rc| rows.push(row_change_to_napi(rc)));
                }
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
                            let mut header_key = HashMap::new();
                            header_key.insert("version".to_string(), NapiValue {
                                kind: "str".into(), bool_val: None, f64_val: None,
                                str_val: Some(version.to_string()), json_val: None,
                            });
                            header_key.insert("numChanges".to_string(), NapiValue {
                                kind: "f64".into(), bool_val: None, f64_val: Some(num_changes as f64),
                                str_val: None, json_val: None,
                            });
                            header_key.insert("aborted".to_string(), NapiValue {
                                kind: "bool".into(), bool_val: Some(false), f64_val: None,
                                str_val: None, json_val: None,
                            });
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
                                    let mut reset_key = HashMap::new();
                                    reset_key.insert("reason".to_string(), NapiValue {
                                        kind: "str".into(), bool_val: None, f64_val: None,
                                        str_val: Some(reason.clone()), json_val: None,
                                    });
                                    reset_key.insert("msg".to_string(), NapiValue {
                                        kind: "str".into(), bool_val: None, f64_val: None,
                                        str_val: Some(advance_result.reset_msg.clone().unwrap_or_default()),
                                        json_val: None,
                                    });
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
                if state.parallel_hydrate {
                    let result = eng.parallel_add_queries_streaming(
                        &specs,
                        state.hydrate_lanes,
                        state.hydrate_bound,
                        do_hydrate,
                    );
                    if let Err(_e) = result {
                        // Parallel failed — partial rows may have been streamed.
                        // Fall back to serial (the driver's row-set signatures
                        // reconcile any duplicates). The parallel flag is
                        // default-off; this path is rare.
                        eng.add_queries_streaming(&specs, do_hydrate);
                    }
                } else {
                    eng.add_queries_streaming(&specs, do_hydrate);
                }
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
                            let mut header_key = HashMap::new();
                            header_key.insert("version".to_string(), NapiValue {
                                kind: "str".into(), bool_val: None, f64_val: None,
                                str_val: Some(version.to_string()), json_val: None,
                            });
                            header_key.insert("numChanges".to_string(), NapiValue {
                                kind: "f64".into(), bool_val: None, f64_val: Some(num_changes as f64),
                                str_val: None, json_val: None,
                            });
                            header_key.insert("aborted".to_string(), NapiValue {
                                kind: "bool".into(), bool_val: Some(false), f64_val: None,
                                str_val: None, json_val: None,
                            });
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
                                    let mut reset_key = HashMap::new();
                                    reset_key.insert("reason".to_string(), NapiValue {
                                        kind: "str".into(), bool_val: None, f64_val: None,
                                        str_val: Some(reason.clone()), json_val: None,
                                    });
                                    reset_key.insert("msg".to_string(), NapiValue {
                                        kind: "str".into(), bool_val: None, f64_val: None,
                                        str_val: Some(advance_result.reset_msg.clone().unwrap_or_default()),
                                        json_val: None,
                                    });
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
        row_key: rc.row_key.iter().map(|(k, v)| (k.to_string(), value_to_napi(v))).collect(),
        row: rc.row.as_ref().map(|r| {
            r.iter().map(|(k, v)| (k.to_string(), value_to_napi(v))).collect()
        }),
        is_hidden: rc.is_hidden,
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
