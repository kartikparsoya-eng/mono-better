//! IvmPipelines — the pure-Rust IVM bridge (Stage A of the Phase 7 wiring).
//!
//! Port of the engine-side of `pipeline-driver.ts` (behavior) and of the
//! `EngineState` construction/hydrate/advance logic the removed native bridge
//! carried (a5e502ad9), with its thread-hop machinery stripped out. This struct is owned by the ViewSyncer and lives on
//! its dedicated CG thread — it is intentionally NOT `Send`/`Sync` because the
//! `rust-ivm` `Engine` holds `Rc<RefCell<..>>` sources.
//!
//! Responsibilities (rust-ivm only):
//!   - open the SQLite replica via a `Snapshotter`
//!   - build `TableSource`s and hydrate query ASTs (streaming `RowChange`s)
//!   - advance the replica to head, streaming `RowChange`s (with reset/panic
//!     handling that matches the TS lifecycle — pipeline-driver.ts:794-812)
//!   - `get_row` for catchup, `row_set_signature` passthrough
//!
//! The CVR combination (feeding these `RowChange`s into `rust-cvr`'s
//! `ChangeProcessor` / `CVRQueryDrivenUpdater` / pokers) is Stage B and lives in
//! the ViewSyncer, exactly as `view-syncer.ts` owns it in TS.
//!
//! See `packages/zero-cache/docs/rust-cvr-port/90-phase7-real-wiring-plan.md`.

use crate::workers::connection::JsError;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use rust_ivm::builder::debug_delegate::{RowCountsBySource, SharedDebug, runtime_debug_flags};
use rust_ivm::engine::{AdvanceStream, HydrateStream};
use rust_ivm::engine::{Engine, QuerySpec, ScalarResetError};
use rust_ivm::ivm::change::ChangeType;
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::Source;
use rust_ivm::ivm::stream::StreamItem;
use rust_ivm::snapshotter::spec::{ColumnSchema, LiteAndZqlSpec, TableSpec};
use rust_ivm::snapshotter::{SharedConn, Snapshotter};
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

// ─── Input specs (the TS `LiteTableSpec` / column shape, zero-cache db/specs.ts) ─

/// Column schema for a syncable table. `type` is one of
/// `"string" | "number" | "boolean" | "json"` (anything else is treated as
/// `string`, matching the TS mapping).
#[derive(Clone, Debug)]
pub struct IvmColumnSchema {
    pub r#type: String,
    pub optional: bool,
}

/// Port of TS `PipelineDriver#initAndResetCommon(clientSchema)`
/// (pipeline-driver.ts:354-372) minus the engine (re)build that follows it in
/// rust (`IvmPipelines::init`): recompute the replica's `tableSpecs` +
/// `fullTables` from the CURRENT replica schema, then `checkClientSchema`. The
/// state TS keeps on the driver (`#tableSpecs`, `#shardID`) is passed in
/// because rust keeps it on the service (`tables` / `full_tables`). The init
/// half is CG creation (`server/syncer.rs` computes the specs) + the
/// initConnection check; the reset half is `reset_pipelines_and_rehydrate`,
/// which fails the group with the returned body (TS: the thrown ProtocolError
/// fails the view syncer).
// The Err IS the wire body TS throws (ProtocolError.errorBody).
#[allow(clippy::result_large_err)]
pub fn init_and_reset_common(
    replica_path: Option<&str>,
    shard: &rust_cvr::shards::ShardID,
    client_schema: &serde_json::Value,
    tables: &mut Vec<IvmTableSpec>,
    full_tables: &mut Vec<crate::db::specs::LiteTableSpec>,
) -> Result<(), crate::protocol::ErrorBody> {
    // Schema-change resets must re-read the replica schema. Reusing the specs
    // captured at CG creation would rebuild the engine with the same stale
    // table/column set and either reset-loop or serve an obsolete schema.
    if let Some(path) = replica_path {
        // pipeline-driver.ts:357-363 `computeZqlSpecs(lc, db,
        // {includeBackfillingColumns: false}, tableSpecs, fullTables)`.
        match crate::db::lite_tables::open_replica_read_only(path).and_then(|conn| {
            crate::db::lite_tables::compute_zql_specs(
                &conn,
                &crate::db::lite_tables::ZqlSpecOptions {
                    include_backfilling_columns: false,
                },
                Some(full_tables),
            )
        }) {
            Ok(specs) => *tables = specs,
            Err(e) => {
                tracing::error!("schema reload after reset failed: {e}");
                return Err(crate::protocol::ErrorBody::internal(e));
            }
        }
    }
    // pipeline-driver.ts:364-369.
    crate::services::view_syncer::client_schema::check_client_schema(
        shard,
        client_schema,
        tables,
        full_tables,
    )
}

/// Table spec used to build a `TableSource` and the snapshotter diff spec.
#[derive(Clone, Debug)]
pub struct IvmTableSpec {
    pub table: String,
    pub columns: HashMap<String, IvmColumnSchema>,
    /// Column names in DECLARED (`pragma_table_info`) order — the order TS emits
    /// the SELECT column list in (`Object.keys(columns)`), which is
    /// client-observable in an analyzeQuery result. Empty ⇒ fall back to the
    /// (unordered) `columns` keys.
    pub column_order: Vec<String>,
    pub primary_key: Vec<String>,
    /// PK plus any unique indexes; drives scalar-subquery resolution. Defaults
    /// to `[primary_key]` when `None`.
    pub unique_keys: Option<Vec<Vec<String>>>,
    /// TS `tableSpec.allPotentialPrimaryKeys` (lite-tables.ts:293): every unique
    /// key over non-null synced columns, shortest first — the set a client
    /// `primaryKey` must match (`checkClientSchema`).
    pub all_potential_primary_keys: Vec<Vec<String>>,
    pub min_row_version: Option<String>,
}

/// Result of an `advance()` call.
#[derive(Debug, Clone)]
pub enum AdvanceOutcome {
    /// Advanced cleanly to `version` after streaming `num_changes` row changes.
    Advanced { version: String, num_changes: usize },
    /// The engine requested an in-place reset (rehydrate at head) — mirrors TS
    /// `ResetPipelinesSignal`. Reasons: `"scalar-subquery"`, `"schema-change"`,
    /// or whatever the engine's `advance_to_head_stream` reports.
    Reset { reason: String, msg: String },
}

// ─── IvmPipelines ────────────────────────────────────────────────────────────

/// The engine + snapshotter + sources for a single client group — the state
/// TS keeps on `PipelineDriver` (`#pipelines`, `#snapshotter`, `#tables`).
///
/// ── FIELD ORDER IS LOAD-BEARING — DO NOT REORDER ─────────────────────────
/// Rust drops struct fields in declaration order, and every CG teardown in
/// rust-syncer is a plain struct drop (no teardown path calls `destroy()`).
/// The connection-holding fields MUST drop as:
///
///   1. `engine`  — its `Drop` runs `Engine::destroy()`, breaking the
///      operator-graph Rc cycles and releasing the engine-held source/conn
///      clones;
///   2. `sources` — drops the per-table `TableSource` cells, releasing their
///      inner snapshot-conn `Rc` clones;
///   3. `snapshotter` LAST — its `Snapshot::drop` is then the SOLE owner of
///      the snapshot's SQLite connection and takes the explicit, checked,
///      LOUD close (snapshotter.rs). If anything still holds a conn clone at
///      that point, `Snapshot::drop` early-returns and the eventual close is
///      rusqlite's implicit `Drop`, which calls `sqlite3_close` and SWALLOWS
///      the error — a `SQLITE_BUSY` close then silently leaks the whole
///      handle (~11.5MB page cache + fds per CG churn; the release leak gate).
///
/// Port of TS `PipelineHydrationReason` (pipeline-driver.ts:123-125): why a
/// pipeline was (re)hydrated, carried on every lifecycle log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineHydrationReason {
    /// `'query-set-sync'` — added by `#syncQueryPipelineSet` (the default
    /// `addQuery` parameter, pipeline-driver.ts:580).
    QuerySetSync,
    /// `'unchanged-query-rehydrate'` — rebuilt by `#hydrateUnchangedQueries`
    /// (view-syncer.ts:1625).
    UnchangedQueryRehydrate,
}

impl PipelineHydrationReason {
    /// The TS string-literal union member.
    pub fn as_str(self) -> &'static str {
        match self {
            PipelineHydrationReason::QuerySetSync => "query-set-sync",
            PipelineHydrationReason::UnchangedQueryRehydrate => "unchanged-query-rehydrate",
        }
    }
}

/// One query of a [`IvmPipelines::hydrate`] batch — the TS `addQuery`
/// parameter list (pipeline-driver.ts:575-582: `transformationHash`, `queryID`,
/// `query`, `queryName?`, `hydrationReason`) minus the `timer`, which is shared
/// by the whole batch. `ast_json` is the TS-shaped transformed AST.
#[derive(Debug, Clone)]
pub struct HydrateQuery {
    pub query_id: String,
    pub ast_json: String,
    pub transformation_hash: String,
    pub query_name: Option<String>,
    pub hydration_reason: PipelineHydrationReason,
}

/// A bare `(query_id, ast_json)` pair hydrates with TS's `addQuery` defaults:
/// no `queryName`, `hydrationReason = 'query-set-sync'` (pipeline-driver.ts:
/// 580) and an empty transformation hash (the caller records the real one via
/// `set_query_transformation_hash`, exactly as before this struct existed).
impl From<(String, String)> for HydrateQuery {
    fn from((query_id, ast_json): (String, String)) -> Self {
        HydrateQuery {
            query_id,
            ast_json,
            transformation_hash: String::new(),
            query_name: None,
            hydration_reason: PipelineHydrationReason::QuerySetSync,
        }
    }
}

/// The per-pipeline identity TS keeps on its `Pipeline` record
/// (pipeline-driver.ts:91-101: `pipelineRunID`, `transformationHash`,
/// `queryName`, `hydrationReason`) and stamps on every lifecycle log line for
/// that pipeline. Recorded at hydrate START (TS `const pipelineRunID =
/// randomID()` precedes the `-start` line, :607-615) so `-start`, `-finish`,
/// `-failed`, `-aborted` and `-stop` all correlate on the same run id.
#[derive(Debug, Clone)]
struct PipelineLogInfo {
    pipeline_run_id: String,
    transformation_hash: String,
    query_name: Option<String>,
    hydration_reason: PipelineHydrationReason,
}

/// Port of TS `randomID()` (pipeline-driver.ts:176-178):
/// `randInt(1, Number.MAX_SAFE_INTEGER).toString(36)`.
fn random_id() -> String {
    const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
    let n: u64 = rand::Rng::gen_range(&mut rand::thread_rng(), 1..=MAX_SAFE_INTEGER);
    to_string_radix_36(n)
}

/// JS `Number.prototype.toString(36)` for a non-negative integer.
fn to_string_radix_36(mut n: u64) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".to_string();
    }
    let mut buf = Vec::with_capacity(11);
    while n > 0 {
        buf.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    buf.reverse();
    String::from_utf8(buf).expect("base36 digits are ASCII")
}

/// Port of TS `QueryPipelineLifecycleLog` (pipeline-driver.ts:133-143) — the
/// record `#logQueryPipelineLifecycle` formats. `zero_event` is always a fixed
/// literal; the `Option` fields mirror TS's optional properties and are omitted
/// from the emitted line when `None` (TS `withContext` only when defined).
#[derive(Default)]
struct QueryPipelineLifecycleLog {
    zero_event: &'static str,
    pipeline_run_id: String,
    query_hash: String,
    transformation_hash: String,
    query_name: Option<String>,
    hydration_reason: Option<PipelineHydrationReason>,
    hydration_time_ms: Option<f64>,
    hydration_row_count: Option<u64>,
    stop_reason: Option<&'static str>,
    pipeline_lifetime_ms: Option<f64>,
}

impl QueryPipelineLifecycleLog {
    /// Seed the identity fields from the pipeline's recorded [`PipelineLogInfo`]
    /// (TS reads them off the `Pipeline` record, e.g. `#destroyPipeline`
    /// pipeline-driver.ts:851-856).
    fn for_pipeline(
        zero_event: &'static str,
        query_hash: &str,
        info: Option<&PipelineLogInfo>,
    ) -> Self {
        QueryPipelineLifecycleLog {
            zero_event,
            pipeline_run_id: info.map(|i| i.pipeline_run_id.clone()).unwrap_or_default(),
            query_hash: query_hash.to_string(),
            transformation_hash: info
                .map(|i| i.transformation_hash.clone())
                .unwrap_or_default(),
            query_name: info.and_then(|i| i.query_name.clone()),
            hydration_reason: info.map(|i| i.hydration_reason),
            ..Default::default()
        }
    }
}

/// Port of TS `Timer` (pipeline-driver.ts:158-161): the caller-controlled
/// process-time timer a hydrate / advance is measured with. `elapsed_lap` is
/// the current time slice's age (what `should_yield` compares against the
/// threshold); `total_elapsed` excludes yielded time (what `hydration_time_ms`
/// and the advance budget are measured in). Implemented by the view-syncer's
/// `TimeSliceTimer`.
pub trait Timer {
    fn elapsed_lap(&self) -> f64;
    fn total_elapsed(&self) -> f64;
}

/// Port of TS `#hydrateContext: {timer: Timer} | null` (pipeline-driver.ts:275).
pub struct HydrateContext {
    pub timer: Rc<dyn Timer>,
}

/// Port of TS `#advanceContext` (pipeline-driver.ts:276, set at :955-961):
/// the caller's time-slice [`Timer`] for the yield arm of
/// `#shouldAdvanceYieldMaybeAbortAdvance` (:1156 — `checkYield &&
/// advanceTimer.elapsedLap() > yieldThresholdMs`). TS's other fields
/// (`totalHydrationTimeMs`, `numChanges`, `pos`, `currentChangeStartMs`) feed
/// the abort arms, which live in `rust_ivm::advance_gate` (I-11).
pub struct AdvanceContext {
    pub timer: Rc<dyn Timer>,
}

pub struct IvmPipelines {
    engine: Option<Engine>,
    /// Port of TS `PipelineDriver`'s `enablePlanner` ctor param
    /// (pipeline-driver.ts:305/315, fed by `config.enableQueryPlanner`,
    /// zero-config default true). `false` ⇒ no cost model is installed and
    /// `plan_ast` passes ASTs through unplanned — the documented "planner is
    /// picking bad strategies" opt-out.
    pub enable_query_planner: bool,
    syncable_tables: HashMap<String, LiteAndZqlSpec>,
    all_table_names: HashSet<String>,
    sources: HashMap<String, Rc<RefCell<dyn Source>>>,
    primary_keys: HashMap<String, Vec<String>>,
    /// Client-declared primary keys per table (from the client schema). Applied
    /// to the engine for client-facing rowKey EMISSION — TS
    /// `buildPrimaryKeys(clientSchema)`. Stored here so it survives an engine
    /// rebuild (`build_engine`) and is re-applied. Empty ⇒ emit `keyCmp[0]`.
    client_primary_keys: HashMap<String, Vec<String>>,
    /// Query ids currently hydrated in the engine. Mirrors TS
    /// `pipelineDriver.queries()` — used by the ViewSyncer to add only queries
    /// missing from the pipeline (`#syncQueryPipelineSet`), rather than
    /// re-hydrating the whole set on every config change.
    /// query_id → the transformation hash the pipeline was hydrated with. Port
    /// of TS `this.#pipelines.queries()` (a map whose entries carry
    /// `transformationHash`). A query must be re-hydrated when its transformation
    /// hash changes (e.g. after an auth change re-transforms read-permission
    /// rules), not only when it is absent — so we track the hash, not just the id.
    active_queries: HashMap<String, String>,
    /// query_id → the TS-shaped transformed AST JSON the pipeline was hydrated
    /// with. Port of the `transformedAst` carried by TS `pipelineDriver.queries()`
    /// (`QueryInfo`). Only consumed by the shadow-mode query-covering index
    /// (`enable_query_covering`); it has no effect on what is served. `Arc<str>`
    /// so `running_queries()` snapshots are refcount bumps, not string copies
    /// (these ASTs can be large and per-CG RSS matters).
    query_asts: HashMap<String, std::sync::Arc<str>>,
    /// Hydration insertion order of `query_asts` keys — TS `queries()` is a Map
    /// with insertion order, and the covering index's "first covering query"
    /// tie-break depends on it; iterating the HashMap made it nondeterministic
    /// run-to-run.
    query_order: Vec<String>,
    /// query_id → the identity fields TS keeps on its `Pipeline` record for the
    /// lifecycle log (see [`PipelineLogInfo`]). Set at hydrate start, dropped
    /// with the pipeline (`destroy_pipeline`).
    pipeline_log_info: HashMap<String, PipelineLogInfo>,
    /// Set when a non-scalar panic was caught mid-advance; forces the next
    /// advance to emit a reset instead of running on a half-mutated graph.
    poisoned: bool,
    /// Port of TS `#yieldThresholdMs: () => number` (pipeline-driver.ts:274,
    /// ctor param :304) — the per-call time-slice threshold selector (see
    /// server/syncer.ts:230-233: the priority-op threshold while a priority op
    /// is running, else the normal one). `None` (tests / the throwaway analyze
    /// driver) installs TS's ctor-default `shouldYield = () => false` on every
    /// source, i.e. never slices.
    yield_threshold_ms: Option<Rc<dyn Fn() -> f64>>,
    /// Port of TS `#hydrateContext: {timer: Timer} | null` (pipeline-driver.ts:
    /// 275/627/809): set for the duration of a hydrate so `should_yield` can
    /// read the caller's time-slice timer. Shared (`Rc<RefCell>`) with the
    /// `should_yield` closure installed on every `TableSource` — the rust twin
    /// of that closure capturing `this` (TS :1071).
    hydrate_context: Rc<RefCell<Option<HydrateContext>>>,
    /// Port of TS `#advanceContext` (pipeline-driver.ts:276/955-961/1000); set
    /// for the duration of an advance (see [`AdvanceContext`]).
    advance_context: Rc<RefCell<Option<AdvanceContext>>>,
    /// MUST stay the LAST field — dropped after `engine` and `sources` so the
    /// pinned snapshot's `Snapshot::drop` sole-owner loud close runs (see the
    /// struct-level "FIELD ORDER IS LOAD-BEARING" comment).
    snapshotter: Option<Snapshotter>,
}

impl Default for IvmPipelines {
    fn default() -> Self {
        Self::new()
    }
}

impl IvmPipelines {
    pub fn new() -> Self {
        IvmPipelines {
            engine: None,
            enable_query_planner: true,
            syncable_tables: HashMap::new(),
            all_table_names: HashSet::new(),
            sources: HashMap::new(),
            primary_keys: HashMap::new(),
            client_primary_keys: HashMap::new(),
            active_queries: HashMap::new(),
            query_asts: HashMap::new(),
            query_order: Vec::new(),
            pipeline_log_info: HashMap::new(),
            poisoned: false,
            yield_threshold_ms: None,
            hydrate_context: Rc::new(RefCell::new(None)),
            advance_context: Rc::new(RefCell::new(None)),
            snapshotter: None,
        }
    }

    /// Install the time-slice threshold selector — the TS ctor's
    /// `yieldThresholdMs: () => number` param (pipeline-driver.ts:304/316). Must
    /// be called BEFORE `init` so `build_engine` wires `should_yield` into every
    /// `TableSource` it creates (TS :1071).
    pub fn set_yield_threshold_ms(&mut self, yield_threshold_ms: Rc<dyn Fn() -> f64>) {
        self.yield_threshold_ms = Some(yield_threshold_ms);
    }

    /// Port of TS `#shouldYield()` (pipeline-driver.ts:1080-1089):
    /// ```ts
    /// if (this.#hydrateContext) {
    ///   return this.#hydrateContext.timer.elapsedLap() > this.#yieldThresholdMs();
    /// }
    /// if (this.#advanceContext) {
    ///   return this.#shouldAdvanceYieldMaybeAbortAdvance();
    /// }
    /// throw new Error('shouldYield called outside of hydration or advancement');
    /// ```
    pub fn should_yield(&self) -> bool {
        Self::should_yield_with(
            &self.hydrate_context,
            &self.advance_context,
            self.yield_threshold_ms.as_ref(),
        )
    }

    /// The body of [`should_yield`](Self::should_yield) over the shared cells,
    /// so the per-source closure (TS `() => this.#shouldYield()`) can call it
    /// without holding a borrow of the driver.
    fn should_yield_with(
        hydrate_context: &RefCell<Option<HydrateContext>>,
        advance_context: &RefCell<Option<AdvanceContext>>,
        yield_threshold_ms: Option<&Rc<dyn Fn() -> f64>>,
    ) -> bool {
        if let Some(hydrate) = hydrate_context.borrow().as_ref() {
            let threshold = yield_threshold_ms.map(|f| f()).unwrap_or(f64::INFINITY);
            return hydrate.timer.elapsed_lap() > threshold;
        }
        if let Some(advance) = advance_context.borrow().as_ref() {
            // Port of the yield arm of TS `#shouldAdvanceYieldMaybeAbortAdvance`
            // (pipeline-driver.ts:1156): `checkYield && advanceTimer.elapsedLap()
            // > this.#yieldThresholdMs()`. Its abort arms (:1095-1155) are
            // evaluated by the thread-local `advance_gate` (I-11) on the same
            // row-read path that calls this.
            let threshold = yield_threshold_ms.map(|f| f()).unwrap_or(f64::INFINITY);
            return advance.timer.elapsed_lap() > threshold;
        }
        panic!("shouldYield called outside of hydration or advancement");
    }

    /// The per-source `shouldYield` hook — TS `() => this.#shouldYield()`
    /// passed to every `TableSource` ctor (pipeline-driver.ts:1071). Without a
    /// threshold selector this is TS's ctor default `() => false`
    /// (zqlite/src/table-source.ts:103).
    fn should_yield_hook(&self) -> Rc<dyn Fn() -> bool> {
        match &self.yield_threshold_ms {
            None => Rc::new(|| false),
            Some(threshold) => {
                let hydrate_context = Rc::clone(&self.hydrate_context);
                let advance_context = Rc::clone(&self.advance_context);
                let threshold = Rc::clone(threshold);
                Rc::new(move || {
                    Self::should_yield_with(&hydrate_context, &advance_context, Some(&threshold))
                })
            }
        }
    }

    /// Whether `query_id` is currently hydrated in the engine. Port of the
    /// `this.#pipelines.queries().has(id)` check in TS `#syncQueryPipelineSet`.
    pub fn has_query(&self, query_id: &str) -> bool {
        self.active_queries.contains_key(query_id)
    }

    /// The transformation hash a query is currently hydrated with, or `None` if
    /// it is not hydrated. Port of
    /// `this.#pipelines.queries().get(id)?.transformationHash`.
    pub fn query_transformation_hash(&self, query_id: &str) -> Option<&str> {
        self.active_queries.get(query_id).map(|s| s.as_str())
    }

    /// Record (or overwrite) the transformation hash a query is hydrated with.
    /// Called after a successful hydrate so drift detection can compare hashes.
    pub fn set_query_transformation_hash(&mut self, query_id: &str, hash: &str) {
        if let Some(h) = self.active_queries.get_mut(query_id) {
            *h = hash.to_string();
        } else {
            self.active_queries
                .insert(query_id.to_string(), hash.to_string());
        }
    }

    /// The set of currently-hydrated query ids (snapshot). Port of
    /// `pipelineDriver.queries()`.
    pub fn active_query_ids(&self) -> Vec<String> {
        self.active_queries.keys().cloned().collect()
    }

    /// The engine's per-query hydration time (ms), or `None` if the query is not
    /// a registered pipeline. Surfaces the `add_queries_streaming`
    /// `QueryResult.hydration_time_ms` for the inspector's
    /// `query-materialization-server` metric (the TS `elapsed` recorded by the
    /// view-syncer around `pipelines.addQuery`).
    pub fn hydration_time_ms(&self, query_id: &str) -> Option<f64> {
        self.engine
            .as_ref()
            .and_then(|e| e.hydration_time_ms(query_id))
    }

    /// The currently-hydrated queries as `(query_id, transformed_ast_json,
    /// transformation_hash)`. Full port of TS `pipelineDriver.queries()` (which
    /// carries `transformedAst` + `transformationHash`), used to seed the
    /// shadow-mode query-covering index. Queries whose AST was not captured
    /// (e.g. hydrated directly in a unit test) are omitted.
    pub fn running_queries(&self) -> Vec<(String, std::sync::Arc<str>, String)> {
        self.query_order
            .iter()
            .filter_map(|qid| {
                let hash = self.active_queries.get(qid)?;
                let ast = self.query_asts.get(qid)?;
                Some((qid.clone(), ast.clone(), hash.clone()))
            })
            .collect()
    }

    /// Whether the engine has been initialized.
    /// Port of TS `PipelineDriver.currentPermissions()` (pipeline-driver.ts:403-418):
    /// re-read the `{app}.permissions` hash through the CURRENT snapshot
    /// connection (`this.#snapshotter.current().db`) and reload the doc only
    /// when the hash changed. TS calls this at its transform sites
    /// (view-syncer.ts:1553 `#hydrateUnchangedQueries`, :1933
    /// `#syncQueryPipelineSet`) — never per replica notification, and never
    /// through a freshly opened connection. The snapshot is pinned, so a
    /// permissions change becomes visible exactly when `advance` moves the
    /// snapshot past the replicated change (same as TS).
    ///
    /// `None` when no snapshotter is attached (`init_from_connection` tests):
    /// the caller then keeps the permissions it was configured with.
    pub fn current_permissions(
        &self,
        app_id: &str,
        current_hash: Option<&str>,
    ) -> Option<crate::auth::load_permissions::PermissionsReload> {
        let conn = self.snapshotter.as_ref()?.current_conn().ok()?;
        let guard = conn.borrow();
        Some(
            crate::auth::load_permissions::reload_permissions_if_changed(
                &guard,
                app_id,
                current_hash,
            ),
        )
    }

    pub fn initialized(&self) -> bool {
        self.engine.is_some()
    }

    /// The current database version, if a snapshotter is attached.
    pub fn current_version(&self) -> Option<String> {
        self.snapshotter
            .as_ref()
            .and_then(|s| s.current_version().ok().map(|v| v.to_string()))
    }

    /// Initialize the engine with table schemas and an optional SQLite replica.
    /// When `db_path` is `Some`, `TableSource`s backed by the replica are used;
    /// when `None`, in-memory `MemorySource`s are used (test/dev mode — no
    /// snapshotter, so `advance()` is unavailable).
    ///
    /// Port of TS `PipelineDriver.init` (pipeline-driver.ts:325), via the
    /// removed native bridge's `EngineState::init` (a5e502ad9).
    pub fn init(
        &mut self,
        tables: Vec<IvmTableSpec>,
        db_path: Option<&str>,
        app_id: &str,
    ) -> Result<(), String> {
        // Port of TS `reset(clientSchema)` (pipeline-driver.ts:343): rebuilding on
        // a schema change stops each existing pipeline with reason `reset`.
        for query_id in self.query_order.clone() {
            self.destroy_pipeline(&query_id, "reset");
        }
        if let Some(eng) = self.engine.as_mut() {
            eng.destroy();
        }
        // Preserve the snapshotter if one was already created; clear the rest.
        let preserved_snap = self.snapshotter.take();
        self.engine = None;
        self.syncable_tables.clear();
        self.all_table_names.clear();
        self.sources.clear();
        self.primary_keys.clear();
        self.active_queries.clear();
        self.poisoned = false;
        self.snapshotter = preserved_snap;

        // A per-table connection fallback would serve rows outside the pinned
        // snapshot and mix DB versions within one hydrate, so propagate every
        // snapshotter failure, as TS `init` does (pipeline-driver.ts:325).
        let snapshot_conn = if let Some(path) = db_path {
            if self.snapshotter.is_none() {
                let mut snap = Snapshotter::new(path, app_id, None);
                snap.init().map_err(|e| format!("snapshotter init: {e}"))?;
                self.snapshotter = Some(snap);
            }
            Some(
                self.snapshotter
                    .as_ref()
                    .expect("set above when it was None")
                    .current_conn()
                    .map_err(|e| format!("snapshotter current connection: {e}"))?,
            )
        } else {
            None
        };

        self.build_engine(&tables, snapshot_conn);
        Ok(())
    }

    /// Initialize the engine to hydrate directly from a plain SQLite connection,
    /// WITHOUT a snapshotter. Sources read the given connection's user tables.
    /// This supports the initial-hydrate path (and tests) — `advance()` still
    /// requires a snapshotter-backed `init`, so it is unavailable after this.
    pub fn init_from_connection(
        &mut self,
        tables: Vec<IvmTableSpec>,
        conn: SharedConn,
    ) -> Result<(), String> {
        // Port of TS `reset(clientSchema)` (pipeline-driver.ts:343): rebuilding on
        // a schema change stops each existing pipeline with reason `reset`.
        for query_id in self.query_order.clone() {
            self.destroy_pipeline(&query_id, "reset");
        }
        if let Some(eng) = self.engine.as_mut() {
            eng.destroy();
        }
        self.engine = None;
        // Ordered teardown (see the struct-level "FIELD ORDER IS LOAD-BEARING"
        // comment): clear `sources` BEFORE dropping the snapshotter, so the
        // snapshotter's `Snapshot::drop` is the sole conn owner and takes the
        // explicit loud close instead of leaving the last conn clone to
        // rusqlite's silent implicit close.
        self.syncable_tables.clear();
        self.all_table_names.clear();
        self.sources.clear();
        self.primary_keys.clear();
        self.active_queries.clear();
        self.poisoned = false;
        self.snapshotter = None;
        self.build_engine(&tables, Some(conn));
        Ok(())
    }

    /// Build sources (TableSource-backed by `source_conn`, else MemorySource),
    /// syncable specs, and the engine. Shared by `init` / `init_from_connection`.
    fn build_engine(&mut self, tables: &[IvmTableSpec], source_conn: Option<SharedConn>) {
        let mut primary_keys: HashMap<String, Vec<String>> = HashMap::new();

        for spec in tables {
            let mut columns: HashMap<String, ColumnType> = HashMap::new();
            for (col, schema) in &spec.columns {
                columns.insert(col.clone(), column_type(&schema.r#type, schema.optional));
            }

            // Declared column order (TS `Object.keys(columns)`) for the SELECT
            // list; fall back to the HashMap keys when a spec carries none.
            let column_order = if spec.column_order.is_empty() {
                columns.keys().cloned().collect()
            } else {
                spec.column_order.clone()
            };
            let rc_source: Rc<RefCell<dyn Source>> = if let Some(conn) = &source_conn {
                let table_source = TableSource::with_column_order(
                    conn.clone(),
                    &spec.table,
                    columns,
                    column_order,
                    spec.primary_key.clone(),
                    // TS `#getSource` → `new TableSource(..., () => this.#shouldYield())`
                    // (pipeline-driver.ts:1071).
                    self.should_yield_hook(),
                );
                Rc::new(RefCell::new(table_source))
            } else {
                let source = MemorySource::new(&spec.table, columns, spec.primary_key.clone());
                Rc::new(RefCell::new(source))
            };
            self.sources.insert(spec.table.clone(), rc_source);
            primary_keys.insert(spec.table.clone(), spec.primary_key.clone());

            let table_spec = TableSpec {
                name: spec.table.clone(),
                columns: spec
                    .columns
                    .iter()
                    .map(|(k, v)| (k.clone(), column_schema(v)))
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
                .map(|(k, v)| (k.clone(), column_schema(v)))
                .collect();
            self.syncable_tables.insert(
                spec.table.clone(),
                LiteAndZqlSpec {
                    table_spec,
                    zql_spec,
                },
            );
            self.all_table_names.insert(spec.table.clone());
        }

        let mut eng = Engine::new(primary_keys.clone());
        // Parity with TS `buildPipeline` → `planQuery(ast, costModel)`: give the
        // engine the replica connection so it plans correlated-subquery `flip`s
        // before building. Without this, exists-in-OR is built non-flipped and
        // over-emits WHERE-EXISTS backing rows to the CVR (the release-gate data differential). Only the
        // replica-backed (TableSource) path gets a cost model; MemorySource
        // fallbacks (some tests) stay unplanned. Gated on `enable_query_planner`
        // exactly like TS (`#costModels = enablePlanner ? new WeakMap() :
        // undefined`, pipeline-driver.ts:315 → costModel undefined → no
        // planQuery).
        if let Some(conn) = &source_conn
            && self.enable_query_planner
        {
            eng.set_cost_model_conn(conn.clone());
            // TS `createSQLiteCostModel(db, this.#tableSpecs)`
            // (pipeline-driver.ts:436): the scanstatus model probes with the
            // visible zql columns of every syncable table. Without specs the
            // engine degrades (loudly) to the filter-blind COUNT model —
            // the exact wiring gap behind the 144 s
            // flipped-join `tickets` hydrate incident.
            let specs: HashMap<String, HashMap<String, ColumnType>> = self
                .syncable_tables
                .iter()
                .map(|(table, spec)| {
                    (
                        table.clone(),
                        spec.zql_spec
                            .iter()
                            .map(|(col, cs)| (col.clone(), zql_column_type(cs)))
                            .collect(),
                    )
                })
                .collect();
            eng.set_cost_model_table_specs(specs);
        }
        for source in self.sources.values() {
            eng.register_source(source.clone());
        }
        for spec in tables {
            if let Some(mrv) = &spec.min_row_version {
                eng.set_table_spec(&spec.table, Some(mrv.clone()));
            }
            let unique_keys = spec
                .unique_keys
                .clone()
                .unwrap_or_else(|| vec![spec.primary_key.clone()]);
            eng.set_unique_keys(&spec.table, unique_keys);
        }
        // TS `buildPrimaryKeys(clientSchema)`: emission uses the client PKs.
        eng.set_client_primary_keys(self.client_primary_keys.clone());
        self.engine = Some(eng);
        self.primary_keys = primary_keys;
    }

    /// Install the client-declared primary keys (from the client schema) used
    /// for client-facing rowKey emission. Stored so a later `init`/rebuild
    /// re-applies them, and applied immediately if the engine already exists.
    /// Port of TS `buildPrimaryKeys(clientSchema, primaryKeys)`.
    pub fn set_client_primary_keys(&mut self, client_primary_keys: HashMap<String, Vec<String>>) {
        self.client_primary_keys = client_primary_keys;
        if let Some(eng) = self.engine.as_mut() {
            eng.set_client_primary_keys(self.client_primary_keys.clone());
        }
    }

    /// Remove a query's pipeline (and its row-set signature entry).
    /// Port of TS `removeQuery(queryID, stopReason)` (pipeline-driver.ts:834):
    /// `#destroyPipeline` (stop-log + teardown), then delete the bookkeeping.
    pub fn remove_query(&mut self, query_id: &str, stop_reason: &'static str) {
        self.destroy_pipeline(query_id, stop_reason);
        self.active_queries.remove(query_id);
        if self.query_asts.remove(query_id).is_some() {
            self.query_order.retain(|q| q != query_id);
        }
    }

    /// Port of TS `PipelineDriver.#logQueryPipelineLifecycle`
    /// (pipeline-driver.ts:470). Emits one `query pipeline lifecycle` info event
    /// per query-pipeline transition so a slow/heavy query is identifiable from
    /// logs by `hydration_time_ms` + `hydration_row_count`.
    ///
    /// Rust-only shape (HARD RULE 5): an associated fn (no `&self`) rather than a
    /// method, because the caller holds a `&mut self.engine` borrow across the
    /// hydrate and cannot also borrow `&self`; TS `this.#lc.withContext(...)` is
    /// replaced by the global `tracing` subscriber. The optional TS contexts
    /// (`queryName`, `hydrationReason`, `stopReason`, `hydrationTimeMs`,
    /// `hydrationRowCount`, `pipelineLifetimeMs`) are passed as `Option`s:
    /// `tracing` records nothing for a `None`, so each is omitted from the line
    /// exactly when TS skips its `withContext` (pipeline-driver.ts:488-505).
    fn log_query_pipeline_lifecycle(log: QueryPipelineLifecycleLog) {
        let QueryPipelineLifecycleLog {
            zero_event,
            pipeline_run_id,
            query_hash,
            transformation_hash,
            query_name,
            hydration_reason,
            hydration_time_ms,
            hydration_row_count,
            stop_reason,
            pipeline_lifetime_ms,
        } = log;
        tracing::info!(
            zero_event,
            pipeline_run_id,
            query_hash,
            transformation_hash,
            query_name = query_name.as_deref(),
            hydration_reason = hydration_reason.map(PipelineHydrationReason::as_str),
            stop_reason,
            hydration_time_ms,
            hydration_row_count,
            pipeline_lifetime_ms,
            "query pipeline lifecycle"
        );
    }

    /// VENDED per-table debug log — port of the `runtimeDebugFlags
    /// .trackRowCountsVended` block in TS `#addQueryImpl`
    /// (pipeline-driver.ts:704-721). For a slow query, logs how many rows each
    /// source table VENDED (scanned) — keyed by SQL — plus the grand total
    /// "rows considered". This is the query-efficiency diagnostic: a query that
    /// scans many rows to emit few reveals a missing index / unbounded filter.
    ///
    /// Deviation from TS iterating `this.#tables.keys()` (every registered
    /// source, printing zero-vend tables as `[]`): rust iterates the tables THIS
    /// query's sources actually prepared (the entries `getVendedRowCounts()`
    /// holds — `initQuery` seeds one per fetched table). The non-empty VENDED
    /// lines and `Total rows considered` are identical; only all-zero,
    /// query-untouched tables are omitted (pure log noise, no diagnostic loss).
    /// `table_names` must be sorted so the log is deterministic (TS relies on
    /// `#tables` insertion order; rust's `HashMap` is unordered).
    fn log_vended_row_counts(
        query_id: &str,
        hydration_time_ms: f64,
        vended: Option<&RowCountsBySource>,
        table_names: &[String],
    ) {
        let mut total_rows_considered: u64 = 0;
        // TS iterates `this.#tables.keys()` — EVERY table the driver has a
        // source for — not the keys of the vended map, and reads
        // `debugDelegate?.getVendedRowCounts()[tableName] ?? {}` per table
        // (pipeline-driver.ts:710-718). So a table that vended nothing still
        // logs `<table> VENDED: []`, and with `trackRowsVended` off (prod: only
        // `trackRowCountsVended` is set, zero-config.ts:1214) EVERY table logs
        // an empty entry list and the total is 0. Iterating the vended map's
        // keys instead emitted no per-table line at all in that configuration.
        //
        // Known difference: TS fills `#tables` LAZILY in `#getSource`
        // (pipeline-driver.ts:1073), so its set is the tables touched so far;
        // rust builds every source up front in `build_engine`, so this logs the
        // full table set. Same shape, wider set — a consequence of eager source
        // construction, not of this diagnostic.
        let empty: HashMap<String, u64> = HashMap::new();
        for table_name in table_names {
            let by_query = vended.and_then(|c| c.get(table_name)).unwrap_or(&empty);
            // TS: `totalRowsConsidered += entries.reduce((a, e) => a + e[1], 0)`.
            let table_total: u64 = by_query.values().copied().sum();
            total_rows_considered += table_total;
            // TS: `lc.info?.(tableName + ' VENDED: ', entries)` — the entries
            // are the [(sql, count)] pairs for this table.
            tracing::info!(
                query_id,
                hydration_time_ms,
                table = %table_name,
                entries = ?by_query,
                "{table_name} VENDED"
            );
        }
        // TS: `lc.info?.(`Total rows considered: ${totalRowsConsidered}`)`.
        tracing::info!(
            query_id,
            hydration_time_ms,
            total_rows_considered,
            "Total rows considered: {total_rows_considered}"
        );
    }

    /// Port of TS `#destroyPipeline` (pipeline-driver.ts:846): emit the
    /// `query-pipeline-stop` lifecycle event for `query_id`, then tear the
    /// pipeline down. TS's `pipeline.input.destroy()` half is delegated to
    /// `Engine::remove_query` (the operator graph lives in `rust-ivm::Engine`) —
    /// the sole point where this one TS method is split across the crate boundary;
    /// the log stays in the driver, exactly as in TS. The log is a no-op when the
    /// query has no registered pipeline, matching TS's `if (pipeline)` guard in
    /// `removeQuery`/`destroy`/`reset`.
    fn destroy_pipeline(&mut self, query_id: &str, stop_reason: &'static str) {
        if let Some(eng) = self.engine.as_ref()
            && let (Some(t), Some(n), Some(lt)) = (
                eng.hydration_time_ms(query_id),
                eng.hydration_row_count(query_id),
                eng.pipeline_lifetime_ms(query_id),
            )
        {
            Self::log_query_pipeline_lifecycle(QueryPipelineLifecycleLog {
                hydration_time_ms: Some(t),
                hydration_row_count: Some(n),
                stop_reason: Some(stop_reason),
                pipeline_lifetime_ms: Some(lt),
                ..QueryPipelineLifecycleLog::for_pipeline(
                    "query-pipeline-stop",
                    query_id,
                    self.pipeline_log_info.get(query_id),
                )
            });
        }
        self.pipeline_log_info.remove(query_id);
        if let Some(eng) = self.engine.as_mut() {
            eng.remove_query(query_id);
        }
    }

    /// Hydrate the given queries against the current snapshot, returning the
    /// pull-based change stream. `queries` is a slice of `(query_id, ast_json)`
    /// where `ast_json` is the TS-shaped transformed AST.
    ///
    /// Port of TS `addQuery` (pipeline-driver.ts:575-596) → `#addQueryImpl`
    /// (:598-810) for a batch of queries (the documented `!Send` batching
    /// invention: rust hydrates the whole query set in one engine call). Like
    /// the TS generator, the returned [`HydrateChanges`] produces one
    /// `RowChange` per pull and surfaces a `StreamItem::Yield` whenever a
    /// `TableSource`'s `should_yield` fired mid-fetch (`generateWithYields`), so
    /// the consumer (`#processChanges`) can `await timer.yield_process()` before
    /// pulling again. `timer` is the caller-controlled process-time [`Timer`]
    /// (TS `addQuery(..., timer, ...)`), stored in `#hydrateContext` for
    /// `should_yield` (pipeline-driver.ts:627/1080) and handed to the engine as
    /// the clock each query's `hydration_time_ms` is measured on (:703).
    ///
    /// Row-set-signature maintenance is intentionally NOT done here — it is
    /// caller-driven (the view-syncer), as TS `#trackRowSetSignatures` is
    /// (pipeline-driver.ts:884-899).
    pub fn hydrate<Q: Clone + Into<HydrateQuery>>(
        &mut self,
        queries: &[Q],
        timer: Rc<dyn Timer>,
    ) -> Result<HydrateChanges<'_>, JsError> {
        // Rehydrate rebuilds pipelines fresh, so any poison is cleared.
        self.poisoned = false;
        if self.engine.is_none() {
            return Err("Engine not initialized".into());
        }
        let queries: Vec<HydrateQuery> = queries.iter().cloned().map(Into::into).collect();
        let queries = &queries;

        let mut specs: Vec<QuerySpec> = Vec::with_capacity(queries.len());
        for q in queries {
            let ast = parse_ts_ast(&q.ast_json)
                .map_err(|e| format!("AST parse error for qid={}: {e}", q.query_id))?;
            specs.push(QuerySpec {
                query_id: q.query_id.clone(),
                ast,
            });
        }

        // Per-query hydrate lifecycle logging — port of TS
        // `#logQueryPipelineLifecycle` (pipeline-driver.ts:470/608/784/796/815).
        // TS wraps each `addQuery` in a start/finish/failed/aborted envelope; Rust
        // hydrates the whole query set in ONE engine call (the documented !Send
        // batching invention), so the per-query boundaries come from the
        // returned `QueryResult`s: `-start` before the batch, `-finish` (with
        // timing + row count) for each pipeline the engine registered, `-aborted`
        // for a started-but-unregistered query (cancel-during-hydrate), and
        // `-failed` on a hydrate panic. This is the always-on analog of TS
        // `VENDED` (which is gated behind the `trackRowCountsVended` debug flag) —
        // it makes a slow/heavy query identifiable from logs by time + rows.
        for q in queries {
            // TS: `const pipelineRunID = randomID()` then the `-start` line
            // (pipeline-driver.ts:607-615); the identity is kept on the pipeline
            // record so every later line for this run carries the same id.
            let info = PipelineLogInfo {
                pipeline_run_id: random_id(),
                transformation_hash: q.transformation_hash.clone(),
                query_name: q.query_name.clone(),
                hydration_reason: q.hydration_reason,
            };
            Self::log_query_pipeline_lifecycle(QueryPipelineLifecycleLog::for_pipeline(
                "query-pipeline-hydrate-start",
                &q.query_id,
                Some(&info),
            ));
            self.pipeline_log_info.insert(q.query_id.clone(), info);
        }

        // TS pipeline-driver.ts:623-629.
        assert!(
            self.advance_context.borrow().is_none(),
            "Cannot hydrate while advance is in progress"
        );
        *self.hydrate_context.borrow_mut() = Some(HydrateContext {
            timer: Rc::clone(&timer),
        });

        // A hydrate panic (e.g. a source-drift assert) must roll back the
        // partially-wired source connections before re-throwing, so a follow-up
        // rehydrate builds a clean graph.
        // The build (phase 1) is guarded here; every later pull is guarded by
        // `HydrateChanges::next`.
        let eng = self.engine.as_mut().expect("checked above");
        let checkpoint = eng.source_connection_checkpoint();
        let total_elapsed: Rc<dyn Fn() -> f64> = Rc::new(move || timer.total_elapsed());
        let started = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            eng.start_hydrate(&specs, Some(total_elapsed))
        }));
        let stream = match started {
            Ok(stream) => stream,
            Err(payload) => {
                // TS `#addQueryImpl` logs `query-pipeline-hydrate-failed` and
                // RETHROWS (pipeline-driver.ts:794-812); the throw reaches the
                // view-syncer, which fails the group with an error frame
                // (`#cleanup(err)` → `client.fail`). Rust surfaces the same
                // failure through the `Err` channel the caller already routes to
                // `fail_group`: a bare `resume_unwind` here instead killed the CG
                // TASK, which bypasses `fail_group` entirely — clients lost their
                // socket with NO error frame (cg_executor.rs:299 logs the panic
                // and counts `fail_group("panic")`). See the cost-model probe
                // (`sqlite_cost_model.rs`), whose TS twin `db.prepare(sql)`
                // simply THROWS a `SqliteError`.
                self.on_hydrate_panic(&checkpoint, queries, &payload);
                return Err(hydrate_js_error(&payload));
            }
        };
        Ok(HydrateChanges {
            driver: self,
            stream: Some(stream),
            checkpoint,
            queries: queries.clone(),
            outcome: None,
        })
    }

    /// Shared by every hydrate panic path (build or a later pull): clear the
    /// hydrate context (TS `finally`), roll the partially-wired source
    /// connections back, and emit the `-failed` lifecycle line per query.
    fn on_hydrate_panic(
        &mut self,
        checkpoint: &HashMap<String, usize>,
        queries: &[HydrateQuery],
        payload: &Box<dyn std::any::Any + Send>,
    ) {
        *self.hydrate_context.borrow_mut() = None;
        if let Some(eng) = self.engine.as_mut() {
            eng.rollback_source_connections(checkpoint);
        }
        for q in queries {
            Self::log_query_pipeline_lifecycle(QueryPipelineLifecycleLog::for_pipeline(
                "query-pipeline-hydrate-failed",
                &q.query_id,
                self.pipeline_log_info.get(&q.query_id),
            ));
            // TS logs the lifecycle event and THEN the failure itself
            // (`logQueryFailure(this.#lc, {queryHash, transformationHash,
            // queryName}, 'query hydration failed', e)`, pipeline-driver.ts
            // :806-812) — one per query in the failed batch, before rethrowing.
            log_query_failure(
                self.pipeline_log_info
                    .get(&q.query_id)
                    .map(|info| QueryLogInfo {
                        query_hash: &q.query_id,
                        transformation_hash: &info.transformation_hash,
                        query_name: info.query_name.as_deref(),
                    }),
                QueryFailureMessage::Hydration,
                payload,
            );
            // No pipeline was registered for a failed hydrate (TS `#pipelines`
            // is only set on the success path, :771), so drop its identity.
            self.pipeline_log_info.remove(&q.query_id);
        }
    }

    /// The source table names the VENDED diagnostic iterates, or an EMPTY
    /// vec when the diagnostic cannot fire.
    ///
    /// TS reads `#tables.keys()` INSIDE
    /// `if (runtimeDebugFlags.trackRowCountsVended)` (pipeline-driver.ts:704-710),
    /// so with the flag off it does no work at all. Rust has to collect before
    /// the `&mut engine` borrow in `finish_hydrate`, but the flag is a
    /// process-static, so read it here too: with ~150 sources this was 150
    /// `String` clones plus a sort on EVERY hydrate to feed a diagnostic that
    /// is off in production. The per-query slow-hydrate threshold check stays
    /// at the callsite, where TS has it (:705).
    ///
    /// The sort has no TS twin and is deliberate: TS iterates a `Map` in
    /// insertion order, rust's `HashMap` has no order at all, so sorting is
    /// what makes the log lines deterministic.
    fn vended_table_names(&self) -> Vec<String> {
        if !runtime_debug_flags().track_row_counts_vended() {
            return Vec::new();
        }
        let mut names: Vec<String> = self.sources.keys().cloned().collect();
        names.sort();
        names
    }

    /// Phase 3 for the driver: register the pipelines in the engine (or destroy
    /// them when the stream was cancelled / abandoned), emit the `-finish` /
    /// `-aborted` lifecycle lines and the VENDED diagnostic, and record the
    /// hydrated queries. Port of the TS `#addQueryImpl` register tail +
    /// `finally` (pipeline-driver.ts:723-810).
    fn finish_hydrate(&mut self, stream: HydrateStream, queries: &[HydrateQuery]) {
        *self.hydrate_context.borrow_mut() = None;
        let vended_table_names = self.vended_table_names();
        let Some(eng) = self.engine.as_mut() else {
            return;
        };
        let results = eng.finish_hydrate(stream);
        let finished: HashSet<&str> = results.iter().map(|r| r.query_id.as_str()).collect();
        for r in &results {
            Self::log_query_pipeline_lifecycle(QueryPipelineLifecycleLog {
                hydration_time_ms: Some(r.hydration_time_ms),
                hydration_row_count: Some(r.hydration_row_count),
                ..QueryPipelineLifecycleLog::for_pipeline(
                    "query-pipeline-hydrate-finish",
                    &r.query_id,
                    self.pipeline_log_info.get(&r.query_id),
                )
            });
            // VENDED per-table debug log — port of TS `#addQueryImpl`'s
            // `runtimeDebugFlags.trackRowCountsVended` block (pipeline-driver.ts:
            // 704-721). Gated on the flag AND a slow hydrate; reports how many
            // rows each source table VENDED (scanned) for this query, the
            // rows-considered diagnostic distinct from the output row count.
            if runtime_debug_flags().track_row_counts_vended()
                && r.hydration_time_ms > super::view_syncer::slow_hydrate_threshold_ms()
            {
                Self::log_vended_row_counts(
                    &r.query_id,
                    r.hydration_time_ms,
                    r.vended_row_counts.as_ref(),
                    &vended_table_names,
                );
            }
        }
        // A query that started but the engine never registered was aborted
        // mid-stream (cancel-during-hydrate → engine discards partial pipelines
        // and returns no result for it).
        for q in queries {
            if !finished.contains(q.query_id.as_str()) {
                Self::log_query_pipeline_lifecycle(QueryPipelineLifecycleLog::for_pipeline(
                    "query-pipeline-hydrate-aborted",
                    &q.query_id,
                    self.pipeline_log_info.get(&q.query_id),
                ));
                self.pipeline_log_info.remove(&q.query_id);
            }
        }
        // Track the newly-hydrated queries so `has_query` reports them — ONLY
        // the ones the engine registered, exactly as TS adds to `#pipelines`
        // only on the success path (pipeline-driver.ts:771); a cancelled or
        // abandoned query has no pipeline and must be re-added by the next
        // sync. The transformation hash is recorded by the caller
        // (`hydrate_and_sync`) right after this returns, via
        // `set_query_transformation_hash`; entries hydrated directly (tests)
        // keep an empty-string placeholder hash.
        for q in queries {
            if !finished.contains(q.query_id.as_str()) {
                continue;
            }
            self.active_queries.entry(q.query_id.clone()).or_default();
            if self
                .query_asts
                .insert(
                    q.query_id.clone(),
                    std::sync::Arc::from(q.ast_json.as_str()),
                )
                .is_none()
            {
                self.query_order.push(q.query_id.clone());
            }
        }
    }

    /// Hydrate one AST with an explicit `Debug` delegate attached, collecting the
    /// ADD rows, for the analyzeQuery path. The engine of a THROWAWAY analysis
    /// `IvmPipelines` (see `services::analyze::analyze_query`) — never the live
    /// serving engine. Port of the hydrate half of TS `runAst` (run-ast.ts:118-
    /// 169): the debug delegate the source records vended-rows / nvisit / plans
    /// on is `run_ast`'s (TS `host.debug`), read back by the caller afterward.
    pub fn hydrate_analyze(
        &mut self,
        ast_json: &str,
        debug: SharedDebug,
    ) -> Result<Vec<(String, Row)>, String> {
        let ast = parse_ts_ast(ast_json).map_err(|e| format!("AST parse error: {e}"))?;
        let eng = self
            .engine
            .as_mut()
            .ok_or_else(|| "Engine not initialized".to_string())?;
        eng.set_analyze_debug(Some(debug));

        let collected: Rc<RefCell<Vec<(String, Row)>>> = Rc::new(RefCell::new(Vec::new()));
        let sink = collected.clone();
        // TS `runAst` collects every ADD rowChange (main + companion), then dedups
        // in the loop; the dedup lives in `run_ast`.
        eng.add_queries_streaming(
            &[QuerySpec {
                query_id: "analyze".to_string(),
                ast,
            }],
            move |rc: &RowChange| {
                if rc.change_type == ChangeType::Add
                    && let Some(row) = &rc.row
                {
                    sink.borrow_mut().push((rc.table.clone(), row.clone()));
                }
            },
        );

        if let Some(eng) = self.engine.as_mut() {
            eng.set_analyze_debug(None);
        }
        Ok(Rc::try_unwrap(collected)
            .map(|c| c.into_inner())
            .unwrap_or_else(|rc| rc.borrow().clone()))
    }

    /// Advance the replica to head, returning the pull-based change stream. Port
    /// of TS `advance(timer)` (pipeline-driver.ts:926-947): the header
    /// (`version`, `numChanges`) is available immediately via
    /// [`AdvanceChanges::header`]; the changes are produced one per pull and a
    /// `StreamItem::Yield` is surfaced before a change whenever the yield arm
    /// of `#shouldAdvanceYieldMaybeAbortAdvance` fires (:975-977), so the
    /// consumer (`#advancePipelines` → `#processChanges`) can `await` its time
    /// slice. `timer` is stored in `#advanceContext` (:955-961) for that arm.
    ///
    /// Port of `AdvanceTask::compute`'s panic handling: an engine panic is
    /// caught so it cannot cross into the process and abort every CG. A
    /// `ScalarResetError` panic maps to an in-place `Reset` (rehydrate at
    /// curr); any other panic poisons the engine and surfaces as `Err` (TS
    /// teardown parity — the caller tears down and the client reconnects). A
    /// `reset_reason` reported by the engine also maps to `Reset`. Either is
    /// delivered by [`AdvanceChanges::finish`].
    pub fn advance(&mut self, timer: Rc<dyn Timer>) -> Result<AdvanceChanges<'_>, String> {
        if self.poisoned {
            self.poisoned = false;
            return Ok(AdvanceChanges {
                driver: self,
                stream: None,
                header: (String::new(), 0),
                outcome: Some(Ok(AdvanceOutcome::Reset {
                    reason: "schema-change".to_string(),
                    msg: "engine reset after a prior advance panic; rehydrating".to_string(),
                })),
            });
        }

        let syncable_tables = self.syncable_tables.clone();
        let all_table_names = self.all_table_names.clone();
        if self.engine.is_none() {
            return Err("Engine not initialized".into());
        }
        if self.snapshotter.is_none() {
            return Err("Snapshotter not initialized".to_string());
        }

        // TS pipeline-driver.ts:951-961: `assert(this.#hydrateContext === null,
        // 'Cannot advance while hydration is in progress')`, then
        // `this.#advanceContext = {...}` for the duration of the advance.
        assert!(
            self.hydrate_context.borrow().is_none(),
            "Cannot advance while hydration is in progress"
        );
        *self.advance_context.borrow_mut() = Some(AdvanceContext {
            timer: Rc::clone(&timer),
        });
        // The between-change yield arm handed to the engine's stream — the same
        // `should_yield` the sources consult, evaluated at TS's per-change site
        // (pipeline-driver.ts:975-977).
        let should_yield = self
            .yield_threshold_ms
            .as_ref()
            .map(|_| self.should_yield_hook());

        // The economic budget's clock: TS reads `advanceTimer.totalElapsed()`
        // (pipeline-driver.ts:1101) off the SAME `TimeSliceTimer`
        // `#advancePipelines` hands to `pipelines.advance(timer)`. Hand it down
        // so the gate charges the advance only the process time TS charges it —
        // the initial `timer.start()` queue turn and every yielded lap are
        // outside that timer (view-syncer.ts:2952-2969), so they are outside
        // the budget.
        let budget_clock: Rc<dyn Fn() -> f64> = {
            let t = Rc::clone(&timer);
            Rc::new(move || t.total_elapsed())
        };
        let eng = self.engine.as_mut().expect("checked above");
        let snapshotter = self.snapshotter.as_mut().expect("checked above");
        let started = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            eng.start_advance(
                snapshotter,
                &syncable_tables,
                &all_table_names,
                should_yield,
                Some(budget_clock),
            )
        }));
        match started {
            Ok(Ok(stream)) => {
                let header = (stream.version().to_string(), stream.num_changes());
                Ok(AdvanceChanges {
                    driver: self,
                    stream: Some(stream),
                    header,
                    outcome: None,
                })
            }
            Ok(Err(e)) => {
                *self.advance_context.borrow_mut() = None;
                Err(format!("advance failed: {e}"))
            }
            Err(payload) => {
                *self.advance_context.borrow_mut() = None;
                Err(self
                    .advance_panic_outcome(payload)
                    .err()
                    .unwrap_or_default())
            }
        }
    }

    /// Map an engine panic to the advance outcome exactly as the callback path
    /// did: a `ScalarResetError` → in-place `Reset`; anything else poisons the
    /// engine and surfaces as `Err`.
    fn advance_panic_outcome(
        &mut self,
        payload: Box<dyn std::any::Any + Send>,
    ) -> Result<AdvanceOutcome, String> {
        if let Some(msg) = scalar_reset_message(&payload) {
            Ok(AdvanceOutcome::Reset {
                reason: "scalar-subquery".to_string(),
                msg,
            })
        } else {
            // Engine panic (e.g. a source-drift assert): mark poisoned
            // and surface as a thrown error (TS teardown parity).
            //
            // TS's twin is `QueryPipeline.push`'s catch, which logs
            // `'query pipeline failed'` with THAT pipeline's identity and
            // rethrows (pipeline-driver.ts:1432-1444). Rust cannot name the
            // pipeline: one `stream.next()` pushes the change through every
            // pipeline inside a single engine call, so the panic is not
            // attributable to one query. The identity fields are therefore
            // ABSENT — TS's own `queryInfo === undefined` branch (:1240-1248,
            // when the pipeline is no longer registered) — rather than wrong.
            self.poisoned = true;
            log_query_failure(None, QueryFailureMessage::Pipeline, &payload);
            Err(format!("engine advance panic: {}", panic_message(&payload)))
        }
    }

    /// Close the advance: engine phase 3 (restore sources to head, map the
    /// result), then `finally { this.#advanceContext = null }`
    /// (pipeline-driver.ts:1000).
    fn finish_advance(&mut self, stream: AdvanceStream) -> Result<AdvanceOutcome, String> {
        let result = match self.engine.as_mut() {
            Some(eng) => eng.finish_advance(stream),
            None => Err(rust_ivm::snapshotter::DiffError::Other(
                "Engine not initialized".to_string(),
            )),
        };
        *self.advance_context.borrow_mut() = None;
        match result {
            Ok(result) => {
                if let Some(reason) = result.reset_reason {
                    Ok(AdvanceOutcome::Reset {
                        reason,
                        msg: result.reset_msg.unwrap_or_default(),
                    })
                } else {
                    Ok(AdvanceOutcome::Advanced {
                        version: result.version,
                        num_changes: result.num_changes,
                    })
                }
            }
            Err(e) => Err(format!("advance failed: {e}")),
        }
    }

    /// Get a row by primary key, for catchup. Port of `Engine::get_row`.
    pub fn get_row(&self, table: &str, pk: &[(String, Value)]) -> Option<Row> {
        self.engine.as_ref()?.get_row(table, pk)
    }

    // NOTE: a `row_set_signature(query_id)` passthrough to
    // `Engine::row_set_signature` once lived here; it had no caller (the
    // sync_engine reads the CVR's persisted `row_set_signature` field and
    // parses it via `rust_cvr::row_set_signature::parse_signature` directly)
    // and no TS twin at this layer (TS tracks rowSetSignature in cvr-store.ts,
    // mirrored by rust-cvr) — removed as dead drift.

    /// Tear down pipelines and drop the engine + snapshotter.
    ///
    /// Teardown ORDER is load-bearing (see the struct-level "FIELD ORDER IS
    /// LOAD-BEARING" comment): engine first (breaks operator-graph cycles,
    /// releases engine-held conn clones), then `sources` (releases the
    /// per-table conn clones), then the snapshotter LAST — so its
    /// `Snapshot::drop` is the sole conn owner and runs the explicit,
    /// checked, LOUD sqlite close instead of rusqlite's silent implicit one.
    pub fn destroy(&mut self) {
        // Port of TS `destroy()` (pipeline-driver.ts:447): stop every pipeline
        // with reason `destroy` before releasing the engine.
        for query_id in self.query_order.clone() {
            self.destroy_pipeline(&query_id, "destroy");
        }
        if let Some(eng) = self.engine.as_mut() {
            eng.destroy();
        }
        self.engine = None;
        self.sources.clear();
        self.snapshotter = None;
        self.syncable_tables.clear();
        self.all_table_names.clear();
        self.primary_keys.clear();
        self.active_queries.clear();
        self.query_asts.clear();
        self.query_order.clear();
    }
}

/// The driver-level hydrate change stream — TS `addQuery`'s
/// `Iterable<RowChange | 'yield'>` (pipeline-driver.ts:575-596). Wraps the
/// engine's [`HydrateStream`] with the driver's panic handling (rollback of the
/// partially-wired source connections, the `-failed` lifecycle line) on every
/// pull, and guarantees the engine's phase 3 runs exactly once — on
/// [`finish`](Self::finish), or on drop if the consumer abandons the stream
/// (TS's generator `finally`, :801-810: an unfinished hydrate logs `-aborted`
/// and registers nothing).
pub struct HydrateChanges<'a> {
    driver: &'a mut IvmPipelines,
    stream: Option<HydrateStream>,
    checkpoint: HashMap<String, usize>,
    queries: Vec<HydrateQuery>,
    /// Set when a pull panicked: the hydrate is over and [`finish`](Self::finish)
    /// reports the failure as an `Err`, mirroring `AdvanceChanges::outcome`.
    outcome: Option<JsError>,
}

impl HydrateChanges<'_> {
    /// Run phase 3 now (register / destroy + lifecycle lines) and report the
    /// hydrate outcome. Dropping the stream runs phase 3 too (outcome
    /// discarded); this makes it explicit at the call site.
    ///
    /// An `Err` here is TS `addQuery` THROWING: the view-syncer must fail the
    /// group with it (TS `#cleanup(err)` → `client.fail`), not continue.
    pub fn finish(mut self) -> Result<(), JsError> {
        if let Some(outcome) = self.outcome.take() {
            self.stream = None;
            return Err(outcome);
        }
        if let Some(stream) = self.stream.take() {
            self.driver.finish_hydrate(stream, &self.queries);
        }
        Ok(())
    }
}

impl Iterator for HydrateChanges<'_> {
    type Item = StreamItem<RowChange>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.outcome.is_some() {
            return None;
        }
        let stream = self.stream.as_mut()?;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| stream.next())) {
            Ok(item) => item,
            Err(payload) => {
                // Roll the partially-wired source connections back, then end the
                // stream and hand the message to `finish` as an `Err` — TS
                // `#addQueryImpl`'s `catch (e) { ...'query-pipeline-hydrate-
                // failed'...; throw e; }` (pipeline-driver.ts:794-812), whose
                // throw the view-syncer turns into `#cleanup(err)`.
                self.driver
                    .on_hydrate_panic(&self.checkpoint, &self.queries, &payload);
                // Drop the stream here: its Drop must run OUTSIDE the caught
                // panic (the RefCell borrows a mid-fetch panic held are already
                // released) but before `finish`, exactly as `AdvanceChanges`
                // does. `Drop for HydrateChanges` then sees `None` and skips
                // `finish_hydrate` on a half-built graph.
                self.stream = None;
                self.outcome = Some(hydrate_js_error(&payload));
                None
            }
        }
    }
}

impl Drop for HydrateChanges<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // The panic path already rolled back; `finish_hydrate` must not
            // touch the half-unwound graph.
            return;
        }
        if let Some(stream) = self.stream.take() {
            self.driver.finish_hydrate(stream, &self.queries);
        }
    }
}

/// The driver-level advance change stream — TS `advance(timer)`'s
/// `{version, numChanges, changes: Iterable<RowChange | 'yield'>}`
/// (pipeline-driver.ts:926-947). Wraps the engine's [`AdvanceStream`] with the
/// driver's panic mapping on every pull (a `ScalarResetError` → `Reset`,
/// anything else poisons + `Err`, delivered by [`finish`](Self::finish)), and
/// guarantees the engine's phase 3 (sources back to head, `#advanceContext =
/// null`) runs exactly once — on `finish`, or on drop if the consumer abandons
/// the stream (TS `finally`, :1000-1010).
pub struct AdvanceChanges<'a> {
    driver: &'a mut IvmPipelines,
    stream: Option<AdvanceStream>,
    header: (String, usize),
    /// Set when the outcome is known before / instead of streaming (a poisoned
    /// engine, or a panic caught mid-stream).
    outcome: Option<Result<AdvanceOutcome, String>>,
}

impl AdvanceChanges<'_> {
    /// TS `advance()`'s `{version, numChanges}` — the version the snapshot
    /// advanced TO and the number of changes in the diff.
    pub fn header(&self) -> (&str, usize) {
        (&self.header.0, self.header.1)
    }

    /// Run phase 3 now and return the advance outcome. Dropping the stream runs
    /// phase 3 too (outcome discarded); this makes it explicit at the call site.
    pub fn finish(mut self) -> Result<AdvanceOutcome, String> {
        if let Some(outcome) = self.outcome.take() {
            self.stream = None;
            *self.driver.advance_context.borrow_mut() = None;
            return outcome;
        }
        match self.stream.take() {
            Some(stream) => self.driver.finish_advance(stream),
            None => Err("advance already finished".to_string()),
        }
    }
}

impl Iterator for AdvanceChanges<'_> {
    type Item = StreamItem<RowChange>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.outcome.is_some() {
            return None;
        }
        let stream = self.stream.as_mut()?;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| stream.next())) {
            Ok(item) => item,
            Err(payload) => {
                // Drop the stream now (its Drop restores the sources to head —
                // the RefCell borrows a mid-push panic held were released by
                // the unwind) and hand the mapped outcome to `finish`.
                self.stream = None;
                self.outcome = Some(self.driver.advance_panic_outcome(payload));
                None
            }
        }
    }
}

impl Drop for AdvanceChanges<'_> {
    fn drop(&mut self) {
        if let Some(stream) = self.stream.take() {
            if std::thread::panicking() {
                // Let the engine stream's own Drop restore the sources; the
                // driver bookkeeping is cleared without touching the engine.
                drop(stream);
            } else {
                let _ = self.driver.finish_advance(stream);
            }
        }
        *self.driver.advance_context.borrow_mut() = None;
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn column_type(type_str: &str, optional: bool) -> ColumnType {
    match type_str {
        "boolean" => ColumnType::Boolean { optional },
        "number" => ColumnType::Number { optional },
        "json" => ColumnType::Json { optional },
        _ => ColumnType::String { optional },
    }
}

fn column_schema(v: &IvmColumnSchema) -> ColumnSchema {
    ColumnSchema {
        r#type: v.r#type.clone(),
        optional: v.optional,
    }
}

/// Map a zql column spec to the `ColumnType` the scanstatus cost model probes
/// with — same string→type mapping as the TS zqlSpec `SchemaValue.type` the
/// cost model receives via `tableSpecs` (and rust-ivm's server.rs source
/// builder; unknown types default to Number there too).
fn zql_column_type(cs: &ColumnSchema) -> ColumnType {
    let optional = cs.optional;
    match cs.r#type.as_str() {
        "boolean" => ColumnType::Boolean { optional },
        "string" => ColumnType::String { optional },
        "json" => ColumnType::Json { optional },
        "number" => ColumnType::Number { optional },
        _ => ColumnType::Number { optional },
    }
}

/// Extract a message from a caught panic payload — the twin of reading
/// `e.message` in a TS `catch`. Rust-only helper (AGENTS.md rule 5).
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .or_else(|| {
            payload
                .downcast_ref::<rust_ivm::sqlite::sqlite_cost_model::SqliteError>()
                .map(|e| e.0.clone())
        })
        .unwrap_or_else(|| "engine job panicked".to_string())
}

/// The JS value TS would have thrown where rust's hydrate panicked. The
/// cost-model probe's `db.prepare` failure is better-sqlite3's `SqliteError`
/// (sqlite-cost-model.ts:78 — rust-ivm panics with the typed
/// `sqlite_cost_model::SqliteError` payload there); every other payload (an
/// `assert`, a Take/Cap invariant) is a plain `Error`. TS's `String(e)` in
/// `ClientHandler.fail` prints that name — `SqliteError: unrecognized token…`
/// on the TS arm of a sandbox replay — so the name has to travel with the
/// message.
fn hydrate_js_error(payload: &Box<dyn std::any::Any + Send>) -> JsError {
    match payload.downcast_ref::<rust_ivm::sqlite::sqlite_cost_model::SqliteError>() {
        Some(e) => JsError::new("SqliteError", e.0.clone()),
        None => JsError::plain(panic_message(payload)),
    }
}

/// The identity a query-failure log line carries.
/// Port of TS `QueryLogInfo` (pipeline-driver.ts:110-114).
struct QueryLogInfo<'a> {
    query_hash: &'a str,
    transformation_hash: &'a str,
    query_name: Option<&'a str>,
}

/// The two messages TS ever passes to `logQueryFailure`
/// (pipeline-driver.ts:809 and :1439).
///
/// TS takes `message: string`; rust takes the closed set instead, because
/// `tracing` interns the message at the CALLSITE — a `"{message}"` template
/// would put a placeholder on the wire where TS puts the text, and
/// `parity/log_differential.py` joins rust lines to TS lines by exactly that text. The values
/// are TS's verbatim (AGENTS.md rule 5: a labeled adaptation that preserves
/// what an operator sees).
enum QueryFailureMessage {
    Hydration,
    Pipeline,
}

/// Port of TS `logQueryFailure` (pipeline-driver.ts:1451-1465).
///
/// TS logs a query failure at ERROR with the query's identity attached as
/// LogContext context, and SKIPS a `ResetPipelinesSignal` entirely — a reset is
/// an expected control-flow signal, not a failure, and logging it at ERROR
/// would page on normal operation. Rust's twin of that signal in a caught
/// panic payload is `ScalarResetError`, so the guard is the same test
/// `advance_panic_outcome` uses to classify one.
///
/// Rust had no twin for this at all: the `query-pipeline-hydrate-failed`
/// lifecycle line was ported but the ERROR line beside it was not, so a query
/// failure that pages a TS operator was invisible in rust.
///
/// TS reaches the function from three places. Two are ported below (the
/// hydrate catch, `#addQueryImpl` :806-812, and the advance push catch,
/// `QueryPipeline.push` :1436-1444). The third — `Streamer.stream`'s
/// `catch { this.#logQueryFailure?.(queryID, e); throw e; }` (:1288-1291), via
/// the private `#logQueryFailure` method at :1240 — has no separate rust site:
/// rust's streamer walk runs INSIDE the same `stream.next()` that the per-pull
/// `catch_unwind` guards, so a failure there is already reported by the
/// advance/hydrate site rather than by a second one.
fn log_query_failure(
    query_info: Option<QueryLogInfo<'_>>,
    message: QueryFailureMessage,
    error: &Box<dyn std::any::Any + Send>,
) {
    // TS: `if (error instanceof ResetPipelinesSignal) { return; }`
    if scalar_reset_message(error).is_some() {
        return;
    }
    let err = panic_message(error);
    // `None` fields are not recorded by `tracing`, which is how the
    // `queryInfo === undefined` branch (no `withContext` calls at all) comes
    // out with no identity fields rather than with null ones.
    let query_hash = query_info.as_ref().map(|i| i.query_hash);
    let transformation_hash = query_info.as_ref().map(|i| i.transformation_hash);
    let query_name = query_info.as_ref().and_then(|i| i.query_name);
    match message {
        QueryFailureMessage::Hydration => tracing::error!(
            query_hash,
            transformation_hash,
            query_name,
            error = %err,
            "query hydration failed"
        ),
        QueryFailureMessage::Pipeline => tracing::error!(
            query_hash,
            transformation_hash,
            query_name,
            error = %err,
            "query pipeline failed"
        ),
    }
}

/// If a caught advance panic is a `ScalarResetError`, return its message — the
/// twin of TS's `instanceof ResetPipelinesSignal` check; maps to an in-place
/// `scalar-subquery` reset. Rust-only helper (AGENTS.md rule 5).
fn scalar_reset_message(payload: &Box<dyn std::any::Any + Send>) -> Option<String> {
    payload
        .downcast_ref::<ScalarResetError>()
        .map(|e| e.to_string())
}

// ─── TS AST → Rust AST conversion ────────────────────────────────────────────
// The TS AST JSON uses `{ type: "..." }` internal tagging, camelCase field
// names, `[string, string]` order-by tuples, and `correlation: { parentField,
// childField }`. Carried over verbatim from the removed native bridge
// (a5e502ad9) so the syncer path deserializes transformed ASTs identically to
// the parity-tested path it replaced.

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

/// Parse a TS-shaped transformed-AST JSON string into a `rust-ivm` `Ast`.
pub fn parse_ts_ast(json: &str) -> Result<rust_ivm::builder::ast::Ast, String> {
    let ts: TsAst = serde_json::from_str(json).map_err(|e| format!("{e}"))?;
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

pub(crate) fn json_to_value(v: serde_json::Value) -> rust_ivm::ivm::data::Value {
    match v {
        serde_json::Value::Null => rust_ivm::ivm::data::Value::Null,
        serde_json::Value::Bool(b) => rust_ivm::ivm::data::Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                // IVM values are JS numbers (f64), matching TS. An integer beyond
                // the ±2^53 safe range loses precision here exactly as it would in
                // TS (`i as f64` is the same IEEE-754 round-to-nearest as JS's
                // `Number(bigint)`). Do NOT panic: this runs on client-supplied
                // query literals and int8 column values, so a large value must
                // not be able to take down the whole client-group task.
                rust_ivm::ivm::data::Value::F64(i as f64)
            } else if let Some(f) = n.as_f64() {
                rust_ivm::ivm::data::Value::F64(f)
            } else {
                rust_ivm::ivm::data::Value::Null
            }
        }
        serde_json::Value::String(s) => rust_ivm::ivm::data::Value::Str(s.into()),
        // Arrays/objects (e.g. an `IN [ids]` list literal) -> JSON string
        // (falling through to Null silently drops every
        // row of any IN / NOT IN query).
        other => rust_ivm::ivm::data::Value::Json(other.to_string().into()),
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Test stand-in for the view-syncer's `TimeSliceTimer`: a single
    /// never-yielding lap (TS tests pass a `TimeSliceTimer` too).
    struct WallTimer(std::time::Instant);

    impl Default for WallTimer {
        fn default() -> Self {
            WallTimer(std::time::Instant::now())
        }
    }

    impl Timer for WallTimer {
        fn elapsed_lap(&self) -> f64 {
            self.0.elapsed().as_secs_f64() * 1000.0
        }
        fn total_elapsed(&self) -> f64 {
            self.elapsed_lap()
        }
    }

    /// Port of TS `ResetPipelinesSignal('scalar-subquery')` classification
    /// (view-syncer.ts reset branch; rust-ivm `ScalarResetError` is its twin):
    /// only a `ScalarResetError` panic payload maps to an in-place reset
    /// (`Some(message)` — the group REHYDRATES); any other payload returns
    /// `None` and the caller fails the group instead.
    /// TS's `logQueryFailure` (pipeline-driver.ts:1451-1465) had no rust twin:
    /// the `query-pipeline-hydrate-failed` LIFECYCLE line was ported, but the
    /// ERROR line TS emits beside it was not, so a query failure that pages a
    /// TS operator produced no ERROR line at all in rust.
    ///
    /// Three properties, all of them TS's:
    ///   * the two message texts are verbatim, so an operator's alert on
    ///     `query hydration failed` / `query pipeline failed` fires the same;
    ///   * the query identity rides along as context (TS's three
    ///     `withContext` calls), and is ABSENT — not null — on the
    ///     `queryInfo === undefined` branch;
    ///   * a `ResetPipelinesSignal` is SKIPPED. A reset is expected control
    ///     flow, and logging it at ERROR would page on normal operation. Rust's
    ///     twin in a caught payload is `ScalarResetError`.
    ///
    /// Mutation test: drop the `scalar_reset_message` guard from
    /// `log_query_failure` and the reset case logs (third assertion fails);
    /// drop the identity fields and the first fails; change either message
    /// string and `parity/log_differential.py` unpairs it from its TS twin.
    ///
    /// Safe as a lib test — these two ERROR callsites are unique to
    /// `log_query_failure`, so the process-global callsite-interest cache
    /// cannot be poisoned by another test's subscriber (same reasoning as
    /// `log_vended_row_counts_emits_per_table_and_grand_total`).
    #[test]
    fn log_query_failure_carries_the_ts_text_and_skips_a_reset_signal() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        struct BufGuard(Arc<Mutex<Vec<u8>>>);
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
            type Writer = BufGuard;
            fn make_writer(&'a self) -> BufGuard {
                BufGuard(self.0.clone())
            }
        }
        impl std::io::Write for BufGuard {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let capture = |f: &dyn Fn()| -> String {
            let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
            let subscriber = tracing_subscriber::fmt()
                .with_writer(BufWriter(buf.clone()))
                .with_ansi(false)
                .with_max_level(tracing::Level::ERROR)
                .finish();
            crate::ensure_permissive_global_subscriber();
            tracing::subscriber::with_default(subscriber, f);
            String::from_utf8(buf.lock().unwrap().clone()).unwrap()
        };

        let plain: Box<dyn std::any::Any + Send> = Box::new("source drift".to_string());

        // 1. Hydration failure WITH identity — TS `#addQueryImpl`'s catch.
        let logged = capture(&|| {
            log_query_failure(
                Some(QueryLogInfo {
                    query_hash: "qhash1",
                    transformation_hash: "thash1",
                    query_name: Some("issueList"),
                }),
                QueryFailureMessage::Hydration,
                &plain,
            )
        });
        assert!(
            logged.contains("query hydration failed"),
            "TS's message verbatim; got: {logged}"
        );
        for field in ["qhash1", "thash1", "issueList", "source drift"] {
            assert!(
                logged.contains(field),
                "the query identity and the error must ride along ({field}); got: {logged}"
            );
        }

        // 2. Pipeline failure with NO identity — TS's `queryInfo === undefined`
        //    branch makes no `withContext` calls at all, so the fields are
        //    absent rather than null.
        let logged = capture(&|| log_query_failure(None, QueryFailureMessage::Pipeline, &plain));
        assert!(
            logged.contains("query pipeline failed"),
            "TS's message verbatim; got: {logged}"
        );
        assert!(
            !logged.contains("query_hash") && !logged.contains("transformation_hash"),
            "an unattributable failure must omit the identity fields, not log them \
             as null; got: {logged}"
        );

        // 3. A reset signal must produce NOTHING.
        let reset: Box<dyn std::any::Any + Send> = Box::new(ScalarResetError {
            table: "issue".to_string(),
            resolved: "1".to_string(),
            new: "2".to_string(),
        });
        let logged = capture(&|| {
            log_query_failure(
                Some(QueryLogInfo {
                    query_hash: "qhash1",
                    transformation_hash: "thash1",
                    query_name: None,
                }),
                QueryFailureMessage::Hydration,
                &reset,
            )
        });
        assert!(
            logged.is_empty(),
            "a ResetPipelinesSignal is expected control flow — TS returns before \
             logging (pipeline-driver.ts:1457-1459); got: {logged}"
        );
    }

    #[test]
    fn scalar_reset_message_classifies_only_scalar_reset_panics() {
        let reset: Box<dyn std::any::Any + Send> = Box::new(ScalarResetError {
            table: "issue".to_string(),
            resolved: "1".to_string(),
            new: "2".to_string(),
        });
        // Message mirrors the TS signal text (rust-ivm engine/mod.rs Display).
        assert_eq!(
            scalar_reset_message(&reset).as_deref(),
            Some("Scalar subquery value changed for issue: 1 -> 2")
        );

        // A non-reset panic (assert message) must NOT classify as a reset.
        let plain: Box<dyn std::any::Any + Send> = Box::new("source drift".to_string());
        assert_eq!(scalar_reset_message(&plain), None);
        let strpanic: Box<dyn std::any::Any + Send> = Box::new("boom");
        assert_eq!(scalar_reset_message(&strpanic), None);
    }

    /// `panic_message` extracts `&str` and `String` panic
    /// payloads verbatim; any other payload type falls back to the fixed
    /// "engine job panicked" string (the message the advance error embeds).
    #[test]
    fn panic_message_extracts_str_string_and_falls_back() {
        let s: Box<dyn std::any::Any + Send> = Box::new("assertion failed: rows");
        assert_eq!(panic_message(&s), "assertion failed: rows");
        let owned: Box<dyn std::any::Any + Send> = Box::new("owned panic".to_string());
        assert_eq!(panic_message(&owned), "owned panic");
        let opaque: Box<dyn std::any::Any + Send> = Box::new(42_u32);
        assert_eq!(panic_message(&opaque), "engine job panicked");
    }

    #[test]
    fn json_to_value_out_of_safe_range_int_coerces_not_panics() {
        use rust_ivm::ivm::data::Value;
        // 2^53 + 1 and its negative — beyond the JS safe-integer range. Must
        // coerce to f64 (matching TS/JS), never panic.
        for i in [9_007_199_254_740_993_i64, -9_007_199_254_740_993_i64] {
            match json_to_value(serde_json::json!(i)) {
                Value::F64(f) => assert_eq!(f, i as f64),
                _ => panic!("expected F64 for {i}"),
            }
        }
        // In-range integers still round-trip through f64 as before.
        assert!(matches!(
            json_to_value(serde_json::json!(42)),
            Value::F64(f) if f == 42.0
        ));
    }

    fn users_spec() -> IvmTableSpec {
        IvmTableSpec {
            table: "users".to_string(),
            column_order: Vec::new(),
            columns: HashMap::from([
                (
                    "id".to_string(),
                    IvmColumnSchema {
                        r#type: "string".to_string(),
                        optional: false,
                    },
                ),
                (
                    "name".to_string(),
                    IvmColumnSchema {
                        r#type: "string".to_string(),
                        optional: true,
                    },
                ),
            ]),
            primary_key: vec!["id".to_string()],
            unique_keys: None,
            all_potential_primary_keys: vec![vec!["id".to_string()]],
            min_row_version: None,
        }
    }

    #[test]
    fn parse_ts_ast_order_by() {
        let ast = parse_ts_ast(r#"{"table":"users","orderBy":[["id","asc"]]}"#).unwrap();
        assert_eq!(ast.table, "users");
        let ob = ast.order_by.as_ref().unwrap();
        assert_eq!(ob.len(), 1);
        assert_eq!(ob[0].column, "id");
        assert_eq!(ob[0].direction, "asc");
    }

    #[test]
    fn parse_ts_ast_where_and_related() {
        let json = r#"{
            "table":"issue",
            "where":{"type":"simple","op":"=",
                "left":{"type":"column","name":"open"},
                "right":{"type":"literal","value":true}},
            "related":[{
                "correlation":{"parentField":["id"],"childField":["issueId"]},
                "subquery":{"table":"comment","alias":"comments"}
            }]
        }"#;
        let ast = parse_ts_ast(json).unwrap();
        assert_eq!(ast.table, "issue");
        assert!(ast.where_clause.is_some());
        assert_eq!(ast.related.len(), 1);
        assert_eq!(ast.related[0].relationship_name, "comments");
        assert_eq!(ast.related[0].parent_key, vec!["id".to_string()]);
        assert_eq!(ast.related[0].child_key, vec!["issueId".to_string()]);
    }

    #[test]
    fn init_and_hydrate_empty_memory_source() {
        let mut p = IvmPipelines::new();
        p.init(vec![users_spec()], None, "zero").unwrap();
        assert!(p.initialized());

        let mut count = 0usize;
        {
            let mut changes = p
                .hydrate(
                    &[("q1".to_string(), r#"{"table":"users"}"#.to_string())],
                    Rc::new(WallTimer::default()),
                )
                .unwrap();
            for item in changes.by_ref() {
                if matches!(item, StreamItem::Data(_)) {
                    count += 1;
                }
            }
            changes.finish().unwrap();
        }
        // Empty in-memory source → no rows streamed.
        assert_eq!(count, 0);

        // get_row on an empty source returns None.
        assert!(
            p.get_row("users", &[("id".to_string(), Value::Str("x".into()))])
                .is_none()
        );

        p.destroy();
        assert!(!p.initialized());
    }

    #[test]
    fn hydrate_before_init_errors() {
        let mut p = IvmPipelines::new();
        let err = p
            .hydrate(
                &[("q1".to_string(), r#"{"table":"users"}"#.to_string())],
                Rc::new(WallTimer::default()),
            )
            .err()
            .expect("hydrate before init must fail");
        assert!(err.message.contains("Engine not initialized"));
    }

    // The per-query hydrate lifecycle log (TS `#logQueryPipelineLifecycle`) is
    // exercised end-to-end in `tests/hydrate_lifecycle_log_test.rs`. That test
    // captures `tracing` output, so it lives in its OWN integration-test binary:
    // `tracing`'s callsite-interest cache is process-global, and other in-process
    // lib tests installing their own subscribers would poison it (the 2-field
    // `-start` callsite got cached disabled by an unrelated test).

    /// Mutation test: the `VENDED` per-table debug log (port of TS `#addQueryImpl`
    /// pipeline-driver.ts:704-721) must, for a slow query, emit a `<table>
    /// VENDED` line per table AND a `Total rows considered: <sum>` line whose
    /// value is the grand total across every table+SQL. The synthetic counts
    /// below total 204 (200 + 4), so a dropped table, a missing line, or a
    /// wrong sum (e.g. per-table instead of grand-total) all fail distinctly.
    ///
    /// TS builds the VENDED table list INSIDE the flag check
    /// (`if (runtimeDebugFlags.trackRowCountsVended)` then
    /// `for (const tableName of this.#tables.keys())`, pipeline-driver.ts:
    /// 704-710), so a production process with the flag off does no work for
    /// this diagnostic at all. Rust must collect before `finish_hydrate` takes
    /// its `&mut engine` borrow, so the flag is read in `vended_table_names`
    /// instead — with ~150 sources the ungated version was 150 `String` clones
    /// plus a sort on every hydrate.
    ///
    /// This pins BOTH directions, including through the real `finish_hydrate`
    /// path: flag off => no list and no VENDED line; flag on => the full
    /// sorted list and the line still fires.
    ///
    /// Mutation test: drop the early return from `vended_table_names` and the
    /// flag-off assertions fail (the list is populated, and the line appears);
    /// make it return `Vec::new()` unconditionally and the flag-on assertions
    /// fail.
    #[test]
    fn vended_table_names_is_built_only_when_the_flag_can_use_it() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        struct BufGuard(Arc<Mutex<Vec<u8>>>);
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
            type Writer = BufGuard;
            fn make_writer(&'a self) -> BufGuard {
                BufGuard(self.0.clone())
            }
        }
        impl std::io::Write for BufGuard {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        // Every hydrate is "slow" so the threshold arm never masks the flag arm.
        super::super::view_syncer::set_slow_hydrate_threshold_for_test(Some(-1.0));
        let flags = runtime_debug_flags();
        let prev = flags.track_row_counts_vended();

        // Runs one hydrate to completion (so `finish_hydrate` fires) and
        // returns whatever it logged.
        let hydrate_capturing = || -> String {
            let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
            let subscriber = tracing_subscriber::fmt()
                .with_writer(BufWriter(buf.clone()))
                .with_ansi(false)
                .with_max_level(tracing::Level::INFO)
                .finish();
            crate::ensure_permissive_global_subscriber();
            tracing::subscriber::with_default(subscriber, || {
                let mut p = IvmPipelines::new();
                p.init(vec![users_spec()], None, "zero").unwrap();
                let mut changes = p
                    .hydrate(
                        &[("q1".to_string(), r#"{"table":"users"}"#.to_string())],
                        Rc::new(WallTimer::default()),
                    )
                    .unwrap();
                for _ in changes.by_ref() {}
                changes.finish().unwrap();
                p.destroy();
            });
            String::from_utf8(buf.lock().unwrap().clone()).unwrap()
        };

        // --- flag OFF: no list, no line ---
        flags.set_track_row_counts_vended(false);
        {
            let mut p = IvmPipelines::new();
            p.init(vec![users_spec()], None, "zero").unwrap();
            assert!(
                p.vended_table_names().is_empty(),
                "with the diagnostic off, the table list must not be built"
            );
            p.destroy();
        }
        let logged_off = hydrate_capturing();
        assert!(
            !logged_off.contains("VENDED"),
            "with the flag off no VENDED line may be emitted; got: {logged_off}"
        );

        // --- flag ON: full sorted list, and the line still fires ---
        flags.set_track_row_counts_vended(true);
        {
            let mut p = IvmPipelines::new();
            p.init(vec![users_spec()], None, "zero").unwrap();
            assert_eq!(
                p.vended_table_names(),
                vec!["users".to_string()],
                "with the diagnostic on, every source table must be listed"
            );
            p.destroy();
        }
        let logged_on = hydrate_capturing();

        flags.set_track_row_counts_vended(prev);
        super::super::view_syncer::set_slow_hydrate_threshold_for_test(None);

        assert!(
            logged_on.contains("users VENDED"),
            "gating the list must not stop the VENDED line from firing when \
             the flag is on; got: {logged_on}"
        );
    }

    /// Unlike the shared `query pipeline lifecycle` callsite (moved to its own
    /// integration binary), the `VENDED` / `Total rows considered` callsites are
    /// UNIQUE to `log_vended_row_counts` and exercised only by this test, so the
    /// process-global callsite-interest cache cannot be poisoned by another
    /// test's subscriber. `capture_vended` scopes its own subscriber.
    #[test]
    fn log_vended_row_counts_emits_per_table_and_grand_total() {
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct BufWriter(Arc<Mutex<Vec<u8>>>);
        struct BufGuard(Arc<Mutex<Vec<u8>>>);
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
            type Writer = BufGuard;
            fn make_writer(&'a self) -> BufGuard {
                BufGuard(self.0.clone())
            }
        }
        impl std::io::Write for BufGuard {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut counts: RowCountsBySource = HashMap::new();
        counts.insert(
            "issue".to_string(),
            HashMap::from([("SELECT * FROM issue WHERE open=?".to_string(), 200u64)]),
        );
        counts.insert(
            "comment".to_string(),
            HashMap::from([("SELECT * FROM comment".to_string(), 4u64)]),
        );

        let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_writer(BufWriter(buf.clone()))
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish();
        // `users` has NO vended rows: TS still logs `users VENDED: []` because it
        // iterates `#tables.keys()`, so the table list drives the loop.
        let tables = [
            "comment".to_string(),
            "issue".to_string(),
            "users".to_string(),
        ];
        crate::ensure_permissive_global_subscriber();
        tracing::subscriber::with_default(subscriber, || {
            IvmPipelines::log_vended_row_counts("q1", 1234.0, Some(&counts), &tables);
        });

        let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            logged.contains("issue VENDED"),
            "per-table VENDED line for `issue`; got: {logged}"
        );
        // The regression this pins: a table with no vended rows must STILL log a
        // VENDED line (TS loops `#tables.keys()`, not the vended map's keys).
        assert!(
            logged.contains("users VENDED"),
            "a table that vended nothing still logs a VENDED line; got: {logged}"
        );
        assert!(
            logged.contains("comment VENDED"),
            "per-table VENDED line for `comment`; got: {logged}"
        );
        assert!(
            logged.contains("Total rows considered: 204"),
            "grand total across all tables (200 + 4); got: {logged}"
        );
    }
}
