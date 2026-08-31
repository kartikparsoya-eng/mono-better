//! IvmPipelines — the pure-Rust IVM bridge (Stage A of the Phase 7 wiring).
//!
//! Port of the engine-side of `pipeline-driver.ts` (behavior) and of the
//! `EngineState` construction/hydrate/advance logic in `rust-ivm/napi/src/lib.rs`
//! (the parity-tested Rust integration), with all napi / TSFN / actor-thread
//! machinery stripped out. This struct is owned by the ViewSyncer and lives on
//! its dedicated CG thread — it is intentionally NOT `Send`/`Sync` because the
//! `rust-ivm` `Engine` holds `Rc<RefCell<..>>` sources.
//!
//! Responsibilities (rust-ivm only):
//!   - open the SQLite replica via a `Snapshotter`
//!   - build `TableSource`s and hydrate query ASTs (streaming `RowChange`s)
//!   - advance the replica to head, streaming `RowChange`s (with reset/panic
//!     handling that matches the napi/TS lifecycle)
//!   - `get_row` for catchup, `row_set_signature` passthrough
//!
//! The CVR combination (feeding these `RowChange`s into `rust-cvr`'s
//! `ChangeProcessor` / `CVRQueryDrivenUpdater` / pokers) is Stage B and lives in
//! the ViewSyncer, exactly as `view-syncer.ts` owns it in TS.
//!
//! See `packages/zero-cache/docs/rust-cvr-port/90-phase7-real-wiring-plan.md`.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use rust_ivm::builder::debug_delegate::{RowCountsBySource, SharedDebug, runtime_debug_flags};
use rust_ivm::engine::{Engine, QuerySpec, ScalarResetError};
use rust_ivm::ivm::change::ChangeType;
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::Source;
use rust_ivm::snapshotter::spec::{ColumnSchema, LiteAndZqlSpec, TableSpec};
use rust_ivm::snapshotter::{SharedConn, Snapshotter};
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

// ─── Input specs (mirrors napi's NapiTableSpec / NapiColumnSchema) ───────────

/// Column schema for a syncable table. `type` is one of
/// `"string" | "number" | "boolean" | "json"` (anything else is treated as
/// `string`, matching the napi/TS mapping).
#[derive(Clone, Debug)]
pub struct IvmColumnSchema {
    pub r#type: String,
    pub optional: bool,
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

/// The engine + snapshotter + sources for a single client group, mirroring the
/// napi `EngineState`.
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
///      handle (~11.5MB page cache + fds per CG churn; ART G6).
///
/// Port of TS `QueryPipelineLifecycleLog` (pipeline-driver.ts:133) — the record
/// `#logQueryPipelineLifecycle` formats. `pipelineRunID` / `transformationHash` /
/// `queryName` / `hydrationReason` are omitted: they are not available at this
/// rust layer (they live in the view-syncer or are set after hydrate), and TS
/// itself drops each from the log line when it is `undefined`, so their absence
/// is protocol-faithful. `zero_event` is always a fixed literal; `stop_reason`
/// only present for the `query-pipeline-stop` event.
#[derive(Default)]
struct QueryPipelineLifecycleLog {
    zero_event: &'static str,
    query_hash: String,
    hydration_time_ms: Option<f64>,
    hydration_row_count: Option<u64>,
    stop_reason: Option<&'static str>,
    pipeline_lifetime_ms: Option<f64>,
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
    /// Set when a non-scalar panic was caught mid-advance; forces the next
    /// advance to emit a reset instead of running on a half-mutated graph.
    poisoned: bool,
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
            poisoned: false,
            snapshotter: None,
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
    /// Port of `EngineState`/`init` in `rust-ivm/napi/src/lib.rs`.
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
        // snapshotter failure (matches napi/TS).
        let snapshot_conn = if let Some(path) = db_path {
            if self.snapshotter.is_none() {
                let mut snap = Snapshotter::new(path, app_id, None);
                snap.init().map_err(|e| format!("snapshotter init: {e}"))?;
                self.snapshotter = Some(snap);
            }
            Some(
                self.snapshotter
                    .as_ref()
                    .unwrap()
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
        // over-emits WHERE-EXISTS backing rows to the CVR (ART G8). Only the
        // replica-backed (TableSource) path gets a cost model; MemorySource
        // fallbacks (some tests) stay unplanned. Gated on `enable_query_planner`
        // exactly like TS (`#costModels = enablePlanner ? new WeakMap() :
        // undefined`, pipeline-driver.ts:315 → costModel undefined → no
        // planQuery).
        if let Some(conn) = &source_conn
            && self.enable_query_planner
        {
            {
                eng.set_cost_model_conn(conn.clone());
                // TS `createSQLiteCostModel(db, this.#tableSpecs)`
                // (pipeline-driver.ts:436): the scanstatus model probes with the
                // visible zql columns of every syncable table. Without specs the
                // engine degrades (loudly) to the filter-blind COUNT model —
                // the exact wiring gap behind the 2026-08-29 prod 144s
                // flipped-join tickets hydrate.
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
    /// replaced by the global `tracing` subscriber. `pipelineRunID` /
    /// `transformationHash` / `queryName` / `hydrationReason` are not available at
    /// this layer (they live one layer up in the view-syncer, or are set after
    /// hydrate) and TS itself omits each from the log line when it is undefined —
    /// so their absence here is protocol-faithful, not a divergence.
    fn log_query_pipeline_lifecycle(log: QueryPipelineLifecycleLog) {
        let QueryPipelineLifecycleLog {
            zero_event,
            query_hash,
            hydration_time_ms,
            hydration_row_count,
            stop_reason,
            pipeline_lifetime_ms,
        } = log;
        // tracing fields cannot be conditionally omitted within a single macro
        // call, so branch on the event shape: `-stop` (all fields), `-finish`
        // (timing + rows), and `-start`/`-failed`/`-aborted` (hash only).
        match (
            stop_reason,
            hydration_time_ms,
            hydration_row_count,
            pipeline_lifetime_ms,
        ) {
            (Some(sr), Some(t), Some(n), Some(lt)) => tracing::info!(
                zero_event,
                query_hash,
                stop_reason = sr,
                hydration_time_ms = t,
                hydration_row_count = n,
                pipeline_lifetime_ms = lt,
                "query pipeline lifecycle"
            ),
            (None, Some(t), Some(n), _) => tracing::info!(
                zero_event,
                query_hash,
                hydration_time_ms = t,
                hydration_row_count = n,
                "query pipeline lifecycle"
            ),
            _ => tracing::info!(zero_event, query_hash, "query pipeline lifecycle"),
        }
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
    /// Tables are sorted so the log is deterministic (TS relies on `#tables`
    /// insertion order; rust's `HashMap` is unordered).
    fn log_vended_row_counts(
        query_id: &str,
        hydration_time_ms: f64,
        vended: Option<&RowCountsBySource>,
    ) {
        let mut total_rows_considered: u64 = 0;
        if let Some(counts) = vended {
            let mut tables: Vec<&String> = counts.keys().collect();
            tables.sort();
            for table_name in tables {
                let by_query = &counts[table_name];
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
                zero_event: "query-pipeline-stop",
                query_hash: query_id.to_string(),
                hydration_time_ms: Some(t),
                hydration_row_count: Some(n),
                stop_reason: Some(stop_reason),
                pipeline_lifetime_ms: Some(lt),
            });
        }
        if let Some(eng) = self.engine.as_mut() {
            eng.remove_query(query_id);
        }
    }

    /// Hydrate the given queries against the current snapshot, streaming each
    /// `RowChange` to `on_row` as it is produced. `queries` is a slice of
    /// `(query_id, ast_json)` where `ast_json` is the TS-shaped transformed AST.
    ///
    /// Port of `HydrateTask::compute` / the hydrate half of `HydrateAndSyncTask`.
    /// Row-set-signature maintenance is intentionally NOT done here — it is
    /// caller-driven (Stage B / view-syncer), matching the napi path.
    pub fn hydrate<F: FnMut(&RowChange)>(
        &mut self,
        queries: &[(String, String)],
        mut on_row: F,
    ) -> Result<(), String> {
        // Rehydrate rebuilds pipelines fresh, so any poison is cleared.
        self.poisoned = false;
        let eng = self
            .engine
            .as_mut()
            .ok_or_else(|| "Engine not initialized".to_string())?;

        let mut specs: Vec<QuerySpec> = Vec::with_capacity(queries.len());
        for (query_id, ast_json) in queries {
            let ast = parse_ts_ast(ast_json)
                .map_err(|e| format!("AST parse error for qid={query_id}: {e}"))?;
            specs.push(QuerySpec {
                query_id: query_id.clone(),
                ast,
            });
        }

        // Per-query hydrate lifecycle logging — port of TS
        // `#logQueryPipelineLifecycle` (pipeline-driver.ts:470/608/784/796/815).
        // TS wraps each `addQuery` in a start/finish/failed/aborted envelope; Rust
        // hydrates the whole query set in ONE `add_queries_streaming` call (the
        // documented !Send batching invention), so the per-query boundaries come
        // from the returned `QueryResult`s: `-start` before the batch, `-finish`
        // (with timing + row count) for each pipeline the engine registered,
        // `-aborted` for a started-but-unregistered query (cancel-during-hydrate),
        // and `-failed` on a hydrate panic. This is the always-on analog of TS
        // `VENDED` (which is gated behind the `trackRowCountsVended` debug flag) —
        // it makes a slow/heavy query identifiable from logs by time + rows.
        for (query_id, _ast_json) in queries {
            Self::log_query_pipeline_lifecycle(QueryPipelineLifecycleLog {
                zero_event: "query-pipeline-hydrate-start",
                query_hash: query_id.clone(),
                ..Default::default()
            });
        }

        // A hydrate panic (e.g. a source-drift assert) must roll back the
        // partially-wired source connections before re-throwing, so a follow-up
        // rehydrate builds a clean graph — matching napi `HydrateAndSyncTask`.
        let checkpoint = eng.source_connection_checkpoint();
        let hydrated = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            eng.add_queries_streaming(&specs, |rc| on_row(rc))
        }));
        let results = match hydrated {
            Ok(results) => results,
            Err(payload) => {
                eng.rollback_source_connections(&checkpoint);
                for (query_id, _ast_json) in queries {
                    Self::log_query_pipeline_lifecycle(QueryPipelineLifecycleLog {
                        zero_event: "query-pipeline-hydrate-failed",
                        query_hash: query_id.clone(),
                        ..Default::default()
                    });
                }
                std::panic::resume_unwind(payload);
            }
        };
        let finished: HashSet<&str> = results.iter().map(|r| r.query_id.as_str()).collect();
        for r in &results {
            Self::log_query_pipeline_lifecycle(QueryPipelineLifecycleLog {
                zero_event: "query-pipeline-hydrate-finish",
                query_hash: r.query_id.clone(),
                hydration_time_ms: Some(r.hydration_time_ms),
                hydration_row_count: Some(r.hydration_row_count),
                ..Default::default()
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
                );
            }
        }
        // A query that started but the engine never registered was aborted
        // mid-stream (cancel-during-hydrate → engine discards partial pipelines
        // and returns no result for it).
        for (query_id, _ast_json) in queries {
            if !finished.contains(query_id.as_str()) {
                Self::log_query_pipeline_lifecycle(QueryPipelineLifecycleLog {
                    zero_event: "query-pipeline-hydrate-aborted",
                    query_hash: query_id.clone(),
                    ..Default::default()
                });
            }
        }
        // Track the newly-hydrated queries so `has_query` reports them. The
        // transformation hash is recorded by the caller (`hydrate_and_sync`)
        // right after this returns, via `set_query_transformation_hash`; entries
        // hydrated directly (tests) keep an empty-string placeholder hash.
        for (query_id, ast_json) in queries {
            self.active_queries.entry(query_id.clone()).or_default();
            if self
                .query_asts
                .insert(query_id.clone(), std::sync::Arc::from(ast_json.as_str()))
                .is_none()
            {
                self.query_order.push(query_id.clone());
            }
        }
        Ok(())
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

    /// Advance the replica to head, streaming each `RowChange` to `on_row` and
    /// invoking `on_header(version, num_changes)` once before the rows.
    ///
    /// Port of `AdvanceTask::compute`: an engine panic is caught so it cannot
    /// cross into the process and abort every CG. A `ScalarResetError` panic
    /// maps to an in-place `Reset` (rehydrate at curr); any other panic poisons
    /// the engine and surfaces as `Err` (TS teardown parity — the caller tears
    /// down and the client reconnects). A `reset_reason` reported by the engine
    /// also maps to `Reset`.
    pub fn advance<H, F>(
        &mut self,
        mut on_header: H,
        mut on_row: F,
    ) -> Result<AdvanceOutcome, String>
    where
        H: FnMut(&str, usize),
        F: FnMut(&RowChange),
    {
        if self.poisoned {
            self.poisoned = false;
            return Ok(AdvanceOutcome::Reset {
                reason: "schema-change".to_string(),
                msg: "engine reset after a prior advance panic; rehydrating".to_string(),
            });
        }

        let syncable_tables = self.syncable_tables.clone();
        let all_table_names = self.all_table_names.clone();
        let mut eng = self
            .engine
            .take()
            .ok_or_else(|| "Engine not initialized".to_string())?;
        let mut snapshotter = match self.snapshotter.take() {
            Some(s) => s,
            None => {
                self.engine = Some(eng);
                return Err("Snapshotter not initialized".to_string());
            }
        };

        let advance = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            eng.advance_to_head_stream(
                &mut snapshotter,
                &syncable_tables,
                &all_table_names,
                |version, num_changes| on_header(version, num_changes),
                |rc| on_row(rc),
            )
        }));

        // Restore engine + snapshotter on every path so a follow-up
        // reset()/rehydrate can run.
        self.engine = Some(eng);
        self.snapshotter = Some(snapshotter);

        match advance {
            Ok(Ok(result)) => {
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
            Ok(Err(e)) => Err(format!("advance failed: {e}")),
            Err(payload) => {
                if let Some(msg) = scalar_reset_message(&payload) {
                    Ok(AdvanceOutcome::Reset {
                        reason: "scalar-subquery".to_string(),
                        msg,
                    })
                } else {
                    // Engine panic (e.g. a source-drift assert): mark poisoned
                    // and surface as a thrown error (TS teardown parity).
                    self.poisoned = true;
                    Err(format!("engine advance panic: {}", panic_message(&payload)))
                }
            }
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

/// Extract a message from a caught panic payload. Port of napi `panic_message`.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "engine job panicked".to_string())
}

/// If a caught advance panic is a `ScalarResetError`, return its message. Port
/// of napi `scalar_reset_message` — maps to an in-place `scalar-subquery` reset.
fn scalar_reset_message(payload: &Box<dyn std::any::Any + Send>) -> Option<String> {
    payload
        .downcast_ref::<ScalarResetError>()
        .map(|e| e.to_string())
}

// ─── TS AST → Rust AST conversion ────────────────────────────────────────────
// The TS AST JSON uses `{ type: "..." }` internal tagging, camelCase field
// names, `[string, string]` order-by tuples, and `correlation: { parentField,
// childField }`. Ported verbatim from `rust-ivm/napi/src/lib.rs` so the syncer
// path deserializes transformed ASTs identically to the parity-tested napi path.

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
        // Arrays/objects (e.g. an `IN [ids]` list literal) -> JSON string,
        // matching the napi path (falling through to Null silently drops every
        // row of any IN / NOT IN query).
        other => rust_ivm::ivm::data::Value::Json(other.to_string().into()),
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Port of TS `ResetPipelinesSignal('scalar-subquery')` classification
    /// (view-syncer.ts reset branch; rust-ivm `ScalarResetError` is its twin):
    /// only a `ScalarResetError` panic payload maps to an in-place reset
    /// (`Some(message)` — the group REHYDRATES); any other payload returns
    /// `None` and the caller fails the group instead.
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

    /// Port of napi `panic_message`: extracts `&str` and `String` panic
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
        p.hydrate(
            &[("q1".to_string(), r#"{"table":"users"}"#.to_string())],
            |_rc| {
                count += 1;
            },
        )
        .unwrap();
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
                |_| {},
            )
            .unwrap_err();
        assert!(err.contains("Engine not initialized"));
    }

    // The per-query hydrate lifecycle log (TS `#logQueryPipelineLifecycle`) is
    // exercised end-to-end in `tests/hydrate_lifecycle_log_test.rs`. That test
    // captures `tracing` output, so it lives in its OWN integration-test binary:
    // `tracing`'s callsite-interest cache is process-global, and other in-process
    // lib tests installing their own subscribers would poison it (the 2-field
    // `-start` callsite got cached disabled by an unrelated test).

    /// Non-vacuous: the `VENDED` per-table debug log (port of TS `#addQueryImpl`
    /// pipeline-driver.ts:704-721) must, for a slow query, emit a `<table>
    /// VENDED` line per table AND a `Total rows considered: <sum>` line whose
    /// value is the grand total across every table+SQL. The synthetic counts
    /// below total 204 (200 + 4), so a dropped table, a missing line, or a
    /// wrong sum (e.g. per-table instead of grand-total) all fail distinctly.
    ///
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
        tracing::subscriber::with_default(subscriber, || {
            IvmPipelines::log_vended_row_counts("q1", 1234.0, Some(&counts));
        });

        let logged = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            logged.contains("issue VENDED"),
            "per-table VENDED line for `issue`; got: {logged}"
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
