//! Engine — per-clientGroup IVM engine.
//!
//! Port of `pipeline-driver.ts` PipelineDriver class (main branch).
//! Manages sources, builds pipelines, hydrates queries, processes
//! incremental advances, tracks row-set signatures, and supports
//! advance abort (economic circuit breaker).
//!
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use crate::builder::ast::Ast;
use crate::builder::builder::{BuilderDelegate, build_pipeline};
use crate::ivm::change::Change;
use crate::ivm::data::{Row, Value};
use crate::ivm::memory_source::CollectOutput;
use crate::ivm::operator::{Input, InputBase, Output, OutputHandle, Shared, Storage};
use crate::ivm::schema::SourceSchema;
use crate::ivm::source::{Source, SourceChange};
use crate::query::complete_ordering::complete_ordering;
use crate::streamer::{RowChange, Streamer, TableSpecInfo};

/// Floor capacity retained by a per-push collector buffer after `clear()`.
/// Keeps normal reuse capacity (no per-push reallocation) while reclaiming the
/// memory a one-off giant batch would otherwise pin for the engine's lifetime.
const COLLECTOR_CAP_FLOOR: usize = 1024;

/// Clear a per-push collector buffer, reclaiming pathologically large capacity.
/// `Vec::clear` retains capacity, so a single huge advance would keep that
/// allocation alive forever; shrink back to `COLLECTOR_CAP_FLOOR` once past it.
fn clear_and_cap<T>(v: &mut Vec<T>) {
    v.clear();
    if v.capacity() > COLLECTOR_CAP_FLOOR {
        v.shrink_to(COLLECTOR_CAP_FLOOR);
    }
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

pub struct QuerySpec {
    pub query_id: String,
    pub ast: Ast,
}

pub struct QueryResult {
    pub query_id: String,
    pub changes: Vec<RowChange>,
    /// Wall-clock hydration time for this query's pipeline (Phase 2 fetch +
    /// materialize). Port of the TS pipeline `hydrationTimeMs` field
    /// (pipeline-driver.ts:773) so the driver can emit the per-query
    /// `#logQueryPipelineLifecycle` line.
    pub hydration_time_ms: f64,
    /// Number of `RowChange`s produced by this query's initial hydration
    /// (main rows + scalar-subquery companion rows). Port of the TS pipeline
    /// `hydrationRowCount` field (pipeline-driver.ts:774).
    pub hydration_row_count: u64,
}

/// Per-query build products carried across the three hydrate phases (build,
/// hydrate, register).
pub(crate) struct Built {
    pub query_id: String,
    pub transformed_ast: Ast,
    pub pipeline: Shared<dyn Input>,
    pub collector: Shared<CollectOutput>,
    pub schema: SourceSchema,
    pub timer: Instant,
    pub companion_rows: Vec<(String, Vec<String>, Row)>,
    pub companions: Vec<CompanionBuilt>,
}

/// A registered pipeline: the pipeline input + its push collector + schema.
struct PipelineEntry {
    pipeline: Shared<dyn Input>,
    collector: Shared<CollectOutput>,
    query_id: String,
    hydration_time_ms: f64,
    /// Rows produced by this query's initial hydration. Port of the TS pipeline
    /// `hydrationRowCount` field (pipeline-driver.ts:774).
    hydration_row_count: u64,
    /// When this pipeline finished hydrating and was registered. Port of the TS
    /// pipeline `pipelineReadyAtMs` field (pipeline-driver.ts:770/777); drives
    /// the `pipelineLifetimeMs` in the `query-pipeline-stop` log. (Rust uses
    /// `Instant` where TS uses `Date.now()`.)
    pipeline_ready_at: Instant,
    transformed_ast: Ast,
    /// Live companion pipelines monitoring resolved scalar subqueries for this
    /// query (empty for queries with no scalar subqueries).
    companions: Vec<CompanionPipeline>,
}

// ---------------------------------------------------------------------------
// Row-set signature tracking (XOR of row hashes per query)
// Port of TS `#rowSetSignatures` + `#trackRowSetSignatures`.
// ---------------------------------------------------------------------------

/// Compute the signature unit for a row key (XOR into the query's signature).
/// Port of TS `rowIDSignatureUnit` (`services/view-syncer/row-set-signature.ts`):
/// `rowIDSignatureUnit(id) = h64(rowIDString(id))` with `id = {schema: '', table,
/// rowKey}` (the exact shape pipeline-driver.ts:890 folds in).
///
/// This MUST be byte-identical to TS: the `rowSetSignature` (XOR-fold of these
/// units) is PERSISTED to the shared CVR (`cvr.queries[queryID].rowSetSignature`)
/// and later compared against a freshly-computed value to detect row-set drift
/// (view-syncer.ts:1659-1669). A non-TS hash (the previous `FxHasher` over
/// `format!("{:?}", v)`) would mismatch any signature written by a TS process →
/// forced re-hydration of every query on a rolling / mixed / shadow deploy. So
/// we reuse the ported xxHash-based `h64` over the ported canonical
/// `row_id_string` — the same two functions TS composes.
pub fn row_signature_unit(table: &str, row_key: &Row) -> u64 {
    // Build the `RowID {schema: "", table, rowKey}` TS hashes. `row_id_string`
    // re-sorts keys into `normalizedKeyOrder`, so the source key order is
    // irrelevant; the ivm `Value` serializes as plain JSON (matching the wire
    // rowKey values TS hashes).
    let mut key_map = serde_json::Map::with_capacity(row_key.len());
    for (k, v) in row_key.iter() {
        key_map.insert(
            k.clone(),
            serde_json::to_value(v).unwrap_or(serde_json::Value::Null),
        );
    }
    let id = rust_cvr::schema::types::RowID {
        schema: String::new(),
        table: table.to_string(),
        row_key: key_map,
    };
    // Delegate to the 1:1 port of TS `rowIDSignatureUnit` so there is exactly
    // ONE implementation of the persisted-signature hash on the live path —
    // a second inline `h64(row_id_string(...))` composition here is how an
    // FxHasher-class drift could re-enter without the cvr parity harness
    // (which pins `row_id_signature_unit`) ever noticing.
    rust_cvr::row_set_signature::row_id_signature_unit(&id)
}

// ---------------------------------------------------------------------------
// Advance abort — economic circuit breaker.
// Port of TS `#shouldAdvanceYieldMaybeAbortAdvance`.
// ---------------------------------------------------------------------------

/// Minimum advancement time before an abort is considered (ms).
///
/// Single source of truth, shared with the per-row economic breaker in
/// `advance_gate` (review #7). Previously this was a second `const 50.0` here
/// with a comment warning it MUST match `advance_gate`'s — importing the one
/// canonical value makes divergence (a TS-parity hazard) impossible.
use crate::advance_gate::MIN_ADVANCEMENT_TIME_LIMIT_MS;

/// Error thrown when advancement exceeds the economic time limit.
#[derive(Debug)]
pub struct ResetPipelinesSignal {
    pub reason: String,
    pub msg: String,
}

impl std::fmt::Display for ResetPipelinesSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for ResetPipelinesSignal {}

// ---------------------------------------------------------------------------
// Scalar-subquery companion monitoring.
// Port of pipeline-driver.ts scalar-subquery companion pipelines: a resolved
// scalar subquery is baked into the main query as a literal, and a live
// companion pipeline watches the subquery table. If the resolved value
// changes on advance, the whole query must reset+rehydrate (the baked literal
// is stale) — TS throws ResetPipelinesSignal('scalar-subquery'). Production
// advance returns ScalarResetError explicitly and maps it to a reset; the
// legacy in-memory advance API preserves its historical panic contract.
// ---------------------------------------------------------------------------

/// Recorded by a companion output when a resolved scalar subquery's value
/// changes mid-advance. The twin of TS's
/// `ResetPipelinesSignal('scalar-subquery')`. Message mirrors the TS signal.
#[derive(Debug, Clone)]
pub struct ScalarResetError {
    pub table: String,
    /// JS-`String()` rendering of the resolved (baked) value — message only.
    pub resolved: String,
    /// JS-`String()` rendering of the pushed value — message only.
    pub new: String,
}

impl std::fmt::Display for ScalarResetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Scalar subquery value changed for {}: {} -> {}",
            self.table, self.resolved, self.new
        )
    }
}

impl std::error::Error for ScalarResetError {}

/// Approximate JS `String(v)` for the scalar values a resolvable subquery
/// yields. `undefined` = no row matched at resolve; `null` = matched but the
/// field was NULL. Message rendering only — never compared or parsed.
fn js_scalar_string(value: &Option<Value>, undefined: bool) -> String {
    if undefined {
        return "undefined".to_string();
    }
    match value {
        None => "null".to_string(),
        Some(Value::Null) => "null".to_string(),
        Some(Value::Bool(b)) => {
            if *b {
                "true".into()
            } else {
                "false".into()
            }
        }
        Some(Value::F64(n)) => {
            // JS Number stringification for the integer/float literals a
            // resolvable scalar subquery yields.
            if n.fract() == 0.0 && n.is_finite() {
                format!("{}", *n as i64)
            } else {
                format!("{}", n)
            }
        }
        Some(Value::Str(s)) => s.to_string(),
        Some(Value::Json(s)) => s.to_string(),
    }
}

/// Port of TS `scalarValuesEqual` (strict `a === b` over
/// `LiteralValue | null | undefined`). JS `===` distinguishes `undefined`
/// (no row matched) from `null` (row matched, field NULL), so each side
/// carries an explicit `undefined` flag: flags differ → unequal; both
/// undefined → equal; otherwise compare the values (with `None`/`Value::Null`
/// both meaning SQL/JS null).
fn scalar_values_equal(
    a: &Option<Value>,
    a_undefined: bool,
    b: &Option<Value>,
    b_undefined: bool,
) -> bool {
    if a_undefined != b_undefined {
        return false;
    }
    if a_undefined {
        return true;
    }
    // Normalize None and Value::Null to the same "null" for comparison.
    let norm = |v: &Option<Value>| -> Option<Value> {
        match v {
            None | Some(Value::Null) => None,
            Some(x) => Some(x.clone()),
        }
    };
    norm(a) == norm(b)
}

/// Result of `resolve_scalar_subqueries`: the resolved AST plus the live
/// companion pipelines (built during resolution and kept alive to monitor the
/// resolved values) and the matched companion rows to emit on hydrate.
struct ScalarResolveOut {
    ast: Ast,
    /// (table, primary_key, row) for each matched scalar subquery — emitted as
    /// ADD rows on hydrate so the client's own EXISTS rewrite has the row it
    /// needs. The primary key is captured here from the companion pipeline's
    /// OWN schema (the source it was built from), so the row key is always
    /// well-formed at emission time regardless of what the top-level
    /// `primary_keys` map happens to contain — mirroring TS, where the EXISTS
    /// companion row is keyed by the subquery table's own primary key, and
    /// mirroring the Streamer's `schema.primary_key` fallback (streamer/mod.rs).
    companion_rows: Vec<(String, Vec<String>, Row)>,
    companions: Vec<CompanionBuilt>,
}

/// A companion pipeline built during scalar resolution, awaiting a monitoring
/// output. `resolved_undefined == true` ⇔ TS `undefined` (no row matched);
/// `resolved_value == None` with `resolved_undefined == false` ⇔ null.
pub struct CompanionBuilt {
    input: Shared<dyn Input>,
    table: String,
    child_field: String,
    resolved_value: Option<Value>,
    resolved_undefined: bool,
}

/// A live companion pipeline attached to a registered query, monitoring a
/// resolved scalar subquery's value. Port of TS `CompanionPipeline`.
struct CompanionPipeline {
    input: Shared<dyn Input>,
    output: Shared<CompanionOutput>,
    schema: SourceSchema,
}

/// The monitoring output for a companion pipeline. On each push it recomputes
/// the scalar value and records `ScalarResetError` if it differs from the
/// resolved (baked) value; otherwise it collects the change so the advance loop
/// can stream it under the owning query. Port of TS's companion
/// `setOutput({push})` handler.
pub struct CompanionOutput {
    table: String,
    child_field: String,
    resolved_value: Option<Value>,
    resolved_undefined: bool,
    changes: Vec<Change>,
    reset: Option<ScalarResetError>,
}

impl Output for CompanionOutput {
    fn push(&mut self, change: Change, _pusher: &dyn InputBase) {
        use crate::ivm::change::ChangeType;
        let (new_value, new_undefined) = match change.change_type() {
            ChangeType::Add | ChangeType::Edit => {
                // TS: newValue = change.node.row[childField] ?? null — never
                // undefined for ADD/EDIT.
                let v = match change.node().row.get(&self.child_field) {
                    None | Some(Value::Null) => None,
                    Some(x) => Some(x.clone()),
                };
                (v, false)
            }
            // TS: newValue = undefined for REMOVE.
            ChangeType::Remove => (None, true),
            // TS returns [] for CHILD: a relationship-only change does not move
            // the scalar value — neither reset nor accumulate.
            ChangeType::Child => return,
        };
        if !scalar_values_equal(
            &new_value,
            new_undefined,
            &self.resolved_value,
            self.resolved_undefined,
        ) {
            self.reset.get_or_insert_with(|| ScalarResetError {
                table: self.table.clone(),
                resolved: js_scalar_string(&self.resolved_value, self.resolved_undefined),
                new: js_scalar_string(&new_value, new_undefined),
            });
            return;
        }
        self.changes.push(change);
    }
}

/// Result of advance_to_head_stream.
pub struct AdvanceToHeadResult {
    pub version: String,
    pub num_changes: usize,
    pub aborted: bool,
    pub reset_reason: Option<String>,
    pub reset_msg: Option<String>,
}

struct AdvanceContext {
    timer: Instant,
    total_hydration_time_ms: f64,
    num_changes: usize,
    pos: usize,
}

impl AdvanceContext {
    fn should_abort(&self) -> bool {
        let elapsed = self.timer.elapsed().as_secs_f64() * 1000.0;
        if elapsed > MIN_ADVANCEMENT_TIME_LIMIT_MS {
            if elapsed > self.total_hydration_time_ms {
                return true;
            }
            if elapsed > self.total_hydration_time_ms / 2.0 && self.pos <= self.num_changes / 2 {
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

pub struct Engine {
    sources: HashMap<String, Shared<dyn Source>>,
    /// The single logical primary key per table — `keyCmp[0]` (shortest
    /// replicated unique key) OVERLAID with the client-declared PK via
    /// `apply_client_primary_keys`. Drives EVERYTHING: source identity, query
    /// ordering completion, Take/limit, advance edit-vs-remove classification,
    /// and client-facing rowKey emission. This mirrors TS's single `#primaryKeys`
    /// map (keyCmp[0] then `buildPrimaryKeys(clientSchema)`), which is threaded
    /// everywhere incl. the `TableSource` ctor (`#getSource`). `keyCmp[0]`
    /// survives only in the snapshotter/changelog diff spec (the replicator key,
    /// shared with replication). Emitting `keyCmp[0]` where it differs from the
    /// client PK would store a rowKey missing a client PK column → client
    /// `toPrimaryKeyString` throws "Got undefined".
    primary_keys: HashMap<String, Vec<String>>,
    /// The client-declared PK OVERRIDE, retained so it can be re-applied to
    /// `primary_keys` and the sources after an engine rebuild (`build_engine`).
    /// Empty ⇒ no override (everything uses `keyCmp[0]`). Source of truth for
    /// `apply_client_primary_keys`; TS `clientSchema.tables[t].primaryKey`.
    client_primary_keys: HashMap<String, Vec<String>>,
    /// All unique indexes per table (PK plus any unique keys). Used by
    /// scalar-subquery resolution to decide whether a subquery is "simple"
    /// (returns at most one deterministic row). Port of TS `#tableSpecs`
    /// unique-key info.
    unique_keys: HashMap<String, Vec<Vec<String>>>,
    table_specs: HashMap<String, TableSpecInfo>,
    pipelines: Vec<PipelineEntry>,
    /// XOR signature of the row-set per query.
    row_set_signatures: HashMap<String, u64>,
    /// Whether NOT EXISTS is allowed (server-side: true).
    enable_not_exists: bool,
    /// Storage factory counter.
    _next_storage_id: usize,
    /// Cancellation token for advance/hydrate abort.
    cancellation_token: CancellationToken,
    /// Optional SQLite connection used to build the query-planner cost model.
    /// Port-parity: TS `buildPipeline` runs `planQuery(ast, costModel)` to assign
    /// `flip` annotations before building (builder.ts:140). rust's builder READS
    /// `csq.flip` but nothing SET it, so exists-in-OR was always built
    /// non-flipped — over-attaching (and over-emitting to the CVR) the backing
    /// rows of a redundant WHERE-EXISTS branch (the ART G8 divergence). When this
    /// connection is present, `plan_ast` runs the ported planner so flips match
    /// TS. `None` (most unit tests) ⇒ no planning, unchanged legacy behavior.
    cost_model_conn: Option<Rc<RefCell<rusqlite::Connection>>>,
    /// Visible (zql) column specs per table for the scanstatus cost model's
    /// probe SQL — the analog of TS `PipelineDriver.#tableSpecs` passed to
    /// `createSQLiteCostModel(db, tableSpecs)` (pipeline-driver.ts:436).
    /// Pre-sorted once per schema (see `prepare_table_specs`).
    cost_model_specs: Option<Rc<crate::sqlite::sqlite_cost_model::PreparedTableSpecs>>,
    /// Lazily-built scanstatus cost model, cached for the engine's life — the
    /// analog of TS `PipelineDriver.#costModels` (a WeakMap keyed by snapshot
    /// db; TS gets a fresh db handle per snapshot advance, rust keeps ONE wal2
    /// connection whose data advances in place, so engine-lifetime caching is
    /// the equivalent scope). Invalidated when the conn or specs change.
    cached_cost_model: Option<crate::planner::ConnectionCostModel>,
}

impl Engine {
    pub fn new(primary_keys: HashMap<String, Vec<String>>) -> Self {
        Engine {
            sources: HashMap::new(),
            primary_keys,
            client_primary_keys: HashMap::new(),
            unique_keys: HashMap::new(),
            table_specs: HashMap::new(),
            pipelines: Vec::new(),
            row_set_signatures: HashMap::new(),
            enable_not_exists: true, // server-side
            _next_storage_id: 0,
            cancellation_token: CancellationToken::new(),
            cost_model_conn: None,
            cost_model_specs: None,
            cached_cost_model: None,
        }
    }

    /// Enable query-flip planning (port parity with TS `buildPipeline` →
    /// `planQuery`). `conn` backs the scanstatus cost model's probe statements
    /// (TS `createSQLiteCostModel(db, ...)`); the planner then annotates each
    /// query AST's correlated-subquery `flip` before `build_pipeline`. Without
    /// this, exists-in-OR is built non-flipped and over-emits WHERE-EXISTS
    /// backing rows (ART G8). Pair with [`Self::set_cost_model_table_specs`] —
    /// conn without specs degrades (loudly) to the legacy COUNT(*) model.
    pub fn set_cost_model_conn(&mut self, conn: Rc<RefCell<rusqlite::Connection>>) {
        self.cost_model_conn = Some(conn);
        self.cached_cost_model = None;
    }

    /// Install the visible-column specs the scanstatus cost model probes with —
    /// the analog of TS `PipelineDriver.#tableSpecs` in
    /// `createSQLiteCostModel(db, this.#tableSpecs)` (pipeline-driver.ts:436).
    pub fn set_cost_model_table_specs(
        &mut self,
        specs: HashMap<String, HashMap<String, crate::ivm::schema::ColumnType>>,
    ) {
        self.cost_model_specs = Some(Rc::new(
            crate::sqlite::sqlite_cost_model::prepare_table_specs(specs),
        ));
        self.cached_cost_model = None;
    }

    /// Build (or reuse) the production cost model — the scanstatus/stat-fanout
    /// model, the port of TS `createSQLiteCostModel`, what TS ALWAYS plans with
    /// (pipeline-driver.ts:436). Returns `None` when it can't be built (specs
    /// missing or SQLite lacks SQLITE_ENABLE_STMT_SCANSTATUS), so the caller
    /// runs UNPLANNED — TS parity (`if (costModel)`). There is NO fallback cost
    /// model; the removed COUNT(*) fallback + `RUST_IVM_PLANNER_COST_MODEL` env
    /// were a rust-only divergence (the 2026-08-29 mis-flip).
    ///
    /// The 2026-08-29 prod 144s `tickets` hydrate was this exact seam: the
    /// engine planned with the filter-blind COUNT model (constrained fetch ≈ 1
    /// row, fanout 1.0), flipped a join whose real parent-side fanout was tens
    /// of thousands of rows, and the flipped-join batch fetch ran 44.8s per
    /// batch. TS on the same query plans with scanstatus and does not flip.
    /// Build (once, cached) the scanstatus cost model, or `None` when it can't
    /// be built. TS has NO alternate cost model: `buildPipeline` plans only
    /// `if (costModel)` and otherwise runs the query UNPLANNED (builder.ts:140;
    /// `#ensureCostModelExistsIfEnabled` returns undefined when the planner is
    /// off). Rust mirrors that exactly — a missing table-spec or a scanstatus
    /// build error logs once and returns `None`, so the caller runs unplanned
    /// (correct, just un-flipped). The removed COUNT(*) fallback was a
    /// rust-only divergence that produced DIFFERENT plans than TS (the
    /// 2026-08-29 144s `tickets` mis-flip). `build.rs` compiles in
    /// SCANSTATUS/STAT4, so a real binary returns `Some`; `None` means a wiring
    /// bug (specs not set), where un-flipped is the safe TS-parity baseline.
    fn ensure_cost_model(
        &mut self,
        conn: &Rc<RefCell<rusqlite::Connection>>,
    ) -> Option<crate::planner::ConnectionCostModel> {
        if let Some(model) = &self.cached_cost_model {
            return Some(model.clone());
        }
        let Some(specs) = &self.cost_model_specs else {
            eprintln!(
                "WARNING: planner cost-model conn set without table specs; \
                 running UNPLANNED (TS plans with scanstatus — call \
                 set_cost_model_table_specs)"
            );
            return None;
        };
        match crate::sqlite::sqlite_cost_model::create_sqlite_cost_model_prepared(
            conn.clone(),
            specs.clone(),
        ) {
            Ok(model) => {
                self.cached_cost_model = Some(model.clone());
                Some(model)
            }
            Err(e) => {
                eprintln!(
                    "WARNING: scanstatus cost model unavailable; running \
                     UNPLANNED (TS plans with scanstatus): {e}"
                );
                None
            }
        }
    }

    /// Plan `ast` (assign `flip`s via the ported cost-based planner) when a
    /// cost-model connection is configured; otherwise return it unchanged.
    /// Mirror of TS `buildPipeline`'s `if (costModel) ast = planQuery(...)`.
    ///
    /// A scanstatus probe interrupted by the watchdog (`sqlite3_interrupt`
    /// racing planning) unwinds with the typed `CostProbeInterrupted` payload;
    /// like the napi-era caller, degrade to planning WITHOUT flips instead of
    /// letting the panic tear down the client group.
    fn plan_ast(&mut self, ast: &Ast) -> Ast {
        let Some(conn) = self.cost_model_conn.clone() else {
            return ast.clone();
        };
        // TS: `if (costModel) ast = planQuery(...)` — no model ⇒ run unplanned.
        let Some(model) = self.ensure_cost_model(&conn) else {
            return ast.clone();
        };
        let planned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::planner::plan_query(ast, model)
        }));
        match planned {
            Ok(planned) => planned,
            Err(payload) => {
                if let Some(interrupted) =
                    payload.downcast_ref::<crate::sqlite::sqlite_cost_model::CostProbeInterrupted>()
                {
                    eprintln!(
                        "planner probe interrupted; building without flips: {}",
                        interrupted.0
                    );
                    ast.clone()
                } else {
                    std::panic::resume_unwind(payload)
                }
            }
        }
    }

    /// Rust-only test/inspection hook (no TS twin — TS pins planner behavior
    /// through PipelineDriver tests; the standalone engine needs direct access
    /// to the flip decisions `plan_ast` actually produces, model selection
    /// included). Returns the ordered flips per `flip_order`.
    #[doc(hidden)]
    pub fn planned_flips_for_test(&mut self, ast_json: &serde_json::Value) -> Vec<Option<bool>> {
        let ast = crate::replay::json_to_ast(ast_json);
        let planned = self.plan_ast(&ast);
        crate::planner::flip_order(&planned)
    }

    /// Install the client-declared primary keys (TS `buildPrimaryKeys(clientSchema)`)
    /// and apply them to the single logical `primary_keys` map + the sources, so
    /// source identity, ordering, edit classification, and emission all use the
    /// client PK. Idempotent; safe to call on every config (before hydrate).
    pub fn set_client_primary_keys(&mut self, client_primary_keys: HashMap<String, Vec<String>>) {
        self.client_primary_keys = client_primary_keys;
        self.apply_client_primary_keys();
    }

    /// Overlay `client_primary_keys` onto `primary_keys` and re-key each source
    /// (TS threads the client-overlaid `#primaryKeys` everywhere incl. the
    /// `TableSource` ctor). Only entries for known tables with a non-empty key
    /// take effect; `keyCmp[0]` remains for everything else. Re-key runs before
    /// the first fetch, so no cached row/statement state is stale.
    fn apply_client_primary_keys(&mut self) {
        for (table, pk) in &self.client_primary_keys {
            if pk.is_empty() || !self.primary_keys.contains_key(table) {
                continue;
            }
            self.primary_keys.insert(table.clone(), pk.clone());
            if let Some(src) = self.sources.get(table) {
                src.borrow_mut().set_primary_key(pk.clone());
            }
        }
    }

    /// Set table spec info (for minRowVersion bumping in Streamer).
    pub fn set_table_spec(&mut self, table: &str, min_row_version: Option<String>) {
        self.table_specs
            .insert(table.to_string(), TableSpecInfo { min_row_version });
    }

    /// Set the unique keys for a table (PK plus any unique indexes), used by
    /// scalar-subquery resolution. Mirrors the unique-key info TS carries in
    /// its tableSpecs.
    pub fn set_unique_keys(&mut self, table: &str, keys: Vec<Vec<String>>) {
        self.unique_keys.insert(table.to_string(), keys);
    }

    pub fn register_source(&mut self, source: Shared<dyn Source>) {
        let table_name = source.borrow().table_name().to_string();
        let pk = source.borrow().primary_key().to_vec();
        self.primary_keys.insert(table_name.clone(), pk);
        self.sources.insert(table_name, source);
    }

    /// Capture source connection lengths before building a pipeline. A failed
    /// build must remove every partially wired connection, just as TS drops its
    /// unregistered TableSource graph on an exception.
    pub fn source_connection_checkpoint(&self) -> HashMap<String, usize> {
        self.sources
            .iter()
            .map(|(table, source)| (table.clone(), source.borrow().connection_count()))
            .collect()
    }

    pub fn rollback_source_connections(&mut self, checkpoint: &HashMap<String, usize>) {
        for (table, source) in &self.sources {
            source
                .borrow_mut()
                .truncate_connections(checkpoint.get(table).copied().unwrap_or_default());
        }
    }

    /// TEST-ONLY: drop a table's entry from the top-level `primary_keys` map,
    /// leaving its source (and thus its pipeline schema's `primary_key`) intact.
    /// Used to simulate the (normally impossible) asymmetry where a scalar-EXISTS
    /// companion table is absent from the map — proving the emission path still
    /// keys the companion row by the source schema's primary key (faithful to
    /// TS), rather than emitting an empty `{}` rowKey. Never called in prod.
    #[doc(hidden)]
    pub fn __test_drop_primary_key(&mut self, table: &str) {
        self.primary_keys.remove(table);
    }

    /// Get the row-set signature for a query.
    /// Port of TS `rowSetSignature()`.
    pub fn row_set_signature(&self, query_id: &str) -> Option<u64> {
        self.row_set_signatures.get(query_id).copied()
    }

    /// Total hydration time across all pipelines.
    /// Port of TS `totalHydrationTimeMs()`.
    pub fn total_hydration_time_ms(&self) -> f64 {
        let total: f64 = self.pipelines.iter().map(|p| p.hydration_time_ms).sum();
        // `Iterator::sum::<f64>()` uses negative zero for an empty iterator.
        // JavaScript observes that distinction through `Object.is`, while the
        // TS driver returns ordinary positive zero when no pipelines exist.
        if total == 0.0 { 0.0 } else { total }
    }

    /// Rows produced by a query's initial hydration, or `None` if the query
    /// has no registered pipeline. Port of reading TS `pipeline.hydrationRowCount`
    /// (pipeline-driver.ts:774) — the payload the driver's per-query lifecycle
    /// log reports.
    pub fn hydration_row_count(&self, query_id: &str) -> Option<u64> {
        self.pipelines
            .iter()
            .find(|p| p.query_id == query_id)
            .map(|p| p.hydration_row_count)
    }

    /// Replace the native wall-clock measurement with the caller's
    /// pause-aware hydration timer.
    pub fn set_hydration_time_ms(&mut self, query_id: &str, hydration_time_ms: f64) -> bool {
        if let Some(entry) = self.pipelines.iter_mut().find(|p| p.query_id == query_id) {
            entry.hydration_time_ms = hydration_time_ms;
            true
        } else {
            false
        }
    }

    /// Wall-clock hydration time for a query's pipeline, or `None` if the query
    /// has no registered pipeline. Port of reading TS `pipeline.hydrationTimeMs`
    /// (pipeline-driver.ts:773) — read by the driver at teardown to emit
    /// `query-pipeline-stop`.
    pub fn hydration_time_ms(&self, query_id: &str) -> Option<f64> {
        self.pipelines
            .iter()
            .find(|p| p.query_id == query_id)
            .map(|p| p.hydration_time_ms)
    }

    /// Milliseconds since a query's pipeline finished hydrating (was registered),
    /// or `None` if it has no registered pipeline. Port of TS
    /// `Date.now() - pipeline.pipelineReadyAtMs` (pipeline-driver.ts:861) — the
    /// `pipelineLifetimeMs` the driver reports in `query-pipeline-stop`.
    pub fn pipeline_lifetime_ms(&self, query_id: &str) -> Option<f64> {
        self.pipelines
            .iter()
            .find(|p| p.query_id == query_id)
            .map(|p| p.pipeline_ready_at.elapsed().as_secs_f64() * 1000.0)
    }

    /// Remove a query's pipeline.
    /// Port of TS `removeQuery()`.
    pub fn remove_query(&mut self, query_id: &str) {
        if let Some(pos) = self.pipelines.iter().position(|p| p.query_id == query_id) {
            self.pipelines[pos].pipeline.borrow_mut().destroy();
            for c in &self.pipelines[pos].companions {
                c.input.borrow_mut().destroy();
            }
            self.pipelines.remove(pos);
        }
        self.row_set_signatures.remove(query_id);
    }

    /// Build pipelines and hydrate them — STREAMING version.
    /// Calls `on_row_change` for each RowChange as it's produced, row by row.
    /// No collecting into Vec — true streaming matching TS generator behavior.
    pub fn add_queries_streaming<F: FnMut(&RowChange)>(
        &mut self,
        queries: &[QuerySpec],
        mut on_row_change: F,
    ) -> Vec<QueryResult> {
        // Reset cancellation at the start of hydration.
        self.cancellation_token.reset();
        crate::perf_trace::reset();
        let perf_timer = Instant::now();
        for q in queries {
            self.remove_query(&q.query_id);
        }

        // Phase 1: Resolve scalar subqueries, then build all pipelines
        // sequentially (mutates source connections). Resolution runs first so
        // the main pipeline is built from the literal-resolved AST, and the
        // companion pipelines it builds are retained for advance-time monitoring.
        let mut built: Vec<Built> = Vec::new();

        for q in queries {
            let _t = crate::perf_trace::scope("hydrate.build");
            let timer = Instant::now();
            // Resolve scalar subqueries against the live sources (`&self`),
            // producing the resolved AST + retained live companion pipelines.
            let resolved = self.resolve_scalar_subqueries(&q.ast);

            let primary_keys = &self.primary_keys;
            let ordered = complete_ordering(&resolved.ast, &|table: &str| {
                primary_keys.get(table).cloned().unwrap_or_default()
            });
            // Assign correlated-subquery `flip`s via the cost-based planner
            // (parity with TS `buildPipeline` → `planQuery`). No-op when no
            // cost-model connection is configured.
            let ast = self.plan_ast(&ordered);
            let mut delegate = EngineDelegate {
                sources: &self.sources,
                enable_not_exists: self.enable_not_exists,
            };
            let pipeline = build_pipeline(&ast, &mut delegate);

            let collector = Rc::new(RefCell::new(CollectOutput::new()));
            pipeline
                .borrow()
                .set_output(collector.clone() as OutputHandle);

            let schema = pipeline.borrow().get_schema();
            built.push(Built {
                query_id: q.query_id.clone(),
                transformed_ast: resolved.ast,
                pipeline,
                collector,
                schema,
                timer,
                companion_rows: resolved.companion_rows,
                companions: resolved.companions,
            });
        }

        // Phase 2: Hydrate. Sequential — Rc/RefCell are !Send, so all fetch()
        // calls run on this thread. After the main query rows, emit the matched
        // scalar-subquery companion rows as ADDs (TS yields them post-hydrate
        // so the client's own EXISTS rewrite has the row).
        let primary_keys = self.primary_keys.clone();
        let table_specs = self.table_specs.clone();

        // Cancellation: the consumer (view-syncer) may abandon a hydrate mid-
        // stream (client disconnect / teardown). The driver flips this token
        // via the out-of-band `cancel()`; we check it between rows so we stop
        // producing promptly instead of materializing the whole result into a
        // queue nobody drains. A partially-fetched pipeline is left in an
        // inconsistent operator state, so on cancel we register NOTHING and
        // destroy what we built (the queries are being discarded anyway).
        let mut cancelled = false;
        // Per-query hydration row count (main rows + companion rows). Port of the
        // TS `hydrationRowCount++` counter in `#addQueryImpl` (pipeline-driver.ts:
        // 633/686/693) — the payload the driver's `#logQueryPipelineLifecycle`
        // reports so a slow query is identifiable by rows-considered.
        let mut row_counts: HashMap<String, u64> = HashMap::new();
        'hydrate: for b in &built {
            let _t = crate::perf_trace::scope("hydrate.fetch");
            let mut b_row_count: u64 = 0;
            if self.cancellation_token.is_cancelled() {
                cancelled = true;
                break 'hydrate;
            }
            let stream = b.pipeline.borrow().fetch(&Default::default());
            let mut streamer = Streamer::new(primary_keys.clone(), table_specs.clone());
            let mut nodes = crate::ivm::stream::skip_yields(stream);
            while let Some(node) = nodes.next() {
                if self.cancellation_token.is_cancelled() {
                    cancelled = true;
                    // TS-faithful graceful cancel: the TS view-syncer ALWAYS
                    // fully drains the hydrate generator, so a Take/Cap stream
                    // is never abandoned mid-iteration. The Rust `break 'hydrate`
                    // is a new early-return path with no TS analog; dropping the
                    // in-flight `nodes` iterator here (limit not reached, input
                    // not exhausted) would trip the Take/Cap `InitialFetchGuard`
                    // panic (take.rs:117 / cap.rs). Mirror TS by draining the
                    // remaining stream to exhaustion (discarding rows — the query
                    // is being discarded anyway) so the Take sees a normal
                    // end-of-stream, persists, and its guard no-ops. The
                    // `cancelled` cleanup below then destroys the pipelines.
                    // A Take stream abandoned with NO cancel in flight still
                    // panics (the genuine-bug guard is intact).
                    drop(node);
                    for discarded in nodes.by_ref() {
                        drop(discarded);
                    }
                    break 'hydrate;
                }
                let change = crate::ivm::change::make_add_change(node);
                streamer.accumulate(&b.query_id, &b.schema, std::slice::from_ref(&change));
                for rc in streamer.stream() {
                    let _t = crate::perf_trace::scope("deliver.row");
                    b_row_count += 1;
                    on_row_change(&rc);
                }
            }

            // Companion rows: raw ADD RowChanges for each matched subquery row.
            for (table, schema_pk, row) in &b.companion_rows {
                // Faithful to TS: the EXISTS companion row is keyed by the
                // subquery table's OWN primary key, which is always available.
                // Prefer the top-level `primary_keys` map (the registered PK),
                // but fall back to the primary key captured from the companion
                // pipeline's own schema at resolve time (`schema_pk`) — exactly
                // the `schema.primary_key` fallback the Streamer uses for the
                // main-hydrate path (streamer/mod.rs). This guarantees a
                // well-formed row key in the normal path, so the client never
                // sees `rowKey:"{}"` ("Got undefined").
                //
                // The panic below is retained as a NEVER-HAPPEN assertion: it
                // can only fire if BOTH the map lacks the table AND the source
                // schema carried no primary key — which is impossible for a
                // registered source. It stays as a loud guard so a future
                // wiring regression that emits an empty-PK companion row fails
                // fast here instead of crashing the client.
                let pk: &Vec<String> = match primary_keys.get(table) {
                    Some(pk) if !pk.is_empty() => pk,
                    _ if !schema_pk.is_empty() => schema_pk,
                    _ => panic!(
                        "companion/scalar-EXISTS table {table:?} has no primary \
                         key in the registered map NOR in its pipeline schema — \
                         cannot emit its row key (would produce an empty rowKey \
                         and crash the client). Registered PK tables: {:?}",
                        primary_keys.keys().collect::<Vec<_>>(),
                    ),
                };
                let row_key = crate::streamer::get_row_key(pk, row);
                let _t = crate::perf_trace::scope("deliver.row");
                b_row_count += 1;
                on_row_change(&RowChange {
                    change_type: crate::ivm::change::ChangeType::Add,
                    query_id: b.query_id.clone(),
                    table: table.clone(),
                    row_key,
                    row: Some(row.clone()),
                    is_hidden: false,
                });
            }
            row_counts.insert(b.query_id.clone(), b_row_count);
        }

        if cancelled {
            // Discard everything built this call — no partial pipeline is
            // registered, so a later advance can never run on a half-fetched
            // graph. The consumer will re-add the query on the next connection.
            for b in built {
                b.pipeline.borrow_mut().destroy();
                for cb in b.companions {
                    cb.input.borrow_mut().destroy();
                }
            }
            return Vec::new();
        }

        // Phase 3: Attach monitoring outputs to companion pipelines and
        // register everything.
        let mut results = Vec::new();
        for b in built {
            let hydration_time_ms = b.timer.elapsed().as_secs_f64() * 1000.0;
            let hydration_row_count = row_counts.get(&b.query_id).copied().unwrap_or(0);
            results.push(QueryResult {
                query_id: b.query_id.clone(),
                changes: Vec::new(),
                hydration_time_ms,
                hydration_row_count,
            });

            let mut live_companions = Vec::with_capacity(b.companions.len());
            for cb in b.companions {
                let schema = cb.input.borrow().get_schema();
                let output = Rc::new(RefCell::new(CompanionOutput {
                    table: cb.table,
                    child_field: cb.child_field,
                    resolved_value: cb.resolved_value,
                    resolved_undefined: cb.resolved_undefined,
                    changes: Vec::new(),
                    reset: None,
                }));
                cb.input.borrow().set_output(output.clone() as OutputHandle);
                live_companions.push(CompanionPipeline {
                    input: cb.input,
                    output,
                    schema,
                });
            }

            b.collector.borrow_mut().configure_streaming(
                b.query_id.clone(),
                b.schema.clone(),
                self.primary_keys.clone(),
                self.table_specs.clone(),
            );
            self.pipelines.push(PipelineEntry {
                pipeline: b.pipeline,
                collector: b.collector,
                query_id: b.query_id,
                hydration_time_ms,
                hydration_row_count,
                pipeline_ready_at: Instant::now(),
                transformed_ast: b.transformed_ast,
                companions: live_companions,
            });
        }

        crate::perf_trace::report("hydrate", perf_timer.elapsed().as_secs_f64() * 1000.0);
        results
    }

    /// Advance with streaming — calls `on_row_change` for each RowChange as produced.
    /// Returns `true` when every change was pushed; `false` when the
    /// economic budget (the pre-#6206 model retained on this legacy path) or a
    /// cancellation aborted the batch early. A `false` batch is TRUNCATED —
    /// the graph reflects only the pushed prefix, so the caller must treat the
    /// output as invalid and rehydrate (TS's old model threw
    /// ResetPipelinesSignal here; a silent partial-success previously let the
    /// parity harness serialize a truncated batch as a normal response).
    pub fn advance_streaming<F: FnMut(&RowChange)>(
        &mut self,
        changes: &[(String, SourceChange)],
        mut on_row_change: F,
    ) -> bool {
        // Reset cancellation at the start of each advance.
        self.cancellation_token.reset();

        // Advance-boundary bookkeeping reset. The snapshotter-driven path
        // (`advance_to_head_stream`) clears per-advance source state via its
        // PREV/CURR `set_snapshot_db` calls; this plain path has no snapshot
        // swap, so clear explicitly — otherwise the same-advance removed-PK /
        // applied-changes sets grow by one entry per removed row FOREVER
        // (+1 block/advance, dhat-measured). Safe here: this function is not
        // on the `advance_to_head_stream` path, so no mid-advance clearing.
        for source in self.sources.values() {
            source.borrow_mut().clear_advance_state();
        }

        let total_hydration_time_ms = self.total_hydration_time_ms();

        let advance_ctx = AdvanceContext {
            timer: Instant::now(),
            total_hydration_time_ms,
            num_changes: changes.len(),
            pos: 0,
        };
        let mut completed = true;
        for (table, change) in changes {
            if advance_ctx.should_abort() || self.cancellation_token.is_cancelled() {
                completed = false;
                break;
            }

            if let Some(source) = self.sources.get(table) {
                for entry in &self.pipelines {
                    clear_and_cap(&mut entry.collector.borrow_mut().changes);
                    clear_and_cap(&mut entry.collector.borrow_mut().row_changes);
                    for c in &entry.companions {
                        let mut output = c.output.borrow_mut();
                        clear_and_cap(&mut output.changes);
                        output.reset = None;
                    }
                }

                let _pipeline_changes = source.borrow_mut().push(change.clone());
                // Preserve the legacy in-memory API contract. Production
                // advance_to_head_stream returns this condition explicitly.
                if let Some(reset) = take_scalar_reset(&self.pipelines) {
                    std::panic::panic_any(reset);
                }

                for entry in &self.pipelines {
                    let row_changes = std::mem::take(&mut entry.collector.borrow_mut().row_changes);
                    for rc in row_changes {
                        let _t = crate::perf_trace::scope("deliver.row");
                        on_row_change(&rc);
                    }
                    // Stream surviving companion changes (the resolved value was
                    // unchanged) under the owning query, like TS's companion
                    // `streamer.accumulate(queryID, companionSchema, [change])`.
                    for c in &entry.companions {
                        let cc: Vec<Change> = std::mem::take(&mut c.output.borrow_mut().changes);
                        if !cc.is_empty() {
                            let mut streamer =
                                Streamer::new(self.primary_keys.clone(), self.table_specs.clone());
                            streamer.accumulate(&entry.query_id, &c.schema, &cc);
                            for rc in streamer.stream() {
                                let _t = crate::perf_trace::scope("deliver.row");
                                on_row_change(&rc);
                            }
                        }
                    }
                }
            }
        }
        completed
    }

    /// Build pipelines and hydrate them.
    /// Delegates to add_queries_streaming — single code path.
    pub fn add_queries(&mut self, queries: &[QuerySpec]) -> Vec<QueryResult> {
        let mut by_qid: HashMap<String, Vec<RowChange>> = HashMap::new();
        let results = self.add_queries_streaming(queries, |rc| {
            by_qid
                .entry(rc.query_id.clone())
                .or_default()
                .push(rc.clone());
        });
        for changes in by_qid.values() {
            for rc in changes {
                if rc.change_type != crate::ivm::change::ChangeType::Edit {
                    let sig = *self.row_set_signatures.get(&rc.query_id).unwrap_or(&0);
                    let unit = row_signature_unit(&rc.table, &rc.row_key);
                    self.row_set_signatures
                        .insert(rc.query_id.clone(), sig ^ unit);
                }
            }
        }
        results
            .into_iter()
            .map(|mut r| {
                r.changes = by_qid.remove(&r.query_id).unwrap_or_default();
                r
            })
            .collect()
    }

    /// Advance to head: Rust derives its own diff from the snapshotter,
    /// pushes the changes through all pipelines, and streams RowChanges.
    ///
    /// This is the Go-primary architecture: Rust owns the snapshotter,
    /// derives the diff from `_zero.changeLog2`, and drives the engine.
    /// TS never computes a diff or sends SourceChange[] — it just calls
    /// this method and consumes the RowChange stream.
    ///
    /// The snapshotter must be initialized and the diff must be valid.
    /// Returns the new version string on success.
    pub fn advance_to_head_stream<F, H>(
        &mut self,
        snapshotter: &mut crate::snapshotter::Snapshotter,
        syncable_tables: &HashMap<String, crate::snapshotter::spec::LiteAndZqlSpec>,
        all_table_names: &std::collections::HashSet<String>,
        mut on_header: H,
        mut on_row_change: F,
    ) -> Result<AdvanceToHeadResult, crate::snapshotter::DiffError>
    where
        F: FnMut(&RowChange),
        H: FnMut(&str, usize),
    {
        // Reset cancellation at the start of each advance, exactly as
        // `add_queries_streaming` and `advance_streaming` do. This makes the
        // method LOCALLY correct instead of relying on the cross-call invariant
        // that a rehydrate (which resets the token) always runs between an
        // early-abandoned advance and the next one. Without it, a leftover
        // cancel from a prior aborted advance would trip the very first diff
        // callback below ("advance cancelled before all changes were
        // delivered") if that flow were ever reordered.
        self.cancellation_token.reset();
        crate::perf_trace::reset();
        let perf_timer = Instant::now();

        // 1. Advance the snapshotter — get the diff between prev and curr.
        let diff = snapshotter
            .advance(syncable_tables, all_table_names)
            .map_err(crate::snapshotter::DiffError::Other)?;

        let new_version = diff.curr_version().to_string();
        let num_changes = diff.changes() as usize;

        // Notify the caller of the header (version + numChanges) BEFORE
        // iterating the diff. This lets the NAPI layer push the header row
        // and unblock the JS side so it can start consuming rows while we
        // continue producing them.
        on_header(&new_version, num_changes);

        // 2. Economic abort — port of TS #shouldAdvanceYieldMaybeAbortAdvance.
        let total_hydration_time_ms = self.total_hydration_time_ms();
        let advance_start = Instant::now();
        let mut pos = 0usize;

        // Per-FETCH arm of the same economic budget (TS parity, point 2): arm a
        // thread-local gate that the TableSource row-read loop checks between
        // rows, so a single fat change (e.g. a big correlated-EXISTS re-fetch) is
        // abandoned mid-fetch instead of grinding to the change boundary. The
        // per-change check below is the other arm. Disarmed on every exit path.
        let advance_gate = crate::advance_gate::AdvanceGate::new(
            advance_start,
            total_hydration_time_ms,
            num_changes,
        );
        // Guard disarms the thread-local on scope exit (incl. panic unwind).
        let _gate_guard = crate::advance_gate::arm(advance_gate.clone());

        // 3. Iterate the diff, converting each SnapshotChange to SourceChange(s).
        let prev_conn = snapshotter.prev_conn()?;
        let curr_conn = snapshotter.current_conn()?;

        // Point every TableSource at the PREV snapshot while changes are
        // processed, so validate_change/fetches read prev — exactly as TS
        // (`pipeline-driver.ts` reads prev during advance, setDB(curr) after).
        // Without this the sources read a newer (head) snapshot and
        // validate_change spuriously panics "Add duplicate row". MemorySource
        // ignores this (no-op).
        {
            let _t = crate::perf_trace::scope("advance.setdb");
            for source in self.sources.values() {
                source.borrow_mut().set_snapshot_db(prev_conn.clone());
            }
        }

        // Per-table column types, so raw diff rows are coerced (bool/json) the
        // same way the fetch path coerces them — otherwise a boolean column
        // reads Bool on hydrate but F64 on advance and compare_values panics.
        let table_columns: HashMap<String, HashMap<String, crate::ivm::schema::ColumnType>> = self
            .sources
            .iter()
            .map(|(t, s)| (t.clone(), s.borrow().column_types()))
            .collect();
        let cancellation_token = self.cancellation_token.clone();
        let mut result =
            crate::snapshotter::diff::iterate_diff(&diff, &prev_conn, &curr_conn, |sc| {
                // A watchdog/consumer cancel is not a successful short stream. In
                // particular, the boundary callback can cancel after failing to
                // acquire credit; continuing would mutate the graph to head while
                // silently dropping the undelivered tail. Fail the advance so the
                // view-syncer cannot commit a partial CVR.
                if cancellation_token.is_cancelled() {
                    return Err(crate::snapshotter::DiffError::Other(
                        "advance cancelled before all changes were delivered".to_string(),
                    ));
                }
                let col_types = table_columns.get(&sc.table);
                // Publish current progress to the per-fetch gate so its budget check
                // (run inside the row-read loop during this change's push) uses the
                // same `pos` as the per-change check below.
                advance_gate.set_pos(pos);
                // Smarter load-shedding per-change abort (TS #6206): project the
                // cost of pushing the remaining backlog and bail if it materially
                // exceeds a rehydrate, catch a single pathological change, and let
                // an already-mostly-done advance finish. `advance_reset` owns the
                // three-arm decision; both this per-change site and the per-fetch
                // `should_stop_fetch` evaluate the same formula.
                if let Some(reset) = advance_gate.advance_reset() {
                    return Err(advance_reset_error(
                        reset,
                        advance_gate.elapsed_ms(),
                        total_hydration_time_ms,
                        pos,
                        num_changes,
                    ));
                }
                pos += 1;

                // Per-change wall-clock start for `zero.sync.ivm.advance-time`
                // (TS `#advanceTime`, pipeline-driver.ts: `const start =
                // timer.totalElapsed()`). Recorded only on the successful tail
                // below — the inactive-source early return, like TS's `continue`,
                // does not record.
                let change_timer = std::time::Instant::now();

                // PipelineDriver creates TableSources lazily and skips a diff entry
                // when no live pipeline reads that table. Rust keeps schema sources
                // registered up front, so explicitly distinguish an inactive source
                // from a live one before validation/push mutates it.
                if self
                    .sources
                    .get(&sc.table)
                    .is_none_or(|source| !source.borrow().has_active_connections())
                {
                    return Ok(());
                }

                // Mark the start of this change's push so the slow-current-change
                // arm (checked per-row via `should_stop_fetch`) can measure a
                // single pathological push. Cleared at the change boundary below.
                // TS `AdvanceContext.currentChangeStartMs = start`.
                advance_gate.set_current_change_start(advance_gate.elapsed_ms());

                // Port of TS pipeline-driver.ts #advance (744-776): the prev_value
                // whose PK equals next's PK becomes the EDIT old-row and is NOT
                // removed; every OTHER prev_value (a different-PK unique conflict) is
                // removed. Then next → EDIT(that old-row) or ADD. (Previously we
                // removed ALL prev_values including the same-PK one and used
                // prev_values[0] as the edit-old — an extra REMOVE + wrong old-row
                // pick for an in-place update, diverging from TS.)
                let pk_cols = self
                    .primary_keys
                    .get(&sc.table)
                    .cloned()
                    .unwrap_or_default();
                let same_pk =
                    |pv: &std::collections::HashMap<String, rusqlite::types::Value>| -> bool {
                        match &sc.next_value {
                            Some(next) if !pk_cols.is_empty() => pk_cols.iter().all(|col| {
                                pv.get(col).cloned().unwrap_or(rusqlite::types::Value::Null)
                                    == next
                                        .get(col)
                                        .cloned()
                                        .unwrap_or(rusqlite::types::Value::Null)
                            }),
                            _ => false,
                        }
                    };

                let mut edit_old_row = None;
                for prev_row in &sc.prev_values {
                    if same_pk(prev_row) {
                        edit_old_row = Some(sqlite_value_to_row(prev_row, col_types));
                    } else {
                        // A different-PK prev row displaced by this change is a
                        // unique-conflict deletion — TS `#conflictRowsDeleted`,
                        // counted only when the change carries a nextValue
                        // (pipeline-driver.ts:755-757).
                        if sc.next_value.is_some() {
                            crate::otel_metrics::record_conflict_row_deleted();
                        }
                        let change = crate::ivm::source::make_source_change_remove(
                            sqlite_value_to_row(prev_row, col_types),
                        );
                        let push_reset = {
                            let _t = crate::perf_trace::scope("advance.push");
                            push_source_change(
                                &self.sources,
                                &self.pipelines,
                                &sc.table,
                                change,
                                &self.primary_keys,
                                &self.table_specs,
                                &mut on_row_change,
                            )
                        };
                        if let Some(reset) = push_reset {
                            return Err(crate::snapshotter::DiffError::Reset(
                                crate::snapshotter::ResetPipelinesSignal {
                                    reason: "scalar-subquery",
                                    msg: reset.to_string(),
                                },
                            ));
                        }
                    }
                }

                if let Some(next) = &sc.next_value {
                    let row = sqlite_value_to_row(next, col_types);
                    let change = if let Some(old_row) = edit_old_row {
                        crate::ivm::source::make_source_change_edit(row, old_row)
                    } else {
                        crate::ivm::source::make_source_change_add(row)
                    };
                    let push_reset = {
                        let _t = crate::perf_trace::scope("advance.push");
                        push_source_change(
                            &self.sources,
                            &self.pipelines,
                            &sc.table,
                            change,
                            &self.primary_keys,
                            &self.table_specs,
                            &mut on_row_change,
                        )
                    };
                    if let Some(reset) = push_reset {
                        return Err(crate::snapshotter::DiffError::Reset(
                            crate::snapshotter::ResetPipelinesSignal {
                                reason: "scalar-subquery",
                                msg: reset.to_string(),
                            },
                        ));
                    }
                }

                // Post-push gate check, WHILE the current-change window is still
                // set (TS pipeline-driver.ts: `#shouldAdvanceYieldMaybeAbortAdvance`
                // runs inside the `currentChangeStartMs` scope, after the inner
                // `pos++` finally). This is the primary firing site of the
                // slow-current-change arm: a change whose cost lands in operator
                // compute (not in sampled row fetches) is only observable here,
                // and once the advance is >=80% complete the late-finish
                // exception suppresses the other arms — without this check a
                // single pathological late change escapes reset entirely.
                // `set_pos(pos)` first: `pos` was already incremented for this
                // change, matching TS's post-increment value at this check.
                advance_gate.set_pos(pos);
                if let Some(reset) = advance_gate.advance_reset() {
                    return Err(advance_reset_error(
                        reset,
                        advance_gate.elapsed_ms(),
                        total_hydration_time_ms,
                        pos,
                        num_changes,
                    ));
                }

                // Change boundary: the slow-current-change arm no longer applies
                // until the next change's push begins. TS resets
                // `currentChangeStartMs = undefined` after each change.
                advance_gate.clear_current_change();

                // Per-fetch arm: a fetch during this change's push blew the budget
                // and ended its stream early (truncated — discarded on rehydrate).
                // Surface it as the same advancement-timeout reset the per-change
                // arm produces, so we rehydrate at head rather than emit a partial.
                if let Some(latched) = advance_gate.tripped_reset() {
                    // Attribute the arm that latched mid-fetch instead of a
                    // generic "timed out mid-fetch" (TS throws arm-specific
                    // messages immediately).
                    return Err(advance_reset_error(
                        latched,
                        advance_gate.elapsed_ms(),
                        advance_gate.budget_ms(),
                        pos,
                        num_changes,
                    ));
                }

                if cancellation_token.is_cancelled() {
                    return Err(crate::snapshotter::DiffError::Other(
                        "advance cancelled before all changes were delivered".to_string(),
                    ));
                }

                // This change fully processed — record its advance time (TS
                // `#advanceTime.recordMs(elapsed, {table})` at pipeline-driver.ts).
                crate::otel_metrics::record_ivm_advance(
                    &sc.table,
                    change_timer.elapsed().as_secs_f64() * 1000.0,
                );
                Ok(())
            });

        // Restore every TableSource to the CURR (head) snapshot for subsequent
        // reads (incremental fetches + next hydration), on every path — matches
        // TS `table.setDB(curr.db.db)` after the change loop.
        {
            let _t = crate::perf_trace::scope("advance.setdb");
            for source in self.sources.values() {
                source.borrow_mut().set_snapshot_db(curr_conn.clone());
            }
        }

        // Cancellation can land after the final diff callback (or while its
        // final TSFN delivery is blocked). Never translate that race into Ok.
        if result.is_ok() && cancellation_token.is_cancelled() {
            result = Err(crate::snapshotter::DiffError::Other(
                "advance cancelled before all changes were delivered".to_string(),
            ));
        }

        let perf_elapsed_ms = perf_timer.elapsed().as_secs_f64() * 1000.0;
        match result {
            Ok(()) => {
                crate::perf_trace::report("advance", perf_elapsed_ms);
                Ok(AdvanceToHeadResult {
                    version: new_version,
                    num_changes,
                    aborted: false,
                    reset_reason: None,
                    reset_msg: None,
                })
            }
            Err(crate::snapshotter::DiffError::Reset(sig)) => {
                // Every breaker trip (advancement-timeout / scalar-subquery /
                // schema / permissions / truncation reset) logs its own
                // breakdown before returning.
                crate::perf_trace::report("advance-TRIPPED", perf_elapsed_ms);
                Ok(AdvanceToHeadResult {
                    version: new_version,
                    num_changes: pos,
                    aborted: true,
                    reset_reason: Some(sig.reason.to_string()),
                    reset_msg: Some(sig.msg),
                })
            }
            // Hard errors, including InvalidDiff, propagate to teardown exactly
            // as they do in PipelineDriver. They must not be smoothed into a
            // recoverable reset.
            Err(e) => Err(e),
        }
    }

    /// Advance: push source changes through all pipelines.
    /// Delegates to advance_streaming — single code path.
    pub fn advance(&mut self, changes: &[(String, SourceChange)]) -> Vec<RowChange> {
        let mut all_row_changes = Vec::new();
        let _completed = self.advance_streaming(changes, |rc| {
            all_row_changes.push(rc.clone());
        });
        all_row_changes
    }

    /// Resolve scalar subqueries in the AST before building the pipeline.
    /// Port of TS `#resolveScalarSubqueries`.
    ///
    /// Simple scalar subqueries (flagged `scalar` and equality-constrained on
    /// all columns of a unique key) are pre-resolved to literal values by
    /// building + fetching the subquery pipeline against the live sources. The
    /// built pipeline is retained as a live companion (returned in
    /// `ScalarResolveOut.companions`) so a monitoring output can later detect
    /// a value change on advance. Matched rows are returned in
    /// `companion_rows` for emission on hydrate.
    fn resolve_scalar_subqueries(&self, ast: &Ast) -> ScalarResolveOut {
        use crate::sqlite::resolve_scalar_subqueries::{
            ScalarExecutor, TableSpecWithUniqueKeys, resolve_simple_scalar_subqueries,
        };

        // Build the unique-key table specs from the engine's known keys.
        let table_specs: HashMap<String, TableSpecWithUniqueKeys> = self
            .unique_keys
            .iter()
            .map(|(t, keys)| {
                (
                    t.clone(),
                    TableSpecWithUniqueKeys {
                        unique_keys: keys.clone(),
                    },
                )
            })
            .collect();

        let sources = &self.sources;
        let primary_keys = &self.primary_keys;
        let enable_not_exists = self.enable_not_exists;

        // Collected during resolution by the executor closure (Fn → interior
        // mutability). `companion_rows`: matched (table, primary_key, row) for
        // hydrate. `companions`: the built live pipelines + resolved values.
        let companion_rows: RefCell<Vec<(String, Vec<String>, Row)>> = RefCell::new(Vec::new());
        let companions: RefCell<Vec<CompanionBuilt>> = RefCell::new(Vec::new());

        let executor: ScalarExecutor = Box::new(|subquery_ast: &Ast, child_field: &str| {
            // Build the subquery pipeline against the live sources (mirrors the
            // main-hydrate build path). complete_ordering first so the pipeline
            // has a deterministic sort like every other built pipeline.
            let completed = complete_ordering(subquery_ast, &|table: &str| {
                primary_keys.get(table).cloned().unwrap_or_default()
            });
            let mut delegate = EngineDelegate {
                sources,
                enable_not_exists,
            };
            let input = build_pipeline(&completed, &mut delegate);

            // Consume the full stream (the subquery is at-most-one-row) and
            // take the first node. Mirrors TS `for (const n of skipYields(...))
            // node ??= n`.
            let mut first: Option<crate::ivm::data::Node> = None;
            let stream = input.borrow().fetch(&Default::default());
            for node in crate::ivm::stream::skip_yields(stream) {
                if first.is_none() {
                    first = Some(node);
                }
            }

            match first {
                None => {
                    // No row matched → TS `undefined`. Keep the companion alive
                    // so a future insert that creates the row is detected.
                    companions.borrow_mut().push(CompanionBuilt {
                        input: input.clone(),
                        table: subquery_ast.table.clone(),
                        child_field: child_field.to_string(),
                        resolved_value: None,
                        resolved_undefined: true,
                    });
                    (None, false)
                }
                Some(node) => {
                    // TS: (node.row[childField] as LiteralValue) ?? null.
                    let value = match node.row.get(child_field) {
                        None | Some(Value::Null) => None,
                        Some(v) => Some(v.clone()),
                    };
                    // Capture the subquery table's primary key from the
                    // companion pipeline's OWN schema. This is the same source
                    // of truth the Streamer uses for the main-hydrate path
                    // (`schema.primary_key`, streamer/mod.rs), and it is always
                    // present because a companion is only ever produced for a
                    // table that has a registered source (is_simple_subquery
                    // requires the table's unique keys, which are registered
                    // together with its source/PK). TS keys the EXISTS companion
                    // row by this same primary key — so the emitted rowKey is
                    // always well-formed, never `{}`.
                    let companion_pk = input.borrow().get_schema().primary_key.clone();
                    companion_rows.borrow_mut().push((
                        subquery_ast.table.clone(),
                        companion_pk,
                        node.row.clone(),
                    ));
                    companions.borrow_mut().push(CompanionBuilt {
                        input: input.clone(),
                        table: subquery_ast.table.clone(),
                        child_field: child_field.to_string(),
                        resolved_value: value.clone(),
                        resolved_undefined: false,
                    });
                    (value, true)
                }
            }
        });

        let result = resolve_simple_scalar_subqueries(ast, &table_specs, &executor);
        drop(executor);

        ScalarResolveOut {
            ast: result.ast,
            companion_rows: companion_rows.into_inner(),
            companions: companions.into_inner(),
        }
    }

    /// List active query IDs.
    pub fn pipeline_query_ids(&self) -> Vec<String> {
        self.pipelines.iter().map(|p| p.query_id.clone()).collect()
    }

    /// The scalar-resolved logical AST exposed by PipelineDriver. This is kept
    /// separate from the completed physical ordering used to build the graph.
    pub fn transformed_ast(&self, query_id: &str) -> Option<Ast> {
        self.pipelines
            .iter()
            .find(|pipeline| pipeline.query_id == query_id)
            .map(|pipeline| pipeline.transformed_ast.clone())
    }

    /// Check if the engine has been initialized (has sources registered).
    pub fn initialized(&self) -> bool {
        !self.sources.is_empty()
    }

    /// Get the cancellation token (for wiring into SQLite progress handler).
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    /// Cancel any in-progress advance/hydrate.
    pub fn cancel(&self) {
        self.cancellation_token.cancel();
    }

    /// Reset the engine: clear all pipelines and sources.
    /// Port of TS `reset()`.
    pub fn reset(&mut self) {
        for entry in self.pipelines.drain(..) {
            entry.pipeline.borrow_mut().destroy();
            for c in &entry.companions {
                c.input.borrow_mut().destroy();
            }
        }
        self.sources.clear();
        self.row_set_signatures.clear();
        self.primary_keys.clear();
        self.table_specs.clear();
        self.unique_keys.clear();
    }

    /// Get a row by table name and primary key.
    /// Port of TS `getRow()`.
    pub fn get_row(&self, table: &str, pk: &[(String, Value)]) -> Option<Row> {
        let source = self.sources.get(table)?;
        let data = source.borrow().get_row(pk)?;
        Some(data)
    }

    /// Get the sources map (for NAPI to access).
    pub fn sources(&self) -> &HashMap<String, Shared<dyn Source>> {
        &self.sources
    }

    /// Destroy the engine and release all resources.
    /// Port of TS `destroy()`.
    pub fn destroy(&mut self) {
        for entry in self.pipelines.drain(..) {
            entry.pipeline.borrow_mut().destroy();
            for c in &entry.companions {
                c.input.borrow_mut().destroy();
            }
        }
        self.sources.clear();
        self.row_set_signatures.clear();
    }
}

/// Break the operator graph's `Rc` cycles on teardown.
///
/// IVM operators form strong `Rc` cycles: each operator holds its `input`
/// (down-edge) and, via `set_output`, the upstream input holds a strong `output`
/// back-edge to the downstream operator. Only `destroy()` breaks these. When a
/// client group is torn down (e.g. the last client disconnects), the owning
/// `SyncEngine`/`CgState` simply *drops* the engine — nothing calls `destroy()`
/// explicitly — so without this impl the whole operator subtree (Join / Exists /
/// TableSourceInput / TableConnection, plus the SQLite connection each
/// TableConnection pins) leaks on every CG churn. That is the G6 RSS climb: the
/// `TableSource` root drops (it is not in the cycle) but its registered
/// inputs/connections and the operators above them are retained forever.
///
/// `destroy()` is idempotent — the explicit calls in `init`/reset drain the
/// pipelines, so a later drop-time call is a no-op — so this composes safely
/// with the existing teardown paths.
impl Drop for Engine {
    fn drop(&mut self) {
        self.destroy();
    }
}

// ---------------------------------------------------------------------------
// BuilderDelegate — complete implementation matching TS.
// ---------------------------------------------------------------------------

struct EngineDelegate<'a> {
    sources: &'a HashMap<String, Shared<dyn Source>>,
    enable_not_exists: bool,
}

impl<'a> BuilderDelegate for EngineDelegate<'a> {
    fn get_source(&self, table_name: &str) -> Option<Shared<dyn Source>> {
        self.sources.get(table_name).cloned()
    }

    fn enable_not_exists(&self) -> bool {
        self.enable_not_exists
    }

    fn create_storage(&mut self) -> Shared<dyn Storage> {
        Rc::new(RefCell::new(
            crate::ivm::memory_storage::MemoryStorage::new(),
        ))
    }
}

// ---------------------------------------------------------------------------
// Helper functions for advance_to_head_stream
// ---------------------------------------------------------------------------

/// Convert a rusqlite Value map to an IVM Row.
/// Convert a raw SQLite row (from the snapshotter diff) to an IVM Row, coercing
/// each column by its declared type via the SAME path the fetch uses
/// (`sqlite_value_to_ivm`) — so a boolean column becomes `Bool`, not `F64(1)`,
/// on both the hydrate AND advance paths. Without `col_types`, values pass
/// through unchanged (matching an untyped source).
fn sqlite_value_to_row(
    map: &std::collections::HashMap<String, rusqlite::types::Value>,
    col_types: Option<&std::collections::HashMap<String, crate::ivm::schema::ColumnType>>,
) -> crate::ivm::data::Row {
    let row: crate::ivm::data::Row = Arc::new(
        map.iter()
            .map(|(k, v)| {
                let ct = col_types.and_then(|c| c.get(k));
                let val =
                    crate::sqlite::table_source::sqlite_value_to_ivm(Ok(v.clone()), ct, "", k);
                (k.clone(), val)
            })
            .collect(),
    );
    row
}

/// Push a source change through all pipelines and stream RowChanges.
fn push_source_change(
    sources: &HashMap<String, Shared<dyn Source>>,
    pipelines: &[PipelineEntry],
    table: &str,
    change: SourceChange,
    primary_keys: &HashMap<String, Vec<String>>,
    table_specs: &HashMap<String, crate::streamer::TableSpecInfo>,
    on_row_change: &mut impl FnMut(&RowChange),
) -> Option<ScalarResetError> {
    if let Some(source) = sources.get(table) {
        if !source.borrow().has_active_connections() {
            return None;
        }
        // Clear collectors (and companion monitors) for this push.
        for entry in pipelines {
            entry.collector.borrow_mut().changes.clear();
            entry.collector.borrow_mut().row_changes.clear();
            for c in &entry.companions {
                let mut output = c.output.borrow_mut();
                output.changes.clear();
                output.reset = None;
            }
        }

        let _pipeline_changes = source.borrow_mut().push(change);
        if let Some(reset) = take_scalar_reset(pipelines) {
            // A reset invalidates every row produced by this push. Do not leak
            // a partial change before the reset sentinel.
            for entry in pipelines {
                entry.collector.borrow_mut().row_changes.clear();
                for c in &entry.companions {
                    c.output.borrow_mut().changes.clear();
                }
            }
            return Some(reset);
        }

        // Collect and stream RowChanges from each pipeline.
        for entry in pipelines {
            let row_changes = {
                let _t = crate::perf_trace::scope("advance.collect");
                std::mem::take(&mut entry.collector.borrow_mut().row_changes)
            };
            for rc in row_changes {
                let delivery_start = Instant::now();
                {
                    let _t = crate::perf_trace::scope("deliver.row");
                    on_row_change(&rc);
                }
                crate::advance_gate::exclude_current(delivery_start.elapsed());
            }
            // Surviving companion changes (resolved value unchanged) stream
            // under the owning query, per TS companion accumulation.
            for c in &entry.companions {
                let streamed: Vec<RowChange> = {
                    let _t = crate::perf_trace::scope("advance.collect");
                    let cc: Vec<Change> = std::mem::take(&mut c.output.borrow_mut().changes);
                    if cc.is_empty() {
                        Vec::new()
                    } else {
                        let mut streamer = Streamer::new(primary_keys.clone(), table_specs.clone());
                        streamer.accumulate(&entry.query_id, &c.schema, &cc);
                        streamer.stream()
                    }
                };
                for rc in streamed {
                    let delivery_start = Instant::now();
                    {
                        let _t = crate::perf_trace::scope("deliver.row");
                        on_row_change(&rc);
                    }
                    crate::advance_gate::exclude_current(delivery_start.elapsed());
                }
            }
        }
    }
    None
}

/// Format a tripped [`crate::advance_gate::AdvanceReset`] arm into the
/// advancement-timeout reset error. Shared by the top-of-change and the
/// post-push (change-boundary) gate sites so both attribute the arm that fired.
fn advance_reset_error(
    reset: crate::advance_gate::AdvanceReset,
    elapsed_ms: f64,
    budget: f64,
    pos: usize,
    num_changes: usize,
) -> crate::snapshotter::DiffError {
    let msg = match reset {
        crate::advance_gate::AdvanceReset::SlowCurrentChange { current_change_ms } => format!(
            "Advancement aborted: current change took {:.0}ms (> hydration budget {:.0}ms) at {}/{}",
            current_change_ms, budget, pos, num_changes
        ),
        crate::advance_gate::AdvanceReset::Projected { projected_ms } => format!(
            "Advancement aborted: projected {:.0}ms exceeds hydration budget {:.0}ms at {}/{} ({:.0}ms elapsed)",
            projected_ms, budget, pos, num_changes, elapsed_ms
        ),
        crate::advance_gate::AdvanceReset::Timeout => format!(
            "Advancement timed out after {:.0}ms (budget: {:.0}ms, pos: {}/{})",
            elapsed_ms, budget, pos, num_changes
        ),
        crate::advance_gate::AdvanceReset::WallClockCeiling { wall_ms } => format!(
            "Advancement exceeded the wall-clock ceiling: {:.0}ms including delivery time \
             (ceiling: {:.0}ms, pos: {}/{})",
            wall_ms,
            crate::advance_gate::ADVANCE_WALL_CLOCK_CEILING_MS,
            pos,
            num_changes
        ),
    };
    crate::snapshotter::DiffError::Reset(crate::snapshotter::ResetPipelinesSignal {
        reason: "advancement-timeout",
        msg,
    })
}

fn take_scalar_reset(pipelines: &[PipelineEntry]) -> Option<ScalarResetError> {
    for entry in pipelines {
        for companion in &entry.companions {
            if let Some(reset) = companion.output.borrow_mut().reset.take() {
                return Some(reset);
            }
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Cancellation — Phase 4.
// ---------------------------------------------------------------------------

/// A cancellation token backed by an Arc<AtomicBool>.
/// Checked by the engine during advance and by SQLite's progress handler.
#[derive(Clone)]
pub struct CancellationToken {
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        CancellationToken {
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn reset(&self) {
        self.cancelled
            .store(false, std::sync::atomic::Ordering::Release);
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod scalar_reset_tests {
    use super::*;
    use crate::ivm::data::Node;
    use crate::ivm::memory_source::MemorySource;
    use crate::ivm::schema::ColumnType;

    struct UnusedPusher;

    impl InputBase for UnusedPusher {
        fn get_schema(&self) -> SourceSchema {
            panic!("schema is not used by CompanionOutput")
        }

        fn destroy(&mut self) {}
    }

    #[test]
    fn companion_value_change_records_reset_without_unwinding() {
        let mut output = CompanionOutput {
            table: "users".to_string(),
            child_field: "name".to_string(),
            resolved_value: Some(Value::Str(Arc::from("Alice"))),
            resolved_undefined: false,
            changes: Vec::new(),
            reset: None,
        };
        let row: Row = Arc::new(
            [
                ("id".to_string(), Value::Str(Arc::from("u1"))),
                ("name".to_string(), Value::Str(Arc::from("Alicia"))),
            ]
            .into_iter()
            .collect(),
        );

        output.push(Change::Add(Node::new(row)), &UnusedPusher);

        let reset = output.reset.expect("changed scalar must request a reset");
        assert_eq!(reset.table, "users");
        assert_eq!(reset.resolved, "Alice");
        assert_eq!(reset.new, "Alicia");
        assert!(output.changes.is_empty());
    }

    #[test]
    fn inactive_source_skips_invalid_change() {
        let source: Shared<dyn Source> = Rc::new(RefCell::new(MemorySource::new(
            "unqueried",
            HashMap::from([("id".to_string(), ColumnType::String { optional: false })]),
            vec!["id".to_string()],
        )));
        let sources = HashMap::from([("unqueried".to_string(), source)]);
        let missing_row = Arc::new(
            [("id".to_string(), Value::Str(Arc::from("missing")))]
                .into_iter()
                .collect(),
        );

        // MemorySource would panic on removal of a missing row if pushed. TS
        // has no TableSource for this table, so the change must be skipped.
        assert!(
            push_source_change(
                &sources,
                &[],
                "unqueried",
                crate::ivm::source::make_source_change_remove(missing_row),
                &HashMap::new(),
                &HashMap::new(),
                &mut |_| {},
            )
            .is_none()
        );
    }

    /// Pins `row_signature_unit` to the REAL TS `rowIDSignatureUnit`
    /// (`h64(rowIDString({schema:'', table, rowKey}))`). The unit is XOR-folded
    /// into `rowSetSignature`, which is PERSISTED to the shared CVR and compared
    /// across processes — so a non-TS hash forces mass re-hydration on any
    /// rolling/mixed/shadow deploy. Goldens from
    /// `agentic/parity/generate-row-signature-fixture.mjs` (drives real TS).
    /// Regression guard for the former `FxHasher` divergence.
    #[test]
    fn row_signature_unit_matches_ts_golden() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/agentic/parity/row-signature-fixture.json"
        );
        let bytes = std::fs::read(path)
            .unwrap_or_else(|e| panic!("failed to read row-signature fixture {path}: {e}"));
        let fixture: serde_json::Value =
            serde_json::from_slice(&bytes).expect("row-signature fixture is not valid JSON");
        let cases = fixture["cases"].as_array().expect("fixture.cases missing");
        assert!(!cases.is_empty());

        for case in cases {
            let table = case["table"].as_str().unwrap();
            let want: u64 = case["unit"].as_str().unwrap().parse().unwrap();
            // Build the ivm Row from the fixture rowKey (ivm Value ← plain JSON).
            let mut map: rustc_hash::FxHashMap<String, Value> = rustc_hash::FxHashMap::default();
            for (k, v) in case["rowKey"].as_object().unwrap() {
                map.insert(
                    k.clone(),
                    serde_json::from_value::<Value>(v.clone()).unwrap(),
                );
            }
            let row: Row = Arc::new(map);
            assert_eq!(
                row_signature_unit(table, &row),
                want,
                "row_signature_unit divergence for table {table:?} rowKey {:?}",
                case["rowKey"]
            );
        }
    }
}
