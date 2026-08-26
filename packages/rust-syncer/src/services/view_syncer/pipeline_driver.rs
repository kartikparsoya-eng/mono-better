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

use rust_ivm::engine::{Engine, QuerySpec, ScalarResetError};
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::{MemorySource, Source};
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
/// This ordering IS the fix for that silent sqlite-close leak: with
/// `snapshotter` declared after `sources`, the census warning in
/// `Snapshot::drop` fires only on true bypasses instead of on 100% of
/// teardowns. `destroy()` and `init_from_connection` mirror this order.
pub struct IvmPipelines {
    engine: Option<Engine>,
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

            let rc_source: Rc<RefCell<dyn Source>> = if let Some(conn) = &source_conn {
                let table_source =
                    TableSource::new(conn.clone(), &spec.table, columns, spec.primary_key.clone());
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
    pub fn remove_query(&mut self, query_id: &str) {
        if let Some(eng) = self.engine.as_mut() {
            eng.remove_query(query_id);
        }
        self.active_queries.remove(query_id);
        if self.query_asts.remove(query_id).is_some() {
            self.query_order.retain(|q| q != query_id);
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

        // A hydrate panic (e.g. a source-drift assert) must roll back the
        // partially-wired source connections before re-throwing, so a follow-up
        // rehydrate builds a clean graph — matching napi `HydrateAndSyncTask`.
        let checkpoint = eng.source_connection_checkpoint();
        let hydrated = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = eng.add_queries_streaming(&specs, |rc| on_row(rc));
        }));
        if let Err(payload) = hydrated {
            eng.rollback_source_connections(&checkpoint);
            std::panic::resume_unwind(payload);
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
}
