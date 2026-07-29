//! Coarse per-pipeline parallel hydrate (DESIGN §2, Phase 1).
//!
//! On a parallel-hydrate request the actor:
//! 1. Builds all pipelines (Phase 1: resolve scalar subqueries, build) —
//!    sequential because it mutates source connections.
//! 2. Extracts `Send` source specs from each `!Send` source (rows for
//!    `MemorySource`, db_path for `TableSource`) so workers can rebuild
//!    transient copies.
//! 3. Dispatches one task per pipeline to the `ParallelJob` worker pool. Each
//!    task builds a TRANSIENT pipeline from the AST + a fresh source (bound to
//!    a pooled connection or copied rows), fetches it, and streams `RowChange`s
//!    one at a time through the bounded channel.
//! 4. The actor drains task channels in dispatch order and calls `on_row_change`
//!    per `RowChange` immediately — true streaming, byte-identical to serial.
//! 5. Registers the ACTOR's pipelines (built in step 1) for advance.
//!
//! Workers see only `Send` data + their own connection. They physically cannot
//! reach the graph — enforced by types, not discipline (DESIGN §7).
//!
//! Any failure (pool exhaustion, version mismatch, worker panic, first error)
//! → serial fallback (S4). The actor emits ONE reset and re-runs serially.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::builder::ast::Ast;
use crate::builder::builder::{build_pipeline, BuilderDelegate};
use crate::engine::worker::{ParallelError, ParallelJob};
use crate::engine::CancellationToken;
use crate::ivm::change::{make_add_change, ChangeType};
use crate::ivm::data::Row;
use crate::ivm::memory_storage::MemoryStorage;
use crate::ivm::operator::{Input, OutputHandle, Shared};
use crate::ivm::schema::ColumnType;
use crate::ivm::source::{MemorySource, ParallelSourceSpec, Source};
use crate::sqlite::table_source::TableSource;
use crate::streamer::{RowChange, Streamer, TableSpecInfo};

/// A `Send` spec for one source, extractable from the `!Send` source so a worker
/// can rebuild a transient copy on its own thread.
#[derive(Clone)]
pub struct SourceSpec {
    pub table_name: String,
    pub columns: HashMap<String, ColumnType>,
    pub primary_key: Vec<String>,
    pub data: ParallelSourceSpec,
}

/// A `Send` task spec for one pipeline's parallel hydrate. The worker rebuilds
/// a transient pipeline from this spec, fetches it, and streams `RowChange`s.
pub struct HydrateTaskSpec {
    pub query_id: String,
    pub ast: Ast,
    pub source_specs: Vec<SourceSpec>,
    pub primary_keys: HashMap<String, Vec<String>>,
    pub table_specs: HashMap<String, TableSpecInfo>,
    pub enable_not_exists: bool,
    /// Companion rows to emit as ADDs after the main hydrate (same as serial).
    pub companion_rows: Vec<(String, Row)>,
}

/// Extract `Send` source specs from the engine's `!Send` sources. Only the
/// tables referenced by the ASTs are included (a query doesn't need every
/// table in the CG).
pub fn extract_source_specs(
    sources: &HashMap<String, Shared<dyn Source>>,
    referenced_tables: &std::collections::HashSet<String>,
) -> Vec<SourceSpec> {
    let mut specs = Vec::new();
    for table in referenced_tables {
        if let Some(source) = sources.get(table) {
            let src = source.borrow();
            specs.push(SourceSpec {
                table_name: src.table_name().to_string(),
                columns: src.column_types(),
                primary_key: src.primary_key().to_vec(),
                data: src.parallel_spec(),
            });
        }
    }
    specs
}

/// Collect all table names referenced by an AST (the root table + all related
/// subquery tables, recursively).
pub fn referenced_tables(ast: &Ast) -> std::collections::HashSet<String> {
    let mut tables = std::collections::HashSet::new();
    collect_tables(ast, &mut tables);
    tables
}

fn collect_tables(ast: &Ast, tables: &mut std::collections::HashSet<String>) {
    tables.insert(ast.table.clone());
    for rel in &ast.related {
        collect_tables(&rel.subquery, tables);
    }
    // Also scan the WHERE clause — correlated subquery conditions reference
    // their own tables (e.g., EXISTS comment WHERE ...). Without this, the
    // worker would be missing the subquery's source → 0 rows.
    if let Some(ref wc) = ast.where_clause {
        collect_tables_from_condition(wc, tables);
    }
}

fn collect_tables_from_condition(
    cond: &crate::builder::ast::Condition,
    tables: &mut std::collections::HashSet<String>,
) {
    use crate::builder::ast::Condition;
    match cond {
        Condition::Simple(_) => {}
        Condition::And(conds) | Condition::Or(conds) => {
            for c in conds {
                collect_tables_from_condition(c, tables);
            }
        }
        Condition::CorrelatedSubquery(csq) => {
            collect_tables(&csq.related.subquery, tables);
        }
    }
}

/// A `BuilderDelegate` that creates fresh sources from `SourceSpec`s on the
/// worker thread. The sources are `Rc<RefCell<>>` (thread-local, `!Send`) —
/// created, used, and dropped entirely within the worker thread.
pub(crate) struct WorkerDelegate {
    sources: HashMap<String, Shared<dyn Source>>,
    enable_not_exists: bool,
}

impl WorkerDelegate {
    pub(crate) fn new(specs: Vec<SourceSpec>, enable_not_exists: bool) -> Result<Self, String> {
        let mut sources: HashMap<String, Shared<dyn Source>> = HashMap::new();
        for spec in specs {
            let source: Shared<dyn Source> = match &spec.data {
                ParallelSourceSpec::Memory { rows } => {
                    let mut ms = MemorySource::new(
                        &spec.table_name,
                        spec.columns.clone(),
                        spec.primary_key.clone(),
                    );
                    for row in rows {
                        ms.add_row((**row).clone());
                    }
                    Rc::new(RefCell::new(ms))
                }
                ParallelSourceSpec::Sqlite { db_path } => {
                    let conn = rusqlite::Connection::open_with_flags(
                        db_path,
                        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                            | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                            | rusqlite::OpenFlags::SQLITE_OPEN_URI,
                    )
                    .map_err(|e| format!("worker: open sqlite {}: {}", db_path, e))?;
                    // Match Postgres semantics and the main napi connection:
                    // LIKE/ILIKE must be case-sensitive.
                    let _ = conn.pragma_update(None, "case_sensitive_like", "ON");
                    let ts = TableSource::new(
                        Rc::new(RefCell::new(conn)),
                        &spec.table_name,
                        spec.columns.clone(),
                        spec.primary_key.clone(),
                    );
                    Rc::new(RefCell::new(ts))
                }
            };
            sources.insert(spec.table_name, source);
        }
        Ok(WorkerDelegate {
            sources,
            enable_not_exists,
        })
    }
}

impl BuilderDelegate for WorkerDelegate {
    fn get_source(&self, table_name: &str) -> Option<Shared<dyn Source>> {
        self.sources.get(table_name).cloned()
    }

    fn enable_not_exists(&self) -> bool {
        self.enable_not_exists
    }

    fn create_storage(&mut self) -> Shared<dyn crate::ivm::operator::Storage> {
        Rc::new(RefCell::new(MemoryStorage::new()))
    }
}

/// Run coarse parallel hydrate: one task per pipeline, streaming `RowChange`s
/// to `on_row_change` in dispatch order (byte-identical to serial).
///
/// On any failure (pool exhaustion, version mismatch, worker panic, first
/// error), returns `Err` → the caller falls back to serial (S4).
///
/// `workers` = bounded pool size (≤ cores). `per_task_bound` = bounded channel
/// capacity per task (L5 backpressure).
pub fn parallel_hydrate_streaming<F: FnMut(&RowChange)>(
    specs: Vec<HydrateTaskSpec>,
    cancel: CancellationToken,
    workers: usize,
    per_task_bound: usize,
    mut on_row_change: F,
) -> Result<(), ParallelError<String>> {
    let n = specs.len();
    if n == 0 {
        return Ok(());
    }
    let job: ParallelJob<RowChange, String> = ParallelJob::new(workers, per_task_bound);

    // Build task closures. Each is `Send` (captures only owned `Send` data).
    let tasks: Vec<Box<dyn FnOnce(&crate::engine::worker::WorkerScope, &dyn Fn(RowChange)) -> Result<(), String> + Send>> = specs
        .into_iter()
        .map(|spec| {
            Box::new(move |scope: &crate::engine::worker::WorkerScope, sink: &dyn Fn(RowChange)| -> Result<(), String> {
                if scope.aborted() {
                    return Ok(());
                }
                // Build the transient pipeline on this worker thread. The AST
                // is already resolved + complete_ordering'd by the actor — do
                // NOT re-apply complete_ordering (not idempotent).
                let mut delegate = WorkerDelegate::new(spec.source_specs.clone(), spec.enable_not_exists)?;
                let pipeline = build_pipeline(&spec.ast, &mut delegate);

                let collector = Rc::new(RefCell::new(crate::ivm::source::CollectOutput::new()));
                pipeline.borrow().set_output(collector.clone() as OutputHandle);

                let schema = pipeline.borrow().get_schema();

                // Fetch → stream nodes → stream RowChanges one at a time.
                let stream = pipeline.borrow().fetch(&Default::default());
                let mut streamer = Streamer::new(spec.primary_keys.clone(), spec.table_specs.clone());

                for node in crate::ivm::stream::skip_yields(stream) {
                    if scope.aborted() {
                        // Clean up the transient pipeline and stop.
                        pipeline.borrow_mut().destroy();
                        return Ok(());
                    }
                    let change = make_add_change(node);
                    streamer.accumulate(&spec.query_id, &schema, std::slice::from_ref(&change));
                    for rc in streamer.stream() {
                        if scope.aborted() {
                            pipeline.borrow_mut().destroy();
                            return Ok(());
                        }
                        sink(rc);
                    }
                }

                // Companion rows: raw ADD RowChanges for each matched subquery row.
                for (table, row) in &spec.companion_rows {
                    if scope.aborted() {
                        pipeline.borrow_mut().destroy();
                        return Ok(());
                    }
                    let pk = spec.primary_keys.get(table).cloned().unwrap_or_default();
                    let row_key = crate::streamer::get_row_key(&pk, row);
                    sink(RowChange {
                        change_type: ChangeType::Add,
                        query_id: spec.query_id.clone(),
                        table: table.clone(),
                        row_key,
                        row: Some(row.clone()),
                        is_hidden: false,
                    });
                }

                // Drop the transient pipeline — it is NOT registered for advance.
                pipeline.borrow_mut().destroy();
                Ok(())
            }) as Box<_>
        })
        .collect();

    job.run_streaming(tasks, cancel, |rc| on_row_change(&rc))
}
