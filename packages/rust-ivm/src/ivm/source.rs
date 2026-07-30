//! Source trait + MemorySource — port of `zql/src/ivm/source.ts` + `memory-source.ts`.
//!
//! The `Source` trait abstracts over MemorySource (in-memory / test) and
//! TableSource (SQLite-backed / production), matching TS `Source` interface.
//!
//! MemorySource supports two modes:
//! - In-memory mode (default): all rows stored in a Vec<Row>. Used by tests.
//! - SQLite-backed mode: rows read from SQLite on-demand via `set_db()`.
//!   No preloading needed — fetch() queries SQLite with constraints.

use std::cell::RefCell;
use std::rc::Rc;
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::builder::ast::Condition;
use crate::ivm::change::{
    make_add_change, make_edit_change, make_remove_change, Change, SourceChange,
};
use crate::ivm::constraint::{
    constraint_matches_primary_key, constraint_matches_row, Constraint,
};
use crate::ivm::data::{
    make_comparator, values_equal, Comparator, Node, Row, SortOrder, Value,
};
use crate::ivm::filter_push::filter_push;
use crate::ivm::operator::{
    Basis, FetchRequest, Input, InputBase, Output, OutputHandle, Shared, Start,
};
use crate::ivm::schema::{SourceSchema, System};
use crate::ivm::stream::{StreamItem, empty_stream, from_vec, NodeStream};

// ---------------------------------------------------------------------------
// Source trait — port of TS `Source` interface (source.ts:42).
// ---------------------------------------------------------------------------

/// A source is the root data source of the pipeline. Abstracts over
/// MemorySource (in-memory/test) and TableSource (SQLite/production).
pub trait Source {
    fn table_name(&self) -> &str;
    fn primary_key(&self) -> &[String];

    /// Connect a new downstream consumer.
    fn connect(
        &mut self,
        sort: Option<SortOrder>,
        filter_condition: Option<Condition>,
        filter_predicate: Option<Arc<dyn Fn(&Row) -> bool>>,
        split_edit_keys: Option<Vec<String>>,
    ) -> Shared<dyn Input>;

    /// Push a source change through all connections.
    fn push(&mut self, change: SourceChange) -> Vec<Change>;

    /// Push with parallel fan-out across connection groups (inter-CG).
    fn push_parallel(&mut self, change: SourceChange) -> Vec<Change>;

    /// Gen push — yields per-connection results.
    fn gen_push(&mut self, change: SourceChange) -> Vec<Change>;

    /// Get a row by primary key.
    fn get_row(&self, pk: &[(String, Value)]) -> Option<Row>;

    /// Set the SQLite database path for SQLite-backed fetch.
    fn set_db_path(&mut self, _path: &str) {}

    /// Point this source at a specific pinned snapshot connection. Used during
    /// advance to read PREV while changes are processed and CURR afterwards
    /// (matches TS `pipeline-driver.ts` `table.setDB(...)`). MemorySource has no
    /// SQLite connection, so this is a no-op there.
    fn set_snapshot_db(&mut self, _db: std::rc::Rc<std::cell::RefCell<rusqlite::Connection>>) {}

    /// Column types for this table, so the advance path can coerce raw SQLite
    /// values (Integer/Real → Bool for boolean cols, Text → Json) identically to
    /// the fetch path. Default empty (untyped → pass-through).
    fn column_types(&self) -> HashMap<String, crate::ivm::schema::ColumnType> {
        HashMap::new()
    }

    /// Set the next connect group (for parallel push grouping).
    fn set_next_connect_group(&mut self, _group: &str) {}

    /// Set parallel push mode.
    fn set_parallel(&mut self, _enabled: bool, _threshold: usize) {}

    /// Attach the frame-pinned parallel-read pool (read-level parallelism).
    /// The source uses it to fan multi-constraint hydrate reads out across the
    /// co-pinned connections; `MemorySource` has no SQLite reads → no-op.
    /// See DESIGN-read-parallelism.md.
    fn set_read_pool(
        &mut self,
        _pool: std::sync::Arc<crate::snapshotter::read_pool::FramePinnedPool>,
    ) {}
}


/// Connection: a downstream consumer of the source.
pub struct Connection {
    pub sort: Option<SortOrder>,
    pub split_edit_keys: Option<Vec<String>>,
    pub compare_rows: Comparator,
    pub filter_condition: Option<Condition>,
    pub filter_predicate: Option<Arc<dyn Fn(&Row) -> bool>>,
    pub last_pushed_epoch: usize,
    pub group: String,
    pub output: Option<OutputHandle>,
}

pub type SharedOverlay = Rc<RefCell<Option<(usize, SourceChange)>>>;

/// RAII guard that clears the overlay on drop, even if a panic occurs.
struct OverlayGuard(SharedOverlay);
impl Drop for OverlayGuard {
    fn drop(&mut self) {
        *self.0.borrow_mut() = None;
    }
}

pub type SharedData = Rc<RefCell<Vec<Row>>>;

pub type ConnPool = Rc<RefCell<Vec<rusqlite::Connection>>>;

pub struct MemorySource {
    table_name: String,
    columns: HashMap<String, crate::ivm::schema::ColumnType>,
    column_names: Vec<String>,
    primary_key: Vec<String>,
    primary_index_sort: SortOrder,
    data: SharedData,
    comparator: Comparator,
    connections: Vec<Shared<Connection>>,
    overlay: SharedOverlay,
    push_epoch: usize,
    next_connect_group: String,
    parallel: bool,
    parallel_threshold: usize,
    db_path: Option<String>,
    db_conn: Rc<RefCell<Option<rusqlite::Connection>>>,
    /// Cross-thread interrupt handle for `db_conn` (seam 1). Installed at open
    /// so the connection is interruptible from another thread; may be
    /// registered with a `JobWatchdog` when this source runs under one.
    /// `None` when the source is pure in-memory (no SQLite backing) or the
    /// connection failed to open.
    interrupt_handle: Option<rusqlite::InterruptHandle>,
    /// PK-keys removed during the CURRENT advance. On a SQLite-backed source the
    /// fetch reads the PREV snapshot, which still contains a same-advance-removed
    /// row; the in-memory `data` merge can't distinguish "removed this advance"
    /// from "never pushed" (data is a partial cache), so we track removals
    /// explicitly and drop them from the merged fetch. Cleared each advance in
    /// `set_snapshot_db`. Shared (like `data`) so the connection's `SourceInput`
    /// fetch sees the same set.
    removed_this_advance: Rc<RefCell<std::collections::HashSet<String>>>,
}

impl MemorySource {
    pub fn new(table_name: &str, columns: HashMap<String, crate::ivm::schema::ColumnType>, primary_key: Vec<String>) -> Self {
        let primary_index_sort: SortOrder = Arc::new(
            primary_key.iter().map(|k| [k.clone(), "asc".to_string()]).collect(),
        );
        let comparator = make_comparator(primary_index_sort.clone(), false);
        let column_names: Vec<String> = columns.keys().cloned().collect();

        MemorySource {
            table_name: table_name.to_string(),
            columns,
            column_names,
            primary_key: primary_key.clone(),
            primary_index_sort,
            data: Rc::new(RefCell::new(Vec::new())),
            comparator,
            connections: Vec::new(),
            overlay: Rc::new(RefCell::new(None)),
            push_epoch: 0,
            next_connect_group: "default".to_string(),
            parallel: false,
            parallel_threshold: 2,
            db_path: None,
            db_conn: Rc::new(RefCell::new(None)),
            interrupt_handle: None,
            removed_this_advance: Rc::new(RefCell::new(std::collections::HashSet::new())),
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    pub fn set_next_connect_group(&mut self, group: &str) {
        self.next_connect_group = group.to_string();
    }

    pub fn set_parallel(&mut self, enabled: bool, threshold: usize) {
        self.parallel = enabled;
        self.parallel_threshold = threshold;
    }

    /// Set the SQLite database path and open a dedicated read-only connection.
    /// One connection per source, opened once and reused for all fetches.
    /// Matches TS (one better-sqlite3 Database per syncer) and Go (one *sql.Conn per Source).
    pub fn set_db_path(&mut self, path: &str) {
        self.db_path = Some(path.to_string());
        match rusqlite::Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX
                | rusqlite::OpenFlags::SQLITE_OPEN_URI,
        ) {
            Ok(c) => {
                let _ = c.busy_timeout(std::time::Duration::from_millis(5000));
                let _ = c.execute_batch("PRAGMA case_sensitive_like = ON; PRAGMA query_only = ON;");
                // Seam 1: install a cross-thread interrupt handle at every
                // connection open so the source's fetch connection is
                // interruptible. Phase 1's worker pool reuses this chokepoint.
                let handle = crate::sqlite::install_interrupt(&c);
                self.interrupt_handle = Some(handle);
                *self.db_conn.borrow_mut() = Some(c);
            }
            Err(e) => {
                eprintln!("[rust-ivm] Failed to open connection for {}: {}", self.table_name, e);
            }
        }
    }

    /// Check if this source is SQLite-backed.
    pub fn has_db(&self) -> bool {
        self.db_path.is_some()
    }

    pub fn add_row(&mut self, row_data: FxHashMap<String, Value>) {
        let r = Arc::new(row_data);
        let data = self.data.clone();
        let comparator = self.comparator.clone();
        let pk = self.primary_key.clone();
        let mut data = data.borrow_mut();
        // Replace any existing row with the same primary key, so the in-memory
        // source stays consistent when tests mutate rows between hydrates.
        if let Some(idx) = data.iter().position(|existing| {
            pk.iter().all(|k| {
                let a = existing.get(k).cloned().unwrap_or(Value::Null);
                let b = r.get(k).cloned().unwrap_or(Value::Null);
                values_equal(&a, &b)
            })
        }) {
            data[idx] = r;
            return;
        }
        let pos = data.partition_point(|existing| {
            comparator(existing, &r) == CmpOrdering::Less
        });
        data.insert(pos, r);
    }

    fn has(&self, row: &Row) -> bool {
        let data = self.data.borrow();
        data.iter().any(|existing| {
            self.primary_key.iter().all(|pk| {
                let a = existing.get(pk).cloned().unwrap_or(Value::Null);
                let b = row.get(pk).cloned().unwrap_or(Value::Null);
                values_equal(&a, &b)
            })
        })
    }

    /// Get a row by primary key.
    /// Port of TS `TableSource.getRow()`.
    pub fn get_row(&self, pk: &[(String, Value)]) -> Option<Row> {
        let data = self.data.borrow();
        data.iter().find(|existing| {
            pk.iter().all(|(col, val)| {
                let a = existing.get(col).cloned().unwrap_or(Value::Null);
                values_equal(&a, val)
            })
        }).cloned()
    }

    /// Get all rows (for preloading into the engine).
    pub fn all_rows(&self) -> Vec<Row> {
        self.data.borrow().clone()
    }

    /// Connect a new downstream consumer.
    pub fn connect(
        &mut self,
        sort: Option<SortOrder>,
        filter_condition: Option<Condition>,
        filter_predicate: Option<Arc<dyn Fn(&Row) -> bool>>,
        split_edit_keys: Option<Vec<String>>,
    ) -> Shared<dyn Input> {
        let internal_sort = sort.clone().unwrap_or_else(|| self.primary_index_sort.clone());
        let compare_rows = make_comparator(internal_sort.clone(), false);
        let group = self.next_connect_group.clone();

        let conn = Rc::new(RefCell::new(Connection {
            sort: sort.clone(),
            split_edit_keys,
            compare_rows: compare_rows.clone(),
            filter_condition,
            filter_predicate,
            last_pushed_epoch: 0,
            group,
            output: None,
        }));

        let schema = SourceSchema {
            table_name: self.table_name.clone(),
            columns: self.columns.clone(),
            primary_key: self.primary_key.clone(),
            relationships: HashMap::new(),
            relationship_order: Vec::new(),
            is_hidden: false,
            system: System::Client,
            compare_rows,
            sort,
        };

        let data = self.data.clone();
        let comparator = self.comparator.clone();
        let overlay = self.overlay.clone();
        let db_path = self.db_path.clone();
        let column_names = self.column_names.clone();
        let table_name = self.table_name.clone();

        let input: Shared<dyn Input> = Rc::new(RefCell::new(SourceInput {
            data,
            comparator,
            conn: conn.clone(),
            schema,
            overlay,
            db_path,
            db_conn: self.db_conn.clone(),
            column_names,
            table_name: table_name.clone(),
            filter_condition: conn.borrow().filter_condition.clone(),
            removed_this_advance: self.removed_this_advance.clone(),
        }));

        self.connections.push(conn.clone());
        input
    }

    /// Push a source change through all connections.
    pub fn push(&mut self, change: SourceChange) -> Vec<Change> {
        self.push_internal(change, false)
    }

    /// Push with parallel fan-out across connection groups (inter-CG).
    pub fn push_parallel(&mut self, change: SourceChange) -> Vec<Change> {
        self.push_internal(change, self.parallel)
    }

    /// Push a change through all connected outputs, one connection at a time.
    /// Returns an iterator yielding the changes produced by each connection.
    /// Port of TS `genPush()` — yields per-connection results (no yield
    /// cooperative scheduling token in Rust).
    pub fn gen_push(&mut self, change: SourceChange) -> Vec<Change> {
        self.push_internal(change, false)
    }

    fn push_internal(&mut self, change: SourceChange, force_parallel: bool) -> Vec<Change> {
        // Split-edit: if any connection has split_edit_keys and this Edit
        // changes one of them, split into Remove(OldRow) + Add(Row) BEFORE
        // pushing. This prevents Join panics on key-changing edits.
        // Port of Go IVM's genPushAndWriteWithSplitEdit (source.go:282-308).
        if let SourceChange::Edit { ref row, ref old_row } = change {
            let should_split = self.connections.iter().any(|c| {
                let conn = c.borrow();
                if let Some(ref keys) = conn.split_edit_keys {
                    keys.iter().any(|k| {
                        let old_val = old_row.get(k).cloned().unwrap_or(Value::Null);
                        let new_val = row.get(k).cloned().unwrap_or(Value::Null);
                        old_val != new_val
                    })
                } else {
                    false
                }
            });
            if should_split {
                let old_row = old_row.clone();
                let new_row = row.clone();
                self.push_internal(SourceChange::Remove { row: old_row }, force_parallel);
                return self.push_internal(SourceChange::Add { row: new_row }, force_parallel);
            }
        }

        // Validate to prevent source drift (matching TableSource and TS
        // memory-source.ts dev assertions). Skip validation when using
        // SQLite-backed fetch — the in-memory Vec may be empty (data was
        // fetched from SQLite, not stored in-memory). apply_change below will
        // add rows to in-memory Vec for future pushes.
        if self.db_path.is_none() {
            match &change {
                SourceChange::Add { row } => {
                    if self.has(row) {
                        panic!("source drift: Add duplicate row in {}", self.table_name);
                    }
                }
                SourceChange::Remove { row } => {
                    if !self.has(row) {
                        panic!("source drift: Remove missing row from {}", self.table_name);
                    }
                }
                SourceChange::Edit { old_row, .. } => {
                    if !self.has(old_row) {
                        panic!("source drift: Edit missing old row from {}", self.table_name);
                    }
                }
            }
        }

        self.push_epoch += 1;
        let epoch = self.push_epoch;
        *self.overlay.borrow_mut() = Some((epoch, change.clone()));
        let _overlay_guard = OverlayGuard(self.overlay.clone());

        let active: Vec<Shared<Connection>> = self
            .connections
            .iter()
            .filter(|c| c.borrow().output.is_some())
            .cloned()
            .collect();

        let groups = group_connections(&active);
        let mut all_changes = Vec::new();

        // A pusher standing in for this source (TS passes `this`). Carries the
        // source schema so a downstream operator that reads pusher.get_schema()
        // gets a valid schema rather than panicking. No operator currently
        // does, but this removes the latent NullInputBase panic.
        let pusher = SourcePusher {
            schema: SourceSchema {
                table_name: self.table_name.clone(),
                columns: self.columns.clone(),
                primary_key: self.primary_key.clone(),
                relationships: HashMap::new(),
                relationship_order: Vec::new(),
                is_hidden: false,
                system: System::Client,
                compare_rows: make_comparator(self.primary_index_sort.clone(), false),
                sort: None,
            },
        };

        // Sequential or parallel push
        // For the initial port, sequential. Parallel is wired in engine.
        for group in &groups {
            for conn in group {
                // Extract what we need and release the borrow before pushing.
                // During push, downstream operators may call fetch on this
                // source, which needs to borrow the connection immutably.
                let (output, predicate) = {
                    let mut conn_ref = conn.borrow_mut();
                    conn_ref.last_pushed_epoch = epoch;
                    (conn_ref.output.clone(), conn_ref.filter_predicate.clone())
                };
                if let Some(output) = output {
                    let output_change = self.source_change_to_change(&change);
                    filter_push(output_change, output, &pusher, predicate.as_ref());
                }
            }
        }

        // Always apply the change to in-memory data, even when using SQLite-backed
        // fetch. The in-memory data is used by the push path (advance) to look up
        // old nodes for Edit changes on subsequent pushes. SQLite is only for the
        // fetch path (hydration). The Go IVM always updates its in-memory map.
        self.apply_change(&change);
        // Overlay is cleared by _overlay_guard Drop
        all_changes
    }

    fn apply_change(&mut self, change: &SourceChange) {
        let data = self.data.clone();
        let comparator = self.comparator.clone();
        let pk = self.primary_key.clone();
        let mut data = data.borrow_mut();
        match change {
            SourceChange::Add { row } => {
                let pos = data.partition_point(|existing| {
                    comparator(existing, row) == CmpOrdering::Less
                });
                data.insert(pos, row.clone());
                // Re-added after a same-advance remove: no longer removed.
                self.removed_this_advance.borrow_mut().remove(&pk_key(row, &pk));
            }
            SourceChange::Remove { row } => {
                if let Some(pos) = data.iter().position(|existing| {
                    pk.iter().all(|pk| {
                        let a = existing.get(pk).cloned().unwrap_or(Value::Null);
                        let b = row.get(pk).cloned().unwrap_or(Value::Null);
                        values_equal(&a, &b)
                    })
                }) {
                    data.remove(pos);
                }
                // Record the removal so the SQLite (PREV) fetch merge drops it.
                self.removed_this_advance.borrow_mut().insert(pk_key(row, &pk));
            }
            SourceChange::Edit { row, old_row } => {
                if let Some(pos) = data.iter().position(|existing| {
                    pk.iter().all(|pk| {
                        let a = existing.get(pk).cloned().unwrap_or(Value::Null);
                        let b = old_row.get(pk).cloned().unwrap_or(Value::Null);
                        values_equal(&a, &b)
                    })
                }) {
                    data.remove(pos);
                }
                let pos = data.partition_point(|existing| {
                    comparator(existing, row) == CmpOrdering::Less
                });
                data.insert(pos, row.clone());
                // A PK-changing edit is split into remove(old)+add(new) upstream,
                // but a value-only edit lands here: the new PK exists, and if the
                // PK changed the old PK is now removed. Reconcile both keys.
                let mut removed = self.removed_this_advance.borrow_mut();
                removed.remove(&pk_key(row, &pk));
                let old_key = pk_key(old_row, &pk);
                if old_key != pk_key(row, &pk) {
                    removed.insert(old_key);
                }
            }
        }
    }

    fn source_change_to_change(&self, sc: &SourceChange) -> Change {
        match sc {
            SourceChange::Add { row } => make_add_change(Node::new(row.clone())),
            SourceChange::Remove { row } => make_remove_change(Node::new(row.clone())),
            SourceChange::Edit { row, old_row } => {
                make_edit_change(Node::new(row.clone()), Node::new(old_row.clone()))
            }
        }
    }
}

impl Source for MemorySource {
    fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Called by the engine at advance start (PREV) and end (CURR). We keep the
    /// db no-op (the connection is set once via set_db_path) but use this as the
    /// advance boundary to clear the same-advance removed-PK set.
    fn set_snapshot_db(&mut self, _db: std::rc::Rc<std::cell::RefCell<rusqlite::Connection>>) {
        self.removed_this_advance.borrow_mut().clear();
    }

    fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    fn connect(
        &mut self,
        sort: Option<SortOrder>,
        filter_condition: Option<Condition>,
        filter_predicate: Option<Arc<dyn Fn(&Row) -> bool>>,
        split_edit_keys: Option<Vec<String>>,
    ) -> Shared<dyn Input> {
        self.connect(sort, filter_condition, filter_predicate, split_edit_keys)
    }

    fn push(&mut self, change: SourceChange) -> Vec<Change> {
        self.push(change)
    }

    fn push_parallel(&mut self, change: SourceChange) -> Vec<Change> {
        self.push_parallel(change)
    }

    fn gen_push(&mut self, change: SourceChange) -> Vec<Change> {
        self.gen_push(change)
    }

    fn get_row(&self, pk: &[(String, Value)]) -> Option<Row> {
        self.get_row(pk)
    }

    fn set_db_path(&mut self, path: &str) {
        self.set_db_path(path);
    }

    fn set_next_connect_group(&mut self, group: &str) {
        self.set_next_connect_group(group);
    }

    fn set_parallel(&mut self, enabled: bool, threshold: usize) {
        self.set_parallel(enabled, threshold);
    }
}

fn group_connections(connections: &[Shared<Connection>]) -> Vec<Vec<Shared<Connection>>> {
    let mut groups: HashMap<String, Vec<Shared<Connection>>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for conn in connections {
        let g = conn.borrow().group.clone();
        if !groups.contains_key(&g) {
            order.push(g.clone());
        }
        groups.entry(g).or_default().push(conn.clone());
    }
    order.into_iter().map(|g| groups.remove(&g).unwrap()).collect()
}

/// SourceInput — implements the Input trait for a connection.
pub struct SourceInput {
    data: SharedData,
    comparator: Comparator,
    conn: Shared<Connection>,
    schema: SourceSchema,
    overlay: SharedOverlay,
    db_path: Option<String>,
    db_conn: Rc<RefCell<Option<rusqlite::Connection>>>,
    column_names: Vec<String>,
    table_name: String,
    filter_condition: Option<Condition>,
    /// Shared with the owning MemorySource — PK-keys removed this advance, so
    /// the PREV-snapshot fetch merge can drop same-advance-removed rows.
    removed_this_advance: Rc<RefCell<std::collections::HashSet<String>>>,
}

impl InputBase for SourceInput {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        // Clear the back-edge to break the Rc cycle.
        self.conn.borrow_mut().output = None;
    }
}

impl Input for SourceInput {
    fn set_output(&self, output: OutputHandle) {
        self.conn.borrow_mut().output = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        let conn = self.conn.borrow();
        let sort = conn.sort.clone();
        let db_path = self.db_path.clone();
        let column_names = self.column_names.clone();
        let table_name = self.table_name.clone();
        let filter_condition = self.filter_condition.clone();
        let schema_columns = self.schema.columns.clone();

        if let Some(ref db_path) = db_path {
            let order: Vec<(String, String)> = sort.as_ref()
                .map(|s| s.iter().map(|p| (p[0].clone(), p[1].clone())).collect())
                .unwrap_or_default();
            let reverse = req.reverse;

            let query = crate::sqlite::query_builder::build_select_query(
                &table_name,
                &column_names,
                req,
                filter_condition.as_ref(),
                if order.is_empty() { None } else { Some(&order) },
                reverse,
            );

            // Use the dedicated connection for this source (one per source,
            // matching TS's one better-sqlite3 Database per syncer worker).
            // The NAPI worker thread is single-threaded, so this is safe.
            let db_conn = self.db_conn.borrow();
            let db = match db_conn.as_ref() {
                Some(c) => c,
                None => return from_vec(Vec::new()),
            };

            let mut stmt = match db.prepare(&query.text) {
                Ok(s) => s,
                Err(_) => return from_vec(Vec::new()),
            };

            let param_refs: Vec<&dyn rusqlite::ToSql> =
                query.params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();

            let col_names: Vec<String> = if !self.column_names.is_empty() {
                self.column_names.clone()
            } else {
                (0..stmt.column_count())
                    .filter_map(|i| stmt.column_name(i).ok())
                    .map(String::from)
                    .collect()
            };

            let column_types = schema_columns;
            let rows_result = stmt.query_map(
                rusqlite::params_from_iter(param_refs.iter().copied()),
                |row| {
                    let mut map: FxHashMap<String, Value> = FxHashMap::default();
                    for (i, col) in col_names.iter().enumerate() {
                        let val = row.get::<usize, rusqlite::types::Value>(i);
                        let col_type = column_types.get(col);
                        let value = match val {
                            Ok(rusqlite::types::Value::Integer(n)) => {
                                match col_type {
                                    Some(crate::ivm::schema::ColumnType::Boolean { .. }) => Value::Bool(n != 0),
                                    _ => Value::F64(n as f64),
                                }
                            }
                            Ok(rusqlite::types::Value::Real(n)) => Value::F64(n),
                            Ok(rusqlite::types::Value::Text(s)) => {
                                match col_type {
                                    Some(crate::ivm::schema::ColumnType::Json { .. }) => Value::Json(Arc::from(s.as_str())),
                                    _ => Value::Str(Arc::from(s.as_str())),
                                }
                            }
                            Ok(rusqlite::types::Value::Blob(b)) => {
                                Value::Str(Arc::from(String::from_utf8_lossy(&b).as_ref()))
                            }
                            Ok(rusqlite::types::Value::Null) => Value::Null,
                            Err(_) => Value::Null,
                        };
                        map.insert(col.clone(), value);
                    }
                    Ok(Arc::new(map))
                },
            );

            let rows_result = match rows_result {
                Ok(r) => r,
                Err(_) => return from_vec(Vec::new()),
            };

            let mut rows: Vec<Row> = Vec::new();
            for row_result in rows_result {
                let row = match row_result {
                    Ok(r) => r,
                    Err(_) => continue,
                };
                if let Some(pred) = &conn.filter_predicate {
                    if !pred(&row) { continue; }
                }
                rows.push(row);
            }

            // During advance the SQLite snapshot is PREV; same-advance changes
            // live only in the in-memory `data` vec (+ the removed-PK set). PREV
            // is stale for ALL of add/edit/remove, so reconcile the full delta:
            //   - a same-advance ADD is in `data`, not in PREV        -> add it;
            //   - a same-advance EDIT is in both (PREV=old, data=new)  -> use data;
            //   - a same-advance REMOVE is in PREV, gone from `data`   -> drop it.
            // `data` is a PARTIAL cache (only pushed rows), so "in PREV, not in
            // data" is ambiguous (untouched vs removed) — the removed-PK set
            // disambiguates. Fixes G15 (add) + its symmetric edit/remove staleness
            // on re-entrant EXISTS/join re-fetches (e.g. leaveChannel).
            {
                let pk = &self.schema.primary_key;
                let data = self.data.borrow();
                let removed = self.removed_this_advance.borrow();

                // Filter a data-sourced row through the same predicates the
                // SQLite query + fetch applied (its value may have changed).
                let passes = |r: &Row| -> bool {
                    if let Some(c) = &req.constraint {
                        if !constraint_matches_row(c, r) { return false; }
                    }
                    if !req.multi_constraints.is_empty()
                        && !crate::ivm::constraint::row_matches_multi_constraints(
                            &req.multi_constraints, r) { return false; }
                    if let Some(pred) = &conn.filter_predicate {
                        if !pred(r) { return false; }
                    }
                    if let Some(start) = &req.start {
                        let cmp = &conn.compare_rows;
                        let sr = &start.row;
                        let ord = cmp(r, sr);
                        let keep = if req.reverse {
                            match start.basis {
                                Basis::At => ord != CmpOrdering::Greater,
                                Basis::After => ord == CmpOrdering::Less,
                            }
                        } else {
                            match start.basis {
                                Basis::At => ord != CmpOrdering::Less,
                                Basis::After => ord == CmpOrdering::Greater,
                            }
                        };
                        if !keep { return false; }
                    }
                    true
                };

                let data_keys: std::collections::HashSet<String> =
                    data.iter().map(|r| pk_key(r, pk)).collect();

                // Drop PREV rows superseded by `data` (edit) or removed this
                // advance; keep untouched rows as-is.
                rows.retain(|r| {
                    let key = pk_key(r, pk);
                    !data_keys.contains(&key) && !removed.contains(&key)
                });
                // Add the current value for every touched row that passes filters
                // (covers adds AND edits — the PREV copy was dropped above).
                for data_row in data.iter() {
                    if passes(data_row) {
                        rows.push(data_row.clone());
                    }
                }
                rows.sort_by(|a, b| (conn.compare_rows)(a, b));
                if req.reverse { rows.reverse(); }
            }

            return self.apply_overlay_and_stream(rows, &conn, req);
        }

        // In-memory path (original)
        let conn = self.conn.borrow();

        // Filter by reference first and clone only the rows that pass, rather
        // than cloning the whole table up front. The result must still be
        // sorted (TS serves each connection from a maintained per-sort index:
        // memory-source.ts getOrCreateIndex), so we can't stream fully lazily,
        // but we avoid materializing filtered-out rows. Filtering commutes with
        // the sort, so the result is identical to sort-then-retain.
        let mut rows: Vec<Row> = {
            let data = self.data.borrow();
            data.iter()
                .filter(|r| {
                    if let Some(constraint) = &req.constraint {
                        if !constraint_matches_row(constraint, r) {
                            return false;
                        }
                    }
                    if !req.multi_constraints.is_empty()
                        && !crate::ivm::constraint::row_matches_multi_constraints(
                            &req.multi_constraints,
                            r,
                        )
                    {
                        return false;
                    }
                    if let Some(predicate) = &conn.filter_predicate {
                        if !predicate(r) {
                            return false;
                        }
                    }
                    true
                })
                .cloned()
                .collect()
        };

        rows.sort_by(|a, b| (conn.compare_rows)(a, b));

        if let Some(start) = &req.start {
            let compare = &conn.compare_rows;
            let start_row = &start.row;
            if req.reverse {
                match start.basis {
                    Basis::At => rows.retain(|r| compare(r, start_row) != CmpOrdering::Greater),
                    Basis::After => rows.retain(|r| compare(r, start_row) == CmpOrdering::Less),
                }
            } else {
                match start.basis {
                    Basis::At => rows.retain(|r| compare(r, start_row) != CmpOrdering::Less),
                    Basis::After => rows.retain(|r| compare(r, start_row) == CmpOrdering::Greater),
                }
            }
        }

        if req.reverse {
            rows.reverse();
        }

        self.apply_overlay_and_stream(rows, &conn, req)
    }
}

impl SourceInput {
    /// Apply overlay (pending push change) on top of fetched rows and return as stream.
    /// Streams lazily — port of TS generateWithOverlay pattern.
    fn apply_overlay_and_stream(
        &self,
        rows: Vec<Row>,
        conn: &Connection,
        req: &FetchRequest,
    ) -> NodeStream {
        let overlay_change = {
            let ov = self.overlay.borrow();
            match *ov {
                Some((epoch, ref change)) if conn.last_pushed_epoch >= epoch => {
                    Some(change.clone())
                }
                _ => None,
            }
        };
        let index_compare = compute_index_compare(
            conn.sort.as_ref(),
            req,
            &self.schema.primary_key,
        );
        apply_source_overlay(
            Box::new(rows.into_iter()),
            overlay_change,
            conn.compare_rows.clone(),
            index_compare,
            conn.filter_predicate.clone(),
            req,
        )
    }
}

/// Build the index comparator TS uses to place overlay rows
/// (memory-source.ts `#getOrCreateIndex(indexSort, ...)` then
/// `generateWithOverlay(... compare = index.comparator)`): constraint keys
/// (asc) first, then the connection's requested sort — EXCEPT when the
/// fetch constraint is exactly the single-column primary key, in which case
/// the requested sort is dropped (the PK alone fully orders a one-row
/// result). `overlaysForStartAt` uses this comparator to decide whether an
/// overlay row sorts before `req.start` and should be dropped; using the
/// connection comparator here instead (as Rust previously did) suppresses a
/// removed row that TS returns when the row is before `start` in index order
/// but after `start` in connection order.
pub(crate) fn compute_index_compare(
    conn_sort: Option<&SortOrder>,
    req: &FetchRequest,
    primary_key: &[String],
) -> Comparator {
    let mut index_sort: Vec<[String; 2]> = Vec::new();
    if let Some(c) = &req.constraint {
        let mut keys: Vec<&String> = c.keys().collect();
        keys.sort();
        for k in keys {
            index_sort.push([k.clone(), "asc".to_string()]);
        }
    }
    // Multi-constraint fetches (used by FlippedJoin batched parent lookups)
    // logically AND the constraint keys with each multi-constraint entry's keys.
    // Include those leading keys when choosing the index comparator so overlay
    // start-filtering matches TS's per-sub-fetch index behavior.
    let sample_multi_constraint: Option<&crate::ivm::constraint::Constraint> = req
        .multi_constraints
        .iter()
        .find(|mc| !mc.is_empty())
        .and_then(|mc| mc.first());
    if let Some(sample) = sample_multi_constraint {
        let mut keys: Vec<&String> = sample.keys().collect();
        keys.sort();
        for k in keys {
            if index_sort.iter().all(|p| &p[0] != k) {
                index_sort.push([k.clone(), "asc".to_string()]);
            }
        }
    }
    let effective_pk_constraint: Option<Constraint> = {
        let mut m = Constraint::default();
        if let Some(c) = &req.constraint {
            for (k, v) in c {
                m.insert(k.clone(), v.clone());
            }
        }
        for mc in &req.multi_constraints {
            if let Some(c) = mc.first() {
                for (k, v) in c {
                    m.insert(k.clone(), v.clone());
                }
            }
        }
        if m.is_empty() { None } else { Some(m) }
    };
    let pk_match = effective_pk_constraint
        .as_ref()
        .map(|c| constraint_matches_primary_key(c, primary_key))
        .unwrap_or(false);
    let append_requested = primary_key.len() > 1 || effective_pk_constraint.is_none() || !pk_match;
    if append_requested {
        if let Some(s) = conn_sort {
            for p in s.iter() {
                if index_sort.iter().all(|existing| &existing[0] != &p[0]) {
                    index_sort.push(p.clone());
                }
            }
        }
    }
    make_comparator(Arc::new(index_sort), false)
}

/// Source-level overlay application (port of memory-source.ts
/// `generateWithOverlay` / `generateWithOverlayUnordered`). INJECTS the add row
/// (storage does not yet contain it — `writeChange` runs after propagation) and
/// SUPPRESSES the remove row (still in storage). This is the OPPOSITE of the
/// join-utils overlay (suppress-add) and has NO "overlay never applied" assert:
/// the overlay row may legitimately be filtered out by the fetch constraint.
/// Shared by MemorySource and TableSource so both match TS exactly.
pub(crate) fn apply_source_overlay(
    rows: Box<dyn Iterator<Item = Row>>,
    overlay_change: Option<SourceChange>,
    compare: Comparator,
    index_compare: Comparator,
    filter_predicate: Option<Arc<dyn Fn(&Row) -> bool>>,
    req: &FetchRequest,
) -> NodeStream {
    apply_source_overlay_impl(rows, overlay_change, compare, index_compare, filter_predicate, req)
}

fn apply_source_overlay_impl(
    rows: Box<dyn Iterator<Item = Row>>,
    overlay_change: Option<SourceChange>,
    compare: Comparator,
    index_compare: Comparator,
    filter_predicate: Option<Arc<dyn Fn(&Row) -> bool>>,
    req: &FetchRequest,
) -> NodeStream {
    use std::cell::Cell;
    let reverse = req.reverse;

    match overlay_change {
        None => {
            // No overlay — stream rows directly as Nodes
            Box::new(rows.map(|r| StreamItem::Data(Node::new(r))))
        }
        Some(change) => {
            let (add_row, remove_row) = match change {
                SourceChange::Add { row } => (Some(row), None),
                SourceChange::Remove { row } => (None, Some(row)),
                SourceChange::Edit { row, old_row } => (Some(row), Some(old_row)),
            };

            // Filter add_row and remove_row by constraint/predicate/start.
            // The start check uses `index_compare` (the constraint-first
            // index comparator), matching TS `overlaysForStartAt`
            // (memory-source.ts): an overlay row that sorts before
            // `req.start` in INDEX order is dropped — even if it sorts
            // after `req.start` in the connection's requested sort. The
            // constraint/predicate checks are comparator-independent.
            let index_compare_for_filter = index_compare.clone();
            let filter_fn = move |r: &Row| -> bool {
                if let Some(c) = &req.constraint {
                    if !constraint_matches_row(c, r) { return false; }
                }
                if !req.multi_constraints.is_empty() {
                    if !crate::ivm::constraint::row_matches_multi_constraints(&req.multi_constraints, r) {
                        return false;
                    }
                }
                if let Some(pred) = &filter_predicate {
                    if !pred(r) { return false; }
                }
                // Match TS `overlaysForStartAt` (memory-source.ts): drop an
                // overlay row only if it sorts STRICTLY before `start` in
                // INDEX order (compare < 0). Basis (At/After) is NOT
                // consulted for overlays — the exclusive/inclusive bound is
                // enforced by the source scan and the downstream Skip using
                // the full connection comparator, which can distinguish rows
                // that tie on the index key (e.g. an edited row sharing its
                // PK with the start row). Previously the After branch dropped
                // Equal too, suppressing a re-entrant cascade child change.
                if let Some(start) = &req.start {
                    let ord = index_compare_for_filter(r, &start.row);
                    if reverse {
                        if ord == CmpOrdering::Greater { return false; }
                    } else {
                        if ord == CmpOrdering::Less { return false; }
                    }
                }
                true
            };

            let add_row = add_row.filter(&filter_fn);
            let remove_row = remove_row.filter(&filter_fn);

            // Stream rows, splicing add_row at correct position, skipping remove_row
            let add_yielded = Rc::new(Cell::new(false));
            let remove_skipped = Rc::new(Cell::new(false));
            let compare2 = compare.clone();
            let add_row2 = add_row.clone();
            let remove_row2 = remove_row.clone();
            let ay = add_yielded.clone();
            let rs = remove_skipped.clone();

            let inner = rows.flat_map(move |row| {
                let mut out: Vec<Row> = Vec::new();

                if !ay.get() {
                    if let Some(ref add) = add_row2 {
                        let ord = if reverse { compare2(&row, add) } else { compare2(add, &row) };
                        if ord == CmpOrdering::Less {
                            out.push(add.clone());
                            ay.set(true);
                        }
                    }
                }
                if !rs.get() {
                    if let Some(ref rm) = remove_row2 {
                        let ord = if reverse { compare2(&row, rm) } else { compare2(rm, &row) };
                        if ord == CmpOrdering::Equal {
                            rs.set(true);
                            // skip this row
                            return out;
                        }
                    }
                }
                out.push(row);
                out
            }).map(|r| StreamItem::Data(Node::new(r)));

            // Handle trailing add_row if not yet yielded.
            // Check add_yielded LAZILY (at stream exhaustion, not at binding time).
            let ay_for_trailing = add_yielded.clone();
            let add_for_trailing = Rc::new(RefCell::new(add_row.clone()));

            Box::new(inner.chain(std::iter::from_fn(move || {
                if ay_for_trailing.get() {
                    return None;
                }
                ay_for_trailing.set(true);
                add_for_trailing.borrow_mut().take().map(|r| StreamItem::Data(Node::new(r)))
            })))
        }
    }
}

/// Pusher passed to `filter_push` during a source push, standing in for the
/// source (TS passes the source `this`). Carries the source schema so
/// `get_schema()` returns a valid schema instead of panicking.
struct SourcePusher {
    schema: SourceSchema,
}
impl InputBase for SourcePusher {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }
    fn destroy(&mut self) {}
}

/// Empty input — returns no rows. Used when a source is not found
/// (e.g. querying a table that wasn't registered). Prevents panics.
pub struct EmptyInput {
    schema: SourceSchema,
}

impl EmptyInput {
    pub fn new() -> Self {
        EmptyInput {
            schema: SourceSchema {
                table_name: String::new(),
                columns: HashMap::new(),
                primary_key: vec![],
                relationships: HashMap::new(),
                relationship_order: Vec::new(),
                is_hidden: false,
                system: System::Client,
                compare_rows: make_comparator(Arc::new(vec![]), false),
                sort: None,
            },
        }
    }
}

impl Default for EmptyInput {
    fn default() -> Self {
        Self::new()
    }
}

impl InputBase for EmptyInput {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }
    fn destroy(&mut self) {}
}

impl Input for EmptyInput {
    fn set_output(&self, _output: OutputHandle) {}

    fn fetch(&self, _req: &FetchRequest) -> NodeStream {
        empty_stream()
    }
}

/// Collecting output — terminal sink for pushed changes.
pub struct CollectOutput {
    pub changes: Vec<Change>,
}

impl CollectOutput {
    pub fn new() -> Self {
        CollectOutput { changes: Vec::new() }
    }
}

impl Output for CollectOutput {
    fn push(&mut self, change: Change, _pusher: &dyn InputBase) {
        crate::ivm::trace::recv("source#1", &change);
        self.changes.push(change);
    }
}

impl Default for CollectOutput {
    fn default() -> Self {
        Self::new()
    }
}

/// Comparator function type for nodes.
pub type NodeCompare = Rc<dyn Fn(&Node, &Node) -> CmpOrdering>;

/// Lazy k-way merge of sorted node streams.
/// Port of TS `mergeSortedStreams` (memory-source.ts:1051).
///
/// Uses a min-heap internally so each `next()` is O(log k).
/// Streams are consumed lazily — one node is pulled from each
/// stream to prime the heap, then refilled one at a time as
/// nodes are yielded. Early drop closes remaining streams.
pub fn merge_sorted_streams(
    streams: Vec<NodeStream>,
    compare: NodeCompare,
) -> NodeStream {
    if streams.is_empty() {
        return empty_stream();
    }
    if streams.len() == 1 {
        return streams.into_iter().next().unwrap();
    }
    Box::new(KWayMerge::new(streams, compare))
}

/// Heap entry: (node, stream_index). Ordered by compare function.
/// BinaryHeap is a max-heap, so we reverse the comparison to get a min-heap.
struct HeapEntry {
    node: Node,
    idx: usize,
    compare: NodeCompare,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        // Equal iff same row AND same stream index. Including `idx` keeps
        // Ord/Eq consistent now that `cmp` uses `idx` as a tiebreaker.
        self.idx == other.idx
            && (self.compare)(&self.node, &other.node) == CmpOrdering::Equal
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reverse row comparison for min-heap (BinaryHeap is max-heap by default):
        // the node with the smallest row is the "greatest" HeapEntry and pops
        // first.
        let row_cmp = (self.compare)(&other.node, &self.node);
        if row_cmp != CmpOrdering::Equal {
            return row_cmp;
        }
        // Tiebreaker: on equal rows, the LOWER stream index pops first. This
        // matches TS `mergeFetches` (union-fan-in.ts), whose linear reduce
        // picks the lower-index branch on ties (`comparator(c, acc[0]) < 0` is
        // strict, so equal keeps the existing lower-index acc). Without this,
        // BinaryHeap tie-breaking is undefined, so a row appearing in two
        // UnionFanIn branches (e.g. a row matching both the non-flipped and
        // flipped OR branches) may yield the flipped branch's node first —
        // leaking the flipped subquery's relationship into the output where TS
        // suppresses it. Reverse idx (`other.cmp(self)`) so lower idx => Greater
        // => popped first.
        other.idx.cmp(&self.idx)
    }
}

struct KWayMerge {
    streams: Vec<Option<NodeStream>>,
    heap: std::collections::BinaryHeap<HeapEntry>,
    compare: NodeCompare,
}

impl KWayMerge {
    fn new(streams: Vec<NodeStream>, compare: NodeCompare) -> Self {
        let n = streams.len();
        let mut km = KWayMerge {
            streams: streams.into_iter().map(Some).collect(),
            heap: std::collections::BinaryHeap::new(),
            compare: compare.clone(),
        };
        for i in 0..n {
            km.advance(i);
        }
        km
    }

    fn advance(&mut self, idx: usize) {
        if let Some(stream) = &mut self.streams[idx] {
            while let Some(item) = stream.next() {
                match item {
                    crate::ivm::stream::StreamItem::Data(n) => {
                        self.heap.push(HeapEntry {
                            node: n,
                            idx,
                            compare: self.compare.clone(),
                        });
                        return;
                    }
                    crate::ivm::stream::StreamItem::Yield => continue,
                }
            }
        }
    }
}

impl Iterator for KWayMerge {
    type Item = crate::ivm::stream::StreamItem<Node>;

    fn next(&mut self) -> Option<crate::ivm::stream::StreamItem<Node>> {
        match self.heap.pop() {
            None => None,
            Some(entry) => {
                self.advance(entry.idx);
                Some(crate::ivm::stream::StreamItem::Data(entry.node))
            }
        }
    }
}


/// String-based PK key for deduplication (Value doesn't implement Hash).
fn pk_key(row: &Row, pk: &[String]) -> String {
    pk.iter()
        .map(|k| match row.get(k) {
            Some(Value::Str(s)) => s.to_string(),
            Some(Value::F64(n)) => n.to_string(),
            Some(Value::Bool(b)) => b.to_string(),
            Some(Value::Json(s)) => s.to_string(),
            Some(Value::Null) => "null".to_string(),
            None => "?".to_string(),
        })
        .collect::<Vec<_>>()
        .join("\x1f")
}
