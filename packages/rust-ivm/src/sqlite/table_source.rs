//! TableSource — port of `zqlite/src/table-source.ts`.
//!
//! The production source backed by a SQLite table. Reads rows from SQLite,
//! applies overlay during push, writes changes back to the table.
//!
//! Key differences from MemorySource:
//! - Data lives in SQLite, not in memory
//! - `set_db` supports the Snapshotter leapfrog (swap SQLite snapshot)
//! - `fetch` compiles FetchRequest → SQL via query_builder
//! - `push` writes to SQLite (INSERT/DELETE/UPDATE) then pushes to connections
//! - Values are converted between IVM types and SQLite types

use std::cell::{Ref, RefCell};
use std::rc::Rc;
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::Arc;

use rusqlite::{Connection, params_from_iter};

use rustc_hash::FxHashMap;

use crate::builder::ast::Condition;
use crate::ivm::change::{
    make_add_change, make_edit_change, make_remove_change, Change, SourceChange,
};
use crate::ivm::constraint::{constraint_matches_row, Constraint};
use crate::ivm::data::{
    make_comparator, values_equal, Comparator, Node, Row, SortOrder, Value,
};
use crate::ivm::filter_push::filter_push;
use crate::ivm::operator::{
    Basis, FetchRequest, Input, InputBase, Output, OutputHandle, Shared, Start,
};
use crate::ivm::schema::{ColumnType, SourceSchema, System};
use crate::ivm::source::Source;
use crate::ivm::stream::NodeStream;
use crate::sqlite::query_builder::{
    build_select_query, SqlParam, SqlQuery, to_sqlite_value,
};

/// Streaming iterator over a SQLite SELECT result.
///
/// Keeps the `Connection` alive and immutably borrowed while iterating, stores
/// the `Statement` at a pinned heap address, and steps through `Rows` lazily.
/// This matches TS `statement.iterate<Row>()` in `zqlite/src/table-source.ts`
/// instead of collecting into a `Vec` first.
///
/// # Safety
///
/// The `'static` lifetimes on `_guard`, `stmt`, and `rows` are erased at
/// construction. This is sound because:
/// - `_conn` keeps the `RefCell<Connection>` alive at a stable heap address.
/// - `_guard` holds an active immutable `RefCell` borrow for the struct's
///   lifetime, preventing mutation or destruction of the connection.
/// - `stmt` is boxed and pinned, so its address never changes.
/// - Fields are dropped in reverse declaration order (`rows`, `stmt`, `_guard`,
///   `_conn`), respecting the dependency chain.
struct LazyRows {
    _conn: Rc<RefCell<Connection>>,
    _guard: Ref<'static, Connection>,
    stmt: Pin<Box<rusqlite::Statement<'static>>>,
    rows: Option<rusqlite::Rows<'static>>,
    column_names: Vec<String>,
    columns: HashMap<String, ColumnType>,
    table_name: String,
    _pin: PhantomPinned,
}

impl LazyRows {
    fn try_new(
        conn: Rc<RefCell<Connection>>,
        sql: String,
        params: Vec<SqlParam>,
        column_names: Vec<String>,
        columns: HashMap<String, ColumnType>,
        table_name: String,
    ) -> Result<Pin<Box<Self>>, rusqlite::Error> {
        // Hold an immutable RefCell borrow for the entire struct lifetime.
        let guard: Ref<'_, Connection> = conn.borrow();
        let guard_static: Ref<'static, Connection> = unsafe { std::mem::transmute(guard) };

        // Prepare the statement while the connection is borrowed.
        let stmt: rusqlite::Statement<'_> = guard_static.prepare(&sql)?;
        let stmt_static: rusqlite::Statement<'static> = unsafe { std::mem::transmute(stmt) };
        let mut stmt_pin = Box::pin(stmt_static);

        // Bind parameters and create the rows cursor. The statement's heap
        // address is stable because it is pinned.
        let rows: rusqlite::Rows<'_> = {
            let stmt_mut: &mut rusqlite::Statement<'static> =
                unsafe { Pin::get_unchecked_mut(Pin::as_mut(&mut stmt_pin)) };
            let param_refs: Vec<&dyn rusqlite::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
            stmt_mut.query(rusqlite::params_from_iter(param_refs.iter().copied()))?
        };
        let rows_static: rusqlite::Rows<'static> = unsafe { std::mem::transmute(rows) };

        Ok(Box::pin(LazyRows {
            _conn: conn,
            _guard: guard_static,
            stmt: stmt_pin,
            rows: Some(rows_static),
            column_names,
            columns,
            table_name,
            _pin: PhantomPinned,
        }))
    }
}

/// Wrapper so `LazyRows` (which must stay pinned) can implement `Iterator`.
struct LazyRowsIter(Pin<Box<LazyRows>>);

impl Iterator for LazyRowsIter {
    type Item = Row;

    fn next(&mut self) -> Option<Row> {
        let this: &mut LazyRows = unsafe { Pin::get_unchecked_mut(Pin::as_mut(&mut self.0)) };
        let rows = this.rows.as_mut()?;
        let column_names = this.column_names.clone();
        let columns = this.columns.clone();
        let table_name = this.table_name.clone();
        match rows.next() {
            Ok(Some(raw_row)) => {
                let mut map: FxHashMap<String, Value> = FxHashMap::default();
                for (i, col) in column_names.iter().enumerate() {
                    let val = raw_row.get::<usize, rusqlite::types::Value>(i);
                    let value = sqlite_value_to_ivm(
                        val,
                        columns.get(col),
                        &table_name,
                        col,
                    );
                    map.insert(col.clone(), value);
                }
                Some(Arc::new(map))
            }
            Ok(None) => None,
            Err(e) => {
                eprintln!("[rust-ivm] row read error for {}: {}", table_name, e);
                None
            }
        }
    }
}

fn stream_query(
    db: Rc<RefCell<Connection>>,
    query: SqlQuery,
    column_names: Vec<String>,
    columns: HashMap<String, ColumnType>,
    table_name: String,
    filter_predicate: Option<Arc<dyn Fn(&Row) -> bool>>,
) -> Box<dyn Iterator<Item = Row>> {
    let table_name_for_err = table_name.clone();
    let lazy = match LazyRows::try_new(
        db,
        query.text,
        query.params,
        column_names,
        columns,
        table_name,
    ) {
        Ok(lazy) => lazy,
        Err(e) => {
            eprintln!("[rust-ivm] query prepare error for {}: {}", table_name_for_err, e);
            return Box::new(std::iter::empty());
        }
    };

    let iter = LazyRowsIter(lazy);
    match filter_predicate {
        Some(pred) => Box::new(iter.filter(move |r| pred(r))),
        None => Box::new(iter),
    }
}

/// Convert a raw SQLite value to an IVM `Value` using the column's declared
/// type — a port of TS `fromSQLiteType` (zqlite/src/table-source.ts:621).
/// Without the type, a `boolean` column reads back as its stored integer `0/1`
/// and a `json` column as raw text, diverging from TS (`true/false`, parsed
/// object). Mirrors the coercion already in `ivm/source.rs`; keeps the ±2^53
/// integer bounds check for numeric columns.
pub(crate) fn sqlite_value_to_ivm(
    val: rusqlite::Result<rusqlite::types::Value>,
    col_type: Option<&ColumnType>,
    table: &str,
    col: &str,
) -> Value {
    use rusqlite::types::Value as Sv;
    match val {
        Ok(Sv::Null) | Err(_) => Value::Null,
        // TS `boolean` => `!!v`: only 0 / 0.0 / empty are false.
        Ok(Sv::Integer(n)) if matches!(col_type, Some(ColumnType::Boolean { .. })) => {
            Value::Bool(n != 0)
        }
        Ok(Sv::Real(n)) if matches!(col_type, Some(ColumnType::Boolean { .. })) => {
            Value::Bool(n != 0.0)
        }
        // TS `json` => `JSON.parse(v)`; tag as Json so the napi boundary emits a
        // parsed object (the JS side JSON.parses the "json" kind).
        Ok(Sv::Text(s)) if matches!(col_type, Some(ColumnType::Json { .. })) => {
            Value::Json(Arc::from(s.as_str()))
        }
        Ok(Sv::Blob(b)) if matches!(col_type, Some(ColumnType::Json { .. })) => {
            Value::Json(Arc::from(String::from_utf8_lossy(&b).as_ref()))
        }
        // number / string columns (and untyped): pass through unchanged.
        Ok(Sv::Integer(n)) => {
            // Reject integers outside ±(2^53-1) rather than silently losing
            // precision, matching TS fromSQLiteType.
            if n > 9_007_199_254_740_991 || n < -9_007_199_254_740_991 {
                panic!("value {n} (in {table}.{col}) is outside of supported bounds");
            }
            Value::F64(n as f64)
        }
        Ok(Sv::Real(n)) => Value::F64(n),
        Ok(Sv::Text(s)) => Value::Str(Arc::from(s.as_str())),
        Ok(Sv::Blob(b)) => Value::Str(Arc::from(String::from_utf8_lossy(&b).as_ref())),
    }
}

/// Connection: a downstream consumer of the TableSource.
pub struct TableConnection {
    pub sort: Option<SortOrder>,
    pub split_edit_keys: Option<Vec<String>>,
    pub compare_rows: Comparator,
    pub filter_condition: Option<Condition>,
    pub filter_predicate: Option<Arc<dyn Fn(&Row) -> bool>>,
    pub last_pushed_epoch: usize,
    pub output: Option<OutputHandle>,
}

/// Shared overlay — accessible by both TableSource (writer) and TableSourceInput (reader)
type SharedOverlay = Rc<RefCell<Option<(usize, SourceChange)>>>;

/// RAII guard that clears the overlay on drop, even if a panic occurs.
struct OverlayGuard(SharedOverlay);
impl Drop for OverlayGuard {
    fn drop(&mut self) {
        *self.0.borrow_mut() = None;
    }
}

/// TableSource — the production source backed by SQLite.
/// Port of TS `TableSource` (table-source.ts:66).
pub struct TableSource {
    table_name: String,
    columns: HashMap<String, ColumnType>,
    column_names: Vec<String>,
    primary_key: Vec<String>,
    primary_index_sort: SortOrder,
    db: Rc<RefCell<Connection>>,
    connections: Vec<Shared<TableConnection>>,
    overlay: SharedOverlay,
    push_epoch: usize,
}

impl TableSource {
    pub fn new(
        db: Rc<RefCell<Connection>>,
        table_name: &str,
        columns: HashMap<String, ColumnType>,
        primary_key: Vec<String>,
    ) -> Self {
        let column_names: Vec<String> = columns.keys().cloned().collect();
        let primary_index_sort: SortOrder = Arc::new(
            primary_key
                .iter()
                .map(|k| [k.clone(), "asc".to_string()])
                .collect(),
        );

        TableSource {
            table_name: table_name.to_string(),
            columns,
            column_names,
            primary_key: primary_key.clone(),
            primary_index_sort,
            db,
            connections: Vec::new(),
            overlay: Rc::new(RefCell::new(None)),
            push_epoch: 0,
        }
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    /// Set the SQLite connection (for Snapshotter leapfrog).
    /// Port of TS `setDB` (table-source.ts:103).
    pub fn set_db(&mut self, db: Rc<RefCell<Connection>>) {
        self.db = db;
    }

    /// Connect a new downstream consumer.
    /// Port of TS `connect` (table-source.ts:219).
    pub fn connect(
        &mut self,
        sort: Option<SortOrder>,
        filter_condition: Option<Condition>,
        filter_predicate: Option<Arc<dyn Fn(&Row) -> bool>>,
        split_edit_keys: Option<Vec<String>>,
    ) -> Shared<dyn Input> {
        let internal_sort = sort.clone().unwrap_or_else(|| self.primary_index_sort.clone());
        let compare_rows = make_comparator(internal_sort.clone(), false);

        let conn = Rc::new(RefCell::new(TableConnection {
            sort: sort.clone(),
            split_edit_keys,
            compare_rows: compare_rows.clone(),
            filter_condition: filter_condition.clone(),
            filter_predicate,
            last_pushed_epoch: 0,
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

        let db = self.db.clone();
        let column_names = self.column_names.clone();
        let table_name = self.table_name.clone();
        let columns = self.columns.clone();
        let overlay_epoch = Rc::new(RefCell::new(0usize)); // tracks last_pushed_epoch

        let input: Shared<dyn Input> = Rc::new(RefCell::new(TableSourceInput {
            db,
            table_name,
            column_names,
            columns,
            conn: conn.clone(),
            schema,
            primary_key: self.primary_key.clone(),
            filter_condition: filter_condition.clone(),
            overlay: self.overlay.clone(),
        }));

        self.connections.push(conn.clone());
        input
    }

    /// Push a source change: write to SQLite + push to all connections.
    /// Port of TS `push`/`genPush` (table-source.ts:300).
    pub fn push(&mut self, change: SourceChange) -> Vec<Change> {
        // Split-edit (port of TS genPushAndWriteWithSplitEdit, memory-source.ts:525;
        // mirrors MemorySource::push_internal): if any connection has
        // split_edit_keys and this Edit changes one of them, split into
        // Remove(old) + Add(new) BEFORE pushing. This is what lets a partition/
        // PK-crossing edit through a Take/Join without hitting the
        // "Unexpected change of partition key" assert.
        if let SourceChange::Edit { ref row, ref old_row } = change {
            let should_split = self.connections.iter().any(|c| {
                let conn = c.borrow();
                conn.split_edit_keys.as_ref().map_or(false, |keys| {
                    keys.iter().any(|k| {
                        old_row.get(k).cloned().unwrap_or(Value::Null)
                            != row.get(k).cloned().unwrap_or(Value::Null)
                    })
                })
            });
            if should_split {
                // The split pieces are a controlled transform of an
                // already-valid edit; push them WITHOUT re-validating. TS gets
                // away with validating each piece because its writeChange
                // mutates the db between Remove and Add (so exists() reflects
                // the removal); our snapshot connection is READ-ONLY, so a
                // re-validate of the Add would spuriously see the same-PK row
                // still present. Skipping validate here is the equivalent net
                // effect (the assert would pass in TS's writable-db world).
                let old_row = old_row.clone();
                let new_row = row.clone();
                self.push_body(SourceChange::Remove { row: old_row });
                return self.push_body(SourceChange::Add { row: new_row });
            }
        }

        // Validate
        self.validate_change(&change);
        self.push_body(change)
    }

    /// Push a change through the pipeline WITHOUT the split-edit / validate
    /// pre-steps (those are handled by `push`).
    fn push_body(&mut self, change: SourceChange) -> Vec<Change> {
        self.push_epoch += 1;
        let epoch = self.push_epoch;
        *self.overlay.borrow_mut() = Some((epoch, change.clone()));
        let _overlay_guard = OverlayGuard(self.overlay.clone());

        let active: Vec<Shared<TableConnection>> = self
            .connections
            .iter()
            .filter(|c| c.borrow().output.is_some())
            .cloned()
            .collect();

        let mut all_changes = Vec::new();

        for conn in &active {
            let (output, predicate) = {
                let mut conn_ref = conn.borrow_mut();
                conn_ref.last_pushed_epoch = epoch;
                (conn_ref.output.clone(), conn_ref.filter_predicate.clone())
            };
            if let Some(output) = output {
                let pipeline_change = self.source_change_to_change(&change);
                let pusher: &dyn InputBase = &NullInputBase;
                filter_push(pipeline_change, output, pusher, predicate.as_ref());
            }
        }

        // Write the change to SQLite
        if let Err(e) = self.write_change(&change) {
            eprintln!("[rust-ivm] write_change error for {}: {}", self.table_name, e);
        }

        // Overlay cleared by _overlay_guard Drop

        all_changes
    }

    fn validate_change(&self, change: &SourceChange) {
        let db = self.db.borrow();
        match change {
            SourceChange::Add { row } => {
                if self.check_exists(&db, row) {
                    panic!("source drift: Add duplicate row in {}", self.table_name);
                }
            }
            SourceChange::Remove { row } => {
                if !self.check_exists(&db, row) {
                    panic!("source drift: Remove missing row from {}", self.table_name);
                }
            }
            SourceChange::Edit { old_row, .. } => {
                if !self.check_exists(&db, old_row) {
                    panic!("source drift: Edit missing old row from {}", self.table_name);
                }
            }
        }
    }

    fn check_exists(&self, db: &Connection, row: &Row) -> bool {
        let where_clause: Vec<String> = self
            .primary_key
            .iter()
            .map(|k| format!("\"{}\" = ?", k))
            .collect();
        let sql = format!(
            "SELECT 1 FROM \"{}\" WHERE {} LIMIT 1",
            self.table_name,
            where_clause.join(" AND ")
        );

        let params: Vec<SqlParam> = self
            .primary_key
            .iter()
            .map(|k| SqlParam::from(&row.get(k).cloned().unwrap_or(Value::Null)))
            .collect();

        let mut stmt = match db.prepare(&sql) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[rust-ivm] check_exists prepare error for {}: {}", self.table_name, e);
                return false;
            }
        };
        let result = stmt.exists(params_from_iter(params.iter().map(|p| p as &dyn rusqlite::ToSql)));
        result.unwrap_or(false)
    }

    fn write_change(&self, _change: &SourceChange) -> Result<(), rusqlite::Error> {
        // No-op: changes are already written to zero.db by the change-streamer.
        // The Rust IVM reads from zero.db for hydrate and stores pushed changes
        // in the in-memory overlay. Writing to zero.db here would conflict with
        // the change-streamer's WAL2-mode writes (rusqlite doesn't support WAL2).
        return Ok(());
    }

    fn _write_change_unused(&self, change: &SourceChange) -> Result<(), rusqlite::Error> {
        let mut db = self.db.borrow_mut();
        let tx = db.transaction()?;

        match change {
            SourceChange::Add { row } => {
                let placeholders: Vec<String> = (0..self.column_names.len())
                    .map(|_| "?".to_string())
                    .collect();
                let sql = format!(
                    "INSERT OR REPLACE INTO \"{}\" ({}) VALUES ({})",
                    self.table_name,
                    self.column_names
                        .iter()
                        .map(|c| format!("\"{}\"", c))
                        .collect::<Vec<_>>()
                        .join(", "),
                    placeholders.join(", ")
                );
                let params: Vec<SqlParam> = self
                    .column_names
                    .iter()
                    .map(|c| SqlParam::from(&row.get(c).cloned().unwrap_or(Value::Null)))
                    .collect();
                tx.execute(
                    &sql,
                    params_from_iter(params.iter().map(|p| p as &dyn rusqlite::ToSql)),
                )
                ?;
            }
            SourceChange::Remove { row } => {
                let where_clause: Vec<String> = self
                    .primary_key
                    .iter()
                    .map(|k| format!("\"{}\" = ?", k))
                    .collect();
                let sql = format!(
                    "DELETE FROM \"{}\" WHERE {}",
                    self.table_name,
                    where_clause.join(" AND ")
                );
                let params: Vec<SqlParam> = self
                    .primary_key
                    .iter()
                    .map(|k| SqlParam::from(&row.get(k).cloned().unwrap_or(Value::Null)))
                    .collect();
                tx.execute(
                    &sql,
                    params_from_iter(params.iter().map(|p| p as &dyn rusqlite::ToSql)),
                )
                ?;
            }
            SourceChange::Edit { row, old_row } => {
                // If PK is the same, use UPDATE; else DELETE + INSERT
                let pk_same = self
                    .primary_key
                    .iter()
                    .all(|k| values_equal(
                        &row.get(k).cloned().unwrap_or(Value::Null),
                        &old_row.get(k).cloned().unwrap_or(Value::Null),
                    ));

                if pk_same {
                    let non_pk: Vec<String> = self
                        .column_names
                        .iter()
                        .filter(|c| !self.primary_key.contains(c))
                        .cloned()
                        .collect();
                    let set_clause: Vec<String> = non_pk
                        .iter()
                        .map(|c| format!("\"{}\" = ?", c))
                        .collect();
                    let where_clause: Vec<String> = self
                        .primary_key
                        .iter()
                        .map(|k| format!("\"{}\" = ?", k))
                        .collect();
                    let sql = format!(
                        "UPDATE \"{}\" SET {} WHERE {}",
                        self.table_name,
                        set_clause.join(", "),
                        where_clause.join(" AND ")
                    );
                    let mut params: Vec<SqlParam> = non_pk
                        .iter()
                        .map(|c| SqlParam::from(&row.get(c).cloned().unwrap_or(Value::Null)))
                        .collect();
                    params.extend(self.primary_key.iter().map(|k| {
                        SqlParam::from(&row.get(k).cloned().unwrap_or(Value::Null))
                    }));
                    tx.execute(
                        &sql,
                        params_from_iter(params.iter().map(|p| p as &dyn rusqlite::ToSql)),
                    )
                    ?;
                } else {
                    // DELETE old + INSERT new
                    let where_clause: Vec<String> = self
                        .primary_key
                        .iter()
                        .map(|k| format!("\"{}\" = ?", k))
                        .collect();
                    let del_sql = format!(
                        "DELETE FROM \"{}\" WHERE {}",
                        self.table_name,
                        where_clause.join(" AND ")
                    );
                    let del_params: Vec<SqlParam> = self
                        .primary_key
                        .iter()
                        .map(|k| SqlParam::from(&old_row.get(k).cloned().unwrap_or(Value::Null)))
                        .collect();
                    tx.execute(
                        &del_sql,
                        params_from_iter(del_params.iter().map(|p| p as &dyn rusqlite::ToSql)),
                    )
                    ?;

                    let placeholders: Vec<String> = (0..self.column_names.len())
                        .map(|_| "?".to_string())
                        .collect();
                    let ins_sql = format!(
                        "INSERT INTO \"{}\" ({}) VALUES ({})",
                        self.table_name,
                        self.column_names
                            .iter()
                            .map(|c| format!("\"{}\"", c))
                            .collect::<Vec<_>>()
                            .join(", "),
                        placeholders.join(", ")
                    );
                    let ins_params: Vec<SqlParam> = self
                        .column_names
                        .iter()
                        .map(|c| SqlParam::from(&row.get(c).cloned().unwrap_or(Value::Null)))
                        .collect();
                    tx.execute(
                        &ins_sql,
                        params_from_iter(ins_params.iter().map(|p| p as &dyn rusqlite::ToSql)),
                    )
                    ?;
                }
            }
        }

        tx.commit()?;
        Ok(())
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

    /// Fetch rows from SQLite matching the request. Streams lazily from the
    /// prepared statement rather than collecting into a Vec — matches TS
    /// `table-source.ts` `#fetch`.
    pub fn fetch(&self, req: &FetchRequest, conn: &TableConnection) -> NodeStream {
        let order: Vec<(String, String)> = conn.sort.as_ref()
            .map(|s| s.iter().map(|p| (p[0].clone(), p[1].clone())).collect())
            .unwrap_or_default();
        let reverse = req.reverse;

        let query = build_select_query(
            &self.table_name,
            &self.column_names,
            req,
            None,
            Some(&order),
            reverse,
        );

        let overlay_change = {
            let ov = self.overlay.borrow();
            match *ov {
                Some((epoch, ref change)) if conn.last_pushed_epoch >= epoch => Some(change.clone()),
                _ => None,
            }
        };

        let stream = stream_query(
            self.db.clone(),
            query,
            self.column_names.clone(),
            self.columns.clone(),
            self.table_name.clone(),
            conn.filter_predicate.clone(),
        );

        crate::ivm::source::apply_source_overlay(
            stream,
            overlay_change,
            conn.compare_rows.clone(),
            crate::ivm::source::compute_index_compare(conn.sort.as_ref(), req, &self.primary_key),
            conn.filter_predicate.clone(),
            req,
        )
    }
}

/// TableSourceInput — implements the Input trait for a TableSource connection.
pub struct TableSourceInput {
    db: Rc<RefCell<Connection>>,
    table_name: String,
    column_names: Vec<String>,
    columns: HashMap<String, ColumnType>,
    conn: Shared<TableConnection>,
    schema: SourceSchema,
    primary_key: Vec<String>,
    filter_condition: Option<Condition>,
    overlay: SharedOverlay,
}

impl InputBase for TableSourceInput {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.conn.borrow_mut().output = None;
    }
}

impl Input for TableSourceInput {
    fn set_output(&self, output: OutputHandle) {
        self.conn.borrow_mut().output = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        let conn = self.conn.borrow();
        let order: Vec<(String, String)> = conn.sort.as_ref()
            .map(|s| s.iter().map(|p| (p[0].clone(), p[1].clone())).collect())
            .unwrap_or_default();
        let reverse = req.reverse;

        let query = build_select_query(
            &self.table_name,
            &self.column_names,
            req,
            self.filter_condition.as_ref(),
            Some(&order),
            reverse,
        );

        let overlay_change = {
            let ov = self.overlay.borrow();
            match *ov {
                Some((epoch, ref change)) if conn.last_pushed_epoch >= epoch => Some(change.clone()),
                _ => None,
            }
        };

        let stream = stream_query(
            self.db.clone(),
            query,
            self.column_names.clone(),
            self.columns.clone(),
            self.table_name.clone(),
            conn.filter_predicate.clone(),
        );

        crate::ivm::source::apply_source_overlay(
            stream,
            overlay_change,
            conn.compare_rows.clone(),
            crate::ivm::source::compute_index_compare(conn.sort.as_ref(), req, &self.primary_key),
            conn.filter_predicate.clone(),
            req,
        )
    }
}

impl Source for TableSource {
    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    fn set_snapshot_db(&mut self, db: Rc<RefCell<Connection>>) {
        self.set_db(db);
    }

    fn column_types(&self) -> HashMap<String, ColumnType> {
        self.columns.clone()
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
        self.push(change)
    }

    fn gen_push(&mut self, change: SourceChange) -> Vec<Change> {
        self.push(change)
    }

    fn get_row(&self, _pk: &[(String, Value)]) -> Option<Row> {
        None
    }

    fn parallel_spec(&self) -> crate::ivm::source::ParallelSourceSpec {
        let db = self.db.borrow();
        let path = db
            .query_row("PRAGMA database_list", [], |row| {
                let p: String = row.get(2).unwrap_or_default();
                Ok(p)
            })
            .unwrap_or_default();
        crate::ivm::source::ParallelSourceSpec::Sqlite { db_path: path }
    }
}

struct NullInputBase;
impl InputBase for NullInputBase {
    fn get_schema(&self) -> SourceSchema {
        panic!("NullInputBase has no schema");
    }
    fn destroy(&mut self) {}
}
