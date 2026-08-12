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
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use rusqlite::{Connection, params_from_iter};

use rustc_hash::FxHashMap;

use crate::builder::ast::Condition;
use crate::ivm::change::{
    Change, SourceChange, make_add_change, make_edit_change, make_remove_change,
};
use crate::ivm::data::{
    Comparator, Node, Row, SortOrder, Value, compare_values, make_comparator, values_equal,
};
use crate::ivm::filter_push::filter_push;
use crate::ivm::operator::{Basis, FetchRequest, Input, InputBase, OutputHandle, Shared, Start};
use crate::ivm::schema::{ColumnType, SourceSchema, System};
use crate::ivm::source::Source;
use crate::ivm::stream::NodeStream;
use crate::sqlite::query_builder::{SqlParam, SqlQuery, build_select_query};

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
/// - Rust drops struct fields in declaration order. The dependent fields are
///   therefore declared first: `rows`, then `_stmt`, then `_guard`, then the
///   owning `_conn`. This order is part of the safety contract.
struct LazyRows {
    rows: Option<rusqlite::Rows<'static>>,
    /// Cached statement (TS parity: zqlite's `StatementCache` keyed by SQL
    /// text, table-source.ts:289). Dropping a `CachedStatement` RESETS it and
    /// returns it to the connection's cache instead of finalizing — the next
    /// fetch with the same SQL skips the prepare. A reset statement is not
    /// busy, so it can never block the snapshotter's ROLLBACK; the cache is
    /// flushed (statements finalized) in `Snapshot::drop` before close.
    _stmt: Pin<Box<rusqlite::CachedStatement<'static>>>,
    _guard: Ref<'static, Connection>,
    _conn: Rc<RefCell<Connection>>,
    column_names: Vec<String>,
    columns: HashMap<String, ColumnType>,
    table_name: String,
    /// Rows produced so far — used to THROTTLE the per-fetch economic budget
    /// check (advance_gate) to every 64 rows.
    fetched: u64,
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
        let _t = crate::perf_trace::scope("source.sql_prepare");
        // Hold an immutable RefCell borrow for the entire struct lifetime.
        let guard: Ref<'_, Connection> = conn.borrow();
        let guard_static: Ref<'static, Connection> = unsafe { std::mem::transmute(guard) };

        // Prepare (or fetch from the per-connection statement cache) while the
        // connection is borrowed. TS parity: zqlite caches prepared statements
        // by SQL text; re-preparing per fetch costs ~25% of a correlated-EXISTS
        // hydrate (one child SELECT per parent row, same SQL every time).
        let stmt: rusqlite::CachedStatement<'_> = guard_static.prepare_cached(&sql)?;
        let stmt_static: rusqlite::CachedStatement<'static> = unsafe { std::mem::transmute(stmt) };
        let mut stmt_pin = Box::pin(stmt_static);

        // Bind parameters and create the rows cursor. The statement's heap
        // address is stable because it is pinned.
        let rows: rusqlite::Rows<'_> = {
            let stmt_mut: &mut rusqlite::CachedStatement<'static> =
                unsafe { Pin::get_unchecked_mut(Pin::as_mut(&mut stmt_pin)) };
            let param_refs: Vec<&dyn rusqlite::ToSql> =
                params.iter().map(|p| p as &dyn rusqlite::ToSql).collect();
            stmt_mut.query(rusqlite::params_from_iter(param_refs.iter().copied()))?
        };
        let rows_static: rusqlite::Rows<'static> = unsafe { std::mem::transmute(rows) };

        Ok(Box::pin(LazyRows {
            rows: Some(rows_static),
            _stmt: stmt_pin,
            _guard: guard_static,
            _conn: conn,
            column_names,
            columns,
            table_name,
            fetched: 0,
            _pin: PhantomPinned,
        }))
    }
}

/// Classification of a SQLite step (`rows.next()`) error, deciding how the row
/// iterator reacts.
///
/// TS (`zqlite/src/table-source.ts` `#mapFromSQLiteTypes`, line 377) calls
/// `rowIterator.next()` with NO try/catch — any step error THROWS and
/// propagates out of `#fetch`, aborting the pipeline (→ view-syncer teardown →
/// client rehydrate at a consistent frame). It never truncates the stream.
/// Cancellation in TS is driver-level (the JS caller stops pulling); a
/// SQLITE_INTERRUPT surfaces only when the actor's own cancel path has already
/// been engaged.
///
/// Rust must match:
/// - `Interrupt` (SQLITE_INTERRUPT): a cancellation. The engine's advance loop
///   checks `cancellation_token.is_cancelled()` between rows and unwinds
///   cleanly; the row iterator just stops quietly (no "row read error" log, no
///   panic) and lets that cancel path drive teardown.
/// - `HardError` (SQLITE_CORRUPT / "malformed" / anything else): a real read
///   failure. We must NOT silently return `None` (that truncates the result and
///   serves a partial/corrupt view). We propagate it — a panic here is caught by
///   the napi `catch_unwind` (napi/src/lib.rs:222) and surfaced as a thrown
///   error, exactly the TS lifecycle for a thrown SQLite step error.
enum RowErr {
    Interrupt,
    HardError,
}

/// Classify a `rows.next()` error into clean-cancel vs propagate. Uses the
/// extended-code-aware `sqlite_error_code()` (rusqlite 0.32) so any
/// SQLITE_CORRUPT variant (e.g. SQLITE_CORRUPT_VTAB) also classifies as a hard
/// error.
fn classify_row_error(e: &rusqlite::Error) -> RowErr {
    match e.sqlite_error_code() {
        Some(rusqlite::ffi::ErrorCode::OperationInterrupted) => RowErr::Interrupt,
        // Everything else — corruption ("database disk image is malformed"),
        // I/O errors, etc. — is a hard error that must abort rather than
        // truncate.
        _ => RowErr::HardError,
    }
}

/// Wrapper so `LazyRows` (which must stay pinned) can implement `Iterator`.
struct LazyRowsIter(Pin<Box<LazyRows>>);

impl Iterator for LazyRowsIter {
    type Item = Row;

    fn next(&mut self) -> Option<Row> {
        let this: &mut LazyRows = unsafe { Pin::get_unchecked_mut(Pin::as_mut(&mut self.0)) };
        // Per-fetch economic budget check (TS parity, point 2): if an advance is
        // in flight on this (actor) thread and has blown its hydration-time
        // budget, END this stream now instead of grinding through a fat fetch.
        // Returning None reads as a normal short-input end-of-stream (Take/Cap
        // finalize cleanly — no guard trip), and the advance loop sees the gate
        // tripped and rehydrates. Throttled to every 64 rows. Off during hydrate
        // and on worker threads (gate is thread-local + only armed for advance).
        this.fetched = this.fetched.wrapping_add(1);
        if this.fetched % 64 == 1 && crate::advance_gate::should_stop_fetch() {
            return None;
        }
        // Borrow the shared per-source fields instead of cloning them on every
        // row (review #1). `rows` is a disjoint field, so it can be borrowed
        // mutably alongside these immutable borrows — this avoids three heap
        // allocations (Vec<String> + HashMap + String) per fetched row, which at
        // 100K rows is 300K needless allocations on the hot read path. The
        // clones only ever fed `&str` args and the cold panic! path.
        let rows = this.rows.as_mut()?;
        let column_names = &this.column_names;
        let columns = &this.columns;
        let table_name = &this.table_name;
        let stepped = {
            let _t = crate::perf_trace::scope("source.sql_step");
            rows.next()
        };
        match stepped {
            Ok(Some(raw_row)) => {
                let _t = crate::perf_trace::scope("source.row_mat");
                let mut map: FxHashMap<String, Value> = FxHashMap::default();
                for (i, col) in column_names.iter().enumerate() {
                    let val = crate::sqlite::db::read_value_lossy(raw_row, i);
                    let value = sqlite_value_to_ivm(val, columns.get(col), table_name, col);
                    map.insert(col.clone(), value);
                }
                Some(Arc::new(map))
            }
            Ok(None) => None,
            Err(e) => match classify_row_error(&e) {
                // Cancellation: stop iterating quietly. The engine's between-rows
                // cancel check + napi cancel path own teardown; do not log this
                // as a "row read error" and do not panic. Matches TS, where
                // cancellation is driver-level and never a corruption error.
                RowErr::Interrupt => None,
                // Corruption / I/O / other: DO NOT truncate — a swallowed corrupt
                // read serves a partial result (a correctness leak). Propagate as
                // a hard error so the napi catch_unwind surfaces it as a thrown
                // error → view-syncer teardown → rehydrate at a consistent frame.
                // This mirrors TS `rowIterator.next()` throwing out of `#fetch`.
                RowErr::HardError => {
                    panic!("[rust-ivm] row read error for {}: {}", table_name, e);
                }
            },
        }
    }
}

fn stream_query(
    db: Rc<RefCell<Connection>>,
    query: SqlQuery,
    column_names: Vec<String>,
    columns: HashMap<String, ColumnType>,
    table_name: String,
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
            if matches!(classify_row_error(&e), RowErr::Interrupt) {
                // An out-of-band interrupt can race any SQLite phase. Treat it
                // as the same clean cancellation whether it lands in prepare,
                // bind/query setup, or rows.next(). The previous phase-specific
                // handling made cancellation nondeterministically panic when
                // the interrupt arrived just before the first step.
                return Box::new(std::iter::empty());
            }
            // Propagate, never swallow. A prepare/bind failure (schema drift,
            // missing column, malformed SQL) must NOT masquerade as an empty
            // result — that silently corrupts hydration/removals. Panic here is
            // caught by EngineHandle::call's catch_unwind → thrown error →
            // view-syncer teardown/reset, mirroring TS which lets the error
            // propagate out of `#fetch` (zqlite/table-source.ts:283).
            panic!(
                "[rust-ivm] query prepare/bind error for {}: {}",
                table_name_for_err, e
            );
        }
    };

    // The SQL query has already applied the source condition. TS only applies
    // the JS predicate inside generateWithOverlay, to overlay rows that did not
    // pass through SQLite. Reapplying it here changes SQLite storage-class
    // semantics (notably for JSON text compared with scalar literals).
    Box::new(LazyRowsIter(lazy))
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
    let is_bool = matches!(col_type, Some(ColumnType::Boolean { .. }));
    let is_json = matches!(col_type, Some(ColumnType::Json { .. }));
    match val {
        Ok(Sv::Null) => Value::Null,
        // TS/better-sqlite3 surfaces a read error as a thrown error; do the same
        // (the napi catch_unwind turns this panic into a thrown JS error) instead
        // of silently coercing a decode failure to NULL. Unreachable for a
        // `get::<Value>` (Value is the universal storage type), but never swallow.
        Err(e) => panic!("failed to read {table}.{col} from SQLite: {e}"),

        // TS `boolean` => `!!v`. 0 / 0.0 / "" are false; a non-empty string is
        // true; a Blob (JS Buffer) is ALWAYS truthy — match each.
        Ok(Sv::Integer(n)) if is_bool => Value::Bool(n != 0),
        Ok(Sv::Real(n)) if is_bool => Value::Bool(n != 0.0),
        Ok(Sv::Text(s)) if is_bool => Value::Bool(!s.is_empty()),
        Ok(Sv::Blob(_)) if is_bool => Value::Bool(true),

        // TS `json` => `JSON.parse(v)`, which THROWS on invalid JSON. Validate at
        // ingest (matching TS's failure point + message); the raw string is then
        // guaranteed-valid for the wire, which re-parses it into an object on JS.
        Ok(Sv::Integer(n)) if is_json => json_sqlite_text_to_ivm(&n.to_string(), table, col),
        Ok(Sv::Real(n)) if is_json => json_sqlite_text_to_ivm(&n.to_string(), table, col),
        Ok(Sv::Text(s)) if is_json => json_sqlite_text_to_ivm(&s, table, col),
        Ok(Sv::Blob(b)) if is_json => {
            let s = String::from_utf8_lossy(&b);
            json_sqlite_text_to_ivm(&s, table, col)
        }

        // number / string columns (and untyped): pass through unchanged.
        Ok(Sv::Integer(n)) => {
            // Reject integers outside ±(2^53-1) rather than silently losing
            // precision — TS `fromSQLiteType` throws UnsupportedValueError here
            // (same message); our panic is caught by the napi boundary and
            // rethrown to JS, so the failure surfaces identically.
            if !(-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&n) {
                panic!("value {n} (in {table}.{col}) is outside of supported bounds");
            }
            Value::F64(n as f64)
        }
        Ok(Sv::Real(n)) => Value::F64(n),
        Ok(Sv::Text(s)) => Value::Str(Arc::from(s.as_str())),
        // Blob in a non-json/non-string column: Zero has no bytes Value type
        // (TS returns the raw Buffer, which then breaks downstream). Best-effort
        // lossy-string decode; documented unsupported in both engines.
        Ok(Sv::Blob(b)) => Value::Str(Arc::from(String::from_utf8_lossy(&b).as_ref())),
    }
}

/// `fromSQLiteType('json', value)` is `JSON.parse(value)` in TS. JSON.parse
/// first stringifies non-string SQLite scalars, then returns the corresponding
/// JS scalar; only arrays and objects remain JSON containers.
fn json_sqlite_text_to_ivm(text: &str, table: &str, col: &str) -> Value {
    let parsed = serde_json::from_str::<serde_json::Value>(text)
        .unwrap_or_else(|error| panic!("Failed to parse JSON for {table}.{col}: {error}"));
    match parsed {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Bool(value),
        serde_json::Value::Number(value) => {
            Value::F64(value.as_f64().unwrap_or_else(|| {
                panic!("Failed to parse JSON for {table}.{col}: invalid number")
            }))
        }
        serde_json::Value::String(value) => Value::Str(Arc::from(value)),
        value => Value::Json(Arc::from(value.to_string())),
    }
}

/// Connection: a downstream consumer of the TableSource.
pub struct TableConnection {
    pub sort: Option<SortOrder>,
    pub internal_sort: SortOrder,
    pub split_edit_keys: Option<Vec<String>>,
    pub compare_rows: Comparator,
    pub filter_condition: Option<Condition>,
    pub filter_predicate: Option<Arc<dyn Fn(&Row) -> bool>>,
    pub last_pushed_epoch: usize,
    pub output: Option<OutputHandle>,
}

/// Shared overlay — accessible by both TableSource (writer) and TableSourceInput (reader)
type SharedOverlay = Rc<RefCell<Option<(usize, SourceChange)>>>;
type SharedSnapshotDb = Rc<RefCell<Rc<RefCell<Connection>>>>;

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
    db: SharedSnapshotDb,
    /// Shared with every `TableSourceInput` so `destroy()` can splice its
    /// connection out (TS parity: zqlite table-source.ts `destroy` removes the
    /// connection from `#connections`). A plain Vec leaked one TableConnection
    /// per removed query, growing memory AND the per-push scan forever.
    connections: Rc<RefCell<Vec<Shared<TableConnection>>>>,
    overlay: SharedOverlay,
    /// Changes already pushed during the current advance. TS persists these
    /// into its private PREV transaction; Rust layers them over the read-only
    /// PREV snapshot on every fetch and clears them when the snapshot changes.
    applied_changes: Rc<RefCell<Vec<SourceChange>>>,
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

        crate::live_count::inc(&crate::live_count::TABLE_SOURCE);
        TableSource {
            table_name: table_name.to_string(),
            columns,
            column_names,
            primary_key: primary_key.clone(),
            primary_index_sort,
            db: Rc::new(RefCell::new(db)),
            connections: Rc::new(RefCell::new(Vec::new())),
            overlay: Rc::new(RefCell::new(None)),
            applied_changes: Rc::new(RefCell::new(Vec::new())),
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
        *self.db.borrow_mut() = db;
        self.applied_changes.borrow_mut().clear();
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
        let internal_sort = sort
            .clone()
            .unwrap_or_else(|| self.primary_index_sort.clone());
        let compare_rows = make_comparator(internal_sort.clone(), false);

        crate::live_count::inc(&crate::live_count::TABLE_CONNECTION);
        let conn = Rc::new(RefCell::new(TableConnection {
            sort: sort.clone(),
            internal_sort,
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
        let _overlay_epoch = Rc::new(RefCell::new(0usize)); // tracks last_pushed_epoch

        crate::live_count::inc(&crate::live_count::TABLE_SOURCE_INPUT);
        let input: Shared<dyn Input> = Rc::new(RefCell::new(TableSourceInput {
            db,
            table_name,
            column_names,
            columns,
            conn: conn.clone(),
            connections: self.connections.clone(),
            schema,
            filter_condition: filter_condition.clone(),
            overlay: self.overlay.clone(),
            applied_changes: self.applied_changes.clone(),
        }));

        self.connections.borrow_mut().push(conn.clone());
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
        if let SourceChange::Edit {
            ref row,
            ref old_row,
        } = change
        {
            let should_split = self.connections.borrow().iter().any(|c| {
                let conn = c.borrow();
                conn.split_edit_keys.as_ref().is_some_and(|keys| {
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
                // the removal); our pinned snapshot transaction is not mutated, so a
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
            .borrow()
            .iter()
            .filter(|c| c.borrow().output.is_some())
            .cloned()
            .collect();

        let all_changes = Vec::new();

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
        let write_result = {
            let _t = crate::perf_trace::scope("source.write");
            self.write_change(&change)
        };
        if let Err(e) = write_result {
            eprintln!(
                "[rust-ivm] write_change error for {}: {}",
                self.table_name, e
            );
        }
        self.applied_changes.borrow_mut().push(change);

        // Overlay cleared by _overlay_guard Drop

        all_changes
    }

    fn validate_change(&self, change: &SourceChange) {
        let _t = crate::perf_trace::scope("source.validate");
        let snapshot_db = self.db.borrow();
        let db = snapshot_db.borrow();
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
                    panic!(
                        "source drift: Edit missing old row from {}",
                        self.table_name
                    );
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

        // Propagate, never swallow. A prepare/execution failure must NOT be
        // read as "row does not exist" — that misclassifies edit/remove input
        // and can wrongly accept an add. Panic is caught upstream (catch_unwind)
        // → teardown/reset, matching TS which lets the error propagate.
        let mut stmt = db.prepare(&sql).unwrap_or_else(|e| {
            panic!(
                "[rust-ivm] check_exists prepare error for {}: {}",
                self.table_name, e
            )
        });
        stmt.exists(params_from_iter(
            params.iter().map(|p| p as &dyn rusqlite::ToSql),
        ))
        .unwrap_or_else(|e| {
            panic!(
                "[rust-ivm] check_exists query error for {}: {}",
                self.table_name, e
            )
        })
    }

    fn write_change(&self, _change: &SourceChange) -> Result<(), rusqlite::Error> {
        // No-op: changes are already written to zero.db by the change-streamer.
        // The Rust IVM reads from zero.db for hydrate and stores pushed changes
        // in the in-memory overlay. Writing to zero.db here would conflict with
        // the change-streamer's WAL2-mode writes (rusqlite doesn't support WAL2).
        Ok(())
    }

    fn _write_change_unused(&self, change: &SourceChange) -> Result<(), rusqlite::Error> {
        let snapshot_db = self.db.borrow().clone();
        let mut db = snapshot_db.borrow_mut();
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
                )?;
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
                )?;
            }
            SourceChange::Edit { row, old_row } => {
                // If PK is the same, use UPDATE; else DELETE + INSERT
                let pk_same = self.primary_key.iter().all(|k| {
                    values_equal(
                        &row.get(k).cloned().unwrap_or(Value::Null),
                        &old_row.get(k).cloned().unwrap_or(Value::Null),
                    )
                });

                if pk_same {
                    let non_pk: Vec<String> = self
                        .column_names
                        .iter()
                        .filter(|c| !self.primary_key.contains(c))
                        .cloned()
                        .collect();
                    let set_clause: Vec<String> =
                        non_pk.iter().map(|c| format!("\"{}\" = ?", c)).collect();
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
                    params.extend(
                        self.primary_key
                            .iter()
                            .map(|k| SqlParam::from(&row.get(k).cloned().unwrap_or(Value::Null))),
                    );
                    tx.execute(
                        &sql,
                        params_from_iter(params.iter().map(|p| p as &dyn rusqlite::ToSql)),
                    )?;
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
                    )?;

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
                    )?;
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
        let order: Vec<(String, String)> = conn
            .sort
            .as_ref()
            .map(|s| s.iter().map(|p| (p[0].clone(), p[1].clone())).collect())
            .unwrap_or_default();
        let reverse = req.reverse;

        let query = build_select_query(
            &self.table_name,
            &self.column_names,
            &self.columns,
            req,
            None,
            Some(&order),
            reverse,
        );

        let overlay_change = {
            let ov = self.overlay.borrow();
            match *ov {
                Some((epoch, ref change)) if conn.last_pushed_epoch >= epoch => {
                    Some(change.clone())
                }
                _ => None,
            }
        };

        let mut overlay_changes = {
            let _t = crate::perf_trace::scope("source.overlay");
            applied_changes_for_request(&self.applied_changes.borrow(), req, &order, &self.columns)
        };
        let historical_change_count = overlay_changes.len();
        if let Some(change) = overlay_change {
            overlay_changes.push(change);
        }

        let stream = stream_query(
            self.db.borrow().clone(),
            query,
            self.column_names.clone(),
            self.columns.clone(),
            self.table_name.clone(),
        );

        let _t = crate::perf_trace::scope("source.overlay");
        crate::ivm::source::apply_source_overlays(
            stream,
            overlay_changes,
            conn.compare_rows.clone(),
            // TS TableSource passes the connection/query comparator to
            // generateWithOverlay (zqlite/src/table-source.ts #fetch). The
            // constraint-first index comparator belongs to MemorySource's
            // internal indexes and changes start/limit replacement behavior
            // when used at this SQLite boundary.
            conn.compare_rows.clone(),
            conn.filter_predicate.clone(),
            req,
            crate::ivm::source::HistoricalOverlayContext {
                change_count: historical_change_count,
                primary_key: self.primary_key.clone(),
                sort: conn.internal_sort.clone(),
            },
        )
    }
}

/// TableSourceInput — implements the Input trait for a TableSource connection.
pub struct TableSourceInput {
    db: SharedSnapshotDb,
    table_name: String,
    column_names: Vec<String>,
    columns: HashMap<String, ColumnType>,
    conn: Shared<TableConnection>,
    /// Back-reference to the owning source's connection list so `destroy()`
    /// can splice this connection out (TS parity: zqlite table-source.ts:242).
    connections: Rc<RefCell<Vec<Shared<TableConnection>>>>,
    schema: SourceSchema,
    filter_condition: Option<Condition>,
    overlay: SharedOverlay,
    applied_changes: Rc<RefCell<Vec<SourceChange>>>,
}

impl InputBase for TableSourceInput {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.conn.borrow_mut().output = None;
        // Splice this connection out of the source (TS parity: zqlite
        // table-source.ts destroy() → #connections.splice). Without this every
        // removed query permanently retained its TableConnection, growing both
        // memory and the per-push connection scan.
        self.connections
            .borrow_mut()
            .retain(|c| !Rc::ptr_eq(c, &self.conn));
    }
}

impl Input for TableSourceInput {
    fn set_output(&self, output: OutputHandle) {
        self.conn.borrow_mut().output = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        let conn = self.conn.borrow();
        let order: Vec<(String, String)> = conn
            .sort
            .as_ref()
            .map(|s| s.iter().map(|p| (p[0].clone(), p[1].clone())).collect())
            .unwrap_or_default();
        let reverse = req.reverse;

        let query = build_select_query(
            &self.table_name,
            &self.column_names,
            &self.columns,
            req,
            self.filter_condition.as_ref(),
            Some(&order),
            reverse,
        );

        let overlay_change = {
            let ov = self.overlay.borrow();
            match *ov {
                Some((epoch, ref change)) if conn.last_pushed_epoch >= epoch => {
                    Some(change.clone())
                }
                _ => None,
            }
        };

        let mut overlay_changes = {
            let _t = crate::perf_trace::scope("source.overlay");
            applied_changes_for_request(&self.applied_changes.borrow(), req, &order, &self.columns)
        };
        let historical_change_count = overlay_changes.len();
        if let Some(change) = overlay_change {
            overlay_changes.push(change);
        }

        let stream = stream_query(
            self.db.borrow().clone(),
            query,
            self.column_names.clone(),
            self.columns.clone(),
            self.table_name.clone(),
        );

        let _t = crate::perf_trace::scope("source.overlay");
        crate::ivm::source::apply_source_overlays(
            stream,
            overlay_changes,
            conn.compare_rows.clone(),
            // See TableSource::fetch: production TS uses the query comparator
            // for both splice ordering and overlaysForStartAt.
            conn.compare_rows.clone(),
            conn.filter_predicate.clone(),
            req,
            crate::ivm::source::HistoricalOverlayContext {
                change_count: historical_change_count,
                primary_key: self.schema.primary_key.clone(),
                sort: conn.internal_sort.clone(),
            },
        )
    }
}

fn applied_changes_for_request(
    changes: &[SourceChange],
    req: &FetchRequest,
    order: &[(String, String)],
    columns: &HashMap<String, ColumnType>,
) -> Vec<SourceChange> {
    let Some(start) = &req.start else {
        return changes.to_vec();
    };

    changes
        .iter()
        .filter_map(|change| match change {
            SourceChange::Add { row } => {
                sql_start_matches(row, start, req.reverse, order, columns).then(|| change.clone())
            }
            SourceChange::Remove { row } => {
                sql_start_matches(row, start, req.reverse, order, columns).then(|| change.clone())
            }
            SourceChange::Edit { row, old_row } => {
                let old_matches = sql_start_matches(old_row, start, req.reverse, order, columns);
                let new_matches = sql_start_matches(row, start, req.reverse, order, columns);
                match (old_matches, new_matches) {
                    (true, true) => Some(change.clone()),
                    (true, false) => Some(SourceChange::Remove {
                        row: old_row.clone(),
                    }),
                    (false, true) => Some(SourceChange::Add { row: row.clone() }),
                    (false, false) => None,
                }
            }
        })
        .collect()
}

fn sql_start_matches(
    row: &Row,
    start: &Start,
    reverse: bool,
    order: &[(String, String)],
    columns: &HashMap<String, ColumnType>,
) -> bool {
    let is_optional = |field: &str| {
        matches!(
            columns.get(field),
            Some(ColumnType::Boolean { optional: true })
                | Some(ColumnType::Number { optional: true })
                | Some(ColumnType::String { optional: true })
                | Some(ColumnType::Json { optional: true })
        )
    };
    let range_matches = |field: &str, direction: &str| {
        let row_value = row.get(field).cloned().unwrap_or(Value::Null);
        let start_value = start.row.get(field).cloned().unwrap_or(Value::Null);
        let greater = if direction == "asc" {
            !reverse
        } else {
            reverse
        };
        let optional = is_optional(field);

        // Mirror of query_builder's VALUE-aware NULL guard (take-bound
        // divergence fix): the generated SQL takes the NULL-aware branch
        // whenever the column is declared optional OR the start value is
        // NULL (spec-drift robustness). The overlay matcher must agree with
        // the SQL exactly, or fetch results diverge between the db rows and
        // the same-advance overlay.
        if greater && start_value.is_null() {
            // SQL: `(? IS NULL OR col > ?)` with a NULL param — always true.
            return true;
        }
        if !greater && row_value.is_null() && (optional || start_value.is_null()) {
            // SQL: `(col IS NULL OR col < ?)` — row NULL matches.
            return true;
        }
        if row_value.is_null() || start_value.is_null() {
            return false;
        }
        let ordering = compare_values(&row_value, &start_value);
        if greater {
            ordering == CmpOrdering::Greater
        } else {
            ordering == CmpOrdering::Less
        }
    };
    let equality_matches = |field: &str| {
        let row_value = row.get(field).cloned().unwrap_or(Value::Null);
        let start_value = start.row.get(field).cloned().unwrap_or(Value::Null);
        if row_value.is_null() || start_value.is_null() {
            // VALUE-aware IS semantics (see query_builder): a NULL start value
            // now always generates `col IS ?`, which matches exactly the
            // NULL rows — declared optionality no longer changes the outcome.
            return row_value.is_null() && start_value.is_null();
        }
        compare_values(&row_value, &start_value) == CmpOrdering::Equal
    };

    for (index, (field, direction)) in order.iter().enumerate() {
        if order[..index]
            .iter()
            .all(|(prefix, _)| equality_matches(prefix))
            && range_matches(field, direction)
        {
            return true;
        }
    }
    start.basis == Basis::At && order.iter().all(|(field, _)| equality_matches(field))
}

impl Source for TableSource {
    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn primary_key(&self) -> &[String] {
        &self.primary_key
    }

    fn has_active_connections(&self) -> bool {
        self.connections
            .borrow()
            .iter()
            .any(|connection| connection.borrow().output.is_some())
    }

    fn connection_count(&self) -> usize {
        self.connections.borrow().len()
    }

    fn truncate_connections(&mut self, count: usize) {
        self.connections.borrow_mut().truncate(count);
    }

    fn set_snapshot_db(&mut self, db: Rc<RefCell<Connection>>) {
        self.set_db(db);
    }

    fn clear_advance_state(&mut self) {
        self.applied_changes.borrow_mut().clear();
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

    fn gen_push(&mut self, change: SourceChange) -> Vec<Change> {
        self.push(change)
    }

    fn get_row(&self, _pk: &[(String, Value)]) -> Option<Row> {
        None
    }
}

struct NullInputBase;
impl InputBase for NullInputBase {
    fn get_schema(&self) -> SourceSchema {
        panic!("NullInputBase has no schema");
    }
    fn destroy(&mut self) {}
}

#[cfg(test)]
mod value_parity_tests {
    //! `sqlite_value_to_ivm` must match TS `fromSQLiteType` (zqlite table-source.ts).
    use super::*;
    use rusqlite::types::Value as Sv;

    fn conv(v: Sv, ct: Option<ColumnType>) -> Value {
        sqlite_value_to_ivm(Ok(v), ct.as_ref(), "t", "c")
    }

    #[test]
    fn boolean_matches_ts_double_bang() {
        let b = || Some(ColumnType::Boolean { optional: false });
        assert_eq!(conv(Sv::Integer(0), b()), Value::Bool(false));
        assert_eq!(conv(Sv::Integer(5), b()), Value::Bool(true));
        assert_eq!(conv(Sv::Real(0.0), b()), Value::Bool(false));
        assert_eq!(conv(Sv::Real(1.0), b()), Value::Bool(true));
        assert_eq!(conv(Sv::Text(String::new()), b()), Value::Bool(false)); // !!"" == false
        assert_eq!(conv(Sv::Text("x".into()), b()), Value::Bool(true)); // !!"x" == true
        assert_eq!(conv(Sv::Blob(vec![]), b()), Value::Bool(true)); // !!Buffer == true
        assert_eq!(conv(Sv::Null, b()), Value::Null);
    }

    #[test]
    fn valid_json_tagged() {
        let j = Some(ColumnType::Json { optional: false });
        assert!(matches!(
            conv(Sv::Text("{\"a\":1}".into()), j),
            Value::Json(value) if value.as_ref() == "{\"a\":1}"
        ));
    }

    #[test]
    #[should_panic(expected = "Failed to parse JSON for t.c")]
    fn invalid_json_panics_like_ts() {
        let _ = conv(
            Sv::Text("{not json".into()),
            Some(ColumnType::Json { optional: false }),
        );
    }

    #[test]
    #[should_panic(expected = "outside of supported bounds")]
    fn integer_over_2_53_panics_like_ts() {
        conv(Sv::Integer(9_007_199_254_740_992), None);
    }

    #[test]
    fn read_error_panics_not_swallowed_to_null() {
        let r = std::panic::catch_unwind(|| {
            sqlite_value_to_ivm(Err(rusqlite::Error::InvalidQuery), None, "t", "c")
        });
        assert!(r.is_err(), "a read error must panic, not become NULL");
    }

    #[test]
    fn number_string_passthrough() {
        assert_eq!(conv(Sv::Integer(42), None), Value::F64(42.0));
        assert_eq!(conv(Sv::Real(1.5), None), Value::F64(1.5));
        assert_eq!(
            conv(Sv::Text("hi".into()), None),
            Value::Str(Arc::from("hi"))
        );
    }

    #[test]
    fn applied_change_obeys_ts_sql_null_start_semantics() {
        let row = Arc::new(FxHashMap::from_iter([
            ("c".to_string(), Value::Null),
            ("id".to_string(), Value::Str("r35".into())),
        ]));
        let start = Start {
            row: Arc::new(FxHashMap::from_iter([
                ("c".to_string(), Value::Null),
                ("id".to_string(), Value::Str("r23".into())),
            ])),
            basis: Basis::At,
        };
        let columns = HashMap::from([
            ("c".to_string(), ColumnType::String { optional: false }),
            ("id".to_string(), ColumnType::String { optional: false }),
        ]);
        let order = vec![
            ("c".to_string(), "asc".to_string()),
            ("id".to_string(), "asc".to_string()),
        ];

        // VALUE-aware NULL guard (take-bound divergence fix): a NULL start
        // value now generates the NULL-aware SQL branches even on declared
        // non-optional columns (`(? IS NULL OR c > ?)` — always true for a
        // NULL param), so the overlay matcher must agree and MATCH this row.
        // The old expectation (no match, empty applied changes) mirrored SQL
        // that could never return rows past a NULL bound — the take.rs
        // divergence class (see tests/take_bound_fuzz_test.rs).
        assert!(sql_start_matches(&row, &start, false, &order, &columns));
        assert_eq!(
            applied_changes_for_request(
                &[SourceChange::Add { row }],
                &FetchRequest {
                    start: Some(start),
                    ..Default::default()
                },
                &order,
                &columns,
            )
            .len(),
            1,
            "the overlay must mirror the NULL-aware SQL and surface the prior write",
        );
    }
}

#[cfg(test)]
mod advance_gate_fetch_tests {
    //! Drives the REAL fetch path (`stream_query` → `LazyRowsIter::next`) to
    //! prove the per-fetch economic breaker actually stops a live fetch — the
    //! thing a load run can't reliably force (timeouts need a disproportionately
    //! slow advance, not uniform slowness).
    use super::*;
    use crate::sqlite::query_builder::SqlQuery;
    use std::time::{Duration, Instant};

    fn conn_with_rows(n: i64) -> Rc<RefCell<Connection>> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t(id INTEGER)").unwrap();
        for i in 0..n {
            conn.execute("INSERT INTO t(id) VALUES (?1)", [i]).unwrap();
        }
        Rc::new(RefCell::new(conn))
    }

    fn conn_with_value(value: i64) -> Rc<RefCell<Connection>> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY)")
            .unwrap();
        conn.execute("INSERT INTO t(id) VALUES (?1)", [value])
            .unwrap();
        Rc::new(RefCell::new(conn))
    }

    fn fetch_count(db: Rc<RefCell<Connection>>) -> usize {
        let q = SqlQuery {
            text: "SELECT id FROM t".to_string(),
            params: vec![],
        };
        stream_query(
            db,
            q,
            vec!["id".to_string()],
            HashMap::new(),
            "t".to_string(),
        )
        .count()
    }

    fn past_gate(ms_ago: u64, budget_ms: f64) -> std::sync::Arc<crate::advance_gate::AdvanceGate> {
        let start = Instant::now()
            .checked_sub(Duration::from_millis(ms_ago))
            .unwrap_or_else(Instant::now);
        crate::advance_gate::AdvanceGate::new(start, budget_ms, 4)
    }

    #[test]
    fn fetch_returns_all_rows_when_no_gate_armed() {
        // Hydrate / worker path: no advance gate → every row is produced.
        assert_eq!(fetch_count(conn_with_rows(300)), 300);
    }

    #[test]
    fn fetch_reads_all_columns_and_values() {
        // Regression for the hot-path allocation fix (review #1): next() now
        // BORROWS column_names/columns/table_name instead of cloning them per
        // row. Prove multi-column rows still map correctly across many rows —
        // right keys, and values coerced per the column's declared type.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE u(id INTEGER, name TEXT, flag INTEGER);")
            .unwrap();
        for i in 0..200i64 {
            conn.execute(
                "INSERT INTO u(id, name, flag) VALUES (?1, ?2, ?3)",
                rusqlite::params![i, format!("n{i}"), i % 2],
            )
            .unwrap();
        }
        let db = Rc::new(RefCell::new(conn));
        let columns = HashMap::from([
            ("id".to_string(), ColumnType::Number { optional: false }),
            ("name".to_string(), ColumnType::String { optional: true }),
            ("flag".to_string(), ColumnType::Boolean { optional: false }),
        ]);
        let q = SqlQuery {
            text: "SELECT id, name, flag FROM u ORDER BY id".to_string(),
            params: vec![],
        };
        let rows: Vec<Row> = stream_query(
            db,
            q,
            vec!["id".to_string(), "name".to_string(), "flag".to_string()],
            columns,
            "u".to_string(),
        )
        .collect();

        assert_eq!(rows.len(), 200);
        // Number column coerces to F64, String passes through, Boolean maps 0/1.
        assert_eq!(rows[0].get("id").cloned(), Some(Value::F64(0.0)));
        assert_eq!(rows[0].get("name").cloned(), Some(Value::Str("n0".into())));
        assert_eq!(rows[0].get("flag").cloned(), Some(Value::Bool(false)));
        assert_eq!(rows[199].get("id").cloned(), Some(Value::F64(199.0)));
        assert_eq!(
            rows[199].get("name").cloned(),
            Some(Value::Str("n199".into())),
        );
        assert_eq!(rows[199].get("flag").cloned(), Some(Value::Bool(true)));
    }

    #[test]
    fn existing_input_uses_replacement_snapshot_connection() {
        let first = conn_with_value(1);
        let first_weak = Rc::downgrade(&first);
        let second = conn_with_value(2);
        let columns = HashMap::from([("id".to_string(), ColumnType::Number { optional: false })]);
        let mut source = TableSource::new(first, "t", columns, vec!["id".to_string()]);
        let input = source.connect(None, None, None, None);

        let fetch_id = || {
            let rows: Vec<Row> =
                crate::ivm::stream::skip_yields(input.borrow().fetch(&FetchRequest::default()))
                    .map(|node| node.row)
                    .collect();
            assert_eq!(rows.len(), 1);
            rows[0].get("id").cloned()
        };

        assert_eq!(fetch_id(), Some(Value::F64(1.0)));
        source.set_db(second);
        assert!(
            first_weak.upgrade().is_none(),
            "replacing the snapshot must release the obsolete SQLite connection and its WAL2 read mark",
        );
        assert_eq!(
            fetch_id(),
            Some(Value::F64(2.0)),
            "pipeline inputs must follow TableSource::set_db like TS setDB; retaining the old Rc keeps stale WAL2 read marks alive",
        );
    }

    #[test]
    #[should_panic(expected = "prepare/bind error")]
    fn stream_query_prepare_failure_propagates_not_empty() {
        // Review finding #2: a prepare/bind failure (here a non-existent column,
        // standing in for schema drift / malformed SQL) must PROPAGATE, not be
        // silently converted into an empty result set. Pre-fix this returned
        // std::iter::empty() (count()==0, no panic) → this test fails; post-fix
        // it panics (→ caught upstream → teardown/reset), matching TS.
        let db = conn_with_rows(3);
        let q = SqlQuery {
            text: "SELECT no_such_col FROM t".to_string(),
            params: vec![],
        };
        let _ = stream_query(
            db,
            q,
            vec!["no_such_col".to_string()],
            HashMap::new(),
            "t".to_string(),
        )
        .count();
    }

    #[test]
    #[should_panic(expected = "prepare/bind error")]
    fn stream_query_bind_failure_propagates_not_empty() {
        let db = conn_with_rows(3);
        let q = SqlQuery {
            text: "SELECT id FROM t WHERE id = ?".to_string(),
            params: vec![],
        };
        let _ = stream_query(
            db,
            q,
            vec!["id".to_string()],
            HashMap::new(),
            "t".to_string(),
        )
        .count();
    }

    #[test]
    fn stream_query_busy_propagates_not_empty() {
        let path = std::env::temp_dir().join(format!(
            "rust-ivm-busy-{}-{:?}.db",
            std::process::id(),
            std::thread::current().id()
        ));
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode=DELETE; CREATE TABLE t(id INTEGER); INSERT INTO t VALUES (1);",
            )
            .unwrap();
        let reader = Connection::open(&path).unwrap();
        reader.busy_timeout(Duration::ZERO).unwrap();
        writer.execute_batch("BEGIN EXCLUSIVE").unwrap();

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let q = SqlQuery {
                text: "SELECT id FROM t".to_string(),
                params: vec![],
            };
            let _ = stream_query(
                Rc::new(RefCell::new(reader)),
                q,
                vec!["id".to_string()],
                HashMap::new(),
                "t".to_string(),
            )
            .count();
        }));

        writer.execute_batch("ROLLBACK").unwrap();
        drop(writer);
        let _ = std::fs::remove_file(&path);
        assert!(
            result.is_err(),
            "SQLITE_BUSY must propagate, not return empty"
        );
    }

    #[test]
    #[should_panic(expected = "check_exists prepare error")]
    fn check_exists_failure_propagates_not_false() {
        let db = Rc::new(RefCell::new(Connection::open_in_memory().unwrap()));
        let source = TableSource::new(
            db.clone(),
            "missing_table",
            HashMap::from([("id".to_string(), ColumnType::Number { optional: false })]),
            vec!["id".to_string()],
        );
        let mut values = FxHashMap::default();
        values.insert("id".to_string(), Value::F64(1.0));
        let row = Arc::new(values);
        let conn = db.borrow();
        let _ = source.check_exists(&conn, &row);
    }

    #[test]
    fn fetch_stops_when_gate_over_budget() {
        let db = conn_with_rows(300);
        let gate = past_gate(1000, 1.0); // 1s elapsed, 1ms budget → immediately over
        let _guard = crate::advance_gate::arm(gate.clone());
        let got = fetch_count(db);
        assert!(
            got < 300,
            "expected the breaker to stop the fetch, got {got}/300"
        );
        assert!(
            gate.tripped(),
            "gate must latch tripped after stopping a fetch"
        );
        // _guard drops here → thread-local disarmed
    }

    #[test]
    fn fetch_resumes_all_rows_after_guard_drops() {
        // Prove the RAII guard disarms: a fetch AFTER an over-budget advance
        // (whose guard has dropped) is unaffected — no stale-budget leakage.
        {
            let gate = past_gate(1000, 1.0);
            let _guard = crate::advance_gate::arm(gate);
            let _ = fetch_count(conn_with_rows(100)); // stops early
        }
        assert_eq!(fetch_count(conn_with_rows(300)), 300);
    }

    #[test]
    fn fetch_returns_all_rows_when_gate_under_floor() {
        // Gate armed but only ~5ms elapsed (< 50ms floor) → never trips.
        let db = conn_with_rows(300);
        let gate = past_gate(5, 1.0);
        let _guard = crate::advance_gate::arm(gate.clone());
        assert_eq!(fetch_count(db), 300);
        assert!(!gate.tripped());
    }
}

impl Drop for TableConnection {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::TABLE_CONNECTION);
    }
}

impl Drop for TableSourceInput {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::TABLE_SOURCE_INPUT);
    }
}

impl Drop for TableSource {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::TABLE_SOURCE);
    }
}
