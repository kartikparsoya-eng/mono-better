//! Diff — lazy iteration over `_zero.changeLog2` between two snapshot versions.
//!
//! Port of TS `Diff` class (snapshotter.ts:398) and Go `Diff.Each()` (diff.go).
//!
//! The diff reads the changelog in (prevVersion, curr] ordered by
//! (stateVersion ASC, pos ASC). For each entry:
//!   - RESET op → ResetPipelinesSignal('schema-change')
//!   - TRUNCATE op → ResetPipelinesSignal('truncation')
//!   - SET op → read the row from curr snapshot, find unique-key conflicts in prev
//!   - DEL op → read the row from prev snapshot
//!
//! The caller provides two `&rusqlite::Connection` references (prev and curr
/// snapshots). The diff is only valid until the next `advance()`.
use std::collections::HashMap;

use crate::snapshotter::snapshotter::{
    DiffOwned, InvalidDiffError, REASON_PERMISSIONS_CHANGE, REASON_SCHEMA_CHANGE,
    REASON_TRUNCATION, ResetPipelinesSignal, SnapshotChange,
};
use crate::snapshotter::spec::{TableSpec, quote_ident, sorted_keys};
use crate::snapshotter::{RESET_OP, SET_OP, TRUNCATE_OP, ZERO_VERSION_COLUMN_NAME};

/// Change-log entry row.
struct ChangeLogEntry {
    state_version: String,
    table: String,
    row_key: String, // JSON text
    op: String,
}

/// Read the change-log entries between prev_version and head.
fn read_changelog(
    conn: &rusqlite::Connection,
    prev_version: &str,
) -> Result<Vec<ChangeLogEntry>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT \"stateVersion\", \"table\", \"rowKey\", \"op\" \
             FROM \"_zero.changeLog2\" \
             WHERE \"stateVersion\" > ? \
             ORDER BY \"stateVersion\" ASC, \"pos\" ASC",
        )
        .map_err(|e| format!("read_changelog prepare: {}", e))?;

    let entries = stmt
        .query_map([prev_version], |row| {
            Ok(ChangeLogEntry {
                state_version: row.get::<_, String>(0)?,
                table: row.get::<_, String>(1)?,
                row_key: row.get::<_, String>(2)?,
                op: row.get::<_, String>(3)?,
            })
        })
        .map_err(|e| format!("read_changelog query: {}", e))?;

    let mut out = Vec::new();
    for entry in entries {
        let e = entry.map_err(|e| format!("read_changelog row: {}", e))?;
        out.push(e);
    }
    Ok(out)
}

/// Parse a JSON rowKey string into a map of column→Value.
fn parse_row_key(json_text: &str) -> Result<HashMap<String, rusqlite::types::Value>, String> {
    // Lightweight JSON parse — we only need string/number/null values.
    // rusqlite::types::Value maps naturally from JSON types.
    let v: serde_json::Value =
        serde_json::from_str(json_text).map_err(|e| format!("parse_rowKey: {}", e))?;
    let obj = v
        .as_object()
        .ok_or_else(|| format!("rowKey is not an object: {}", json_text))?;
    let mut map = HashMap::new();
    for (k, val) in obj {
        map.insert(k.clone(), json_to_sqlite_value(val));
    }
    Ok(map)
}

fn json_to_sqlite_value(v: &serde_json::Value) -> rusqlite::types::Value {
    match v {
        serde_json::Value::Null => rusqlite::types::Value::Null,
        serde_json::Value::Bool(b) => rusqlite::types::Value::Integer(*b as i64),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rusqlite::types::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                rusqlite::types::Value::Real(f)
            } else {
                rusqlite::types::Value::Text(n.to_string())
            }
        }
        serde_json::Value::String(s) => rusqlite::types::Value::Text(s.clone()),
        other => rusqlite::types::Value::Text(other.to_string()),
    }
}

/// Read a single row from a snapshot connection, keyed by rowKey columns.
fn get_row(
    conn: &rusqlite::Connection,
    spec: &TableSpec,
    row_key: &HashMap<String, rusqlite::types::Value>,
) -> Result<Option<HashMap<String, rusqlite::types::Value>>, String> {
    let key_cols = sorted_keys(row_key);
    let conds: Vec<String> = key_cols
        .iter()
        .map(|c| format!("{}=?", quote_ident(c)))
        .collect();
    let cols = spec.cols();
    let col_list: Vec<String> = cols
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("+{} AS {}", q, q)
        })
        .collect();

    let sql = format!(
        "SELECT {} FROM {} WHERE {}",
        col_list.join(","),
        quote_ident(&spec.name),
        conds.join(" AND ")
    );

    // Cached: one SELECT shape per table, executed once per change-log entry —
    // the advance hot path the advancement-timeout budget measures.
    let mut stmt = conn
        .prepare_cached(&sql)
        .map_err(|e| format!("get_row prepare: {}", e))?;
    let params: Vec<&dyn rusqlite::ToSql> = key_cols
        .iter()
        .map(|c| row_key.get(c).unwrap() as &dyn rusqlite::ToSql)
        .collect();

    let mut rows = stmt
        .query(rusqlite::params_from_iter(params.iter()))
        .map_err(|e| format!("get_row query: {}", e))?;

    if let Some(row) = rows.next().map_err(|e| format!("get_row next: {}", e))? {
        let mut result = HashMap::new();
        for (i, col) in cols.iter().enumerate() {
            let val: rusqlite::types::Value = crate::sqlite::db::read_value_lossy(row, i)
                .map_err(|e| format!("get_row get {}: {}", col, e))?;
            result.insert(col.clone(), val);
        }
        Ok(Some(result))
    } else {
        Ok(None)
    }
}

/// Read all rows from prev that conflict on ANY unique key with the given row.
fn get_rows(
    conn: &rusqlite::Connection,
    spec: &TableSpec,
    unique_keys: &[Vec<String>],
    row: &HashMap<String, rusqlite::types::Value>,
) -> Result<Vec<HashMap<String, rusqlite::types::Value>>, String> {
    let valid_keys: Vec<&Vec<String>> = unique_keys
        .iter()
        .filter(|key| {
            key.iter().all(|c| {
                row.get(c)
                    .map(|v| v != &rusqlite::types::Value::Null)
                    .unwrap_or(false)
            })
        })
        .collect();

    if valid_keys.is_empty() {
        return Ok(Vec::new());
    }

    let or_conds: Vec<String> = valid_keys
        .iter()
        .map(|key| {
            let and_conds: Vec<String> = key
                .iter()
                .map(|c| format!("{}=?", quote_ident(c)))
                .collect();
            format!("({})", and_conds.join(" AND "))
        })
        .collect();

    let cols = spec.cols();
    let col_list: Vec<String> = cols
        .iter()
        .map(|c| {
            let q = quote_ident(c);
            format!("+{} AS {}", q, q)
        })
        .collect();

    let sql = format!(
        "SELECT {} FROM {} WHERE {}",
        col_list.join(","),
        quote_ident(&spec.name),
        or_conds.join(" OR ")
    );

    let mut binds: Vec<&dyn rusqlite::ToSql> = Vec::new();
    for key in &valid_keys {
        for c in *key {
            binds.push(row.get(c).unwrap() as &dyn rusqlite::ToSql);
        }
    }

    // Cached: same rationale as get_row above.
    let mut stmt = conn
        .prepare_cached(&sql)
        .map_err(|e| format!("get_rows prepare: {}", e))?;
    let mut rows = stmt
        .query(rusqlite::params_from_iter(binds.iter()))
        .map_err(|e| format!("get_rows query: {}", e))?;

    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("get_rows next: {}", e))? {
        let mut map = HashMap::new();
        for (i, col) in cols.iter().enumerate() {
            let val: rusqlite::types::Value = crate::sqlite::db::read_value_lossy(row, i)
                .map_err(|e| format!("get_rows get {}: {}", col, e))?;
            map.insert(col.clone(), val);
        }
        result.push(map);
    }
    Ok(result)
}

/// Stale-diff version check — detects diff consumed after snapshots advanced.
fn check_valid(
    state_version: &str,
    op: &str,
    prev_values: &[HashMap<String, rusqlite::types::Value>],
    next_raw: Option<&HashMap<String, rusqlite::types::Value>>,
    prev_version: &str,
    curr_version: &str,
) -> Result<(), InvalidDiffError> {
    if state_version > curr_version {
        return Err(InvalidDiffError {
            msg: format!(
                "Diff is no longer valid. curr db has advanced past {}",
                curr_version
            ),
        });
    }
    for pv in prev_values {
        let ver = pv
            .get(ZERO_VERSION_COLUMN_NAME)
            .map(|v| match v {
                rusqlite::types::Value::Text(s) => s.as_str(),
                _ => "~",
            })
            .unwrap_or("~");
        if ver > prev_version {
            return Err(InvalidDiffError {
                msg: format!(
                    "Diff is no longer valid. prev db has advanced past {}.",
                    prev_version
                ),
            });
        }
    }
    if op == SET_OP
        && let Some(next) = next_raw
    {
        let ver = next
            .get(ZERO_VERSION_COLUMN_NAME)
            .map(|v| match v {
                rusqlite::types::Value::Text(s) => s.as_str(),
                _ => "",
            })
            .unwrap_or("");
        if ver != state_version {
            return Err(InvalidDiffError {
                msg: "Diff is no longer valid. curr db has advanced.".to_string(),
            });
        }
    }
    Ok(())
}

/// Iterate the diff, calling `emit` for each `SnapshotChange` — the eager
/// composition of [`DiffIter`]. Kept for callers that need no yielding.
pub fn iterate_diff<F>(
    diff: &DiffOwned,
    prev_conn: &std::cell::RefCell<rusqlite::Connection>,
    curr_conn: &std::cell::RefCell<rusqlite::Connection>,
    mut emit: F,
) -> Result<(), DiffError>
where
    F: FnMut(SnapshotChange) -> Result<(), DiffError>,
{
    // The two connections are shared `Rc`s in production; this legacy entry
    // point only has `&RefCell`s, so it borrows through a non-owning iterator.
    let entries = {
        let _t = crate::perf_trace::scope("advance.diff");
        read_changelog(&curr_conn.borrow(), &diff.prev_version)?
    };
    let mut it = DiffIter {
        diff: DiffRef::Borrowed(diff),
        prev_conn: ConnRef::Borrowed(prev_conn),
        curr_conn: ConnRef::Borrowed(curr_conn),
        permissions_table: format!("{}.permissions", diff.app_id),
        entries: entries.into_iter(),
        done: false,
    };
    for change in it.by_ref() {
        emit(change?)?;
    }
    Ok(())
}

enum DiffRef<'a> {
    Borrowed(&'a DiffOwned),
    Owned(std::rc::Rc<DiffOwned>),
}

impl std::ops::Deref for DiffRef<'_> {
    type Target = DiffOwned;
    fn deref(&self) -> &DiffOwned {
        match self {
            DiffRef::Borrowed(d) => d,
            DiffRef::Owned(d) => d,
        }
    }
}

enum ConnRef<'a> {
    Borrowed(&'a std::cell::RefCell<rusqlite::Connection>),
    Owned(std::rc::Rc<std::cell::RefCell<rusqlite::Connection>>),
}

impl ConnRef<'_> {
    fn borrow(&self) -> std::cell::Ref<'_, rusqlite::Connection> {
        match self {
            ConnRef::Borrowed(c) => c.borrow(),
            ConnRef::Owned(c) => c.borrow(),
        }
    }
}

/// Pull-based diff iteration: the per-entry work of `iterate_diff`, one
/// `SnapshotChange` per `next()`, so the engine's advance can suspend between
/// changes — TS `#advance` is a generator over `diff` (pipeline-driver.ts:
/// 948-1000) and yields `'yield'` before a change when the time slice is up
/// (:975-977). Terminal errors (reset signals, invalid diff, hard errors) are
/// surfaced once as `Err` and the iterator is then exhausted.
///
/// NB: borrows the shared snapshot connections only for the duration of each
/// read (`get_row`/`get_rows`/`read_changelog` all return OWNED data), never
/// across the caller's processing of a change. During that processing a
/// TableSource may be pointed at this same connection (set to PREV) and needs
/// to borrow it, so a held borrow here would RefCell-panic.
pub struct DiffIter<'a> {
    diff: DiffRef<'a>,
    prev_conn: ConnRef<'a>,
    curr_conn: ConnRef<'a>,
    permissions_table: String,
    entries: std::vec::IntoIter<ChangeLogEntry>,
    done: bool,
}

impl DiffIter<'static> {
    /// Open the iterator over `diff`'s changelog entries (read up front, as
    /// `iterate_diff` does), owning its inputs so it can live inside an
    /// engine-level advance stream held across an `.await`.
    pub fn new(
        diff: std::rc::Rc<DiffOwned>,
        prev_conn: std::rc::Rc<std::cell::RefCell<rusqlite::Connection>>,
        curr_conn: std::rc::Rc<std::cell::RefCell<rusqlite::Connection>>,
    ) -> Result<Self, DiffError> {
        let entries = {
            let _t = crate::perf_trace::scope("advance.diff");
            read_changelog(&curr_conn.borrow(), &diff.prev_version)?
        };
        let permissions_table = format!("{}.permissions", diff.app_id);
        Ok(DiffIter {
            diff: DiffRef::Owned(diff),
            prev_conn: ConnRef::Owned(prev_conn),
            curr_conn: ConnRef::Owned(curr_conn),
            permissions_table,
            entries: entries.into_iter(),
            done: false,
        })
    }
}

impl Iterator for DiffIter<'_> {
    type Item = Result<SnapshotChange, DiffError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            let Some(e) = self.entries.next() else {
                self.done = true;
                return None;
            };
            let e = &e;
            let item: Option<Result<SnapshotChange, DiffError>> = (|| {
                // Times the per-entry SnapshotChange read (prev/curr row lookups etc.);
                // dropped just before `emit` so the push/deliver work is excluded.
                let diff_scope = crate::perf_trace::scope("advance.diff");
                // RESET → schema change, abort.
                if e.op == RESET_OP {
                    return Some(Err(DiffError::Reset(ResetPipelinesSignal {
                        reason: REASON_SCHEMA_CHANGE,
                        msg: format!("schema for table {} has changed", e.table),
                    })));
                }
                // TRUNCATE → abort & rehydrate.
                if e.op == TRUNCATE_OP {
                    return Some(Err(DiffError::Reset(ResetPipelinesSignal {
                        reason: REASON_TRUNCATION,
                        msg: format!("table {} has been truncated", e.table),
                    })));
                }

                // Non-syncable: skip if known, error if truly unknown.
                let spec = match self.diff.syncable_tables.get(&e.table) {
                    Some(s) => &s.table_spec,
                    None => {
                        if self.diff.all_table_names.contains(&e.table) {
                            return None;
                        }
                        return Some(Err(DiffError::Other(format!(
                            "change for unknown table {}",
                            e.table
                        ))));
                    }
                };

                // Catch-up invariant: every change-log op has stateVersion strictly
                // greater than the table's minRowVersion.
                let min = spec.min_row_version.as_deref().unwrap_or("");
                if min >= e.state_version.as_str() {
                    return Some(Err(DiffError::Other(format!(
                        "unexpected change @{} for table {} with minRowVersion {:?}: {}({})",
                        e.state_version, e.table, spec.min_row_version, e.op, e.row_key
                    ))));
                }

                let row_key = match parse_row_key(&e.row_key) {
                    Ok(k) => k,
                    Err(err) => return Some(Err(err.into())),
                };

                // nextValue: the new contents for a set, None for a delete.
                let next_raw = if e.op == SET_OP {
                    match get_row(&self.curr_conn.borrow(), spec, &row_key) {
                        Ok(v) => v,
                        Err(err) => return Some(Err(err.into())),
                    }
                } else {
                    None
                };

                // prevValues: unique-conflicts on a set, or the old row on a delete.
                let prev_values = if let Some(ref next) = next_raw {
                    match get_rows(&self.prev_conn.borrow(), spec, &spec.unique_keys, next) {
                        Ok(v) => v,
                        Err(err) => return Some(Err(err.into())),
                    }
                } else {
                    match get_row(&self.prev_conn.borrow(), spec, &row_key) {
                        Ok(pv) => pv.map(|v| vec![v]).unwrap_or_default(),
                        Err(err) => return Some(Err(err.into())),
                    }
                };

                // A set whose row is missing in curr is a hard inconsistency.
                if e.op == SET_OP && next_raw.is_none() {
                    return Some(Err(DiffError::Other(format!(
                        "Missing value for {} {}",
                        e.table, e.row_key
                    ))));
                }

                // Match TS exactly: consuming a diff after either snapshot has moved is
                // an InvalidDiffError, not a recoverable pipeline-reset signal.
                if let Err(err) = check_valid(
                    &e.state_version,
                    &e.op,
                    &prev_values,
                    next_raw.as_ref(),
                    &self.diff.prev_version,
                    &self.diff.curr_version,
                ) {
                    return Some(Err(DiffError::InvalidDiff(err)));
                }

                // No-op filter: delete of a row absent in prev.
                if prev_values.is_empty() && next_raw.is_none() {
                    return None;
                }

                // Permissions change → abort & rehydrate.
                if e.table == self.permissions_table
                    && let Some(ref next) = next_raw
                {
                    for pv in &prev_values {
                        let old_perms = pv.get("permissions");
                        let new_perms = next.get("permissions");
                        if old_perms != new_perms {
                            return Some(Err(DiffError::Reset(ResetPipelinesSignal {
                                reason: REASON_PERMISSIONS_CHANGE,
                                msg: format!(
                                    "Permissions have changed {:?} => {:?}",
                                    pv.get("hash"),
                                    next.get("hash")
                                ),
                            })));
                        }
                    }
                }

                let change = SnapshotChange {
                    table: e.table.clone(),
                    prev_values,
                    next_value: next_raw,
                    row_key,
                };
                drop(diff_scope);
                Some(Ok(change))
            })();
            match item {
                // `continue` inside the closure yields `None` for a skipped entry.
                None => continue,
                Some(Err(err)) => {
                    self.done = true;
                    return Some(Err(err));
                }
                Some(Ok(change)) => return Some(Ok(change)),
            }
        }
    }
}

/// Error type for diff iteration.
#[derive(Debug)]
pub enum DiffError {
    Reset(ResetPipelinesSignal),
    InvalidDiff(InvalidDiffError),
    Other(String),
}

impl std::fmt::Display for DiffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiffError::Reset(r) => write!(f, "{}", r),
            DiffError::InvalidDiff(e) => write!(f, "{}", e),
            DiffError::Other(s) => write!(f, "{}", s),
        }
    }
}

impl From<String> for DiffError {
    fn from(s: String) -> Self {
        DiffError::Other(s)
    }
}

impl std::error::Error for DiffError {}

// Keep the old Diff type as a re-export for backward compat with existing code.
// The new code uses DiffOwned + iterate_diff.
pub use crate::snapshotter::snapshotter::DiffOwned as Diff;
