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

use std::collections::{HashMap, HashSet};

use crate::snapshotter::spec::{LiteAndZqlSpec, TableSpec, quote_ident, sorted_keys};
use crate::snapshotter::snapshotter::{
    DiffOwned, InvalidDiffError, ResetPipelinesSignal, SnapshotChange,
    REASON_PERMISSIONS_CHANGE, REASON_SCHEMA_CHANGE, REASON_TRUNCATION,
};
use crate::snapshotter::{SET_OP, DEL_OP, RESET_OP, TRUNCATE_OP, ZERO_VERSION_COLUMN_NAME};

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
    let col_list: Vec<String> = cols.iter().map(|c| {
        let q = quote_ident(c);
        format!("+{} AS {}", q, q)
    }).collect();

    let sql = format!(
        "SELECT {} FROM {} WHERE {}",
        col_list.join(","),
        quote_ident(&spec.name),
        conds.join(" AND ")
    );

    let mut stmt = conn.prepare(&sql).map_err(|e| format!("get_row prepare: {}", e))?;
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
            let val: rusqlite::types::Value = row
                .get(i)
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
    let col_list: Vec<String> = cols.iter().map(|c| {
        let q = quote_ident(c);
        format!("+{} AS {}", q, q)
    }).collect();

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

    let mut stmt = conn.prepare(&sql).map_err(|e| format!("get_rows prepare: {}", e))?;
    let mut rows = stmt
        .query(rusqlite::params_from_iter(binds.iter()))
        .map_err(|e| format!("get_rows query: {}", e))?;

    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|e| format!("get_rows next: {}", e))? {
        let mut map = HashMap::new();
        for (i, col) in cols.iter().enumerate() {
            let val: rusqlite::types::Value = row
                .get(i)
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
    if op == SET_OP {
        if let Some(next) = next_raw {
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
    }
    Ok(())
}

/// Raw scalar comparison for permissions change detection.
fn raw_scalar_equal(a: &rusqlite::types::Value, b: &rusqlite::types::Value) -> bool {
    a == b
}

/// Iterate the diff, calling `emit` for each Change. Stops on first error
/// (including ResetPipelinesSignal). Port of TS Diff[Symbol.iterator] and
/// Go Diff.Each().
pub fn iterate_diff<F>(
    diff: &DiffOwned,
    prev_conn: &std::cell::RefCell<rusqlite::Connection>,
    curr_conn: &std::cell::RefCell<rusqlite::Connection>,
    mut emit: F,
) -> Result<(), DiffError>
where
    F: FnMut(SnapshotChange) -> Result<(), DiffError>,
{
    // NB: borrow the shared snapshot connections only for the duration of each
    // read (get_row/get_rows/read_changelog all return OWNED data), never across
    // `emit(...)`. During emit a TableSource may be pointed at this same
    // connection (set to PREV) and needs to borrow it, so a held borrow here
    // would RefCell-panic.
    let permissions_table = format!("{}.permissions", diff.app_id);
    let entries = read_changelog(&curr_conn.borrow(), &diff.prev_version)?;

    for e in &entries {
        // RESET → schema change, abort.
        if e.op == RESET_OP {
            return Err(DiffError::Reset(ResetPipelinesSignal {
                reason: REASON_SCHEMA_CHANGE,
                msg: format!("schema for table {} has changed", e.table),
            }));
        }
        // TRUNCATE → abort & rehydrate.
        if e.op == TRUNCATE_OP {
            return Err(DiffError::Reset(ResetPipelinesSignal {
                reason: REASON_TRUNCATION,
                msg: format!("table {} has been truncated", e.table),
            }));
        }

        // Non-syncable: skip if known, error if truly unknown.
        let spec = match diff.syncable_tables.get(&e.table) {
            Some(s) => &s.table_spec,
            None => {
                if diff.all_table_names.contains(&e.table) {
                    continue;
                }
                return Err(DiffError::Other(format!(
                    "change for unknown table {}",
                    e.table
                )));
            }
        };

        // Catch-up invariant: every change-log op has stateVersion strictly
        // greater than the table's minRowVersion.
        if !spec.min_row_version.as_deref().unwrap_or("").lt(&e.state_version) {
            // Actually we need lexicographic comparison. In Rust, String comparison
            // IS lexicographic, matching TS's `<`.
            let min = spec.min_row_version.as_deref().unwrap_or("");
            if !(min < e.state_version.as_str()) {
                return Err(DiffError::Other(format!(
                    "unexpected change @{} for table {} with minRowVersion {:?}: {}({})",
                    e.state_version, e.table, spec.min_row_version, e.op, e.row_key
                )));
            }
        }

        let row_key = parse_row_key(&e.row_key)?;

        // nextValue: the new contents for a set, None for a delete.
        let next_raw = if e.op == SET_OP {
            let row = get_row(&curr_conn.borrow(), spec, &row_key)?;
            row
        } else {
            None
        };

        // prevValues: unique-conflicts on a set, or the old row on a delete.
        let prev_values = if let Some(ref next) = next_raw {
            get_rows(&prev_conn.borrow(), spec, &spec.unique_keys, next)?
        } else {
            let pv = get_row(&prev_conn.borrow(), spec, &row_key)?;
            pv.map(|v| vec![v]).unwrap_or_default()
        };

        // A set whose row is missing in curr is a hard inconsistency.
        if e.op == SET_OP && next_raw.is_none() {
            return Err(DiffError::Other(format!(
                "Missing value for {} {}",
                e.table, e.row_key
            )));
        }

        // Stale-diff detection.
        check_valid(
            &e.state_version,
            &e.op,
            &prev_values,
            next_raw.as_ref(),
            &diff.prev_version,
            &diff.curr_version,
        )
        .map_err(DiffError::InvalidDiff)?;

        // No-op filter: delete of a row absent in prev.
        if prev_values.is_empty() && next_raw.is_none() {
            continue;
        }

        // Permissions change → abort & rehydrate.
        if e.table == permissions_table {
            if let Some(ref next) = next_raw {
                for pv in &prev_values {
                    let old_perms = pv.get("permissions");
                    let new_perms = next.get("permissions");
                    if old_perms != new_perms {
                        return Err(DiffError::Reset(ResetPipelinesSignal {
                            reason: REASON_PERMISSIONS_CHANGE,
                            msg: format!(
                                "Permissions have changed {:?} => {:?}",
                                pv.get("hash"),
                                next.get("hash")
                            ),
                        }));
                    }
                }
            }
        }

        let change = SnapshotChange {
            table: e.table.clone(),
            prev_values,
            next_value: next_raw,
            row_key,
        };

        emit(change)?;
    }

    Ok(())
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
