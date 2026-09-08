//! Table spec for snapshotter diff resolution.
//! Port of TS `LiteAndZqlSpec` + `LiteTableSpecWithKeysAndVersion` and
//! Go `TableSpec` (spec.go).

use std::collections::HashMap;

/// Column schema — type + optional flag.
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub r#type: String,
    pub optional: bool,
}

/// Table spec — the subset of TS's LiteTableSpecWithKeysAndVersion that the
/// Diff needs. Mirrors Go's `TableSpec` (spec.go).
#[derive(Debug, Clone)]
pub struct TableSpec {
    pub name: String,
    pub columns: HashMap<String, ColumnSchema>,
    pub unique_keys: Vec<Vec<String>>,
    pub min_row_version: Option<String>,
}

impl TableSpec {
    /// Return column names in stable (sorted) order — deterministic SELECT list.
    pub fn cols(&self) -> Vec<String> {
        let mut cols: Vec<String> = self.columns.keys().cloned().collect();
        cols.sort();
        cols
    }
}

/// LiteAndZqlSpec — port of TS's LiteAndZqlSpec (specs.ts).
/// Contains the table spec and the zql column specs.
#[derive(Debug, Clone)]
pub struct LiteAndZqlSpec {
    pub table_spec: TableSpec,
    pub zql_spec: HashMap<String, ColumnSchema>,
}

/// Double-quote a SQLite identifier, escaping embedded quotes.
/// Port of Go `quoteIdent` (spec.go:68).
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Return the keys of m in ascending order — TS's `normalizedKeyOrder`.
pub fn sorted_keys(m: &HashMap<String, rusqlite::types::Value>) -> Vec<String> {
    let mut keys: Vec<String> = m.keys().cloned().collect();
    keys.sort();
    keys
}
