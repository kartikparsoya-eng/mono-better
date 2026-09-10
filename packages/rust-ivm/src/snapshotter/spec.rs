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
    let mut out = String::with_capacity(name.len() + 2);
    push_quoted_ident(&mut out, name);
    out
}

/// Append `name` to `out`, SQL-quoted. Zero-allocation twin of
/// [`quote_ident`], and its single implementation — `quote_ident` is now
/// defined in terms of this, so the two cannot drift and the emitted bytes are
/// identical by construction.
///
/// Rust-only (AGENTS.md rule 5): TS builds identifiers with `sql.ident(c)` into
/// a template that appends to an existing string, so it never materialises a
/// per-identifier String. `quote_ident` allocated TWO per call (the `replace`
/// plus the `format!`), which is per-column per-fetch on the query-builder
/// path — and a correlated EXISTS issues one fetch per PARENT ROW, so per-fetch
/// cost is effectively per-row.
pub fn push_quoted_ident(out: &mut String, name: &str) {
    out.push('"');
    for ch in name.chars() {
        // TS `sql.ident` doubles an embedded quote; so does
        // `name.replace('"', "\"\"")`, which this replaces.
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
}

/// Return the keys of m in ascending order — TS's `normalizedKeyOrder`.
pub fn sorted_keys(m: &HashMap<String, rusqlite::types::Value>) -> Vec<String> {
    let mut keys: Vec<String> = m.keys().cloned().collect();
    keys.sort();
    keys
}
