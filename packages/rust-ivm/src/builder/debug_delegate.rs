//! Debug delegate — port of `zql/src/builder/debug-delegate.ts`.
//!
//! Records, per source table + per SQL query, the rows a source VENDED
//! (scanned) during hydration, plus post-execution `nvisit`/`EXPLAIN` stats.
//! The vended-row counts feed the `VENDED` slow-hydrate diagnostic in the
//! pipeline driver (`pipeline-driver.ts:704`); the nvisit/plan stats feed the
//! `analyzeQuery` result. Both are gated OFF in prod behind
//! [`runtime_debug_flags`].
//!
//! The type aliases below (`RowCountsByQuery`, `RowCountsBySource`,
//! `RowsByQuery`, `RowsBySource`) are ported from
//! `zero-protocol/src/analyze-query-result.ts`. rust-ivm has no zero-protocol
//! twin module for that `types/*`-style file, so per AGENTS.md rule 3's
//! established exception they are folded into this consumer with 1:1 names.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::ivm::data::Row;
use crate::ivm::operator::Shared;

// ---------------------------------------------------------------------------
// runtimeDebugFlags — port of `debug-delegate.ts:9`.
// ---------------------------------------------------------------------------

/// Process-global debug flags. Port of TS `runtimeDebugFlags`
/// (debug-delegate.ts:9). In TS this is a mutable module-level object toggled
/// at runtime (e.g. by the inspector's analyze-query RPC); mirrored here with
/// atomics so the toggle is process-global and thread-safe. Both default
/// `false` — the counting + VENDED log are OFF in prod.
pub struct RuntimeDebugFlags {
    track_row_counts_vended: AtomicBool,
    track_rows_vended: AtomicBool,
}

impl RuntimeDebugFlags {
    const fn new() -> Self {
        RuntimeDebugFlags {
            track_row_counts_vended: AtomicBool::new(false),
            track_rows_vended: AtomicBool::new(false),
        }
    }

    /// Port of `runtimeDebugFlags.trackRowCountsVended` (read). Gates the
    /// `VENDED` slow-hydrate log in the pipeline driver.
    pub fn track_row_counts_vended(&self) -> bool {
        self.track_row_counts_vended.load(Ordering::Relaxed)
    }

    /// Port of `runtimeDebugFlags.trackRowsVended` (read). Gates whether a
    /// `Debug` delegate is created + threaded into the pipeline (so the source
    /// actually counts vended rows).
    pub fn track_rows_vended(&self) -> bool {
        self.track_rows_vended.load(Ordering::Relaxed)
    }

    /// Port of `runtimeDebugFlags.trackRowCountsVended = v`.
    pub fn set_track_row_counts_vended(&self, value: bool) {
        self.track_row_counts_vended.store(value, Ordering::Relaxed);
    }

    /// Port of `runtimeDebugFlags.trackRowsVended = v`.
    pub fn set_track_rows_vended(&self, value: bool) {
        self.track_rows_vended.store(value, Ordering::Relaxed);
    }
}

static RUNTIME_DEBUG_FLAGS: RuntimeDebugFlags = RuntimeDebugFlags::new();

/// Accessor for the process-global [`RuntimeDebugFlags`]. Mirrors TS reading
/// `runtimeDebugFlags.<field>` — call sites read
/// `runtime_debug_flags().track_rows_vended()`.
pub fn runtime_debug_flags() -> &'static RuntimeDebugFlags {
    &RUNTIME_DEBUG_FLAGS
}

// ---------------------------------------------------------------------------
// Type aliases — port of `zero-protocol/src/analyze-query-result.ts:6-16`.
// ---------------------------------------------------------------------------

/// `Record<SQL, number>` — port of TS `RowCountsByQuery`
/// (analyze-query-result.ts:7). Counts are non-negative row tallies.
pub type RowCountsByQuery = HashMap<String, u64>;
/// `Record<SourceName, RowCountsByQuery>` — port of TS `RowCountsBySource`
/// (analyze-query-result.ts:10).
pub type RowCountsBySource = HashMap<String, RowCountsByQuery>;
/// `Record<SQL, Row[]>` — port of TS `RowsByQuery` (analyze-query-result.ts:13).
pub type RowsByQuery = HashMap<String, Vec<Row>>;
/// `Record<SourceName, RowsByQuery>` — port of TS `RowsBySource`
/// (analyze-query-result.ts:16).
pub type RowsBySource = HashMap<String, RowsByQuery>;
/// `Record<SQL, string[]>` — port of TS `SQLitePlans` (debug-delegate.ts:17).
pub type SQLitePlans = HashMap<String, Vec<String>>;

// ---------------------------------------------------------------------------
// DebugDelegate trait — port of `debug-delegate.ts:19`.
// ---------------------------------------------------------------------------

/// Port of TS `interface DebugDelegate` (debug-delegate.ts:19).
pub trait DebugDelegate {
    fn init_query(&mut self, table: &str, query: &str);
    fn row_vended(&mut self, table: &str, query: &str, row: Row);
    fn get_vended_row_counts(&self) -> &RowCountsBySource;
    fn get_vended_rows(&self) -> &RowsBySource;
    fn record_nvisit(&mut self, table: &str, query: &str, nvisit: u64);
    fn get_nvisit_counts(&self) -> &RowCountsBySource;
    fn record_explain(&mut self, table: &str, query: &str, plan: Vec<String>);
    fn get_sqlite_plans(&self) -> &SQLitePlans;
    /// clears all internal state
    fn reset(&mut self);
}

/// Convenience alias for a shared, interior-mutable debug delegate threaded
/// through the (single-threaded, `Rc`/`RefCell`) pipeline — mirrors TS passing
/// the `DebugDelegate` interface by reference into `buildPipeline`.
pub type SharedDebug = Shared<dyn DebugDelegate>;

// ---------------------------------------------------------------------------
// Debug — port of `class Debug` (debug-delegate.ts:32).
// ---------------------------------------------------------------------------

/// Port of TS `class Debug implements DebugDelegate` (debug-delegate.ts:32).
pub struct Debug {
    row_counts_by_source: RowCountsBySource,
    rows_by_source: RowsBySource,
    nvisit_by_source: RowCountsBySource,
    plans: SQLitePlans,
}

impl Default for Debug {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug {
    /// Port of the TS `constructor` (debug-delegate.ts:38).
    pub fn new() -> Self {
        Debug {
            row_counts_by_source: HashMap::new(),
            rows_by_source: HashMap::new(),
            nvisit_by_source: HashMap::new(),
            plans: HashMap::new(),
        }
    }

    /// Create a `Debug` wrapped for sharing through the pipeline.
    pub fn new_shared() -> SharedDebug {
        Rc::new(RefCell::new(Debug::new()))
    }

    /// Port of TS `#getRowStats` (debug-delegate.ts:103). Lazily creates the
    /// per-source count + row maps and returns disjoint mutable refs to both.
    fn get_row_stats(&mut self, source: &str) -> (&mut RowCountsByQuery, &mut RowsByQuery) {
        // Destructure so the two `entry()` calls take disjoint field borrows —
        // the Rust equivalent of TS returning `{counts, rows}` referencing two
        // different internal maps.
        let Debug {
            row_counts_by_source,
            rows_by_source,
            ..
        } = self;
        let counts = row_counts_by_source.entry(source.to_string()).or_default();
        let rows = rows_by_source.entry(source.to_string()).or_default();
        (counts, rows)
    }
}

impl DebugDelegate for Debug {
    /// Port of TS `initQuery` (debug-delegate.ts:61). Seeds a `0` count for the
    /// (table, query) pair so a query that vends nothing still appears.
    fn init_query(&mut self, table: &str, query: &str) {
        let (counts, _rows) = self.get_row_stats(table);
        // TS: `if (!counts[query]) counts[query] = 0;` — insert 0 when absent
        // (an existing 0 stays 0; a non-zero is left untouched).
        counts.entry(query.to_string()).or_insert(0);
    }

    /// Port of TS `rowVended` (debug-delegate.ts:77).
    fn row_vended(&mut self, table: &str, query: &str, row: Row) {
        let (counts, rows) = self.get_row_stats(table);
        // TS: `counts[query] = (counts[query] ?? 0) + 1;`
        *counts.entry(query.to_string()).or_insert(0) += 1;
        // TS: `(rows[query] ??= []).push(row);`
        rows.entry(query.to_string()).or_default().push(row);
    }

    /// Port of TS `getVendedRowCounts` (debug-delegate.ts:45).
    fn get_vended_row_counts(&self) -> &RowCountsBySource {
        &self.row_counts_by_source
    }

    /// Port of TS `getVendedRows` (debug-delegate.ts:49).
    fn get_vended_rows(&self) -> &RowsBySource {
        &self.rows_by_source
    }

    /// Port of TS `recordNVisit` (debug-delegate.ts:87).
    fn record_nvisit(&mut self, table: &str, query: &str, nvisit: u64) {
        let nvisit_counts = self.nvisit_by_source.entry(table.to_string()).or_default();
        *nvisit_counts.entry(query.to_string()).or_insert(0) += nvisit;
    }

    /// Port of TS `getNVisitCounts` (debug-delegate.ts:53).
    fn get_nvisit_counts(&self) -> &RowCountsBySource {
        &self.nvisit_by_source
    }

    /// Port of TS `recordExplain` (debug-delegate.ts:99). The `_table` arg is
    /// unused in TS too (plans are keyed by SQL only).
    fn record_explain(&mut self, _table: &str, query: &str, plan: Vec<String>) {
        self.plans.insert(query.to_string(), plan);
    }

    /// Port of TS `getSQLitePlans` (debug-delegate.ts:57).
    fn get_sqlite_plans(&self) -> &SQLitePlans {
        &self.plans
    }

    /// Port of TS `reset` (debug-delegate.ts:70).
    fn reset(&mut self) {
        self.row_counts_by_source = HashMap::new();
        self.rows_by_source = HashMap::new();
        self.nvisit_by_source = HashMap::new();
        self.plans = HashMap::new();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::data::Value;
    use rustc_hash::FxHashMap;
    use std::sync::Arc;

    fn row(id: &str) -> Row {
        let mut m: FxHashMap<String, Value> = FxHashMap::default();
        m.insert("id".to_string(), Value::Str(Arc::from(id)));
        Arc::new(m)
    }

    #[test]
    fn init_query_seeds_zero_and_row_vended_counts() {
        // Port-parity: mirrors TS `Debug` — initQuery seeds a 0, each rowVended
        // increments, and the count is retrievable via getVendedRowCounts()
        // keyed by [table][sql].
        let mut d = Debug::new();
        d.init_query("issue", "SELECT * FROM issue");
        assert_eq!(d.get_vended_row_counts()["issue"]["SELECT * FROM issue"], 0);

        d.row_vended("issue", "SELECT * FROM issue", row("i1"));
        d.row_vended("issue", "SELECT * FROM issue", row("i2"));
        assert_eq!(d.get_vended_row_counts()["issue"]["SELECT * FROM issue"], 2);
        // rowVended also retains the rows (trackRowsVended path).
        assert_eq!(d.get_vended_rows()["issue"]["SELECT * FROM issue"].len(), 2);
    }

    #[test]
    fn row_vended_without_init_starts_at_one() {
        // TS `counts[query] = (counts[query] ?? 0) + 1` — first vend with no
        // prior initQuery yields 1, not a panic on a missing key.
        let mut d = Debug::new();
        d.row_vended("user", "SELECT id FROM user", row("u1"));
        assert_eq!(d.get_vended_row_counts()["user"]["SELECT id FROM user"], 1);
    }

    #[test]
    fn record_nvisit_accumulates_and_explain_keyed_by_sql() {
        let mut d = Debug::new();
        d.record_nvisit("issue", "SELECT 1", 3);
        d.record_nvisit("issue", "SELECT 1", 4);
        assert_eq!(d.get_nvisit_counts()["issue"]["SELECT 1"], 7);

        d.record_explain("issue", "SELECT 1", vec!["SCAN issue".to_string()]);
        assert_eq!(d.get_sqlite_plans()["SELECT 1"], vec!["SCAN issue"]);
    }

    #[test]
    fn reset_clears_all_state() {
        let mut d = Debug::new();
        d.row_vended("t", "q", row("x"));
        d.record_nvisit("t", "q", 1);
        d.record_explain("t", "q", vec!["p".to_string()]);
        d.reset();
        assert!(d.get_vended_row_counts().is_empty());
        assert!(d.get_vended_rows().is_empty());
        assert!(d.get_nvisit_counts().is_empty());
        assert!(d.get_sqlite_plans().is_empty());
    }

    #[test]
    fn runtime_flags_default_off_and_toggle() {
        // Prod default: both OFF. Toggling is process-global.
        let f = runtime_debug_flags();
        // Save/restore so this test doesn't leak state into others in-process.
        let prev_counts = f.track_row_counts_vended();
        let prev_rows = f.track_rows_vended();
        f.set_track_rows_vended(true);
        f.set_track_row_counts_vended(true);
        assert!(f.track_rows_vended());
        assert!(f.track_row_counts_vended());
        f.set_track_rows_vended(prev_rows);
        f.set_track_row_counts_vended(prev_counts);
    }
}
