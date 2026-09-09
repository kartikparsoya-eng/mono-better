//! SQLite integration — port of `zqlite/src/`.
//!
//! TableSource: the production source backed by SQLite.
//! query-builder: compiles FetchRequest → SQL.
//! db: SQLite connection wrapper.
//! database-storage: persistent storage for Take/Cap.
//! resolve-scalar-subqueries: scalar subquery resolution.
//! sqlite-cost-model: query planning cost model.
//! sqlite-stat-fanout: join fanout estimation from SQLite stats.
//! explain-queries: EXPLAIN QUERY PLAN utilities.
//! internal/statement-cache: prepared-statement LRU (TS `#stmts.cache`).
//! query-delegate: ZQLite QueryDelegate implementation.

pub mod database_storage;
pub mod db;
pub mod explain_queries;
pub mod internal;
pub mod interrupt;
pub mod options;
pub mod query_builder;
pub mod query_delegate;
pub mod resolve_scalar_subqueries;
pub mod sqlite_cost_model;
pub mod sqlite_stat_fanout;
pub mod table_source; // cross-thread SQLite interrupt + job-scoped watchdog (N1/N2)

pub use database_storage::*;
pub use db::*;
pub use explain_queries::*;
pub use interrupt::{JobWatchdog, WatchGuard, install_interrupt};
pub use options::*;
pub use query_builder::*;
pub use query_delegate::*;
pub use resolve_scalar_subqueries::*;
pub use sqlite_cost_model::*;
pub use sqlite_stat_fanout::*;
pub use table_source::*;

/// Page-cache budget for the connections rust opens PER CLIENT GROUP and PER
/// TABLE SOURCE, in KiB (`PRAGMA cache_size = -2000`).
///
/// Rust-only (AGENTS.md rule 5/10, INVENTIONS.md I-19). The vendored SQLite is
/// compiled with zero-sqlite3's `SQLITE_DEFAULT_CACHE_SIZE=-16000` (16 MiB per
/// connection, 16325e611) because that define is part of the planner-stats
/// parity. TS opens two better-sqlite3 Databases per view-syncer (the
/// snapshotter's curr/prev) and its TableSources share them; rust opens a
/// connection per TableSource (one per replicated table, ~136 on the sandbox)
/// plus the snapshot pair, so the same per-connection default multiplies by
/// ~70x per client group. Measured 2026-09-09 (full-catalog differential
/// oracle, 24g cgroup cap, hog query excluded): `095af74e6` (2 MiB compiled
/// default) PASS; `16325e611` and every later image OOM-killed at 24.6-24.9 GB
/// anon RSS. This restores the 2 MiB per serving connection that every image
/// before 16325e611 ran with; the page cache is not client-observable.
pub const SERVING_CONNECTION_CACHE_SIZE_KIB: i64 = 2000;

/// Apply [`SERVING_CONNECTION_CACHE_SIZE_KIB`] to a serving connection.
pub fn apply_serving_page_cache(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    conn.pragma_update(None, "cache_size", -SERVING_CONNECTION_CACHE_SIZE_KIB)
}

#[cfg(test)]
mod page_cache_budget_tests {
    /// The compiled default IS zero-sqlite3's 16 MiB (the define parity holds)
    /// and the serving budget overrides it to 2 MiB — the value every image
    /// before 16325e611 ran with, which passed the 24g differential where the
    /// 16 MiB-per-connection images were OOM-killed. Non-vacuous: drop the
    /// pragma from `apply_serving_page_cache` and the second assertion reads
    /// -16000.
    #[test]
    fn serving_connections_get_a_2mib_page_cache_over_the_16mib_compiled_default() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let compiled: i64 = conn
            .pragma_query_value(None, "cache_size", |r| r.get(0))
            .unwrap();
        assert_eq!(
            compiled, -16000,
            "SQLITE_DEFAULT_CACHE_SIZE parity with zero-sqlite3"
        );
        super::apply_serving_page_cache(&conn).unwrap();
        let budget: i64 = conn
            .pragma_query_value(None, "cache_size", |r| r.get(0))
            .unwrap();
        assert_eq!(
            budget, -2000,
            "per-source / per-snapshot connections carry the 2 MiB budget"
        );
    }
}
