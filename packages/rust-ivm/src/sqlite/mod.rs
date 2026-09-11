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

/// Page cache for every serving SQLite connection, in KiB
/// (`PRAGMA cache_size = -16000`, 16 MiB) — TS parity.
///
/// TS sets no pragma: `new Snapshotter(logger, replicaFile, shard)`
/// (zero-cache/src/server/syncer.ts:225) passes no `pageCacheSizeKib`, so
/// `Snapshot.create` (snapshotter.ts:284) skips it and every serving
/// connection runs zero-sqlite3's compiled `SQLITE_DEFAULT_CACHE_SIZE=-16000`.
/// The vendored SQLite carries the same define (16325e611, rust-syncer
/// build.rs), so this constant is an explicit PIN of TS's effective value
/// against define drift, not a budget.
///
/// History (INVENTIONS.md I-19): from 0a0d456d9 to 92be04854 this was 2000
/// (2 MiB) on the belief that rust opened one connection per TableSource
/// (~136 per client group). That described `MemorySource::set_db_path`, the
/// no-snapshotter TEST fallback — production `TableSource`s share the
/// Snapshotter's two pinned connections exactly like TS (`pipeline_driver.rs`
/// `build_engine`; measured: 49 `replica.db` fds for 12 client
/// groups). So the 24g OOM behind I-19 was 16 MiB × 2 × N client groups —
/// the footprint TS carries too — and the 2 MiB budget cut each rust client
/// group's page cache to 1/8 of TS's (sandbox pod: a 60 s cold
/// hydrate at 397 µs per sql_step vs a 49 µs median).
pub const SERVING_CONNECTION_CACHE_SIZE_KIB: i64 = 16000;

/// Apply [`SERVING_CONNECTION_CACHE_SIZE_KIB`] to a serving connection.
pub fn apply_serving_page_cache(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    conn.pragma_update(None, "cache_size", -SERVING_CONNECTION_CACHE_SIZE_KIB)
}

#[cfg(test)]
mod page_cache_budget_tests {
    /// Every serving connection carries TS's effective page cache — the
    /// zero-sqlite3 compiled default, 16 MiB — regardless of what the
    /// connection started with. Mutation test: the sentinel is set FIRST, so
    /// dropping the pragma from `apply_serving_page_cache` leaves -500 and the
    /// assertion fails under both links (wal2 static lib in CI, whose compiled
    /// default already IS -16000, and rusqlite's bundled SQLite otherwise).
    #[test]
    fn serving_connections_carry_ts_16mib_page_cache() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let compiled: i64 = conn
            .pragma_query_value(None, "cache_size", |r| r.get(0))
            .unwrap();
        if compiled < 0 {
            // Negative = KiB: the zero-sqlite3 define parity (16325e611).
            assert_eq!(compiled, -16000, "SQLITE_DEFAULT_CACHE_SIZE define parity");
        }
        conn.pragma_update(None, "cache_size", -500).unwrap();
        super::apply_serving_page_cache(&conn).unwrap();
        let applied: i64 = conn
            .pragma_query_value(None, "cache_size", |r| r.get(0))
            .unwrap();
        assert_eq!(
            applied, -16000,
            "serving connections carry TS's 16 MiB page cache"
        );
        assert_eq!(super::SERVING_CONNECTION_CACHE_SIZE_KIB, 16000);
    }
}
