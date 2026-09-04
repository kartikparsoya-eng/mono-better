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
