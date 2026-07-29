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
//! query-delegate: ZQLite QueryDelegate implementation.

pub mod query_builder;
pub mod table_source;
pub mod db;
pub mod database_storage;
pub mod resolve_scalar_subqueries;
pub mod sqlite_stat_fanout;
pub mod sqlite_cost_model;
pub mod explain_queries;
pub mod options;
pub mod query_delegate;
pub mod interrupt; // cross-thread SQLite interrupt + job-scoped watchdog (N1/N2)

pub use table_source::*;
pub use query_builder::*;
pub use db::*;
pub use database_storage::*;
pub use resolve_scalar_subqueries::*;
pub use sqlite_stat_fanout::*;
pub use sqlite_cost_model::*;
pub use explain_queries::*;
pub use options::*;
pub use query_delegate::*;
pub use interrupt::{install_interrupt, JobWatchdog, WatchGuard};
