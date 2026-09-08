//! Port of `zqlite/src/internal/statement-cache.ts`.
//!
//! TS keeps prepared statements in an LRU keyed by SQL TEXT
//! (`#cache: Map<string, Statement[]>`), so a statement re-used with the same
//! SQL is prepared exactly once. `TableSource.getRow` goes through it on every
//! call (`this.#stmts.cache.use(stmt, …)`, table-source.ts:522), and the
//! catch-up path calls `getRow` once per row patch — so without the cache the
//! SQLite parser runs per row.
//!
//! Rust realisation: rusqlite's `Statement<'conn>` borrows the `Connection`, so
//! a struct cannot own both the connection and its live statements the way the
//! TS class does. `Connection::prepare_cached` IS the same construct — an LRU
//! keyed by SQL text, held by the connection — so this type delegates to it and
//! carries TS's `maxSize` policy plus a prepare counter that makes the cache
//! hit/miss behaviour observable to tests (there is no TS twin for the counter;
//! it is bookkeeping, not a second cache).
//!
//! TS keys its statement set per `Database` (`#dbCache: WeakMap<Database,
//! Statements>`, table-source.ts:137). `prepare_cached` is per-`Connection`, so
//! a snapshot swap (`set_db`) naturally starts from an empty cache on the new
//! connection — the same per-db keying.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;

use rusqlite::{CachedStatement, Connection};

/// Port of TS `DEFAULT_MAX_CACHED_STATEMENTS` (statement-cache.ts:21).
pub const DEFAULT_MAX_CACHED_STATEMENTS: usize = 1_000;

/// Port of TS `StatementCache` (statement-cache.ts:55).
pub struct StatementCache {
    /// TS `#maxSize` (statement-cache.ts:62).
    max_size: usize,
    /// SQL texts this cache has already prepared on the CURRENT connection.
    /// Rust-only: makes a miss observable so a regression test can pin that
    /// repeated `get_row` calls prepare once, not once per row.
    prepared_sql: RefCell<HashSet<String>>,
    /// Count of actual prepares (cache misses). Rust-only, see above.
    prepares: Cell<u64>,
}

impl StatementCache {
    /// Port of TS `constructor(db, maxSize = DEFAULT_MAX_CACHED_STATEMENTS)`
    /// (statement-cache.ts:71). TS asserts `maxSize >= 0`; a `usize` is that
    /// assertion in the type system.
    pub fn new(max_size: usize) -> Self {
        StatementCache {
            max_size,
            prepared_sql: RefCell::new(HashSet::new()),
            prepares: Cell::new(0),
        }
    }

    /// Port of TS `get maxSize()` (statement-cache.ts:83).
    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Prepares performed (cache misses) since construction. Rust-only test
    /// observability — no TS twin.
    pub fn prepares(&self) -> u64 {
        self.prepares.get()
    }

    /// Reset the miss bookkeeping — called when the underlying connection is
    /// swapped, since `prepare_cached` is per-connection. Rust-only.
    pub fn reset(&self) {
        self.prepared_sql.borrow_mut().clear();
        self.prepares.set(0);
    }

    /// Port of TS `use(sql, fn)` (statement-cache.ts): run `f` against the
    /// prepared statement for `sql`, preparing it only on a miss.
    pub fn use_stmt<T>(
        &self,
        conn: &Connection,
        sql: &str,
        f: impl FnOnce(&mut CachedStatement<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        if !self.prepared_sql.borrow().contains(sql) {
            // Match TS's cache bound on this connection before the first
            // insert; rusqlite's default (16) is far below TS's 1000.
            conn.set_prepared_statement_cache_capacity(self.max_size);
            self.prepared_sql.borrow_mut().insert(sql.to_string());
            self.prepares.set(self.prepares.get() + 1);
        }
        let mut stmt = conn.prepare_cached(sql)?;
        f(&mut stmt)
    }
}

impl Default for StatementCache {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_CACHED_STATEMENTS)
    }
}
