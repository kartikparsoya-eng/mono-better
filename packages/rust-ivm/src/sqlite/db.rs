//! SQLite database wrapper — port of `zqlite/src/db.ts`.
//!
//! Wraps a `rusqlite::Connection` with prepared statement caching and
//! case-sensitive LIKE (to match Postgres semantics).

use std::cell::RefCell;
use std::rc::Rc;

use rusqlite::Connection;

/// Read a SQLite cell without rusqlite's infallible `ValueRef -> Value`
/// conversion, which panics on malformed UTF-8 in a TEXT value. better-sqlite3
/// decodes the same bytes with replacement characters, so use the identical
/// lossy UTF-8 contract at every SQLite -> Rust boundary.
pub fn read_value_lossy(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<rusqlite::types::Value> {
    use rusqlite::types::{Value, ValueRef};

    Ok(match row.get_ref(index)? {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::Integer(value),
        ValueRef::Real(value) => Value::Real(value),
        ValueRef::Text(value) => Value::Text(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::Blob(value.to_vec()),
    })
}

/// A SQLite database connection wrapper.
/// Port of TS `Database` (db.ts:30).
pub struct Database {
    conn: Rc<RefCell<Connection>>,
    path: String,
    page_size: usize,
    /// Cross-thread interrupt handle (seam 1). Installed at every open so the
    /// connection is interruptible from another thread; may be registered with
    /// a `JobWatchdog` when this Database runs under one. `None` only if
    /// `install_interrupt` was skipped (it never is — infallible).
    _interrupt_handle: Option<rusqlite::InterruptHandle>,
}

/// Error initializing the database.
#[derive(Debug)]
pub struct DatabaseInitError(pub String);

impl std::fmt::Display for DatabaseInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Database init error: {}", self.0)
    }
}

impl std::error::Error for DatabaseInitError {}

impl Database {
    /// Open a database at `path`.
    pub fn new(path: &str) -> Result<Self, DatabaseInitError> {
        let conn = Connection::open(path).map_err(|e| DatabaseInitError(e.to_string()))?;
        // Install a handle so an in-flight query can be cancelled out-of-band.
        let interrupt_handle = crate::sqlite::install_interrupt(&conn);

        // Match Postgres LIKE/ILIKE semantics: case-sensitive LIKE.
        conn.pragma_update(None, "case_sensitive_like", "ON")
            .map_err(|e| DatabaseInitError(e.to_string()))?;

        let page_size: usize = conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .map_err(|e| DatabaseInitError(e.to_string()))?;

        Ok(Database {
            conn: Rc::new(RefCell::new(conn)),
            path: path.to_string(),
            page_size,
            _interrupt_handle: Some(interrupt_handle),
        })
    }

    /// Open an in-memory database (for testing).
    pub fn in_memory() -> Result<Self, DatabaseInitError> {
        let conn = Connection::open_in_memory().map_err(|e| DatabaseInitError(e.to_string()))?;
        let interrupt_handle = crate::sqlite::install_interrupt(&conn);
        conn.pragma_update(None, "case_sensitive_like", "ON")
            .map_err(|e| DatabaseInitError(e.to_string()))?;

        Ok(Database {
            conn: Rc::new(RefCell::new(conn)),
            path: ":memory:".to_string(),
            page_size: 4096,
            _interrupt_handle: Some(interrupt_handle),
        })
    }

    /// Get the underlying connection.
    pub fn conn(&self) -> Rc<RefCell<Connection>> {
        self.conn.clone()
    }

    /// Execute raw SQL (no return rows).
    pub fn exec(&self, sql: &str) -> Result<(), rusqlite::Error> {
        self.conn.borrow().execute_batch(sql)
    }

    /// Run a PRAGMA query and return the first value as a string.
    pub fn pragma_query_value_string(&self, name: &str) -> Result<String, rusqlite::Error> {
        let sql = format!("PRAGMA {}", name);
        self.conn
            .borrow()
            .query_row(&sql, [], |row| row.get::<_, String>(0))
    }

    /// Run a PRAGMA query and return the first value as an integer.
    pub fn pragma_query_value_int(&self, name: &str) -> Result<i64, rusqlite::Error> {
        let sql = format!("PRAGMA {}", name);
        self.conn
            .borrow()
            .query_row(&sql, [], |row| row.get::<_, i64>(0))
    }

    /// Get the database path/name.
    pub fn name(&self) -> &str {
        &self.path
    }

    /// Get page size.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Compact: run incremental vacuum if freeable bytes exceed threshold.
    pub fn compact(&self, freeable_bytes_threshold: usize) -> Result<(), rusqlite::Error> {
        let freelist_count = self.pragma_query_value_int("freelist_count")? as usize;
        let freeable = freelist_count * self.page_size;
        if freeable < freeable_bytes_threshold {
            return Ok(());
        }

        let auto_vacuum = self.pragma_query_value_int("auto_vacuum")?;
        if auto_vacuum != 2 {
            return Ok(()); // AUTO_VACUUM is not INCREMENTAL
        }

        self.exec("PRAGMA incremental_vacuum")?;
        Ok(())
    }

    /// Close the database (runs PRAGMA optimize first).
    pub fn close(&self) -> Result<(), rusqlite::Error> {
        let _ = self.exec("PRAGMA optimize");
        Ok(())
    }
}

/// A prepared statement wrapper with logging support.
/// Port of TS `Statement` (db.ts:161).
///
/// In Rust, prepared statements borrow the connection, so we use a
/// different pattern: the `Database` provides methods that prepare and
/// execute within a single borrow scope.
pub struct Statement {
    sql: String,
    conn: Rc<RefCell<Connection>>,
}

impl Statement {
    /// Create a statement handle (SQL is prepared lazily on each call).
    pub fn new(conn: Rc<RefCell<Connection>>, sql: &str) -> Result<Self, rusqlite::Error> {
        Ok(Statement {
            sql: sql.to_string(),
            conn,
        })
    }

    /// Run the statement with parameters (no return rows).
    pub fn run(&self, params: &[&dyn rusqlite::ToSql]) -> Result<usize, rusqlite::Error> {
        let conn = self.conn.borrow();
        let mut stmt = conn.prepare(&self.sql)?;
        stmt.execute(params)
    }

    /// Get a single row as a map.
    #[allow(clippy::needless_range_loop)]
    pub fn get(
        &self,
        params: &[&dyn rusqlite::ToSql],
    ) -> Result<Option<rustc_hash::FxHashMap<String, rusqlite::types::Value>>, rusqlite::Error>
    {
        let conn = self.conn.borrow();
        let mut stmt = conn.prepare(&self.sql)?;
        let col_count = stmt.column_count();
        let col_names: Vec<String> = (0..col_count)
            .map(|i| stmt.column_name(i).unwrap().to_string())
            .collect();
        let mut rows = stmt.query(params)?;
        if let Some(row) = rows.next()? {
            let mut map: rustc_hash::FxHashMap<String, rusqlite::types::Value> =
                rustc_hash::FxHashMap::default();
            for i in 0..col_count {
                let name = col_names[i].clone();
                let val = read_value_lossy(row, i)?;
                map.insert(name, val);
            }
            Ok(Some(map))
        } else {
            Ok(None)
        }
    }

    /// Get all rows as a list of maps.
    #[allow(clippy::needless_range_loop)]
    pub fn all(
        &self,
        params: &[&dyn rusqlite::ToSql],
    ) -> Result<Vec<rustc_hash::FxHashMap<String, rusqlite::types::Value>>, rusqlite::Error> {
        let conn = self.conn.borrow();
        let mut stmt = conn.prepare(&self.sql)?;
        let col_count = stmt.column_count();
        // Collect column names before querying to avoid borrow conflicts.
        let col_names: Vec<String> = (0..col_count)
            .map(|i| stmt.column_name(i).unwrap().to_string())
            .collect();
        let mut rows = stmt.query(params)?;
        let mut result = Vec::new();
        while let Some(row) = rows.next()? {
            let mut map: rustc_hash::FxHashMap<String, rusqlite::types::Value> =
                rustc_hash::FxHashMap::default();
            for i in 0..col_count {
                let name = col_names[i].clone();
                let val = read_value_lossy(row, i)?;
                map.insert(name, val);
            }
            result.push(map);
        }
        Ok(result)
    }
}
