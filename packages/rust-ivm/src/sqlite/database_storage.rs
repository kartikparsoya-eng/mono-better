//! Database storage — port of `zqlite/src/database-storage.ts`.
//!
//! SQLite-backed persistent storage for Take/Cap operator state.
//! Uses a `storage` table with (clientGroupID, op, key) primary key.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::ivm::data::Value;
use crate::ivm::operator::Storage;
use crate::sqlite::db::Database;

/// SQL to create the storage table.
pub const CREATE_STORAGE_TABLE: &str = "
  CREATE TABLE IF NOT EXISTS storage (
    clientGroupID TEXT,
    op INTEGER,
    key TEXT,
    val TEXT,
    PRIMARY KEY(clientGroupID, op, key)
  )
";

/// Client group storage — creates Storage instances per operator.
pub struct ClientGroupStorage {
    db: Rc<RefCell<Database>>,
    cg_id: String,
    next_op_id: std::sync::atomic::AtomicUsize,
}

impl ClientGroupStorage {
    /// Create a new ClientGroupStorage for the given database and client group ID.
    pub fn new(db: Rc<RefCell<Database>>, cg_id: String) -> Self {
        ClientGroupStorage {
            db,
            cg_id,
            next_op_id: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Create a new Storage instance for a single operator.
    pub fn create_storage(&self) -> Rc<RefCell<DatabaseStorage>> {
        let op_id = {
            let next = self
                .next_op_id
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            next + 1
        };
        Rc::new(RefCell::new(DatabaseStorage {
            db: self.db.clone(),
            cg_id: self.cg_id.clone(),
            op_id,
        }))
    }

    /// Delete all storage for this client group.
    pub fn destroy(&self) {
        let sql = "DELETE FROM storage WHERE clientGroupID = ?";
        let conn = self.db.borrow().conn();
        conn.borrow()
            .execute(sql, [&self.cg_id])
            .expect("DatabaseStorage destroy failed");
    }
}

/// SQLite-backed storage for a single operator instance.
/// Port of TS `DatabaseStorage` createClientGroupStorage (database-storage.ts:157).
pub struct DatabaseStorage {
    db: Rc<RefCell<Database>>,
    cg_id: String,
    op_id: usize,
}

impl Storage for DatabaseStorage {
    fn get(&self, key: &str) -> Option<Value> {
        let sql = "SELECT val FROM storage WHERE clientGroupID = ? AND op = ? AND key = ?";
        let conn = self.db.borrow().conn();
        let conn = conn.borrow();
        // Storage errors are never demoted to "no state": returning None for a
        // failed read would present missing operator state (e.g. a Take
        // partition with no bound) and silently corrupt the pipeline. TS
        // better-sqlite3 THROWS here -> view-syncer teardown; panic = parity.
        let mut stmt = conn
            .prepare(sql)
            .expect("DatabaseStorage get prepare failed");
        let mut rows = stmt
            .query(rusqlite::params![&self.cg_id, self.op_id as i64, key])
            .expect("DatabaseStorage get query failed");
        if let Some(row) = rows.next().expect("DatabaseStorage get step failed") {
            let val: String = row.get(0).expect("DatabaseStorage get column failed");
            Some(parse_json_value(&val))
        } else {
            None
        }
    }

    fn set(&mut self, key: String, value: Value) {
        let sql = "INSERT INTO storage (clientGroupID, op, key, val) VALUES(?, ?, ?, ?)
                   ON CONFLICT(clientGroupID, op, key) DO UPDATE SET val = excluded.val";
        let json = value_to_json_string(&value);
        let conn = self.db.borrow().conn();
        // A swallowed write here would diverge persisted operator state from
        // the in-memory view with no signal (the take bound=None class). TS
        // better-sqlite3 .run() THROWS -> teardown; panic = parity.
        conn.borrow()
            .execute(
                sql,
                rusqlite::params![&self.cg_id, self.op_id as i64, key, json],
            )
            .expect("DatabaseStorage set failed");
    }

    fn del(&mut self, key: &str) {
        let sql = "DELETE FROM storage WHERE clientGroupID = ? AND op = ? AND key = ?";
        let conn = self.db.borrow().conn();
        conn.borrow()
            .execute(sql, rusqlite::params![&self.cg_id, self.op_id as i64, key])
            .expect("DatabaseStorage del failed");
    }

    fn scan(&self, prefix: Option<&str>) -> Vec<(String, Value)> {
        let pfx = prefix.unwrap_or("");
        let sql = "SELECT key, val FROM storage WHERE clientGroupID = ? AND op = ? AND key >= ?";
        let conn = self.db.borrow().conn();
        let conn = conn.borrow();
        // Same contract as get(): a failed scan must not read as "empty state".
        let mut stmt = conn
            .prepare(sql)
            .expect("DatabaseStorage scan prepare failed");
        let mut rows = stmt
            .query(rusqlite::params![&self.cg_id, self.op_id as i64, pfx])
            .expect("DatabaseStorage scan query failed");
        let mut result = Vec::new();
        while let Some(row) = rows.next().expect("DatabaseStorage scan step failed") {
            let key: String = row.get(0).expect("DatabaseStorage scan key failed");
            let val: String = row.get(1).expect("DatabaseStorage scan val failed");
            if !pfx.is_empty() && !key.starts_with(pfx) {
                break;
            }
            result.push((key, parse_json_value(&val)));
        }
        result
    }
}

/// Create a DatabaseStorage instance.
pub fn create_database_storage(path: &str) -> Result<Database, String> {
    let db = Database::new(path).map_err(|e| e.0)?;
    db.exec("PRAGMA journal_mode = OFF")
        .map_err(|e| e.to_string())?;
    db.exec("PRAGMA synchronous = OFF")
        .map_err(|e| e.to_string())?;
    db.exec(CREATE_STORAGE_TABLE).map_err(|e| e.to_string())?;
    Ok(db)
}

/// Convert a Value to a JSON string for storage.
fn value_to_json_string(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::F64(n) => n.to_string(),
        Value::Str(s) => format!("\"{}\"", s),
        Value::Json(s) => s.to_string(),
    }
}

/// Parse a JSON string back into a Value.
fn parse_json_value(s: &str) -> Value {
    let s = s.trim();
    if s == "null" {
        return Value::Null;
    }
    if s == "true" {
        return Value::Bool(true);
    }
    if s == "false" {
        return Value::Bool(false);
    }
    if s.starts_with('"') && s.ends_with('"') {
        return Value::Str(Arc::from(&s[1..s.len() - 1]));
    }
    if let Ok(n) = s.parse::<f64>() {
        return Value::F64(n);
    }
    Value::Str(Arc::from(s))
}
