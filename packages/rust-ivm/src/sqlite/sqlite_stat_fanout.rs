//! SQLite stat fanout — port of `zqlite/src/sqlite-stat-fanout.ts`.
//!
//! Computes join fanout factors from SQLite statistics tables (stat4, stat1).
//! Used by the query planner to estimate join cardinality.

use std::cell::RefCell;
use std::rc::Rc;

use crate::sqlite::db::Database;

/// Result of fanout calculation from SQLite statistics.
/// Port of TS `FanoutResult` (sqlite-stat-fanout.ts:9).
#[derive(Clone, Debug)]
pub struct FanoutResult {
    pub fanout: f64,
    pub confidence: Confidence,
    pub source: FanoutSource,
}

/// Confidence level of the fanout estimate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Confidence {
    High,
    Med,
    None,
}

/// Source of the fanout calculation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FanoutSource {
    Stat4,
    Stat1,
    Default,
}

/// Computes join fanout factors from SQLite statistics tables.
/// Port of TS `SQLiteStatFanout` (sqlite-stat-fanout.ts:71).
pub struct SQLiteStatFanout {
    db: Rc<RefCell<Database>>,
}

/// Default fanout when no statistics are available.
const DEFAULT_FANOUT: f64 = 10.0;

impl SQLiteStatFanout {
    pub fn new(db: Rc<RefCell<Database>>) -> Self {
        SQLiteStatFanout { db }
    }

    /// Get the fanout for a table column (average rows per distinct value).
    /// Port of TS `getFanout` (sqlite-stat-fanout.ts).
    pub fn get_fanout(&self, table: &str, column: &str) -> FanoutResult {
        // Try stat4 first (most accurate, excludes NULLs).
        if let Some(fanout) = self.get_stat4_fanout(table, column) {
            return FanoutResult {
                fanout,
                confidence: Confidence::High,
                source: FanoutSource::Stat4,
            };
        }

        // Fall back to stat1 (includes NULLs, may overestimate).
        if let Some(fanout) = self.get_stat1_fanout(table, column) {
            return FanoutResult {
                fanout,
                confidence: Confidence::Med,
                source: FanoutSource::Stat1,
            };
        }

        // Default fallback.
        FanoutResult {
            fanout: DEFAULT_FANOUT,
            confidence: Confidence::None,
            source: FanoutSource::Default,
        }
    }

    /// Get fanout from sqlite_stat4 histogram.
    fn get_stat4_fanout(&self, table: &str, column: &str) -> Option<f64> {
        let db = self.db.borrow();
        let conn = db.conn();
        let conn = conn.borrow();

        // Check if sqlite_stat4 exists and has data.
        let sql = format!(
            "SELECT neq, nlt, ndlt FROM sqlite_stat4 WHERE tbl = '{}' AND idx LIKE '%{}%' LIMIT 1",
            table, column
        );

        let mut stmt = conn.prepare(&sql).ok()?;
        let mut rows = stmt.query([]).ok()?;

        if let Some(row) = rows.next().ok()? {
            let neq: String = row.get(0).ok()?;
            // neq format: "N1 N2" for composite keys. Take first value.
            let neq_val: f64 = neq.split(' ').next()?.parse().ok()?;
            if neq_val > 0.0 {
                return Some(neq_val);
            }
        }

        None
    }

    /// Get fanout from sqlite_stat1.
    fn get_stat1_fanout(&self, table: &str, column: &str) -> Option<f64> {
        let db = self.db.borrow();
        let conn = db.conn();
        let conn = conn.borrow();

        // stat1 format: "rows nc" where nc is the approximate number of
        // rows for each distinct value of the indexed columns.
        let sql = format!(
            "SELECT stat FROM sqlite_stat1 WHERE tbl = '{}' AND idx LIKE '%{}%' LIMIT 1",
            table, column
        );

        let mut stmt = conn.prepare(&sql).ok()?;
        let mut rows = stmt.query([]).ok()?;

        if let Some(row) = rows.next().ok()? {
            let stat: String = row.get(0).ok()?;
            // Format: "total_rows distinct_count" — fanout = total / distinct.
            let parts: Vec<&str> = stat.split(' ').collect();
            if parts.len() >= 2 {
                let total: f64 = parts[0].parse().ok()?;
                let distinct: f64 = parts[1].parse().ok()?;
                if distinct > 0.0 {
                    return Some(total / distinct);
                }
            }
        }

        None
    }
}
