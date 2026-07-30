//! Explain queries — port of `zqlite/src/explain-queries.ts`.
//!
//! Runs EXPLAIN QUERY PLAN for a set of queries and returns the plans.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::sqlite::db::Database;

/// Row counts by source — maps query strings to their row counts.
/// Port of TS `RowCountsBySource`.
pub type RowCountsBySource = HashMap<String, HashMap<String, usize>>;

/// Run EXPLAIN QUERY PLAN for each query in the row counts.
/// Port of TS `explainQueries` (explain-queries.ts:4).
pub fn explain_queries(
    counts: &RowCountsBySource,
    db: &Rc<RefCell<Database>>,
) -> HashMap<String, Vec<String>> {
    let mut plans: HashMap<String, Vec<String>> = HashMap::new();

    for query_set in counts.values() {
        for query in query_set.keys() {
            // Replace ? placeholders with a literal for EXPLAIN.
            let explained = query.replace('?', "'sdfse'");
            let sql = format!("EXPLAIN QUERY PLAN {}", explained);

            let db = db.borrow();
            let conn = db.conn();
            let conn = conn.borrow();
            if let Ok(mut stmt) = conn.prepare(&sql)
                && let Ok(mut rows) = stmt.query([]) {
                    let mut plan: Vec<String> = Vec::new();
                    while let Ok(Some(row)) = rows.next() {
                        if let Ok(detail) = row.get::<_, String>(2) {
                            plan.push(detail);
                        }
                    }
                    plans.insert(query.clone(), plan);
                }
        }
    }

    plans
}
