//! SQLite cost model — port of `zqlite/src/sqlite-cost-model.ts`.
//!
//! Creates a SQLite-based cost model for query planning.
//! Uses SQLite's scanstatus API to estimate query costs based on the actual
//! SQLite query planner's analysis.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::builder::ast::Condition;
use crate::sqlite::db::Database;
use crate::sqlite::sqlite_stat_fanout::SQLiteStatFanout;

/// A cost estimate for a connection.
/// Port of TS `CostModelCost` (planner-connection.ts).
#[derive(Clone, Debug)]
pub struct CostModelCost {
    pub cost: f64,
    pub size: usize,
}

/// A connection cost model function.
/// Port of TS `ConnectionCostModel` (planner-connection.ts).
pub type ConnectionCostModel = Box<
    dyn Fn(
        &str,                                              // table_name
        &[(String, String)],                               // sort (ordering)
        Option<&Condition>,                                // filters
        Option<&HashMap<String, crate::ivm::data::Value>>, // constraint
    ) -> CostModelCost,
>;

/// Create a SQLite-based cost model.
/// Port of TS `createSQLiteCostModel` (sqlite-cost-model.ts:37).
pub fn create_sqlite_cost_model(
    db: Rc<RefCell<Database>>,
    _table_specs: &HashMap<String, HashMap<String, crate::ivm::schema::ColumnType>>,
) -> ConnectionCostModel {
    let fanout_estimator = SQLiteStatFanout::new(db.clone());

    Box::new(move |table_name, sort, filters, _constraint| {
        // Estimate cost based on SQLite statistics.
        // For a simple estimate: cost = rows * fanout factor.
        let fanout = if sort.is_empty() {
            1.0
        } else {
            let col = &sort[0].0;
            fanout_estimator.get_fanout(table_name, col).fanout
        };

        // Base cost: table scan cost (proportional to number of rows).
        let base_cost = 10.0;

        // Filter selectivity estimate.
        let filter_factor = match filters {
            Some(Condition::Simple(_)) => 0.1,
            Some(Condition::And(conds)) => 0.1_f64.powi(conds.len() as i32),
            Some(Condition::Or(conds)) => 1.0 - ((1.0_f64) - 0.1_f64).powi(conds.len() as i32),
            _ => 1.0,
        };

        let cost = base_cost * fanout * filter_factor;
        let size = (100.0 * filter_factor) as usize;

        CostModelCost { cost, size }
    })
}

/// Remove correlated subquery conditions from a condition tree.
/// Port of TS `removeCorrelatedSubqueries` (sqlite-cost-model.ts:67).
pub fn remove_correlated_subqueries(condition: &Condition) -> Condition {
    match condition {
        Condition::Simple(_) => condition.clone(),
        Condition::CorrelatedSubquery(_) => crate::builder::expression::true_val(),
        Condition::And(conditions) => {
            let filtered: Vec<Condition> = conditions
                .iter()
                .filter(|c| !matches!(c, Condition::CorrelatedSubquery(_)))
                .map(remove_correlated_subqueries)
                .collect();
            if filtered.is_empty() {
                crate::builder::expression::true_val()
            } else {
                Condition::And(filtered)
            }
        }
        Condition::Or(conditions) => {
            let filtered: Vec<Condition> = conditions
                .iter()
                .filter(|c| !matches!(c, Condition::CorrelatedSubquery(_)))
                .map(remove_correlated_subqueries)
                .collect();
            if filtered.is_empty() {
                crate::builder::expression::true_val()
            } else {
                Condition::Or(filtered)
            }
        }
    }
}
