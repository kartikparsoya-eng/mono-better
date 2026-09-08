//! Validates the runtime planner path (plan_ast_flips + the snapshot-connection
//! cost model) end to end against a REAL SQLite with known table sizes — the
//! step-3/4 wiring, minus the napi boundary.

use std::cell::RefCell;
use std::rc::Rc;

use rust_ivm::planner::{create_snapshot_cost_model, plan_ast_flips};

fn conn_with(tables: &[(&str, usize)]) -> Rc<RefCell<rusqlite::Connection>> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    for (name, rows) in tables {
        conn.execute_batch(&format!(
            "CREATE TABLE \"{name}\" (id INTEGER PRIMARY KEY, parent_id INTEGER);"
        ))
        .unwrap();
        for i in 0..*rows {
            conn.execute(
                &format!("INSERT INTO \"{name}\" (id, parent_id) VALUES (?, ?)"),
                rusqlite::params![i as i64, (i % 7) as i64],
            )
            .unwrap();
        }
    }
    Rc::new(RefCell::new(conn))
}

fn exists_ast(child: &str) -> serde_json::Value {
    serde_json::json!({
        "table": "parent",
        "where": {
            "type": "correlatedSubquery",
            "op": "EXISTS",
            "related": {
                "correlation": {"parentField": ["id"], "childField": ["parent_id"]},
                "subquery": {"table": child, "alias": child}
            }
        }
    })
}

#[test]
fn flips_when_child_is_smaller() {
    // parent=200, child=10 → flipping (iterate 10 children, seek parents) wins.
    let conn = conn_with(&[("parent", 200), ("child", 10)]);
    let flips = plan_ast_flips(&exists_ast("child"), create_snapshot_cost_model(conn));
    assert_eq!(flips, vec![Some(true)], "should flip when child is smaller");
}

#[test]
fn no_flip_when_child_is_larger() {
    // parent=10, child=200 → semi-join (iterate 10 parents) wins.
    let conn = conn_with(&[("parent", 10), ("child", 200)]);
    let flips = plan_ast_flips(&exists_ast("child"), create_snapshot_cost_model(conn));
    assert_eq!(
        flips,
        vec![Some(false)],
        "should not flip when child is larger"
    );
}
