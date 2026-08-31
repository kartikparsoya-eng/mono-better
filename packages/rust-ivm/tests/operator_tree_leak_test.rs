//! Pins operator-TREE deallocation on teardown.
//!
//! The operator graph links parent→child via strong `input` Rcs and child→
//! parent via strong `output` back-edges (`OutputHandle`); TS relies on GC to
//! collect these cycles, rust relies on every `destroy()` clearing its own
//! back-edge. A missed edge retains the whole subtree, which pins the source
//! DB cells and defers SQLite closes ("N outstanding conn holder(s) at
//! drop"). These tests assert the live-instance census returns to baseline
//! after remove_query / engine.destroy() for join and EXISTS graphs — the
//! regression guard for that destroy-severs-cycles invariant.
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::Ordering;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::live_count;

/// The census counters are process-global, so these tests must not interleave
/// with each other (cargo runs same-binary tests on parallel threads; a
/// concurrent test's live engine skews the counts — observed as false FAILs).
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn census() -> (i64, i64, i64) {
    (
        live_count::JOIN.load(Ordering::Relaxed),
        live_count::EXISTS.load(Ordering::Relaxed),
        live_count::UNION_FAN_OUT.load(Ordering::Relaxed),
    )
}

fn make_source(name: &str, cols: &[&str], pk: &[&str]) -> Rc<RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = cols
        .iter()
        .map(|c| (c.to_string(), ColumnType::Number { optional: false }))
        .collect();
    Rc::new(RefCell::new(MemorySource::new(
        name,
        columns,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

fn add_row(source: &Rc<RefCell<MemorySource>>, pairs: &[(&str, Value)]) {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    source.borrow_mut().add_row(map);
}

fn basic_ast(table: &str) -> Ast {
    Ast {
        schema: None,
        table: table.to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    }
}

fn joined_ast() -> Ast {
    let mut ast = basic_ast("users");
    ast.related = vec![RelatedSubquery {
        subquery: Box::new(basic_ast("posts")),
        relationship_name: "posts".to_string(),
        parent_key: vec!["id".to_string()],
        child_key: vec!["author_id".to_string()],
        hidden: false,
        system: None,
    }];
    ast
}

fn exists_ast() -> Ast {
    let mut ast = basic_ast("users");
    ast.where_clause = Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: RelatedSubquery {
            subquery: Box::new(basic_ast("posts")),
            relationship_name: "zsubq_posts".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["author_id".to_string()],
            hidden: true,
            system: None,
        },
        op: "EXISTS".to_string(),
        flip: None,
        scalar: false,
        plan_id: None,
    }));
    ast
}

fn make_engine() -> Engine {
    let users = make_source("users", &["id"], &["id"]);
    let posts = make_source("posts", &["id", "author_id"], &["id"]);
    add_row(&users, &[("id", Value::F64(1.0))]);
    add_row(
        &posts,
        &[("id", Value::F64(10.0)), ("author_id", Value::F64(1.0))],
    );
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(users);
    engine.register_source(posts);
    engine
}

/// remove_query must deallocate the pipeline's operator tree, not just
/// unregister it. A retained Join pins its TableSourceInputs, which pin the
/// source DB cell — the "outstanding conn holder(s)" class on SQLite.
#[test]
fn remove_query_frees_join_tree() {
    let _serial = SERIAL.lock().unwrap();
    let mut engine = make_engine();
    let (join0, _, _) = census();

    engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast: joined_ast(),
    }]);
    let (join1, _, _) = census();
    assert!(join1 > join0, "join query must allocate a Join");

    engine.remove_query("q1");
    let (join2, _, _) = census();
    assert_eq!(
        join2,
        join0,
        "remove_query retained {} Join operator(s) — tree not deallocated \
         (Rc cycle not fully severed by destroy())",
        join2 - join0
    );
}

/// Same for engine.destroy() — the CG-teardown path.
#[test]
fn engine_destroy_frees_all_trees() {
    let _serial = SERIAL.lock().unwrap();
    let (join0, exists0, _) = census();
    {
        let mut engine = make_engine();
        engine.add_queries(&[
            QuerySpec {
                query_id: "qj".to_string(),
                ast: joined_ast(),
            },
            QuerySpec {
                query_id: "qe".to_string(),
                ast: exists_ast(),
            },
        ]);
        let (join1, exists1, _) = census();
        assert!(
            join1 > join0 && exists1 >= exists0,
            "queries must build ops"
        );
        engine.destroy();
        // engine dropped at scope end
    }
    let (join2, exists2, _) = census();
    assert_eq!(
        (join2, exists2),
        (join0, exists0),
        "engine.destroy() retained operators: {} Join(s), {} Exists — \
         trees not deallocated",
        join2 - join0,
        exists2 - exists0
    );
}

/// Churn shape: repeated add/remove at stable live-query count must hold the
/// operator census flat (the prod ~171 teardowns/hr regime).
#[test]
fn add_remove_churn_holds_operator_census_flat() {
    let _serial = SERIAL.lock().unwrap();
    let mut engine = make_engine();
    engine.add_queries(&[QuerySpec {
        query_id: "stable".to_string(),
        ast: joined_ast(),
    }]);
    let steady = census();
    for i in 0..50 {
        engine.add_queries(&[QuerySpec {
            query_id: format!("churn-{i}"),
            ast: joined_ast(),
        }]);
        engine.remove_query(&format!("churn-{i}"));
        assert_eq!(
            census(),
            steady,
            "operator census grew after churn cycle {i}"
        );
    }
}
