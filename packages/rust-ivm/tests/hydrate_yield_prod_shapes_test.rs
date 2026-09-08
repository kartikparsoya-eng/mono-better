//! The `'yield'` sentinel must reach the hydrate consumer for the query
//! shapes production actually runs — a flipped EXISTS (FlippedJoin over the
//! SQLite `TableSource`) and a plain EXISTS (Join + Exists filter chain) —
//! not only for a single-table scan. Before this port every flipped-join
//! fetch collected its child stream with `skip_yields`, `mergeSortedStreams`
//! dropped sub-stream yields, and the filter chain's `filter(node) -> bool`
//! could not surface the yields of an EXISTS child fetch, so a 33 s prod
//! hydrate of these shapes reported `yields=0` and froze its shard thread
//! (ART run 20260903-012323). TS forwards yields through all of them
//! (flipped-join.ts:180/289, memory-source.ts:1117-1136, exists.ts:254-258,
//! filter-operators.ts:37).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;
use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, OrderPart, RelatedSubquery, SimpleCondition,
    ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::stream::StreamItem;
use rust_ivm::sqlite::table_source::TableSource;

const ISSUES: usize = 12;

fn seeded_db() -> Rc<RefCell<Connection>> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE issues (id TEXT PRIMARY KEY, ownerId TEXT NOT NULL, _0_version TEXT NOT NULL);
        CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL, _0_version TEXT NOT NULL);
        INSERT INTO users VALUES ('u1','Alice','01');
        INSERT INTO users VALUES ('u2','Bob','01');
        "#,
    )
    .unwrap();
    for i in 0..ISSUES {
        // Even issues belong to Alice (u1), odd ones to Bob (u2).
        let owner = if i % 2 == 0 { "Alice" } else { "Bob" };
        conn.execute(
            "INSERT INTO issues VALUES (?1, ?2, '01')",
            rusqlite::params![format!("i{i:03}"), owner],
        )
        .unwrap();
    }
    Rc::new(RefCell::new(conn))
}

fn cols(names: &[&str]) -> HashMap<String, ColumnType> {
    names
        .iter()
        .map(|n| (n.to_string(), ColumnType::String { optional: false }))
        .collect()
}

/// issues WHERE EXISTS(users WHERE id='u1') correlated ownerId=users.name.
fn exists_ast(flip: bool) -> Ast {
    let subquery = Ast {
        table: "users".to_string(),
        alias: Some("users".to_string()),
        where_clause: Some(Condition::Simple(SimpleCondition {
            op: "=".to_string(),
            left: ValuePosition::Column {
                name: "id".to_string(),
            },
            right: ValuePosition::Literal {
                value: Value::Str("u1".into()),
            },
        })),
        ..Default::default()
    };
    Ast {
        table: "issues".to_string(),
        where_clause: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: RelatedSubquery {
                subquery: Box::new(subquery),
                relationship_name: "users".to_string(),
                parent_key: vec!["ownerId".to_string()],
                child_key: vec!["name".to_string()],
                hidden: false,
                system: None,
            },
            op: "EXISTS".to_string(),
            flip: Some(flip),
            scalar: false,
            plan_id: None,
        })),
        order_by: Some(vec![OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        ..Default::default()
    }
}

/// Hydrate `ast` over TableSources whose `shouldYield` is `always`; returns
/// (yields, issue ids delivered).
fn hydrate(ast: Ast, always: bool) -> (usize, Vec<String>) {
    let db = seeded_db();
    let should_yield: Rc<dyn Fn() -> bool> = Rc::new(move || always);
    let issues = TableSource::with_column_order(
        db.clone(),
        "issues",
        cols(&["id", "ownerId", "_0_version"]),
        vec![],
        vec!["id".to_string()],
        should_yield.clone(),
    );
    let users = TableSource::with_column_order(
        db.clone(),
        "users",
        cols(&["id", "name", "_0_version"]),
        vec![],
        vec!["id".to_string()],
        should_yield,
    );
    let mut eng = Engine::new(HashMap::from([
        ("issues".to_string(), vec!["id".to_string()]),
        ("users".to_string(), vec!["id".to_string()]),
    ]));
    eng.register_source(Rc::new(RefCell::new(issues)));
    eng.register_source(Rc::new(RefCell::new(users)));
    eng.set_unique_keys("issues", vec![vec!["id".to_string()]]);
    eng.set_unique_keys("users", vec![vec!["id".to_string()]]);

    let mut stream = eng.start_hydrate(
        &[QuerySpec {
            query_id: "q".into(),
            ast,
        }],
        None,
    );
    let (mut yields, mut ids) = (0usize, Vec::new());
    for item in stream.by_ref() {
        match item {
            StreamItem::Yield => yields += 1,
            StreamItem::Data(rc) => {
                if rc.table == "issues" && !rc.is_hidden {
                    match rc.row_key.get("id") {
                        Some(Value::Str(s)) => ids.push(s.to_string()),
                        other => panic!("id: {other:?}"),
                    }
                }
            }
        }
    }
    eng.finish_hydrate(stream);
    (yields, ids)
}

fn alices_issues() -> Vec<String> {
    (0..ISSUES)
        .filter(|i| i % 2 == 0)
        .map(|i| format!("i{i:03}"))
        .collect()
}

#[test]
fn flipped_exists_hydrate_surfaces_the_table_source_yields() {
    let (yields, ids) = hydrate(exists_ast(true), true);
    assert_eq!(ids, alices_issues());
    // zqlite `generateWithYields` yields before every row when shouldYield is
    // true (table-source.ts:692-699): at least one per delivered parent.
    assert!(
        yields >= ids.len(),
        "flipped exists: {yields} yields for {} rows",
        ids.len()
    );

    let (yields, ids) = hydrate(exists_ast(true), false);
    assert_eq!(ids, alices_issues());
    assert_eq!(yields, 0, "shouldYield=false never yields");
}

#[test]
fn exists_filter_chain_hydrate_surfaces_the_child_fetch_yields() {
    let (yields, ids) = hydrate(exists_ast(false), true);
    assert_eq!(ids, alices_issues());
    // Every issue row is scanned (one yield each from the parent scan, 12)
    // AND the EXISTS child fetch of every Alice row yields before its child
    // node (exists.ts:254-258 forwards them, 6) — the pre-port filter chain
    // only surfaced the parent scan's 12.
    assert!(
        yields >= ISSUES + ids.len(),
        "exists chain: {yields} yields, expected >= {} (parent scan) + {} (child fetches)",
        ISSUES,
        ids.len()
    );

    let (yields, ids) = hydrate(exists_ast(false), false);
    assert_eq!(ids, alices_issues());
    assert_eq!(yields, 0);
}

/// issues.related('users') — a Join whose child relationship stream is
/// walked by the streamer (TS `#streamNodes` recursing into
/// `node.relationships`, pipeline-driver.ts:1380-1383).
fn related_ast() -> Ast {
    Ast {
        table: "issues".to_string(),
        related: vec![RelatedSubquery {
            subquery: Box::new(Ast {
                table: "users".to_string(),
                alias: Some("users".to_string()),
                ..Default::default()
            }),
            relationship_name: "users".to_string(),
            parent_key: vec!["ownerId".to_string()],
            child_key: vec!["name".to_string()],
            hidden: false,
            system: None,
        }],
        order_by: Some(vec![OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        ..Default::default()
    }
}

#[test]
fn related_hydrate_surfaces_the_child_relationship_yields() {
    let (yields, ids) = hydrate(related_ast(), true);
    let all: Vec<String> = (0..ISSUES).map(|i| format!("i{i:03}")).collect();
    assert_eq!(ids, all);
    // 12 from the parent scan plus one per child row (every issue has exactly
    // one owner) — TS `#streamNodes` yields the child stream's `'yield'`s
    // (:1361-1364); the pre-port streamer drained them with `skip_yields`.
    assert!(
        yields >= 2 * ISSUES,
        "related: {yields} yields, expected >= {} (parent scan) + {} (child streams)",
        ISSUES,
        ISSUES
    );
    let (yields, _) = hydrate(related_ast(), false);
    assert_eq!(yields, 0);
}
