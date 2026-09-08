//! Release-mode timing probe for the hydrate streamer (ignored; run with
//! `cargo test --release --test hydrate_streamer_bench -- --ignored --nocapture`).
//! A `related` query over a SQLite TableSource with ISSUES parents × 1 child
//! each: the per-node cost of the streamer walk dominates.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;
use rust_ivm::builder::ast::{Ast, OrderPart, RelatedSubquery};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::stream::StreamItem;
use rust_ivm::sqlite::table_source::TableSource;

const ISSUES: usize = 100_000;

fn cols(names: &[&str]) -> HashMap<String, ColumnType> {
    names
        .iter()
        .map(|n| (n.to_string(), ColumnType::String { optional: false }))
        .collect()
}

fn bench(label: &str, make_ast: fn() -> Ast) {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE issues (id TEXT PRIMARY KEY, ownerId TEXT NOT NULL, _0_version TEXT NOT NULL);
        CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT NOT NULL, _0_version TEXT NOT NULL);
        CREATE INDEX users_name ON users(name);
        "#,
    )
    .unwrap();
    for i in 0..ISSUES {
        conn.execute(
            "INSERT INTO issues VALUES (?1, ?2, '01')",
            rusqlite::params![format!("i{i:06}"), format!("u{}", i % 1000)],
        )
        .unwrap();
    }
    for u in 0..1000 {
        conn.execute(
            "INSERT INTO users VALUES (?1, ?2, '01')",
            rusqlite::params![format!("user{u}"), format!("u{u}")],
        )
        .unwrap();
    }
    let db = Rc::new(RefCell::new(conn));
    let should_yield: Rc<dyn Fn() -> bool> = Rc::new(|| false);
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
    let ast = make_ast();
    let _unused = || Ast {
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
    };
    let started = std::time::Instant::now();
    let mut stream = eng.start_hydrate(
        &[QuerySpec {
            query_id: "q".into(),
            ast,
        }],
        None,
    );
    let mut rows = 0usize;
    for item in stream.by_ref() {
        if let StreamItem::Data(_) = item {
            rows += 1;
        }
    }
    eng.finish_hydrate(stream);
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    println!(
        "BENCH {label} rows={rows} elapsed_ms={ms:.1} per_row_us={:.2}",
        ms * 1000.0 / rows as f64
    );
}

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

fn exists_ast(flip: bool) -> Ast {
    use rust_ivm::builder::ast::{Condition, CorrelatedSubqueryCondition};
    Ast {
        table: "issues".to_string(),
        where_clause: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: RelatedSubquery {
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

#[test]
#[ignore = "timing probe"]
fn related_hydrate_100k() {
    bench("related", related_ast);
}

#[test]
#[ignore = "timing probe"]
fn exists_hydrate_100k() {
    bench("exists", || exists_ast(false));
}

#[test]
#[ignore = "timing probe"]
fn flipped_exists_hydrate_100k() {
    bench("flipped_exists", || exists_ast(true));
}
