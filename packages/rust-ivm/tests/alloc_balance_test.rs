//! Allocation-balance proof — a COUNTING GLOBAL ALLOCATOR asserts that full
//! engine lifecycles return the heap to baseline.
//!
//! Census counters (`live_count`) prove tracked STRUCTS free; `Weak` upgrade
//! tests prove specific cycles are gone. Neither can see a logical leak in an
//! untracked allocation (a Vec that grows in a cache, a String retained in a
//! map). This test closes that hole the way a leak checker would: wrap the
//! system allocator, count live bytes, run the same lifecycle prod churns
//! through (snapshotter leapfrog on a real SQLite file, engine hydrate/push/
//! remove churn with EXISTS joins, planner runs with the real scanstatus cost
//! model), and assert the live-byte count does not ratchet across cycles.
//!
//! Methodology: 3 warm-up cycles absorb one-time allocations (SQLite global
//! caches, lazy statics, hash seeds), then 20 measured cycles must not grow
//! live bytes by more than a small jitter budget. The historical planner leak
//! (~78KB per planAst round) exceeds the budget within a single cycle, so the
//! test fails loudly against any regression of that class.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicIsize, Ordering};

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{Ast, RelatedSubquery};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;
use rust_ivm::snapshotter::Snapshotter;

// ---------------------------------------------------------------------------
// Counting allocator
// ---------------------------------------------------------------------------

static LIVE_BYTES: AtomicIsize = AtomicIsize::new(0);

struct CountingAlloc;

// SAFETY: defers all allocation to `System`; only bookkeeping is added.
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(layout.size() as isize, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as isize, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            LIVE_BYTES.fetch_add(
                new_size as isize - layout.size() as isize,
                Ordering::Relaxed,
            );
        }
        p
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

fn live_bytes() -> isize {
    LIVE_BYTES.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// The lifecycle under measurement
// ---------------------------------------------------------------------------

fn replica_path() -> String {
    let dir = std::env::temp_dir()
        .join(format!("alloc-balance-{}", std::process::id()))
        .to_string_lossy()
        .to_string();
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = format!("{dir}/replica.db");
    let conn = rusqlite::Connection::open(&db_path).unwrap();
    conn.execute_batch(
        r#"
        PRAGMA journal_mode=wal;
        CREATE TABLE "_zero.replicationState" (
            lock TEXT PRIMARY KEY DEFAULT 'singleton',
            stateVersion TEXT NOT NULL
        );
        INSERT INTO "_zero.replicationState" (lock, stateVersion) VALUES ('singleton', '01');
        CREATE TABLE parent (id INTEGER PRIMARY KEY, parent_id INTEGER);
        CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER);
        "#,
    )
    .unwrap();
    for i in 0..200i64 {
        conn.execute(
            "INSERT INTO parent (id, parent_id) VALUES (?, ?)",
            rusqlite::params![i, i % 7],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO child (id, parent_id) VALUES (?, ?)",
            rusqlite::params![i, i % 11],
        )
        .unwrap();
    }
    conn.execute_batch("ANALYZE;").unwrap();
    db_path
}

fn make_source(name: &str, cols: &[&str]) -> Rc<RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = cols
        .iter()
        .map(|c| (c.to_string(), ColumnType::Number { optional: false }))
        .collect();
    Rc::new(RefCell::new(MemorySource::new(
        name,
        columns,
        vec!["id".to_string()],
    )))
}

fn joined_ast() -> Ast {
    let mut ast = Ast {
        schema: None,
        table: "users".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    };
    ast.related = vec![RelatedSubquery {
        subquery: Box::new(Ast {
            schema: None,
            table: "posts".to_string(),
            alias: Some("posts".to_string()),
            where_clause: None,
            related: vec![],
            limit: None,
            order_by: None,
            start: None,
        }),
        relationship_name: "posts".to_string(),
        parent_key: vec!["id".to_string()],
        child_key: vec!["user_id".to_string()],
        hidden: false,
        system: None,
    }];
    ast
}

fn planner_exists_ast() -> serde_json::Value {
    serde_json::json!({
        "table": "parent",
        "where": {
            "type": "correlatedSubquery", "op": "EXISTS", "related": {
                "correlation": {"parentField": ["id"], "childField": ["parent_id"]},
                "subquery": {"table": "child", "alias": "child"}
            }
        }
    })
}

/// One full lifecycle: snapshotter leapfrog on a real file, engine
/// hydrate/push/remove churn, planner runs with the real scanstatus model.
fn one_cycle(db_path: &str, cycle: i64) {
    // --- snapshotter: init + advances + destroy (pinned-conn lifecycle) ---
    {
        let writer = rusqlite::Connection::open(db_path).unwrap();
        let mut snap = Snapshotter::new(db_path, "alloc-test", None);
        snap.init().unwrap();
        for a in 0..3i64 {
            let v = format!("{:02}", 2 + cycle * 3 + a);
            writer
                .execute(
                    "UPDATE \"_zero.replicationState\" SET stateVersion = ?",
                    [&v],
                )
                .unwrap();
            writer
                .execute(
                    "INSERT OR REPLACE INTO parent (id, parent_id) VALUES (?, ?)",
                    rusqlite::params![1000 + a, a],
                )
                .unwrap();
            let got = snap.advance_without_diff().unwrap().to_string();
            assert_eq!(got, v);
        }
        snap.destroy();
    }

    // --- engine: hydrate + push churn + remove + destroy ---
    {
        let users = make_source("users", &["id"]);
        let posts = make_source("posts", &["id", "user_id"]);
        for i in 0..50i64 {
            let row: FxHashMap<String, Value> = [("id".to_string(), Value::F64(i as f64))]
                .into_iter()
                .collect();
            users.borrow_mut().add_row(row);
            let row: FxHashMap<String, Value> = [
                ("id".to_string(), Value::F64(i as f64)),
                ("user_id".to_string(), Value::F64((i % 10) as f64)),
            ]
            .into_iter()
            .collect();
            posts.borrow_mut().add_row(row);
        }
        let mut engine = Engine::new(HashMap::new());
        engine.register_source(users.clone());
        engine.register_source(posts.clone());
        for q in 0..4 {
            engine.add_queries(&[QuerySpec {
                query_id: format!("q{q}"),
                ast: joined_ast(),
            }]);
        }
        for i in 50..80i64 {
            let row: FxHashMap<String, Value> = [
                ("id".to_string(), Value::F64(i as f64)),
                ("user_id".to_string(), Value::F64((i % 10) as f64)),
            ]
            .into_iter()
            .collect();
            posts.borrow_mut().add_row(row);
        }
        for q in 0..4 {
            engine.remove_query(&format!("q{q}"));
        }
        engine.destroy();
    }

    // --- planner: real scanstatus cost model against the replica ---
    if rust_ivm::sqlite::sqlite_cost_model::scanstatus_available() {
        let conn = Rc::new(RefCell::new(rusqlite::Connection::open(db_path).unwrap()));
        let mut specs: HashMap<String, HashMap<String, ColumnType>> = HashMap::new();
        for t in ["parent", "child"] {
            let mut cols = HashMap::new();
            cols.insert("id".to_string(), ColumnType::Number { optional: false });
            cols.insert(
                "parent_id".to_string(),
                ColumnType::Number { optional: false },
            );
            specs.insert(t.to_string(), cols);
        }
        let ast = rust_ivm::replay::json_to_ast(&planner_exists_ast());
        for _ in 0..8 {
            let model = rust_ivm::sqlite::sqlite_cost_model::create_sqlite_cost_model(
                conn.clone(),
                specs.clone(),
            )
            .unwrap();
            let planned = rust_ivm::planner::plan_query(&ast, model);
            assert!(planned.where_clause.is_some());
        }
    }
}

#[test]
fn full_lifecycle_returns_heap_to_baseline() {
    let db_path = replica_path();

    // Warm-up: absorb one-time allocations (SQLite global caches, statics).
    for c in 0..3 {
        one_cycle(&db_path, c);
    }

    let baseline = live_bytes();
    let mut per_cycle = Vec::new();
    for c in 3..23 {
        let before = live_bytes();
        one_cycle(&db_path, c);
        per_cycle.push(live_bytes() - before);
    }
    let grown = live_bytes() - baseline;

    eprintln!(
        "[alloc-balance] baseline={} bytes, after 20 cycles delta={} bytes, per-cycle={:?}",
        baseline, grown, per_cycle
    );

    // Jitter budget: hashmap growth-doubling and allocator bookkeeping may
    // move a few KB; a real per-cycle leak (the planner class was ~hundreds
    // of KB per cycle) blows through this within one or two cycles.
    assert!(
        grown < 512 * 1024,
        "heap ratcheted {grown} bytes over 20 lifecycle cycles \
         (per-cycle deltas: {per_cycle:?}) — a logical leak"
    );

    let _ = std::fs::remove_file(&db_path);
}
