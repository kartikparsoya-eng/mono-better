//! Cross-CG parallel hydrate benchmark.
//! Spawns N engine instances (simulating N client groups) on separate threads,
//! each hydrating a query. Measures wall-clock time vs single-threaded baseline.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::collections::HashMap;
use std::thread;

use rust_ivm::builder::ast::Ast;
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::{MemorySource, Source};

fn make_engine_with_data(n_rows: usize) -> Engine {
    let mut pks = HashMap::new();
    pks.insert("t".to_string(), vec!["id".to_string()]);
    let mut eng = Engine::new(pks, 1);

    let mut cols = HashMap::new();
    cols.insert("id".to_string(), ColumnType::Number { optional: false });
    cols.insert("name".to_string(), ColumnType::String { optional: false });
    let source: Rc<RefCell<dyn Source>> = Rc::new(RefCell::new(
        MemorySource::new("t", cols, vec!["id".to_string()]),
    ));

    // Add rows via push
    for i in 0..n_rows {
        let mut row: rustc_hash::FxHashMap<String, Value> = rustc_hash::FxHashMap::default();
        row.insert("id".to_string(), Value::F64(i as f64));
        row.insert("name".to_string(), Value::Str(Arc::from(format!("row{}", i))));
        let sc = rust_ivm::ivm::change::make_source_change_add(Arc::new(row));
        let _ = source.borrow_mut().push(sc);
    }

    eng.register_source(source);
    eng
}

fn make_ast() -> Ast {
    Ast {
        schema: None,
        table: "t".to_string(),
        alias: None,
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: Some(vec![
            rust_ivm::builder::ast::OrderPart {
                column: "id".to_string(),
                direction: "asc".to_string(),
            },
        ]),
        start: None,
    }
}

fn hydrate_one_engine(n_rows: usize) -> usize {
    let mut eng = make_engine_with_data(n_rows);
    let specs = vec![QuerySpec {
        query_id: "q1".to_string(),
        ast: make_ast(),
    }];
    let results = eng.add_queries(&specs);
    results.iter().map(|r| r.changes.len()).sum()
}

#[test]
fn test_cross_cg_parallel_hydrate() {
    let n_cgs = 4;
    let n_rows = 1000;

    // Sequential baseline
    let seq_start = std::time::Instant::now();
    for _ in 0..n_cgs {
        let count = hydrate_one_engine(n_rows);
        assert_eq!(count, n_rows);
    }
    let seq_elapsed = seq_start.elapsed();

    // Parallel: each CG on its own thread
    let par_start = std::time::Instant::now();
    let handles: Vec<_> = (0..n_cgs)
        .map(|_| {
            thread::spawn(move || {
                let count = hydrate_one_engine(n_rows);
                assert_eq!(count, n_rows);
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let par_elapsed = par_start.elapsed();

    eprintln!(
        "Cross-CG parallel hydrate: {} CGs x {} rows: seq={:?} par={:?} speedup={:.2}x",
        n_cgs,
        n_rows,
        seq_elapsed,
        par_elapsed,
        seq_elapsed.as_secs_f64() / par_elapsed.as_secs_f64()
    );

    // Parallel should be faster (or at least not much slower) than sequential.
    // On a multicore machine, we expect speedup > 1.0.
    // We don't assert a hard threshold — this is a benchmark, not a correctness test.
    // But we do assert correctness: all rows hydrated in both paths.
}
