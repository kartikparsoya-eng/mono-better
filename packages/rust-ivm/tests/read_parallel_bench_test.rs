//! Microbench: parallel vs serial cold hydrate on a TableSource-backed Join.
//!
//! Creates a replica DB with N parent rows and M child rows, hydrates a
//! Join pipeline, and compares wall time with pool_lanes=0 (serial)
//! vs pool_lanes=2 (parallel). The join-batching path is exercised
//! because every parent needs a child fetch → N child SELECTs.
//!
//! Run with:
//!   cargo test --test read_parallel_bench_test -- --nocapture --ignored

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;

use std::collections::HashMap;
use rusqlite::Connection;

use rust_ivm::builder::ast::{Ast, RelatedSubquery};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::Source;
use rust_ivm::snapshotter::Snapshotter;
use rust_ivm::sqlite::table_source::TableSource;
use rust_ivm::streamer::RowChange;

/// Deterministic string for a RowChange (sorted keys) so serial and parallel
/// output can be compared byte-for-byte regardless of hash-map iteration order.
fn canon(rc: &RowChange) -> String {
    fn row_str(r: &rust_ivm::ivm::data::Row) -> String {
        let mut kv: Vec<(String, String)> =
            r.iter().map(|(k, v)| (k.clone(), format!("{:?}", v))).collect();
        kv.sort();
        format!("{:?}", kv)
    }
    format!(
        "ct={:?} q={} t={} key={} row={} hidden={}",
        rc.change_type,
        rc.query_id,
        rc.table,
        row_str(&rc.row_key),
        rc.row.as_ref().map(row_str).unwrap_or_default(),
        rc.is_hidden,
    )
}

fn create_replica(path: &str, num_parents: usize, children_per_parent: usize) {
    let conn = Connection::open(path).unwrap();
    let _: String = conn
        .query_row("PRAGMA journal_mode=WAL", [], |r| r.get(0))
        .unwrap();
    conn.execute_batch(
        "CREATE TABLE \"_zero.replicationState\" (stateVersion TEXT PRIMARY KEY);
         INSERT INTO \"_zero.replicationState\" (stateVersion) VALUES ('v1');
         CREATE TABLE parents (id TEXT PRIMARY KEY, name TEXT);
         CREATE TABLE children (id TEXT PRIMARY KEY, parent_id TEXT, name TEXT);",
    )
    .unwrap();

    for i in 0..num_parents {
        let pid = format!("p{}", i);
        conn.execute(
            "INSERT INTO parents (id, name) VALUES (?, ?)",
            [&pid, &format!("parent-{}", i)],
        )
        .unwrap();
        for j in 0..children_per_parent {
            let cid = format!("c{}_{}", i, j);
            conn.execute(
                "INSERT INTO children (id, parent_id, name) VALUES (?, ?, ?)",
                [&cid, &pid, &format!("child-{}-{}", i, j)],
            )
            .unwrap();
        }
    }
    drop(conn);
}

fn build_join_ast() -> Ast {
    Ast {
        table: "parents".to_string(),
        related: vec![RelatedSubquery {
            subquery: Box::new(Ast {
                table: "children".to_string(),
                ..Default::default()
            }),
            relationship_name: "children".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["parent_id".to_string()],
            hidden: false,
            system: Some(rust_ivm::ivm::schema::System::Client),
        }],
        ..Default::default()
    }
}

fn hydrate_bench(path: &str, pool_lanes: usize) -> std::time::Duration {
    hydrate_run(path, pool_lanes).0
}

/// Hydrate the join once, returning (wall time, canonical RowChange list in
/// emission order). `pool_lanes > 0` co-pins the read pool and exercises the
/// parallel Join-batching path; `0` is the serial baseline.
fn hydrate_run(path: &str, pool_lanes: usize) -> (std::time::Duration, Vec<String>) {
    let pks: HashMap<String, Vec<String>> = [
        ("parents".to_string(), vec!["id".to_string()]),
        ("children".to_string(), vec!["id".to_string()]),
    ]
    .into_iter()
    .collect();

    let mut snap = Snapshotter::with_read_pool(path, "bench", None, pool_lanes, None);
    snap.init().unwrap();
    let curr_conn = snap.current_conn().unwrap();

    let parent_columns: std::collections::HashMap<String, ColumnType> = [
        ("id".to_string(), ColumnType::String { optional: false }),
        ("name".to_string(), ColumnType::String { optional: false }),
    ]
    .into_iter()
    .collect();
    let child_columns: std::collections::HashMap<String, ColumnType> = [
        ("id".to_string(), ColumnType::String { optional: false }),
        ("parent_id".to_string(), ColumnType::String { optional: false }),
        ("name".to_string(), ColumnType::String { optional: false }),
    ]
    .into_iter()
    .collect();

    let mut parent_source = TableSource::new(
        curr_conn.clone(),
        "parents",
        parent_columns,
        vec!["id".to_string()],
    );
    if pool_lanes > 0 {
        parent_source.set_read_pool(snap.read_pool());
    }
    let mut child_source = TableSource::new(
        curr_conn.clone(),
        "children",
        child_columns,
        vec!["id".to_string()],
    );
    if pool_lanes > 0 {
        child_source.set_read_pool(snap.read_pool());
    }

    let mut eng = Engine::new(pks);
    eng.register_source(Rc::new(RefCell::new(parent_source)));
    eng.register_source(Rc::new(RefCell::new(child_source)));

    let ast = build_join_ast();
    let specs = vec![QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }];

    let start = Instant::now();
    let mut rows: Vec<String> = Vec::new();
    eng.add_queries_streaming(&specs, |rc| {
        rows.push(canon(rc));
    });
    let elapsed = start.elapsed();
    eprintln!(
        "  pool_lanes={}: {} rows in {:?}",
        pool_lanes, rows.len(), elapsed
    );
    (elapsed, rows)
}

/// The correctness gate for the parallel Join-batching path: its emitted
/// RowChanges must be byte-identical to the serial path.
#[test]
fn parallel_join_batch_matches_serial() {
    let path = "/tmp/rust-ivm-parallel-equiv.db";
    for p in [path, &format!("{}-wal", path), &format!("{}-shm", path)] {
        let _ = std::fs::remove_file(p);
    }
    // Duplicate join keys, empty-child parents, and >1 child/parent all exercised.
    create_replica(path, 25, 4);

    let (_, serial) = hydrate_run(path, 0);
    let (_, parallel) = hydrate_run(path, 3);

    assert!(!serial.is_empty(), "hydrate produced rows");
    assert_eq!(
        serial, parallel,
        "parallel Join-batch output must be byte-identical to serial"
    );

    for p in [path, &format!("{}-wal", path), &format!("{}-shm", path)] {
        let _ = std::fs::remove_file(p);
    }
}

#[test]
#[ignore]
fn bench_parallel_vs_serial() {
    let path = "/tmp/rust-ivm-bench-parallel.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path));
    let _ = std::fs::remove_file(format!("{}-shm", path));

    // 200 parents × 5 children = 1000 child rows → 200 child SELECTs in serial.
    create_replica(path, 200, 5);

    // Warm up (page cache).
    let _ = hydrate_bench(path, 0);
    let _ = hydrate_bench(path, 2);

    // Serial
    let serial = hydrate_bench(path, 0);
    let serial2 = hydrate_bench(path, 0);

    // Parallel
    let parallel = hydrate_bench(path, 2);
    let parallel2 = hydrate_bench(path, 2);

    eprintln!("\n=== Results ===");
    eprintln!("Serial:   {:?} / {:?}", serial, serial2);
    eprintln!("Parallel: {:?} / {:?}", parallel, parallel2);

    let best_serial = serial.min(serial2);
    let best_parallel = parallel.min(parallel2);
    eprintln!("Best serial:   {:?}", best_serial);
    eprintln!("Best parallel: {:?}", best_parallel);

    if best_parallel < best_serial {
        eprintln!(
            "✅ Parallel is {:.1}% faster",
            (1.0 - best_parallel.as_secs_f64() / best_serial.as_secs_f64()) * 100.0
        );
    } else {
        eprintln!(
            "⚠️  Parallel is {:.1}% SLOWER (keep RUST_IVM_READ_LANES=0 default)",
            (best_parallel.as_secs_f64() / best_serial.as_secs_f64() - 1.0) * 100.0
        );
    }

    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path));
    let _ = std::fs::remove_file(format!("{}-shm", path));
}
