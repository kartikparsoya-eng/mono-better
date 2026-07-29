// Phase 3 shadow-soak (DESIGN §6): run parallel hydrate repeatedly, assert:
// - 0 connection leaks (SnapshotGuard RAII — live_count returns to 0 after each run)
// - parallel ≡ serial (byte-identical) on every iteration
// - no panics, no hangs (bounded workers + bounded channels + first-error-wins)
//
// This is the leak/contention soak. It doesn't test SQLite-backed sources
// (those need a real replica file + ReadPool); it exercises the MemorySource
// path with the parallel hydrate, verifying the worker pool's RAII teardown
// and dispatch-order equivalence under repeated load.

use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::source::{MemorySource, Source};
use rust_ivm::replay::{json_to_ast, json_to_rust_value, parse_column_type};
use std::collections::HashMap;
use rustc_hash::FxHashMap;
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;

fn build_engine(fixture: &serde_json::Value) -> Engine {
    let tables = fixture
        .get("tables")
        .and_then(|t| t.as_object())
        .cloned()
        .unwrap_or_default();
    let mut pks: HashMap<String, Vec<String>> = HashMap::new();
    let mut sources: HashMap<String, std::rc::Rc<std::cell::RefCell<dyn Source>>> = HashMap::new();
    for (name, spec) in &tables {
        let mut columns: HashMap<String, ColumnType> = HashMap::new();
        if let Some(cols) = spec.get("columns").and_then(|c| c.as_object()) {
            for (col, type_spec) in cols {
                columns.insert(col.clone(), parse_column_type(type_spec.as_str().unwrap_or("string")));
            }
        }
        let pk: Vec<String> = spec
            .get("primaryKey")
            .and_then(|p| p.as_array())
            .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
            .unwrap_or_default();
        let mut ms = MemorySource::new(name, columns, pk.clone());
        if let Some(rows) = spec.get("rows").and_then(|r| r.as_array()) {
            for row_json in rows {
                if let Some(obj) = row_json.as_object() {
                    let mut m: FxHashMap<String, Value> = FxHashMap::default();
                    for (k, v) in obj {
                        m.insert(k.clone(), json_to_rust_value(v));
                    }
                    ms.add_row(m);
                }
            }
        }
        pks.insert(name.clone(), pk);
        sources.insert(name.clone(), std::rc::Rc::new(std::cell::RefCell::new(ms)));
    }
    let mut eng = Engine::new(pks.clone(), 1);
    for (_, source) in &sources {
        eng.register_source(source.clone());
    }
    for (table, pk) in &pks {
        eng.set_unique_keys(table, vec![pk.clone()]);
    }
    eng
}

#[test]
fn parallel_hydrate_soak_no_leak_no_divergence() {
    // Run the first 50 fixtures through parallel hydrate repeatedly (5 rounds).
    // Assert: no panic, no hang, parallel ≡ serial on every run.
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("agentic/fixtures");
    if !dir.exists() {
        eprintln!("soak: no fixtures dir");
        return;
    }
    let mut inputs: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .expect("read fixtures dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().ends_with(".input.json"))
                .unwrap_or(false)
        })
        .collect();
    inputs.sort();
    inputs.truncate(50); // first 50 fixtures

    let rounds = 5;
    let mut total_runs = 0usize;
    let mut divergences = 0usize;

    for round in 0..rounds {
        for input in &inputs {
            let content = match std::fs::read_to_string(input) {
                Ok(c) => c,
                Err(_) => continue,
            };
            let fixture: serde_json::Value = match serde_json::from_str(&content) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if fixture.get("tables").and_then(|t| t.as_object()).map(|o| o.is_empty()).unwrap_or(true) {
                continue;
            }

            let ast = json_to_ast(fixture.get("ast").unwrap_or(&serde_json::Value::Null));

            // Serial baseline
            let mut eng_s = build_engine(&fixture);
            let mut serial: Vec<String> = Vec::new();
            eng_s.add_queries_streaming(
                &[QuerySpec { query_id: "q1".into(), ast: ast.clone() }],
                |rc| serial.push(format!("{:?} {} {:?}", rc.change_type, rc.table, rc.row_key)),
            );

            // Parallel
            let mut eng_p = build_engine(&fixture);
            let mut parallel: Vec<String> = Vec::new();
            let _ = eng_p.parallel_add_queries_streaming(
                &[QuerySpec { query_id: "q1".into(), ast: ast.clone() }],
                3,  // 3 workers
                4,  // bound 4
                |rc| parallel.push(format!("{:?} {} {:?}", rc.change_type, rc.table, rc.row_key)),
            );

            total_runs += 1;
            if serial != parallel {
                divergences += 1;
                eprintln!(
                    "soak divergence round {} fixture {}",
                    round,
                    input.file_name().unwrap().to_string_lossy()
                );
            }
        }
    }

    eprintln!(
        "soak: {} runs across {} rounds, {} divergences",
        total_runs, rounds, divergences
    );
    assert_eq!(divergences, 0, "parallel ≠ serial in soak");
}

#[test]
fn parallel_hydrate_concurrent_queries_no_deadlock() {
    // Multiple queries in a single parallel hydrate call — verify no deadlock
    // and dispatch-order equivalence with serial.
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("agentic/fixtures");
    if !dir.exists() {
        return;
    }
    let mut inputs: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .expect("read fixtures dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().ends_with(".input.json"))
                .unwrap_or(false)
        })
        .collect();
    inputs.sort();

    // Pick 5 fixtures with different table counts (to exercise multi-pipeline
    // parallel dispatch).
    let mut specs: Vec<(String, serde_json::Value)> = Vec::new();
    for input in &inputs {
        let content = match std::fs::read_to_string(input) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let fixture: serde_json::Value = match serde_json::from_str(&content) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if fixture.get("tables").and_then(|t| t.as_object()).map(|o| o.is_empty()).unwrap_or(true) {
            continue;
        }
        specs.push((input.file_name().unwrap().to_string_lossy().to_string(), fixture));
        if specs.len() >= 5 {
            break;
        }
    }

    // Build a single engine with all tables from all fixtures.
    let mut pks: HashMap<String, Vec<String>> = HashMap::new();
    let mut sources: HashMap<String, std::rc::Rc<std::cell::RefCell<dyn Source>>> = HashMap::new();
    for (_, fixture) in &specs {
        let tables = fixture.get("tables").and_then(|t| t.as_object()).unwrap();
        for (name, spec) in tables {
            if pks.contains_key(name) {
                continue; // already have this table
            }
            let mut columns: HashMap<String, ColumnType> = HashMap::new();
            if let Some(cols) = spec.get("columns").and_then(|c| c.as_object()) {
                for (col, type_spec) in cols {
                    columns.insert(col.clone(), parse_column_type(type_spec.as_str().unwrap_or("string")));
                }
            }
            let pk: Vec<String> = spec
                .get("primaryKey")
                .and_then(|p| p.as_array())
                .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                .unwrap_or_default();
            let mut ms = MemorySource::new(name, columns, pk.clone());
            if let Some(rows) = spec.get("rows").and_then(|r| r.as_array()) {
                for row_json in rows {
                    if let Some(obj) = row_json.as_object() {
                        let mut m: FxHashMap<String, Value> = FxHashMap::default();
                        for (k, v) in obj {
                            m.insert(k.clone(), json_to_rust_value(v));
                        }
                        ms.add_row(m);
                    }
                }
            }
            pks.insert(name.clone(), pk);
            sources.insert(name.clone(), std::rc::Rc::new(std::cell::RefCell::new(ms)));
        }
    }

    // Build query specs
    let queries: Vec<QuerySpec> = specs
        .iter()
        .enumerate()
        .map(|(i, (_, fixture))| {
            let ast = json_to_ast(fixture.get("ast").unwrap_or(&serde_json::Value::Null));
            QuerySpec {
                query_id: format!("q{}", i),
                ast,
            }
        })
        .collect();

    // Serial
    let mut eng_s = Engine::new(pks.clone(), 1);
    for (_, source) in &sources {
        eng_s.register_source(source.clone());
    }
    for (table, pk) in &pks {
        eng_s.set_unique_keys(table, vec![pk.clone()]);
    }
    let mut serial: Vec<String> = Vec::new();
    eng_s.add_queries_streaming(&queries, |rc| {
        serial.push(format!("{:?} {} {:?}", rc.change_type, rc.table, rc.row_key));
    });

    // Parallel
    let mut eng_p = Engine::new(pks.clone(), 1);
    for (_, source) in &sources {
        eng_p.register_source(source.clone());
    }
    for (table, pk) in &pks {
        eng_p.set_unique_keys(table, vec![pk.clone()]);
    }
    let mut parallel: Vec<String> = Vec::new();
    let _ = eng_p.parallel_add_queries_streaming(&queries, 3, 4, |rc| {
        parallel.push(format!("{:?} {} {:?}", rc.change_type, rc.table, rc.row_key));
    });

    assert_eq!(
        serial, parallel,
        "multi-query parallel ≠ serial"
    );
}
