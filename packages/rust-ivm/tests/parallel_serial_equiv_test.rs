// Parallel ≡ serial oracle (DESIGN §6): for every fixture, run hydrate through
// BOTH the serial path (Engine::add_queries_streaming) and the parallel path
// (Engine::parallel_add_queries_streaming), and assert the RowChange streams
// are byte-identical. Parallel hydrate is read-only ⇒ result-preserving; any
// divergence is a bug in the parallel path, never in the serial oracle.

use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::change::ChangeType;
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::memory_storage::MemoryStorage;
use rust_ivm::ivm::source::{MemorySource, ParallelSourceSpec, Source};
use rust_ivm::replay::{json_to_ast, json_to_rust_value, parse_column_type, push_to_source_change};
use rust_ivm::streamer::RowChange;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use rustc_hash::FxHashMap;
use rust_ivm::ivm::schema::ColumnType;

/// Build a fresh Engine from a fixture, with MemorySources for each table.
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
                let tstr = type_spec.as_str().unwrap_or("string");
                columns.insert(col.clone(), parse_column_type(tstr));
            }
        }
        let pk: Vec<String> = spec
            .get("primaryKey")
            .and_then(|p| p.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(String::from))
                    .collect()
            })
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

/// Serialize a RowChange to a comparable JSON value (matches the oracle format).
fn row_change_to_json(rc: &RowChange) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    m.insert(
        "changeType".into(),
        serde_json::Value::String(format!("{:?}", rc.change_type)),
    );
    m.insert("queryId".into(), serde_json::Value::String(rc.query_id.clone()));
    m.insert("table".into(), serde_json::Value::String(rc.table.clone()));
    m.insert(
        "rowKey".into(),
        serde_json::Value::Array(
            rc.row_key
                .iter()
                .map(|(k, v)| {
                    let mut pair = serde_json::Map::new();
                    pair.insert("column".into(), serde_json::Value::String(k.clone()));
                    pair.insert("value".into(), rust_ivm::replay::rust_value_to_json(v));
                    serde_json::Value::Object(pair)
                })
                .collect(),
        ),
    );
    m.insert(
        "isHidden".into(),
        serde_json::Value::Bool(rc.is_hidden),
    );
    if let Some(ref row) = rc.row {
        m.insert("row".into(), rust_ivm::replay::row_to_json(row));
    }
    serde_json::Value::Object(m)
}

/// Run serial hydrate + advances, collecting all RowChanges as JSON.
fn run_serial(fixture: &serde_json::Value) -> Vec<serde_json::Value> {
    let mut eng = build_engine(fixture);
    let ast = json_to_ast(fixture.get("ast").unwrap_or(&serde_json::Value::Null));
    let query_id = fixture
        .get("queryId")
        .and_then(|q| q.as_str())
        .unwrap_or("q1")
        .to_string();

    let mut collected: Vec<serde_json::Value> = Vec::new();
    let queries = vec![QuerySpec {
        query_id: query_id.clone(),
        ast,
    }];
    eng.add_queries_streaming(&queries, |rc| {
        collected.push(row_change_to_json(rc));
    });

    // Run pushes (advances).
    let pushes = fixture
        .get("pushes")
        .and_then(|p| p.as_array())
        .cloned()
        .unwrap_or_default();
    for push in &pushes {
        let (table, sc) = push_to_source_change(push);
        eng.advance_streaming(&[(table, sc)], |rc| {
            collected.push(row_change_to_json(rc));
        });
    }

    collected
}

/// Run parallel hydrate + advances, collecting all RowChanges as JSON.
fn run_parallel(fixture: &serde_json::Value, workers: usize, bound: usize) -> Vec<serde_json::Value> {
    let mut eng = build_engine(fixture);
    let ast = json_to_ast(fixture.get("ast").unwrap_or(&serde_json::Value::Null));
    let query_id = fixture
        .get("queryId")
        .and_then(|q| q.as_str())
        .unwrap_or("q1")
        .to_string();

    let mut collected: Vec<serde_json::Value> = Vec::new();
    let queries = vec![QuerySpec {
        query_id: query_id.clone(),
        ast,
    }];
    let _ = eng.parallel_add_queries_streaming(&queries, workers, bound, |rc| {
        collected.push(row_change_to_json(rc));
    });

    // Run pushes (advances) — serial, same as the serial path.
    let pushes = fixture
        .get("pushes")
        .and_then(|p| p.as_array())
        .cloned()
        .unwrap_or_default();
    for push in &pushes {
        let (table, sc) = push_to_source_change(push);
        eng.advance_streaming(&[(table, sc)], |rc| {
            collected.push(row_change_to_json(rc));
        });
    }

    collected
}

#[test]
fn parallel_equiv_serial_all_fixtures() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("agentic/fixtures");
    if !dir.exists() {
        eprintln!("parallel_equiv: no fixtures dir");
        return;
    }
    let mut inputs: Vec<PathBuf> = fs::read_dir(&dir)
        .expect("read fixtures dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .map(|n| n.to_string_lossy().ends_with(".input.json"))
                .unwrap_or(false)
        })
        .collect();
    inputs.sort();

    let mut failures: Vec<String> = Vec::new();
    let mut ran = 0usize;
    for input in &inputs {
        let content = match fs::read_to_string(input) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let fixture: serde_json::Value = match serde_json::from_str(&content) {
            Ok(v) => v,
            Err(_) => continue,
        };

        // Skip fixtures with no tables (degenerate).
        if fixture.get("tables").and_then(|t| t.as_object()).map(|o| o.is_empty()).unwrap_or(true) {
            continue;
        }

        ran += 1;
        let serial = run_serial(&fixture);
        let parallel = run_parallel(&fixture, 2, 4);

        if serial != parallel {
            // Find first divergence for the error message.
            let mut diff_msg = String::new();
            for (i, (s, p)) in serial.iter().zip(parallel.iter()).enumerate() {
                if s != p {
                    diff_msg = format!(
                        "  first divergence at index {}:\n    serial:   {}\n    parallel: {}",
                        i,
                        serde_json::to_string_pretty(s).unwrap_or_default(),
                        serde_json::to_string_pretty(p).unwrap_or_default()
                    );
                    break;
                }
            }
            if diff_msg.is_empty() {
                diff_msg = format!(
                    "  length mismatch: serial={} parallel={}",
                    serial.len(),
                    parallel.len()
                );
            }
            failures.push(format!(
                "{}\n{}",
                input.file_name().unwrap().to_string_lossy(),
                diff_msg
            ));
        }
    }

    eprintln!(
        "parallel_equiv: {} fixtures compared, {} diverged",
        ran,
        failures.len()
    );
    assert!(
        failures.is_empty(),
        "parallel ≠ serial in {} fixture(s):\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
