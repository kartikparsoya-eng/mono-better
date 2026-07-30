//! HTTP server binary for the Rust IVM engine.
//!
//! Exposes the engine via a JSON HTTP API for testing with ART.
//! Single-threaded — the Engine uses Rc<RefCell<>> (not Send/Sync),
//! matching the TS single-threaded event loop model.
//!
//! Endpoints:
//!   GET  /health           — health check
//!   GET  /version          — version info
//!   POST /init             — initialize with table schemas
//!   POST /add-queries      — add queries and hydrate
//!   POST /advance          — push source changes through pipelines
//!   POST /add-row          — add a row to a source (in-memory)
//!   POST /remove-query     — remove a query's pipeline
//!   POST /destroy          — destroy the engine
//!   GET  /queries          — list active query IDs
//!   GET  /sources          — list registered source tables

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;
use serde_json::Value as JsonValue;
use tiny_http::{Header, Method, Response, Server};

use rust_ivm::builder::ast::{
    Ast, Bound, Condition, CorrelatedSubqueryCondition, OrderPart, RelatedSubquery,
    SimpleCondition, ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::change::{
    ChangeType, SourceChange, make_source_change_add, make_source_change_edit,
    make_source_change_remove,
};
use rust_ivm::ivm::data::{Row, Value, row as make_row};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;

// ---------------------------------------------------------------------------
// Value / Row conversion: serde_json ↔ Rust
// ---------------------------------------------------------------------------

fn json_to_rust_value(v: &JsonValue) -> Value {
    match v {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Bool(*b),
        JsonValue::Number(n) => Value::F64(n.as_f64().unwrap_or(0.0)),
        JsonValue::String(s) => Value::Str(Arc::from(s.as_str())),
        JsonValue::Object(_) | JsonValue::Array(_) => {
            Value::Json(Arc::from(v.to_string().as_str()))
        }
    }
}

fn rust_value_to_json(v: &Value) -> JsonValue {
    match v {
        Value::Null => JsonValue::Null,
        Value::Bool(b) => JsonValue::Bool(*b),
        Value::F64(n) => {
            if n.fract() == 0.0 && n.is_finite() && *n >= i64::MIN as f64 && *n <= i64::MAX as f64 {
                JsonValue::Number(serde_json::Number::from(*n as i64))
            } else {
                JsonValue::Number(
                    serde_json::Number::from_f64(*n).unwrap_or_else(|| serde_json::Number::from(0)),
                )
            }
        }
        Value::Str(s) => JsonValue::String(s.to_string()),
        Value::Json(s) => {
            serde_json::from_str(s).unwrap_or_else(|_| JsonValue::String(s.to_string()))
        }
    }
}

fn json_to_row(obj: &serde_json::Map<String, JsonValue>) -> Row {
    let mut map: FxHashMap<String, Value> = FxHashMap::default();
    for (k, v) in obj {
        map.insert(k.clone(), json_to_rust_value(v));
    }
    make_row(map)
}

fn row_to_json(row: &Row) -> JsonValue {
    let mut map = serde_json::Map::new();
    for (k, v) in row.iter() {
        map.insert(k.clone(), rust_value_to_json(v));
    }
    JsonValue::Object(map)
}

// ---------------------------------------------------------------------------
// AST conversion: JSON → Rust Ast (adapted from NAPI addon)
// ---------------------------------------------------------------------------

fn json_to_value_position(v: &JsonValue) -> ValuePosition {
    let kind = v.get("type").and_then(|t| t.as_str()).unwrap_or("literal");
    match kind {
        "column" => {
            let name = v.get("name").and_then(|n| n.as_str()).unwrap_or("");
            ValuePosition::Column {
                name: name.to_string(),
            }
        }
        _ => {
            let val = v
                .get("value")
                .map(json_to_rust_value)
                .unwrap_or(Value::Null);
            ValuePosition::Literal { value: val }
        }
    }
}

fn json_to_simple_condition(v: &JsonValue) -> SimpleCondition {
    SimpleCondition {
        op: v
            .get("op")
            .and_then(|o| o.as_str())
            .unwrap_or("=")
            .to_string(),
        left: json_to_value_position(v.get("left").unwrap_or(&JsonValue::Null)),
        right: json_to_value_position(v.get("right").unwrap_or(&JsonValue::Null)),
    }
}

fn json_to_condition(v: &JsonValue) -> Condition {
    let kind = v.get("type").and_then(|t| t.as_str()).unwrap_or("simple");
    match kind {
        "simple" => Condition::Simple(json_to_simple_condition(v)),
        "and" => {
            let conds: Vec<Condition> = v
                .get("conditions")
                .and_then(|c| c.as_array())
                .unwrap_or(&vec![])
                .iter()
                .map(json_to_condition)
                .collect();
            Condition::And(conds)
        }
        "or" => {
            let conds: Vec<Condition> = v
                .get("conditions")
                .and_then(|c| c.as_array())
                .unwrap_or(&vec![])
                .iter()
                .map(json_to_condition)
                .collect();
            Condition::Or(conds)
        }
        "correlatedSubquery" => {
            let related = json_to_related_subquery(v.get("related").unwrap_or(&JsonValue::Null));
            let op = v
                .get("op")
                .and_then(|o| o.as_str())
                .unwrap_or("EXISTS")
                .to_string();
            let flip = v.get("flip").and_then(|f| f.as_bool());
            let scalar = v.get("scalar").and_then(|s| s.as_bool()).unwrap_or(false);
            Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
                related,
                op,
                flip,
                scalar,
                plan_id: None,
            })
        }
        _ => panic!("Unknown condition type: {}", kind),
    }
}

fn json_to_related_subquery(v: &JsonValue) -> RelatedSubquery {
    let subquery = json_to_ast(v.get("subquery").unwrap_or(&JsonValue::Null));
    let relationship_name = v
        .get("alias")
        .and_then(|a| a.as_str())
        .unwrap_or("")
        .to_string();

    let (parent_key, child_key) = if let Some(corr) = v.get("correlation") {
        let parent = corr
            .get("parentField")
            .and_then(|p| p.as_array())
            .map(|a| {
                a.iter()
                    .map(|s| s.as_str().unwrap_or("").to_string())
                    .collect()
            })
            .unwrap_or_default();
        let child = corr
            .get("childField")
            .and_then(|c| c.as_array())
            .map(|a| {
                a.iter()
                    .map(|s| s.as_str().unwrap_or("").to_string())
                    .collect()
            })
            .unwrap_or_default();
        (parent, child)
    } else {
        (vec![], vec![])
    };

    let hidden = v.get("hidden").and_then(|h| h.as_bool()).unwrap_or(false);
    let system = v.get("system").and_then(|s| s.as_str()).map(|s| match s {
        "permissions" => rust_ivm::ivm::schema::System::Permissions,
        "test" => rust_ivm::ivm::schema::System::Test,
        _ => rust_ivm::ivm::schema::System::Client,
    });

    RelatedSubquery {
        subquery: Box::new(subquery),
        relationship_name,
        parent_key,
        child_key,
        hidden,
        system,
    }
}

fn json_to_ast(v: &JsonValue) -> Ast {
    let table = v
        .get("table")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    let alias = v
        .get("alias")
        .and_then(|a| a.as_str())
        .map(|s| s.to_string());
    let where_clause = v.get("where").map(json_to_condition);

    let related: Vec<RelatedSubquery> = v
        .get("related")
        .and_then(|r| r.as_array())
        .unwrap_or(&vec![])
        .iter()
        .map(json_to_related_subquery)
        .collect();

    let limit = v.get("limit").and_then(|l| l.as_i64()).map(|l| l as usize);

    let order_by = v.get("orderBy").and_then(|o| o.as_array()).map(|parts| {
        parts
            .iter()
            .map(|p| {
                let empty_arr = vec![];
                let arr = p.as_array().unwrap_or(&empty_arr);
                OrderPart {
                    column: arr
                        .first()
                        .and_then(|c| c.as_str())
                        .unwrap_or("")
                        .to_string(),
                    direction: arr
                        .get(1)
                        .and_then(|d| d.as_str())
                        .unwrap_or("asc")
                        .to_string(),
                }
            })
            .collect()
    });

    let start = v.get("start").map(|s| {
        let row_json = s.get("row").unwrap_or(&JsonValue::Null);
        let mut map: FxHashMap<String, Value> = FxHashMap::default();
        if let Some(obj) = row_json.as_object() {
            for (k, val) in obj {
                map.insert(k.clone(), json_to_rust_value(val));
            }
        }
        Bound {
            row: make_row(map),
            exclusive: s
                .get("exclusive")
                .and_then(|e| e.as_bool())
                .unwrap_or(false),
        }
    });

    Ast {
        schema: None,
        table,
        alias,
        where_clause,
        related,
        limit,
        order_by,
        start,
    }
}

// ---------------------------------------------------------------------------
// RowChange → JSON
// ---------------------------------------------------------------------------

fn change_type_str(ct: ChangeType) -> &'static str {
    match ct {
        ChangeType::Add => "add",
        ChangeType::Remove => "remove",
        ChangeType::Edit => "edit",
        ChangeType::Child => "child",
    }
}

fn row_change_to_json(rc: &rust_ivm::streamer::RowChange) -> JsonValue {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "type".into(),
        JsonValue::String(change_type_str(rc.change_type).into()),
    );
    obj.insert("query_id".into(), JsonValue::String(rc.query_id.clone()));
    obj.insert("table".into(), JsonValue::String(rc.table.clone()));
    obj.insert("row_key".into(), row_to_json(&rc.row_key));
    obj.insert(
        "row".into(),
        match &rc.row {
            Some(r) => row_to_json(r),
            None => JsonValue::Null,
        },
    );
    JsonValue::Object(obj)
}

// ---------------------------------------------------------------------------
// Server state
// ---------------------------------------------------------------------------

struct ServerState {
    engine: Engine,
    sources: HashMap<String, Rc<RefCell<MemorySource>>>,
}

// ---------------------------------------------------------------------------
// Request handlers
// ---------------------------------------------------------------------------

fn handle_health() -> Response<std::io::Cursor<Vec<u8>>> {
    json_response(200, &serde_json::json!({"status": "ok"}))
}

fn handle_version() -> Response<std::io::Cursor<Vec<u8>>> {
    json_response(
        200,
        &serde_json::json!({
            "version": "0.1.0",
            "protocol_rev": 12
        }),
    )
}

fn handle_init(state: &mut ServerState, body: &JsonValue) -> Response<std::io::Cursor<Vec<u8>>> {
    let tables = match body.get("tables").and_then(|t| t.as_object()) {
        Some(t) => t,
        None => return error_response(400, "Missing 'tables' field"),
    };

    for (name, schema) in tables {
        let columns_json = schema.get("columns").and_then(|c| c.as_object());
        let pk: Vec<String> = schema
            .get("primary_key")
            .and_then(|p| p.as_array())
            .map(|a| {
                a.iter()
                    .map(|s| s.as_str().unwrap_or("").to_string())
                    .collect()
            })
            .unwrap_or_default();

        let mut columns = HashMap::new();
        if let Some(cols) = columns_json {
            for (col, spec) in cols {
                let type_str = spec
                    .get("type")
                    .and_then(|t| t.as_str())
                    .unwrap_or("number");
                let optional = spec
                    .get("optional")
                    .and_then(|o| o.as_bool())
                    .unwrap_or(false);
                let ct = match type_str {
                    "boolean" => ColumnType::Boolean { optional },
                    "number" => ColumnType::Number { optional },
                    "string" => ColumnType::String { optional },
                    "json" => ColumnType::Json { optional },
                    _ => ColumnType::Number { optional },
                };
                columns.insert(col.clone(), ct);
            }
        }

        let source = Rc::new(RefCell::new(MemorySource::new(name, columns, pk.clone())));
        state
            .engine
            .register_source(source.clone() as Rc<RefCell<dyn rust_ivm::ivm::source::Source>>);
        state.sources.insert(name.clone(), source);

        if let Some(mrv) = schema.get("min_row_version").and_then(|m| m.as_str()) {
            state.engine.set_table_spec(name, Some(mrv.to_string()));
        }
    }

    json_response(200, &serde_json::json!({"ok": true}))
}

fn handle_add_queries(
    state: &mut ServerState,
    body: &JsonValue,
) -> Response<std::io::Cursor<Vec<u8>>> {
    let queries = match body.get("queries").and_then(|q| q.as_array()) {
        Some(q) => q,
        None => return error_response(400, "Missing 'queries' field"),
    };

    let specs: Vec<QuerySpec> = queries
        .iter()
        .map(|q| QuerySpec {
            query_id: q
                .get("query_id")
                .and_then(|q| q.as_str())
                .unwrap_or("")
                .to_string(),
            ast: json_to_ast(q.get("ast").unwrap_or(&JsonValue::Null)),
        })
        .collect();

    let results = state.engine.add_queries(&specs);

    let json_results: Vec<JsonValue> = results
        .iter()
        .map(|r| {
            let changes: Vec<JsonValue> = r.changes.iter().map(row_change_to_json).collect();
            serde_json::json!({
                "query_id": r.query_id,
                "changes": changes,
            })
        })
        .collect();

    json_response(200, &serde_json::json!({"results": json_results}))
}

fn handle_advance(state: &mut ServerState, body: &JsonValue) -> Response<std::io::Cursor<Vec<u8>>> {
    let changes = match body.get("changes").and_then(|c| c.as_array()) {
        Some(c) => c,
        None => return error_response(400, "Missing 'changes' field"),
    };

    let rust_changes: Vec<(String, SourceChange)> = changes
        .iter()
        .map(|c| {
            let table = c
                .get("table")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_string();
            let change_type = c.get("type").and_then(|t| t.as_str()).unwrap_or("add");
            let empty = serde_json::Map::new();
            let row = c.get("row").and_then(|r| r.as_object()).unwrap_or(&empty);
            let sc = match change_type {
                "add" => make_source_change_add(json_to_row(row)),
                "remove" => make_source_change_remove(json_to_row(row)),
                "edit" => {
                    let old_row = c
                        .get("old_row")
                        .and_then(|r| r.as_object())
                        .unwrap_or(&empty);
                    make_source_change_edit(json_to_row(row), json_to_row(old_row))
                }
                _ => panic!("Unknown change type: {}", change_type),
            };
            (table, sc)
        })
        .collect();

    let row_changes = state.engine.advance(&rust_changes);
    let json_changes: Vec<JsonValue> = row_changes.iter().map(row_change_to_json).collect();

    json_response(200, &serde_json::json!({"changes": json_changes}))
}

fn handle_add_row(state: &mut ServerState, body: &JsonValue) -> Response<std::io::Cursor<Vec<u8>>> {
    let table = match body.get("table").and_then(|t| t.as_str()) {
        Some(t) => t.to_string(),
        None => return error_response(400, "Missing 'table' field"),
    };
    let row_json = match body.get("row").and_then(|r| r.as_object()) {
        Some(r) => r,
        None => return error_response(400, "Missing 'row' field"),
    };

    match state.sources.get(&table) {
        Some(source) => {
            let mut map: FxHashMap<String, Value> = FxHashMap::default();
            for (k, v) in row_json {
                map.insert(k.clone(), json_to_rust_value(v));
            }
            source.borrow_mut().add_row(map);
            json_response(200, &serde_json::json!({"ok": true}))
        }
        None => error_response(404, &format!("Source '{}' not found", table)),
    }
}

fn handle_remove_query(
    state: &mut ServerState,
    body: &JsonValue,
) -> Response<std::io::Cursor<Vec<u8>>> {
    let query_id = match body.get("query_id").and_then(|q| q.as_str()) {
        Some(q) => q.to_string(),
        None => return error_response(400, "Missing 'query_id' field"),
    };
    state.engine.remove_query(&query_id);
    json_response(200, &serde_json::json!({"ok": true}))
}

fn handle_destroy(state: &mut ServerState) -> Response<std::io::Cursor<Vec<u8>>> {
    state.engine.destroy();
    state.sources.clear();
    json_response(200, &serde_json::json!({"ok": true}))
}

fn handle_queries(state: &ServerState) -> Response<std::io::Cursor<Vec<u8>>> {
    let queries: Vec<JsonValue> = state
        .engine
        .pipeline_query_ids()
        .into_iter()
        .map(JsonValue::String)
        .collect();
    json_response(200, &serde_json::json!({"queries": queries}))
}

fn handle_sources(state: &ServerState) -> Response<std::io::Cursor<Vec<u8>>> {
    let sources: Vec<JsonValue> = state
        .sources
        .keys()
        .cloned()
        .map(JsonValue::String)
        .collect();
    json_response(200, &serde_json::json!({"sources": sources}))
}

// ---------------------------------------------------------------------------
// Streaming endpoint (Phase 1 — chunked NDJSON frames)
// ---------------------------------------------------------------------------

fn handle_add_queries_stream(
    state: &mut ServerState,
    body: &JsonValue,
) -> Response<std::io::Cursor<Vec<u8>>> {
    let queries = match body.get("queries").and_then(|q| q.as_array()) {
        Some(q) => q,
        None => return error_response(400, "Missing 'queries' field"),
    };

    let specs: Vec<QuerySpec> = queries
        .iter()
        .map(|q| QuerySpec {
            query_id: q
                .get("query_id")
                .and_then(|q| q.as_str())
                .unwrap_or("")
                .to_string(),
            ast: json_to_ast(q.get("ast").unwrap_or(&JsonValue::Null)),
        })
        .collect();

    // Use the Chunker to batch rows into frames, collect as NDJSON.
    let sink = rust_ivm::streamer::CollectSink::new();
    let mut chunker = rust_ivm::streamer::Chunker::new(sink, 64);

    let results = state.engine.add_queries_streaming(&specs, |rc| {
        chunker.push_row_change(&rc.query_id, rc.clone());
    });

    // Emit Final for each completed query.
    for r in &results {
        chunker.flush_query(&r.query_id);
    }
    chunker.done();

    let sink = chunker.into_sink();
    let mut ndjson = String::new();
    for frame in &sink.frames {
        let json = match frame {
            rust_ivm::streamer::StreamFrame::Partial {
                chunk_index,
                query_id,
                changes,
            } => {
                let changes_json: Vec<JsonValue> = changes.iter().map(row_change_to_json).collect();
                serde_json::json!({
                    "type": "partial",
                    "chunkIndex": chunk_index,
                    "queryId": query_id,
                    "changes": changes_json,
                })
            }
            rust_ivm::streamer::StreamFrame::Final {
                chunk_index,
                query_id,
            } => {
                serde_json::json!({
                    "type": "final",
                    "chunkIndex": chunk_index,
                    "queryId": query_id,
                })
            }
            rust_ivm::streamer::StreamFrame::Done { chunk_index } => {
                serde_json::json!({
                    "type": "done",
                    "chunkIndex": chunk_index,
                })
            }
            rust_ivm::streamer::StreamFrame::Error {
                chunk_index,
                message,
            } => {
                serde_json::json!({
                    "type": "error",
                    "chunkIndex": chunk_index,
                    "message": message,
                })
            }
        };
        ndjson.push_str(&json.to_string());
        ndjson.push('\n');
    }

    let mut response = Response::from_string(ndjson).with_status_code(200);
    if let Ok(header) = Header::from_bytes(b"Content-Type", b"application/x-ndjson") {
        response = response.with_header(header);
    }
    if let Ok(header) = Header::from_bytes(b"Access-Control-Allow-Origin", b"*") {
        response = response.with_header(header);
    }
    response
}

fn handle_advance_stream(
    state: &mut ServerState,
    body: &JsonValue,
) -> Response<std::io::Cursor<Vec<u8>>> {
    let changes = match body.get("changes").and_then(|c| c.as_array()) {
        Some(c) => c,
        None => return error_response(400, "Missing 'changes' field"),
    };

    let rust_changes: Vec<(String, SourceChange)> = changes
        .iter()
        .map(|c| {
            let table = c
                .get("table")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_string();
            let change_type = c.get("type").and_then(|t| t.as_str()).unwrap_or("add");
            let empty = serde_json::Map::new();
            let row = c.get("row").and_then(|r| r.as_object()).unwrap_or(&empty);
            let sc = match change_type {
                "add" => make_source_change_add(json_to_row(row)),
                "remove" => make_source_change_remove(json_to_row(row)),
                "edit" => {
                    let old_row = c
                        .get("old_row")
                        .and_then(|r| r.as_object())
                        .unwrap_or(&empty);
                    make_source_change_edit(json_to_row(row), json_to_row(old_row))
                }
                _ => panic!("Unknown change type: {}", change_type),
            };
            (table, sc)
        })
        .collect();

    let sink = rust_ivm::streamer::CollectSink::new();
    let mut chunker = rust_ivm::streamer::Chunker::new(sink, 64);

    state.engine.advance_streaming(&rust_changes, |rc| {
        chunker.push_row_change(&rc.query_id, rc.clone());
    });
    chunker.done();

    let sink = chunker.into_sink();
    let mut ndjson = String::new();
    for frame in &sink.frames {
        let json = match frame {
            rust_ivm::streamer::StreamFrame::Partial {
                chunk_index,
                query_id,
                changes,
            } => {
                let changes_json: Vec<JsonValue> = changes.iter().map(row_change_to_json).collect();
                serde_json::json!({
                    "type": "partial",
                    "chunkIndex": chunk_index,
                    "queryId": query_id,
                    "changes": changes_json,
                })
            }
            rust_ivm::streamer::StreamFrame::Final {
                chunk_index,
                query_id,
            } => {
                serde_json::json!({
                    "type": "final",
                    "chunkIndex": chunk_index,
                    "queryId": query_id,
                })
            }
            rust_ivm::streamer::StreamFrame::Done { chunk_index } => {
                serde_json::json!({
                    "type": "done",
                    "chunkIndex": chunk_index,
                })
            }
            rust_ivm::streamer::StreamFrame::Error {
                chunk_index,
                message,
            } => {
                serde_json::json!({
                    "type": "error",
                    "chunkIndex": chunk_index,
                    "message": message,
                })
            }
        };
        ndjson.push_str(&json.to_string());
        ndjson.push('\n');
    }

    let mut response = Response::from_string(ndjson).with_status_code(200);
    if let Ok(header) = Header::from_bytes(b"Content-Type", b"application/x-ndjson") {
        response = response.with_header(header);
    }
    if let Ok(header) = Header::from_bytes(b"Access-Control-Allow-Origin", b"*") {
        response = response.with_header(header);
    }
    response
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

fn json_response(status: u16, body: &JsonValue) -> Response<std::io::Cursor<Vec<u8>>> {
    let body_str = serde_json::to_string(body).unwrap_or_else(|_| "{}".to_string());
    let mut response = Response::from_string(body_str).with_status_code(status);
    if let Ok(header) = Header::from_bytes(b"Content-Type", b"application/json") {
        response = response.with_header(header);
    }
    if let Ok(header) = Header::from_bytes(b"Access-Control-Allow-Origin", b"*") {
        response = response.with_header(header);
    }
    response
}

fn error_response(status: u16, msg: &str) -> Response<std::io::Cursor<Vec<u8>>> {
    json_response(status, &serde_json::json!({"error": msg}))
}

fn read_body(request: &mut tiny_http::Request) -> Option<JsonValue> {
    let mut content = String::new();
    request.as_reader().read_to_string(&mut content).ok()?;
    if content.is_empty() {
        return Some(JsonValue::Object(serde_json::Map::new()));
    }
    serde_json::from_str(&content).ok()
}

// ---------------------------------------------------------------------------
// Main — single-threaded server loop
// ---------------------------------------------------------------------------

fn main() {
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let addr = format!("0.0.0.0:{}", port);
    let server = Server::http(&addr).expect("Failed to bind server");
    eprintln!("rust-ivm-server listening on http://{}", addr);

    let mut state = ServerState {
        engine: Engine::new(HashMap::new()),
        sources: HashMap::new(),
    };

    for mut request in server.incoming_requests() {
        let path = request.url().to_string();
        let method = request.method().clone();

        let response = match (&method, path.as_str()) {
            (Method::Get, "/health") => handle_health(),
            (Method::Get, "/version") => handle_version(),
            (Method::Get, "/queries") => handle_queries(&state),
            (Method::Get, "/sources") => handle_sources(&state),

            (Method::Post, "/init") => match read_body(&mut request) {
                Some(body) => handle_init(&mut state, &body),
                None => error_response(400, "Invalid JSON body"),
            },
            (Method::Post, "/add-queries") => match read_body(&mut request) {
                Some(body) => handle_add_queries(&mut state, &body),
                None => error_response(400, "Invalid JSON body"),
            },
            (Method::Post, "/advance") => match read_body(&mut request) {
                Some(body) => handle_advance(&mut state, &body),
                None => error_response(400, "Invalid JSON body"),
            },
            (Method::Post, "/add-row") => match read_body(&mut request) {
                Some(body) => handle_add_row(&mut state, &body),
                None => error_response(400, "Invalid JSON body"),
            },
            (Method::Post, "/remove-query") => match read_body(&mut request) {
                Some(body) => handle_remove_query(&mut state, &body),
                None => error_response(400, "Invalid JSON body"),
            },
            (Method::Post, "/destroy") => handle_destroy(&mut state),
            (Method::Post, "/add-queries-stream") => match read_body(&mut request) {
                Some(body) => handle_add_queries_stream(&mut state, &body),
                None => error_response(400, "Invalid JSON body"),
            },
            (Method::Post, "/advance-stream") => match read_body(&mut request) {
                Some(body) => handle_advance_stream(&mut state, &body),
                None => error_response(400, "Invalid JSON body"),
            },

            (Method::Options, _) => {
                let mut resp = Response::from_string("").with_status_code(204);
                if let Ok(h) = Header::from_bytes(b"Access-Control-Allow-Origin", b"*") {
                    resp = resp.with_header(h);
                }
                if let Ok(h) =
                    Header::from_bytes(b"Access-Control-Allow-Methods", b"GET, POST, OPTIONS")
                {
                    resp = resp.with_header(h);
                }
                if let Ok(h) = Header::from_bytes(b"Access-Control-Allow-Headers", b"Content-Type")
                {
                    resp = resp.with_header(h);
                }
                resp
            }

            _ => error_response(404, &format!("Not found: {} {}", method, path)),
        };

        let _ = request.respond(response);
    }
}
