//! Fixture replayer: runs an inputs-only fixture through the Rust engine and
//! emits the `{hydrate, pushChanges, finalView}` JSON that the TS oracle
//! (agentic/oracle/ts-runner.mjs) produces. Shared by tests/fixture_replay_test.rs
//! and the `replay` binary (src/bin/replay.rs).
//!
//! Pipeline construction mirrors the TS oracle exactly:
//!   complete_ordering_ast(json_to_ast(ast), pks)   // TS buildPipeline does this internally
//!   build_pipeline(ast, &mut FixtureDelegate{sources, enable_not_exists})
//!   Catch::new(pipeline, false)                    // same sink the oracle uses
//!   hydrate = catch.fetch(); per-push deltas = catch.pushes; finalView = catch.fetch()

use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use rustc_hash::FxHashMap;
use serde_json::Value as JsonValue;

use crate::builder::ast::{
    Ast, Bound, Condition, CorrelatedSubqueryCondition, OrderPart, RelatedSubquery,
    SimpleCondition, ValuePosition,
};
use crate::builder::builder::{build_pipeline, complete_ordering_ast, BuilderDelegate};
use crate::ivm::catch::{Catch, CaughtChange, CaughtNode};
use crate::ivm::change::{
    make_source_change_add, make_source_change_edit, make_source_change_remove, SourceChange,
};
use crate::ivm::data::{row as make_row, Row, Value};
use crate::ivm::operator::{FetchRequest, Shared};
use crate::ivm::schema::ColumnType;
use crate::ivm::source::{MemorySource, Source};

// Value / Row conversion: serde_json <-> Rust (mirrors src/bin/server.rs).

pub fn json_to_rust_value(v: &JsonValue) -> Value {
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

pub fn rust_value_to_json(v: &Value) -> JsonValue {
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
        Value::Json(s) => serde_json::from_str(s).unwrap_or_else(|_| JsonValue::String(s.to_string())),
    }
}

pub fn json_to_row(obj: &serde_json::Map<String, JsonValue>) -> Row {
    let mut map: FxHashMap<String, Value> = FxHashMap::default();
    for (k, v) in obj {
        map.insert(k.clone(), json_to_rust_value(v));
    }
    make_row(map)
}

pub fn row_to_json(row: &Row) -> JsonValue {
    let mut map = serde_json::Map::new();
    for (k, v) in row.iter() {
        map.insert(k.clone(), rust_value_to_json(v));
    }
    JsonValue::Object(map)
}

// AST conversion: fixture JSON (TS shape) -> Rust Ast (mirrors src/bin/server.rs).
// The Rust Ast's serde derives do NOT match the TS fixture shape (camelCase,
// tuple orderBy, nested correlation, bare literals), so we convert by hand.

pub fn json_to_value_position(v: &JsonValue) -> ValuePosition {
    let kind = v.get("type").and_then(|t| t.as_str()).unwrap_or("literal");
    match kind {
        "column" => {
            let name = v.get("name").and_then(|n| n.as_str()).unwrap_or("");
            ValuePosition::Column { name: name.to_string() }
        }
        "literal" | _ => {
            let val = v.get("value").map(json_to_rust_value).unwrap_or(Value::Null);
            ValuePosition::Literal { value: val }
        }
    }
}

pub fn json_to_simple_condition(v: &JsonValue) -> SimpleCondition {
    SimpleCondition {
        op: v.get("op").and_then(|o| o.as_str()).unwrap_or("=").to_string(),
        left: json_to_value_position(v.get("left").unwrap_or(&JsonValue::Null)),
        right: json_to_value_position(v.get("right").unwrap_or(&JsonValue::Null)),
    }
}

pub fn json_to_condition(v: &JsonValue) -> Condition {
    let kind = v.get("type").and_then(|t| t.as_str()).unwrap_or("simple");
    match kind {
        "simple" => Condition::Simple(json_to_simple_condition(v)),
        "and" => {
            let conds: Vec<Condition> = v
                .get("conditions").and_then(|c| c.as_array()).unwrap_or(&vec![])
                .iter().map(json_to_condition).collect();
            Condition::And(conds)
        }
        "or" => {
            let conds: Vec<Condition> = v
                .get("conditions").and_then(|c| c.as_array()).unwrap_or(&vec![])
                .iter().map(json_to_condition).collect();
            Condition::Or(conds)
        }
        "correlatedSubquery" => {
            let related = json_to_related_subquery(v.get("related").unwrap_or(&JsonValue::Null));
            let op = v.get("op").and_then(|o| o.as_str()).unwrap_or("EXISTS").to_string();
            let flip = v.get("flip").and_then(|f| f.as_bool()).unwrap_or(false);
            let scalar = v.get("scalar").and_then(|s| s.as_bool()).unwrap_or(false);
            Condition::CorrelatedSubquery(CorrelatedSubqueryCondition { related, op, flip, scalar })
        }
        _ => panic!("unknown condition type: {kind}"),
    }
}

pub fn json_to_related_subquery(v: &JsonValue) -> RelatedSubquery {
    let subquery = json_to_ast(v.get("subquery").unwrap_or(&JsonValue::Null));
    let relationship_name = v
        .get("subquery").and_then(|sq| sq.get("alias")).and_then(|a| a.as_str())
        .or_else(|| v.get("alias").and_then(|a| a.as_str()))
        .unwrap_or("").to_string();
    let (parent_key, child_key) = if let Some(corr) = v.get("correlation") {
        let parent = corr.get("parentField").and_then(|p| p.as_array())
            .map(|a| a.iter().map(|s| s.as_str().unwrap_or("").to_string()).collect())
            .unwrap_or_default();
        let child = corr.get("childField").and_then(|c| c.as_array())
            .map(|a| a.iter().map(|s| s.as_str().unwrap_or("").to_string()).collect())
            .unwrap_or_default();
        (parent, child)
    } else {
        (vec![], vec![])
    };
    let hidden = v.get("hidden").and_then(|h| h.as_bool()).unwrap_or(false);
    let system = v.get("system").and_then(|s| s.as_str()).map(|s| match s {
        "permissions" => crate::ivm::schema::System::Permissions,
        "test" => crate::ivm::schema::System::Test,
        _ => crate::ivm::schema::System::Client,
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

pub fn json_to_ast(v: &JsonValue) -> Ast {
    let table = v.get("table").and_then(|t| t.as_str()).unwrap_or("").to_string();
    let alias = v.get("alias").and_then(|a| a.as_str()).map(|s| s.to_string());
    let where_clause = v.get("where").map(json_to_condition);
    let related: Vec<RelatedSubquery> = v
        .get("related").and_then(|r| r.as_array()).unwrap_or(&vec![])
        .iter().map(json_to_related_subquery).collect();
    let limit = v.get("limit").and_then(|l| l.as_i64()).map(|l| l as usize);
    let order_by = v.get("orderBy").and_then(|o| o.as_array()).map(|parts| {
        parts.iter().map(|p| {
            let empty_arr = vec![];
            let arr = p.as_array().unwrap_or(&empty_arr);
            OrderPart {
                column: arr.get(0).and_then(|c| c.as_str()).unwrap_or("").to_string(),
                direction: arr.get(1).and_then(|d| d.as_str()).unwrap_or("asc").to_string(),
            }
        }).collect()
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
            exclusive: s.get("exclusive").and_then(|e| e.as_bool()).unwrap_or(false),
        }
    });
    Ast { schema: None, table, alias, where_clause, related, limit, order_by, start }
}

// Fixture helpers: column-type string -> ColumnType, push -> SourceChange.

pub fn parse_column_type(s: &str) -> ColumnType {
    let parts: Vec<&str> = s.split('|').collect();
    let optional = parts.iter().any(|p| *p == "null");
    let base = parts.iter().find(|p| **p != "null").copied().unwrap_or("string");
    match base {
        "boolean" => ColumnType::Boolean { optional },
        "number" => ColumnType::Number { optional },
        "json" => ColumnType::Json { optional },
        _ => ColumnType::String { optional },
    }
}

pub fn push_to_source_change(push: &JsonValue) -> (String, SourceChange) {
    let table = push.get("table").and_then(|t| t.as_str()).unwrap_or("").to_string();
    let row = match push.get("row").and_then(|r| r.as_object()) {
        Some(obj) => json_to_row(obj),
        None => make_row(FxHashMap::default()),
    };
    let sc = match push.get("type").and_then(|t| t.as_str()).unwrap_or("add") {
        "remove" => make_source_change_remove(row),
        "edit" => {
            let old_row = match push.get("oldRow").and_then(|r| r.as_object()) {
                Some(obj) => json_to_row(obj),
                None => make_row(FxHashMap::default()),
            };
            make_source_change_edit(row, old_row)
        }
        _ => make_source_change_add(row),
    };
    (table, sc)
}

// Delegate the replayer uses to resolve sources by table name (mirrors the
// TS oracle's TestBuilderDelegate).
struct FixtureDelegate {
    sources: HashMap<String, Shared<dyn Source>>,
    enable_not_exists: bool,
}

impl BuilderDelegate for FixtureDelegate {
    fn get_source(&self, table: &str) -> Option<Shared<dyn Source>> {
        self.sources.get(table).cloned()
    }
    fn enable_not_exists(&self) -> bool {
        self.enable_not_exists
    }
}

// Serialization of CaughtNode / CaughtChange to the TS oracle JSON shape
// (mirrors TS expandNode / expandChange in ivm/catch.ts).

pub fn caught_node_to_json(node: &CaughtNode) -> JsonValue {
    let mut rels = serde_json::Map::new();
    for (name, children) in &node.relationships {
        let arr: Vec<JsonValue> = children.iter().map(caught_node_to_json).collect();
        rels.insert(name.clone(), JsonValue::Array(arr));
    }
    let mut obj = serde_json::Map::new();
    obj.insert("row".into(), row_to_json(&node.row));
    obj.insert("relationships".into(), JsonValue::Object(rels));
    JsonValue::Object(obj)
}

pub fn caught_change_to_json(change: &CaughtChange) -> JsonValue {
    let mut obj = serde_json::Map::new();
    match change {
        CaughtChange::Add { node } => {
            obj.insert("type".into(), JsonValue::String("add".into()));
            obj.insert("node".into(), caught_node_to_json(node));
        }
        CaughtChange::Remove { node } => {
            obj.insert("type".into(), JsonValue::String("remove".into()));
            obj.insert("node".into(), caught_node_to_json(node));
        }
        CaughtChange::Edit { old_row, row } => {
            obj.insert("type".into(), JsonValue::String("edit".into()));
            obj.insert("oldRow".into(), row_to_json(old_row));
            obj.insert("row".into(), row_to_json(row));
        }
        CaughtChange::Child { row, child } => {
            obj.insert("type".into(), JsonValue::String("child".into()));
            obj.insert("row".into(), row_to_json(row));
            let mut child_obj = serde_json::Map::new();
            child_obj.insert("relationshipName".into(), JsonValue::String(child.0.clone()));
            child_obj.insert("change".into(), caught_change_to_json(&child.1));
            obj.insert("child".into(), JsonValue::Object(child_obj));
        }
    }
    JsonValue::Object(obj)
}

// The core replayer: parse a fixture, build sources + pipeline (the same way the
// TS oracle builds it), hydrate, apply pushes, serialize {hydrate, pushChanges, finalView}.
pub fn run_fixture(fixture: &JsonValue) -> JsonValue {
    let mut sources: HashMap<String, Shared<dyn Source>> = HashMap::new();
    let mut pks: HashMap<String, Vec<String>> = HashMap::new();
    let tables = fixture.get("tables").and_then(|t| t.as_object()).cloned().unwrap_or_default();
    for (name, spec) in &tables {
        let mut columns: HashMap<String, ColumnType> = HashMap::new();
        if let Some(cols) = spec.get("columns").and_then(|c| c.as_object()) {
            for (col, type_spec) in cols {
                let tstr = type_spec.as_str().unwrap_or("string");
                columns.insert(col.clone(), parse_column_type(tstr));
            }
        }
        let pk: Vec<String> = spec.get("primaryKey").and_then(|p| p.as_array())
            .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
            .unwrap_or_default();
        let source: Shared<MemorySource> =
            std::rc::Rc::new(std::cell::RefCell::new(MemorySource::new(name, columns, pk.clone())));
        if let Some(rows) = spec.get("rows").and_then(|r| r.as_array()) {
            for row_json in rows {
                if let Some(obj) = row_json.as_object() {
                    let mut m: FxHashMap<String, Value> = FxHashMap::default();
                    for (k, v) in obj {
                        m.insert(k.clone(), json_to_rust_value(v));
                    }
                    source.borrow_mut().add_row(m);
                }
            }
        }
        pks.insert(name.clone(), pk);
        sources.insert(name.clone(), source as Shared<dyn Source>);
    }

    let ast = json_to_ast(fixture.get("ast").unwrap_or(&JsonValue::Null));
    let enable_not_exists = fixture.get("enableNotExists").and_then(|b| b.as_bool()).unwrap_or(false);

    // Resolve simple scalar subqueries (scalar-flagged EXISTS that pin a unique
    // key) exactly like the engine hydrate path (Engine::resolve_scalar_subqueries):
    // replace each with a literal condition and capture the matched row as a
    // companion. Without this, replay hardcoded companionRows to empty and every
    // scalar-match fixture falsely diverged from the oracle. Unique keys for a
    // fixture table are its primary key (matches the generator, which pins `id`).
    use crate::sqlite::resolve_scalar_subqueries::{
        resolve_simple_scalar_subqueries, ScalarExecutor, TableSpecWithUniqueKeys,
    };
    let table_specs: HashMap<String, TableSpecWithUniqueKeys> = pks
        .iter()
        .map(|(t, pk)| (t.clone(), TableSpecWithUniqueKeys { unique_keys: vec![pk.clone()] }))
        .collect();
    let companion_rows: std::cell::RefCell<Vec<(String, Row)>> = std::cell::RefCell::new(Vec::new());
    let resolved_ast = {
        let sources_ref = &sources;
        let pks_ref = &pks;
        let executor: ScalarExecutor = Box::new(|subquery_ast: &Ast, child_field: &str| {
            let completed =
                complete_ordering_ast(subquery_ast, &|t: &str| pks_ref.get(t).cloned().unwrap_or_default());
            let mut delegate = FixtureDelegate { sources: sources_ref.clone(), enable_not_exists };
            let input = build_pipeline(&completed, &mut delegate);
            // The subquery is at-most-one-row; take the first node (mirrors TS).
            let mut first: Option<crate::ivm::data::Node> = None;
            for node in crate::ivm::stream::skip_yields(input.borrow().fetch(&Default::default())) {
                if first.is_none() {
                    first = Some(node);
                }
            }
            match first {
                None => (None, false),
                Some(node) => {
                    let value = match node.row.get(child_field) {
                        None | Some(Value::Null) => None,
                        Some(v) => Some(v.clone()),
                    };
                    companion_rows
                        .borrow_mut()
                        .push((subquery_ast.table.clone(), node.row.clone()));
                    (value, true)
                }
            }
        });
        resolve_simple_scalar_subqueries(&ast, &table_specs, &executor).ast
    };

    let completed = complete_ordering_ast(&resolved_ast, &|t| pks.get(t).cloned().unwrap_or_default());
    let mut delegate = FixtureDelegate { sources: sources.clone(), enable_not_exists };
    let pipeline = build_pipeline(&completed, &mut delegate);

    let catch = Catch::new(pipeline, false);

    // When RUST_IVM_PARALLEL_HYDRATE is not explicitly disabled, dispatch the
    // hydrate fetch to a parallel worker (exercising the production parallel-
    // hydrate path: source spec extraction, WorkerDelegate transient rebuild,
    // ParallelJob dispatch). The worker rebuilds a transient pipeline from Send
    // source specs, fetches through a Catch, and streams CaughtNodes — same
    // output format as serial, diffable against the TS oracle. Pushes +
    // finalView still run on the actor's Catch (serial, same as the production
    // path where advance is single-threaded).
    let parallel = std::env::var("RUST_IVM_PARALLEL_HYDRATE")
        .ok()
        .map(|v| {
            v != "0" && !v.eq_ignore_ascii_case("false") && !v.eq_ignore_ascii_case("off")
        })
        .unwrap_or(true);

    let hydrate: Vec<CaughtNode> = if parallel {
        use crate::engine::parallel_hydrate::{extract_source_specs, referenced_tables};
        use crate::engine::worker::ParallelJob;
        use crate::engine::CancellationToken;

        let tables = referenced_tables(&completed);
        let source_specs = extract_source_specs(&sources, &tables);
        let ast_clone = completed.clone();
        let enable_ne = enable_not_exists;
        let task = Box::new(
            move |_scope: &crate::engine::worker::WorkerScope,
                  sink: &dyn Fn(CaughtNode)| -> Result<(), String> {
                // Rebuild transient sources from Send specs (same as production
                // WorkerDelegate).
                let mut wdelegate = crate::engine::parallel_hydrate::WorkerDelegate::new(
                    source_specs.clone(),
                    enable_ne,
                )?;
                let tpipeline = build_pipeline(&ast_clone, &mut wdelegate);
                let tcatch = Catch::new(tpipeline, false);
                let nodes = tcatch.borrow().fetch(&FetchRequest::default());
                for node in nodes {
                    sink(node);
                }
                Ok(())
            },
        );
        let job: ParallelJob<CaughtNode, String> = ParallelJob::new(2, 4);
        let mut collected: Vec<CaughtNode> = Vec::new();
        let cancel = CancellationToken::new();
        match job.run_streaming(vec![task], cancel, |node| collected.push(node)) {
            Ok(()) => collected,
            Err(e) => {
                // Parallel failed — fall back to serial (S4).
                eprintln!("[replay] parallel hydrate failed, falling back to serial: {:?}", e);
                catch.borrow().fetch(&FetchRequest::default())
            }
        }
    } else {
        catch.borrow().fetch(&FetchRequest::default())
    };

    // Phase 2b (parallel only): the worker fetched a TRANSIENT pipeline. The
    // actor's pipeline was built but never fetched, so its operators lack the
    // fetch-time state (join caches, cap counters, etc.) that pushes rely on.
    // Fetch the actor's pipeline (discarding the output — the worker already
    // produced the hydrate rows) to set up operator state for pushes. The
    // Catch's fetch eagerly expands relationships, so child operators are
    // set up too (same as the recurse_node_relationships fix in the engine).
    if parallel {
        let _ = catch.borrow().fetch(&FetchRequest::default());
    }

    let hydrate_json = JsonValue::Array(hydrate.iter().map(caught_node_to_json).collect());

    let pushes = fixture.get("pushes").and_then(|p| p.as_array()).cloned().unwrap_or_default();
    let mut push_changes: Vec<JsonValue> = Vec::new();
    for push in &pushes {
        let (table, sc) = push_to_source_change(push);
        let src = sources.get(&table)
            .unwrap_or_else(|| panic!("unknown source for push to table: {table}"));
        let before = catch.borrow().pushes.len();
        src.borrow_mut().push(sc);
        let changes_json: Vec<JsonValue> = catch.borrow().pushes[before..]
            .iter().map(caught_change_to_json).collect();
        push_changes.push(JsonValue::Array(changes_json));
    }

    let final_view = catch.borrow().fetch(&FetchRequest::default());
    let final_json = JsonValue::Array(final_view.iter().map(caught_node_to_json).collect());

    let mut result = serde_json::Map::new();
    let companion_json: Vec<JsonValue> = companion_rows
        .into_inner()
        .into_iter()
        .map(|(table, row)| {
            let mut m = serde_json::Map::new();
            m.insert("table".into(), JsonValue::String(table));
            m.insert("row".into(), row_to_json(&row));
            JsonValue::Object(m)
        })
        .collect();
    result.insert("companionRows".into(), JsonValue::Array(companion_json));
    result.insert("hydrate".into(), hydrate_json);
    result.insert("pushChanges".into(), JsonValue::Array(push_changes));
    result.insert("finalView".into(), final_json);
    JsonValue::Object(result)
}

pub fn run_fixture_file(input_path: &str) -> JsonValue {
    let content = fs::read_to_string(input_path)
        .unwrap_or_else(|e| panic!("failed to read {input_path}: {e}"));
    let fixture: JsonValue = serde_json::from_str(&content)
        .unwrap_or_else(|e| panic!("failed to parse fixture {input_path}: {e}"));
    run_fixture(&fixture)
}

// Canonical JSON comparison — mirrors agentic/oracle/diff.mjs: sorted object
// keys, -0 -> 0, integer-valued floats -> ints. Numeric comparison is otherwise
// exact (no epsilon): precision differences are real bugs.

pub fn canonicalize(v: &JsonValue) -> JsonValue {
    match v {
        JsonValue::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f == 0.0 {
                    return JsonValue::Number(serde_json::Number::from(0i64));
                }
                if f.fract() == 0.0 && f.is_finite()
                    && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
                    return JsonValue::Number(serde_json::Number::from(f as i64));
                }
                return JsonValue::Number(
                    serde_json::Number::from_f64(f).unwrap_or_else(|| serde_json::Number::from(0)),
                );
            }
            JsonValue::Number(n.clone())
        }
        JsonValue::Array(a) => JsonValue::Array(a.iter().map(canonicalize).collect()),
        JsonValue::Object(o) => {
            let mut keys: Vec<&String> = o.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for k in keys {
                out.insert(k.clone(), canonicalize(&o[k]));
            }
            JsonValue::Object(out)
        }
        _ => v.clone(),
    }
}

fn json_deep_equal(a: &JsonValue, b: &JsonValue) -> bool {
    match (a, b) {
        (JsonValue::Number(x), JsonValue::Number(y)) => x.as_f64() == y.as_f64(),
        (JsonValue::Array(x), JsonValue::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(p, q)| json_deep_equal(p, q))
        }
        (JsonValue::Object(x), JsonValue::Object(y)) => {
            x.len() == y.len()
                && x.iter().all(|(k, v)| y.get(k).map(|w| json_deep_equal(v, w)).unwrap_or(false))
        }
        _ => a == b,
    }
}

pub fn diff_path(a: &JsonValue, b: &JsonValue, path: &str) -> Option<(String, JsonValue, JsonValue)> {
    if json_deep_equal(a, b) {
        return None;
    }
    match (a, b) {
        (JsonValue::Array(x), JsonValue::Array(y)) => {
            if x.len() != y.len() {
                return Some((
                    format!("{path}.length"),
                    JsonValue::Number(serde_json::Number::from(x.len() as u64)),
                    JsonValue::Number(serde_json::Number::from(y.len() as u64)),
                ));
            }
            for (i, (p, q)) in x.iter().zip(y.iter()).enumerate() {
                if let Some(d) = diff_path(p, q, &format!("{path}[{i}]")) {
                    return Some(d);
                }
            }
            None
        }
        (JsonValue::Object(x), JsonValue::Object(y)) => {
            let mut keys: Vec<&String> = x.keys().chain(y.keys()).collect();
            keys.sort();
            keys.dedup();
            for k in keys {
                match (x.get(k), y.get(k)) {
                    (None, Some(v)) => return Some((format!("{path}.{k}"), JsonValue::Null, v.clone())),
                    (Some(v), None) => return Some((format!("{path}.{k}"), v.clone(), JsonValue::Null)),
                    (Some(p), Some(q)) => {
                        if let Some(d) = diff_path(p, q, &format!("{path}.{k}")) {
                            return Some(d);
                        }
                    }
                    (None, None) => {}
                }
            }
            None
        }
        _ => Some((
            if path.is_empty() { "<root>".to_string() } else { path.to_string() },
            a.clone(),
            b.clone(),
        )),
    }
}

/// Compare actual (Rust) vs expected (TS oracle) output. Ok on match; Err holds
/// the first diverging path + values for a readable failure message.
/// Drop an empty top-level `companionRows` array for comparison only: replay
/// always emits the field (companionRows:[] absent any scalar-subquery
/// companion), while pre-companion fixtures omit it. This normalization is
/// comparison-only — the emitted output (bin/replay) keeps companionRows so the
/// live oracle differential, where both sides emit it, still matches exactly.
fn strip_empty_companion_rows(v: &JsonValue) -> JsonValue {
    match v {
        JsonValue::Object(o) => {
            let mut out = serde_json::Map::new();
            for (k, val) in o {
                if k == "companionRows" {
                    if let JsonValue::Array(a) = val {
                        if a.is_empty() {
                            continue;
                        }
                    }
                }
                out.insert(k.clone(), val.clone());
            }
            JsonValue::Object(out)
        }
        _ => v.clone(),
    }
}

pub fn assert_matches(actual: &JsonValue, expected: &JsonValue) -> Result<(), String> {
    let ca = strip_empty_companion_rows(&canonicalize(actual));
    let cb = strip_empty_companion_rows(&canonicalize(expected));
    if json_deep_equal(&ca, &cb) {
        Ok(())
    } else {
        // diff_path(actual, expected) returns (path, actual_val, expected_val)
        // in argument order — do NOT relabel them swapped.
        match diff_path(&ca, &cb, "") {
            Some((p, actual_val, expected_val)) => Err(format!(
                "DIFF at {p}\n  expected: {expected_val}\n  actual:   {actual_val}"
            )),
            None => Err("DIFF (structural mismatch)".to_string()),
        }
    }
}
