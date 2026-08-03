//! Repro for BUG 6: a `{app}_{shard}.mutations`-shaped table whose `result`
//! column is Postgres type JSON is stored in the SQLite replica as stringified
//! text. TS re-parses it to an OBJECT on read (`fromSQLiteTypes` →
//! `case 'json': JSON.parse(v)`), and the client-handler's `mutationRowSchema`
//! REQUIRES `result` to be an object. If rust-ivm emits the column as a JSON
//! STRING (`Value::Str`) instead of a parsed object (`Value::Json`), the napi
//! boundary serializes it as a JSON string and TS `v.parse(mutationRowSchema)`
//! throws a fatal `ProtocolError` that tears down the WebSocket connection —
//! even for a lawful app-level mutation error.
//!
//! The fix: the `result` json column must be typed `ColumnType::Json` on the
//! source so the value is tagged `Value::Json` (parsed object) — matching TS.
//! This test asserts the emitted value is `Value::Json`, NOT `Value::Str`.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rusqlite::Connection;

use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::sqlite::table_source::TableSource;

/// A `MutationACLError` result exactly as the custom-mutation backend returns
/// it — a lawful failed-mutation result stored as a JSON string in SQLite.
const RESULT_JSON: &str = r#"{"error":"app","message":"Acl not defined for upsert on table X","details":{"name":"MutationACLError"}}"#;

fn mutations_source() -> TableSource {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        r#"CREATE TABLE "zero_0.mutations" (
             "clientGroupID" TEXT NOT NULL,
             "clientID"      TEXT NOT NULL,
             "mutationID"    INTEGER NOT NULL,
             "result"        TEXT NOT NULL,
             PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
           );"#,
        [],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO "zero_0.mutations"
             ("clientGroupID", "clientID", "mutationID", "result")
             VALUES ('cg1', 'c1', 1, ?1);"#,
        [RESULT_JSON],
    )
    .unwrap();

    // Column types as computeZqlSpecs would produce for this table: the
    // `result` column is Postgres JSON → zqlValueType 'json' → ColumnType::Json.
    let mut columns = HashMap::new();
    columns.insert(
        "clientGroupID".to_string(),
        ColumnType::String { optional: false },
    );
    columns.insert(
        "clientID".to_string(),
        ColumnType::String { optional: false },
    );
    columns.insert(
        "mutationID".to_string(),
        ColumnType::Number { optional: false },
    );
    columns.insert("result".to_string(), ColumnType::Json { optional: false });

    TableSource::new(
        Rc::new(RefCell::new(conn)),
        "zero_0.mutations",
        columns,
        vec![
            "clientGroupID".to_string(),
            "clientID".to_string(),
            "mutationID".to_string(),
        ],
    )
}

/// FETCH/hydrate path: the `result` json column must come back tagged
/// `Value::Json` (a parsed object), NOT `Value::Str`.
#[test]
fn mutations_result_json_column_is_parsed_object_on_fetch() {
    let mut source = mutations_source();
    let input = source.connect(None, None, None, None);
    let stream = input.borrow().fetch(&Default::default());
    let nodes: Vec<_> = rust_ivm::ivm::stream::skip_yields(stream).collect();

    assert_eq!(nodes.len(), 1, "expected one mutation row");
    let result = nodes[0].row.get("result").cloned().unwrap_or(Value::Null);

    match &result {
        Value::Json(j) => {
            // Value::Json carries the raw JSON text; the napi boundary re-parses
            // it into a nested object. Confirm it is the object we stored.
            assert_eq!(j.as_ref(), RESULT_JSON);
        }
        Value::Str(_) => panic!(
            "BUG 6: `result` json column emitted as Value::Str (JSON string) — \
             napi would serialize it as a JSON string, breaking mutationRowSchema \
             and fatally tearing down the connection. Expected Value::Json."
        ),
        other => panic!("unexpected value for `result`: {other:?}"),
    }
}

/// Guard: the same column WITHOUT the json typing falls through to Value::Str.
/// This documents the exact typing gap that BUG 6 hit (mutations table not
/// registered / result not typed json).
#[test]
fn untyped_result_column_falls_through_to_string() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute(
        r#"CREATE TABLE "zero_0.mutations" ("id" INTEGER PRIMARY KEY, "result" TEXT NOT NULL);"#,
        [],
    )
    .unwrap();
    conn.execute(
        r#"INSERT INTO "zero_0.mutations" ("id", "result") VALUES (1, ?1);"#,
        [RESULT_JSON],
    )
    .unwrap();

    let mut columns = HashMap::new();
    columns.insert("id".to_string(), ColumnType::Number { optional: false });
    // result intentionally NOT typed json (the bug condition)
    columns.insert("result".to_string(), ColumnType::String { optional: false });

    let mut source = TableSource::new(
        Rc::new(RefCell::new(conn)),
        "zero_0.mutations",
        columns,
        vec!["id".to_string()],
    );
    let input = source.connect(None, None, None, None);
    let stream = input.borrow().fetch(&Default::default());
    let nodes: Vec<_> = rust_ivm::ivm::stream::skip_yields(stream).collect();

    let result = nodes[0].row.get("result").cloned().unwrap_or(Value::Null);
    assert_eq!(
        result,
        Value::Str(Arc::from(RESULT_JSON)),
        "untyped column should pass through as a raw string (the bug condition)"
    );
}
