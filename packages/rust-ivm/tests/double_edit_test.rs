//! G15 repro: Two Edits to the same row in the same advance.
//! archiveChannel: isArchived false -> true
//! unarchiveChannel: isArchived true -> false
//! Final state should be isArchived=false.

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{Ast, Condition, SimpleCondition, ValuePosition};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::change::ChangeType;
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::{make_source_change_add, make_source_change_edit};
use rust_ivm::streamer::RowChange;

fn source(
    name: &str,
    cols: &[(&str, ColumnType)],
    pk: &[&str],
) -> Rc<std::cell::RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = cols
        .iter()
        .map(|(c, t)| (c.to_string(), t.clone()))
        .collect();
    Rc::new(std::cell::RefCell::new(MemorySource::new(
        name,
        columns,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

fn row(pairs: Vec<(&str, Value)>) -> Arc<FxHashMap<String, Value>> {
    Arc::new(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

fn channels_ast() -> Ast {
    Ast {
        schema: None,
        table: "channels".to_string(),
        alias: None,
        where_clause: Some(Condition::Simple(SimpleCondition {
            left: ValuePosition::Column {
                name: "id".to_string(),
            },
            op: "=".to_string(),
            right: ValuePosition::Literal {
                value: Value::Str(Arc::from("ch1")),
            },
        })),
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    }
}

fn change_summary(changes: &[RowChange]) -> Vec<(String, ChangeType, bool)> {
    changes
        .iter()
        .filter(|c| c.table == "channels")
        .map(|c| {
            let id = match c.row.as_ref().and_then(|r| r.get("id")) {
                Some(Value::Str(s)) => s.to_string(),
                _ => "?".to_string(),
            };
            let archived = match c.row.as_ref().and_then(|r| r.get("isArchived")) {
                Some(Value::Bool(b)) => *b,
                _ => false,
            };
            (id, c.change_type, archived)
        })
        .collect()
}

#[test]
fn double_edit_same_advance_final_state_correct() {
    let src = source(
        "channels",
        &[
            ("id", ColumnType::String { optional: false }),
            ("name", ColumnType::String { optional: false }),
            ("isArchived", ColumnType::Boolean { optional: false }),
        ],
        &["id"],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(src);
    engine.add_queries(&[QuerySpec {
        query_id: "q".to_string(),
        ast: channels_ast(),
    }]);

    // 1. Add channel with isArchived=false
    let changes1 = engine.advance(&[(
        "channels".to_string(),
        make_source_change_add(row(vec![
            ("id", Value::Str(Arc::from("ch1"))),
            ("name", Value::Str(Arc::from("general"))),
            ("isArchived", Value::Bool(false)),
        ])),
    )]);
    println!("After Add: {:?}", change_summary(&changes1));

    // 2. Same advance: archive (false->true) then unarchive (true->false)
    let old1 = row(vec![
        ("id", Value::Str(Arc::from("ch1"))),
        ("name", Value::Str(Arc::from("general"))),
        ("isArchived", Value::Bool(false)),
    ]);
    let new1 = row(vec![
        ("id", Value::Str(Arc::from("ch1"))),
        ("name", Value::Str(Arc::from("general"))),
        ("isArchived", Value::Bool(true)),
    ]);
    let old2 = new1.clone();
    let new2 = row(vec![
        ("id", Value::Str(Arc::from("ch1"))),
        ("name", Value::Str(Arc::from("general"))),
        ("isArchived", Value::Bool(false)),
    ]);

    let changes2 = engine.advance(&[
        ("channels".to_string(), make_source_change_edit(new1, old1)),
        ("channels".to_string(), make_source_change_edit(new2, old2)),
    ]);
    println!("After double Edit: {:?}", change_summary(&changes2));

    // The final state should reflect isArchived=false
    // The engine should emit Edit(true) then Edit(false), or just a no-op
    // Either way, a fresh fetch should show isArchived=false
    // Check the last edit's value
    let edits: Vec<_> = changes2
        .iter()
        .filter(|c| c.table == "channels" && c.change_type == ChangeType::Edit)
        .collect();
    println!("Edits: {}", edits.len());
    for e in &edits {
        if let Some(r) = e.row.as_ref() {
            println!("  isArchived={:?}", r.get("isArchived"));
        }
    }
}
