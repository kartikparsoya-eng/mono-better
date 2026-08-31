//! G15 repro probe: EXISTS 0→1 flip when a NEW parent and its EXISTS-enabling
//! child are added in the SAME advance.
//!
//! Models joinChannel: a new `convos` row (a channel conversation) and a new
//! `members` row (the joining user's membership) are inserted together. The
//! query returns convos WHERE EXISTS(members correlated on chan). Both orders
//! within the advance must end with the convo emitted (it becomes visible the
//! moment the membership exists). If the engine drops the convo (pushed with
//! EXISTS=0 before the membership indexes, and never re-triggered), that is the
//! persistent live-advance under-emission the G15 matrix caught.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::change::ChangeType;
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;
use rust_ivm::ivm::source::make_source_change_add;
use rust_ivm::streamer::RowChange;

fn str_source(name: &str, cols: &[&str], pk: &[&str]) -> Rc<RefCell<MemorySource>> {
    let columns: HashMap<String, ColumnType> = cols
        .iter()
        .map(|c| (c.to_string(), ColumnType::String { optional: false }))
        .collect();
    Rc::new(RefCell::new(MemorySource::new(
        name,
        columns,
        pk.iter().map(|s| s.to_string()).collect(),
    )))
}

fn row(pairs: &[(&str, &str)]) -> Arc<FxHashMap<String, Value>> {
    Arc::new(
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Value::Str((*v).into())))
            .collect(),
    )
}

/// convos WHERE EXISTS(members WHERE members.chan == convos.chan)
fn exists_membership_ast() -> Ast {
    let subquery = Ast {
        schema: None,
        table: "members".to_string(),
        alias: Some("zsubq_members".to_string()),
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    };
    Ast {
        schema: None,
        table: "convos".to_string(),
        alias: None,
        where_clause: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: RelatedSubquery {
                subquery: Box::new(subquery),
                relationship_name: "zsubq_members".to_string(),
                parent_key: vec!["chan".to_string()],
                child_key: vec!["chan".to_string()],
                hidden: false,
                system: None,
            },
            op: "EXISTS".to_string(),
            flip: Some(false),
            scalar: false,
            plan_id: None,
        })),
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    }
}

/// convos WHERE EXISTS(members on chan) WITH related messages (initial msg),
/// mirroring the real shape: a permission/membership EXISTS plus a related
/// child join that must be populated on the emitted parent.
fn exists_with_related_ast() -> Ast {
    let members_sub = Ast {
        schema: None,
        table: "members".to_string(),
        alias: Some("zsubq_members".to_string()),
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: None,
        start: None,
    };
    let messages_sub = Ast {
        schema: None,
        table: "messages".to_string(),
        alias: Some("messages".to_string()),
        where_clause: None,
        related: vec![],
        limit: None,
        order_by: Some(vec![rust_ivm::builder::ast::OrderPart {
            column: "id".to_string(),
            direction: "asc".to_string(),
        }]),
        start: None,
    };
    Ast {
        schema: None,
        table: "convos".to_string(),
        alias: None,
        where_clause: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: RelatedSubquery {
                subquery: Box::new(members_sub),
                relationship_name: "zsubq_members".to_string(),
                parent_key: vec!["chan".to_string()],
                child_key: vec!["chan".to_string()],
                hidden: false,
                system: None,
            },
            op: "EXISTS".to_string(),
            flip: Some(false),
            scalar: false,
            plan_id: None,
        })),
        related: vec![RelatedSubquery {
            subquery: Box::new(messages_sub),
            relationship_name: "messages".to_string(),
            parent_key: vec!["id".to_string()],
            child_key: vec!["convo".to_string()],
            hidden: false,
            system: None,
        }],
        limit: None,
        order_by: None,
        start: None,
    }
}

fn msg_ids(changes: &[RowChange]) -> Vec<String> {
    let mut ids: Vec<String> = changes
        .iter()
        .filter(|c| c.table == "messages" && c.change_type == ChangeType::Add)
        .filter_map(|c| match c.row.as_ref()?.get("id")? {
            Value::Str(s) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

fn convo_ids(changes: &[RowChange]) -> Vec<String> {
    let mut ids: Vec<String> = changes
        .iter()
        .filter(|c| c.table == "convos" && c.change_type == ChangeType::Add)
        .filter_map(|c| match c.row.as_ref()?.get("id")? {
            Value::Str(s) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

fn setup() -> Engine {
    // Hydrate EMPTY: no convos, no members yet — the join-channel case where
    // both rows are brand new.
    let convos = str_source("convos", &["id", "chan"], &["id"]);
    let members = str_source("members", &["id", "chan"], &["id"]);
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(convos);
    engine.register_source(members);
    engine.add_queries(&[QuerySpec {
        query_id: "q".to_string(),
        ast: exists_membership_ast(),
    }]);
    engine
}

#[test]
fn exists_flip_parent_then_child_same_advance() {
    let mut engine = setup();
    // Order A (parent first): add the convo, then the membership — in one advance.
    let changes = engine.advance(&[
        (
            "convos".to_string(),
            make_source_change_add(row(&[("id", "c1"), ("chan", "A")])),
        ),
        (
            "members".to_string(),
            make_source_change_add(row(&[("id", "m1"), ("chan", "A")])),
        ),
    ]);
    assert_eq!(
        convo_ids(&changes),
        vec!["c1".to_string()],
        "convo c1 must be emitted once the membership makes EXISTS true \
         (parent-first order); got {:?}",
        changes
            .iter()
            .map(|c| (c.table.clone(), c.change_type, c.row.is_some()))
            .collect::<Vec<_>>(),
    );
}

fn setup_with_related() -> Engine {
    let convos = str_source("convos", &["id", "chan"], &["id"]);
    let members = str_source("members", &["id", "chan"], &["id"]);
    let messages = str_source("messages", &["id", "convo"], &["id"]);
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(convos);
    engine.register_source(members);
    engine.register_source(messages);
    engine.add_queries(&[QuerySpec {
        query_id: "q".to_string(),
        ast: exists_with_related_ast(),
    }]);
    engine
}

#[test]
fn exists_flip_emits_parent_with_related_message_same_advance() {
    // joinChannel shape: a new convo (channel conversation), the joining user's
    // membership (flips the permission EXISTS), and the "X joined" system
    // message (the convo's related message) — all in ONE advance. The live view
    // must gain BOTH the convo AND its related message, exactly as a fresh
    // hydrate would. This is the G15 divergence: convo + message only_mirror.
    let mut engine = setup_with_related();
    let changes = engine.advance(&[
        (
            "convos".to_string(),
            make_source_change_add(row(&[("id", "c1"), ("chan", "A")])),
        ),
        (
            "messages".to_string(),
            make_source_change_add(row(&[("id", "msg1"), ("convo", "c1")])),
        ),
        (
            "members".to_string(),
            make_source_change_add(row(&[("id", "m1"), ("chan", "A")])),
        ),
    ]);
    assert_eq!(
        convo_ids(&changes),
        vec!["c1".to_string()],
        "convo c1 must be emitted (EXISTS flipped by membership); got {:?}",
        changes
            .iter()
            .map(|c| (c.table.clone(), c.change_type))
            .collect::<Vec<_>>(),
    );
    assert_eq!(
        msg_ids(&changes),
        vec!["msg1".to_string()],
        "the convo's related message msg1 must be emitted with the flipped parent; got {:?}",
        changes
            .iter()
            .map(|c| (c.table.clone(), c.change_type))
            .collect::<Vec<_>>(),
    );
}

#[test]
fn exists_flip_child_then_parent_same_advance() {
    let mut engine = setup();
    // Order B (child first): add the membership, then the convo — in one advance.
    let changes = engine.advance(&[
        (
            "members".to_string(),
            make_source_change_add(row(&[("id", "m1"), ("chan", "A")])),
        ),
        (
            "convos".to_string(),
            make_source_change_add(row(&[("id", "c1"), ("chan", "A")])),
        ),
    ]);
    assert_eq!(
        convo_ids(&changes),
        vec!["c1".to_string()],
        "convo c1 must be emitted (child-first order); got {:?}",
        changes
            .iter()
            .map(|c| (c.table.clone(), c.change_type, c.row.is_some()))
            .collect::<Vec<_>>(),
    );
}
