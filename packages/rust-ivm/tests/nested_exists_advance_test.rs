//! G15 repro: Two-level nested EXISTS where a NEW parent, its intermediate
//! EXISTS-enabling child, and the leaf EXISTS-enabling grandchild are all
//! added in the SAME advance.
//!
//! Real production shape (channelConversationsPaginatedV3):
//!   conversations WHERE channelId = X
//!     AND EXISTS(channels WHERE id = conversations.channelId
//!       AND EXISTS(channel_participants WHERE userId = "user1" AND channelId = X))
//!
//! joinChannel mutation inserts:
//!   1. channel_participants (userId=user1, channelId=X) — flips inner EXISTS
//!   2. conversations (channelId=X) — the row we want to see emitted
//! Both in the same advance. The live view must gain the conversation,
//! exactly as a fresh hydrate would.

use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashMap;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery,
    SimpleCondition, ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::change::{make_source_change_add, ChangeType};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;
use rust_ivm::streamer::RowChange;

fn make_source(name: &str, columns: &[(&str, ColumnType)], pk: &[&str]) -> Rc<RefCell<MemorySource>> {
    let cols: HashMap<String, ColumnType> = columns
        .iter()
        .map(|(n, t)| (n.to_string(), t.clone()))
        .collect();
    Rc::new(RefCell::new(MemorySource::new(
        name,
        cols,
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

fn simple(col: &str, op: &str, val: &str) -> Condition {
    Condition::Simple(SimpleCondition {
        op: op.to_string(),
        left: ValuePosition::Column { name: col.to_string() },
        right: ValuePosition::Literal { value: Value::Str(val.into()) },
    })
}

fn exists(rel: RelatedSubquery) -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: rel,
        op: "EXISTS".to_string(),
        flip: Some(false),
        scalar: false,
                plan_id: None,
    })
}

fn related_subquery(
    alias: &str,
    table: &str,
    parent_key: &[&str],
    child_key: &[&str],
    where_clause: Option<Condition>,
) -> RelatedSubquery {
    RelatedSubquery {
        subquery: Box::new(Ast {
            schema: None,
            table: table.to_string(),
            alias: Some(alias.to_string()),
            where_clause,
            related: Vec::new(),
            limit: None,
            order_by: None,
            start: None,
        }),
        relationship_name: alias.to_string(),
        parent_key: parent_key.iter().map(|s| s.to_string()).collect(),
        child_key: child_key.iter().map(|s| s.to_string()).collect(),
        hidden: false,
        system: None,
    }
}

fn convo_ids(changes: &[RowChange]) -> Vec<String> {
    let mut ids: Vec<String> = changes
        .iter()
        .filter(|c| c.table == "conversations" && c.change_type == ChangeType::Add)
        .filter_map(|c| match c.row.as_ref()?.get("id")? {
            Value::Str(s) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

/// Build the production AST:
/// conversations WHERE channelId = "ch1"
///   AND EXISTS(channels WHERE id = conversations.channelId
///     AND EXISTS(channel_participants WHERE userId = "user1" AND channelId = "ch1"))
fn production_ast() -> Ast {
    // Inner EXISTS: channel_participants WHERE userId = "user1" AND channelId = "ch1"
    let zsubq_participants = related_subquery(
        "zsubq_participants", "channel_participants",
        &["id"], &["channelId"],
        Some(Condition::And(vec![
            simple("userId", "=", "user1"),
            simple("channelId", "=", "ch1"),
        ])),
    );

    // Outer EXISTS: channels WHERE id = conversations.channelId
    //   AND EXISTS(channel_participants ...)
    let zsubq_channel = related_subquery(
        "zsubq_channel", "channels",
        &["channelId"], &["id"],
        Some(Condition::And(vec![
            exists(zsubq_participants),
        ])),
    );

    Ast {
        schema: None,
        table: "conversations".to_string(),
        alias: None,
        where_clause: Some(Condition::And(vec![
            simple("channelId", "=", "ch1"),
            exists(zsubq_channel),
        ])),
        related: Vec::new(),
        limit: None,
        order_by: None,
        start: None,
    }
}

fn setup() -> Engine {
    let channels = make_source("channels", &[
        ("id", ColumnType::String { optional: false }),
    ], &["id"]);

    let channel_participants = make_source("channel_participants", &[
        ("id", ColumnType::String { optional: false }),
        ("channelId", ColumnType::String { optional: false }),
        ("userId", ColumnType::String { optional: false }),
    ], &["id"]);

    let conversations = make_source("conversations", &[
        ("id", ColumnType::String { optional: false }),
        ("channelId", ColumnType::String { optional: false }),
    ], &["id"]);

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(channels);
    engine.register_source(channel_participants);
    engine.register_source(conversations);
    engine.add_queries(&[QuerySpec {
        query_id: "q".to_string(),
        ast: production_ast(),
    }]);
    engine
}

/// Pre-existing channel, new participant + new conversation in same advance.
/// This is the joinChannel case: channel already exists, user joins + conversation created.
#[test]
fn nested_exists_channel_exists_new_participant_new_conversation_same_advance() {
    let mut engine = setup();

    // Pre-populate: channel ch1 already exists (but no participants → inner EXISTS false)
    let _ = engine.advance(&[
        ("channels".to_string(), make_source_change_add(row(&[("id", "ch1")]))),
    ]);

    // Now advance: add participant (flips inner EXISTS) + add conversation (the row we want)
    // Order: participant first, then conversation
    let changes = engine.advance(&[
        ("channel_participants".to_string(), make_source_change_add(row(&[
            ("id", "cp1"), ("channelId", "ch1"), ("userId", "user1"),
        ]))),
        ("conversations".to_string(), make_source_change_add(row(&[
            ("id", "conv1"), ("channelId", "ch1"),
        ]))),
    ]);

    assert_eq!(
        convo_ids(&changes),
        vec!["conv1".to_string()],
        "conv1 must be emitted (inner EXISTS flipped by participant, outer EXISTS passes); \
         got {:?}",
        changes.iter().map(|c| (c.table.clone(), c.change_type)).collect::<Vec<_>>(),
    );
}

/// Same as above but conversation first, then participant.
/// The conversation is added before the participant exists → EXISTS=false at insert time.
/// Then participant is added → inner EXISTS flips → outer EXISTS should flip → conversation emitted.
#[test]
fn nested_exists_conversation_first_then_participant_same_advance() {
    let mut engine = setup();

    // Pre-populate: channel ch1 already exists
    let _ = engine.advance(&[
        ("channels".to_string(), make_source_change_add(row(&[("id", "ch1")]))),
    ]);

    // Advance: conversation first, then participant
    let changes = engine.advance(&[
        ("conversations".to_string(), make_source_change_add(row(&[
            ("id", "conv1"), ("channelId", "ch1"),
        ]))),
        ("channel_participants".to_string(), make_source_change_add(row(&[
            ("id", "cp1"), ("channelId", "ch1"), ("userId", "user1"),
        ]))),
    ]);

    assert_eq!(
        convo_ids(&changes),
        vec!["conv1".to_string()],
        "conv1 must be emitted even when added before the participant (EXISTS should flip); \
         got {:?}",
        changes.iter().map(|c| (c.table.clone(), c.change_type)).collect::<Vec<_>>(),
    );
}

/// All three rows new in same advance: channel + participant + conversation.
/// This is the case where the channel didn't exist before joinChannel.
#[test]
fn nested_exists_all_new_same_advance() {
    let mut engine = setup();

    // All three in one advance, channel first
    let changes = engine.advance(&[
        ("channels".to_string(), make_source_change_add(row(&[("id", "ch1")]))),
        ("channel_participants".to_string(), make_source_change_add(row(&[
            ("id", "cp1"), ("channelId", "ch1"), ("userId", "user1"),
        ]))),
        ("conversations".to_string(), make_source_change_add(row(&[
            ("id", "conv1"), ("channelId", "ch1"),
        ]))),
    ]);

    assert_eq!(
        convo_ids(&changes),
        vec!["conv1".to_string()],
        "conv1 must be emitted when all three are new in one advance; \
         got {:?}",
        changes.iter().map(|c| (c.table.clone(), c.change_type)).collect::<Vec<_>>(),
    );
}

/// All three new, conversation first (hardest case — conversation sees EXISTS=false,
/// then channel is added, then participant flips inner EXISTS, which should flip outer).
#[test]
fn nested_exists_all_new_conversation_first() {
    let mut engine = setup();

    let changes = engine.advance(&[
        ("conversations".to_string(), make_source_change_add(row(&[
            ("id", "conv1"), ("channelId", "ch1"),
        ]))),
        ("channels".to_string(), make_source_change_add(row(&[("id", "ch1")]))),
        ("channel_participants".to_string(), make_source_change_add(row(&[
            ("id", "cp1"), ("channelId", "ch1"), ("userId", "user1"),
        ]))),
    ]);

    assert_eq!(
        convo_ids(&changes),
        vec!["conv1".to_string()],
        "conv1 must be emitted even when conversation is added first (hardest case); \
         got {:?}",
        changes.iter().map(|c| (c.table.clone(), c.change_type)).collect::<Vec<_>>(),
    );
}
