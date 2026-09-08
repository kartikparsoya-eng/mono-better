//! Test: Nested EXISTS with OR conditions — reproduces the 67 conversation/channel_stats mismatches.
//!
//! Scenario: conversation_participants with nested EXISTS:
//!   conversation_participants WHERE EXISTS(conversations WHERE EXISTS(channels WHERE
//!     workspaceId = ? AND OR(visibility = 'PUBLIC', EXISTS(channel_participants WHERE userId = ?))))

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery, SimpleCondition, ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::memory_source::MemorySource;
use rust_ivm::ivm::schema::ColumnType;

fn make_source(
    name: &str,
    columns: &[(&str, ColumnType)],
    pk: &[&str],
) -> Rc<RefCell<MemorySource>> {
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

fn add_row(source: &Rc<RefCell<MemorySource>>, pairs: &[(&str, Value)]) {
    let mut m: FxHashMap<String, Value> = FxHashMap::default();
    for (k, v) in pairs {
        m.insert(k.to_string(), v.clone());
    }
    source.borrow_mut().add_row(m);
}

fn simple(col: &str, op: &str, val: Value) -> Condition {
    Condition::Simple(SimpleCondition {
        op: op.to_string(),
        left: ValuePosition::Column {
            name: col.to_string(),
        },
        right: ValuePosition::Literal { value: val },
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

#[test]
fn test_nested_exists_with_or() {
    let channels = make_source(
        "channels",
        &[
            ("id", ColumnType::String { optional: false }),
            ("workspaceId", ColumnType::String { optional: false }),
            ("visibility", ColumnType::String { optional: false }),
        ],
        &["id"],
    );

    let channel_participants = make_source(
        "channel_participants",
        &[
            ("id", ColumnType::String { optional: false }),
            ("channelId", ColumnType::String { optional: false }),
            ("userId", ColumnType::String { optional: false }),
        ],
        &["id"],
    );

    let conversations = make_source(
        "conversations",
        &[
            ("id", ColumnType::String { optional: false }),
            ("channelId", ColumnType::String { optional: false }),
        ],
        &["id"],
    );

    let conversation_participants = make_source(
        "conversation_participants",
        &[
            ("id", ColumnType::String { optional: false }),
            ("conversationId", ColumnType::String { optional: false }),
            ("visibleTo", ColumnType::String { optional: true }),
        ],
        &["id"],
    );

    // Channels: 2 PUBLIC, 1 PRIVATE (with participant), 1 PRIVATE (no participant)
    add_row(
        &channels,
        &[
            ("id", Value::Str("ch1".into())),
            ("workspaceId", Value::Str("ws1".into())),
            ("visibility", Value::Str("PUBLIC".into())),
        ],
    );
    add_row(
        &channels,
        &[
            ("id", Value::Str("ch2".into())),
            ("workspaceId", Value::Str("ws1".into())),
            ("visibility", Value::Str("PUBLIC".into())),
        ],
    );
    add_row(
        &channels,
        &[
            ("id", Value::Str("ch3".into())),
            ("workspaceId", Value::Str("ws1".into())),
            ("visibility", Value::Str("PRIVATE".into())),
        ],
    );
    add_row(
        &channels,
        &[
            ("id", Value::Str("ch4".into())),
            ("workspaceId", Value::Str("ws1".into())),
            ("visibility", Value::Str("PRIVATE".into())),
        ],
    );

    // Channel participants: user1 in ch3 (PRIVATE), NOT in ch4
    add_row(
        &channel_participants,
        &[
            ("id", Value::Str("cp1".into())),
            ("channelId", Value::Str("ch3".into())),
            ("userId", Value::Str("user1".into())),
        ],
    );

    // Conversations: ch1 has 2, ch3 has 1, ch4 has 1
    add_row(
        &conversations,
        &[
            ("id", Value::Str("conv1".into())),
            ("channelId", Value::Str("ch1".into())),
        ],
    );
    add_row(
        &conversations,
        &[
            ("id", Value::Str("conv2".into())),
            ("channelId", Value::Str("ch1".into())),
        ],
    );
    add_row(
        &conversations,
        &[
            ("id", Value::Str("conv3".into())),
            ("channelId", Value::Str("ch3".into())),
        ],
    );
    add_row(
        &conversations,
        &[
            ("id", Value::Str("conv4".into())),
            ("channelId", Value::Str("ch4".into())),
        ],
    );

    // Conversation participants: user1 in conv1, conv3, conv4
    add_row(
        &conversation_participants,
        &[
            ("id", Value::Str("conp1".into())),
            ("conversationId", Value::Str("conv1".into())),
            ("visibleTo", Value::Null),
        ],
    );
    add_row(
        &conversation_participants,
        &[
            ("id", Value::Str("conp2".into())),
            ("conversationId", Value::Str("conv3".into())),
            ("visibleTo", Value::Null),
        ],
    );
    add_row(
        &conversation_participants,
        &[
            ("id", Value::Str("conp3".into())),
            ("conversationId", Value::Str("conv4".into())),
            ("visibleTo", Value::Null),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(channels);
    engine.register_source(channel_participants);
    engine.register_source(conversations);
    engine.register_source(conversation_participants);

    // Build: conversation_participants WHERE EXISTS(conversations WHERE EXISTS(channels WHERE
    //   workspaceId = 'ws1' AND OR(visibility = 'PUBLIC', EXISTS(channel_participants WHERE userId = 'user1'))))
    let zsubq_participants = related_subquery(
        "zsubq_participants",
        "channel_participants",
        &["id"],
        &["channelId"],
        Some(simple("userId", "=", Value::Str("user1".into()))),
    );

    let zsubq_channel = related_subquery(
        "zsubq_channel",
        "channels",
        &["channelId"],
        &["id"],
        Some(Condition::And(vec![
            simple("workspaceId", "=", Value::Str("ws1".into())),
            Condition::Or(vec![
                simple("visibility", "=", Value::Str("PUBLIC".into())),
                exists(zsubq_participants),
            ]),
        ])),
    );

    let zsubq_conversation = related_subquery(
        "zsubq_conversation",
        "conversations",
        &["conversationId"],
        &["id"],
        Some(exists(zsubq_channel)),
    );

    let ast = Ast {
        schema: None,
        table: "conversation_participants".to_string(),
        alias: None,
        where_clause: Some(exists(zsubq_conversation)),
        related: Vec::new(),
        limit: None,
        order_by: None,
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    let ids: Vec<String> = results[0]
        .changes
        .iter()
        .filter_map(|c| {
            let row = c.row.as_ref()?;
            match row.get("id")? {
                Value::Str(s) => Some(s.to_string()),
                _ => None,
            }
        })
        .collect();

    println!("Nested EXISTS result IDs: {:?}", ids);

    // conv1 (ch1=PUBLIC) → EXISTS passes → conp1 should be included
    // conv3 (ch3=PRIVATE+participant) → OR passes → EXISTS passes → conp2 should be included
    // conv4 (ch4=PRIVATE, no participant) → OR fails → EXISTS fails → conp3 should NOT be included
    assert!(
        ids.contains(&"conp1".to_string()),
        "conp1 (conv1, ch1=PUBLIC) should pass"
    );
    assert!(
        ids.contains(&"conp2".to_string()),
        "conp2 (conv3, ch3=PRIVATE+participant) should pass"
    );
    assert!(
        !ids.contains(&"conp3".to_string()),
        "conp3 (conv4, ch4=PRIVATE no participant) should NOT pass"
    );
}

#[test]
fn test_or_with_exists_and_cap_limit() {
    // Test that OR(visibility=PUBLIC, EXISTS(participants)) with Cap(3)
    // produces correct results regardless of source ordering.
    // This tests the ordering hypothesis for the 24+24 mismatch.

    let channels = make_source(
        "channels",
        &[
            ("id", ColumnType::String { optional: false }),
            ("workspaceId", ColumnType::String { optional: false }),
            ("visibility", ColumnType::String { optional: false }),
        ],
        &["id"],
    );

    let channel_participants = make_source(
        "channel_participants",
        &[
            ("id", ColumnType::String { optional: false }),
            ("channelId", ColumnType::String { optional: false }),
            ("userId", ColumnType::String { optional: false }),
        ],
        &["id"],
    );

    // 5 channels: 3 PUBLIC, 2 PRIVATE (1 with participant, 1 without)
    add_row(
        &channels,
        &[
            ("id", Value::Str("ch1".into())),
            ("workspaceId", Value::Str("ws1".into())),
            ("visibility", Value::Str("PUBLIC".into())),
        ],
    );
    add_row(
        &channels,
        &[
            ("id", Value::Str("ch2".into())),
            ("workspaceId", Value::Str("ws1".into())),
            ("visibility", Value::Str("PUBLIC".into())),
        ],
    );
    add_row(
        &channels,
        &[
            ("id", Value::Str("ch3".into())),
            ("workspaceId", Value::Str("ws1".into())),
            ("visibility", Value::Str("PUBLIC".into())),
        ],
    );
    add_row(
        &channels,
        &[
            ("id", Value::Str("ch4".into())),
            ("workspaceId", Value::Str("ws1".into())),
            ("visibility", Value::Str("PRIVATE".into())),
        ],
    );
    add_row(
        &channels,
        &[
            ("id", Value::Str("ch5".into())),
            ("workspaceId", Value::Str("ws1".into())),
            ("visibility", Value::Str("PRIVATE".into())),
        ],
    );

    // user1 in ch4 only
    add_row(
        &channel_participants,
        &[
            ("id", Value::Str("cp1".into())),
            ("channelId", Value::Str("ch4".into())),
            ("userId", Value::Str("user1".into())),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(channels);
    engine.register_source(channel_participants);

    let zsubq_participants = related_subquery(
        "zsubq_participants",
        "channel_participants",
        &["id"],
        &["channelId"],
        Some(simple("userId", "=", Value::Str("user1".into()))),
    );

    let ast = Ast {
        schema: None,
        table: "channels".to_string(),
        alias: None,
        where_clause: Some(Condition::And(vec![
            simple("workspaceId", "=", Value::Str("ws1".into())),
            Condition::Or(vec![
                simple("visibility", "=", Value::Str("PUBLIC".into())),
                exists(zsubq_participants),
            ]),
        ])),
        related: Vec::new(),
        limit: Some(3), // Cap with limit 3
        order_by: None,
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    let ids: Vec<String> = results[0]
        .changes
        .iter()
        .filter_map(|c| {
            let row = c.row.as_ref()?;
            match row.get("id")? {
                Value::Str(s) => Some(s.to_string()),
                _ => None,
            }
        })
        .collect();

    println!("OR with Cap(3) result IDs: {:?}", ids);

    // 4 channels pass: ch1, ch2, ch3 (PUBLIC), ch4 (PRIVATE+participant)
    // Cap(3) takes first 3. Since source is unordered (use_cap=true, sort=None),
    // the 3 selected depend on source order.
    // But ALL 4 should be valid — the test just checks that exactly 3 are returned.
    assert_eq!(
        results[0].changes.len(),
        3,
        "Cap(3) should return exactly 3 rows"
    );

    // All returned IDs should be from the valid set
    let valid = ["ch1", "ch2", "ch3", "ch4"];
    for id in &ids {
        assert!(
            valid.contains(&id.as_str()),
            "Unexpected channel ID: {}",
            id
        );
    }

    // ch5 should NOT be in results (PRIVATE, no participant)
    assert!(
        !ids.contains(&"ch5".to_string()),
        "ch5 (PRIVATE, no participant) should NOT pass"
    );
}

#[test]
fn test_missing_table_returns_empty() {
    // Test that querying an unregistered table returns 0 rows (EmptyInput).
    // This reproduces the "3 missing tickets" issue.

    let mut engine = Engine::new(HashMap::new());
    // Don't register the "tickets" table

    let ast = Ast {
        schema: None,
        table: "tickets".to_string(),
        alias: None,
        where_clause: None,
        related: Vec::new(),
        limit: None,
        order_by: None,
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    assert_eq!(
        results[0].changes.len(),
        0,
        "Unregistered table should return 0 rows"
    );
}

#[test]
fn test_table_registered_after_query() {
    // Test that if a table IS registered, queries return rows.
    // This verifies the positive case for the tickets issue.

    let tickets = make_source(
        "tickets",
        &[
            ("id", ColumnType::String { optional: false }),
            ("title", ColumnType::String { optional: false }),
        ],
        &["id"],
    );

    add_row(
        &tickets,
        &[
            ("id", Value::Str("t1".into())),
            ("title", Value::Str("Test ticket".into())),
        ],
    );
    add_row(
        &tickets,
        &[
            ("id", Value::Str("t2".into())),
            ("title", Value::Str("Another ticket".into())),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(tickets);

    let ast = Ast {
        schema: None,
        table: "tickets".to_string(),
        alias: None,
        where_clause: None,
        related: Vec::new(),
        limit: None,
        order_by: None,
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    assert_eq!(
        results[0].changes.len(),
        2,
        "Registered table should return 2 rows"
    );
}
