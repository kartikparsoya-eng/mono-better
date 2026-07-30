//! Reproduction for the ART G8 gap: Rust hydrate drops the querying user's own
//! (position-0 / smallest-id) channel_participants row that TS materializes.
//!
//! Models a channel with 4 participants where the ADMIN/"me" row is the smallest
//! id (position 0), matching the real replica data. Tests three query shapes to
//! localise the drop:
//!   A. plain scan          channel_participants.where(channelId=ch1)
//!   B. related only        channels.where(id=ch1).related('participants')
//!   C. exists + related    channels.where(id=ch1)
//!                            .whereExists('participants', p.where(userId=me))
//!                            .related('participants')     <- browsableChannels shape

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rustc_hash::FxHashMap;

use rust_ivm::builder::ast::{
    Ast, Condition, CorrelatedSubqueryCondition, RelatedSubquery, SimpleCondition, ValuePosition,
};
use rust_ivm::engine::{Engine, QuerySpec};
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;

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

fn s(v: &str) -> Value {
    Value::Str(v.into())
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

fn participants_rel_named(alias: &str, where_clause: Option<Condition>) -> RelatedSubquery {
    RelatedSubquery {
        subquery: Box::new(Ast {
            schema: None,
            table: "channel_participants".to_string(),
            alias: Some(alias.to_string()),
            where_clause,
            related: Vec::new(),
            limit: None,
            order_by: None,
            start: None,
        }),
        relationship_name: alias.to_string(),
        parent_key: vec!["id".to_string()],
        child_key: vec!["channelId".to_string()],
        hidden: false,
        system: None,
    }
}

fn participants_rel(where_clause: Option<Condition>) -> RelatedSubquery {
    participants_rel_named("participants", where_clause)
}

fn channels_source() -> Rc<RefCell<MemorySource>> {
    make_source(
        "channels",
        &[
            ("id", ColumnType::String { optional: false }),
            ("visibility", ColumnType::String { optional: false }),
        ],
        &["id"],
    )
}

fn participants_source() -> Rc<RefCell<MemorySource>> {
    make_source(
        "channel_participants",
        &[
            ("id", ColumnType::String { optional: false }),
            ("channelId", ColumnType::String { optional: false }),
            ("userId", ColumnType::String { optional: false }),
            ("role", ColumnType::String { optional: false }),
        ],
        &["id"],
    )
}

/// 4 participants for ch1; position-0 (smallest id "cp0") is the ADMIN/"me" row.
fn seed_participants(src: &Rc<RefCell<MemorySource>>) {
    add_row(
        src,
        &[
            ("id", s("cp0")),
            ("channelId", s("ch1")),
            ("userId", s("me")),
            ("role", s("ADMIN")),
        ],
    );
    add_row(
        src,
        &[
            ("id", s("cp1")),
            ("channelId", s("ch1")),
            ("userId", s("u1")),
            ("role", s("MEMBER")),
        ],
    );
    add_row(
        src,
        &[
            ("id", s("cp2")),
            ("channelId", s("ch1")),
            ("userId", s("u2")),
            ("role", s("MEMBER")),
        ],
    );
    add_row(
        src,
        &[
            ("id", s("cp3")),
            ("channelId", s("ch1")),
            ("userId", s("u3")),
            ("role", s("MEMBER")),
        ],
    );
}

/// Client-visible channel_participants row ids (excludes hidden EXISTS-companion
/// rows, which the real client discards).
fn participant_ids(changes: &[rust_ivm::streamer::RowChange]) -> Vec<String> {
    changes
        .iter()
        .filter(|c| c.table == "channel_participants" && !c.is_hidden)
        .filter_map(|c| match c.row.as_ref()?.get("id")? {
            Value::Str(v) => Some(v.to_string()),
            _ => None,
        })
        .collect()
}

// A. Plain scan: channel_participants.where(channelId=ch1) -> all 4.
#[test]
fn repro_a_plain_scan() {
    let cp = participants_source();
    seed_participants(&cp);
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(cp);

    let ast = Ast {
        schema: None,
        table: "channel_participants".to_string(),
        alias: None,
        where_clause: Some(simple("channelId", "=", s("ch1"))),
        related: Vec::new(),
        limit: None,
        order_by: None,
        start: None,
    };
    let results = engine.add_queries(&[QuerySpec {
        query_id: "a".into(),
        ast,
    }]);
    let mut ids = participant_ids(&results[0].changes);
    ids.sort();
    assert_eq!(
        ids,
        vec!["cp0", "cp1", "cp2", "cp3"],
        "plain scan must include cp0 (position 0)"
    );
}

// B. Related only: channels.where(id=ch1).related('participants') -> child has all 4.
#[test]
fn repro_b_related_only() {
    let channels = channels_source();
    add_row(&channels, &[("id", s("ch1")), ("visibility", s("PRIVATE"))]);
    let cp = participants_source();
    seed_participants(&cp);
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(channels);
    engine.register_source(cp);

    let ast = Ast {
        schema: None,
        table: "channels".to_string(),
        alias: None,
        where_clause: Some(simple("id", "=", s("ch1"))),
        related: vec![participants_rel(None)],
        limit: None,
        order_by: None,
        start: None,
    };
    let results = engine.add_queries(&[QuerySpec {
        query_id: "b".into(),
        ast,
    }]);
    let mut ids = participant_ids(&results[0].changes);
    ids.sort();
    assert_eq!(
        ids,
        vec!["cp0", "cp1", "cp2", "cp3"],
        "related output must include cp0 (position 0)"
    );
}

// C. browsableChannels: whereExists('participants', p.where(userId=me)) + related('participants').
#[test]
fn repro_c_exists_plus_related() {
    let channels = channels_source();
    add_row(&channels, &[("id", s("ch1")), ("visibility", s("PRIVATE"))]);
    let cp = participants_source();
    seed_participants(&cp);
    let mut engine = Engine::new(HashMap::new());
    engine.register_source(channels);
    engine.register_source(cp);

    let exists_cond = Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: participants_rel_named("zsubq_participants", Some(simple("userId", "=", s("me")))),
        op: "EXISTS".to_string(),
        flip: Some(false),
        scalar: false,
        plan_id: None,
    });

    let ast = Ast {
        schema: None,
        table: "channels".to_string(),
        alias: None,
        where_clause: Some(exists_cond),
        related: vec![participants_rel(None)], // unfiltered output branch
        limit: None,
        order_by: None,
        start: None,
    };
    let results = engine.add_queries(&[QuerySpec {
        query_id: "c".into(),
        ast,
    }]);
    let mut ids = participant_ids(&results[0].changes);
    ids.sort();
    assert_eq!(
        ids,
        vec!["cp0", "cp1", "cp2", "cp3"],
        "exists+related output must include cp0 (position 0 / me)"
    );
}
