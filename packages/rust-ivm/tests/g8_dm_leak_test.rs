//! Regression guard for the channels/DM read-permission shapes that the ART G8
//! investigation exercised. These assert rust EVALUATES the shapes correctly
//! (no over-emission) — the eventual G8 root cause was a transient poke-ordering
//! artifact in the full concurrent oracle run, NOT a rust eval divergence: driven
//! identically, rust and TS produce identical row sets. Kept as positive
//! invariants so a future change can't regress permission enforcement.
//!
//! Shape 1 (this test's companion below): a correlated EXISTS on
//! channel_user_status filtered `userId = me AND isClosed = false AND
//! isDeleted = false`. A PRIVATE channel whose only status row belongs to a
//! DIFFERENT user must NOT be emitted to `me` — i.e. the EXISTS child predicate
//! (userId=me) is enforced, not just the join key (channelId).

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

/// The channels read-permission (channels-acl.ts canSelect, member branch)
/// compiled into `userAllChannels`:
///   channels WHERE workspaceId = <me-ws>
///     AND ( visibility = 'PUBLIC' OR EXISTS(participants p WHERE p.userId = me) )
/// The leaked DM is workspaceId != me-ws, PRIVATE, me not a participant, so the
/// top-level workspaceId conjunct alone must exclude it.
#[test]
fn user_all_channels_acl_excludes_cross_workspace_dm() {
    let me = "cms5zzgo";
    let other = "cms5vksku";
    let ws_me = "cms5zzf8s";
    let ws_other = "cms5vks5c";

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

    let ch = |id: &str, ws: &str, vis: &str| {
        add_row(
            &channels,
            &[
                ("id", Value::Str(id.into())),
                ("workspaceId", Value::Str(ws.into())),
                ("visibility", Value::Str(vis.into())),
            ],
        )
    };
    ch("dm1", ws_other, "PRIVATE"); // the leak: other ws, private, me not a member
    ch("pub_other", ws_other, "PUBLIC"); // other ws, public → excluded by workspaceId
    ch("mine_pub", ws_me, "PUBLIC"); // my ws, public → included
    ch("mine_priv_part", ws_me, "PRIVATE"); // my ws, private, me a member → included
    ch("mine_priv_nopart", ws_me, "PRIVATE"); // my ws, private, me NOT a member → excluded

    let cp = |id: &str, channel: &str, user: &str| {
        add_row(
            &channel_participants,
            &[
                ("id", Value::Str(id.into())),
                ("channelId", Value::Str(channel.into())),
                ("userId", Value::Str(user.into())),
            ],
        )
    };
    cp("cp_other", "dm1", other); // only `other` is in the DM
    cp("cp_me", "mine_priv_part", me);

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(channels);
    engine.register_source(channel_participants);

    let participants = related_subquery(
        "participants",
        "channel_participants",
        &["id"],
        &["channelId"],
        Some(simple("userId", "=", Value::Str(me.into()))),
    );
    let ast = Ast {
        schema: None,
        table: "channels".to_string(),
        alias: None,
        where_clause: Some(Condition::And(vec![
            simple("workspaceId", "=", Value::Str(ws_me.into())),
            Condition::Or(vec![
                simple("visibility", "=", Value::Str("PUBLIC".into())),
                exists(participants),
            ]),
        ])),
        related: Vec::new(),
        limit: None,
        order_by: None,
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);
    let mut ids: Vec<String> = results[0]
        .changes
        .iter()
        .filter_map(|c| match c.row.as_ref()?.get("id")? {
            Value::Str(s) => Some(s.to_string()),
            _ => None,
        })
        .collect();
    ids.sort();
    ids.dedup();
    println!("userAllChannels emitted ids: {ids:?}");

    for good in ["mine_pub", "mine_priv_part"] {
        assert!(ids.contains(&good.to_string()), "{good} must be emitted");
    }
    for bad in ["dm1", "pub_other", "mine_priv_nopart"] {
        assert!(
            !ids.contains(&bad.to_string()),
            "{bad} must NOT leak — got {ids:?}"
        );
    }
}

#[test]
fn dm_channel_does_not_leak_to_non_participant() {
    let me = "cms5zzgo"; // ART user, NOT a participant of the DM
    let other = "cms5vksku"; // real participant of the DM

    let channels = make_source(
        "channels",
        &[
            ("id", ColumnType::String { optional: false }),
            ("scopeType", ColumnType::String { optional: false }),
            ("visibility", ColumnType::String { optional: false }),
        ],
        &["id"],
    );

    let channel_user_status = make_source(
        "channel_user_status",
        &[
            ("id", ColumnType::String { optional: false }),
            ("channelId", ColumnType::String { optional: false }),
            ("userId", ColumnType::String { optional: false }),
            ("isClosed", ColumnType::Boolean { optional: false }),
            ("isDeleted", ColumnType::Boolean { optional: false }),
        ],
        &["id"],
    );

    // DM the querying user is NOT part of (only `other` has a status row).
    add_row(
        &channels,
        &[
            ("id", Value::Str("dm1".into())),
            ("scopeType", Value::Str("DM".into())),
            ("visibility", Value::Str("PRIVATE".into())),
        ],
    );
    // A channel the querying user IS part of — control that must be emitted.
    add_row(
        &channels,
        &[
            ("id", Value::Str("ch_me".into())),
            ("scopeType", Value::Str("DEFAULT".into())),
            ("visibility", Value::Str("PUBLIC".into())),
        ],
    );

    // Only `other`'s status row references the DM. The join key (channelId=dm1)
    // matches, but the child predicate userId=me must reject it.
    add_row(
        &channel_user_status,
        &[
            ("id", Value::Str("cus_other".into())),
            ("channelId", Value::Str("dm1".into())),
            ("userId", Value::Str(other.into())),
            ("isClosed", Value::Bool(false)),
            ("isDeleted", Value::Bool(false)),
        ],
    );
    add_row(
        &channel_user_status,
        &[
            ("id", Value::Str("cus_me".into())),
            ("channelId", Value::Str("ch_me".into())),
            ("userId", Value::Str(me.into())),
            ("isClosed", Value::Bool(false)),
            ("isDeleted", Value::Bool(false)),
        ],
    );

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(channels);
    engine.register_source(channel_user_status);

    // channels WHERE EXISTS(participantsStatus WHERE
    //   userId = me AND isClosed = false AND isDeleted = false)
    let participants_status = related_subquery(
        "participantsStatus",
        "channel_user_status",
        &["id"],
        &["channelId"],
        Some(Condition::And(vec![
            simple("userId", "=", Value::Str(me.into())),
            simple("isClosed", "=", Value::Bool(false)),
            simple("isDeleted", "=", Value::Bool(false)),
        ])),
    );

    let ast = Ast {
        schema: None,
        table: "channels".to_string(),
        alias: None,
        where_clause: Some(exists(participants_status)),
        related: Vec::new(),
        limit: None,
        order_by: None,
        start: None,
    };

    let results = engine.add_queries(&[QuerySpec {
        query_id: "q1".to_string(),
        ast,
    }]);

    let mut ids: Vec<String> = results[0]
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
    ids.sort();

    println!("emitted channel ids: {ids:?}");

    assert!(
        ids.contains(&"ch_me".to_string()),
        "ch_me (me has a matching status row) must be emitted"
    );
    assert!(
        !ids.contains(&"dm1".to_string()),
        "dm1 (private DM, me is NOT a participant) must NOT leak — got {ids:?}"
    );
}
