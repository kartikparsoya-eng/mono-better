//! Regression guard for the OR-with-flipped-subquery union pipeline.
//!
//! `apply_filter_with_flips` builds `UnionFanOut -> [branches] -> UnionFanIn`
//! for a `WHERE (cond OR flipped_subquery)`. The wiring was broken:
//!   - `UnionFanOut::set_output` was a no-op  -> fan-out had zero branches,
//!   - the builder never called `UnionFanIn::add_input` -> fan-in had zero
//!     inputs, so `UnionFanIn::fetch` returned empty,
//!   - the builder never called `UnionFanOut::set_fan_in` -> accumulated
//!     branch pushes never collapsed.
//! Net effect: any query with an OR containing a flipped subquery hydrated to
//! ZERO rows (silent data loss). This test builds exactly that shape and
//! asserts the deduplicated union — it fails (empty) before the wiring fix and
//! passes after.

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

fn make_source(name: &str, columns: &[(&str, ColumnType)], pk: &[&str]) -> Rc<RefCell<MemorySource>> {
    let cols: HashMap<String, ColumnType> =
        columns.iter().map(|(n, t)| (n.to_string(), t.clone())).collect();
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
        left: ValuePosition::Column { name: col.to_string() },
        right: ValuePosition::Literal { value: val },
    })
}

/// A FLIPPED EXISTS correlated subquery (drives the UnionFanOut/FlippedJoin path).
fn flipped_exists(
    alias: &str,
    table: &str,
    parent_key: &[&str],
    child_key: &[&str],
    where_clause: Option<Condition>,
) -> Condition {
    Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
        related: RelatedSubquery {
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
            hidden: true, // EXISTS is a filter — its child rows aren't returned
            system: None,
        },
        op: "EXISTS".to_string(),
        flip: true,
        scalar: false,
    })
}

fn hydrated_ids(engine: &mut Engine, ast: Ast) -> Vec<String> {
    let results = engine.add_queries(&[QuerySpec { query_id: "q1".to_string(), ast }]);
    let mut ids: Vec<String> = results[0]
        .changes
        .iter()
        .filter_map(|c| {
            let row = c.row.as_ref()?;
            // Only count `docs` rows (they carry `kind`); the flipped EXISTS
            // relationship may surface `tags` child rows, which aren't the
            // query result we're asserting on.
            row.get("kind")?;
            match row.get("id") {
                Some(Value::Str(s)) => Some(s.to_string()),
                _ => None,
            }
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

#[test]
fn or_with_flipped_subquery_returns_deduped_union() {
    // docs(id, kind); tags(id, docId).
    let docs = make_source(
        "docs",
        &[
            ("id", ColumnType::String { optional: false }),
            ("kind", ColumnType::String { optional: false }),
        ],
        &["id"],
    );
    let tags = make_source(
        "tags",
        &[
            ("id", ColumnType::String { optional: false }),
            ("docId", ColumnType::String { optional: false }),
        ],
        &["id"],
    );

    // doc1: public, no tag       -> matches simple branch only
    // doc2: private, has tag     -> matches flipped-EXISTS branch only
    // doc3: private, no tag      -> matches neither -> excluded
    // doc4: public, has tag      -> matches BOTH -> must appear exactly once
    add_row(&docs, &[("id", Value::Str("doc1".into())), ("kind", Value::Str("public".into()))]);
    add_row(&docs, &[("id", Value::Str("doc2".into())), ("kind", Value::Str("private".into()))]);
    add_row(&docs, &[("id", Value::Str("doc3".into())), ("kind", Value::Str("private".into()))]);
    add_row(&docs, &[("id", Value::Str("doc4".into())), ("kind", Value::Str("public".into()))]);

    add_row(&tags, &[("id", Value::Str("t2".into())), ("docId", Value::Str("doc2".into()))]);
    add_row(&tags, &[("id", Value::Str("t4".into())), ("docId", Value::Str("doc4".into()))]);

    let mut engine = Engine::new(HashMap::new());
    engine.register_source(docs);
    engine.register_source(tags);

    // docs WHERE (kind = 'public' OR EXISTS(tags WHERE tags.docId = docs.id))
    let ast = Ast {
        schema: None,
        table: "docs".to_string(),
        alias: None,
        where_clause: Some(Condition::Or(vec![
            simple("kind", "=", Value::Str("public".into())),
            flipped_exists("zsubq_tags", "tags", &["id"], &["docId"], None),
        ])),
        related: Vec::new(),
        limit: None,
        order_by: None,
        start: None,
    };

    let ids = hydrated_ids(&mut engine, ast);
    assert_eq!(
        ids,
        vec!["doc1".to_string(), "doc2".to_string(), "doc4".to_string()],
        "OR-with-flipped-subquery must hydrate the deduplicated union of both branches \
         (doc3 excluded, doc4 not duplicated)",
    );
}
