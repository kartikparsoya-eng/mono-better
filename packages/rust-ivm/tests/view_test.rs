//! Tests for view-apply-change: add, remove, edit, child changes.
//!
//! Key semantic: `apply_change(parent, change, schema, relationship, format)`:
//! - `schema` = the schema of the rows IN the relationship (child/target schema)
//! - `relationship` = the name of the relationship in the parent entry
//! - `format` = the format OF this relationship (singular at top level, nested formats inside)

use rustc_hash::FxHashMap;

use rust_ivm::ivm::data::{make_comparator, Row, SortOrder, Value};
use rust_ivm::ivm::schema::{ColumnType, SourceSchema, System};
use rust_ivm::ivm::stream::rel_from_vec;
use rust_ivm::ivm::view::{
    apply_change, apply_changes, change_to_view_change, empty_root_entry, default_format,
    Format, View, ViewChange, ViewNode,
};
use rust_ivm::ivm::change::{make_add_change, make_remove_change};
use rust_ivm::ivm::data::Node;

use std::sync::Arc;
use std::collections::HashMap;

fn make_row(pairs: &[(&str, Value)]) -> Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

fn make_schema(table: &str, pk: &[&str]) -> SourceSchema {
    let order: SortOrder = Arc::new(
        pk.iter()
            .map(|k| [k.to_string(), "asc".to_string()])
            .collect(),
    );
    let comparator = make_comparator(order.clone(), false);

    let columns: HashMap<String, ColumnType> = pk
        .iter()
        .map(|c| (c.to_string(), ColumnType::Number { optional: false }))
        .collect();

    SourceSchema {
        table_name: table.to_string(),
        columns,
        primary_key: pk.iter().map(|s| s.to_string()).collect(),
        relationships: HashMap::new(),
        relationship_order: Vec::new(),
        is_hidden: false,
        system: System::Client,
        compare_rows: comparator,
        sort: Some(order),
    }
}

fn make_node(row: Row) -> Node {
    Node::new(row)
}

// ===========================================================================
// ADD tests
// ===========================================================================

#[test]
fn test_view_add_singular() {
    let profile_schema = make_schema("profile", &["id"]);
    let format = Format { singular: true, relationships: FxHashMap::default() };

    let root = empty_root_entry();

    let child_row = make_row(&[("id", Value::F64(1.0)), ("bio", Value::Str("hello".into()))]);
    let child_node = make_node(child_row);
    let change = ViewChange::Add { node: ViewNode::Lazy(child_node) };

    let result = apply_change(&root, &change, &profile_schema, "profile", &format, false, false);

    match result.relationships.get("profile") {
        Some(View::Single(entry)) => {
            assert_eq!(entry.ref_count, 1);
            assert_eq!(entry.row.get("bio"), Some(&Value::Str("hello".into())));
        }
        _ => panic!("Expected single entry in profile relationship"),
    }
}

#[test]
fn test_view_add_plural() {
    let post_schema = make_schema("post", &["id"]);
    let format = default_format();

    let root = empty_root_entry();

    let post1 = make_node(make_row(&[("id", Value::F64(1.0)), ("title", Value::Str("first".into()))]));
    let result = apply_change(&root, &ViewChange::Add { node: ViewNode::Lazy(post1) }, &post_schema, "posts", &format, false, false);

    let post2 = make_node(make_row(&[("id", Value::F64(2.0)), ("title", Value::Str("second".into()))]));
    let result = apply_change(&result, &ViewChange::Add { node: ViewNode::Lazy(post2) }, &post_schema, "posts", &format, false, false);

    match result.relationships.get("posts") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].row.get("title"), Some(&Value::Str("first".into())));
            assert_eq!(entries[1].row.get("title"), Some(&Value::Str("second".into())));
        }
        _ => panic!("Expected list with 2 entries"),
    }
}

#[test]
fn test_view_add_duplicate_increments_refcount() {
    let post_schema = make_schema("post", &["id"]);
    let format = default_format();

    let root = empty_root_entry();
    let row = make_row(&[("id", Value::F64(1.0)), ("title", Value::Str("dup".into()))]);

    let result = apply_change(&root, &ViewChange::Add { node: ViewNode::Lazy(make_node(row.clone())) }, &post_schema, "posts", &format, false, false);
    let result = apply_change(&result, &ViewChange::Add { node: ViewNode::Lazy(make_node(row.clone())) }, &post_schema, "posts", &format, false, false);

    match result.relationships.get("posts") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].ref_count, 2);
        }
        _ => panic!("Expected 1 entry with ref_count 2"),
    }
}

// ===========================================================================
// REMOVE tests
// ===========================================================================

#[test]
fn test_view_remove_singular() {
    let profile_schema = make_schema("profile", &["id"]);
    let format = Format { singular: true, relationships: FxHashMap::default() };

    let root = empty_root_entry();
    let row = make_row(&[("id", Value::F64(1.0)), ("bio", Value::Str("hello".into()))]);

    let result = apply_change(&root, &ViewChange::Add { node: ViewNode::Lazy(make_node(row.clone())) }, &profile_schema, "profile", &format, false, false);
    let result = apply_change(&result, &ViewChange::Remove { node: ViewNode::Lazy(make_node(row)) }, &profile_schema, "profile", &format, false, false);

    match result.relationships.get("profile") {
        Some(View::None) | None => {} // ok — removed
        Some(View::Single(_)) => panic!("Expected profile to be removed"),
        Some(View::List(_)) => panic!("Expected profile to be None, not List"),
    }
}

#[test]
fn test_view_remove_plural() {
    let post_schema = make_schema("post", &["id"]);
    let format = default_format();

    let root = empty_root_entry();
    let row1 = make_row(&[("id", Value::F64(1.0)), ("title", Value::Str("first".into()))]);
    let row2 = make_row(&[("id", Value::F64(2.0)), ("title", Value::Str("second".into()))]);

    let result = apply_change(&root, &ViewChange::Add { node: ViewNode::Lazy(make_node(row1.clone())) }, &post_schema, "posts", &format, false, false);
    let result = apply_change(&result, &ViewChange::Add { node: ViewNode::Lazy(make_node(row2.clone())) }, &post_schema, "posts", &format, false, false);
    let result = apply_change(&result, &ViewChange::Remove { node: ViewNode::Lazy(make_node(row1)) }, &post_schema, "posts", &format, false, false);

    match result.relationships.get("posts") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].row.get("title"), Some(&Value::Str("second".into())));
        }
        _ => panic!("Expected list with 1 entry"),
    }
}

#[test]
fn test_view_remove_decrements_refcount() {
    let post_schema = make_schema("post", &["id"]);
    let format = default_format();

    let root = empty_root_entry();
    let row = make_row(&[("id", Value::F64(1.0)), ("title", Value::Str("dup".into()))]);

    let result = apply_change(&root, &ViewChange::Add { node: ViewNode::Lazy(make_node(row.clone())) }, &post_schema, "posts", &format, false, false);
    let result = apply_change(&result, &ViewChange::Add { node: ViewNode::Lazy(make_node(row.clone())) }, &post_schema, "posts", &format, false, false);
    let result = apply_change(&result, &ViewChange::Remove { node: ViewNode::Lazy(make_node(row)) }, &post_schema, "posts", &format, false, false);

    match result.relationships.get("posts") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].ref_count, 1);
        }
        _ => panic!("Expected 1 entry with ref_count 1"),
    }
}

// ===========================================================================
// EDIT tests
// ===========================================================================

#[test]
fn test_view_edit_in_place() {
    let post_schema = make_schema("post", &["id"]);
    let format = default_format();

    let root = empty_root_entry();
    let old_row = make_row(&[("id", Value::F64(1.0)), ("title", Value::Str("old".into()))]);
    let result = apply_change(&root, &ViewChange::Add { node: ViewNode::Lazy(make_node(old_row.clone())) }, &post_schema, "posts", &format, false, false);

    let new_row = make_row(&[("id", Value::F64(1.0)), ("title", Value::Str("new".into()))]);
    let change = ViewChange::Edit {
        node: rust_ivm::ivm::view::RowOnlyNode { row: new_row },
        old_node: rust_ivm::ivm::view::RowOnlyNode { row: old_row },
    };

    let result = apply_change(&result, &change, &post_schema, "posts", &format, false, false);

    match result.relationships.get("posts") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].row.get("title"), Some(&Value::Str("new".into())));
        }
        _ => panic!("Expected 1 entry with updated title"),
    }
}

#[test]
fn test_view_edit_moves_position() {
    let post_schema = make_schema("post", &["id"]);
    let format = default_format();

    let root = empty_root_entry();

    // Add posts [1, 3, 5]
    let mut result = root;
    for i in [1, 3, 5] {
        let r = make_row(&[("id", Value::F64(i as f64)), ("title", Value::Str(format!("post{}", i).into()))]);
        result = apply_change(&result, &ViewChange::Add { node: ViewNode::Lazy(make_node(r)) }, &post_schema, "posts", &format, false, false);
    }

    // Edit id=1 → id=2 (changes sort key, moves position)
    let old_row = make_row(&[("id", Value::F64(1.0)), ("title", Value::Str("post1".into()))]);
    let new_row = make_row(&[("id", Value::F64(2.0)), ("title", Value::Str("post1-edited".into()))]);
    let change = ViewChange::Edit {
        node: rust_ivm::ivm::view::RowOnlyNode { row: new_row },
        old_node: rust_ivm::ivm::view::RowOnlyNode { row: old_row },
    };

    result = apply_change(&result, &change, &post_schema, "posts", &format, false, false);

    match result.relationships.get("posts") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 3);
            let ids: Vec<f64> = entries.iter()
                .map(|e| match e.row.get("id") {
                    Some(Value::F64(n)) => *n,
                    _ => panic!("expected number"),
                })
                .collect();
            assert_eq!(ids, vec![2.0, 3.0, 5.0]); // sorted by id
        }
        _ => panic!("Expected list with 3 entries"),
    }
}

// ===========================================================================
// CHILD change (nested) tests
// ===========================================================================

#[test]
fn test_view_child_change() {
    // posts → comments (nested)
    let comment_schema = make_schema("comment", &["id"]);
    let post_schema = make_schema("post", &["id"]);
    let post_schema = post_schema.with_relationship("comments", comment_schema, false, System::Client);

    // Format for the "posts" relationship: plural, with nested "comments" format
    let mut format = default_format();
    format.relationships.insert("comments".to_string(), default_format());

    let root = empty_root_entry();

    // Add a post
    let post_row = make_row(&[("id", Value::F64(1.0)), ("title", Value::Str("hello".into()))]);
    let result = apply_change(&root, &ViewChange::Add { node: ViewNode::Lazy(make_node(post_row)) }, &post_schema, "posts", &format, false, false);

    // Add a comment to the post via CHILD change
    let comment_row = make_row(&[("id", Value::F64(10.0)), ("text", Value::Str("nice".into()))]);
    let comment_node = make_node(comment_row);

    let child_change = ViewChange::Child {
        node: rust_ivm::ivm::view::RowOnlyNode { row: make_row(&[("id", Value::F64(1.0))]) },
        child: rust_ivm::ivm::view::ChildViewChange {
            relationship_name: "comments".to_string(),
            change: Box::new(ViewChange::Add { node: ViewNode::Lazy(comment_node) }),
        },
    };

    let result = apply_change(&result, &child_change, &post_schema, "posts", &format, false, false);

    match result.relationships.get("posts") {
        Some(View::List(posts)) => {
            assert_eq!(posts.len(), 1);
            match &posts[0].relationships.get("comments") {
                Some(View::List(comments)) => {
                    assert_eq!(comments.len(), 1);
                    assert_eq!(comments[0].row.get("text"), Some(&Value::Str("nice".into())));
                }
                _ => panic!("Expected comments list with 1 entry"),
            }
        }
        _ => panic!("Expected posts list with 1 entry"),
    }
}

// ===========================================================================
// change_to_view_change conversion
// ===========================================================================

#[test]
fn test_change_to_view_change_add() {
    let node = make_node(make_row(&[("id", Value::F64(1.0))]));
    let change = make_add_change(node);
    let view_change = change_to_view_change(&change);
    match view_change {
        ViewChange::Add { node: ViewNode::Lazy(n) } => {
            assert_eq!(n.row.get("id"), Some(&Value::F64(1.0)));
        }
        _ => panic!("Expected Add with Lazy node"),
    }
}

#[test]
fn test_change_to_view_change_remove() {
    let node = make_node(make_row(&[("id", Value::F64(1.0))]));
    let change = make_remove_change(node);
    let view_change = change_to_view_change(&change);
    match view_change {
        ViewChange::Remove { node: ViewNode::Lazy(n) } => {
            assert_eq!(n.row.get("id"), Some(&Value::F64(1.0)));
        }
        _ => panic!("Expected Remove with Lazy node"),
    }
}

// ===========================================================================
// apply_changes batch
// ===========================================================================

#[test]
fn test_apply_changes_batch() {
    let post_schema = make_schema("post", &["id"]);
    let format = default_format();

    let root = empty_root_entry();

    let changes = vec![
        ViewChange::Add { node: ViewNode::Lazy(make_node(make_row(&[("id", Value::F64(1.0)), ("title", Value::Str("first".into()))]))) },
        ViewChange::Add { node: ViewNode::Lazy(make_node(make_row(&[("id", Value::F64(2.0)), ("title", Value::Str("second".into()))]))) },
        ViewChange::Remove { node: ViewNode::Lazy(make_node(make_row(&[("id", Value::F64(1.0)), ("title", Value::Str("first".into()))]))) },
    ];

    let result = apply_changes(&root, &changes, &post_schema, "posts", &format, false, false);

    match result.relationships.get("posts") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].row.get("title"), Some(&Value::Str("second".into())));
        }
        _ => panic!("Expected 1 entry after batch"),
    }
}

// ===========================================================================
// ExpandedNode support
// ===========================================================================

#[test]
fn test_view_add_expanded_node() {
    let post_schema = make_schema("post", &["id"]);
    let format = default_format();

    let root = empty_root_entry();

    let expanded = rust_ivm::ivm::view::ExpandedNode {
        row: make_row(&[("id", Value::F64(1.0)), ("title", Value::Str("expanded".into()))]),
        relationships: FxHashMap::default(),
    };

    let result = apply_change(&root, &ViewChange::Add { node: ViewNode::Expanded(expanded) }, &post_schema, "posts", &format, false, false);

    match result.relationships.get("posts") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].row.get("title"), Some(&Value::Str("expanded".into())));
        }
        _ => panic!("Expected 1 expanded entry"),
    }
}

// ===========================================================================
// Nested relationship initialization on ADD
// ===========================================================================

#[test]
fn test_view_add_with_nested_relationship() {
    // post → comments (nested)
    let comment_schema = make_schema("comment", &["id"]);
    let post_schema = make_schema("post", &["id"]);
    let post_schema = post_schema.with_relationship("comments", comment_schema, false, System::Client);

    let mut format = default_format();
    format.relationships.insert("comments".to_string(), default_format());

    let root = empty_root_entry();

    // Create a post node with a comments relationship stream
    let comment_node = make_node(make_row(&[("id", Value::F64(100.0)), ("text", Value::Str("first comment".into()))]));
    let mut post_node = make_node(make_row(&[("id", Value::F64(1.0)), ("title", Value::Str("hello".into()))]));
    post_node = post_node.set_relationship("comments", rel_from_vec(vec![comment_node]));

    let result = apply_change(&root, &ViewChange::Add { node: ViewNode::Lazy(post_node) }, &post_schema, "posts", &format, false, false);

    match result.relationships.get("posts") {
        Some(View::List(posts)) => {
            assert_eq!(posts.len(), 1);
            assert_eq!(posts[0].row.get("title"), Some(&Value::Str("hello".into())));
            match posts[0].relationships.get("comments") {
                Some(View::List(comments)) => {
                    assert_eq!(comments.len(), 1);
                    assert_eq!(comments[0].row.get("text"), Some(&Value::Str("first comment".into())));
                }
                _ => panic!("Expected comments list"),
            }
        }
        _ => panic!("Expected posts list"),
    }
}

// ===========================================================================
// withIDs
// ===========================================================================

#[test]
fn test_view_add_with_ids() {
    let post_schema = make_schema("post", &["id"]);
    let format = default_format();

    let root = empty_root_entry();

    let post = make_node(make_row(&[("id", Value::F64(42.0)), ("title", Value::Str("tagged".into()))]));
    let result = apply_change(&root, &ViewChange::Add { node: ViewNode::Lazy(post) }, &post_schema, "posts", &format, true, false);

    match result.relationships.get("posts") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 1);
            assert!(entries[0].id.is_some());
            assert_eq!(entries[0].id.as_ref().unwrap(), "42");
        }
        _ => panic!("Expected 1 entry with ID"),
    }
}

#[test]
fn test_view_add_with_ids_compound_pk() {
    let post_schema = make_schema("post", &["author_id", "post_id"]);
    let format = default_format();

    let root = empty_root_entry();

    let post = make_node(make_row(&[
        ("author_id", Value::F64(1.0)),
        ("post_id", Value::F64(2.0)),
        ("title", Value::Str("compound".into())),
    ]));
    let result = apply_change(&root, &ViewChange::Add { node: ViewNode::Lazy(post) }, &post_schema, "posts", &format, true, false);

    match result.relationships.get("posts") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 1);
            assert!(entries[0].id.is_some());
            assert_eq!(entries[0].id.as_ref().unwrap(), "[1,2]");
        }
        _ => panic!("Expected 1 entry with compound ID"),
    }
}
