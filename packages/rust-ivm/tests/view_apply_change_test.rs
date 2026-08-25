//! Additional view-apply-change tests ported from TS v1.7.0.
//! Covers: singular format, edit (non-PK + PK), refcount management,
//! children positioning, remove-non-existent panic, multiple entries
//! with nested relationships and compound PKs.

use rustc_hash::FxHashMap;

use rust_ivm::ivm::data::{Node, Row, SortOrder, Value, make_comparator};
use rust_ivm::ivm::schema::{ColumnType, SourceSchema, System};
use rust_ivm::ivm::stream::rel_from_vec;
use rust_ivm::ivm::view::{
    Format, View, ViewChange, ViewNode, apply_change, default_format, empty_root_entry,
};

use std::collections::HashMap;
use std::sync::Arc;

fn make_row(pairs: &[(&str, Value)]) -> Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

fn make_string_schema(table: &str, pk: &[&str], cols: &[(&str, &str)]) -> SourceSchema {
    let order: SortOrder = Arc::new(
        pk.iter()
            .map(|k| [k.to_string(), "asc".to_string()])
            .collect(),
    );
    let comparator = make_comparator(order.clone(), false);
    let columns: HashMap<String, ColumnType> = cols
        .iter()
        .map(|(name, ty)| {
            let ct = match *ty {
                "string" => ColumnType::String { optional: false },
                "number" => ColumnType::Number { optional: false },
                "boolean" => ColumnType::Boolean { optional: false },
                _ => ColumnType::String { optional: true },
            };
            (name.to_string(), ct)
        })
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

fn s(s: &str) -> Value {
    Value::Str(Arc::from(s))
}

// ===========================================================================
// Simple: singular: false — add, add-duplicate, remove, remove-missing
// ===========================================================================

#[test]
fn test_simple_plural_add_remove_refcount() {
    let schema = make_string_schema("event", &["id"], &[("id", "string"), ("name", "string")]);
    let format = default_format();
    let mut root = empty_root_entry();

    let apply = |root: &_, change: &ViewChange| {
        apply_change(root, change, &schema, "", &format, true, false)
    };

    // Add id=1 Aaron
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(make_node(make_row(&[("id", s("1")), ("name", s("Aaron"))]))),
        },
    );
    match root.relationships.get("") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].ref_count, 1);
            assert_eq!(entries[0].id.as_deref(), Some("\"1\""));
        }
        _ => panic!("Expected list with 1 entry"),
    }

    // Add id=2 Greg 5 times → rc=5
    for _ in 0..5 {
        root = apply(
            &root,
            &ViewChange::Add {
                node: ViewNode::Lazy(make_node(make_row(&[("id", s("2")), ("name", s("Greg"))]))),
            },
        );
    }
    match root.relationships.get("") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].ref_count, 1);
            assert_eq!(entries[1].ref_count, 5);
        }
        _ => panic!("Expected list with 2 entries"),
    }

    // Remove id=2 four times → rc=1
    for _ in 0..4 {
        root = apply(
            &root,
            &ViewChange::Remove {
                node: ViewNode::Lazy(make_node(make_row(&[("id", s("2")), ("name", s("Greg"))]))),
            },
        );
    }
    match root.relationships.get("") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[1].ref_count, 1);
        }
        _ => panic!("Expected list with 2 entries"),
    }

    // Remove id=2 one more time → gone
    root = apply(
        &root,
        &ViewChange::Remove {
            node: ViewNode::Lazy(make_node(make_row(&[("id", s("2")), ("name", s("Greg"))]))),
        },
    );
    match root.relationships.get("") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].row.get("id"), Some(&s("1")));
        }
        _ => panic!("Expected list with 1 entry"),
    }
}

#[test]
#[should_panic(expected = "node does not exist")]
fn test_remove_nonexistent_panics() {
    let schema = make_string_schema("event", &["id"], &[("id", "string"), ("name", "string")]);
    let format = default_format();
    let root = empty_root_entry();

    let _ = apply_change(
        &root,
        &ViewChange::Remove {
            node: ViewNode::Lazy(make_node(make_row(&[("id", s("2")), ("name", s("Greg"))]))),
        },
        &schema,
        "",
        &format,
        true,
        false,
    );
}

// ===========================================================================
// Simple: singular: true — add, add-duplicate increments rc, remove
// ===========================================================================

#[test]
fn test_simple_singular_add_remove() {
    let schema = make_string_schema("event", &["id"], &[("id", "string"), ("name", "string")]);
    let format = Format {
        singular: true,
        relationships: FxHashMap::default(),
    };
    let mut root = empty_root_entry();

    let apply = |root: &_, change: &ViewChange| {
        apply_change(root, change, &schema, "", &format, true, false)
    };

    // Add id=1 Aaron
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(make_node(make_row(&[("id", s("1")), ("name", s("Aaron"))]))),
        },
    );
    match root.relationships.get("") {
        Some(View::Single(entry)) => {
            assert_eq!(entry.ref_count, 1);
            assert_eq!(entry.row.get("name"), Some(&s("Aaron")));
        }
        _ => panic!("Expected single entry"),
    }

    // Add again → rc=2
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(make_node(make_row(&[("id", s("1")), ("name", s("Aaron"))]))),
        },
    );
    match root.relationships.get("") {
        Some(View::Single(entry)) => assert_eq!(entry.ref_count, 2),
        _ => panic!("Expected single entry with rc=2"),
    }

    // Remove → rc=1
    root = apply(
        &root,
        &ViewChange::Remove {
            node: ViewNode::Lazy(make_node(make_row(&[("id", s("1")), ("name", s("Aaron"))]))),
        },
    );
    match root.relationships.get("") {
        Some(View::Single(entry)) => assert_eq!(entry.ref_count, 1),
        _ => panic!("Expected single entry with rc=1"),
    }

    // Remove again → gone
    root = apply(
        &root,
        &ViewChange::Remove {
            node: ViewNode::Lazy(make_node(make_row(&[("id", s("1")), ("name", s("Aaron"))]))),
        },
    );
    match root.relationships.get("") {
        Some(View::None) | None => {}
        _ => panic!("Expected None after final remove"),
    }
}

// ===========================================================================
// Edit, singular: false — edit non-PK column, refcount preserved
// ===========================================================================

#[test]
fn test_edit_plural_non_pk() {
    let schema = make_string_schema("event", &["id"], &[("id", "string"), ("name", "string")]);
    let format = default_format();
    let mut root = empty_root_entry();

    let apply = |root: &_, change: &ViewChange| {
        apply_change(root, change, &schema, "", &format, true, false)
    };

    // Add id=1 Aaron
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(make_node(make_row(&[("id", s("1")), ("name", s("Aaron"))]))),
        },
    );

    // Edit name Aaron → Greg (same PK)
    root = apply(
        &root,
        &ViewChange::Edit {
            node: rust_ivm::ivm::view::RowOnlyNode {
                row: make_row(&[("id", s("1")), ("name", s("Greg"))]),
            },
            old_node: rust_ivm::ivm::view::RowOnlyNode {
                row: make_row(&[("id", s("1")), ("name", s("Aaron"))]),
            },
        },
    );
    match root.relationships.get("") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].row.get("name"), Some(&s("Greg")));
            assert_eq!(entries[0].ref_count, 1);
        }
        _ => panic!("Expected 1 entry with edited name"),
    }

    // Add id=1 twice more → rc=3
    for _ in 0..2 {
        root = apply(
            &root,
            &ViewChange::Add {
                node: ViewNode::Lazy(make_node(make_row(&[("id", s("1")), ("name", s("Greg"))]))),
            },
        );
    }
    match root.relationships.get("") {
        Some(View::List(entries)) => assert_eq!(entries[0].ref_count, 3),
        _ => panic!("Expected rc=3"),
    }

    // Edit name Greg → Aaron (rc preserved)
    root = apply(
        &root,
        &ViewChange::Edit {
            node: rust_ivm::ivm::view::RowOnlyNode {
                row: make_row(&[("id", s("1")), ("name", s("Aaron"))]),
            },
            old_node: rust_ivm::ivm::view::RowOnlyNode {
                row: make_row(&[("id", s("1")), ("name", s("Greg"))]),
            },
        },
    );
    match root.relationships.get("") {
        Some(View::List(entries)) => {
            assert_eq!(entries[0].row.get("name"), Some(&s("Aaron")));
            assert_eq!(entries[0].ref_count, 3);
        }
        _ => panic!("Expected rc=3 after edit"),
    }
}

// ===========================================================================
// Edit primary key, singular: false — changing PK moves sort position
// ===========================================================================

#[test]
fn test_edit_plural_primary_key() {
    let schema = make_string_schema("event", &["id"], &[("id", "string"), ("name", "string")]);
    let format = default_format();
    let mut root = empty_root_entry();

    let apply = |root: &_, change: &ViewChange| {
        apply_change(root, change, &schema, "", &format, true, false)
    };

    // Add [id=1, id=3]
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(make_node(make_row(&[("id", s("1")), ("name", s("Aaron"))]))),
        },
    );
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(make_node(make_row(&[
                ("id", s("3")),
                ("name", s("Charlie")),
            ]))),
        },
    );

    // Edit id=1 → id=2 (changes sort key, moves position)
    root = apply(
        &root,
        &ViewChange::Edit {
            node: rust_ivm::ivm::view::RowOnlyNode {
                row: make_row(&[("id", s("2")), ("name", s("Aaron"))]),
            },
            old_node: rust_ivm::ivm::view::RowOnlyNode {
                row: make_row(&[("id", s("1")), ("name", s("Aaron"))]),
            },
        },
    );

    match root.relationships.get("") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[0].row.get("id"), Some(&s("2")));
            assert_eq!(entries[1].row.get("id"), Some(&s("3")));
        }
        _ => panic!("Expected 2 entries with reordered PKs"),
    }
}

// ===========================================================================
// Edit, singular: true
// ===========================================================================

#[test]
fn test_edit_singular_non_pk() {
    let schema = make_string_schema("event", &["id"], &[("id", "string"), ("name", "string")]);
    let format = Format {
        singular: true,
        relationships: FxHashMap::default(),
    };
    let mut root = empty_root_entry();

    let apply = |root: &_, change: &ViewChange| {
        apply_change(root, change, &schema, "", &format, true, false)
    };

    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(make_node(make_row(&[("id", s("1")), ("name", s("Aaron"))]))),
        },
    );
    root = apply(
        &root,
        &ViewChange::Edit {
            node: rust_ivm::ivm::view::RowOnlyNode {
                row: make_row(&[("id", s("1")), ("name", s("Greg"))]),
            },
            old_node: rust_ivm::ivm::view::RowOnlyNode {
                row: make_row(&[("id", s("1")), ("name", s("Aaron"))]),
            },
        },
    );

    match root.relationships.get("") {
        Some(View::Single(entry)) => {
            assert_eq!(entry.row.get("name"), Some(&s("Greg")));
            assert_eq!(entry.ref_count, 1);
        }
        _ => panic!("Expected single entry with edited name"),
    }
}

// ===========================================================================
// Edit primary key, singular: true
// ===========================================================================

#[test]
fn test_edit_singular_primary_key() {
    let schema = make_string_schema("event", &["id"], &[("id", "string"), ("name", "string")]);
    let format = Format {
        singular: true,
        relationships: FxHashMap::default(),
    };
    let mut root = empty_root_entry();

    let apply = |root: &_, change: &ViewChange| {
        apply_change(root, change, &schema, "", &format, true, false)
    };

    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(make_node(make_row(&[("id", s("1")), ("name", s("Aaron"))]))),
        },
    );
    root = apply(
        &root,
        &ViewChange::Edit {
            node: rust_ivm::ivm::view::RowOnlyNode {
                row: make_row(&[("id", s("2")), ("name", s("Greg"))]),
            },
            old_node: rust_ivm::ivm::view::RowOnlyNode {
                row: make_row(&[("id", s("1")), ("name", s("Aaron"))]),
            },
        },
    );

    match root.relationships.get("") {
        Some(View::Single(entry)) => {
            assert_eq!(entry.row.get("id"), Some(&s("2")));
            assert_eq!(entry.row.get("name"), Some(&s("Greg")));
        }
        _ => panic!("Expected single entry with new PK"),
    }
}

// ===========================================================================
// Add with initialized relationships — entry with children placed at
// correct sorted position
// ===========================================================================

fn make_schema_with_children() -> SourceSchema {
    let child_schema = make_string_schema(
        "child",
        &["id"],
        &[("id", "string"), ("parentId", "string")],
    );
    let parent_schema =
        make_string_schema("parent", &["id"], &[("id", "string"), ("name", "string")]);
    parent_schema.with_relationship("children", child_schema, false, System::Client)
}

fn format_with_children() -> Format {
    let mut fmt = default_format();
    fmt.relationships
        .insert("children".to_string(), default_format());
    fmt
}

#[test]
fn test_entry_with_children_correct_position() {
    let schema = make_schema_with_children();
    let format = format_with_children();
    let mut root = empty_root_entry();

    let apply = |root: &_, change: &ViewChange| {
        apply_change(root, change, &schema, "", &format, true, false)
    };

    // Add 'b' Bob with child c1
    let mut bob_node = make_node(make_row(&[("id", s("b")), ("name", s("Bob"))]));
    bob_node = bob_node.set_relationship(
        "children",
        rel_from_vec(vec![make_node(make_row(&[
            ("id", s("c1")),
            ("parentId", s("b")),
        ]))]),
    );
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(bob_node),
        },
    );

    // Add 'd' Dave with child c2
    let mut dave_node = make_node(make_row(&[("id", s("d")), ("name", s("Dave"))]));
    dave_node = dave_node.set_relationship(
        "children",
        rel_from_vec(vec![make_node(make_row(&[
            ("id", s("c2")),
            ("parentId", s("d")),
        ]))]),
    );
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(dave_node),
        },
    );

    // Add 'a' Alice with child c3 — should be inserted at position 0
    let mut alice_node = make_node(make_row(&[("id", s("a")), ("name", s("Alice"))]));
    alice_node = alice_node.set_relationship(
        "children",
        rel_from_vec(vec![make_node(make_row(&[
            ("id", s("c3")),
            ("parentId", s("a")),
        ]))]),
    );
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(alice_node),
        },
    );

    match root.relationships.get("") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 3);
            assert_eq!(entries[0].row.get("id"), Some(&s("a")));
            assert_eq!(entries[1].row.get("id"), Some(&s("b")));
            assert_eq!(entries[2].row.get("id"), Some(&s("d")));

            // Verify children are present
            match entries[0].relationships.get("children") {
                Some(View::List(children)) => {
                    assert_eq!(children.len(), 1);
                    assert_eq!(children[0].row.get("id"), Some(&s("c3")));
                }
                _ => panic!("Expected Alice to have 1 child"),
            }
            match entries[1].relationships.get("children") {
                Some(View::List(children)) => {
                    assert_eq!(children.len(), 1);
                    assert_eq!(children[0].row.get("id"), Some(&s("c1")));
                }
                _ => panic!("Expected Bob to have 1 child"),
            }
            match entries[2].relationships.get("children") {
                Some(View::List(children)) => {
                    assert_eq!(children.len(), 1);
                    assert_eq!(children[0].row.get("id"), Some(&s("c2")));
                }
                _ => panic!("Expected Dave to have 1 child"),
            }
        }
        _ => panic!("Expected list with 3 entries"),
    }
}

#[test]
fn test_entry_inserted_in_middle_with_children() {
    let schema = make_schema_with_children();
    let format = format_with_children();
    let mut root = empty_root_entry();

    let apply = |root: &_, change: &ViewChange| {
        apply_change(root, change, &schema, "", &format, true, false)
    };

    // Add 'a' Alice (no children)
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(make_node(make_row(&[("id", s("a")), ("name", s("Alice"))]))),
        },
    );
    // Add 'c' Charlie (no children)
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(make_node(make_row(&[
                ("id", s("c")),
                ("name", s("Charlie")),
            ]))),
        },
    );

    // Insert 'b' Bob in the middle with 2 children
    let mut bob_node = make_node(make_row(&[("id", s("b")), ("name", s("Bob"))]));
    bob_node = bob_node.set_relationship(
        "children",
        rel_from_vec(vec![
            make_node(make_row(&[("id", s("child1")), ("parentId", s("b"))])),
            make_node(make_row(&[("id", s("child2")), ("parentId", s("b"))])),
        ]),
    );
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(bob_node),
        },
    );

    match root.relationships.get("") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 3);
            assert_eq!(entries[0].row.get("id"), Some(&s("a")));
            assert_eq!(entries[1].row.get("id"), Some(&s("b")));
            assert_eq!(entries[2].row.get("id"), Some(&s("c")));

            match entries[1].relationships.get("children") {
                Some(View::List(children)) => {
                    assert_eq!(children.len(), 2);
                    assert_eq!(children[0].row.get("id"), Some(&s("child1")));
                    assert_eq!(children[1].row.get("id"), Some(&s("child2")));
                }
                _ => panic!("Expected Bob to have 2 children"),
            }
        }
        _ => panic!("Expected list with 3 entries"),
    }
}

// ===========================================================================
// Multiple entries with nested relationships (compound PKs, refcount via
// child changes) — ported from view-apply-change.test.ts "Multiple entries"
// ===========================================================================

fn make_event_schema() -> SourceSchema {
    let athlete_schema = make_string_schema(
        "athlete",
        &["country", "id"],
        &[("id", "string"), ("country", "string"), ("name", "string")],
    );
    let matchup_schema = make_string_schema(
        "matchup",
        &["eventID", "athleteCountry", "athleteID", "disciplineID"],
        &[
            ("eventID", "string"),
            ("athleteCountry", "string"),
            ("athleteID", "string"),
            ("disciplineID", "string"),
        ],
    );
    let matchup_with_athletes =
        matchup_schema.with_relationship("athletes", athlete_schema, false, System::Client);
    let event_schema =
        make_string_schema("event", &["id"], &[("id", "string"), ("name", "string")]);
    event_schema.with_relationship("athletes", matchup_with_athletes, true, System::Client)
}

fn make_event_format(singular_athletes: bool) -> Format {
    // TS shape (view-apply-change.test.ts:86-89, 338-346): the format follows
    // the VISIBLE tree — ONE `athletes` entry for the visible athlete level;
    // the hidden matchup level has no format entry (the outer format passes
    // through it unchanged, view-apply-change.ts:222-262). The previous
    // nested-per-structural-level shape here compensated for the NEW-5 bug.
    let mut fmt = default_format();
    fmt.relationships.insert(
        "athletes".to_string(),
        Format {
            singular: singular_athletes,
            relationships: FxHashMap::default(),
        },
    );
    fmt
}

#[test]
fn test_multiple_entries_plural_athletes() {
    let schema = make_event_schema();
    let format = make_event_format(false);
    let mut root = empty_root_entry();

    let apply = |root: &_, change: &ViewChange| {
        apply_change(root, change, &schema, "", &format, true, false)
    };

    // Add event e1
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(make_node(make_row(&[
                ("id", s("e1")),
                ("name", s("Buffalo Big Board Classic")),
            ]))),
        },
    );

    // Add athlete matchup (e1, USA, a1, d1) with nested athlete (USA, a1, Mason Ho)
    let mut matchup_node = make_node(make_row(&[
        ("eventID", s("e1")),
        ("athleteCountry", s("USA")),
        ("athleteID", s("a1")),
        ("disciplineID", s("d1")),
    ]));
    matchup_node = matchup_node.set_relationship(
        "athletes",
        rel_from_vec(vec![make_node(make_row(&[
            ("country", s("USA")),
            ("id", s("a1")),
            ("name", s("Mason Ho")),
        ]))]),
    );

    let child1 = ViewChange::Child {
        node: rust_ivm::ivm::view::RowOnlyNode {
            row: make_row(&[("id", s("e1")), ("name", s("Buffalo Big Board Classic"))]),
        },
        child: rust_ivm::ivm::view::ChildViewChange {
            relationship_name: "athletes".to_string(),
            change: Box::new(ViewChange::Add {
                node: ViewNode::Lazy(matchup_node),
            }),
        },
    };
    root = apply(&root, &child1);

    // Add another matchup (e1, USA, a1, d2) with same athlete → athlete rc=2
    let mut matchup_node2 = make_node(make_row(&[
        ("eventID", s("e1")),
        ("athleteCountry", s("USA")),
        ("athleteID", s("a1")),
        ("disciplineID", s("d2")),
    ]));
    matchup_node2 = matchup_node2.set_relationship(
        "athletes",
        rel_from_vec(vec![make_node(make_row(&[
            ("country", s("USA")),
            ("id", s("a1")),
            ("name", s("Mason Ho")),
        ]))]),
    );

    let child2 = ViewChange::Child {
        node: rust_ivm::ivm::view::RowOnlyNode {
            row: make_row(&[("id", s("e1")), ("name", s("Buffalo Big Board Classic"))]),
        },
        child: rust_ivm::ivm::view::ChildViewChange {
            relationship_name: "athletes".to_string(),
            change: Box::new(ViewChange::Add {
                node: ViewNode::Lazy(matchup_node2),
            }),
        },
    };
    root = apply(&root, &child2);

    // Verify: hidden matchup schema strips the matchup wrapper and adds
    // athletes directly to the event's "athletes" list. Two matchups with
    // the same athlete → athlete rc=2.
    match root.relationships.get("") {
        Some(View::List(entries)) => {
            assert_eq!(entries.len(), 1);
            match entries[0].relationships.get("athletes") {
                Some(View::List(athletes)) => {
                    assert_eq!(athletes.len(), 1);
                    assert_eq!(athletes[0].ref_count, 2);
                    assert_eq!(athletes[0].row.get("name"), Some(&s("Mason Ho")));
                }
                _ => panic!("Expected athlete list with rc=2"),
            }
        }
        _ => panic!("Expected event list"),
    }

    // Remove matchup d1 → athlete rc=1
    let remove_d1 = ViewChange::Child {
        node: rust_ivm::ivm::view::RowOnlyNode {
            row: make_row(&[("id", s("e1")), ("name", s("Buffalo Big Board Classic"))]),
        },
        child: rust_ivm::ivm::view::ChildViewChange {
            relationship_name: "athletes".to_string(),
            change: Box::new(ViewChange::Remove {
                node: ViewNode::Lazy({
                    let mut n = make_node(make_row(&[
                        ("eventID", s("e1")),
                        ("athleteCountry", s("USA")),
                        ("athleteID", s("a1")),
                        ("disciplineID", s("d1")),
                    ]));
                    n = n.set_relationship(
                        "athletes",
                        rel_from_vec(vec![make_node(make_row(&[
                            ("country", s("USA")),
                            ("id", s("a1")),
                            ("name", s("Mason Ho")),
                        ]))]),
                    );
                    n
                }),
            }),
        },
    };
    root = apply(&root, &remove_d1);

    match root.relationships.get("") {
        Some(View::List(entries)) => match entries[0].relationships.get("athletes") {
            Some(View::List(athletes)) => {
                assert_eq!(athletes.len(), 1);
                assert_eq!(athletes[0].ref_count, 1);
            }
            _ => panic!("Expected athlete with rc=1"),
        },
        _ => panic!("Expected event"),
    }

    // Remove matchup d2 → athletes list empty
    let remove_d2 = ViewChange::Child {
        node: rust_ivm::ivm::view::RowOnlyNode {
            row: make_row(&[("id", s("e1")), ("name", s("Buffalo Big Board Classic"))]),
        },
        child: rust_ivm::ivm::view::ChildViewChange {
            relationship_name: "athletes".to_string(),
            change: Box::new(ViewChange::Remove {
                node: ViewNode::Lazy({
                    let mut n = make_node(make_row(&[
                        ("eventID", s("e1")),
                        ("athleteCountry", s("USA")),
                        ("athleteID", s("a1")),
                        ("disciplineID", s("d2")),
                    ]));
                    n = n.set_relationship(
                        "athletes",
                        rel_from_vec(vec![make_node(make_row(&[
                            ("country", s("USA")),
                            ("id", s("a1")),
                            ("name", s("Mason Ho")),
                        ]))]),
                    );
                    n
                }),
            }),
        },
    };
    root = apply(&root, &remove_d2);

    match root.relationships.get("") {
        Some(View::List(entries)) => match entries[0].relationships.get("athletes") {
            Some(View::List(athletes)) => assert_eq!(athletes.len(), 0),
            _ => panic!("Expected empty athletes list"),
        },
        _ => panic!("Expected event"),
    }
}

#[test]
fn test_multiple_entries_singular_athletes() {
    let schema = make_event_schema();
    let format = make_event_format(true);
    let mut root = empty_root_entry();

    let apply = |root: &_, change: &ViewChange| {
        apply_change(root, change, &schema, "", &format, true, false)
    };

    // Add event e1
    root = apply(
        &root,
        &ViewChange::Add {
            node: ViewNode::Lazy(make_node(make_row(&[
                ("id", s("e1")),
                ("name", s("Buffalo Big Board Classic")),
            ]))),
        },
    );

    // Add two matchups with the same athlete → athlete is singular, rc=2
    for disc in &["d1", "d2"] {
        let mut matchup_node = make_node(make_row(&[
            ("eventID", s("e1")),
            ("athleteCountry", s("USA")),
            ("athleteID", s("a1")),
            ("disciplineID", s(disc)),
        ]));
        matchup_node = matchup_node.set_relationship(
            "athletes",
            rel_from_vec(vec![make_node(make_row(&[
                ("country", s("USA")),
                ("id", s("a1")),
                ("name", s("Mason Ho")),
            ]))]),
        );

        root = apply(
            &root,
            &ViewChange::Child {
                node: rust_ivm::ivm::view::RowOnlyNode {
                    row: make_row(&[("id", s("e1")), ("name", s("Buffalo Big Board Classic"))]),
                },
                child: rust_ivm::ivm::view::ChildViewChange {
                    relationship_name: "athletes".to_string(),
                    change: Box::new(ViewChange::Add {
                        node: ViewNode::Lazy(matchup_node),
                    }),
                },
            },
        );
    }

    // Verify: hidden matchup schema strips the matchup wrapper and adds
    // athletes directly. Athlete format is singular → athlete is Single with rc=2.
    match root.relationships.get("") {
        Some(View::List(entries)) => match entries[0].relationships.get("athletes") {
            Some(View::Single(athlete)) => {
                assert_eq!(athlete.ref_count, 2);
                assert_eq!(athlete.row.get("name"), Some(&s("Mason Ho")));
            }
            _ => panic!("Expected single athlete with rc=2"),
        },
        _ => panic!("Expected event"),
    }

    // Remove d1 → athlete rc=1
    let remove_d1 = ViewChange::Child {
        node: rust_ivm::ivm::view::RowOnlyNode {
            row: make_row(&[("id", s("e1")), ("name", s("Buffalo Big Board Classic"))]),
        },
        child: rust_ivm::ivm::view::ChildViewChange {
            relationship_name: "athletes".to_string(),
            change: Box::new(ViewChange::Remove {
                node: ViewNode::Lazy({
                    let mut n = make_node(make_row(&[
                        ("eventID", s("e1")),
                        ("athleteCountry", s("USA")),
                        ("athleteID", s("a1")),
                        ("disciplineID", s("d1")),
                    ]));
                    n = n.set_relationship(
                        "athletes",
                        rel_from_vec(vec![make_node(make_row(&[
                            ("country", s("USA")),
                            ("id", s("a1")),
                            ("name", s("Mason Ho")),
                        ]))]),
                    );
                    n
                }),
            }),
        },
    };
    root = apply(&root, &remove_d1);

    match root.relationships.get("") {
        Some(View::List(entries)) => match entries[0].relationships.get("athletes") {
            Some(View::Single(athlete)) => assert_eq!(athlete.ref_count, 1),
            _ => panic!("Expected single athlete with rc=1"),
        },
        _ => panic!("Expected event"),
    }

    // Remove d2 → athlete is None (singular, rc went to 0)
    let remove_d2 = ViewChange::Child {
        node: rust_ivm::ivm::view::RowOnlyNode {
            row: make_row(&[("id", s("e1")), ("name", s("Buffalo Big Board Classic"))]),
        },
        child: rust_ivm::ivm::view::ChildViewChange {
            relationship_name: "athletes".to_string(),
            change: Box::new(ViewChange::Remove {
                node: ViewNode::Lazy({
                    let mut n = make_node(make_row(&[
                        ("eventID", s("e1")),
                        ("athleteCountry", s("USA")),
                        ("athleteID", s("a1")),
                        ("disciplineID", s("d2")),
                    ]));
                    n = n.set_relationship(
                        "athletes",
                        rel_from_vec(vec![make_node(make_row(&[
                            ("country", s("USA")),
                            ("id", s("a1")),
                            ("name", s("Mason Ho")),
                        ]))]),
                    );
                    n
                }),
            }),
        },
    };
    root = apply(&root, &remove_d2);

    match root.relationships.get("") {
        Some(View::List(entries)) => match entries[0].relationships.get("athletes") {
            Some(View::None) | None => {}
            _ => panic!("Expected None after final remove"),
        },
        _ => panic!("Expected event"),
    }
}
