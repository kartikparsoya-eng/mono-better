//! Join utilities — port of `zql/src/ivm/join-utils.ts`.
//!
//! The overlay generators are the subtlest part of IVM: during a push,
//! a change is "in flight" (overlay). When a fetch happens concurrently
//! (e.g. Join fetching matching parents during a child push), the
//! in-flight change must be spliced into the fetched stream so the
//! fetcher sees the correct state.
//!
//! `generateWithOverlay` — for sorted streams: inserts/removes the overlay
//! node at the correct position based on the comparator.
//! `generateWithOverlayUnordered` — for unsorted streams: uses PK equality.

use std::rc::Rc;
use std::cell::{Cell, RefCell};
use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;
use std::collections::HashMap;

use crate::ivm::change::{Change, ChangeType};
use crate::ivm::data::{compare_values, values_equal, Comparator, Node, Row, Value};
use crate::ivm::stream::{from_vec, NodeStream, RelStream, skip_yields};
use crate::ivm::schema::SourceSchema;

/// Check if two rows are equal for a compound key.
/// Port of TS `rowEqualsForCompoundKey` (join-utils.ts:232).
pub fn row_equals_for_compound_key(a: &Row, b: &Row, key: &[String]) -> bool {
    for k in key {
        let av = a.get(k).cloned().unwrap_or(Value::Null);
        let bv = b.get(k).cloned().unwrap_or(Value::Null);
        if compare_values(&av, &bv) != CmpOrdering::Equal {
            return false;
        }
    }
    true
}

/// Check if a parent row and child row match on the join keys.
/// Port of TS `isJoinMatch` (join-utils.ts:237).
pub fn is_join_match(
    parent: &Row,
    parent_key: &[String],
    child: &Row,
    child_key: &[String],
) -> bool {
    for (pk, ck) in parent_key.iter().zip(child_key.iter()) {
        let pv = parent.get(pk).cloned().unwrap_or(Value::Null);
        let cv = child.get(ck).cloned().unwrap_or(Value::Null);
        if !values_equal(&pv, &cv) {
            return false;
        }
    }
    true
}

/// Build a constraint by mapping values from `source_row` using `source_key`
/// to keys in `target_key`. Returns None if any source value is null.
/// Port of TS `buildJoinConstraint` (join-utils.ts:247).
pub fn build_join_constraint(
    source_row: &Row,
    source_key: &[String],
    target_key: &[String],
) -> Option<crate::ivm::constraint::Constraint> {
    let mut constraint = crate::ivm::constraint::Constraint::default();
    for (from, to) in source_key.iter().zip(target_key.iter()) {
        let val = source_row.get(from).cloned().unwrap_or(Value::Null);
        if val.is_null() {
            return None;
        }
        constraint.insert(to.clone(), val);
    }
    Some(constraint)
}

/// Generate with overlay — for sorted streams.
/// Port of TS `generateWithOverlay` (join-utils.ts:23).
///
/// Splices the overlay change into the stream at the correct sorted position:
/// - ADD: skip the node if it matches (already being added), yield at end if not found
/// - REMOVE: yield the removed node when we pass its position, suppress the matching node
/// - EDIT: yield old_node at its position, suppress new_node at its position
/// - CHILD: replace the matching node's relationship with an overlaid stream
pub fn generate_with_overlay(
    stream: NodeStream,
    overlay: Change,
    schema: &SourceSchema,
) -> NodeStream {
    let compare = schema.compare_rows.clone();
    let rels = schema.relationships.clone();

    let overlay_type = overlay.change_type();
    let overlay_node = overlay.node().clone();
    let overlay_old_node = overlay.old_node().cloned();

    // Lazy streaming — port of TS generateWithOverlay generator.
    // Uses a state machine that splices the overlay into the stream.
    use crate::ivm::stream::StreamItem;

    match overlay_type {
        ChangeType::Add => {
            let applied = Rc::new(Cell::new(false));
            let applied2 = applied.clone();
            let compare2 = compare.clone();
            let overlay_node2 = overlay_node.clone();
            let inner = skip_yields(stream).flat_map(move |node| {
                let mut out: Vec<StreamItem<Node>> = Vec::new();
                if !applied2.get() {
                    let cmp = compare2(&overlay_node2.row, &node.row);
                    if cmp == CmpOrdering::Equal {
                        applied2.set(true);
                    } else if cmp == CmpOrdering::Less {
                        applied2.set(true);
                        out.push(StreamItem::Data(overlay_node2.clone()));
                    }
                }
                out.push(StreamItem::Data(node));
                out
            });
            // Handle the case where overlay_node goes at the end
            let trailing = Rc::new(RefCell::new(if applied.get() {
                None
            } else {
                Some(overlay_node.clone())
            }));
            let trailing2 = trailing.clone();
            Box::new(inner.chain(std::iter::from_fn(move || {
                trailing2.borrow_mut().take().map(StreamItem::Data)
            })))
        }
        ChangeType::Remove => {
            let applied = Rc::new(Cell::new(false));
            let applied_for_trailing = applied.clone();
            let compare2 = compare.clone();
            let overlay_node2 = overlay_node.clone();
            let inner = skip_yields(stream).filter_map(move |node| {
                if !applied.get() && compare2(&overlay_node2.row, &node.row) == CmpOrdering::Less {
                    applied.set(true);
                }
                // Skip the node that matches the overlay (the removed node)
                if compare2(&overlay_node2.row, &node.row) == CmpOrdering::Equal {
                    None // suppress
                } else {
                    Some(StreamItem::Data(node))
                }
            });
            // After stream, if not applied, yield the removed node (TS does this)
            let trailing = Rc::new(RefCell::new(if applied_for_trailing.get() {
                None
            } else {
                Some(overlay_node.clone())
            }));
            let trailing2 = trailing.clone();
            Box::new(inner.chain(std::iter::from_fn(move || {
                trailing2.borrow_mut().take().map(StreamItem::Data)
            })))
        }
        ChangeType::Edit => {
            let old_node = overlay_old_node.unwrap();
            let edit_old_applied = Rc::new(Cell::new(false));
            let edit_new_applied = Rc::new(Cell::new(false));
            let compare2 = compare.clone();
            let old_node2 = old_node.clone();
            let overlay_node2 = overlay_node.clone();
            let eoa = edit_old_applied.clone();
            let ena = edit_new_applied.clone();
            let inner = skip_yields(stream).flat_map(move |node| {
                let mut out: Vec<StreamItem<Node>> = Vec::new();
                if !eoa.get() && compare2(&old_node2.row, &node.row) == CmpOrdering::Less {
                    eoa.set(true);
                    out.push(StreamItem::Data(old_node2.clone()));
                }
                if !ena.get() && compare2(&overlay_node2.row, &node.row) == CmpOrdering::Equal {
                    ena.set(true);
                    // suppress old version of node
                } else {
                    out.push(StreamItem::Data(node));
                }
                out
            });
            // Handle remaining: if edit_new_applied but not edit_old_applied,
            // yield old_node at end
            let trailing = Rc::new(RefCell::new({
                // If edit_new was applied but edit_old wasn't, we need to yield old_node
                if edit_new_applied.get() && !edit_old_applied.get() {
                    Some(old_node.clone())
                } else {
                    None
                }
            }));
            let trailing2 = trailing.clone();
            Box::new(inner.chain(std::iter::from_fn(move || {
                trailing2.borrow_mut().take().map(StreamItem::Data)
            })))
        }
        ChangeType::Child => {
            let child_data = match &overlay {
                Change::Child { child, .. } => child,
                _ => unreachable!(),
            };
            let rel_name = child_data.relationship_name.clone();
            let inner_change = child_data.change.clone();
            let child_schema = rels.get(&rel_name).cloned();
            let applied = Rc::new(Cell::new(false));
            let compare2 = compare.clone();
            let overlay_node2 = overlay_node.clone();
            let rel_name2 = rel_name.clone();
            let inner_change2 = inner_change.clone();
            let child_schema2 = child_schema.clone();
            let applied2 = applied.clone();
            let inner = skip_yields(stream).filter_map(move |node| {
                if !applied2.get() && compare2(&overlay_node2.row, &node.row) == CmpOrdering::Equal {
                    applied2.set(true);
                    // Replace the matching node's relationship with overlaid stream
                    let existing_rel_fn = node.relationships.get(&rel_name2).cloned();
                    let inner_change3 = inner_change2.clone();
                    let child_schema3 = child_schema2.clone();
                    let rel_name3 = rel_name2.clone();
                    let overlaid_rel: RelStream = Rc::new(move || {
                        match (&existing_rel_fn, &child_schema3) {
                            (Some(rel_fn), Some(cs)) => {
                                generate_with_overlay(rel_fn(), inner_change3.as_ref().clone(), cs)
                            }
                            _ => crate::ivm::stream::empty_stream(),
                        }
                    });
                    let new_node = node.clone().set_relationship(&rel_name3, overlaid_rel);
                    Some(StreamItem::Data(new_node))
                } else {
                    Some(StreamItem::Data(node))
                }
            });
            Box::new(inner)
        }
    }
}

/// Generate with overlay — for unordered streams (no sort).
/// Port of TS `generateWithOverlayUnordered` (join-utils.ts:130).
///
/// Uses PK equality instead of comparator position:
/// - REMOVE/EDIT: eagerly inject old node at start
/// - ADD/EDIT: suppress the matching PK in the stream
/// - CHILD: replace matching PK's relationship
pub fn generate_with_overlay_unordered(
    stream: NodeStream,
    overlay: Change,
    schema: &SourceSchema,
) -> NodeStream {
    use crate::ivm::stream::StreamItem;
    let pk = schema.primary_key.clone();
    let rels = schema.relationships.clone();
    let overlay_type = overlay.change_type();
    let overlay_node = overlay.node().clone();
    let overlay_old_node = overlay.old_node().cloned();

    // Lazy port of TS `generateWithOverlayUnordered` (join-utils.ts:134): a
    // generator that eager-injects for REMOVE/EDIT, streams the input applying
    // inline suppression / child-overlay, and asserts at the end that the
    // overlay was applied. Previously this collected the whole child stream up
    // front, breaking the streaming invariant for unordered (Cap'd EXISTS)
    // children.

    // Eager inject for REMOVE/EDIT, yielded before the body.
    let mut lead: Vec<StreamItem<Node>> = Vec::new();
    match overlay_type {
        ChangeType::Remove => lead.push(StreamItem::Data(overlay_node.clone())),
        ChangeType::Edit => {
            if let Some(old) = &overlay_old_node {
                lead.push(StreamItem::Data(old.clone()));
            }
        }
        _ => {}
    }

    let child = match &overlay {
        Change::Child { child, .. } => Some(child.clone()),
        _ => None,
    };

    let suppressed = Rc::new(Cell::new(false));
    let sup_body = suppressed.clone();
    let overlay_node2 = overlay_node.clone();

    let body = skip_yields(stream).filter_map(move |node| {
        if !sup_body.get() {
            match overlay_type {
                ChangeType::Add | ChangeType::Edit => {
                    if row_equals_for_compound_key(&overlay_node2.row, &node.row, &pk) {
                        sup_body.set(true);
                        return None; // suppress the superseded/added row
                    }
                }
                ChangeType::Child => {
                    if row_equals_for_compound_key(&overlay_node2.row, &node.row, &pk) {
                        sup_body.set(true);
                        let cd = child.as_ref().expect("child overlay without ChildData");
                        let rel_name = cd.relationship_name.clone();
                        let inner_change = cd.change.clone();
                        let child_schema = rels.get(&rel_name).cloned();
                        let existing_rel_fn = node.relationships.get(&rel_name).cloned();
                        let rel_name3 = rel_name.clone();
                        let overlaid_rel: RelStream = Rc::new(move || {
                            match (&existing_rel_fn, &child_schema) {
                                (Some(rel_fn), Some(cs)) => generate_with_overlay(
                                    rel_fn(),
                                    inner_change.as_ref().clone(),
                                    cs,
                                ),
                                _ => crate::ivm::stream::empty_stream(),
                            }
                        });
                        let new_node = node.clone().set_relationship(&rel_name3, overlaid_rel);
                        return Some(StreamItem::Data(new_node));
                    }
                }
                _ => {}
            }
        }
        Some(StreamItem::Data(node))
    });

    // Assert-at-end, mirroring the trailing TS assert. Runs lazily when the
    // consumer exhausts the stream.
    let mut inner = lead.into_iter().chain(body);
    let mut asserted = false;
    Box::new(std::iter::from_fn(move || match inner.next() {
        Some(item) => Some(item),
        None => {
            if !asserted {
                asserted = true;
                assert!(
                    suppressed.get() || overlay_type == ChangeType::Remove,
                    "unordered overlay: overlay was never applied"
                );
            }
            None
        }
    }))
}

/// Generate with start — applies a start position to a sorted stream.
/// Port of TS `generateWithStart` (memory-source.ts:653).
pub fn generate_with_start(
    stream: NodeStream,
    start: &crate::ivm::operator::Start,
    compare: &Comparator,
) -> NodeStream {
    let start_row = start.row.clone();
    let basis = start.basis;
    let compare = compare.clone();

    Box::new(stream.filter_map(move |item| {
        use crate::ivm::stream::StreamItem;
        let node = match item {
            StreamItem::Data(n) => n,
            StreamItem::Yield => return Some(StreamItem::Yield),
        };
        let cmp = compare(&node.row, &start_row);
        let passes = match basis {
            crate::ivm::operator::Basis::At => cmp != CmpOrdering::Less,
            crate::ivm::operator::Basis::After => cmp == CmpOrdering::Greater,
        };
        if passes { Some(StreamItem::Data(node)) } else { None }
    }))
}

/// Wrapper that strips 'yield' from the stream type.
/// In Rust there is no 'yield' token, so this is just an alias.
/// Port of TS `generateWithOverlayNoYield` (join-utils.ts:11).
pub fn generate_with_overlay_no_yield(
    stream: NodeStream,
    overlay: Change,
    schema: &SourceSchema,
) -> NodeStream {
    generate_with_overlay(stream, overlay, schema)
}

/// Wrapper for unordered variant.
/// Port of TS `generateWithOverlayNoYieldUnordered` (join-utils.ts:126).
pub fn generate_with_overlay_no_yield_unordered(
    stream: NodeStream,
    overlay: Change,
    schema: &SourceSchema,
) -> NodeStream {
    generate_with_overlay_unordered(stream, overlay, schema)
}
