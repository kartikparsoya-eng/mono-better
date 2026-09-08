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

use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::rc::Rc;

use crate::ivm::change::{Change, ChangeType};
use crate::ivm::data::{Comparator, Node, Row, Value, compare_values, values_equal};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::{NodeStream, RelStream};

/// Check if two rows are equal for a compound key.
/// Port of TS `rowEqualsForCompoundKey` (join-utils.ts:232).
pub fn row_equals_for_compound_key(a: &Row, b: &Row, key: &[String]) -> bool {
    for k in key {
        let av = a.get(k).unwrap_or(&Value::Null);
        let bv = b.get(k).unwrap_or(&Value::Null);
        if compare_values(av, bv) != CmpOrdering::Equal {
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
        let pv = parent.get(pk).unwrap_or(&Value::Null);
        let cv = child.get(ck).unwrap_or(&Value::Null);
        if !values_equal(pv, cv) {
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

/// Port of TS `generateWithOverlay` (join-utils.ts:19-125) — the join-layer
/// overlay for SORTED child streams. During a push the in-flight change is
/// already in the source, so for parents the push has not reached yet the
/// join UNDOES it in the fetched stream: an ADD overlay suppresses the equal
/// node, a REMOVE overlay re-inserts the removed node at its sorted position,
/// an EDIT re-inserts the old row and suppresses the new one, and a CHILD
/// overlay wraps the matching node's relationship in the same generator.
/// `'yield'` markers pass through (:28-31). The generator asserts the overlay
/// was applied once the stream is exhausted (:104-123) — as in TS, that
/// trailing work runs only if the consumer iterates to completion.
pub fn generate_with_overlay(
    stream: NodeStream,
    overlay: Change,
    schema: &SourceSchema,
) -> NodeStream {
    Box::new(GenerateWithOverlay {
        stream,
        overlay,
        compare: schema.compare_rows.clone(),
        relationships: schema.relationships.clone(),
        applied: false,
        edit_old_applied: false,
        edit_new_applied: false,
        pending: std::collections::VecDeque::new(),
        done: false,
    })
}

/// The generator state of TS `generateWithOverlay`: `applied`,
/// `editOldApplied`, `editNewApplied` (join-utils.ts:24-26) plus the items a
/// single loop iteration produced ahead of the node it is processing
/// (`pending` — a TS iteration can `yield` up to twice).
struct GenerateWithOverlay {
    stream: NodeStream,
    overlay: Change,
    compare: Comparator,
    relationships: HashMap<String, SourceSchema>,
    applied: bool,
    edit_old_applied: bool,
    edit_new_applied: bool,
    pending: std::collections::VecDeque<crate::ivm::stream::StreamItem<Node>>,
    done: bool,
}

impl Iterator for GenerateWithOverlay {
    type Item = crate::ivm::stream::StreamItem<Node>;

    fn next(&mut self) -> Option<Self::Item> {
        use crate::ivm::stream::StreamItem;
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Some(item);
            }
            if self.done {
                return None;
            }
            let node = match self.stream.next() {
                Some(StreamItem::Yield) => return Some(StreamItem::Yield),
                Some(StreamItem::Data(node)) => node,
                None => {
                    self.done = true;
                    // join-utils.ts:104-118.
                    if !self.applied {
                        match &self.overlay {
                            Change::Remove(overlay_node) => {
                                self.applied = true;
                                self.pending
                                    .push_back(StreamItem::Data(overlay_node.clone()));
                            }
                            Change::Edit { old_node, .. } => {
                                assert!(
                                    self.edit_new_applied,
                                    "edit overlay: new node must be applied before old node"
                                );
                                self.edit_old_applied = true;
                                self.applied = true;
                                self.pending.push_back(StreamItem::Data(old_node.clone()));
                            }
                            Change::Add(_) | Change::Child { .. } => {}
                        }
                    }
                    assert!(
                        self.applied,
                        "overlayGenerator: overlay was never applied to any fetched node"
                    );
                    continue;
                }
            };
            let mut yield_node = true;
            if !self.applied {
                match &self.overlay {
                    Change::Add(overlay_node) => {
                        if (self.compare)(&overlay_node.row, &node.row) == CmpOrdering::Equal {
                            self.applied = true;
                            yield_node = false;
                        }
                    }
                    Change::Remove(overlay_node) => {
                        if (self.compare)(&overlay_node.row, &node.row) == CmpOrdering::Less {
                            self.applied = true;
                            self.pending
                                .push_back(StreamItem::Data(overlay_node.clone()));
                        }
                    }
                    Change::Edit {
                        node: overlay_node,
                        old_node,
                    } => {
                        if !self.edit_old_applied
                            && (self.compare)(&old_node.row, &node.row) == CmpOrdering::Less
                        {
                            self.edit_old_applied = true;
                            if self.edit_new_applied {
                                self.applied = true;
                            }
                            self.pending.push_back(StreamItem::Data(old_node.clone()));
                        }
                        if !self.edit_new_applied
                            && (self.compare)(&overlay_node.row, &node.row) == CmpOrdering::Equal
                        {
                            self.edit_new_applied = true;
                            if self.edit_old_applied {
                                self.applied = true;
                            }
                            yield_node = false;
                        }
                    }
                    Change::Child {
                        node: overlay_node,
                        child,
                    } => {
                        if (self.compare)(&overlay_node.row, &node.row) == CmpOrdering::Equal {
                            self.applied = true;
                            // TS: `node.relationships[relationshipName]()` and
                            // `schema.relationships[relationshipName]` are
                            // dereferenced unconditionally (:80-91) — a missing
                            // relationship is a programming error there too.
                            let rel_name = child.relationship_name.clone();
                            let existing_rel = node
                                .relationships
                                .get(&rel_name)
                                .cloned()
                                .unwrap_or_else(|| {
                                    panic!("overlayGenerator: relationship {rel_name} not found on node")
                                });
                            let child_schema = self
                                .relationships
                                .get(&rel_name)
                                .cloned()
                                .unwrap_or_else(|| {
                                    panic!("overlayGenerator: relationship {rel_name} not found in schema")
                                });
                            let inner_change = child.change.clone();
                            let overlaid_rel: RelStream = Rc::new(move || {
                                generate_with_overlay(
                                    existing_rel(),
                                    (*inner_change).clone(),
                                    &child_schema,
                                )
                            });
                            self.pending.push_back(StreamItem::Data(
                                node.clone().set_relationship(&rel_name, overlaid_rel),
                            ));
                            yield_node = false;
                        }
                    }
                }
            }
            if yield_node {
                self.pending.push_back(StreamItem::Data(node));
            }
        }
    }
}

/// Port of TS `generateWithOverlayUnordered` (join-utils.ts:134-202) — the
/// join-layer overlay for UNSORTED child streams (a Cap'd EXISTS child, whose
/// schema has no `sort`). Eager-injects the undone REMOVE / EDIT-old node
/// first, then streams the input suppressing the ADD / EDIT-new node or
/// wrapping the CHILD node's relationship, matching on primary-key equality
/// instead of comparator position. `'yield'` markers pass through (:149-152);
/// the trailing assert (:199-202) runs when the stream is exhausted.
pub fn generate_with_overlay_unordered(
    stream: NodeStream,
    overlay: Change,
    schema: &SourceSchema,
) -> NodeStream {
    use crate::ivm::stream::StreamItem;
    let mut lead: std::collections::VecDeque<StreamItem<Node>> = std::collections::VecDeque::new();
    // Eager inject (:140-144).
    match &overlay {
        Change::Remove(overlay_node) => lead.push_back(StreamItem::Data(overlay_node.clone())),
        Change::Edit { old_node, .. } => lead.push_back(StreamItem::Data(old_node.clone())),
        Change::Add(_) | Change::Child { .. } => {}
    }
    Box::new(GenerateWithOverlayUnordered {
        stream,
        overlay,
        primary_key: schema.primary_key.clone(),
        relationships: schema.relationships.clone(),
        suppressed: false,
        lead,
        done: false,
    })
}

struct GenerateWithOverlayUnordered {
    stream: NodeStream,
    overlay: Change,
    primary_key: Vec<String>,
    relationships: HashMap<String, SourceSchema>,
    suppressed: bool,
    lead: std::collections::VecDeque<crate::ivm::stream::StreamItem<Node>>,
    done: bool,
}

impl Iterator for GenerateWithOverlayUnordered {
    type Item = crate::ivm::stream::StreamItem<Node>;

    fn next(&mut self) -> Option<Self::Item> {
        use crate::ivm::stream::StreamItem;
        loop {
            if let Some(item) = self.lead.pop_front() {
                return Some(item);
            }
            if self.done {
                return None;
            }
            let node = match self.stream.next() {
                Some(StreamItem::Yield) => return Some(StreamItem::Yield),
                Some(StreamItem::Data(node)) => node,
                None => {
                    self.done = true;
                    assert!(
                        self.suppressed || self.overlay.change_type() == ChangeType::Remove,
                        "overlayGenerator: overlay was never applied to any fetched node"
                    );
                    return None;
                }
            };
            if !self.suppressed {
                match &self.overlay {
                    Change::Add(overlay_node)
                    | Change::Edit {
                        node: overlay_node, ..
                    } => {
                        if row_equals_for_compound_key(
                            &overlay_node.row,
                            &node.row,
                            &self.primary_key,
                        ) {
                            self.suppressed = true;
                            continue;
                        }
                    }
                    Change::Child {
                        node: overlay_node,
                        child,
                    } => {
                        if row_equals_for_compound_key(
                            &overlay_node.row,
                            &node.row,
                            &self.primary_key,
                        ) {
                            self.suppressed = true;
                            let rel_name = child.relationship_name.clone();
                            let existing_rel = node
                                .relationships
                                .get(&rel_name)
                                .cloned()
                                .unwrap_or_else(|| {
                                    panic!("overlayGenerator: relationship {rel_name} not found on node")
                                });
                            let child_schema = self
                                .relationships
                                .get(&rel_name)
                                .cloned()
                                .unwrap_or_else(|| {
                                    panic!("overlayGenerator: relationship {rel_name} not found in schema")
                                });
                            let inner_change = child.change.clone();
                            // TS wraps the child stream in the ORDERED
                            // generator here too (:182-190).
                            let overlaid_rel: RelStream = Rc::new(move || {
                                generate_with_overlay(
                                    existing_rel(),
                                    (*inner_change).clone(),
                                    &child_schema,
                                )
                            });
                            return Some(StreamItem::Data(
                                node.clone().set_relationship(&rel_name, overlaid_rel),
                            ));
                        }
                    }
                    Change::Remove(_) => {}
                }
            }
            return Some(StreamItem::Data(node));
        }
    }
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
        if passes {
            Some(StreamItem::Data(node))
        } else {
            None
        }
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
