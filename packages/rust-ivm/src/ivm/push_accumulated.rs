//! Push accumulated changes — port of `zql/src/ivm/push-accumulated.ts`.
//!
//! After FanOut pushes to all branches, FanIn accumulates the results.
//! This function collapses accumulated changes into a single push.
//!
//! Invariants:
//! - add in → only adds out
//! - remove in → only removes out
//! - edit in → adds, removes, or edits out
//! - child in → adds, removes, or children out

use std::collections::HashMap;

use crate::ivm::change::{
    Change, ChangeType, make_add_change, make_child_change, make_edit_change,
    make_remove_change,
};
use crate::ivm::operator::{InputBase, OutputHandle};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::empty_rel;

/// Merge relationships from `right` into `left` (right doesn't overwrite left).
/// Port of TS `mergeRelationships` (push-accumulated.ts:265).
pub fn merge_relationships(left: &Change, right: &Change) -> Change {
    let left_type = left.change_type();
    let right_type = right.change_type();

    if left_type == right_type {
        match (left, right) {
            (Change::Add(ln), Change::Add(rn)) => {
                let mut node = ln.clone();
                for (name, rel) in &rn.relationships {
                    if !node.relationships.contains_key(name) {
                        node = node.set_relationship(name, rel.clone());
                    }
                }
                make_add_change(node)
            }
            (Change::Remove(ln), Change::Remove(rn)) => {
                let mut node = ln.clone();
                for (name, rel) in &rn.relationships {
                    if !node.relationships.contains_key(name) {
                        node = node.set_relationship(name, rel.clone());
                    }
                }
                make_remove_change(node)
            }
            (
                Change::Edit {
                    node: ln,
                    old_node: lo,
                },
                Change::Edit {
                    node: rn,
                    old_node: ro,
                },
            ) => {
                let mut new_node = ln.clone();
                for (name, rel) in &rn.relationships {
                    if !new_node.relationships.contains_key(name) {
                        new_node = new_node.set_relationship(name, rel.clone());
                    }
                }
                let mut old_node = lo.clone();
                for (name, rel) in &ro.relationships {
                    if !old_node.relationships.contains_key(name) {
                        old_node = old_node.set_relationship(name, rel.clone());
                    }
                }
                make_edit_change(new_node, old_node)
            }
            (
                Change::Child {
                    node: ln,
                    child: lc,
                },
                Change::Child {
                    node: rn,
                    child: _rc,
                },
            ) => {
                let mut node = ln.clone();
                for (name, rel) in &rn.relationships {
                    if !node.relationships.contains_key(name) {
                        node = node.set_relationship(name, rel.clone());
                    }
                }
                make_child_change(node, lc.clone())
            }
            _ => panic!("merge_relationships: type mismatch"),
        }
    } else {
        // left is always edit here
        match (left, right) {
            (
                Change::Edit {
                    node: ln,
                    old_node: lo,
                },
                Change::Add(rn),
            ) => {
                let mut new_node = ln.clone();
                for (name, rel) in &rn.relationships {
                    if !new_node.relationships.contains_key(name) {
                        new_node = new_node.set_relationship(name, rel.clone());
                    }
                }
                make_edit_change(new_node, lo.clone())
            }
            (
                Change::Edit {
                    node: ln,
                    old_node: lo,
                },
                Change::Remove(rn),
            ) => {
                let mut old_node = lo.clone();
                for (name, rel) in &rn.relationships {
                    if !old_node.relationships.contains_key(name) {
                        old_node = old_node.set_relationship(name, rel.clone());
                    }
                }
                make_edit_change(ln.clone(), old_node)
            }
            _ => panic!("merge_relationships: unexpected type combination"),
        }
    }
}

/// Create a function that adds empty relationships for schema relationships
/// not already present on a change's node.
/// Port of TS `makeAddEmptyRelationships` (push-accumulated.ts:355).
pub fn add_empty_relationships(schema: &SourceSchema, change: &Change) -> Change {
    if schema.relationships.is_empty() {
        return change.clone();
    }

    let rel_names: Vec<String> = schema.relationships.keys().cloned().collect();

    match change {
        Change::Add(node) => {
            let mut n = node.clone();
            for name in &rel_names {
                if !n.relationships.contains_key(name) {
                    n = n.set_relationship(name, empty_rel());
                }
            }
            make_add_change(n)
        }
        Change::Remove(node) => {
            let mut n = node.clone();
            for name in &rel_names {
                if !n.relationships.contains_key(name) {
                    n = n.set_relationship(name, empty_rel());
                }
            }
            make_remove_change(n)
        }
        Change::Edit { node, old_node } => {
            let mut n = node.clone();
            let mut on = old_node.clone();
            for name in &rel_names {
                if !n.relationships.contains_key(name) {
                    n = n.set_relationship(name, empty_rel());
                }
                if !on.relationships.contains_key(name) {
                    on = on.set_relationship(name, empty_rel());
                }
            }
            make_edit_change(n, on)
        }
        Change::Child { .. } => change.clone(),
    }
}

/// Push accumulated changes — collapses to a single push.
/// Port of TS `pushAccumulatedChanges` (push-accumulated.ts:83).
pub fn push_accumulated_changes(
    accumulated: &mut Vec<Change>,
    output: &OutputHandle,
    pusher: &dyn InputBase,
    fan_out_change_type: ChangeType,
    schema: &SourceSchema,
) {
    if accumulated.is_empty() {
        return;
    }

    // Collapse to a single change per type
    let mut candidates: HashMap<ChangeType, Change> = HashMap::new();

    for change in accumulated.drain(..) {
        let ct = change.change_type();
        if fan_out_change_type == ChangeType::Child && ct != ChangeType::Child {
            assert!(
                !candidates.contains_key(&ct),
                "Fan-in:child expected at most one {:?}",
                ct
            );
        }
        if let Some(existing) = candidates.get(&ct) {
            let merged = merge_relationships(existing, &change);
            candidates.insert(ct, merged);
        } else {
            candidates.insert(ct, change);
        }
    }

    let types: Vec<ChangeType> = candidates.keys().cloned().collect();

    match fan_out_change_type {
        ChangeType::Remove => {
            assert_eq!(types.len(), 1);
            assert_eq!(types[0], ChangeType::Remove);
            let change = candidates.remove(&ChangeType::Remove).unwrap();
            output
                .borrow_mut()
                .push(add_empty_relationships(schema, &change), pusher);
        }
        ChangeType::Add => {
            assert_eq!(types.len(), 1);
            assert_eq!(types[0], ChangeType::Add);
            let change = candidates.remove(&ChangeType::Add).unwrap();
            output
                .borrow_mut()
                .push(add_empty_relationships(schema, &change), pusher);
        }
        ChangeType::Edit => {
            for t in &types {
                assert!(
                    *t == ChangeType::Add || *t == ChangeType::Remove || *t == ChangeType::Edit,
                    "Fan-in:edit expected all adds, removes, or edits"
                );
            }
            let add_change = candidates.get(&ChangeType::Add).cloned();
            let remove_change = candidates.get(&ChangeType::Remove).cloned();
            let edit_change = candidates.get(&ChangeType::Edit).cloned();

            if let Some(mut ec) = edit_change {
                if let Some(ac) = &add_change {
                    ec = merge_relationships(&ec, ac);
                }
                if let Some(rc) = &remove_change {
                    ec = merge_relationships(&ec, rc);
                }
                output
                    .borrow_mut()
                    .push(add_empty_relationships(schema, &ec), pusher);
                return;
            }

            if let (Some(ac), Some(rc)) = (&add_change, &remove_change) {
                let edit = make_edit_change(ac.node().clone(), rc.node().clone());
                output
                    .borrow_mut()
                    .push(add_empty_relationships(schema, &edit), pusher);
                return;
            }

            let change = add_change
                .or(remove_change)
                .expect("expected at least one change");
            output
                .borrow_mut()
                .push(add_empty_relationships(schema, &change), pusher);
        }
        ChangeType::Child => {
            for t in &types {
                assert!(
                    *t == ChangeType::Add || *t == ChangeType::Remove || *t == ChangeType::Child,
                    "Fan-in:child expected all adds, removes, or children"
                );
            }
            assert!(types.len() <= 2, "Fan-in:child expected at most 2 types");

            if let Some(child_change) = candidates.get(&ChangeType::Child) {
                output.borrow_mut().push(child_change.clone(), pusher);
                return;
            }

            let add_change = candidates.get(&ChangeType::Add).cloned();
            let remove_change = candidates.get(&ChangeType::Remove).cloned();

            assert!(
                !(add_change.is_some() && remove_change.is_some()),
                "Fan-in:child expected either add or remove, not both"
            );

            let change = add_change
                .or(remove_change)
                .expect("expected at least one change");
            output
                .borrow_mut()
                .push(add_empty_relationships(schema, &change), pusher);
        }
    }
}
