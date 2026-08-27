//! Filter push — port of `zql/src/ivm/filter-push.ts`.

use std::sync::Arc;

use crate::ivm::change::{Change, make_add_change, make_remove_change};
use crate::ivm::data::Row;

/// Port of TS `filterPush(change, output, pusher, predicate?)`. Returns void
/// (no coop `'yield'` in Rust).
///
/// Rust-only signature delta: TS takes `output: Output` and works for both
/// plain outputs and `FilterOutput`s (which extend `Output`); Rust trait
/// objects can't unify those, so the sink is a `push` closure and callers
/// capture their output + pusher identity in it. The EDIT arm inlines TS
/// `maybeSplitAndPushEditChange`: an edit crossing the predicate boundary
/// splits into a remove/add.
pub fn filter_push(
    change: Change,
    push: &mut dyn FnMut(Change),
    predicate: Option<&Arc<dyn Fn(&Row) -> bool>>,
) {
    match &change {
        Change::Add(node) | Change::Remove(node) | Change::Child { node, .. } => {
            let passes = predicate.map(|p| p(&node.row)).unwrap_or(true);
            if passes {
                push(change);
            }
        }
        Change::Edit { node, old_node } => {
            let old_was_present = predicate.map(|p| p(&old_node.row)).unwrap_or(true);
            let new_is_present = predicate.map(|p| p(&node.row)).unwrap_or(true);

            if old_was_present && new_is_present {
                push(change);
            } else if old_was_present && !new_is_present {
                push(make_remove_change(old_node.clone()));
            } else if !old_was_present && new_is_present {
                push(make_add_change(node.clone()));
            }
        }
    }
}
