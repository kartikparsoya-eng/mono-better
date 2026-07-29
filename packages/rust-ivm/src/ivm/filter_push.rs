//! Filter push — port of `zql/src/ivm/filter-push.ts`.

use std::sync::Arc;

use crate::ivm::change::{make_add_change, make_remove_change, Change};
use crate::ivm::data::Row;
use crate::ivm::operator::{InputBase, Output, OutputHandle};

/// Port of TS `filterPush`. Returns void (no 'yield' in Rust).
pub fn filter_push(
    change: Change,
    output: OutputHandle,
    pusher: &dyn InputBase,
    predicate: Option<&Arc<dyn Fn(&Row) -> bool>>,
) {
    match &change {
        Change::Add(node) | Change::Remove(node) | Change::Child { node, .. } => {
            let passes = predicate.map(|p| p(&node.row)).unwrap_or(true);
            if passes {
                output.borrow_mut().push(change, pusher);
            }
        }
        Change::Edit { node, old_node } => {
            let old_was_present = predicate.map(|p| p(&old_node.row)).unwrap_or(true);
            let new_is_present = predicate.map(|p| p(&node.row)).unwrap_or(true);

            if old_was_present && new_is_present {
                output.borrow_mut().push(change, pusher);
            } else if old_was_present && !new_is_present {
                output.borrow_mut().push(make_remove_change(old_node.clone()), pusher);
            } else if !old_was_present && new_is_present {
                output.borrow_mut().push(make_add_change(node.clone()), pusher);
            }
        }
    }
}
