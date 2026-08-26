use std::cell::RefCell;
use std::rc::Rc;

use crate::builder::ast::Condition;
use crate::builder::filter::create_simple_predicate;
use crate::ivm::change::{Change, ChangeType, make_add_change, make_remove_change};
use crate::ivm::data::Node;
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::{NodeStream, StreamItem};

/// Filters nodes by an OR of conditions that may include EXISTS correlated
/// subqueries (checked against `node.relationships`). Replaces the
/// FanOut/FanIn/Exists pattern for OR-with-subquery because FanIn::fetch is
/// empty. On push it must reproduce Exists's boundary behaviour: a change that
/// moves a row across the OR predicate (e.g. a child add that flips an EXISTS
/// 0->1) is re-emitted as an ADD/REMOVE, not forwarded verbatim.
pub struct NodeFilter {
    input: Shared<dyn Input>,
    conditions: Vec<Condition>,
    schema: SourceSchema,
    output: Rc<RefCell<Option<OutputHandle>>>,
}

impl NodeFilter {
    pub fn new(input: Shared<dyn Input>, conditions: Vec<Condition>) -> Shared<NodeFilter> {
        let schema = input.borrow().get_schema();
        let filter = Rc::new(RefCell::new(NodeFilter {
            input: input.clone(),
            conditions,
            schema,
            output: Rc::new(RefCell::new(None)),
        }));
        let filter_clone = filter.clone();
        input
            .borrow()
            .set_output(Rc::new(RefCell::new(NodeFilterOutput {
                filter: filter_clone,
            })));
        filter
    }
}

impl InputBase for NodeFilter {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }
    fn destroy(&mut self) {
        self.input.borrow_mut().destroy();
        // Break the Rc cycle: clear the back-edge to the downstream output.
        *self.output.borrow_mut() = None;
    }
}

impl Input for NodeFilter {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        let input = self.input.borrow();
        let conds = self.conditions.clone();
        Box::new(input.fetch(req).filter_map(move |item| match item {
            StreamItem::Data(n) if eval_or(&conds, &n) => Some(StreamItem::Data(n)),
            StreamItem::Yield => Some(StreamItem::Yield),
            _ => None,
        }))
    }
}

impl Output for NodeFilter {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {}
}

struct NodeFilterOutput {
    filter: Shared<NodeFilter>,
}

impl Output for NodeFilterOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        let filter = self.filter.borrow();
        let conds = filter.conditions.clone();
        let output = filter.output.borrow().clone();
        drop(filter);
        let Some(output) = output else { return };

        match &change {
            // Add/Remove don't move a row across the predicate by themselves;
            // forward iff the (only) state passes.
            Change::Add(node) => {
                if eval_or(&conds, node) {
                    output.borrow_mut().push(change, pusher);
                }
            }
            Change::Remove(node) => {
                if eval_or(&conds, node) {
                    output.borrow_mut().push(change, pusher);
                }
            }
            // An edit may cross the predicate (split into ADD/REMOVE).
            Change::Edit { node, old_node } => {
                let before = eval_or(&conds, old_node);
                let after = eval_or(&conds, node);
                match (before, after) {
                    (true, true) => output.borrow_mut().push(change, pusher),
                    (false, true) => output
                        .borrow_mut()
                        .push(make_add_change(node.clone()), pusher),
                    (true, false) => output
                        .borrow_mut()
                        .push(make_remove_change(old_node.clone()), pusher),
                    (false, false) => {}
                }
            }
            // The interesting case: a child add/remove changes an EXISTS size,
            // which can flip the row across the OR predicate.
            Change::Child { node, child } => {
                let rel = &child.relationship_name;
                let after_size = rel_size(node, rel);
                let before_size = match child.change.change_type() {
                    ChangeType::Add => after_size.saturating_sub(1),
                    ChangeType::Remove => after_size + 1,
                    // Inner edit/child doesn't change the count.
                    _ => after_size,
                };
                let before = eval_or_with_size(&conds, node, rel, before_size);
                let after = eval_or_with_size(&conds, node, rel, after_size);
                match (before, after) {
                    // Row stays in the result: forward the child change.
                    (true, true) => output.borrow_mut().push(change, pusher),
                    // Row enters the result (e.g. EXISTS 0->1): emit ADD.
                    (false, true) => output
                        .borrow_mut()
                        .push(make_add_change(node.clone()), pusher),
                    // Row leaves the result: emit REMOVE, overriding the flipping
                    // relationship to its PRE-change (before) state. The child
                    // change that caused the flip is not itself pushed to output,
                    // so it must be excluded from / restored to the removed node.
                    // Mirrors TS exists.ts:140-190: a child-add flip (NOT EXISTS
                    // 0->1) emits `[rel]: () => []` (exclude the added child); a
                    // child-remove flip (EXISTS 1->0) emits `() => [removedChild]`
                    // (restore the removed child). Other relationships keep their
                    // current (after) streams.
                    (true, false) => {
                        let before_rel = match child.change.as_ref() {
                            // Added child is excluded from the removed node.
                            Change::Add(_) => vec![],
                            // Removed child is restored on the removed node.
                            Change::Remove(n) => vec![n.clone()],
                            // Size-neutral inner change can't cause a flip.
                            _ => return,
                        };
                        let removed_node = node
                            .clone()
                            .set_relationship(rel, crate::ivm::stream::rel_from_vec(before_rel));
                        output
                            .borrow_mut()
                            .push(make_remove_change(removed_node), pusher)
                    }
                    (false, false) => {}
                }
            }
        }
    }
}

/// Count only Data items in a relationship stream (Yield sentinels don't count).
fn rel_size(node: &Node, rel: &str) -> usize {
    node.relationships
        .get(rel)
        .map(|f| f().filter(|i| matches!(i, StreamItem::Data(_))).count())
        .unwrap_or(0)
}

/// Evaluate one condition against a node, overriding the EXISTS size for
/// `override_rel` with `override_size` (used to compute the pre-change state).
fn condition_passes_with_size(
    cond: &Condition,
    node: &Node,
    override_rel: &str,
    override_size: usize,
) -> bool {
    match cond {
        Condition::Simple(simple) => create_simple_predicate(simple)(&node.row),
        Condition::CorrelatedSubquery(csq) => {
            let size = if csq.related.relationship_name == override_rel {
                override_size
            } else {
                rel_size(node, &csq.related.relationship_name)
            };
            if csq.op == "EXISTS" {
                size > 0
            } else {
                size == 0
            }
        }
        Condition::And(conds) => conds
            .iter()
            .all(|c| condition_passes_with_size(c, node, override_rel, override_size)),
        Condition::Or(conds) => conds
            .iter()
            .any(|c| condition_passes_with_size(c, node, override_rel, override_size)),
    }
}

fn eval_or_with_size(conds: &[Condition], node: &Node, rel: &str, size: usize) -> bool {
    conds
        .iter()
        .any(|c| condition_passes_with_size(c, node, rel, size))
}

fn eval_or(conds: &[Condition], node: &Node) -> bool {
    // No override: "" never matches a real relationship name.
    conds
        .iter()
        .any(|c| condition_passes_with_size(c, node, "", 0))
}
