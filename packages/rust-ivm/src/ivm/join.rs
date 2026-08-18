//! Join operator — port of `zql/src/ivm/join.ts`.
//!
//! Hierarchical join: parent nodes gain a new relationship containing
//! matching child nodes. The relationship is a LAZY stream.

use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::rc::Rc;

use crate::ivm::change::{
    Change, ChildData, make_add_change, make_child_change, make_edit_change, make_remove_change,
};
use crate::ivm::constraint::Constraint;
use crate::ivm::data::{Node, Row, Value, compare_values, values_equal};
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::{SourceSchema, System};
use crate::ivm::stream::{NodeStream, RelStream, empty_stream, skip_yields};

pub type CompoundKey = Vec<String>;

pub struct JoinArgs {
    pub parent: Shared<dyn Input>,
    pub child: Shared<dyn Input>,
    pub parent_key: CompoundKey,
    pub child_key: CompoundKey,
    pub relationship_name: String,
    pub hidden: bool,
    pub system: System,
}

/// The Join operator.
pub struct Join {
    parent: Shared<dyn Input>,
    child: Shared<dyn Input>,
    parent_key: CompoundKey,
    child_key: CompoundKey,
    relationship_name: String,
    schema: SourceSchema,
    output: Rc<RefCell<Option<OutputHandle>>>,
    inprogress_child_change: Rc<RefCell<Option<Change>>>,
    inprogress_child_change_position: Rc<RefCell<Option<Row>>>,
}

/// RAII guard that clears in-progress overlay state on drop, even if a panic occurs.
struct InprogressGuard {
    change: Rc<RefCell<Option<Change>>>,
    position: Rc<RefCell<Option<Row>>>,
}
impl Drop for InprogressGuard {
    fn drop(&mut self) {
        *self.change.borrow_mut() = None;
        *self.position.borrow_mut() = None;
    }
}

impl Join {
    pub fn new(args: JoinArgs) -> Shared<Join> {
        crate::live_count::inc(&crate::live_count::JOIN);
        assert!(
            !Rc::ptr_eq(&args.parent, &args.child),
            "Join parent and child must be different inputs"
        );
        assert_eq!(
            args.parent_key.len(),
            args.child_key.len(),
            "The parentKey and childKey keys must have same length"
        );
        let parent_schema = args.parent.borrow().get_schema();
        let child_schema = args.child.borrow().get_schema();
        let schema = parent_schema.with_relationship(
            &args.relationship_name,
            child_schema,
            args.hidden,
            args.system,
        );

        let join = Rc::new(RefCell::new(Join {
            parent: args.parent.clone(),
            child: args.child.clone(),
            parent_key: args.parent_key.clone(),
            child_key: args.child_key.clone(),
            relationship_name: args.relationship_name.clone(),
            schema,
            output: Rc::new(RefCell::new(None)),
            inprogress_child_change: Rc::new(RefCell::new(None)),
            inprogress_child_change_position: Rc::new(RefCell::new(None)),
        }));

        let join_clone = join.clone();
        args.parent
            .borrow()
            .set_output(Rc::new(RefCell::new(ParentOutput { join: join_clone })));

        let join_clone = join.clone();
        args.child
            .borrow()
            .set_output(Rc::new(RefCell::new(ChildOutput { join: join_clone })));

        join
    }

    fn process_parent_node(
        &self,
        parent_row: Row,
        parent_rels: HashMap<String, RelStream>,
        parent_order: Vec<String>,
    ) -> Node {
        let child = self.child.clone();
        let parent_key = self.parent_key.clone();
        let child_key = self.child_key.clone();
        let inprogress = self.inprogress_child_change.clone();
        let inprogress_pos = self.inprogress_child_change_position.clone();
        let schema = self.schema.clone();
        let relationship_name = self.relationship_name.clone();

        let parent_row_for_closure = parent_row.clone();
        let child_stream: RelStream = Rc::new(move || {
            let constraint =
                build_join_constraint(&parent_row_for_closure, &parent_key, &child_key);
            let child_input = child.borrow();
            let stream = match constraint {
                Some(c) => {
                    let _t = crate::perf_trace::scope("join.child_fetch");
                    child_input.fetch(&FetchRequest {
                        constraint: Some(c),
                        ..Default::default()
                    })
                }
                None => empty_stream(),
            };

            let inprogress_change = inprogress.borrow().clone();
            let inprogress_position = inprogress_pos.borrow().clone();

            if let (Some(change), Some(pos)) =
                (inprogress_change.as_ref(), inprogress_position.as_ref())
            {
                let change_row = change.node().row.clone();
                let matches = is_join_match(
                    &parent_row_for_closure,
                    &parent_key,
                    &change_row,
                    &child_key,
                );

                if matches {
                    let compare = schema.compare_rows.clone();
                    let needs_overlay =
                        compare(&parent_row_for_closure, pos) == CmpOrdering::Greater;

                    if needs_overlay {
                        // TS join.ts: unordered when the child schema has no sort.
                        return if schema.sort.is_none() {
                            crate::ivm::join_utils::generate_with_overlay_unordered(
                                stream,
                                change.clone(),
                                &schema,
                            )
                        } else {
                            crate::ivm::join_utils::generate_with_overlay(
                                stream,
                                change.clone(),
                                &schema,
                            )
                        };
                    }
                }
            }

            stream
        });

        let mut node = Node::new(parent_row);
        let mut taken_rels = parent_rels;
        for name in &parent_order {
            if let Some(rel) = taken_rels.remove(name) {
                node = node.set_relationship(name, rel);
            }
        }
        node = node.set_relationship(&relationship_name, child_stream);
        node
    }

    fn push_parent(&self, change: Change, pusher: &dyn InputBase) {
        let output = self.output.borrow().clone();
        let output = output.expect("Join output not set");

        let parent_rels = change.node().relationships.clone();
        let parent_order = change.node().rel_order.clone();
        let parent_row = change.node().row.clone();

        match &change {
            Change::Add(_) => {
                let node = self.process_parent_node(parent_row, parent_rels, parent_order);
                output.borrow_mut().push(make_add_change(node), pusher);
            }
            Change::Remove(_) => {
                let node = self.process_parent_node(parent_row, parent_rels, parent_order);
                output.borrow_mut().push(make_remove_change(node), pusher);
            }
            Change::Child { child, .. } => {
                let node = self.process_parent_node(parent_row, parent_rels, parent_order);
                output.borrow_mut().push(
                    make_child_change(
                        node,
                        ChildData {
                            relationship_name: child.relationship_name.clone(),
                            change: child.change.clone(),
                        },
                    ),
                    pusher,
                );
            }
            Change::Edit { old_node, .. } => {
                // Port of TS join.ts:167 `assert(rowEqualsForCompoundKey(...),
                // 'Parent edit must not change relationship.')`. Key-changing
                // edits are split into add/remove at the source; one reaching
                // here is an invariant violation. Reset (panic contained at the
                // napi boundary) rather than silently dropping into drift.
                assert!(
                    row_equals_for_compound_key(&old_node.row, &parent_row, &self.parent_key),
                    "Parent edit must not change relationship.",
                );
                let old_rels = old_node.relationships.clone();
                let old_order = old_node.rel_order.clone();
                let old_row = old_node.row.clone();

                let node = self.process_parent_node(parent_row, parent_rels, parent_order);
                let old_node = self.process_parent_node(old_row, old_rels, old_order);
                output
                    .borrow_mut()
                    .push(make_edit_change(node, old_node), pusher);
            }
        }
    }

    fn push_child(&self, change: Change, pusher: &dyn InputBase) {
        match &change {
            Change::Add(_) | Change::Remove(_) => {
                self.push_child_change(&change, pusher);
            }
            Change::Child { .. } => {
                self.push_child_change(&change, pusher);
            }
            Change::Edit { node, old_node } => {
                // Port of TS join.ts:208 `assert(..., 'Child edit must not
                // change relationship.')`. See push_parent Edit above.
                assert!(
                    row_equals_for_compound_key(&old_node.row, &node.row, &self.child_key),
                    "Child edit must not change relationship.",
                );
                self.push_child_change(&change, pusher);
            }
        }
    }

    fn push_child_change(&self, change: &Change, pusher: &dyn InputBase) {
        *self.inprogress_child_change.borrow_mut() = Some(change.clone());
        *self.inprogress_child_change_position.borrow_mut() = None;
        let _inprogress_guard = InprogressGuard {
            change: self.inprogress_child_change.clone(),
            position: self.inprogress_child_change_position.clone(),
        };

        let child_row = change.node().row.clone();
        let constraint = build_join_constraint(&child_row, &self.child_key, &self.parent_key);

        if let Some(c) = constraint {
            let parent_input = self.parent.borrow();
            let parent_stream = skip_yields(parent_input.fetch(&FetchRequest {
                constraint: Some(c),
                ..Default::default()
            }));

            let output = self.output.borrow().clone();
            let output = output.expect("Join output not set");

            let _t = crate::perf_trace::scope("join.push_parents");
            for parent_node in parent_stream {
                *self.inprogress_child_change_position.borrow_mut() = Some(parent_node.row.clone());

                let parent_rels = parent_node.relationships.clone();
                let parent_order = parent_node.rel_order.clone();
                let parent_row = parent_node.row.clone();

                let processed = self.process_parent_node(parent_row, parent_rels, parent_order);
                let child_change = ChildData {
                    relationship_name: self.relationship_name.clone(),
                    change: Box::new(change.clone()),
                };
                output
                    .borrow_mut()
                    .push(make_child_change(processed, child_change), pusher);
            }
        }

        // Overlay cleared by _inprogress_guard Drop
    }
}

impl InputBase for Join {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.parent.borrow_mut().destroy();
        self.child.borrow_mut().destroy();
        // Break the Rc cycle: clear the back-edge to the downstream output.
        *self.output.borrow_mut() = None;
    }
}

impl Input for Join {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        self.fetch_lazy(req)
    }
}

impl Join {
    fn fetch_lazy(&self, req: &FetchRequest) -> NodeStream {
        let parent = self.parent.borrow();
        let parent_stream = parent.fetch(req);

        let child = self.child.clone();
        let parent_key = self.parent_key.clone();
        let child_key = self.child_key.clone();
        let inprogress = self.inprogress_child_change.clone();
        let inprogress_pos = self.inprogress_child_change_position.clone();
        let schema = self.schema.clone();
        let relationship_name = self.relationship_name.clone();

        Box::new(parent_stream.map(move |item| {
            use crate::ivm::stream::StreamItem;
            let pn = match item {
                StreamItem::Data(n) => n,
                StreamItem::Yield => return StreamItem::Yield,
            };
            let parent_rels = pn.relationships.clone();
            let parent_order = pn.rel_order.clone();
            let parent_row = pn.row.clone();

            let parent_row_for_closure = parent_row.clone();
            let child = child.clone();
            let parent_key = parent_key.clone();
            let child_key = child_key.clone();
            let inprogress = inprogress.clone();
            let inprogress_pos = inprogress_pos.clone();
            let schema = schema.clone();
            let child_stream: RelStream = Rc::new(move || {
                let constraint =
                    build_join_constraint(&parent_row_for_closure, &parent_key, &child_key);
                let child_input = child.borrow();
                let stream = match constraint {
                    Some(c) => {
                        let _t = crate::perf_trace::scope("join.child_fetch");
                        child_input.fetch(&FetchRequest {
                            constraint: Some(c),
                            ..Default::default()
                        })
                    }
                    None => empty_stream(),
                };

                let inprogress_change = inprogress.borrow().clone();
                let inprogress_position = inprogress_pos.borrow().clone();

                if let (Some(change), Some(pos)) =
                    (inprogress_change.as_ref(), inprogress_position.as_ref())
                {
                    let change_row = change.node().row.clone();
                    let matches = is_join_match(
                        &parent_row_for_closure,
                        &parent_key,
                        &change_row,
                        &child_key,
                    );

                    if matches {
                        let compare = schema.compare_rows.clone();
                        let needs_overlay =
                            compare(&parent_row_for_closure, pos) == CmpOrdering::Greater;

                        if needs_overlay {
                            // TS join.ts: unordered when the child schema has no sort.
                            return if schema.sort.is_none() {
                                crate::ivm::join_utils::generate_with_overlay_unordered(
                                    stream,
                                    change.clone(),
                                    &schema,
                                )
                            } else {
                                crate::ivm::join_utils::generate_with_overlay(
                                    stream,
                                    change.clone(),
                                    &schema,
                                )
                            };
                        }
                    }
                }

                stream
            });

            let mut node = Node::new(parent_row);
            let mut taken_rels = parent_rels;
            for name in &parent_order {
                if let Some(rel) = taken_rels.remove(name) {
                    node = node.set_relationship(name, rel);
                }
            }
            node = node.set_relationship(&relationship_name, child_stream);
            StreamItem::Data(node)
        }))
    }
}

impl Output for Join {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {}
}

struct ParentOutput {
    join: Shared<Join>,
}

impl Output for ParentOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        crate::ivm::trace::recv("join#1", &change);
        self.join.borrow().push_parent(change, pusher);
    }
}

struct ChildOutput {
    join: Shared<Join>,
}

impl Output for ChildOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        crate::ivm::trace::recv("join#2", &change);
        self.join.borrow().push_child(change, pusher);
    }
}

pub fn build_join_constraint(
    row: &Row,
    from_key: &CompoundKey,
    to_key: &CompoundKey,
) -> Option<Constraint> {
    let mut constraint = Constraint::default();
    for (from, to) in from_key.iter().zip(to_key.iter()) {
        let val = row.get(from).cloned().unwrap_or(Value::Null);
        if val.is_null() {
            return None;
        }
        constraint.insert(to.clone(), val);
    }
    Some(constraint)
}

pub fn is_join_match(
    parent_row: &Row,
    parent_key: &CompoundKey,
    child_row: &Row,
    child_key: &CompoundKey,
) -> bool {
    for (pk, ck) in parent_key.iter().zip(child_key.iter()) {
        let pv = parent_row.get(pk).unwrap_or(&Value::Null);
        let cv = child_row.get(ck).unwrap_or(&Value::Null);
        if !values_equal(pv, cv) {
            return false;
        }
    }
    true
}

pub fn row_equals_for_compound_key(a: &Row, b: &Row, key: &CompoundKey) -> bool {
    for k in key {
        let av = a.get(k).unwrap_or(&Value::Null);
        let bv = b.get(k).unwrap_or(&Value::Null);
        // TS uses compareValues (null === null → 0, i.e. equal).
        // NOT valuesEqual (which treats null as never equal — that's for joins).
        if compare_values(av, bv) != CmpOrdering::Equal {
            return false;
        }
    }
    true
}

#[allow(dead_code)]
fn generate_with_overlay_join(
    stream: NodeStream,
    change: Change,
    schema: &crate::ivm::schema::SourceSchema,
) -> NodeStream {
    crate::ivm::join_utils::generate_with_overlay(stream, change, schema)
}

impl Drop for Join {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::JOIN);
    }
}
