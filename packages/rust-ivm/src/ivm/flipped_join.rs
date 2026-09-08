//! FlippedJoin operator — port of `zql/src/ivm/flipped-join.ts` (v1.7.0).
//!
//! Inner join that fetches child first, then fetches matching parents
//! in batched multi-constraint queries. Uses `multiConstraints` so the
//! source can issue one SQL `IN (...)` query per chunk instead of N
//! per-child cursors. Large child sets are split into chunks of
//! `MULTI_CONSTRAINT_CHUNK_SIZE` and merged with `mergeSortedStreams`.
//!
//! Output nodes are parent nodes with at least one related child.
//! The relationship stream contains the matching children.

use std::cell::RefCell;
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::ivm::change::{
    Change, ChangeType, ChildData, make_add_change, make_child_change, make_edit_change,
    make_remove_change,
};
use crate::ivm::constraint::{Constraint, MultiConstraint, constraints_are_compatible};
use crate::ivm::data::{Node, Row, Value};
use crate::ivm::join_utils::{
    build_join_constraint, generate_with_overlay_no_yield, is_join_match,
    row_equals_for_compound_key,
};
use crate::ivm::memory_source::{NodeCompare, merge_sorted_streams};
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::{SourceSchema, System};
use crate::ivm::stream::{
    NodeStream, RelStream, StreamItem, count_data, empty_stream, from_vec, skip_yields,
};

pub type CompoundKey = Vec<String>;

const MULTI_CONSTRAINT_CHUNK_SIZE: usize = 256;

static MULTI_CONSTRAINT_CHUNK_SIZE_TEST: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(MULTI_CONSTRAINT_CHUNK_SIZE);

pub fn get_multi_constraint_chunk_size() -> usize {
    MULTI_CONSTRAINT_CHUNK_SIZE_TEST.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn set_multi_constraint_chunk_size_for_test(size: usize) -> impl FnOnce() {
    use std::sync::atomic::Ordering;
    let prev = MULTI_CONSTRAINT_CHUNK_SIZE_TEST.swap(size, Ordering::SeqCst);
    move || {
        MULTI_CONSTRAINT_CHUNK_SIZE_TEST.store(prev, Ordering::SeqCst);
    }
}

pub struct FlippedJoinArgs {
    pub parent: Shared<dyn Input>,
    pub child: Shared<dyn Input>,
    pub parent_key: CompoundKey,
    pub child_key: CompoundKey,
    pub relationship_name: String,
    pub hidden: bool,
    pub system: System,
}

pub struct FlippedJoin {
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

impl FlippedJoin {
    pub fn new(args: FlippedJoinArgs) -> Shared<FlippedJoin> {
        crate::live_count::inc(&crate::live_count::FLIPPED_JOIN);
        assert!(
            !Rc::ptr_eq(&args.parent, &args.child),
            "FlippedJoin parent and child must be different inputs"
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

        let fj = Rc::new(RefCell::new(FlippedJoin {
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

        let fj_clone = fj.clone();
        args.parent
            .borrow()
            .set_output(Rc::new(RefCell::new(ParentOutput { fj: fj_clone })));

        let fj_clone = fj.clone();
        args.child
            .borrow()
            .set_output(Rc::new(RefCell::new(ChildOutput { fj: fj_clone })));

        fj
    }

    /// The operator state `#fetchBatched` reads (TS closes over `this`); cloned
    /// up front so the lazy fetch below can run phase 2 without borrowing the
    /// operator.
    fn batch_parts(&self) -> BatchParts {
        BatchParts {
            parent_key: self.parent_key.clone(),
            child_key: self.child_key.clone(),
            compare_rows: self.schema.compare_rows.clone(),
            relationship_name: self.relationship_name.clone(),
            inprogress: self.inprogress_child_change.clone(),
            inprogress_pos: self.inprogress_child_change_position.clone(),
            parent: self.parent.clone(),
            child_schema: self.child.borrow().get_schema(),
        }
    }

    /// Port of TS `#fetchBatched` (flipped-join.ts:230-312).
    fn fetch_batched(parts: BatchParts, req: &FetchRequest, child_nodes: Vec<Node>) -> NodeStream {
        let _t = crate::perf_trace::scope("fjoin.batch_fetch");
        let BatchParts {
            parent_key,
            child_key,
            compare_rows,
            relationship_name,
            inprogress,
            inprogress_pos,
            parent,
            child_schema,
        } = parts;
        let parent_req_constraint = req.constraint.clone();
        let child_key_for_overlay = child_key.clone();
        let parent_key_for_overlay = parent_key.clone();
        let reverse = req.reverse;
        let incoming_multis = req.multi_constraints.clone();

        let mut computed_multi: Vec<Constraint> = Vec::new();
        let mut child_indexes_by_key: HashMap<String, Vec<usize>> = HashMap::new();

        for (i, child_node) in child_nodes.iter().enumerate() {
            let constraint = build_join_constraint(&child_node.row, &child_key, &parent_key);
            if constraint.is_none() {
                continue;
            }
            let c = constraint.unwrap();
            if let Some(prc) = &parent_req_constraint
                && !constraints_are_compatible(&c, prc)
            {
                continue;
            }
            let key = canonical_key_row(&c, &parent_key);
            match child_indexes_by_key.get(&key) {
                Some(existing) => {
                    let mut existing = existing.clone();
                    existing.push(i);
                    child_indexes_by_key.insert(key, existing);
                }
                None => {
                    child_indexes_by_key.insert(key.clone(), vec![i]);
                    computed_multi.push(c);
                }
            }
        }

        if computed_multi.is_empty() {
            return empty_stream();
        }

        let compare_rows_for_overlay = compare_rows.clone();
        let compare: NodeCompare = if reverse {
            Rc::new(move |a: &Node, b: &Node| compare_rows(&b.row, &a.row))
        } else {
            Rc::new(move |a: &Node, b: &Node| compare_rows(&a.row, &b.row))
        };

        let chunk_size = get_multi_constraint_chunk_size();

        let parent_stream = if computed_multi.len() <= chunk_size {
            let mut mc = incoming_multis.clone();
            mc.push(computed_multi.clone());
            let parent_req = FetchRequest {
                constraint: parent_req_constraint.clone(),
                multi_constraints: mc,
                start: req.start.clone(),
                reverse,
                ..Default::default()
            };
            parent.borrow().fetch(&parent_req)
        } else {
            let mut chunk_streams: Vec<NodeStream> = Vec::new();
            let mut i = 0;
            while i < computed_multi.len() {
                let chunk: MultiConstraint =
                    computed_multi[i..(i + chunk_size).min(computed_multi.len())].to_vec();
                let mut mc = incoming_multis.clone();
                mc.push(chunk);
                let parent_req = FetchRequest {
                    constraint: parent_req_constraint.clone(),
                    multi_constraints: mc,
                    start: req.start.clone(),
                    reverse,
                    ..Default::default()
                };
                chunk_streams.push(parent.borrow().fetch(&parent_req));
                i += chunk_size;
            }
            merge_sorted_streams(chunk_streams, compare)
        };

        let child_indexes_by_key = child_indexes_by_key;
        let pk = parent_key.clone();

        // TS :288-311: `for (const node of parentStream) { if (node ===
        // 'yield') { yield 'yield'; continue; } ... }`.
        Box::new(parent_stream.flat_map(move |item| {
            let pn = match item {
                StreamItem::Yield => return vec![StreamItem::Yield],
                StreamItem::Data(pn) => pn,
            };
            let key = canonical_key(&pn.row, &pk);
            let idxs = match child_indexes_by_key.get(&key) {
                Some(idxs) => idxs,
                None => return Vec::new(),
            };
            let related: Vec<Node> = idxs
                .iter()
                .map(|&i| child_nodes[i].clone())
                .collect::<Vec<Node>>();

            let mut overlaid = related;

            let inp = inprogress.borrow().clone();
            let inp_pos = inprogress_pos.borrow().clone();
            if let (Some(change), Some(pos)) = (inp.as_ref(), inp_pos.as_ref()) {
                let matches = is_join_match(
                    &change.node().row,
                    &child_key_for_overlay,
                    &pn.row,
                    &parent_key_for_overlay,
                );
                if matches {
                    let has_been_pushed =
                        (compare_rows_for_overlay)(&pn.row, pos) != CmpOrdering::Greater;

                    match change.change_type() {
                        ChangeType::Remove => {
                            if has_been_pushed {
                                // TS filters by REFERENCE identity
                                // (`n !== inprogressChildChange[NODE]`,
                                // flipped-join.ts:358-360): only the
                                // node spliced back in by fetch() is
                                // removed. Matching by child_key here
                                // wrongly dropped every SIBLING child
                                // sharing the join key (NEW-6), so a
                                // parent with another child vanished
                                // from mid-push fetches. The spliced
                                // node shares the change node's row
                                // `Arc`, so `Arc::ptr_eq` is the Rust
                                // twin of the TS `!==`.
                                let change_row = change.node().row.clone();
                                overlaid.retain(|n| !Arc::ptr_eq(&n.row, &change_row));
                            }
                        }
                        ChangeType::Add | ChangeType::Edit | ChangeType::Child => {
                            if !has_been_pushed {
                                let overlay_change = change.clone();
                                let cs = child_schema.clone();
                                overlaid = crate::ivm::stream::skip_yields(
                                    generate_with_overlay_no_yield(
                                        from_vec(overlaid),
                                        overlay_change,
                                        &cs,
                                    ),
                                )
                                .collect::<Vec<Node>>();
                            }
                        }
                    }
                }
            }

            if overlaid.is_empty() {
                Vec::new()
            } else {
                let rel: RelStream = Rc::new(move || from_vec(overlaid.clone()));
                let node = pn.set_relationship(&relationship_name, rel);
                vec![StreamItem::Data(node)]
            }
        }))
    }

    fn push_child_change(&self, change: &Change, pusher: &dyn InputBase) {
        *self.inprogress_child_change.borrow_mut() = Some(change.clone());
        *self.inprogress_child_change_position.borrow_mut() = None;
        let _inprogress_guard = InprogressGuard {
            change: self.inprogress_child_change.clone(),
            position: self.inprogress_child_change_position.clone(),
        };

        let child_row = change.node().row.clone();
        // "Does the parent already have another child" must compare CHILD rows
        // with the CHILD schema's comparator (TS: this.#child.getSchema()
        // .compareRows) — NOT self.schema (the parent comparator), which sorts
        // by the parent key the child rows don't carry.
        let child_compare = self.child.borrow().get_schema().compare_rows.clone();
        let constraint = build_join_constraint(&child_row, &self.child_key, &self.parent_key);

        if let Some(c) = constraint {
            let parent_input = self.parent.borrow();
            let parent_stream = skip_yields(parent_input.fetch(&FetchRequest {
                constraint: Some(c),
                ..Default::default()
            }));

            let output = self.output.borrow().clone();
            let output = output.expect("FlippedJoin output not set");

            let mut exists = matches!(change.change_type(), ChangeType::Edit | ChangeType::Child);

            for parent_node in parent_stream {
                *self.inprogress_child_change_position.borrow_mut() = Some(parent_node.row.clone());

                let relationship_name = self.relationship_name.clone();
                let child_clone = self.child.clone();
                let pk = self.parent_key.clone();
                let ck = self.child_key.clone();
                let change_clone = change.clone();

                let parent_row = parent_node.row.clone();
                let child_stream: RelStream = Rc::new(move || {
                    let cons = build_join_constraint(&parent_row, &pk, &ck);
                    match cons {
                        Some(c) => child_clone.borrow().fetch(&FetchRequest {
                            constraint: Some(c),
                            ..Default::default()
                        }),
                        None => empty_stream(),
                    }
                });

                if !exists {
                    let stream = child_stream();
                    for n in skip_yields(stream) {
                        if child_compare(&n.row, &change.node().row) != CmpOrdering::Equal {
                            exists = true;
                            break;
                        }
                    }
                }

                let new_node = parent_node
                    .clone()
                    .set_relationship(&relationship_name, child_stream);

                if exists {
                    output.borrow_mut().push(
                        make_child_change(
                            new_node,
                            ChildData {
                                relationship_name,
                                change: Box::new(change_clone.clone()),
                            },
                        ),
                        pusher,
                    );
                } else {
                    let node = parent_node.clone().set_relationship(
                        &relationship_name,
                        Rc::new(move || from_vec(vec![change_clone.node().clone()])),
                    );
                    match change.change_type() {
                        ChangeType::Add => {
                            output.borrow_mut().push(make_add_change(node), pusher);
                        }
                        ChangeType::Remove => {
                            output.borrow_mut().push(make_remove_change(node), pusher);
                        }
                        _ => {
                            output.borrow_mut().push(
                                make_child_change(
                                    node,
                                    ChildData {
                                        relationship_name: relationship_name.clone(),
                                        change: Box::new(change.clone()),
                                    },
                                ),
                                pusher,
                            );
                        }
                    }
                }
            }
        }

        *self.inprogress_child_change.borrow_mut() = None;
        *self.inprogress_child_change_position.borrow_mut() = None;
    }

    fn push_parent_change(&self, change: &Change, pusher: &dyn InputBase) {
        let output = self.output.borrow().clone();
        let output = output.expect("FlippedJoin output not set");

        let pk = self.parent_key.clone();
        let ck = self.child_key.clone();
        let child = self.child.clone();
        let relationship_name = self.relationship_name.clone();

        let constraint = build_join_constraint(&change.node().row, &pk, &ck);
        let has_child = if let Some(c) = constraint {
            let stream = child.borrow().fetch(&FetchRequest {
                constraint: Some(c),
                ..Default::default()
            });
            count_data(stream) > 0
        } else {
            false
        };

        if !has_child {
            return;
        }

        let flip = |node: Node| -> Node {
            let child_clone = child.clone();
            let pk2 = pk.clone();
            let ck2 = ck.clone();
            let node_row = node.row.clone();
            let rel: RelStream = Rc::new(move || {
                let cons = build_join_constraint(&node_row, &pk2, &ck2);
                match cons {
                    Some(c) => child_clone.borrow().fetch(&FetchRequest {
                        constraint: Some(c),
                        ..Default::default()
                    }),
                    None => empty_stream(),
                }
            });
            node.set_relationship(&relationship_name, rel)
        };

        match change {
            Change::Add(node) => {
                output
                    .borrow_mut()
                    .push(make_add_change(flip(node.clone())), pusher);
            }
            Change::Remove(node) => {
                output
                    .borrow_mut()
                    .push(make_remove_change(flip(node.clone())), pusher);
            }
            Change::Child { node, child } => {
                output
                    .borrow_mut()
                    .push(make_child_change(flip(node.clone()), child.clone()), pusher);
            }
            Change::Edit { node, old_node } => {
                assert!(
                    row_equals_for_compound_key(&old_node.row, &node.row, &pk),
                    "Parent edit must not change relationship."
                );
                output.borrow_mut().push(
                    make_edit_change(flip(node.clone()), flip(old_node.clone())),
                    pusher,
                );
            }
        }
    }
}

impl InputBase for FlippedJoin {
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

impl Input for FlippedJoin {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        let mut child_constraint: Constraint = Constraint::default();
        let mut has_child_constraint = false;
        if let Some(constraint) = &req.constraint {
            for (key, value) in constraint {
                if let Some(idx) = self.parent_key.iter().position(|k| k == key) {
                    has_child_constraint = true;
                    child_constraint.insert(self.child_key[idx].clone(), value.clone());
                }
            }
        }

        let child_req = if has_child_constraint {
            FetchRequest {
                constraint: Some(child_constraint),
                ..Default::default()
            }
        } else {
            FetchRequest::default()
        };

        // TS :176-184 collects the child fetch forwarding its `'yield'`s, then
        // (:195-203) splices the in-flight REMOVE node back in, then streams
        // `#fetchBatched` — all inside one lazy generator.
        let child_stream = self.child.borrow().fetch(&child_req);
        Box::new(FlippedJoinFetch {
            child_stream: Some(child_stream),
            child_nodes: Vec::new(),
            parts: Some(self.batch_parts()),
            req: req.clone(),
            child_compare: self.child.borrow().get_schema().compare_rows.clone(),
            parent_stream: None,
        })
    }
}

/// The operator state `#fetchBatched` closes over in TS.
struct BatchParts {
    parent_key: Vec<String>,
    child_key: Vec<String>,
    compare_rows: crate::ivm::data::Comparator,
    relationship_name: String,
    inprogress: Rc<RefCell<Option<Change>>>,
    inprogress_pos: Rc<RefCell<Option<Row>>>,
    parent: Shared<dyn Input>,
    child_schema: SourceSchema,
}

/// The generator body of TS `FlippedJoin.fetch` (flipped-join.ts:161-209).
/// Lazy like the TS generator: nothing runs until the consumer pulls, and the
/// in-flight child change is read when the child stream is exhausted.
struct FlippedJoinFetch {
    child_stream: Option<NodeStream>,
    child_nodes: Vec<Node>,
    parts: Option<BatchParts>,
    req: FetchRequest,
    child_compare: crate::ivm::data::Comparator,
    parent_stream: Option<NodeStream>,
}

impl Iterator for FlippedJoinFetch {
    type Item = StreamItem<Node>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(parent_stream) = self.parent_stream.as_mut() {
                return parent_stream.next();
            }
            let child_stream = self.child_stream.as_mut()?;
            match child_stream.next() {
                Some(StreamItem::Yield) => return Some(StreamItem::Yield),
                Some(StreamItem::Data(node)) => self.child_nodes.push(node),
                None => {
                    self.child_stream = None;
                    let parts = self
                        .parts
                        .take()
                        .expect("flipped-join fetch phase 2 runs once");
                    let mut child_nodes = std::mem::take(&mut self.child_nodes);
                    self.splice_inprogress_remove(&parts, &mut child_nodes);
                    self.parent_stream =
                        Some(FlippedJoin::fetch_batched(parts, &self.req, child_nodes));
                }
            }
        }
    }
}

impl FlippedJoinFetch {
    /// TS flipped-join.ts:186-203.
    fn splice_inprogress_remove(&self, parts: &BatchParts, child_nodes: &mut Vec<Node>) {
        let inprogress = parts.inprogress.borrow().clone();
        if let Some(ref change) = inprogress
            && change.change_type() == ChangeType::Remove
        {
            let removed = change.node().clone();
            let compare = self.child_compare.clone();
            // TS binarySearch (flipped-join.ts:198-201 → shared/binary-search)
            // returns the FIRST index with compare(removed, node) <= 0 — the
            // leftmost sorted insertion point. `partition_point` needs the
            // true-prefix predicate, i.e. the nodes STRICTLY BEFORE the removed
            // row: `removed > n`. The previous `== Less` predicate was true for
            // the SUFFIX, so mixed-position removes spliced at index 0 and the
            // relationship-row order diverged from TS on remove-push refetches
            // (NEW-4; same idiom as source.rs's existing-first partition_point).
            let insert_pos = child_nodes
                .partition_point(|n| compare(&removed.row, &n.row) == CmpOrdering::Greater);
            child_nodes.insert(insert_pos, removed);
        }
    }
}

impl Output for FlippedJoin {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {}
}

struct ParentOutput {
    fj: Shared<FlippedJoin>,
}

impl Output for ParentOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        crate::ivm::trace::recv("flipped_join#1", &change);
        self.fj.borrow().push_parent_change(&change, pusher);
    }
}

struct ChildOutput {
    fj: Shared<FlippedJoin>,
}

impl Output for ChildOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        crate::ivm::trace::recv("flipped_join#2", &change);
        self.fj.borrow().push_child_change(&change, pusher);
    }
}

fn canonical_key_row(record: &Constraint, keys: &[String]) -> String {
    let fake_row: Row = Arc::new(record.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
    canonical_key(&fake_row, keys)
}

fn canonical_key(record: &Row, keys: &[String]) -> String {
    if keys.len() == 1 {
        canonical_value(record.get(&keys[0]).unwrap_or(&Value::Null))
    } else {
        let mut s = String::new();
        for (i, key) in keys.iter().enumerate() {
            if i > 0 {
                s.push('\0');
            }
            s.push_str(&canonical_value(record.get(key).unwrap_or(&Value::Null)));
        }
        s
    }
}

fn canonical_value(v: &Value) -> String {
    match v {
        Value::Null => "n".to_string(),
        Value::Bool(true) => "t".to_string(),
        Value::Bool(false) => "f".to_string(),
        Value::F64(n) => format!("d{}", n),
        Value::Str(s) => format!("s{}", s),
        Value::Json(s) => format!("j{}", s),
    }
}

impl Drop for FlippedJoin {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::FLIPPED_JOIN);
    }
}
