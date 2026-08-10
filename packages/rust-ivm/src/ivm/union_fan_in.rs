//! UnionFanIn operator — port of `zql/src/ivm/union-fan-in.ts`.
//!
//! Merges union branches. Accumulates pushes from branches, collapses
//! them via push_accumulated_changes (same as FanIn but for union).
//! Also implements merge_fetches for sorted merge of multiple fetch streams.

use std::cell::{Cell, RefCell};
use std::cmp::Ordering as CmpOrdering;
use std::collections::HashSet;
use std::rc::Rc;

use crate::ivm::change::{Change, ChangeType};
use crate::ivm::constraint::Constraint;
use crate::ivm::data::Node;
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::push_accumulated::push_accumulated_changes;
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::{NodeStream, from_vec};

/// The UnionFanIn operator — port of TS `UnionFanIn` (union-fan-in.ts:24).
///
/// Push state (`fan_out_push_started`, `accumulated_pushes`) is interior-mutable
/// so the push path runs through `&self` — the operator's RefCell is only
/// *immutably* borrowed during a push (via the [`UfiOutput`] adapter). This is
/// the re-entrancy fix for the flipped push path: a downstream operator (Take,
/// or a nested UnionFanIn) fetches back through UnionFanIn during a push, and a
/// re-entrant *immutable* borrow is allowed where a re-entrant *mutable* borrow
/// would panic. Same pattern as `Cap`/`Exists`/`FlippedJoin` (separate Output
/// adapter; the operator itself is only immutably borrowed).
pub struct UnionFanIn {
    inputs: Vec<Shared<dyn Input>>,
    schema: SourceSchema,
    fan_out_push_started: Cell<bool>,
    accumulated_pushes: RefCell<Vec<Change>>,
    output: Rc<RefCell<Option<OutputHandle>>>,
    fan_out_relationships: HashSet<String>,
    branch_relationships: HashSet<String>,
}

impl UnionFanIn {
    pub fn new(schema: SourceSchema) -> Shared<UnionFanIn> {
        crate::live_count::inc(&crate::live_count::UNION_FAN_IN);
        let fan_out_relationships: HashSet<String> = schema.relationships.keys().cloned().collect();
        Rc::new(RefCell::new(UnionFanIn {
            inputs: Vec::new(),
            schema,
            fan_out_push_started: Cell::new(false),
            accumulated_pushes: RefCell::new(Vec::new()),
            output: Rc::new(RefCell::new(None)),
            fan_out_relationships,
            branch_relationships: HashSet::new(),
        }))
    }

    /// Build the [`OutputHandle`] the branches push to. Returns a [`UfiOutput`]
    /// adapter (NOT `ufi` itself) so UnionFanIn's RefCell is only immutably
    /// borrowed during a push — the re-entrancy fix. The builder calls this per
    /// branch (replacing the old `set_output(ufi.clone())`).
    pub fn output_adapter(ufi: Shared<UnionFanIn>) -> OutputHandle {
        Rc::new(RefCell::new(UfiOutput { ufi }))
    }

    /// Add a branch input and wire its output to this UnionFanIn.
    /// Validates that the input's schema matches the fan-out schema and
    /// merges relationships from the input. Port of TS constructor validation.
    pub fn add_input(&mut self, input: Shared<dyn Input>) {
        let input_schema = input.borrow().get_schema();

        assert_eq!(
            self.schema.table_name, input_schema.table_name,
            "Table name mismatch in union fan-in",
        );
        assert_eq!(
            self.schema.primary_key, input_schema.primary_key,
            "Primary key mismatch in union fan-in",
        );
        assert_eq!(
            self.schema.system, input_schema.system,
            "System mismatch in union fan-in",
        );
        assert_eq!(
            self.schema.sort, input_schema.sort,
            "Sort mismatch in union fan-in",
        );

        for (rel_name, rel_schema) in &input_schema.relationships {
            if self.fan_out_relationships.contains(rel_name) {
                continue;
            }
            assert!(
                !self.branch_relationships.contains(rel_name),
                "Relationship {} exists in multiple upstream inputs to union fan-in",
                rel_name,
            );
            self.schema
                .relationships
                .insert(rel_name.clone(), rel_schema.clone());
            self.schema.relationship_order.push(rel_name.clone());
            self.branch_relationships.insert(rel_name.clone());
        }

        self.inputs.push(input.clone());
        // The builder wires branch outputs via output_adapter() (a UfiOutput
        // adapter), NOT ufi directly — so UnionFanIn's RefCell is only
        // immutably borrowed during a push (re-entrancy fix).
    }

    /// Called by UnionFanOut to signal the start of a push batch.
    pub fn fan_out_started_pushing(&self) {
        assert!(
            !self.fan_out_push_started.get(),
            "UnionFanIn: fanOutStartedPushing called while already pushing"
        );
        self.fan_out_push_started.set(true);
    }

    /// Called by UnionFanOut after all branches have been pushed.
    /// Triggers `push_accumulated_changes`.
    ///
    /// `&self` (interior-mutable) so UnionFanOut can call this via an immutable
    /// borrow — UnionFanIn's RefCell is not borrow_mut'd during the push, so a
    /// re-entrant fetch from downstream (e.g. Take) succeeds.
    pub fn fan_out_done_pushing(&self, fan_out_change_type: ChangeType, pusher: &dyn InputBase) {
        assert!(
            self.fan_out_push_started.get(),
            "UnionFanIn: fanOutDonePushing called without fanOutStartedPushing"
        );
        self.fan_out_push_started.set(false);

        if self.inputs.is_empty() {
            return;
        }
        if self.accumulated_pushes.borrow().is_empty() {
            return;
        }

        let output = self.output.borrow().clone();
        if let Some(output) = output {
            // fan_out_push_started is now false, so a re-entrant push during
            // push_accumulated_changes routes to push_internal_change (not the
            // accumulate path) — no re-entrant borrow_mut on accumulated_pushes.
            push_accumulated_changes(
                &mut self.accumulated_pushes.borrow_mut(),
                &output,
                pusher,
                fan_out_change_type,
                &self.schema,
            );
        }
    }

    /// Push an internal change (from within the UFO/UFI sub-graph).
    /// For child: always forward (child branches are unique).
    /// For add: forward iff the row is in exactly 1 branch (the pusher).
    ///   If 2+ branches have it, the add was already emitted.
    /// For remove: forward iff no branch has the row.
    ///   If another branch has it, that branch will send the remove.
    fn push_internal_change(&self, change: Change, pusher: &dyn InputBase) {
        match change.change_type() {
            ChangeType::Child | ChangeType::Edit => {
                let output = self.output.borrow().clone();
                if let Some(output) = output {
                    output.borrow_mut().push(change, pusher);
                }
            }
            ChangeType::Add | ChangeType::Remove => {
                let node = change.node().clone();
                let pk = self.schema.primary_key.clone();

                let mut match_count = 0usize;
                for input in &self.inputs {
                    let constraint: Constraint = pk
                        .iter()
                        .map(|k| (k.clone(), node.row.get(k).cloned().unwrap_or(Value::Null)))
                        .collect();

                    let req = FetchRequest {
                        constraint: Some(constraint),
                        ..Default::default()
                    };

                    if crate::ivm::stream::first(input.borrow().fetch(&req)).is_some() {
                        match_count += 1;
                    }
                }

                let should_forward = match change.change_type() {
                    ChangeType::Add => match_count <= 1,
                    ChangeType::Remove => match_count == 0,
                    _ => unreachable!(),
                };

                if should_forward {
                    let output = self.output.borrow().clone();
                    if let Some(output) = output {
                        output.borrow_mut().push(change, pusher);
                    }
                }
            }
        }
    }
}

impl InputBase for UnionFanIn {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        for input in &self.inputs {
            input.borrow_mut().destroy();
        }
        // Break the Rc cycle: clear the back-edge to the downstream output.
        *self.output.borrow_mut() = None;
    }
}

impl Input for UnionFanIn {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        let compare_rows = self.schema.compare_rows.clone();
        let compare_rows2 = self.schema.compare_rows.clone();
        let reverse = req.reverse;
        let reverse2 = req.reverse;

        let mut streams: Vec<NodeStream> = Vec::new();
        for input in &self.inputs {
            streams.push(input.borrow().fetch(req));
        }

        let compare: Rc<dyn Fn(&Node, &Node) -> CmpOrdering> = if reverse {
            Rc::new(move |l: &Node, r: &Node| compare_rows(&r.row, &l.row))
        } else {
            Rc::new(move |l: &Node, r: &Node| compare_rows(&l.row, &r.row))
        };

        if streams.is_empty() {
            return from_vec(Vec::new());
        }
        if streams.len() == 1 {
            return streams.into_iter().next().unwrap();
        }

        let merged = crate::ivm::source::merge_sorted_streams(streams, compare);

        let compare_dedup: Rc<dyn Fn(&Node, &Node) -> CmpOrdering> = if reverse2 {
            Rc::new(move |l: &Node, r: &Node| compare_rows2(&r.row, &l.row))
        } else {
            Rc::new(move |l: &Node, r: &Node| compare_rows2(&l.row, &r.row))
        };
        let mut last: Option<Node> = None;
        Box::new(merged.filter_map(move |item| {
            use crate::ivm::stream::StreamItem;
            let node = match item {
                StreamItem::Data(n) => n,
                StreamItem::Yield => return Some(StreamItem::Yield),
            };
            let is_dup = last
                .as_ref()
                .map(|l| compare_dedup(l, &node) == CmpOrdering::Equal)
                .unwrap_or(false);
            last = Some(node.clone());
            if !is_dup {
                Some(StreamItem::Data(node))
            } else {
                None
            }
        }))
    }
}

impl Output for UnionFanIn {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {
        // Pushes arrive via the UfiOutput adapter (re-entrancy fix: the adapter
        // borrows UnionFanIn immutably, so a re-entrant fetch during a push
        // succeeds instead of panicking on a live borrow_mut).
    }
}

/// Output adapter that receives pushes from branches and delegates to
/// UnionFanIn. The adapter's RefCell is borrow_mut'd during a push; UnionFanIn
/// itself is only *immutably* borrowed (via `self.ufi.borrow()`), so a
/// re-entrant fetch from downstream (Take, or a nested UnionFanIn) takes an
/// immutable borrow that succeeds alongside the live one. This is the
/// re-entrancy fix for the flipped push path — same pattern as CapOutput /
/// ExistsOutput / FlippedJoin's ParentOutput/ChildOutput.
struct UfiOutput {
    ufi: Shared<UnionFanIn>,
}

impl UfiOutput {
    /// Route a push into UnionFanIn via an immutable borrow.
    fn push_internal(&self, change: Change, pusher: &dyn InputBase) {
        crate::ivm::trace::recv("union_fan_in#1", &change);
        let ufi = self.ufi.borrow();
        if !ufi.fan_out_push_started.get() {
            ufi.push_internal_change(change, pusher);
        } else {
            ufi.accumulated_pushes.borrow_mut().push(change);
        }
    }
}

impl Output for UfiOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        self.push_internal(change, pusher);
    }
}

/// Merge multiple sorted fetch streams into one sorted stream, deduplicating.
/// Port of TS `mergeFetches` (union-fan-in.ts:196).
pub fn merge_fetches(
    fetches: Vec<Vec<Node>>,
    compare: &dyn Fn(&Node, &Node) -> CmpOrdering,
) -> Vec<Node> {
    let mut result: Vec<Node> = Vec::new();
    let mut indices = vec![0usize; fetches.len()];
    let mut last_yielded: Option<Node> = None;

    loop {
        let mut min: Option<(usize, Node)> = None;
        for (i, fetch) in fetches.iter().enumerate() {
            if indices[i] < fetch.len() {
                let node = &fetch[indices[i]];
                match &min {
                    None => min = Some((i, node.clone())),
                    Some((_, min_node)) => {
                        if compare(node, min_node) == CmpOrdering::Less {
                            min = Some((i, node.clone()));
                        }
                    }
                }
            }
        }

        match min {
            None => break,
            Some((idx, node)) => {
                indices[idx] += 1;
                // Deduplicate: skip if same as last yielded.
                let is_dup = last_yielded
                    .as_ref()
                    .map(|last| compare(last, &node) == CmpOrdering::Equal)
                    .unwrap_or(false);
                if !is_dup {
                    result.push(node.clone());
                    last_yielded = Some(node);
                }
            }
        }
    }

    result
}

use crate::ivm::data::Value;

impl Drop for UnionFanIn {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::UNION_FAN_IN);
    }
}
