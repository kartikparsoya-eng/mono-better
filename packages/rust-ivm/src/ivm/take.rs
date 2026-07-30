//! Take operator — port of `zql/src/ivm/take.ts`.
//!
//! Limit operator: takes the first n nodes as determined by the comparator.
//! Maintains a bound (the last accepted row) so it can evaluate whether
//! new pushes should be accepted or rejected.
//!
//! Can count rows globally or by unique value of a partition key.
//! Maintains the invariant that output size <= limit at all times, even
//! mid-processing of a push.

use std::cell::{Cell, RefCell};
use std::cmp::Ordering as CmpOrdering;
use std::rc::Rc;
use std::sync::Arc;

use crate::ivm::change::{Change, ChangeType, make_add_change, make_remove_change};
use crate::ivm::constraint::{Constraint, constraint_matches_primary_key};
use crate::ivm::data::{Comparator, Node, Row, Value, compare_values};
use crate::ivm::operator::{
    Basis, FetchRequest, Input, InputBase, Output, OutputHandle, Shared, Start,
};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::{NodeStream, StreamItem, first as stream_first, from_vec, skip_yields};

const MAX_BOUND_KEY: &str = "maxBound";

/// Take state — tracks count and bound per partition.
#[derive(Clone, Debug)]
pub struct TakeState {
    size: usize,
    bound: Option<Row>,
}

/// Storage for Take state — tracks size/bound per partition key.
#[derive(Default)]
pub struct TakeStorage {
    states: std::collections::HashMap<String, TakeState>,
}

impl TakeStorage {
    pub fn new() -> Self {
        TakeStorage {
            states: std::collections::HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&TakeState> {
        self.states.get(key)
    }

    pub fn set(&mut self, key: String, state: TakeState) {
        self.states.insert(key, state);
    }

    pub fn del(&mut self, key: &str) {
        self.states.remove(key);
    }
}

/// Partition key — same as PrimaryKey.
pub type PartitionKey = Vec<String>;

/// RAII guard for `Take::initial_fetch`, porting the TS `finally` block
/// (take.ts:198-215). During hydration the input stream must be consumed until
/// the limit is reached or the input is exhausted, so the take state is fully
/// hydrated. If the consumer drops the stream early (before either), the state
/// would be under-hydrated. TS persists the (partial) state then asserts
/// `!downstreamEarlyReturn` — which throws and resets the pipeline. We mirror
/// that: on a clean early drop we persist the partial state and panic (caught
/// at the napi boundary -> reset). We skip both if a panic is already in flight
/// (Rust analog of TS's `if (!exceptionThrown)`) so we never double-panic.
struct InitialFetchGuard {
    persisted: Rc<Cell<bool>>,
    count: Rc<Cell<usize>>,
    bound: Rc<RefCell<Option<Row>>>,
    storage: Shared<TakeStorage>,
    key: String,
    compare: Comparator,
}

impl Drop for InitialFetchGuard {
    fn drop(&mut self) {
        // Reached limit or exhausted the input: state already persisted inline.
        if self.persisted.get() {
            return;
        }
        // A panic is already unwinding (TS `exceptionThrown`): do nothing.
        if std::thread::panicking() {
            return;
        }
        // Clean early return: persist the partial state (TS `setTakeState` in
        // finally) then reset via panic.
        let b = self.bound.borrow().clone();
        let size = self.count.get();
        self.storage.borrow_mut().set(
            self.key.clone(),
            TakeState {
                size,
                bound: b.clone(),
            },
        );
        if let Some(ref bval) = b {
            let current_max = self
                .storage
                .borrow()
                .get(MAX_BOUND_KEY)
                .and_then(|s| s.bound.clone());
            if current_max
                .as_ref()
                .is_none_or(|m| (self.compare)(bval, m) == CmpOrdering::Greater)
            {
                self.storage
                    .borrow_mut()
                    .set(MAX_BOUND_KEY.to_string(), TakeState { size: 0, bound: b });
            }
        }
        panic!("Take: unexpected early return prevented full hydration");
    }
}

/// The Take operator — port of TS `Take` (take.ts:53).
pub struct Take {
    input: Shared<dyn Input>,
    storage: Shared<TakeStorage>,
    limit: usize,
    partition_key: Option<PartitionKey>,
    partition_key_comparator: Option<Comparator>,
    /// Fetch overlay needed for remove-before-add. Shared so TakeOutput (which
    /// holds the Take via Shared<Take>) can set it and Take::fetch can read it.
    row_hidden_from_fetch: Rc<RefCell<Option<Row>>>,
    schema: SourceSchema,
    output: Rc<RefCell<Option<OutputHandle>>>,
}

/// RAII guard that clears row_hidden_from_fetch on drop, even if a panic occurs.
struct HiddenRowGuard(Rc<RefCell<Option<Row>>>);
impl Drop for HiddenRowGuard {
    fn drop(&mut self) {
        *self.0.borrow_mut() = None;
    }
}

impl Take {
    pub fn new(
        input: Shared<dyn Input>,
        storage: Shared<TakeStorage>,
        limit: usize,
        partition_key: Option<PartitionKey>,
    ) -> Shared<Take> {
        // limit is usize, always >= 0. TS asserts limit >= 0 but that's
        // trivially true for unsigned types.
        debug_assert!(limit < usize::MAX, "Limit must be reasonable");
        let schema = input.borrow().get_schema();
        let sort = schema.sort.clone();
        let pk_comparator = partition_key.as_ref().map(make_partition_key_comparator);

        let take = Rc::new(RefCell::new(Take {
            input: input.clone(),
            storage,
            limit,
            partition_key,
            partition_key_comparator: pk_comparator,
            row_hidden_from_fetch: Rc::new(RefCell::new(None)),
            schema: if sort.is_some() {
                schema
            } else {
                let pk = schema.primary_key.clone();
                let order: Vec<[String; 2]> =
                    pk.iter().map(|k| [k.clone(), "asc".to_string()]).collect();
                SourceSchema {
                    sort: Some(Arc::new(order)),
                    ..schema
                }
            },
            output: Rc::new(RefCell::new(None)),
        }));

        let take_clone = take.clone();
        input
            .borrow()
            .set_output(Rc::new(RefCell::new(TakeOutput { take: take_clone })));
        take
    }

    fn compare_rows(&self) -> &Comparator {
        &self.schema.compare_rows
    }

    #[allow(dead_code)]
    fn get_take_state_key(&self, row_or_constraint: Option<(&Row, &Constraint)>) -> String {
        match (&self.partition_key, row_or_constraint) {
            (Some(pk), Some((row, c))) => {
                // Prefer constraint values; fall back to row values
                let mut key = String::new();
                for col in pk {
                    let v = c
                        .get(col)
                        .cloned()
                        .or_else(|| row.get(col).cloned())
                        .unwrap_or(Value::Null);
                    key.push_str(&format!("{}={:?};", col, v));
                }
                key
            }
            (None, _) => "global".to_string(),
            (Some(_), None) => "global".to_string(),
        }
    }

    fn get_state_and_constraint(
        &self,
        row: &Row,
    ) -> Option<(TakeState, String, Option<Row>, Option<Constraint>)> {
        let take_state_key = match &self.partition_key {
            Some(pk) => {
                let mut key = String::new();
                for col in pk {
                    if let Some(v) = row.get(col) {
                        key.push_str(&format!("{}={:?};", col, v));
                    }
                }
                key
            }
            None => "global".to_string(),
        };

        let take_state = self.storage.borrow().get(&take_state_key).cloned()?;
        let max_bound = self
            .storage
            .borrow()
            .get(MAX_BOUND_KEY)
            .and_then(|s| s.bound.clone());
        let constraint = self.partition_key.as_ref().map(|pk| {
            pk.iter()
                .map(|k| (k.clone(), row.get(k).cloned().unwrap_or(Value::Null)))
                .collect::<Constraint>()
        });

        Some((take_state, take_state_key, max_bound, constraint))
    }

    fn set_take_state(
        &self,
        take_state_key: &str,
        size: usize,
        bound: Option<Row>,
        _max_bound: Option<Row>,
    ) {
        self.storage.borrow_mut().set(
            take_state_key.to_string(),
            TakeState {
                size,
                bound: bound.clone(),
            },
        );
        if let Some(ref b) = bound {
            let current_max = self
                .storage
                .borrow()
                .get(MAX_BOUND_KEY)
                .and_then(|s| s.bound.clone());
            let should_update = current_max
                .as_ref()
                .is_none_or(|m| (self.compare_rows())(b, m) == CmpOrdering::Greater);
            if should_update {
                self.storage.borrow_mut().set(
                    MAX_BOUND_KEY.to_string(),
                    TakeState {
                        size: 0,
                        bound: Some(b.clone()),
                    },
                );
            }
        }
    }

    fn initial_fetch(&self, req: &FetchRequest, take_state_key: &str) -> NodeStream {
        if self.limit == 0 {
            self.set_take_state(take_state_key, 0, None, None);
            return from_vec(Vec::new());
        }

        let mut stream = self.input.borrow().fetch(req);
        let limit = self.limit;
        let take_state_key = take_state_key.to_string();
        let storage = self.storage.clone();
        let compare = self.compare_rows().clone();

        // Lazy: yield nodes one at a time, recording bound as a side effect.
        // State is persisted when the stream is exhausted or limit reached.
        // Port of TS Take.#initialFetch (take.ts:138-165).
        let count = Rc::new(Cell::new(0usize));
        let bound: Rc<RefCell<Option<Row>>> = Rc::new(RefCell::new(None));
        let persisted = Rc::new(Cell::new(false));

        let count_c = count.clone();
        let bound_c = bound.clone();
        let persisted_c = persisted.clone();
        let storage_c = storage.clone();
        let take_state_key_c = take_state_key.clone();

        // Fires the TS `finally` early-return assert when the stream is dropped
        // before limit/exhaustion. Captured by the closure so it drops with the
        // iterator.
        let early_return_guard = InitialFetchGuard {
            persisted: persisted.clone(),
            count: count.clone(),
            bound: bound.clone(),
            storage: storage.clone(),
            key: take_state_key.clone(),
            compare: compare.clone(),
        };

        Box::new(std::iter::from_fn(move || {
            let _ = &early_return_guard; // keep the guard owned by this closure
            if persisted_c.get() {
                return None;
            }
            match stream.next() {
                Some(StreamItem::Yield) => Some(StreamItem::Yield),
                Some(StreamItem::Data(node)) => {
                    *bound_c.borrow_mut() = Some(node.row.clone());
                    let c = count_c.get() + 1;
                    count_c.set(c);
                    if c >= limit {
                        let b = bound_c.borrow().clone();
                        storage_c.borrow_mut().set(
                            take_state_key_c.clone(),
                            TakeState {
                                size: c,
                                bound: b.clone(),
                            },
                        );
                        // Update max bound
                        let current_max = storage_c
                            .borrow()
                            .get(MAX_BOUND_KEY)
                            .and_then(|s| s.bound.clone());
                        if let Some(ref bval) = b
                            && current_max
                                .as_ref()
                                .is_none_or(|m| compare(bval, m) == CmpOrdering::Greater)
                        {
                            storage_c.borrow_mut().set(
                                MAX_BOUND_KEY.to_string(),
                                TakeState {
                                    size: 0,
                                    bound: Some(bval.clone()),
                                },
                            );
                        }
                        persisted_c.set(true);
                    }
                    Some(StreamItem::Data(node))
                }
                None => {
                    let b = bound_c.borrow().clone();
                    let size = count_c.get();
                    storage_c.borrow_mut().set(
                        take_state_key_c.clone(),
                        TakeState {
                            size,
                            bound: b.clone(),
                        },
                    );
                    // Update max bound
                    if let Some(ref bval) = b {
                        let current_max = storage_c
                            .borrow()
                            .get(MAX_BOUND_KEY)
                            .and_then(|s| s.bound.clone());
                        if current_max
                            .as_ref()
                            .is_none_or(|m| compare(bval, m) == CmpOrdering::Greater)
                        {
                            storage_c
                                .borrow_mut()
                                .set(MAX_BOUND_KEY.to_string(), TakeState { size: 0, bound: b });
                        }
                    }
                    persisted_c.set(true);
                    None
                }
            }
        }))
    }

    fn push_change(&self, change: &Change, pusher: &dyn InputBase) {
        let output = self.output.borrow().clone();
        let Some(output) = output else { return };

        match change.change_type() {
            ChangeType::Edit => {
                self.push_edit_change(change, &output, pusher);
            }
            ChangeType::Add => {
                let (node, _) = match change {
                    Change::Add(n) => (n.clone(), ()),
                    _ => unreachable!(),
                };
                self.push_add_change(&node, &output, pusher);
            }
            ChangeType::Remove => {
                let (node, _) = match change {
                    Change::Remove(n) => (n.clone(), ()),
                    _ => unreachable!(),
                };
                self.push_remove_change(&node, &output, pusher);
            }
            ChangeType::Child => {
                let (node, _) = match change {
                    Change::Child { node, .. } => (node.clone(), ()),
                    _ => unreachable!(),
                };
                self.push_child_change(change, &node, &output, pusher);
            }
        }
    }

    fn push_add_change(&self, node: &Node, output: &OutputHandle, pusher: &dyn InputBase) {
        let Some((take_state, take_state_key, max_bound, constraint)) =
            self.get_state_and_constraint(&node.row)
        else {
            return;
        };
        let compare = self.compare_rows().clone();

        if take_state.size < self.limit {
            let new_bound = if take_state
                .bound
                .as_ref()
                .is_none_or(|b| compare(b, &node.row) == CmpOrdering::Less)
            {
                Some(node.row.clone())
            } else {
                take_state.bound.clone()
            };
            self.set_take_state(&take_state_key, take_state.size + 1, new_bound, max_bound);
            output
                .borrow_mut()
                .push(make_add_change(node.clone()), pusher);
            return;
        }

        // size === limit
        let Some(bound) = &take_state.bound else {
            return;
        };
        if compare(&node.row, bound) != CmpOrdering::Less {
            return;
        }

        // added row < bound — need to remove the bound row and add the new row
        let bound_node = if self.limit == 1 {
            // Fetch the bound row itself
            let req = FetchRequest {
                start: Some(Start {
                    row: bound.clone(),
                    basis: Basis::At,
                }),
                constraint: constraint.clone(),
                ..Default::default()
            };
            stream_first(self.input.borrow().fetch(&req))
        } else {
            // Fetch bound and the row before it
            let req = FetchRequest {
                start: Some(Start {
                    row: bound.clone(),
                    basis: Basis::At,
                }),
                constraint: constraint.clone(),
                reverse: true,
                ..Default::default()
            };
            let mut iter = skip_yields(self.input.borrow().fetch(&req));
            let bound_node = iter.next();
            let before_bound_node = iter.next();
            // Update bound to the row before the old bound (or the new row if it's larger)
            if let Some(ref bbn) = before_bound_node {
                let new_bound = if compare(&node.row, &bbn.row) == CmpOrdering::Greater {
                    Some(node.row.clone())
                } else {
                    Some(bbn.row.clone())
                };
                self.set_take_state(
                    &take_state_key,
                    take_state.size,
                    new_bound,
                    max_bound.clone(),
                );
            } else {
                self.set_take_state(
                    &take_state_key,
                    take_state.size,
                    Some(node.row.clone()),
                    max_bound.clone(),
                );
            }
            bound_node
        };

        if self.limit > 1 {
            // For limit > 1, we already set the state in the else branch above
            // Just need to do the remove-before-add with row hiding
            if let Some(bn) = bound_node {
                // Remove before add to maintain invariant output size <= limit
                self.push_with_row_hidden_from_fetch(
                    &node.row,
                    make_remove_change(bn),
                    output,
                    pusher,
                );
                output
                    .borrow_mut()
                    .push(make_add_change(node.clone()), pusher);
            }
            return;
        }

        // limit == 1
        if let Some(bn) = bound_node {
            self.set_take_state(
                &take_state_key,
                take_state.size,
                Some(node.row.clone()),
                max_bound.clone(),
            );
            self.push_with_row_hidden_from_fetch(&node.row, make_remove_change(bn), output, pusher);
            output
                .borrow_mut()
                .push(make_add_change(node.clone()), pusher);
        }
    }

    fn push_remove_change(&self, node: &Node, output: &OutputHandle, pusher: &dyn InputBase) {
        let Some((take_state, take_state_key, max_bound, constraint)) =
            self.get_state_and_constraint(&node.row)
        else {
            return;
        };
        let compare = self.compare_rows().clone();

        let Some(bound) = &take_state.bound else {
            return;
        };
        let comp_to_bound = compare(&node.row, bound);
        if comp_to_bound == CmpOrdering::Greater {
            return;
        }

        // Find the row before the bound (replacement candidate)
        let req = FetchRequest {
            start: Some(Start {
                row: bound.clone(),
                basis: Basis::After,
            }),
            constraint: constraint.clone(),
            reverse: true,
            ..Default::default()
        };
        let before_bound_node = stream_first(self.input.borrow().fetch(&req));

        let mut new_bound: Option<(Node, bool)> = None;
        if let Some(ref bbn) = before_bound_node {
            let push = compare(&bbn.row, bound) == CmpOrdering::Greater;
            new_bound = Some((bbn.clone(), push));
        }

        if new_bound.as_ref().is_none_or(|(_, push)| !push) {
            // Iterate the at-bound stream to find the first row > bound.
            // Port of TS: always set newBound to each found node, break when push=true.
            let req = FetchRequest {
                start: Some(Start {
                    row: bound.clone(),
                    basis: Basis::At,
                }),
                constraint: constraint.clone(),
                ..Default::default()
            };
            for n in skip_yields(self.input.borrow().fetch(&req)) {
                let push = compare(&n.row, bound) == CmpOrdering::Greater;
                new_bound = Some((n, push));
                if push {
                    break;
                }
            }
        }

        if let Some((new_bound_node, true)) = new_bound {
            output
                .borrow_mut()
                .push(make_remove_change(node.clone()), pusher);
            self.set_take_state(
                &take_state_key,
                take_state.size,
                Some(new_bound_node.row.clone()),
                max_bound,
            );
            output
                .borrow_mut()
                .push(make_add_change(new_bound_node), pusher);
        } else {
            let new_bound_row = new_bound.map(|(n, _)| n.row);
            self.set_take_state(
                &take_state_key,
                take_state.size - 1,
                new_bound_row,
                max_bound,
            );
            output
                .borrow_mut()
                .push(make_remove_change(node.clone()), pusher);
        }
    }

    fn push_child_change(
        &self,
        change: &Change,
        node: &Node,
        output: &OutputHandle,
        pusher: &dyn InputBase,
    ) {
        let Some((take_state, _, _, _)) = self.get_state_and_constraint(&node.row) else {
            return;
        };
        let compare = self.compare_rows().clone();
        if take_state
            .bound
            .as_ref()
            .is_none_or(|b| compare(&node.row, b) != CmpOrdering::Greater)
        {
            output.borrow_mut().push(change.clone(), pusher);
        }
    }

    fn push_edit_change(&self, change: &Change, output: &OutputHandle, pusher: &dyn InputBase) {
        let (node, old_node) = match change {
            Change::Edit { node, old_node } => (node.clone(), old_node.clone()),
            _ => unreachable!(),
        };

        // Assert partition key didn't change
        if let Some(ref pkc) = self.partition_key_comparator {
            assert!(
                pkc(&old_node.row, &node.row) == CmpOrdering::Equal,
                "Unexpected change of partition key"
            );
        }

        let Some((take_state, take_state_key, max_bound, constraint)) =
            self.get_state_and_constraint(&old_node.row)
        else {
            return;
        };
        let Some(bound) = &take_state.bound else {
            return;
        };
        let compare = self.compare_rows().clone();

        let old_cmp = compare(&old_node.row, bound);
        let new_cmp = compare(&node.row, bound);

        // The bound row was changed
        if old_cmp == CmpOrdering::Equal {
            if new_cmp == CmpOrdering::Equal {
                // No change to bound
                output.borrow_mut().push(change.clone(), pusher);
                return;
            }
            if new_cmp == CmpOrdering::Less {
                if self.limit == 1 {
                    self.set_take_state(
                        &take_state_key,
                        take_state.size,
                        Some(node.row.clone()),
                        max_bound,
                    );
                    output.borrow_mut().push(change.clone(), pusher);
                    return;
                }
                // Find the row before the bound
                let req = FetchRequest {
                    start: Some(Start {
                        row: bound.clone(),
                        basis: Basis::After,
                    }),
                    constraint: constraint.clone(),
                    reverse: true,
                    ..Default::default()
                };
                if let Some(before_bound) = stream_first(self.input.borrow().fetch(&req)) {
                    self.set_take_state(
                        &take_state_key,
                        take_state.size,
                        Some(before_bound.row.clone()),
                        max_bound,
                    );
                }
                output.borrow_mut().push(change.clone(), pusher);
                return;
            }
            // new_cmp > 0: new row is outside the bound (TS take.ts:517).
            assert!(
                new_cmp == CmpOrdering::Greater,
                "New comparison must be greater than 0"
            );
            // Find the first item at the old bound — it becomes the new bound
            let req = FetchRequest {
                start: Some(Start {
                    row: bound.clone(),
                    basis: Basis::At,
                }),
                constraint: constraint.clone(),
                ..Default::default()
            };
            if let Some(new_bound_node) = stream_first(self.input.borrow().fetch(&req)) {
                if compare(&new_bound_node.row, &node.row) == CmpOrdering::Equal {
                    // The new row is the next row — replace bound and keep the edit
                    self.set_take_state(
                        &take_state_key,
                        take_state.size,
                        Some(node.row.clone()),
                        max_bound,
                    );
                    output.borrow_mut().push(change.clone(), pusher);
                    return;
                }
                // The new row is outside bounds — remove old row, add new bound
                self.set_take_state(
                    &take_state_key,
                    take_state.size,
                    Some(new_bound_node.row.clone()),
                    max_bound,
                );
                self.push_with_row_hidden_from_fetch(
                    &new_bound_node.row,
                    make_remove_change(old_node.clone()),
                    output,
                    pusher,
                );
                output
                    .borrow_mut()
                    .push(make_add_change(new_bound_node), pusher);
            }
            return;
        }

        if old_cmp == CmpOrdering::Greater {
            assert!(
                new_cmp != CmpOrdering::Equal,
                "Invalid state. Row has duplicate primary key"
            );
            if new_cmp == CmpOrdering::Greater {
                return;
            } // both outside
            // old was outside, new is inside — push out the old bound (TS take.ts:571).
            assert!(
                new_cmp == CmpOrdering::Less,
                "New comparison must be less than 0"
            );
            let req = FetchRequest {
                start: Some(Start {
                    row: bound.clone(),
                    basis: Basis::At,
                }),
                constraint: constraint.clone(),
                reverse: true,
                ..Default::default()
            };
            let mut iter = skip_yields(self.input.borrow().fetch(&req));
            let old_bound_node = iter.next();
            let new_bound_node = iter.next();
            if let (Some(obn), Some(nbn)) = (old_bound_node, new_bound_node) {
                self.set_take_state(
                    &take_state_key,
                    take_state.size,
                    Some(nbn.row.clone()),
                    max_bound,
                );
                self.push_with_row_hidden_from_fetch(
                    &node.row,
                    make_remove_change(obn),
                    output,
                    pusher,
                );
                output.borrow_mut().push(make_add_change(node), pusher);
            }
            return;
        }

        // old_cmp == Less
        assert!(
            new_cmp != CmpOrdering::Equal,
            "Invalid state. Row has duplicate primary key"
        );
        if new_cmp == CmpOrdering::Less {
            // both inside bounds
            output.borrow_mut().push(change.clone(), pusher);
            return;
        }
        // old was inside, new is outside (greater than bound) (TS take.ts:630).
        assert!(
            new_cmp == CmpOrdering::Greater,
            "New comparison must be greater than 0"
        );
        // Find the row after the bound
        let req = FetchRequest {
            start: Some(Start {
                row: bound.clone(),
                basis: Basis::After,
            }),
            constraint: constraint.clone(),
            ..Default::default()
        };
        if let Some(after_bound) = stream_first(self.input.borrow().fetch(&req)) {
            if compare(&after_bound.row, &node.row) == CmpOrdering::Equal {
                // New row is the new bound — use edit change
                self.set_take_state(
                    &take_state_key,
                    take_state.size,
                    Some(node.row.clone()),
                    max_bound,
                );
                output.borrow_mut().push(change.clone(), pusher);
                return;
            }
            output
                .borrow_mut()
                .push(make_remove_change(old_node.clone()), pusher);
            self.set_take_state(
                &take_state_key,
                take_state.size,
                Some(after_bound.row.clone()),
                max_bound,
            );
            output
                .borrow_mut()
                .push(make_add_change(after_bound), pusher);
        }
    }

    fn push_with_row_hidden_from_fetch(
        &self,
        row: &Row,
        change: Change,
        output: &OutputHandle,
        pusher: &dyn InputBase,
    ) {
        *self.row_hidden_from_fetch.borrow_mut() = Some(row.clone());
        let _guard = HiddenRowGuard(self.row_hidden_from_fetch.clone());
        output.borrow_mut().push(change, pusher);
        // Cleared by _guard Drop
    }
}

impl InputBase for Take {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.input.borrow_mut().destroy();
    }
}

impl Input for Take {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        // Check for existing take state
        let take_state_key = match (&self.partition_key, req.constraint.as_ref()) {
            (Some(pk), Some(c)) => {
                let mut key = String::new();
                for col in pk {
                    if let Some(v) = c.get(col) {
                        key.push_str(&format!("{}={:?};", col, v));
                    }
                }
                key
            }
            (None, _) => "global".to_string(),
            (Some(_), None) => "global".to_string(),
        };

        let take_state = self.storage.borrow().get(&take_state_key).cloned();

        match take_state {
            None => self.initial_fetch(req, &take_state_key),
            Some(ts) => {
                if self.limit == 0 || ts.bound.is_none() {
                    return from_vec(Vec::new());
                }
                let bound = ts.bound.unwrap();
                let compare = self.compare_rows().clone();
                let hidden = self.row_hidden_from_fetch.borrow().clone();
                let input = self.input.borrow();
                let stream = input.fetch(req);
                Box::new(stream.filter_map(move |item| match item {
                    crate::ivm::stream::StreamItem::Data(n) => {
                        if compare(&n.row, &bound) == CmpOrdering::Greater {
                            return None;
                        }
                        if let Some(ref h) = hidden
                            && compare(&n.row, h) == CmpOrdering::Equal
                        {
                            return None;
                        }
                        Some(crate::ivm::stream::StreamItem::Data(n))
                    }
                    crate::ivm::stream::StreamItem::Yield => {
                        Some(crate::ivm::stream::StreamItem::Yield)
                    }
                }))
            }
        }
    }
}

impl Output for Take {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {}
}

/// Output adapter that forwards pushes to the Take operator.
struct TakeOutput {
    take: Shared<Take>,
}

impl Output for TakeOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        self.take.borrow().push_change(&change, pusher);
    }
}

/// Make a partition key comparator.
fn make_partition_key_comparator(partition_key: &PartitionKey) -> Comparator {
    let pk = partition_key.clone();
    Rc::new(move |a: &Row, b: &Row| {
        for col in &pk {
            let av = a.get(col).cloned().unwrap_or(Value::Null);
            let bv = b.get(col).cloned().unwrap_or(Value::Null);
            let cmp = compare_values(&av, &bv);
            if cmp != CmpOrdering::Equal {
                return cmp;
            }
        }
        CmpOrdering::Equal
    })
}

/// Check if a constraint matches a partition key.
pub fn constraint_matches_partition_key(
    constraint: &Constraint,
    partition_key: &PartitionKey,
) -> bool {
    constraint_matches_primary_key(constraint, partition_key)
}
