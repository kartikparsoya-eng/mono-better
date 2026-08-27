//! FanIn operator — port of `zql/src/ivm/fan-in.ts`.
//!
//! Merges the branches forked by a `FanOut` back into one filter stream,
//! eliminating duplicates. Accumulates branch pushes and collapses them via
//! `push_accumulated_changes` when the fan-out signals it is done.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::change::{Change, ChangeType};
use crate::ivm::data::Node;
use crate::ivm::filter_operators::{
    FilterChainPusher, FilterInput, FilterInputHandle, FilterOutput, FilterOutputAsOutput,
    FilterOutputHandle,
};
use crate::ivm::operator::{InputBase, OutputHandle, Shared};
use crate::ivm::push_accumulated::push_accumulated_changes;
use crate::ivm::schema::SourceSchema;

/// Port of TS `FanIn` (fan-in.ts:29).
pub struct FanIn {
    /// Branch tails feeding this fan-in (TS `#inputs`). destroy() forwards to
    /// every branch so the cascade reaches the ref-counted FanOut and,
    /// through it, the source input.
    inputs: Vec<FilterInputHandle>,
    schema: SourceSchema,
    output: Rc<RefCell<Option<FilterOutputHandle>>>,
    accumulated_pushes: RefCell<Vec<Change>>,
}

impl FanIn {
    /// TS constructor `(fanOut, inputs)`: schema from the fan-out; each
    /// branch's filter output is wired to this fan-in.
    pub fn new(fan_out_schema: SourceSchema, inputs: Vec<FilterInputHandle>) -> Shared<FanIn> {
        let fan_in = Rc::new(RefCell::new(FanIn {
            inputs: inputs.clone(),
            schema: fan_out_schema.clone(),
            output: Rc::new(RefCell::new(None)),
            accumulated_pushes: RefCell::new(Vec::new()),
        }));
        // TS asserts `this.#schema === input.getSchema()` (object identity);
        // rust SourceSchema handles are value clones, so identity does not
        // map — branches are built from the same fan-out, which guarantees it.
        for input in &inputs {
            let as_output: FilterOutputHandle = fan_in.clone();
            input.borrow().set_filter_output(as_output);
        }
        fan_in
    }

    /// Port of TS `fanOutDonePushingToAllBranches` (fan-in.ts:76). `&self`:
    /// the collapse pushes downstream, which may re-enter this cell.
    pub fn fan_out_done_pushing_to_all_branches(&self, fan_out_change_type: ChangeType) {
        if self.inputs.is_empty() {
            assert!(
                self.accumulated_pushes.borrow().is_empty(),
                "If there are no inputs then fan-in should not receive any pushes."
            );
            return;
        }
        let output = self.output.borrow().clone();
        // Take the batch out before pushing downstream (drops the borrow so a
        // re-entrant accumulate cannot collide).
        let mut drained = std::mem::take(&mut *self.accumulated_pushes.borrow_mut());
        if let Some(output) = output {
            let out: OutputHandle = Rc::new(RefCell::new(FilterOutputAsOutput(output)));
            let pusher = FilterChainPusher {
                schema: self.schema.clone(),
            };
            push_accumulated_changes(
                &mut drained,
                &out,
                &pusher,
                fan_out_change_type,
                &self.schema,
            );
        }
    }
}

impl InputBase for FanIn {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        for input in &self.inputs {
            input.borrow_mut().destroy();
        }
        // Rust-only: break the Rc cycles on teardown.
        self.inputs.clear();
        *self.output.borrow_mut() = None;
    }
}

impl FilterInput for FanIn {
    fn set_filter_output(&self, output: FilterOutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }
}

impl FilterOutput for FanIn {
    fn begin_filter(&self) {
        if let Some(output) = self.output.borrow().clone() {
            output.borrow().begin_filter();
        }
    }

    fn end_filter(&self) {
        if let Some(output) = self.output.borrow().clone() {
            output.borrow().end_filter();
        }
    }

    /// TS: delegates straight downstream (fan-in.ts:67).
    fn filter(&self, node: &Node) -> bool {
        let output = self.output.borrow().clone().expect("FanIn: output not set");
        output.borrow().filter(node)
    }

    /// TS: accumulate; the fan-out's done-signal collapses (fan-in.ts:71).
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        self.accumulated_pushes.borrow_mut().push(change);
    }
}
