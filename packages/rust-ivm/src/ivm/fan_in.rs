//! FanIn operator — port of `zql/src/ivm/fan-in.ts`.
//!
//! Merges multiple streams into one, eliminating duplicates.
//! Paired with FanOut. Accumulates pushes from branches, then collapses
//! them via `push_accumulated_changes`.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::change::{Change, ChangeType};
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::push_accumulated::push_accumulated_changes;
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::{NodeStream, from_vec};

/// Port of TS `FanIn` (fan-in.ts:24).
pub struct FanIn {
    schema: SourceSchema,
    accumulated_pushes: Vec<Change>,
    output: Rc<RefCell<Option<OutputHandle>>>,
    /// Branch tails feeding this fan-in (TS `FanIn#inputs`). destroy() must
    /// forward to every branch so the cascade reaches the ref-counted FanOut
    /// and, through it, the source input — otherwise a removed query's whole
    /// OR-subtree stays anchored by its source connection (the
    /// connection-splice leak class).
    inputs: Vec<Shared<dyn Input>>,
}

impl FanIn {
    pub fn new(schema: SourceSchema) -> Shared<FanIn> {
        Rc::new(RefCell::new(FanIn {
            schema,
            accumulated_pushes: Vec::new(),
            output: Rc::new(RefCell::new(None)),
            inputs: Vec::new(),
        }))
    }

    /// Register a branch tail (TS receives these in the FanIn constructor).
    pub fn add_input(&mut self, input: Shared<dyn Input>) {
        self.inputs.push(input);
    }

    pub fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    /// Called by FanOut after all branches have been pushed.
    /// Triggers `push_accumulated_changes`.
    pub fn fan_out_done_pushing(
        &mut self,
        fan_out_change_type: ChangeType,
        pusher: &dyn InputBase,
    ) {
        let output = self.output.borrow().clone();
        if let Some(output) = output {
            push_accumulated_changes(
                &mut self.accumulated_pushes,
                &output,
                pusher,
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
        // TS parity (fan-in.ts:49): destroy every branch input so the cascade
        // reaches the ref-counted FanOut and the source input below it.
        for input in &self.inputs {
            input.borrow_mut().destroy();
        }
        self.inputs.clear();
        // Break the Rc cycle: clear the back-edge to the downstream output.
        *self.output.borrow_mut() = None;
    }
}

impl Input for FanIn {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, _req: &FetchRequest) -> NodeStream {
        // FanIn doesn't fetch — it's a merge point for pushes
        from_vec(Vec::new())
    }
}

impl Output for FanIn {
    fn push(&mut self, change: Change, _pusher: &dyn InputBase) {
        crate::ivm::trace::recv("fan_in#1", &change);
        // Accumulate — will be collapsed when fan_out_done_pushing is called
        self.accumulated_pushes.push(change);
    }
}
