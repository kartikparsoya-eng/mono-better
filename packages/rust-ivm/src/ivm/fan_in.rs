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
}

impl FanIn {
    pub fn new(schema: SourceSchema) -> Shared<FanIn> {
        Rc::new(RefCell::new(FanIn {
            schema,
            accumulated_pushes: Vec::new(),
            output: Rc::new(RefCell::new(None)),
        }))
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

    fn destroy(&mut self) {}
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
