//! FanOut operator — port of `zql/src/ivm/fan-out.ts`.
//!
//! Duplicates incoming changes to multiple branches. Paired with FanIn
//! which merges and deduplicates. After pushing to all branches, calls
//! `FanIn::fan_out_done_pushing` to collapse accumulated changes.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::ivm::change::Change;
use crate::ivm::fan_in::FanIn;
use crate::ivm::operator::{Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::NodeStream;

pub struct FanOut {
    input: Shared<dyn Input>,
    outputs: Rc<RefCell<Vec<OutputHandle>>>,
    fan_in: Rc<RefCell<Option<Shared<FanIn>>>>,
    destroy_count: usize,
}

impl FanOut {
    pub fn new(input: Shared<dyn Input>) -> Shared<FanOut> {
        Rc::new(RefCell::new(FanOut {
            input,
            outputs: Rc::new(RefCell::new(Vec::new())),
            fan_in: Rc::new(RefCell::new(None)),
            destroy_count: 0,
        }))
    }

    pub fn set_fan_in(&self, fan_in: Shared<FanIn>) {
        *self.fan_in.borrow_mut() = Some(fan_in);
    }
}

impl InputBase for FanOut {
    fn get_schema(&self) -> SourceSchema {
        self.input.borrow().get_schema()
    }

    fn destroy(&mut self) {
        let outputs_len = self.outputs.borrow().len();
        if self.destroy_count < outputs_len {
            self.destroy_count += 1;
            if self.destroy_count == outputs_len {
                self.input.borrow_mut().destroy();
            }
        }
    }
}

impl Input for FanOut {
    fn set_output(&self, output: OutputHandle) {
        self.outputs.borrow_mut().push(output);
    }

    fn fetch(&self, req: &crate::ivm::operator::FetchRequest) -> NodeStream {
        self.input.borrow().fetch(req)
    }
}

impl Output for FanOut {
    fn push(&mut self, change: Change, _pusher: &dyn InputBase) {
        crate::ivm::trace::recv("fan_out#1", &change);
        let change_type = change.change_type();
        let outputs: Vec<OutputHandle> = self.outputs.borrow().clone();
        for output in &outputs {
            output.borrow_mut().push(change.clone(), self);
        }
        let fan_in = self.fan_in.borrow().clone();
        if let Some(ref fan_in) = fan_in {
            fan_in.borrow_mut().fan_out_done_pushing(change_type, self);
        }
    }
}
