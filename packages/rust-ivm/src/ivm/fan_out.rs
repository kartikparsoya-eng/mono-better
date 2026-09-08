//! FanOut operator — port of `zql/src/ivm/fan-out.ts`.
//!
//! Forks the filter sub-graph into multiple branches; paired with a `FanIn`
//! that merges the forks back together. A `FilterOperator`: `filter(node)`
//! ORs the branches (short-circuiting on the first accept); `push` pushes to
//! every branch and then tells the FanIn the fan-out is done so it can
//! collapse the accumulated branch pushes.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::change::Change;
use crate::ivm::data::Node;
use crate::ivm::fan_in::FanIn;
use crate::ivm::filter_operators::{
    FilterChainPusher, FilterInput, FilterInputHandle, FilterOutput, FilterOutputHandle,
    FilterResult,
};
use crate::ivm::operator::{InputBase, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::StreamItem;

/// Port of TS `FanOut` (fan-out.ts:17).
pub struct FanOut {
    input: FilterInputHandle,
    /// Interior-mutable: TS `setFilterOutput` APPENDS (each branch registers
    /// itself through the shared handle).
    outputs: RefCell<Vec<FilterOutputHandle>>,
    fan_in: Option<Shared<FanIn>>,
    destroy_count: usize,
    schema: SourceSchema,
}

impl FanOut {
    pub fn new(input: FilterInputHandle) -> Shared<FanOut> {
        let schema = input.borrow().get_schema();
        let fan_out = Rc::new(RefCell::new(FanOut {
            input: input.clone(),
            outputs: RefCell::new(Vec::new()),
            fan_in: None,
            destroy_count: 0,
            schema,
        }));
        // TS: `input.setFilterOutput(this)`.
        let as_output: FilterOutputHandle = fan_out.clone();
        input.borrow().set_filter_output(as_output);
        fan_out
    }

    pub fn set_fan_in(&mut self, fan_in: Shared<FanIn>) {
        self.fan_in = Some(fan_in);
    }
}

impl InputBase for FanOut {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    /// TS: ref-counted — the upstream input is destroyed only when EVERY
    /// branch has destroyed its edge into this fan-out (fan-out.ts:36-45).
    fn destroy(&mut self) {
        let n = self.outputs.borrow().len();
        if self.destroy_count < n {
            self.destroy_count += 1;
            if self.destroy_count == n {
                self.input.borrow_mut().destroy();
                // Rust-only: break the Rc cycles on final teardown.
                self.outputs.borrow_mut().clear();
                self.fan_in = None;
            }
        } else {
            panic!("FanOut already destroyed once for each output");
        }
    }
}

impl FilterInput for FanOut {
    /// TS `setFilterOutput` APPENDS — each branch registers itself
    /// (fan-out.ts:32).
    fn set_filter_output(&self, output: FilterOutputHandle) {
        self.outputs.borrow_mut().push(output);
    }
}

impl FilterOutput for FanOut {
    fn begin_filter(&self) {
        for output in self.outputs.borrow().iter() {
            output.borrow().begin_filter();
        }
    }

    fn end_filter(&self) {
        for output in self.outputs.borrow().iter() {
            output.borrow().end_filter();
        }
    }

    /// TS: OR over branches, short-circuiting on the first accept
    /// (fan-out.ts:62-71).
    fn filter(&self, node: &Node) -> FilterResult {
        Box::new(FanOutFilter {
            outputs: self.outputs.borrow().clone(),
            node: node.clone(),
            idx: 0,
            current: None,
            done: false,
        })
    }

    /// TS: push to every branch, then signal the fan-in (fan-out.ts:73-80).
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        let pusher = FilterChainPusher {
            schema: self.schema.clone(),
        };
        for out in self.outputs.borrow().iter() {
            out.borrow().push(change.clone(), &pusher);
        }
        let fan_in = self
            .fan_in
            .clone()
            .expect("fan-out must have a corresponding fan-in set!");
        fan_in
            .borrow()
            .fan_out_done_pushing_to_all_branches(change.change_type());
    }
}

/// The generator body of TS `FanOut.filter` (fan-out.ts:63-71): `result =
/// (yield* output.filter(node)) || result; if (result) return true` over the
/// branches — each branch's yields are forwarded, the first accepting branch
/// short-circuits.
struct FanOutFilter {
    outputs: Vec<FilterOutputHandle>,
    node: Node,
    idx: usize,
    current: Option<FilterResult>,
    done: bool,
}

impl Iterator for FanOutFilter {
    type Item = StreamItem<bool>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }
            if let Some(current) = self.current.as_mut() {
                match current.next() {
                    Some(StreamItem::Yield) => return Some(StreamItem::Yield),
                    Some(StreamItem::Data(true)) => {
                        self.done = true;
                        return Some(StreamItem::Data(true));
                    }
                    Some(StreamItem::Data(false)) => {
                        self.current = None;
                        self.idx += 1;
                        continue;
                    }
                    None => panic!("filter generator ended without a result"),
                }
            }
            if self.idx < self.outputs.len() {
                let result = self.outputs[self.idx].borrow().filter(&self.node);
                self.current = Some(result);
                continue;
            }
            self.done = true;
            return Some(StreamItem::Data(false));
        }
    }
}
