//! UnionFanOut operator — port of `zql/src/ivm/union-fan-out.ts`.
//!
//! Forks a stream into multiple branches for OR with flipped subqueries.
//! Similar to FanOut but for union semantics. Push calls fanOutStartedPushing
//! on the UnionFanIn, pushes to all branches, then calls fanOutDonePushing.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::change::Change;
use crate::ivm::operator::{Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::NodeStream;

pub struct UnionFanOut {
    input: Shared<dyn Input>,
    schema: SourceSchema,
    outputs: Rc<RefCell<Vec<OutputHandle>>>,
    fan_in: Rc<RefCell<Option<Shared<crate::ivm::union_fan_in::UnionFanIn>>>>,
    destroy_count: usize,
}

impl UnionFanOut {
    pub fn new(input: Shared<dyn Input>) -> Shared<UnionFanOut> {
        crate::live_count::inc(&crate::live_count::UNION_FAN_OUT);
        let schema = input.borrow().get_schema();
        let ufo = Rc::new(RefCell::new(UnionFanOut {
            input: input.clone(),
            schema,
            outputs: Rc::new(RefCell::new(Vec::new())),
            fan_in: Rc::new(RefCell::new(None)),
            destroy_count: 0,
        }));
        // Wire a UfoOutput adapter as the upstream's output so pushes flow into
        // us. The adapter (not ufo itself) is borrow_mut'd during a push, so
        // UnionFanOut's RefCell is only immutably borrowed — a re-entrant fetch
        // from downstream (Take via ufi→Filter) takes an immutable borrow that
        // succeeds alongside the live one. Re-entrancy fix for the flipped push
        // path; same pattern as CapOutput/ExistsOutput/UfiOutput.
        input
            .borrow()
            .set_output(Rc::new(RefCell::new(UfoOutput { ufo: ufo.clone() })));
        ufo
    }

    pub fn set_fan_in(&self, fan_in: Shared<crate::ivm::union_fan_in::UnionFanIn>) {
        *self.fan_in.borrow_mut() = Some(fan_in);
    }
}

impl InputBase for UnionFanOut {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        let outputs_len = self.outputs.borrow().len();
        if self.destroy_count < outputs_len {
            self.destroy_count += 1;
            if self.destroy_count == outputs_len {
                self.input.borrow_mut().destroy();
                // Break the Rc cycle: drop the back-edges to downstream outputs
                // and the strong ref to the reconvergence UnionFanIn.
                self.outputs.borrow_mut().clear();
                *self.fan_in.borrow_mut() = None;
            }
        }
    }
}

impl Input for UnionFanOut {
    fn set_output(&self, output: OutputHandle) {
        // Port of TS `UnionFanOut.setOutput` — append the branch to `#outputs`.
        // Each OR branch is built with this UnionFanOut as its input, so the
        // branch's constructor calls `input.set_output(branch)`, registering
        // itself here. `push` then fans out to every registered branch.
        self.outputs.borrow_mut().push(output);
    }

    fn fetch(&self, req: &crate::ivm::operator::FetchRequest) -> NodeStream {
        self.input.borrow().fetch(req)
    }
}

impl Output for UnionFanOut {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {
        // Pushes arrive via the UfoOutput adapter (re-entrancy fix).
    }
}

/// Output adapter that receives pushes from the upstream and delegates to
/// UnionFanIn via an immutable borrow. See `UfiOutput` / `CapOutput` for the
/// rationale: the adapter's RefCell is borrow_mut'd during a push, UnionFanOut
/// itself is only immutably borrowed, so a re-entrant fetch succeeds.
struct UfoOutput {
    ufo: Shared<UnionFanOut>,
}

impl UnionFanOut {
    /// Push logic, run via an immutable borrow (from `UfoOutput`). `outputs`
    /// and `fan_in` are interior-mutable, so `&self` suffices.
    fn push_internal(&self, change: Change, _pusher: &dyn InputBase) {
        crate::ivm::trace::recv("union_fan_out#1", &change);
        let change_type = change.change_type();
        // TS union-fan-out.ts uses `must(this.#unionFanIn)` for BOTH the
        // started- and done-pushing signals — a union-fan-out without its
        // union-fan-in is a graph-construction invariant violation, not a
        // silently-skippable state. Panic (contained per-CG), matching TS.
        let fan_in = self
            .fan_in
            .borrow()
            .clone()
            .expect("union-fan-out must have a corresponding union-fan-in set!");
        fan_in.borrow().fan_out_started_pushing();
        let outputs: Vec<OutputHandle> = self.outputs.borrow().clone();
        for output in &outputs {
            output.borrow_mut().push(change.clone(), self);
        }
        fan_in.borrow().fan_out_done_pushing(change_type, self);
    }
}

impl Output for UfoOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        self.ufo.borrow().push_internal(change, pusher);
    }
}

impl Drop for UnionFanOut {
    fn drop(&mut self) {
        crate::live_count::dec(&crate::live_count::UNION_FAN_OUT);
    }
}
