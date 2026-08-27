//! Filter operators — port of `zql/src/ivm/filter-operators.ts`.
//!
//! The `where` clause of a ZQL query is implemented using a sub-graph of
//! `FilterOperator`s. The sub-graph starts with a `FilterStart` operator that
//! adapts from the normal `Output` to the `FilterInput` protocol, and ends
//! with a `FilterEnd` that adapts a `FilterOutput` back to a normal `Input`.
//! `FilterOperator`s do not have `fetch`; they have `filter(node) -> bool`.
//! Not having `fetch` means they cannot modify node rows/relationships — they
//! just filter. This enables single-fetch processing of `where` clauses with
//! OR conditions (see rocicorp/mono#4339).
//!
//! Rust deltas from TS (established crate idiom, no behavior change):
//! `push` is void (no coop `'yield'`), handles are `Rc<RefCell<_>>`, and the
//! TS `finally { endFilter() }` around a partially-consumed fetch stream is a
//! `Drop` guard on the stream (same guarantee: endFilter runs exactly once
//! even when the consumer stops early or unwinds).

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::change::Change;
use crate::ivm::data::Node;
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::{NodeStream, StreamItem};

/// Port of TS `FilterInput` (filter-operators.ts:27).
pub trait FilterInput: InputBase {
    /// Tell the input where to send its output.
    fn set_filter_output(&self, output: FilterOutputHandle);
}

/// Port of TS `FilterOutput` (filter-operators.ts:32). Includes `push` (TS
/// `FilterOutput extends Output`).
/// All methods take `&self` (interior mutability): a downstream operator
/// (e.g. Take) may re-FETCH through this same chain while a push/filter is in
/// flight — TS generators interleave freely, so `&mut` borrows would collide
/// exactly where TS is re-entrant.
pub trait FilterOutput {
    /// We're entering a loop of filtering nodes (e.g. cache for its duration).
    fn begin_filter(&self);
    fn filter(&self, node: &Node) -> bool;
    fn end_filter(&self);
    fn push(&self, change: Change, pusher: &dyn InputBase);
}

pub type FilterInputHandle = Rc<RefCell<dyn FilterInput>>;
pub type FilterOutputHandle = Rc<RefCell<dyn FilterOutput>>;

/// `FilterStart` — adapts a normal `Input` into the filter sub-graph.
/// Port of TS `FilterStart` (filter-operators.ts:61).
pub struct FilterStart {
    input: Shared<dyn Input>,
    output: Rc<RefCell<Option<FilterOutputHandle>>>,
    schema: SourceSchema,
}

impl FilterStart {
    pub fn new(input: Shared<dyn Input>) -> Shared<FilterStart> {
        let schema = input.borrow().get_schema();
        let start = Rc::new(RefCell::new(FilterStart {
            input: input.clone(),
            output: Rc::new(RefCell::new(None)),
            schema,
        }));
        // TS: `input.setOutput(this)`.
        let start_clone = start.clone();
        input
            .borrow()
            .set_output(Rc::new(RefCell::new(FilterStartOutput {
                start: start_clone,
            })));
        start
    }
}

impl InputBase for FilterStart {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.input.borrow_mut().destroy();
        // Rust-only: break the Rc back-edge cycle on teardown.
        *self.output.borrow_mut() = None;
    }
}

impl FilterInput for FilterStart {
    fn set_filter_output(&self, output: FilterOutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }
}

impl FilterStart {
    /// Port of TS `FilterStart.fetch` (filter-operators.ts:86): beginFilter,
    /// stream the input, keep nodes the filter chain accepts, endFilter in a
    /// `finally` (here: exactly-once via the stream's `Drop`).
    pub fn fetch(&self, req: &FetchRequest) -> NodeStream {
        let output = self
            .output
            .borrow()
            .clone()
            .expect("FilterStart: output not set");
        output.borrow().begin_filter();
        let inner = self.input.borrow().fetch(req);
        Box::new(FilterStartStream {
            inner,
            output,
            ended: false,
        })
    }

    /// Port of TS `FilterStart.push` (filter-operators.ts:82).
    pub fn push(&self, change: Change, _pusher: &dyn InputBase) {
        let output = self.output.borrow().clone();
        if let Some(output) = output {
            let self_base = FilterChainPusher {
                schema: self.schema.clone(),
            };
            output.borrow().push(change, &self_base);
        }
    }
}

/// The pusher identity a filter operator presents downstream (TS passes
/// `this`). Downstream operators only use the pusher for schema/trace
/// identity, so a light stand-in avoids re-borrowing the operator cell.
pub struct FilterChainPusher {
    pub schema: SourceSchema,
}
impl InputBase for FilterChainPusher {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }
    fn destroy(&mut self) {}
}

/// Output adapter registered on the upstream input (TS: FilterStart IS the
/// output; rust needs a distinct cell to avoid a self-borrow).
struct FilterStartOutput {
    start: Shared<FilterStart>,
}
impl Output for FilterStartOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        let start = self.start.borrow();
        start.push(change, pusher);
    }
}

/// The `finally { endFilter() }` stream: filters inner nodes through the
/// filter chain; guarantees `end_filter` runs exactly once — at natural end
/// OR at drop of a partially-consumed stream (TS filter-operators.ts:98-102).
struct FilterStartStream {
    inner: NodeStream,
    output: FilterOutputHandle,
    ended: bool,
}

impl Iterator for FilterStartStream {
    type Item = StreamItem<Node>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.ended {
            return None;
        }
        loop {
            match self.inner.next() {
                None => {
                    self.ended = true;
                    self.output.borrow().end_filter();
                    return None;
                }
                Some(StreamItem::Yield) => return Some(StreamItem::Yield),
                Some(StreamItem::Data(node)) => {
                    if self.output.borrow().filter(&node) {
                        return Some(StreamItem::Data(node));
                    }
                }
            }
        }
    }
}

impl Drop for FilterStartStream {
    fn drop(&mut self) {
        if !self.ended {
            self.ended = true;
            self.output.borrow().end_filter();
        }
    }
}

/// `FilterEnd` — adapts the filter sub-graph back to a normal `Input`.
/// Port of TS `FilterEnd` (filter-operators.ts:106).
pub struct FilterEnd {
    start: Shared<FilterStart>,
    /// The last filter operator in the sub-graph (TS `#input`).
    input: FilterInputHandle,
    output: Rc<RefCell<Option<OutputHandle>>>,
    schema: SourceSchema,
}

impl FilterEnd {
    pub fn new(start: Shared<FilterStart>, input: FilterInputHandle) -> Shared<FilterEnd> {
        let schema = input.borrow().get_schema();
        let end = Rc::new(RefCell::new(FilterEnd {
            start,
            input: input.clone(),
            output: Rc::new(RefCell::new(None)),
            schema,
        }));
        // TS: `input.setFilterOutput(this)`.
        let end_clone = end.clone();
        input
            .borrow()
            .set_filter_output(Rc::new(RefCell::new(FilterEndAsFilterOutput {
                end: end_clone,
            })));
        end
    }
}

impl InputBase for FilterEnd {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        // TS destroys `#input` (the last filter op) — the cascade walks the
        // filter chain down to FilterStart and the source below it.
        self.input.borrow_mut().destroy();
        *self.output.borrow_mut() = None;
    }
}

impl Input for FilterEnd {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        self.start.borrow().fetch(req)
    }
}

/// FilterEnd viewed from the filter chain above it (TS: FilterEnd implements
/// FilterOutput directly): terminal `filter` accepts everything; `push`
/// forwards to the normal downstream output.
struct FilterEndAsFilterOutput {
    end: Shared<FilterEnd>,
}
impl FilterOutput for FilterEndAsFilterOutput {
    fn begin_filter(&self) {}
    fn end_filter(&self) {}
    fn filter(&self, _node: &Node) -> bool {
        true
    }
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        let end = self.end.borrow();
        let output = end.output.borrow().clone();
        if let Some(output) = output {
            let pusher = FilterChainPusher {
                schema: end.schema.clone(),
            };
            drop(end);
            output.borrow_mut().push(change, &pusher);
        }
    }
}

/// Port of TS `buildFilterPipeline` (filter-operators.ts:148): wrap `input`
/// in FilterStart, let `pipeline` build the filter chain, close with
/// FilterEnd. (TS `delegate.addEdge` calls are debug instrumentation — not
/// ported, per the established ledger alias.)
pub fn build_filter_pipeline(
    input: Shared<dyn Input>,
    pipeline: impl FnOnce(FilterInputHandle) -> FilterInputHandle,
) -> Shared<dyn Input> {
    let start = FilterStart::new(input);
    let middle = pipeline(start.clone() as FilterInputHandle);
    FilterEnd::new(start, middle)
}

/// Adapt a `FilterOutputHandle` to a plain `OutputHandle` (TS needs no
/// adapter — `FilterOutput extends Output`). Lets filter operators reuse
/// helpers that take an `OutputHandle` (e.g. `push_accumulated_changes`).
pub struct FilterOutputAsOutput(pub FilterOutputHandle);
impl Output for FilterOutputAsOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        self.0.borrow().push(change, pusher);
    }
}
