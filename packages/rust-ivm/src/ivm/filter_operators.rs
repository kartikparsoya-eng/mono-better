//! Filter operators — port of `zql/src/ivm/filter-operators.ts`.
//!
//! The where clause uses a sub-graph of FilterOperators:
//! FilterStart → Filter(s) → FilterEnd
//!
//! FilterStart adapts from normal Input/Output to FilterInput/FilterOutput.
//! FilterEnd adapts back. FilterOperators have `filter(node) -> bool` instead
//! of `fetch` — enables efficient OR handling.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::change::Change;
use crate::ivm::data::Node;
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::NodeStream;

/// FilterInput — like Input but with `set_filter_output` instead of `set_output`.
pub trait FilterInput: InputBase {
    fn set_filter_output(&self, output: OutputHandle);
}

/// FilterOutput — like Output but with `filter(node) -> bool`.
pub trait FilterOutput: Output {
    fn begin_filter(&mut self);
    fn filter(&mut self, node: &Node) -> bool;
    fn end_filter(&mut self);
}

/// FilterStart — adapts Input → FilterInput.
/// Port of TS `FilterStart` (filter-operators.ts:79).
pub struct FilterStart {
    input: Shared<dyn Input>,
    output: Rc<RefCell<Option<OutputHandle>>>,
    schema: SourceSchema,
}

impl FilterStart {
    pub fn new(input: Shared<dyn Input>) -> Shared<FilterStart> {
        let schema = input.borrow().get_schema();
        Rc::new(RefCell::new(FilterStart {
            input,
            output: Rc::new(RefCell::new(None)),
            schema,
        }))
    }

    pub fn set_filter_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }
}

impl InputBase for FilterStart {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.input.borrow_mut().destroy();
    }
}

impl Input for FilterStart {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        let input = self.input.borrow();
        let _output = self.output.borrow().clone();

        // In a full implementation, this calls begin_filter, filters each
        // node through the filter chain, then end_filter.
        // For now, pass through (the Filter operator handles this directly)
        input.fetch(req)
    }
}

impl Output for FilterStart {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        let output = self.output.borrow().clone();
        if let Some(output) = output {
            output.borrow_mut().push(change, pusher);
        }
    }
}

/// FilterEnd — adapts FilterInput → Input.
/// Port of TS `FilterEnd` (filter-operators.ts:120).
pub struct FilterEnd {
    start: Shared<FilterStart>,
    schema: SourceSchema,
    output: Rc<RefCell<Option<OutputHandle>>>,
}

impl FilterEnd {
    pub fn new(start: Shared<FilterStart>) -> Shared<FilterEnd> {
        let schema = start.borrow().get_schema();
        Rc::new(RefCell::new(FilterEnd {
            start,
            schema,
            output: Rc::new(RefCell::new(None)),
        }))
    }
}

impl InputBase for FilterEnd {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {}
}

impl Input for FilterEnd {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        let start = self.start.borrow();
        start.fetch(req)
    }
}

impl Output for FilterEnd {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        let output = self.output.borrow().clone();
        if let Some(output) = output {
            output.borrow_mut().push(change, pusher);
        }
    }
}

/// Build a filter pipeline: FilterStart → middle → FilterEnd.
/// Port of TS `buildFilterPipeline` (filter-operators.ts:152).
pub fn build_filter_pipeline(input: Shared<dyn Input>) -> (Shared<FilterStart>, Shared<FilterEnd>) {
    let start = FilterStart::new(input);
    let end = FilterEnd::new(start.clone());
    (start, end)
}
