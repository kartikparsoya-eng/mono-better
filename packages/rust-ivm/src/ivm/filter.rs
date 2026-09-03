//! Filter operator — port of `zql/src/ivm/filter.ts`.
//!
//! A stateless predicate filter, participating in the filter sub-graph
//! protocol (`FilterOperator`): `filter(node) -> bool` instead of `fetch`.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::ivm::change::Change;
use crate::ivm::data::{Node, Row};
use crate::ivm::filter_operators::{
    FilterInput, FilterInputHandle, FilterOutput, FilterOutputHandle, FilterResult, filter_result,
};
use crate::ivm::filter_push::filter_push;
use crate::ivm::operator::{InputBase, Shared};
use crate::ivm::schema::SourceSchema;

/// Port of TS `Filter` (filter.ts:18) — stateless predicate FilterOperator.
pub struct Filter {
    input: FilterInputHandle,
    predicate: Arc<dyn Fn(&Row) -> bool>,
    output: Rc<RefCell<Option<FilterOutputHandle>>>,
    schema: SourceSchema,
}

impl Filter {
    pub fn new(input: FilterInputHandle, predicate: Arc<dyn Fn(&Row) -> bool>) -> Shared<Filter> {
        let schema = input.borrow().get_schema();
        let filter = Rc::new(RefCell::new(Filter {
            input: input.clone(),
            predicate,
            output: Rc::new(RefCell::new(None)),
            schema,
        }));
        // TS: `input.setFilterOutput(this)`.
        let as_output: FilterOutputHandle = filter.clone();
        input.borrow().set_filter_output(as_output);
        filter
    }
}

impl InputBase for Filter {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.input.borrow_mut().destroy();
        // Rust-only: break the Rc back-edge cycle on teardown.
        *self.output.borrow_mut() = None;
    }
}

impl FilterInput for Filter {
    fn set_filter_output(&self, output: FilterOutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }
}

impl FilterOutput for Filter {
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

    /// TS: `this.#predicate(node.row) && (yield* this.#output.filter(node))`.
    fn filter(&self, node: &Node) -> FilterResult {
        if !(self.predicate)(&node.row) {
            return filter_result(false);
        }
        let output = self
            .output
            .borrow()
            .clone()
            .expect("Filter: output not set");
        output.borrow().filter(node)
    }

    /// TS: `filterPush(change, this.#output, this, this.#predicate)`.
    fn push(&self, change: Change, _pusher: &dyn InputBase) {
        let output = self.output.borrow().clone();
        if let Some(output) = output {
            let predicate = self.predicate.clone();
            let schema = self.schema.clone();
            filter_push(
                change,
                &mut |c| {
                    let pusher = crate::ivm::filter_operators::FilterChainPusher {
                        schema: schema.clone(),
                    };
                    output.borrow().push(c, &pusher);
                },
                Some(&predicate),
            );
        }
    }
}
