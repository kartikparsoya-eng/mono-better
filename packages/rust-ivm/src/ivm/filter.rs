//! Filter operator — port of `zql/src/ivm/filter.ts`.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::ivm::change::Change;
use crate::ivm::data::Row;
use crate::ivm::filter_push::filter_push;
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::SourceSchema;

/// Port of TS `Filter` — stateless predicate filter.
pub struct Filter {
    input: Shared<dyn Input>,
    predicate: Arc<dyn Fn(&Row) -> bool>,
    output: Rc<RefCell<Option<OutputHandle>>>,
}

impl Filter {
    pub fn new(input: Shared<dyn Input>, predicate: Arc<dyn Fn(&Row) -> bool>) -> Shared<Filter> {
        let filter = Rc::new(RefCell::new(Filter {
            input: input.clone(),
            predicate,
            output: Rc::new(RefCell::new(None)),
        }));

        // Wire the source's output to this filter, matching TS where the
        // FilterStart constructor calls `input.setOutput(this)`.
        let filter_clone = filter.clone();
        input
            .borrow()
            .set_output(Rc::new(RefCell::new(FilterOutputAdapter {
                filter: filter_clone,
            })));

        filter
    }
}

// Implement Input/InputBase on the Shared<Filter> pattern via wrapper methods.
// Since Shared<T> = Rc<RefCell<T>>, we implement on Filter and borrow through.

impl InputBase for Filter {
    fn get_schema(&self) -> SourceSchema {
        self.input.borrow().get_schema()
    }

    fn destroy(&mut self) {
        self.input.borrow_mut().destroy();
        // Break the Rc cycle: clear the back-edge to the downstream output.
        *self.output.borrow_mut() = None;
    }
}

impl Input for Filter {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> crate::ivm::stream::NodeStream {
        let stream = self.input.borrow().fetch(req);
        let predicate = self.predicate.clone();
        Box::new(stream.filter_map(move |item| match item {
            crate::ivm::stream::StreamItem::Data(n) if predicate(&n.row) => {
                Some(crate::ivm::stream::StreamItem::Data(n))
            }
            crate::ivm::stream::StreamItem::Yield => Some(crate::ivm::stream::StreamItem::Yield),
            _ => None,
        }))
    }
}

impl Filter {
    /// Forward a change through the predicate. Takes `&self` (Filter is
    /// stateless: `output` and `predicate` are interior-shared) so that a
    /// nested re-fetch that re-enters this Filter's `fetch` during the
    /// downstream push does not collide with a mutable borrow.
    fn push_change(&self, change: Change, pusher: &dyn InputBase) {
        let output = self.output.borrow().clone();
        if let Some(output) = output {
            filter_push(change, output, pusher, Some(&self.predicate));
        }
    }
}

impl Output for Filter {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        self.push_change(change, pusher);
    }
}

/// Adapter that forwards Output::push to the Filter, matching TS where
/// FilterStart::push calls the Filter's push through the FilterOutput protocol.
struct FilterOutputAdapter {
    filter: Shared<Filter>,
}

impl Output for FilterOutputAdapter {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        // Immutable borrow: see Filter::push_change.
        self.filter.borrow().push_change(change, pusher);
    }
}
