//! ArrayView — port of `zql/src/ivm/array-view.ts`.
//!
//! Implements a materialized view of the output of an operator.
//! Collects fetch results and applies changes immutably via `apply_change`.
//! Notifies listeners on flush.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::ivm::change::Change;
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::NodeStream;
use crate::ivm::view::{apply_change, change_to_view_change, empty_root_entry, Entry, Format, View, ViewChange};

/// A listener callback for view updates.
pub type Listener = Rc<dyn Fn(&View)>;

/// The materialized view state.
pub struct ArrayView {
    input: Shared<dyn Input>,
    schema: SourceSchema,
    format: Format,
    root: Entry,
    listeners: Vec<Listener>,
    dirty: bool,
}

impl ArrayView {
    pub fn new(input: Shared<dyn Input>, format: Format) -> Rc<RefCell<ArrayView>> {
        let schema = input.borrow().get_schema();

        let av = Rc::new(RefCell::new(ArrayView {
            input: input.clone(),
            schema,
            format,
            root: empty_root_entry(),
            listeners: Vec::new(),
            dirty: false,
        }));

        let av_clone = av.clone();
        input.borrow_mut().set_output(Rc::new(RefCell::new(ArrayViewOutput {
            av: av_clone,
        })));

        // Hydrate
        {
            let mut av_borrowed = av.borrow_mut();
            av_borrowed.hydrate();
        }

        av
    }

    /// Get the current view data.
    pub fn data(&self) -> Option<&View> {
        self.root.relationships.get("")
    }

    /// Add a listener that will be called on flush.
    pub fn add_listener(&mut self, listener: Listener) {
        // Fire immediately with current data
        if let Some(data) = self.data() {
            listener(data);
        }
        self.listeners.push(listener);
    }

    /// Flush dirty state and notify listeners.
    pub fn flush(&mut self) {
        if !self.dirty {
            return;
        }
        self.dirty = false;
        if let Some(data) = self.data() {
            for listener in &self.listeners {
                (listener)(data);
            }
        }
    }

    /// Destroy the view and its input.
    pub fn destroy(&self) {
        self.input.borrow_mut().destroy();
    }

    fn hydrate(&mut self) {
        self.dirty = true;
        let req = FetchRequest::default();
        for node in crate::ivm::stream::skip_yields(self.input.borrow().fetch(&req)) {
            let change = ViewChange::Add {
                node: crate::ivm::view::ViewNode::Lazy(node),
            };
            self.root = apply_change(
                &self.root,
                &change,
                &self.schema,
                "",
                &self.format,
                false,
                true, // mutate — fresh root, not yet observed
            );
        }
        self.flush();
    }
}

struct ArrayViewOutput {
    av: Rc<RefCell<ArrayView>>,
}

impl Output for ArrayViewOutput {
    fn push(&mut self, change: Change, _pusher: &dyn InputBase) {
        let mut av = self.av.borrow_mut();
        av.dirty = true;
        let view_change = change_to_view_change(&change);
        av.root = apply_change(
            &av.root,
            &view_change,
            &av.schema,
            "",
            &av.format,
            false,
            false, // immutable — preserve reference stability for listeners
        );
    }
}
// change_to_view_change is re-exported from view.rs
