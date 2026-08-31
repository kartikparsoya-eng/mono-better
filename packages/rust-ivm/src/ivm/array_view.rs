//! ArrayView — port of `zql/src/ivm/array-view.ts`.
//!
//! Implements a materialized view of the output of an operator.
//! Collects fetch results and applies changes immutably via `apply_change`.
//! Notifies listeners on flush.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::change::Change;
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::view::{Entry, Format, View};
use crate::ivm::view_apply_change::{
    ViewChange, apply_change, change_to_view_change, empty_root_entry,
};

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

        // Port of TS array-view.ts:88 `this.#root = {'': format.singular ?
        // undefined : []}`: the root's "" relationship is ALWAYS initialized —
        // to an empty list for list format, or absent (undefined) for singular.
        // Without this, an empty hydrate leaves `data()` as None ("absent")
        // instead of Some(List([])) ("query ran, zero rows"), and add_listener's
        // immediate fire is skipped — an observable divergence from TS.
        let mut root = empty_root_entry();
        root.relationships.insert(
            String::new(),
            if format.singular {
                View::None // TS `undefined`
            } else {
                View::List(Vec::new())
            },
        );

        let av = Rc::new(RefCell::new(ArrayView {
            input: input.clone(),
            schema,
            format,
            root,
            listeners: Vec::new(),
            dirty: false,
        }));

        let av_clone = av.clone();
        input
            .borrow_mut()
            .set_output(Rc::new(RefCell::new(ArrayViewOutput { av: av_clone })));

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
                node: crate::ivm::view_apply_change::ViewNode::Lazy(node),
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
