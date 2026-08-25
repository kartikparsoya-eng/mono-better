//! Typed view — port of `zql/src/query/typed-view.ts`.
//!
//! The client-facing view interface: data, listeners, TTL, lifecycle.

use crate::ivm::view::View;

/// Result type for a view: unknown (loading), complete (data ready), or error.
/// Port of TS `ResultType` (typed-view.ts:11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultType {
    Unknown,
    Complete,
    Error,
}

/// A listener callback for view updates.
/// Port of TS `Listener<T>` (typed-view.ts:18).
pub type Listener = std::rc::Rc<dyn Fn(&View, ResultType)>;

/// A typed view: the materialized result of a query.
/// Port of TS `TypedView<T>` (typed-view.ts:27).
pub trait TypedView {
    /// Add a listener that is called when the view changes.
    /// Returns a cleanup function that removes the listener.
    fn add_listener(&self, listener: Listener) -> Box<dyn FnOnce()>;

    /// Destroy the view and release resources.
    fn destroy(&self);

    /// Update the TTL for this view's query.
    fn update_ttl(&self, ttl_str: &str);

    /// Get the current data.
    fn data(&self) -> &View;
}
