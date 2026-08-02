//! Operator traits — port of `zql/src/ivm/operator.ts`.
//!
//! Key mapping:
//! - `Input` interface → `Input` trait
//! - `Output` interface → `Output` trait
//! - `fetch(): Stream<Node | 'yield'>` → `fetch() -> NodeStream` (no 'yield')
//! - `push(change): Stream<'yield'>` → `push(change)` (void — no 'yield')
//!
//! Ownership: `set_output` takes `OutputHandle = Rc<RefCell<dyn Output>>`
//! for shared ownership, mirroring Go's GC'd pointer semantics.

use std::cell::RefCell;
use std::rc::Rc;

use crate::ivm::change::Change;
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::NodeStream;

/// A shared output handle — used for back-references in the operator graph.
pub type OutputHandle = Rc<RefCell<dyn Output>>;

/// A fetch request — port of TS `FetchRequest`.
#[derive(Clone, Default, Debug)]
pub struct FetchRequest {
    pub constraint: Option<crate::ivm::constraint::Constraint>,
    pub multi_constraints: Vec<crate::ivm::constraint::MultiConstraint>,
    pub start: Option<Start>,
    pub reverse: bool,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct Start {
    pub row: crate::ivm::data::Row,
    pub basis: Basis,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Basis {
    At,
    After,
}

pub trait InputBase {
    fn get_schema(&self) -> SourceSchema;
    fn destroy(&mut self);
}

pub trait Input: InputBase {
    fn set_output(&self, output: OutputHandle);
    fn fetch(&self, req: &FetchRequest) -> NodeStream;

    /// Read-level parallelism (DESIGN-read-parallelism.md §2c): fetch this
    /// input's rows for each of `constraints` **in parallel** across the
    /// frame-pinned pool, returning one materialised `Vec<Node>` per constraint
    /// in input order — byte-identical to calling `fetch(constraint=c)` serially
    /// for each `c`.
    ///
    /// Only a **leaf `TableSource`** can honour this (its read is one SELECT that
    /// can run on a pooled `Send` connection). Every other operator returns
    /// `None` → the caller keeps the serial lazy path. `None` is also returned
    /// when the pool isn't pinned at the read frame (→ serial, never wrong
    /// frame). Used only on the hydrate path (no in-flight overlay).
    fn parallel_leaf_fetch(
        &self,
        _constraints: &[crate::ivm::constraint::Constraint],
    ) -> Option<Vec<Vec<crate::ivm::data::Node>>> {
        None
    }

    /// EXISTS/child-fetch IN-list batching (prototype, flag-gated by the Join
    /// caller). Instead of one SELECT per distinct constraint, run a SINGLE
    /// `... WHERE key IN (?, ?, ...)` over all `constraints` and return every
    /// matching row FLAT, in the source's ORDER BY. The caller buckets rows back
    /// per parent (stable extraction preserves per-parent child order, since the
    /// global order is the child order). Same preconditions as
    /// `parallel_leaf_fetch` (leaf TableSource, pool pinned, no overlay); `None`
    /// → caller falls back. Byte-identical result set to the N-SELECT path.
    fn batched_in_fetch(
        &self,
        _constraints: &[crate::ivm::constraint::Constraint],
    ) -> Option<Vec<crate::ivm::data::Node>> {
        None
    }

    /// Cheap pre-check: is this input a leaf source that can serve
    /// `parallel_leaf_fetch` right now (pool pinned at the read frame, no live
    /// overlay)? Lets a caller (Join) decide whether to gather constraints for a
    /// parallel batch before doing the work. Default `false`.
    fn supports_parallel_leaf(&self) -> bool {
        false
    }
}

pub trait Output {
    fn push(&mut self, change: Change, pusher: &dyn InputBase);
}

pub trait Storage {
    fn get(&self, key: &str) -> Option<crate::ivm::data::Value>;
    fn set(&mut self, key: String, value: crate::ivm::data::Value);
    fn del(&mut self, key: &str);
    fn scan(&self, _prefix: Option<&str>) -> Vec<(String, crate::ivm::data::Value)> {
        Vec::new()
    }
}

/// An Output implementation that throws if pushed to.
pub struct ThrowOutput;
impl Output for ThrowOutput {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {
        panic!("Output not set");
    }
}

/// Shared operator state.
pub type Shared<T> = Rc<RefCell<T>>;
