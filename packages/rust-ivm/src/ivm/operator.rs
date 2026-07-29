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

use std::sync::Arc;
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

#[derive(Clone)]
#[derive(Debug)]
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
}

pub trait Output {
    fn push(&mut self, change: Change, pusher: &dyn InputBase);
}

pub trait Storage {
    fn get(&self, key: &str) -> Option<crate::ivm::data::Value>;
    fn set(&mut self, key: String, value: crate::ivm::data::Value);
    fn del(&mut self, key: &str);
    fn scan(&self, prefix: Option<&str>) -> Vec<(String, crate::ivm::data::Value)> {
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
