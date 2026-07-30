//! Query delegate — port of `zql/src/query/query-delegate.ts` and `query-delegate-base.ts`.
//!
//! Interface for delegates that support materializing, running, and preloading queries.
//! The base class provides default implementations.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::builder::ast::Ast;
use crate::builder::builder::BuilderDelegate;
use crate::builder::metrics_delegate::MetricsDelegate;
use crate::builder::named::CustomQueryID;
use crate::builder::query::Query;
use crate::ivm::operator::{Shared, Storage};
use crate::ivm::source::Source;

/// Commit listener callback.
pub type CommitListener = Arc<dyn Fn()>;

/// Got callback: called with whether the query was received.
pub type GotCallback = Arc<dyn Fn(bool)>;

/// Options for running a query.
#[derive(Clone, Debug, Default)]
pub struct RunOptions {
    pub ttl: Option<String>,
    pub result_type: Option<RunResultType>,
}

/// Result type for run options.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunResultType {
    Unknown,
    Complete,
}

/// Options for materializing a query.
#[derive(Clone, Debug, Default)]
pub struct MaterializeOptions {
    pub ttl: Option<String>,
}

/// Options for preloading a query.
#[derive(Clone, Debug, Default)]
pub struct PreloadOptions {
    pub ttl: Option<String>,
}

/// Query delegate interface — supports materializing, running, and preloading.
/// Port of TS `QueryDelegate` (query-delegate.ts:36).
pub trait QueryDelegate: BuilderDelegate + MetricsDelegate {
    fn add_server_query(
        &mut self,
        ast: &Ast,
        ttl: &str,
        got_callback: Option<GotCallback>,
    ) -> Box<dyn FnOnce()>;
    fn add_custom_query(
        &mut self,
        ast: &Ast,
        custom_query_id: &CustomQueryID,
        ttl: &str,
        got_callback: Option<GotCallback>,
    ) -> Box<dyn FnOnce()>;
    fn update_server_query(&mut self, ast: &Ast, ttl: &str);
    fn update_custom_query(&mut self, custom_query_id: &CustomQueryID, ttl: &str);
    fn flush_query_changes(&mut self);
    fn on_transaction_commit(&mut self, cb: CommitListener) -> Box<dyn FnOnce()>;
    fn batch_view_updates<T>(&mut self, apply: impl FnOnce() -> T) -> T;
    fn assert_valid_run_options(&self, options: &RunOptions);
    fn default_query_complete(&self) -> bool;
    fn create_storage(&mut self) -> Shared<dyn Storage>;
    fn materialize(
        &mut self,
        query: &Query,
        options: Option<MaterializeOptions>,
    ) -> Rc<RefCell<crate::ivm::array_view::ArrayView>>;
    fn run(&mut self, query: &Query, options: Option<RunOptions>) -> Vec<Value>;
    fn preload(
        &mut self,
        query: &Query,
        options: Option<PreloadOptions>,
    ) -> (Box<dyn FnOnce()>, bool);
}

use crate::ivm::data::Value;

/// Base query delegate with default implementations.
/// Port of TS `QueryDelegateBase` (query-delegate-base.ts:36).
pub struct QueryDelegateBase {
    pub sources: std::collections::HashMap<String, Shared<dyn Source>>,
    pub default_query_complete: bool,
    commit_observers: Vec<CommitListener>,
}

impl QueryDelegateBase {
    pub fn new() -> Self {
        QueryDelegateBase {
            sources: std::collections::HashMap::new(),
            default_query_complete: true,
            commit_observers: Vec::new(),
        }
    }

    pub fn batch_view_updates<T>(&mut self, apply: impl FnOnce() -> T) -> T {
        let ret = apply();
        for observer in &self.commit_observers {
            observer();
        }
        ret
    }

    pub fn create_storage(&mut self) -> Shared<dyn Storage> {
        Rc::new(RefCell::new(
            crate::ivm::memory_storage::MemoryStorage::new(),
        ))
    }

    pub fn on_transaction_commit(&mut self, cb: CommitListener) -> Box<dyn FnOnce()> {
        self.commit_observers.push(cb.clone());
        let _cb2 = cb;
        Box::new(move || {
            // In a full impl, this removes the observer.
            // For now, it's a no-op since we can't easily remove Rc callbacks.
        })
    }

    pub fn assert_valid_run_options(&self, _options: &RunOptions) {
        // No-op
    }
}

impl Default for QueryDelegateBase {
    fn default() -> Self {
        Self::new()
    }
}
