//! ZQLite query delegate — port of `zqlite/src/query-delegate.ts`.
//!
//! A QueryDelegate implementation backed by SQLite TableSource.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::builder::ast::Ast;
use crate::ivm::array_view::ArrayView;
use crate::ivm::operator::{Shared, Storage};
use crate::ivm::schema::ColumnType;
use crate::ivm::source::Source;
use crate::query::metrics_delegate::{Metric, MetricsDelegate};
use crate::query::named::CustomQueryID;
use crate::query::query_delegate_base::{
    CommitListener, MaterializeOptions, PreloadOptions, QueryDelegate, RunOptions,
};
use crate::query::query_impl::Query;
use crate::sqlite::table_source::TableSource;

/// ZQLite QueryDelegate implementation.
/// Port of TS `QueryDelegateImpl` (query-delegate.ts:7).
pub struct ZqliteQueryDelegate {
    db: Rc<RefCell<crate::sqlite::db::Database>>,
    table_names: Vec<String>,
    primary_keys: HashMap<String, Vec<String>>,
    sources: HashMap<String, Shared<dyn Source>>,
    commit_observers: Vec<CommitListener>,
    default_query_complete: bool,
}

impl ZqliteQueryDelegate {
    pub fn new(
        db: Rc<RefCell<crate::sqlite::db::Database>>,
        table_names: Vec<String>,
        primary_keys: HashMap<String, Vec<String>>,
    ) -> Self {
        ZqliteQueryDelegate {
            db,
            table_names,
            primary_keys,
            sources: HashMap::new(),
            commit_observers: Vec::new(),
            default_query_complete: true,
        }
    }
}

impl crate::builder::builder::BuilderDelegate for ZqliteQueryDelegate {
    fn get_source(&self, table_name: &str) -> Option<Shared<dyn Source>> {
        // Return cached source if already created.
        if let Some(s) = self.sources.get(table_name) {
            return Some(s.clone());
        }
        // Lazily create a TableSource backed by the SQLite database.
        if !self.table_names.iter().any(|t| t == table_name) {
            return None;
        }
        let pk = self
            .primary_keys
            .get(table_name)
            .cloned()
            .unwrap_or_default();
        let conn = self.db.borrow().conn();
        // Build column schema from the database. For now, default to String type
        // — the actual types are resolved at fetch time via rusqlite's Value enum.
        // No column schema here (types resolved at fetch time) ⇒ `TableSource::new`
        // sees no columns and emits `SELECT *`.
        let columns: HashMap<String, ColumnType> = HashMap::new();
        let source = TableSource::new(conn, table_name, columns, pk);
        let shared: Shared<dyn Source> = Rc::new(RefCell::new(source));
        // Note: can't insert into self.sources because get_source takes &self.
        // The source is created fresh each time get_source is called, which is
        // fine — each pipeline gets its own connection to the source.
        Some(shared)
    }
}

impl MetricsDelegate for ZqliteQueryDelegate {
    fn add_metric(&self, _metric: Metric, _value: f64, _query_id: &str, _ast: Option<&Ast>) {
        // No-op
    }
}

impl QueryDelegate for ZqliteQueryDelegate {
    fn add_server_query(
        &mut self,
        _ast: &Ast,
        _ttl: &str,
        _got_callback: Option<Arc<dyn Fn(bool)>>,
    ) -> Box<dyn FnOnce()> {
        Box::new(|| {})
    }

    fn add_custom_query(
        &mut self,
        _ast: &Ast,
        _custom_query_id: &CustomQueryID,
        _ttl: &str,
        _got_callback: Option<Arc<dyn Fn(bool)>>,
    ) -> Box<dyn FnOnce()> {
        Box::new(|| {})
    }

    fn update_server_query(&mut self, _ast: &Ast, _ttl: &str) {}
    fn update_custom_query(&mut self, _custom_query_id: &CustomQueryID, _ttl: &str) {}
    fn flush_query_changes(&mut self) {}

    fn on_transaction_commit(&mut self, cb: CommitListener) -> Box<dyn FnOnce()> {
        self.commit_observers.push(cb.clone());
        Box::new(|| {})
    }

    fn batch_view_updates<T>(&mut self, apply: impl FnOnce() -> T) -> T {
        let ret = apply();
        for observer in &self.commit_observers {
            observer();
        }
        ret
    }

    fn assert_valid_run_options(&self, _options: &RunOptions) {}

    fn default_query_complete(&self) -> bool {
        self.default_query_complete
    }

    fn create_storage(&mut self) -> Shared<dyn Storage> {
        Rc::new(RefCell::new(
            crate::ivm::memory_storage::MemoryStorage::new(),
        ))
    }

    fn materialize(
        &mut self,
        _query: &Query,
        _options: Option<MaterializeOptions>,
    ) -> Rc<RefCell<ArrayView>> {
        // Full implementation would build the pipeline and create an ArrayView.
        unimplemented!("materialize requires pipeline construction")
    }

    fn run(
        &mut self,
        _query: &Query,
        _options: Option<RunOptions>,
    ) -> Vec<crate::ivm::data::Value> {
        unimplemented!("run requires materialize")
    }

    fn preload(
        &mut self,
        _query: &Query,
        _options: Option<PreloadOptions>,
    ) -> (Box<dyn FnOnce()>, bool) {
        (Box::new(|| {}), true)
    }
}
