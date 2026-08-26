//! Tests for `ZqliteQueryDelegate` — port of `zqlite/src/query-delegate.ts`
//! (`QueryDelegateImpl`). Triage #19: the whole delegate (16 fns) was untested.
//!
//! `materialize`/`run` are an intentional scaffold (`unimplemented!` — the Rust
//! syncer drives IVM through `Engine`/`IvmPipelines`, never this delegate, which
//! has zero workspace callers), so this pins the *testable* surface: source
//! lookup, the commit-observer batch, storage, and the no-op registration
//! methods — so the 1:1 TS-shaped API can't silently rot (same rationale the
//! rust-cvr triage uses for the untested `RowRecordCache` scaffold methods).
//!
//! It also ENCODES two documented divergences from TS `QueryDelegateImpl` that
//! fall out of Rust's `BuilderDelegate::get_source(&self)` signature:
//!   1. `getSource` is NOT cached (TS memoizes in `#sources`; Rust builds a fresh
//!      `TableSource` per call because `&self` can't populate the cache).
//!   2. `onTransactionCommit`'s returned unlisten closure is a no-op (TS deletes
//!      the observer from its set; Rust returns `Box::new(|| {})`).
//! Both are inert in practice (the delegate is unused), but pinned here so the
//! divergence is a deliberate, tested fact rather than silent drift.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rust_ivm::builder::ast::Ast;
use rust_ivm::builder::builder::BuilderDelegate;
use rust_ivm::ivm::data::Value;
use rust_ivm::query::metrics_delegate::{Metric, MetricsDelegate};
use rust_ivm::query::query_delegate_base::{QueryDelegate, RunOptions};
use rust_ivm::sqlite::db::Database;
use rust_ivm::sqlite::query_delegate::ZqliteQueryDelegate;

fn delegate_with_table() -> ZqliteQueryDelegate {
    let db = Database::in_memory().expect("open in-memory db");
    db.exec("CREATE TABLE issue (id TEXT PRIMARY KEY, title TEXT)")
        .expect("create table");
    let db = Rc::new(RefCell::new(db));
    let primary_keys = HashMap::from([("issue".to_string(), vec!["id".to_string()])]);
    ZqliteQueryDelegate::new(db, vec!["issue".to_string()], primary_keys)
}

// Port of TS `getSource`: a declared table yields a `TableSource` carrying that
// table's name + primary key; an undeclared table yields `None`.
//
// TS DIVERGENCE (documented): TS `getSource` reads `schema.tables[tableName]`
// and would THROW a TypeError for an unknown table (`undefined.columns`); the
// Rust `BuilderDelegate::get_source` returns `Option` and yields `None`. The
// `Option` is the Rust idiom for TS's throw-on-absent — the caller (builder)
// branches on `None` instead of catching.
#[test]
fn get_source_known_table_some_unknown_none() {
    let d = delegate_with_table();

    let src = d.get_source("issue").expect("declared table has a source");
    assert_eq!(src.borrow().table_name(), "issue");
    assert_eq!(src.borrow().primary_key(), &["id".to_string()]);

    // Undeclared table → None (TS would throw; see the divergence note above).
    assert!(d.get_source("nope").is_none());
}

// TS DIVERGENCE (documented + pinned): TS memoizes the created source in
// `#sources` so repeated `getSource` calls return the SAME instance. Rust's
// `get_source(&self)` cannot populate the cache, so each call builds a FRESH
// `TableSource`. This test pins that fresh-each-time behavior (two calls return
// distinct `Rc`s) so the divergence is a deliberate, asserted fact.
#[test]
fn get_source_is_not_cached_fresh_each_call() {
    let d = delegate_with_table();
    let a = d.get_source("issue").unwrap();
    let b = d.get_source("issue").unwrap();
    assert!(
        !Rc::ptr_eq(&a, &b),
        "Rust get_source builds a fresh source per call (TS caches — documented divergence)"
    );
}

// Port of TS `batchViewUpdates`/`onTransactionCommit`: every registered commit
// observer fires when a batch completes, and the batch returns the applier's
// value. Observers are not consumed (a second batch fires them again).
#[test]
fn batch_view_updates_fires_all_commit_observers() {
    let mut d = delegate_with_table();
    let hits = Arc::new(AtomicUsize::new(0));
    for _ in 0..3 {
        let h = hits.clone();
        let _unsub = d.on_transaction_commit(Arc::new(move || {
            h.fetch_add(1, Ordering::SeqCst);
        }));
    }

    let ret = d.batch_view_updates(|| 42);
    assert_eq!(ret, 42, "batch returns the applier's value");
    assert_eq!(hits.load(Ordering::SeqCst), 3, "all three observers fired");

    d.batch_view_updates(|| ());
    assert_eq!(hits.load(Ordering::SeqCst), 6, "observers fire again");
}

// TS DIVERGENCE (documented + pinned): TS `onTransactionCommit` returns an
// unlisten closure that deletes the observer; Rust returns a no-op. Calling the
// returned closure therefore does NOT stop the observer from firing.
#[test]
fn on_transaction_commit_unlisten_is_a_noop() {
    let mut d = delegate_with_table();
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let unlisten = d.on_transaction_commit(Arc::new(move || {
        h.fetch_add(1, Ordering::SeqCst);
    }));

    // "Unlisten" (a no-op in Rust — TS would remove the observer here).
    unlisten();

    d.batch_view_updates(|| ());
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "the observer still fires after the no-op unlisten (documented divergence)"
    );
}

// Port of TS `createStorage`: returns a working (in-memory) storage.
#[test]
fn create_storage_returns_working_storage() {
    let mut d = delegate_with_table();
    // `create_storage` exists on both BuilderDelegate and QueryDelegate — pin the
    // QueryDelegate impl (the one the query runtime calls).
    let storage = QueryDelegate::create_storage(&mut d);
    storage.borrow_mut().set("k".to_string(), Value::F64(7.0));
    assert_eq!(storage.borrow().get("k"), Some(Value::F64(7.0)));
    storage.borrow_mut().del("k");
    assert_eq!(storage.borrow().get("k"), None);
}

// The no-op registration surface: `add_server_query`/`update_server_query`/
// `flush_query_changes` don't panic and hand back a callable cleanup closure;
// `assert_valid_run_options` is a no-op; `default_query_complete` is true;
// `add_metric` (MetricsDelegate) is a no-op.
#[test]
fn noop_registration_surface_is_callable() {
    let mut d = delegate_with_table();
    let ast = Ast {
        table: "issue".to_string(),
        ..Ast::default()
    };

    let cleanup = d.add_server_query(&ast, "1s", None);
    d.update_server_query(&ast, "2s");
    d.flush_query_changes();
    // The returned cleanup closure is callable (no-op) and must not panic.
    cleanup();

    d.assert_valid_run_options(&RunOptions::default());
    assert!(d.default_query_complete());

    // MetricsDelegate::add_metric is a no-op sink.
    d.add_metric(Metric::QueryMaterializationEndToEnd, 1.0, "q1", Some(&ast));
}
