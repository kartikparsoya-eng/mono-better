//! Tests for `QueryDelegateBase` default impls — port of
//! `zql/src/query/query-delegate-base.ts`. The base's concrete methods
//! (batch_view_updates firing commit observers, create_storage,
//! on_transaction_commit, assert_valid_run_options) were untested (triage #22).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use rust_ivm::ivm::data::Value;
use rust_ivm::query::query_delegate_base::{QueryDelegateBase, RunOptions};

// Port of TS defaults: a fresh base reports default_query_complete=true and no
// sources.
#[test]
fn new_and_default_match() {
    let d = QueryDelegateBase::new();
    assert!(d.default_query_complete);
    assert!(d.sources.is_empty());
    // Default::default is the same construction.
    let d2 = QueryDelegateBase::default();
    assert!(d2.default_query_complete);
}

// Port of TS `batchViewUpdates`/`onTransactionCommit`: registered commit
// observers all fire when a batch completes, and the batch returns the closure's
// value.
#[test]
fn batch_view_updates_fires_all_commit_observers() {
    let mut d = QueryDelegateBase::new();

    let hits = Arc::new(AtomicUsize::new(0));
    for _ in 0..3 {
        let h = hits.clone();
        // The returned unsubscribe closure is unused here (we want them to fire).
        let _unsub = d.on_transaction_commit(Arc::new(move || {
            h.fetch_add(1, Ordering::SeqCst);
        }));
    }

    let ret = d.batch_view_updates(|| 42);
    assert_eq!(ret, 42, "batch returns the applier's value");
    assert_eq!(
        hits.load(Ordering::SeqCst),
        3,
        "all three observers fired once"
    );

    // A second batch fires them again (observers are not consumed).
    d.batch_view_updates(|| ());
    assert_eq!(hits.load(Ordering::SeqCst), 6);
}

// Port of TS `createStorage`: returns a working (in-memory) storage.
#[test]
fn create_storage_returns_working_storage() {
    let mut d = QueryDelegateBase::new();
    let storage = d.create_storage();

    storage.borrow_mut().set("k".to_string(), Value::F64(7.0));
    assert_eq!(storage.borrow().get("k"), Some(Value::F64(7.0)));

    storage.borrow_mut().del("k");
    assert_eq!(storage.borrow().get("k"), None);
}

// `assert_valid_run_options` is a no-op in the base (does not panic).
#[test]
fn assert_valid_run_options_is_a_noop() {
    let d = QueryDelegateBase::new();
    d.assert_valid_run_options(&RunOptions::default());
}
