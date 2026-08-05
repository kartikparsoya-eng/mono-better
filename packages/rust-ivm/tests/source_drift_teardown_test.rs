//! Source-drift teardown coverage (same bug CLASS as take.rs:670).
//!
//! A Remove/Add-duplicate/Edit whose row does NOT match the source's current
//! contents is a hydrate-vs-changestream *divergence*: the incremental change
//! assumes a row state the snapshot never had. Like the empty-hydrated take
//! partition (`bound == None`), it is reachable by DATA, not by a code bug.
//!
//! DECISION (2026-08-05): unlike take-bound — which we convert to an in-place
//! `-2` reset — source drift deliberately stays on the THROW -> view-syncer
//! teardown -> client reconnect path (matching TS). This test locks two facts
//! that the napi layer depends on for that behavior:
//!   1. The drift assert actually FIRES on the incremental push path.
//!   2. Its panic is `catch_unwind`-safe and carries a "source drift" message
//!      (so lib.rs can surface it as a thrown Err, not a silent drop / SIGABRT).
//!
//! The sibling tripwire in napi/src/lib.rs asserts a "source drift" payload maps
//! to the Err arm (NOT a `-2` reset). If someone later makes drift recoverable,
//! that sibling flips red and forces a conscious decision.
//!
//! Deterministic, single-threaded. Run: cargo test --test source_drift_teardown_test

use std::cell::RefCell;
use std::collections::HashMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::change::SourceChange;
use rust_ivm::ivm::data::{Row, Value};
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;

fn make_row(pairs: &[(&str, Value)]) -> Row {
    let map: FxHashMap<String, Value> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    Arc::new(map)
}

fn num(n: f64) -> Value {
    Value::F64(n)
}

/// MemorySource with in-memory validation active (db_path is None), seeded with
/// a single row id=1. Matches the production source-drift guard at
/// source.rs:379-400, which runs BEFORE any pipeline mutation.
fn seeded_source() -> Rc<RefCell<MemorySource>> {
    let mut cols: HashMap<String, ColumnType> = HashMap::new();
    cols.insert("id".to_string(), ColumnType::Number { optional: false });
    let source = Rc::new(RefCell::new(MemorySource::new(
        "users",
        cols,
        vec!["id".to_string()],
    )));
    // Connect so the source is live, then seed one valid row.
    source.borrow_mut().connect(None, None, None, None);
    source.borrow_mut().push(SourceChange::Add {
        row: make_row(&[("id", num(1.0))]),
    });
    source
}

/// Push `change` and return the panic message, or `None` if it did not panic.
/// Uses `catch_unwind` exactly like the napi advance boundary (lib.rs:1511) so
/// this proves the panic never crosses an FFI boundary / SIGABRTs the process.
fn push_expecting_drift(source: &Rc<RefCell<MemorySource>>, change: SourceChange) -> Option<String> {
    let result = catch_unwind(AssertUnwindSafe(|| {
        source.borrow_mut().push(change);
    }));
    match result {
        Ok(()) => None,
        Err(payload) => payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned()),
    }
}

#[test]
fn remove_missing_row_panics_source_drift() {
    let source = seeded_source();
    // Remove a row (id=99) the source never held.
    let msg = push_expecting_drift(
        &source,
        SourceChange::Remove {
            row: make_row(&[("id", num(99.0))]),
        },
    )
    .expect("Remove of a missing row MUST panic (source drift), not silently no-op");
    assert!(
        msg.contains("source drift") && msg.contains("Remove missing"),
        "panic message must identify the drift class, got: {msg}",
    );
}

#[test]
fn add_duplicate_row_panics_source_drift() {
    let source = seeded_source();
    // Add id=1 again — the snapshot already has it.
    let msg = push_expecting_drift(
        &source,
        SourceChange::Add {
            row: make_row(&[("id", num(1.0))]),
        },
    )
    .expect("Add of a duplicate row MUST panic (source drift)");
    assert!(
        msg.contains("source drift") && msg.contains("Add duplicate"),
        "panic message must identify the drift class, got: {msg}",
    );
}

#[test]
fn edit_missing_old_row_panics_source_drift() {
    let source = seeded_source();
    // Edit whose OLD row (id=42) was never present.
    let msg = push_expecting_drift(
        &source,
        SourceChange::Edit {
            row: make_row(&[("id", num(42.0))]),
            old_row: make_row(&[("id", num(42.0))]),
        },
    )
    .expect("Edit of a missing old row MUST panic (source drift)");
    assert!(
        msg.contains("source drift") && msg.contains("Edit missing"),
        "panic message must identify the drift class, got: {msg}",
    );
}

#[test]
fn source_survives_a_caught_drift_panic() {
    // After a drift panic is CAUGHT (as the napi boundary does), the source
    // object is still usable — the panic unwound cleanly without corrupting the
    // process. A subsequent VALID push succeeds. (In production the engine is
    // torn down + rehydrated; this only asserts no UB / poisoned allocator.)
    let source = seeded_source();
    let _ = push_expecting_drift(
        &source,
        SourceChange::Remove {
            row: make_row(&[("id", num(99.0))]),
        },
    );
    // A legitimate add still works.
    let ok = catch_unwind(AssertUnwindSafe(|| {
        source.borrow_mut().push(SourceChange::Add {
            row: make_row(&[("id", num(2.0))]),
        });
    }));
    assert!(ok.is_ok(), "source must remain usable after a caught drift panic");
}
