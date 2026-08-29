//! Pins the MemorySource accessor + push surface flagged truly-uncovered by
//! the L2 triage (parity/coverage/rust-ivm/triage.md, source.rs rows):
//! `get_row`, `all_rows`, and `gen_push` (port of TS memory-source.ts
//! `getRow`/`push` — rust yields per-connection results with no coop token,
//! plus the labeled Go-IVM split-edit adaptation).

use std::collections::HashMap;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use rust_ivm::ivm::change::SourceChange;
use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::ivm::source::MemorySource;

fn row(pairs: &[(&str, Value)]) -> FxHashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

fn make_source() -> MemorySource {
    let columns: HashMap<String, ColumnType> = [
        ("id".to_string(), ColumnType::Number { optional: false }),
        ("name".to_string(), ColumnType::String { optional: true }),
    ]
    .into();
    MemorySource::new("users", columns, vec!["id".to_string()])
}

/// `get_row` (TS getRow): present PK → the row; absent PK → None. `all_rows`
/// returns the full row set, and add_row REPLACES on PK collision rather
/// than duplicating.
#[test]
fn get_row_and_all_rows_reflect_storage() {
    let mut source = make_source();
    source.add_row(row(&[
        ("id", Value::F64(1.0)),
        ("name", Value::Str("a".into())),
    ]));
    source.add_row(row(&[
        ("id", Value::F64(2.0)),
        ("name", Value::Str("b".into())),
    ]));

    let got = source
        .get_row(&[("id".to_string(), Value::F64(1.0))])
        .expect("row 1");
    assert_eq!(got.get("name"), Some(&Value::Str("a".into())));
    assert!(
        source
            .get_row(&[("id".to_string(), Value::F64(9.0))])
            .is_none()
    );
    assert_eq!(source.all_rows().len(), 2);

    // Same-PK add replaces (keeps the in-memory source consistent).
    source.add_row(row(&[
        ("id", Value::F64(1.0)),
        ("name", Value::Str("a2".into())),
    ]));
    assert_eq!(source.all_rows().len(), 2, "replace, not duplicate");
    let got = source
        .get_row(&[("id".to_string(), Value::F64(1.0))])
        .expect("row 1");
    assert_eq!(got.get("name"), Some(&Value::Str("a2".into())));
}

/// `gen_push` applies Add/Edit/Remove through a live connection: each push
/// reaches the connection's OUTPUT (observed via the Catch collector, the
/// port of TS catch.ts) and mutates storage so `get_row` sees the new state
/// (TS push writes then notifies connections).
#[test]
fn gen_push_add_edit_remove_through_a_connection() {
    let mut source = make_source();
    let input = source.connect(None, None, None, None);
    let catch = rust_ivm::ivm::catch::Catch::new(input, false);

    source.gen_push(SourceChange::Add {
        row: Arc::new(row(&[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("a".into())),
        ])),
    });
    assert!(
        matches!(
            catch.borrow().pushes.as_slice(),
            [rust_ivm::ivm::catch::CaughtChange::Add { .. }]
        ),
        "Add must reach the output"
    );
    assert!(
        source
            .get_row(&[("id".to_string(), Value::F64(1.0))])
            .is_some(),
        "push writes through to storage"
    );

    source.gen_push(SourceChange::Edit {
        row: Arc::new(row(&[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("a2".into())),
        ])),
        old_row: Arc::new(row(&[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("a".into())),
        ])),
    });
    assert!(
        matches!(
            catch.borrow().pushes.last(),
            Some(rust_ivm::ivm::catch::CaughtChange::Edit { .. })
        ),
        "non-key Edit passes through as Edit"
    );
    assert_eq!(
        source
            .get_row(&[("id".to_string(), Value::F64(1.0))])
            .unwrap()
            .get("name"),
        Some(&Value::Str("a2".into()))
    );

    source.gen_push(SourceChange::Remove {
        row: Arc::new(row(&[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("a2".into())),
        ])),
    });
    assert!(
        matches!(
            catch.borrow().pushes.last(),
            Some(rust_ivm::ivm::catch::CaughtChange::Remove { .. })
        ),
        "Remove reaches the output"
    );
    assert!(
        source
            .get_row(&[("id".to_string(), Value::F64(1.0))])
            .is_none(),
        "remove deletes from storage"
    );
}

/// A key-changing Edit on a connection with `split_edit_keys` splits into
/// Remove(old)+Add(new) BEFORE the push (the labeled Go-IVM adaptation that
/// prevents Join panics on key-changing edits); the output must see the
/// Remove leg then the Add leg, never an Edit.
#[test]
fn gen_push_splits_key_changing_edit() {
    let mut source = make_source();
    let input = source.connect(None, None, None, Some(vec!["name".to_string()]));
    let catch = rust_ivm::ivm::catch::Catch::new(input, false);
    source.gen_push(SourceChange::Add {
        row: Arc::new(row(&[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("a".into())),
        ])),
    });
    catch.borrow_mut().reset();

    source.gen_push(SourceChange::Edit {
        row: Arc::new(row(&[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("CHANGED".into())),
        ])),
        old_row: Arc::new(row(&[
            ("id", Value::F64(1.0)),
            ("name", Value::Str("a".into())),
        ])),
    });
    let pushes = &catch.borrow().pushes;
    assert!(
        matches!(
            pushes.as_slice(),
            [
                rust_ivm::ivm::catch::CaughtChange::Remove { .. },
                rust_ivm::ivm::catch::CaughtChange::Add { .. },
            ]
        ),
        "key-changing edit must split into Remove(old)+Add(new), got {} pushes",
        pushes.len()
    );
}
