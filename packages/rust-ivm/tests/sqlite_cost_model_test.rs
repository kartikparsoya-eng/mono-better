//! Tests for the scanstatus/stat-fanout cost model (the TS
//! `createSQLiteCostModel` port) against a REAL seeded SQLite database —
//! filter-aware row estimates, constraint seeks, sorter startup cost,
//! stat4/stat1/default fanout, and the flip decision the old COUNT(*) model
//! provably gets wrong.
//!
//! Scanstatus availability depends on the linked SQLite
//! (`SQLITE_ENABLE_STMT_SCANSTATUS`). macOS system SQLite and the wal2 builds
//! have it; if a build lacks it the model refuses to construct (asserted
//! below) and the data-driven tests are skipped with a notice.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rust_ivm::builder::ast::Condition;
use rust_ivm::ivm::schema::ColumnType;
use rust_ivm::planner::plan_ast_flips;
use rust_ivm::sqlite::sqlite_cost_model::{create_sqlite_cost_model, scanstatus_available};
use rust_ivm::sqlite::sqlite_stat_fanout::{FanoutSource, SQLiteStatFanout};

fn stat4_available() -> bool {
    // ENABLE_STAT4 controls whether ANALYZE writes sqlite_stat4.
    let opt = std::ffi::CString::new("ENABLE_STAT4").unwrap();
    unsafe { rusqlite::ffi::sqlite3_compileoption_used(opt.as_ptr()) != 0 }
}

/// parent: 100 rows. child: 10_000 rows, ~100 per parent_id bucket, plus:
/// - `email`: high-cardinality indexed column (stat1 avg ≈ 1 per value) —
///   selective-equality estimates work on every build,
/// - `kind`: 2-value skewed column (5 'rare' / 9995 'common') — only a stat4
///   histogram can see the skew,
/// - `unsorted`: unindexed (drives the sorter startup-cost test).
fn seed() -> Rc<RefCell<rusqlite::Connection>> {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            kind TEXT,
            email TEXT,
            unsorted TEXT
        );
        CREATE INDEX child_parent ON child (parent_id);
        CREATE INDEX child_kind ON child (kind);
        CREATE INDEX child_email ON child (email);
        "#,
    )
    .unwrap();
    for i in 0..100 {
        conn.execute(
            "INSERT INTO parent VALUES (?, ?)",
            rusqlite::params![i, format!("p{i}")],
        )
        .unwrap();
    }
    for i in 0..10_000i64 {
        conn.execute(
            "INSERT INTO child VALUES (?, ?, ?, ?, ?)",
            rusqlite::params![
                i,
                i % 100,
                if i < 5 { "rare" } else { "common" },
                format!("e{i}"),
                format!("u{}", (i * 7919) % 10_000),
            ],
        )
        .unwrap();
    }
    Rc::new(RefCell::new(conn))
}

fn specs() -> HashMap<String, HashMap<String, ColumnType>> {
    let s = |o| ColumnType::String { optional: o };
    let n = |o| ColumnType::Number { optional: o };
    HashMap::from([
        (
            "parent".to_string(),
            HashMap::from([("id".to_string(), n(false)), ("name".to_string(), s(true))]),
        ),
        (
            "child".to_string(),
            HashMap::from([
                ("id".to_string(), n(false)),
                ("parent_id".to_string(), n(true)),
                ("kind".to_string(), s(true)),
                ("email".to_string(), s(true)),
                ("unsorted".to_string(), s(true)),
            ]),
        ),
    ])
}

fn eq_filter(column: &str, value: &str) -> Condition {
    Condition::Simple(rust_ivm::builder::ast::SimpleCondition {
        op: "=".to_string(),
        left: rust_ivm::builder::ast::ValuePosition::Column {
            name: column.to_string(),
        },
        right: rust_ivm::builder::ast::ValuePosition::Literal {
            value: rust_ivm::ivm::data::Value::Str(value.into()),
        },
    })
}

macro_rules! require_scanstatus {
    () => {
        if !scanstatus_available() {
            eprintln!(
                "SKIP: linked SQLite lacks SQLITE_ENABLE_STMT_SCANSTATUS; \
                 cost-model data tests need it (wal2 builds and macOS system \
                 SQLite have it)"
            );
            return;
        }
    };
}

#[test]
fn model_creation_fails_loudly_without_scanstatus() {
    // Whichever way the linked SQLite was built, the contract holds: with
    // scanstatus the model constructs; without it, construction errors
    // (never silently-blind estimates).
    let conn = seed();
    let result = create_sqlite_cost_model(conn, specs());
    match result {
        Ok(_) => assert!(scanstatus_available()),
        Err(e) => {
            assert!(!scanstatus_available());
            assert!(e.contains("STMT_SCANSTATUS"));
        }
    }
}

#[test]
fn filter_aware_est_selective_filter_shrinks_rows() {
    require_scanstatus!();
    let conn = seed();
    conn.borrow().execute_batch("ANALYZE;").unwrap();
    let model = create_sqlite_cost_model(conn, specs()).unwrap();

    let unfiltered = model("child", &[], None, None);
    let filtered = model("child", &[], Some(&eq_filter("email", "e42")), None);

    assert!(
        unfiltered.rows >= 5_000.0,
        "unfiltered scan should estimate ~table size, got {}",
        unfiltered.rows
    );
    // email='e42' matches 1 of 10_000 rows; with the index + stats the EST
    // must be FAR below the table size (this is exactly what the old COUNT(*)
    // model could not see).
    assert!(
        filtered.rows <= unfiltered.rows / 10.0,
        "selective filter must shrink the estimate: filtered={} unfiltered={}",
        filtered.rows,
        unfiltered.rows
    );

    // The stat4 histogram additionally sees VALUE skew inside a low-cardinality
    // column: kind='rare' is 5/10_000 but stat1's per-value average is 5_000.
    if stat4_available() {
        let skewed = model("child", &[], Some(&eq_filter("kind", "rare")), None);
        assert!(
            skewed.rows <= unfiltered.rows / 10.0,
            "stat4 build must see the kind='rare' skew: skewed={} unfiltered={}",
            skewed.rows,
            unfiltered.rows
        );
    }
}

#[test]
fn constraint_is_estimated_as_indexed_seek() {
    require_scanstatus!();
    let conn = seed();
    conn.borrow().execute_batch("ANALYZE;").unwrap();
    let model = create_sqlite_cost_model(conn, specs()).unwrap();

    let mut constraint = rust_ivm::planner::PlannerConstraint::default();
    constraint.insert("parent_id".to_string(), None);

    let scan = model("child", &[], None, None);
    let seek = model("child", &[], None, Some(&constraint));

    // ~100 rows per parent_id value vs 10_000 total.
    assert!(
        seek.rows < scan.rows / 10.0,
        "constrained probe must be a seek: seek={} scan={}",
        seek.rows,
        scan.rows
    );
}

#[test]
fn order_by_without_index_adds_startup_cost() {
    require_scanstatus!();
    let conn = seed();
    conn.borrow().execute_batch("ANALYZE;").unwrap();
    let model = create_sqlite_cost_model(conn, specs()).unwrap();

    let sorted_by_pk = model(
        "child",
        &[("id".to_string(), "asc".to_string())],
        None,
        None,
    );
    let sorted_unindexed = model(
        "child",
        &[("unsorted".to_string(), "asc".to_string())],
        None,
        None,
    );

    assert_eq!(
        sorted_by_pk.startup_cost, 0.0,
        "PK order needs no sorter b-tree"
    );
    // TS: btreeCost(rows) = rows*log2(rows)/10 accumulated for ORDER BY loops.
    assert!(
        sorted_unindexed.startup_cost > 0.0,
        "unindexed ORDER BY must pay a sorter startup cost"
    );
}

#[test]
fn fanout_default_then_stat1_then_stat4() {
    require_scanstatus!();
    let conn = seed();

    // No ANALYZE yet → default fanout 3 / none.
    {
        let est = SQLiteStatFanout::new(conn.clone());
        let r = est.get_fanout("child", &["parent_id".to_string()]);
        assert_eq!(r.source, FanoutSource::Default);
        assert_eq!(r.fanout, 3.0);
    }

    conn.borrow().execute_batch("ANALYZE;").unwrap();

    // With stats: stat4 when the build writes it, else stat1. Either way the
    // fanout must be ~100 (10_000 child rows / 100 parent_id values).
    let est = SQLiteStatFanout::new(conn.clone());
    let r = est.get_fanout("child", &["parent_id".to_string()]);
    if stat4_available() {
        assert_eq!(r.source, FanoutSource::Stat4, "stat4 build must use stat4");
    } else {
        assert_eq!(
            r.source,
            FanoutSource::Stat1,
            "without ENABLE_STAT4, ANALYZE writes stat1 only"
        );
    }
    assert!(
        (50.0..=200.0).contains(&r.fanout),
        "fanout ≈100 expected, got {} (source {:?})",
        r.fanout,
        r.source
    );

    // Unindexed column → no usable index → default.
    let r = est.get_fanout("child", &["unsorted".to_string()]);
    assert_eq!(r.source, FanoutSource::Default);
}

#[test]
fn stat4_median_ignores_null_samples() {
    if !scanstatus_available() || !stat4_available() {
        eprintln!("SKIP: needs ENABLE_STMT_SCANSTATUS + ENABLE_STAT4 (wal2/prod builds)");
        return;
    }
    // Sparse FK: 90% NULL parent refs, non-NULL fanout 4. stat1 would blend
    // the NULLs in; stat4's non-NULL median must not.
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE tasks (id INTEGER PRIMARY KEY, project_id INTEGER);
        CREATE INDEX tasks_project ON tasks (project_id);
        "#,
    )
    .unwrap();
    let mut id = 0i64;
    for p in 0..25 {
        for _ in 0..4 {
            conn.execute("INSERT INTO tasks VALUES (?, ?)", rusqlite::params![id, p])
                .unwrap();
            id += 1;
        }
    }
    for _ in 0..900 {
        conn.execute("INSERT INTO tasks VALUES (?, NULL)", rusqlite::params![id])
            .unwrap();
        id += 1;
    }
    conn.execute_batch("ANALYZE;").unwrap();

    let conn = Rc::new(RefCell::new(conn));
    let est = SQLiteStatFanout::new(conn);
    let r = est.get_fanout("tasks", &["project_id".to_string()]);
    assert_eq!(r.source, FanoutSource::Stat4);
    assert!(
        r.fanout <= 10.0,
        "stat4 median must exclude the 900-row NULL bucket, got {}",
        r.fanout
    );
}

/// THE decision-level regression guard: a selective EXISTS on a BIG child
/// table. The old COUNT(*) model sees child=10_000 > parent=100 and refuses
/// to flip; the scanstatus model sees the inlined filter's EST (~5 rows) and
/// flips — matching TS.
#[test]
fn selective_exists_flips_where_count_model_does_not() {
    require_scanstatus!();
    let conn = seed();
    conn.borrow().execute_batch("ANALYZE;").unwrap();

    let ast = serde_json::json!({
        "table": "parent",
        "where": {
            "type": "correlatedSubquery",
            "op": "EXISTS",
            "related": {
                "correlation": {"parentField": ["id"], "childField": ["parent_id"]},
                "subquery": {
                    "table": "child",
                    "alias": "child",
                    "where": {
                        "type": "simple",
                        "op": "=",
                        "left": {"type": "column", "name": "email"},
                        "right": {"type": "literal", "value": "e42"}
                    }
                }
            }
        }
    });

    let scanstatus_flips = plan_ast_flips(
        &ast,
        create_sqlite_cost_model(conn.clone(), specs()).unwrap(),
    );
    assert_eq!(
        scanstatus_flips,
        vec![Some(true)],
        "scanstatus model must flip a selective EXISTS on a big child table"
    );

    let count_flips = plan_ast_flips(&ast, rust_ivm::planner::create_snapshot_cost_model(conn));
    assert_eq!(
        count_flips,
        vec![Some(false)],
        "the filter-blind COUNT(*) model does not flip this shape — if this \
         starts flipping, the escape-hatch model changed"
    );
}
