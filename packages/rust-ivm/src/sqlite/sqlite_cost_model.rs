//! SQLite cost model — faithful port of `zqlite/src/sqlite-cost-model.ts`.
//!
//! Builds the REAL SELECT the query would run (same shape as TS
//! `buildSelectQuery` + `compileInline`: filter literals INLINED so SQLite's
//! planner sees actual values; constraint columns left as unbound `?` since
//! their values are unknown at plan time), prepares it (never executes), and
//! reads the planner's own row estimates back out through the
//! `sqlite3_stmt_scanstatus_v2` API:
//!
//! - `rows`        = EST of the first top-level loop (the main scan),
//! - `startupCost` = Σ btree_cost(rows) for subsequent top-level ORDER BY
//!   (sorter) loops,
//! - `fanout`      = stat4-median → stat1 → default-3 via [`SQLiteStatFanout`].
//!
//! Requires SQLite compiled with `SQLITE_ENABLE_STMT_SCANSTATUS` (true for the
//! prod image, the local wal2 build, and macOS system SQLite). Availability is
//! probed at model-creation time via `sqlite3_compileoption_used`; callers get
//! an `Err` (not silently-wrong estimates) when the flag is missing.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::rc::Rc;

use crate::builder::ast::Condition;
use crate::ivm::schema::ColumnType;
use crate::planner::{
    Confidence, ConnectionCostModel, CostModelCost, FanoutEst, PlannerConstraint,
};
use crate::sqlite::query_builder::{SqlParam, condition_to_sql};
use crate::sqlite::sqlite_stat_fanout::{Confidence as FanoutConfidence, SQLiteStatFanout};

// ---------------------------------------------------------------------------
// scanstatus FFI
// ---------------------------------------------------------------------------
// libsqlite3-sys's prebuilt bindings only declare the v1
// `sqlite3_stmt_scanstatus` (no PARENTID, no COMPLEX flag). The v2 symbol IS
// exported by every SQLite this crate links (wal2 static build, prod image,
// macOS system SQLite), so declare it directly. TS reads the same four ops
// with the COMPLEX flag (db.ts `scanStatus(idx, op, 1)`).

unsafe extern "C" {
    fn sqlite3_stmt_scanstatus_v2(
        stmt: *mut rusqlite::ffi::sqlite3_stmt,
        idx: c_int,
        i_scan_status_op: c_int,
        flags: c_int,
        p_out: *mut c_void,
    ) -> c_int;
}

// Constants missing from the prebuilt bindings (sqlite3.h).
const SQLITE_SCANSTAT_PARENTID: c_int = 6;
const SQLITE_SCANSTAT_COMPLEX: c_int = 1;
// Present in the bindings, mirrored here for one coherent set.
const SQLITE_SCANSTAT_EST: c_int = rusqlite::ffi::SQLITE_SCANSTAT_EST;
const SQLITE_SCANSTAT_EXPLAIN: c_int = rusqlite::ffi::SQLITE_SCANSTAT_EXPLAIN;
const SQLITE_SCANSTAT_SELECTID: c_int = rusqlite::ffi::SQLITE_SCANSTAT_SELECTID;

/// True when the linked SQLite was compiled with
/// `SQLITE_ENABLE_STMT_SCANSTATUS` (scanstatus returns real data).
pub fn scanstatus_available() -> bool {
    static AVAILABLE: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *AVAILABLE.get_or_init(|| {
        let opt = CString::new("ENABLE_STMT_SCANSTATUS").expect("static str");
        // SAFETY: sqlite3_compileoption_used is a pure lookup on a static
        // compile-option table; safe with any process-wide SQLite.
        unsafe { rusqlite::ffi::sqlite3_compileoption_used(opt.as_ptr()) != 0 }
    })
}

/// Loop information returned by SQLite's scanstatus API.
/// Port of TS `ScanstatusLoop` (sqlite-cost-model.ts:19).
#[derive(Clone, Debug)]
pub struct ScanstatusLoop {
    pub select_id: i32,
    pub parent_id: i32,
    /// Estimated rows emitted per turn of the parent loop.
    pub est: f64,
    /// EXPLAIN text for this loop (b-tree vs list subquery detection).
    pub explain: String,
}

/// Prefix marking a probe failure caused by `sqlite3_interrupt` (watchdog
/// abort racing the probe), as opposed to a real planner/SQL bug.
pub const INTERRUPT_ERR_PREFIX: &str = "[sqlite-interrupt] ";

/// True when a cost-model error string was marked as an interrupt.
pub fn is_interrupt_error(e: &str) -> bool {
    e.starts_with(INTERRUPT_ERR_PREFIX)
}

/// Typed panic payload thrown by the cost-model closure when its probe was
/// interrupted. The closure signature cannot return a Result, so the interrupt
/// unwinds as a panic; `plan_ast` catches this payload and degrades to
/// planning without flips instead of tearing down the client group.
pub struct CostProbeInterrupted(pub String);

/// Prepare `sql` (without executing) and read all scanstatus loops.
/// Port of TS `getScanstatusLoops` (sqlite-cost-model.ts:139); like TS,
/// iterates idx until the API reports end-of-loops, with the COMPLEX flag so
/// sorter (ORDER BY) loops are included.
pub fn get_scanstatus_loops(
    conn: &rusqlite::Connection,
    sql: &str,
) -> Result<Vec<ScanstatusLoop>, String> {
    let c_sql = CString::new(sql).map_err(|_| format!("probe SQL contains NUL byte: {sql}"))?;
    let mut loops: Vec<ScanstatusLoop> = Vec::new();

    // SAFETY: `db` is the live connection handle owned by `conn` (kept alive
    // by the borrow for this whole scope); the statement is finalized before
    // returning on every path.
    unsafe {
        let db = conn.handle();
        let mut stmt: *mut rusqlite::ffi::sqlite3_stmt = std::ptr::null_mut();
        let rc = rusqlite::ffi::sqlite3_prepare_v2(
            db,
            c_sql.as_ptr(),
            -1,
            &mut stmt,
            std::ptr::null_mut(),
        );
        if rc != rusqlite::ffi::SQLITE_OK || stmt.is_null() {
            let msg = CStr::from_ptr(rusqlite::ffi::sqlite3_errmsg(db))
                .to_string_lossy()
                .into_owned();
            if !stmt.is_null() {
                rusqlite::ffi::sqlite3_finalize(stmt);
            }
            // SQLITE_INTERRUPT is not a planner bug: the watchdog's stuck-actor
            // abort flips sqlite3_interrupt on the shared snapshot connection,
            // and a probe racing that flag gets rejected. Mark it so callers
            // can degrade to planning without stats instead of tearing down.
            if rc == rusqlite::ffi::SQLITE_INTERRUPT {
                return Err(format!(
                    "{INTERRUPT_ERR_PREFIX}cost-model probe prepare failed ({msg}): {sql}"
                ));
            }
            return Err(format!("cost-model probe prepare failed ({msg}): {sql}"));
        }

        let mut idx: c_int = 0;
        loop {
            let mut select_id: c_int = 0;
            let rc = sqlite3_stmt_scanstatus_v2(
                stmt,
                idx,
                SQLITE_SCANSTAT_SELECTID,
                SQLITE_SCANSTAT_COMPLEX,
                &mut select_id as *mut c_int as *mut c_void,
            );
            if rc != 0 {
                break; // end of loops (TS: scanStatus returns undefined)
            }

            let mut parent_id: c_int = 0;
            sqlite3_stmt_scanstatus_v2(
                stmt,
                idx,
                SQLITE_SCANSTAT_PARENTID,
                SQLITE_SCANSTAT_COMPLEX,
                &mut parent_id as *mut c_int as *mut c_void,
            );

            let mut est: f64 = 0.0;
            sqlite3_stmt_scanstatus_v2(
                stmt,
                idx,
                SQLITE_SCANSTAT_EST,
                SQLITE_SCANSTAT_COMPLEX,
                &mut est as *mut f64 as *mut c_void,
            );

            let mut explain_ptr: *const c_char = std::ptr::null();
            sqlite3_stmt_scanstatus_v2(
                stmt,
                idx,
                SQLITE_SCANSTAT_EXPLAIN,
                SQLITE_SCANSTAT_COMPLEX,
                &mut explain_ptr as *mut *const c_char as *mut c_void,
            );
            let explain = if explain_ptr.is_null() {
                String::new()
            } else {
                CStr::from_ptr(explain_ptr).to_string_lossy().into_owned()
            };

            loops.push(ScanstatusLoop {
                select_id,
                parent_id,
                est,
                explain,
            });
            idx += 1;
        }

        rusqlite::ffi::sqlite3_finalize(stmt);
    }

    loops.sort_by_key(|l| l.select_id);
    Ok(loops)
}

// ---------------------------------------------------------------------------
// Cost estimation from scanstatus loops
// ---------------------------------------------------------------------------

/// Port of TS `estimateCost` (sqlite-cost-model.ts:173): the first top-level
/// (parentId=0) loop's EST is the row estimate; each subsequent top-level
/// ORDER BY loop adds a b-tree construction startup cost.
pub fn estimate_cost(scanstats: &[ScanstatusLoop]) -> (f64, f64) {
    let mut sorted: Vec<&ScanstatusLoop> = scanstats.iter().collect();
    sorted.sort_by_key(|l| l.select_id);

    let mut total_rows = 0.0;
    let mut total_cost = 0.0;
    let mut first_loop = true;
    for op in sorted.iter().filter(|l| l.parent_id == 0) {
        if first_loop {
            // First top-level op is the main scan and determines row count.
            total_rows = op.est;
            first_loop = false;
        } else if op.explain.contains("ORDER BY") {
            total_cost += btree_cost(total_rows);
        }
    }
    (total_rows, total_cost)
}

/// Port of TS `btreeCost` (sqlite-cost-model.ts:211).
pub fn btree_cost(rows: f64) -> f64 {
    // B-Tree construction is ~O(n log n); divided by 10 because sorting in
    // SQLite is ~10x faster than sorting materialized rows engine-side.
    (rows * rows.log2()) / 10.0
}

// ---------------------------------------------------------------------------
// Filter transformation
// ---------------------------------------------------------------------------

/// Port of TS `removeCorrelatedSubqueries` (sqlite-cost-model.ts:102): strip
/// correlated-subquery conditions (they can't run inside the probe SQL),
/// collapsing singleton AND/OR and returning `None` when nothing remains.
pub fn remove_correlated_subqueries(condition: &Condition) -> Option<Condition> {
    match condition {
        Condition::CorrelatedSubquery(_) => None,
        Condition::Simple(_) => Some(condition.clone()),
        Condition::And(conditions) => {
            let filtered: Vec<Condition> = conditions
                .iter()
                .filter_map(remove_correlated_subqueries)
                .collect();
            match filtered.len() {
                0 => None,
                1 => Some(filtered.into_iter().next().expect("len checked")),
                _ => Some(Condition::And(filtered)),
            }
        }
        Condition::Or(conditions) => {
            let filtered: Vec<Condition> = conditions
                .iter()
                .filter_map(remove_correlated_subqueries)
                .collect();
            match filtered.len() {
                0 => None,
                1 => Some(filtered.into_iter().next().expect("len checked")),
                _ => Some(Condition::Or(filtered)),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Probe SQL construction (buildSelectQuery + compileInline equivalent)
// ---------------------------------------------------------------------------

/// Render a bound parameter as an inline SQL literal.
/// Port of TS `inlineValue` (internal/sql-inline.ts): strings quoted with
/// doubled single-quotes, numbers as-is, booleans as 1/0, JSON as a quoted
/// string literal, NULL as NULL.
fn inline_param(p: &SqlParam) -> String {
    match p {
        SqlParam::Null => "NULL".to_string(),
        SqlParam::Int(n) => n.to_string(),
        SqlParam::F64(n) => format!("{n}"),
        SqlParam::Text(s) => format!("'{}'", s.replace('\'', "''")),
        SqlParam::Bool(b) => if *b { "1" } else { "0" }.to_string(),
    }
}

/// Replace each `?` placeholder in `text` with the corresponding inline
/// literal. Placeholder/param counts must agree (they do by construction:
/// `condition_to_sql` emits exactly one param per `?`).
fn inline_sql(text: &str, params: &[SqlParam]) -> String {
    let mut out = String::with_capacity(text.len() + params.len() * 8);
    let mut params_iter = params.iter();
    for ch in text.chars() {
        if ch == '?' {
            let p = params_iter
                .next()
                .unwrap_or_else(|| panic!("cost-model inline: more '?' than params in: {text}"));
            out.push_str(&inline_param(p));
        } else {
            out.push(ch);
        }
    }
    assert!(
        params_iter.next().is_none(),
        "cost-model inline: unused params for: {text}"
    );
    out
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build the probe SQL exactly like TS `buildSelectQuery(...)` +
/// `compileInline`:
///
/// `SELECT "c1","c2" FROM "t" WHERE "ck" = ? AND <inlined filters> ORDER BY …`
///
/// - Constraint columns keep unbound `?` placeholders (their runtime values
///   are unknown at plan time; TS inlines `undefined` as a placeholder) —
///   EXCEPT boolean-typed columns, where TS `toSQLiteType(undefined,
///   'boolean')` evaluates to `0` and the literal 0 is inlined. Ported
///   verbatim for decision parity.
/// - Filter values are inlined so SQLite's planner sees real values (stat4).
fn build_probe_sql(
    table_name: &str,
    columns: &[(String, ColumnType)],
    constraint: Option<&PlannerConstraint>,
    filters: Option<&Condition>,
    sort: &[(String, String)],
) -> String {
    let mut sql = String::from("SELECT ");
    for (i, (col, _)) in columns.iter().enumerate() {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&quote_ident(col));
    }
    sql.push_str(" FROM ");
    sql.push_str(&quote_ident(table_name));

    let mut where_parts: Vec<String> = Vec::new();

    if let Some(constraint) = constraint {
        let types: HashMap<&str, &ColumnType> =
            columns.iter().map(|(c, t)| (c.as_str(), t)).collect();
        for (key, value) in constraint {
            let rendered = match value {
                // Planner constraints carry no value (TS `Record<string,
                // undefined>`): placeholder, except the boolean quirk above.
                None => match types.get(key.as_str()) {
                    Some(ColumnType::Boolean { .. }) => "0".to_string(),
                    _ => "?".to_string(),
                },
                // Defensive: a valued constraint inlines like TS would.
                Some(v) => inline_param(&SqlParam::from(v)),
            };
            where_parts.push(format!("{} = {}", quote_ident(key), rendered));
        }
    }

    if let Some(filters) = filters {
        let (filter_sql, filter_params) = condition_to_sql(filters);
        if !filter_sql.is_empty() {
            where_parts.push(inline_sql(&filter_sql, &filter_params));
        }
    }

    if !where_parts.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_parts.join(" AND "));
    }

    if !sort.is_empty() {
        sql.push_str(" ORDER BY ");
        for (i, (col, dir)) in sort.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&quote_ident(col));
            sql.push(' ');
            sql.push_str(dir);
        }
    }

    sql
}

// ---------------------------------------------------------------------------
// The model
// ---------------------------------------------------------------------------

/// Create a SQLite-scanstatus-based cost model over the given (snapshot)
/// connection. Port of TS `createSQLiteCostModel` (sqlite-cost-model.ts:39);
/// the returned closure matches the planner's `ConnectionCostModel` contract
/// exactly like TS: `(table, sort, filters, constraint) → {startupCost, rows,
/// fanout}`.
///
/// `table_specs` maps table → visible (zql) columns with their types — the
/// analog of TS `tableSpecs.get(t).zqlSpec`. Column order in the probe SELECT
/// list is normalized (sorted) for determinism; it does not affect the plan.
///
/// Errors when the linked SQLite lacks `SQLITE_ENABLE_STMT_SCANSTATUS` —
/// callers must fail loudly (or fall back explicitly), never estimate blind.
/// Pre-sorted table specs — the invariant part of the cost model. Building this
/// (a clone of every visible column of every syncable table) depends on neither
/// the query nor the snapshot version; it changes only with the schema (engine
/// init/reset). Callers planning many queries against one schema should build it
/// once via [`prepare_table_specs`] and reuse it through
/// [`create_sqlite_cost_model_prepared`] — rebuilding it per `addQuery` is pure
/// allocator churn (freed each call, but under the non-compacting system
/// allocator it ratchets resident set upward as cold fragmented pages).
pub type PreparedTableSpecs = HashMap<String, Vec<(String, ColumnType)>>;

/// Sort each table's columns for deterministic probe SQL. Extracted from
/// `create_sqlite_cost_model` so it can be computed once and cached.
pub fn prepare_table_specs(
    table_specs: HashMap<String, HashMap<String, ColumnType>>,
) -> PreparedTableSpecs {
    table_specs
        .into_iter()
        .map(|(table, cols)| {
            let mut cols: Vec<(String, ColumnType)> = cols.into_iter().collect();
            cols.sort_by(|a, b| a.0.cmp(&b.0));
            (table, cols)
        })
        .collect()
}

pub fn create_sqlite_cost_model(
    conn: Rc<RefCell<rusqlite::Connection>>,
    table_specs: HashMap<String, HashMap<String, ColumnType>>,
) -> Result<ConnectionCostModel, String> {
    create_sqlite_cost_model_prepared(conn, Rc::new(prepare_table_specs(table_specs)))
}

/// Like [`create_sqlite_cost_model`] but takes already-prepared specs shared by
/// `Rc`, so the engine builds them once per schema and reuses them across every
/// `plan_ast`. This is the hot path in prod (planner on): avoids rebuilding the
/// full column map on each query.
pub fn create_sqlite_cost_model_prepared(
    conn: Rc<RefCell<rusqlite::Connection>>,
    specs: Rc<PreparedTableSpecs>,
) -> Result<ConnectionCostModel, String> {
    if !scanstatus_available() {
        return Err(
            "SQLite was compiled without SQLITE_ENABLE_STMT_SCANSTATUS; the scanstatus \
             cost model cannot run (set RUST_IVM_PLANNER_COST_MODEL=count to use the \
             row-count fallback model)"
                .to_string(),
        );
    }

    let fanout_estimator = Rc::new(SQLiteStatFanout::new(conn.clone()));

    // WEAK capture: the returned model is cached on Engine.cached_cost_model for the engine's
    // life. Moving a STRONG `conn` into this closure would keep the snapshot
    // connection's Rc::strong_count > 1 at Snapshot::drop, skipping the explicit
    // close and leaking the connection (schema/stat4/statement-cache =
    // sqlite3MemMalloc). The probe only runs during planning, when the
    // snapshotter holds the conn strong, so upgrade() cannot fail in practice.
    let conn_weak = Rc::downgrade(&conn);

    Ok(Rc::new(
        move |table_name: &str,
              sort: &[(String, String)],
              filters: Option<&Condition>,
              constraint: Option<&PlannerConstraint>|
              -> CostModelCost {
            // Strip correlated subqueries — the probe can't run them; the
            // estimate is conservative without them (TS comment).
            let no_subquery_filters = filters.and_then(remove_correlated_subqueries);

            let columns = specs
                .get(table_name)
                .unwrap_or_else(|| panic!("cost model: no table spec for '{table_name}'"));

            let sql = build_probe_sql(
                table_name,
                columns,
                constraint,
                no_subquery_filters.as_ref(),
                sort,
            );

            // A dead upgrade cannot happen while planning (the snapshotter
            // holds the conn strong for the duration of the single-threaded
            // actor call). If it EVER does, degrade exactly like a watchdog
            // interrupt — unwind with the typed payload so `plan_ast` returns
            // "no flips" instead of tearing down the client group.
            let conn_rc = match conn_weak.upgrade() {
                Some(c) => c,
                None => std::panic::panic_any(CostProbeInterrupted(
                    "cost-model probe: snapshot connection dropped while planning; \
                     degrading to planning without flips"
                        .to_string(),
                )),
            };
            let loops = get_scanstatus_loops(&conn_rc.borrow(), &sql).unwrap_or_else(|e| {
                if is_interrupt_error(&e) {
                    // Watchdog interrupt racing the probe — unwind with a typed
                    // payload so plan_ast degrades to no-flips (see
                    // CostProbeInterrupted) instead of tearing down the CG.
                    std::panic::panic_any(CostProbeInterrupted(e));
                }
                panic!("{e}")
            });

            // Scanstatus should always be available — parity with TS assert.
            assert!(
                !loops.is_empty(),
                "Expected scanstatus to return at least one loop for query: {sql}"
            );

            let (rows, startup_cost) = estimate_cost(&loops);

            let estimator = fanout_estimator.clone();
            let table = table_name.to_string();
            CostModelCost {
                startup_cost,
                rows,
                fanout: Rc::new(move |cols: &[String]| {
                    let r = estimator.get_fanout(&table, cols);
                    FanoutEst {
                        fanout: r.fanout,
                        confidence: match r.confidence {
                            FanoutConfidence::High => Confidence::High,
                            FanoutConfidence::Med => Confidence::Med,
                            FanoutConfidence::None => Confidence::None,
                        },
                    }
                }),
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::ast::{SimpleCondition, ValuePosition};
    use crate::ivm::data::Value;

    fn simple(col: &str, op: &str, v: Value) -> Condition {
        Condition::Simple(SimpleCondition {
            op: op.to_string(),
            left: ValuePosition::Column {
                name: col.to_string(),
            },
            right: ValuePosition::Literal { value: v },
        })
    }

    fn csq() -> Condition {
        // A minimal correlated subquery condition for strip tests.
        Condition::CorrelatedSubquery(crate::builder::ast::CorrelatedSubqueryCondition {
            related: crate::builder::ast::RelatedSubquery {
                subquery: Box::new(crate::builder::ast::Ast {
                    schema: None,
                    table: "child".into(),
                    alias: None,
                    where_clause: None,
                    related: vec![],
                    limit: None,
                    order_by: None,
                    start: None,
                }),
                relationship_name: "child".to_string(),
                parent_key: vec!["id".into()],
                child_key: vec!["pid".into()],
                hidden: false,
                system: None,
            },
            op: "EXISTS".to_string(),
            flip: None,
            scalar: false,
            plan_id: None,
        })
    }

    #[test]
    fn strip_collapses_like_typescript() {
        // Bare CSQ → None.
        assert!(remove_correlated_subqueries(&csq()).is_none());
        // AND(simple, csq) → collapses to the simple condition (no AND wrap).
        let s = simple("a", "=", Value::Str("x".into()));
        let collapsed =
            remove_correlated_subqueries(&Condition::And(vec![s.clone(), csq()])).unwrap();
        assert!(matches!(collapsed, Condition::Simple(_)));
        // OR(csq, csq) → None.
        assert!(remove_correlated_subqueries(&Condition::Or(vec![csq(), csq()])).is_none());
        // AND(simple, simple, csq) → AND of the two simples.
        let two = remove_correlated_subqueries(&Condition::And(vec![
            s.clone(),
            simple("b", "=", Value::F64(1.0)),
            csq(),
        ]))
        .unwrap();
        match two {
            Condition::And(v) => assert_eq!(v.len(), 2),
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn probe_sql_shapes() {
        let columns = vec![
            ("flag".to_string(), ColumnType::Boolean { optional: false }),
            ("id".to_string(), ColumnType::String { optional: false }),
            ("n".to_string(), ColumnType::Number { optional: false }),
        ];

        // Unconstrained, no filters, no sort.
        assert_eq!(
            build_probe_sql("t", &columns, None, None, &[]),
            r#"SELECT "flag","id","n" FROM "t""#
        );

        // Constraint: non-boolean columns keep `?`; boolean inlines 0 (the TS
        // toSQLiteType(undefined, 'boolean') quirk).
        let mut c: PlannerConstraint = Default::default();
        c.insert("id".to_string(), None);
        c.insert("flag".to_string(), None);
        // WHERE terms follow the constraint's INSERTION order (TS iterates
        // the Record with Object.entries) — id was inserted first. The old
        // expectation pinned BTreeMap's alphabetical re-sort (NEW-2 artifact).
        assert_eq!(
            build_probe_sql("t", &columns, Some(&c), None, &[]),
            r#"SELECT "flag","id","n" FROM "t" WHERE "id" = ? AND "flag" = 0"#
        );

        // Filters inlined (string escape), sort appended.
        let f = simple("id", "=", Value::Str("o'x".into()));
        assert_eq!(
            build_probe_sql(
                "t",
                &columns,
                None,
                Some(&f),
                &[("n".to_string(), "desc".to_string())]
            ),
            r#"SELECT "flag","id","n" FROM "t" WHERE "id" = 'o''x' ORDER BY "n" desc"#
        );
    }

    #[test]
    fn inline_matches_ts_inline_value() {
        assert_eq!(inline_param(&SqlParam::Null), "NULL");
        assert_eq!(inline_param(&SqlParam::Int(5)), "5");
        assert_eq!(inline_param(&SqlParam::F64(5.5)), "5.5");
        assert_eq!(inline_param(&SqlParam::Bool(true)), "1");
        assert_eq!(inline_param(&SqlParam::Bool(false)), "0");
        assert_eq!(inline_param(&SqlParam::Text("a'b".into())), "'a''b'");
    }

    #[test]
    fn btree_cost_matches_ts() {
        // rows * log2(rows) / 10
        assert!((btree_cost(1024.0) - 1024.0).abs() < 1e-9);
        assert_eq!(btree_cost(1.0), 0.0);
    }
}
