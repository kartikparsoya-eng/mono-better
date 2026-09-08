//! ART-class guard (self-contained, runs in normal CI — no replica/PG needed):
//! for EVERY table whose client-declared primary key differs from the IVM
//! `keyCmp[0]` (shortest replicated unique key), the emitted client-facing
//! rowKey MUST be keyed by the CLIENT PK.
//!
//! This generalizes `rowkey_repro` across several divergent schema SHAPES —
//! the dimension xyne-art missed (its corpus only used `id`-keyed tables where
//! client PK == keyCmp[0], so the divergence could never trigger). The bug
//! class is schema-shape-triggered, so we vary the shape, not the data.
//!
//! Each case: build a SQLite table, run the REAL `compute_zql_specs`, assert
//! keyCmp[0] != client PK (so the case is actually exercising the divergence),
//! install the client PK (as `config_and_hydrate` does from the client schema),
//! hydrate through the real pipeline, and assert the emitted rowKey columns ==
//! the client PK.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;
use rust_syncer::services::view_syncer::pipeline_driver::IvmPipelines;

type SharedConnAlias = Rc<RefCell<Connection>>;

struct Shape {
    name: &'static str,
    /// Table DDL + a single INSERT of one row.
    ddl: &'static str,
    table: &'static str,
    /// The IVM keyCmp[0] we expect `compute_zql_specs` to pick (the WRONG key
    /// for client-facing emission) — asserted as a precondition so the case is
    /// meaningful.
    expect_keycmp: &'static [&'static str],
    /// The client-declared primary key (what emission MUST use).
    client_pk: &'static [&'static str],
}

/// Hydrate `table` with `client_pk` installed and return the sorted columns of
/// the emitted rowKey.
fn emitted_rowkey_cols(shape: &Shape) -> Vec<String> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(shape.ddl).unwrap();

    let specs =
        rust_syncer::compute_zql_specs(&conn, &rust_syncer::ZqlSpecOptions::default(), None)
            .unwrap();
    let spec = specs
        .iter()
        .find(|s| s.table == shape.table)
        .unwrap_or_else(|| panic!("{}: table must be syncable", shape.name));
    let expect_keycmp: Vec<String> = shape.expect_keycmp.iter().map(|s| s.to_string()).collect();
    assert_eq!(
        spec.primary_key, expect_keycmp,
        "{}: precondition — keyCmp[0] must be the (wrong) surrogate/alternate key",
        shape.name
    );
    let client_pk: Vec<String> = shape.client_pk.iter().map(|s| s.to_string()).collect();
    assert_ne!(
        spec.primary_key, client_pk,
        "{}: case is only meaningful when client PK != keyCmp[0]",
        shape.name
    );

    let shared_conn: SharedConnAlias = Rc::new(RefCell::new(conn));
    let mut pipelines = IvmPipelines::new();
    pipelines.init_from_connection(specs, shared_conn).unwrap();
    pipelines.set_client_primary_keys(HashMap::from([(shape.table.to_string(), client_pk)]));

    let mut cols: Vec<String> = Vec::new();
    {
        let timer = Rc::new(rust_syncer::services::view_syncer::view_syncer::TimeSliceTimer::new());
        timer.start_without_yielding();
        let mut changes = pipelines
            .hydrate(
                &[(
                    "q1".to_string(),
                    serde_json::json!({ "table": shape.table }).to_string(),
                )],
                timer,
            )
            .unwrap();
        for item in changes.by_ref() {
            if let rust_ivm::ivm::stream::StreamItem::Data(rc) = item
                && rc.table == shape.table
                && !rc.is_hidden
            {
                let mut c: Vec<String> = rc.row_key.keys().cloned().collect();
                c.sort();
                cols = c;
            }
        }
        changes.finish().unwrap();
    }
    assert!(!cols.is_empty(), "{}: expected a row change", shape.name);
    cols
}

#[test]
fn emitted_rowkey_is_always_the_client_pk_across_shapes() {
    let shapes = [
        // 1. Junction table: compound client PK + a SHORTER surrogate unique
        //    index → keyCmp picks the 1-col surrogate.
        Shape {
            name: "compound_pk_with_shorter_surrogate",
            ddl: r#"
                CREATE TABLE "channel_user_status" (
                    "channelId"  "text|NOT_NULL",
                    "userId"     "text|NOT_NULL",
                    "id"         "text|NOT_NULL",
                    "_0_version" "text"
                );
                CREATE UNIQUE INDEX "a_pk" ON "channel_user_status" ("channelId", "userId");
                CREATE UNIQUE INDEX "b_id" ON "channel_user_status" ("id");
                INSERT INTO "channel_user_status" VALUES ('c1', 'u1', 'cus1', '01');
            "#,
            table: "channel_user_status",
            expect_keycmp: &["id"],
            client_pk: &["channelId", "userId"],
        },
        // 2. Same column count (both 1-col), but an alternate unique column sorts
        //    lexicographically BEFORE the client PK → keyCmp tiebreak picks it.
        //    ("email" < "id"), so keyCmp=[email] while the client PK is [id].
        Shape {
            name: "single_pk_with_lexicographically_smaller_alt_unique",
            ddl: r#"
                CREATE TABLE "account" (
                    "id"         "text|NOT_NULL",
                    "email"      "text|NOT_NULL",
                    "_0_version" "text"
                );
                CREATE UNIQUE INDEX "a_email" ON "account" ("email");
                CREATE UNIQUE INDEX "b_id" ON "account" ("id");
                INSERT INTO "account" VALUES ('acc1', 'a@x.com', '01');
            "#,
            table: "account",
            expect_keycmp: &["email"],
            client_pk: &["id"],
        },
        // 3. Compound client PK, plus a shorter alternate compound unique key
        //    that wins keyCmp on column count.
        Shape {
            name: "compound_pk_with_shorter_compound_alt",
            ddl: r#"
                CREATE TABLE "membership" (
                    "orgId"      "text|NOT_NULL",
                    "userId"     "text|NOT_NULL",
                    "seat"       "text|NOT_NULL",
                    "_0_version" "text"
                );
                CREATE UNIQUE INDEX "a_pk" ON "membership" ("orgId", "userId", "seat");
                CREATE UNIQUE INDEX "b_alt" ON "membership" ("orgId", "seat");
                INSERT INTO "membership" VALUES ('o1', 'u1', 's1', '01');
            "#,
            table: "membership",
            expect_keycmp: &["orgId", "seat"],
            client_pk: &["orgId", "userId", "seat"],
        },
    ];

    for shape in &shapes {
        let cols = emitted_rowkey_cols(shape);
        let mut expected: Vec<String> = shape.client_pk.iter().map(|s| s.to_string()).collect();
        expected.sort();
        assert_eq!(
            cols, expected,
            "{}: emitted rowKey must be keyed by the client PK {:?}, got {:?}",
            shape.name, expected, cols
        );
    }
}
