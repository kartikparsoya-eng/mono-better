//! REPRO for the sandbox row-key poison (2026-08-22).
//!
//! Symptom: client crash-loops on every poke with
//! `TypeError: Expected string, number or boolean. Got undefined` from
//! `toPrimaryKeyString` — a rows-patch op whose rowKey is missing a client
//! primary-key column.
//!
//! Root cause (found by static TS-vs-rust diff of the row-key build path):
//! - TS `pipeline-driver.ts` builds the client-facing row key in TWO passes:
//!   `primaryKeys.set(table, spec.tableSpec.primaryKey)` (computeZqlSpecs
//!   `keyCmp[0]`) then `buildPrimaryKeys(clientSchema, primaryKeys)` which
//!   OVERRIDES each table with the CLIENT-declared primary key. So TS always
//!   keys the rowKey by the client PK.
//! - Rust `pipeline_driver.rs::build_engine` does ONE pass: `primary_keys =
//!   spec.primary_key` (= `keyCmp[0]`) and never applies the client schema. The
//!   streamer's `get_row_key` therefore emits the rowKey keyed by `keyCmp[0]`.
//!
//! For any table whose `keyCmp[0]` (shortest / lexicographically-first
//! replicated unique key) differs from the client's declared PK, rust stores
//! the wrong row-key columns → the client reads `value[pkCol] === undefined` →
//! the crash. This exercises the REAL production path (`compute_zql_specs` +
//! `IvmPipelines::init_from_connection` + `hydrate`) — no mocks.
//!
//! Regression gate (post-fix): with the client PK installed via
//! `set_client_primary_keys` (as `config_and_hydrate` now does from the client
//! schema), the emitted rowKey is keyed by the CLIENT PK. Without it, emission
//! falls back to the IVM `keyCmp[0]` (unchanged prior behavior). This exercises
//! the real path (`compute_zql_specs` → `init_from_connection` → `hydrate`).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rusqlite::Connection;
use rust_syncer::pipeline_driver::IvmPipelines;

type SharedConnAlias = Rc<RefCell<Connection>>;

/// Hydrate `channel_user_status` and return the (sorted) columns of the emitted
/// rowKey, optionally installing client-declared primary keys first.
fn emitted_rowkey_cols(client_pks: Option<HashMap<String, Vec<String>>>) -> Vec<String> {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE "channel_user_status" (
            "channelId"  "text|NOT_NULL",
            "userId"     "text|NOT_NULL",
            "id"         "text|NOT_NULL",
            "_0_version" "text"
        );
        CREATE UNIQUE INDEX "cus_client_pk" ON "channel_user_status" ("channelId", "userId");
        CREATE UNIQUE INDEX "cus_surrogate" ON "channel_user_status" ("id");
        INSERT INTO "channel_user_status" ("channelId", "userId", "id", "_0_version")
            VALUES ('c1', 'u1', 'cus1', '01');
        "#,
    )
    .unwrap();

    let specs = rust_syncer::compute_zql_specs(&conn).unwrap();
    // Precondition: keyCmp[0] is the shortest unique key (surrogate `id`).
    let cus = specs
        .iter()
        .find(|s| s.table == "channel_user_status")
        .expect("channel_user_status must be syncable");
    assert_eq!(cus.primary_key, vec!["id".to_string()]);

    let shared_conn: SharedConnAlias = Rc::new(RefCell::new(conn));
    let mut pipelines = IvmPipelines::new();
    pipelines.init_from_connection(specs, shared_conn).unwrap();
    if let Some(pks) = client_pks {
        pipelines.set_client_primary_keys(pks);
    }

    let mut cols: Vec<String> = Vec::new();
    pipelines
        .hydrate(
            &[(
                "q1".to_string(),
                serde_json::json!({"table": "channel_user_status"}).to_string(),
            )],
            |rc| {
                if rc.table == "channel_user_status" && !rc.is_hidden {
                    let mut c: Vec<String> = rc.row_key.keys().cloned().collect();
                    c.sort();
                    cols = c;
                }
            },
        )
        .unwrap();
    assert!(
        !cols.is_empty(),
        "expected a row change for channel_user_status"
    );
    cols
}

/// THE FIX: with the client PK installed, the emitted rowKey carries exactly the
/// client's declared compound PK [channelId, userId] — so the client's
/// `toPrimaryKeyString` finds every PK column and never throws "Got undefined".
#[test]
fn rowkey_uses_client_pk_when_installed() {
    let client_pks = HashMap::from([(
        "channel_user_status".to_string(),
        vec!["channelId".to_string(), "userId".to_string()],
    )]);
    let cols = emitted_rowkey_cols(Some(client_pks));
    assert_eq!(
        cols,
        vec!["channelId".to_string(), "userId".to_string()],
        "FIXED: rowKey must be keyed by the client's declared PK"
    );
    assert!(
        !cols.contains(&"id".to_string()),
        "FIXED: the surrogate keyCmp[0] `id` must NOT be the client-facing rowKey"
    );
}

/// Without a client PK (e.g. system tables / no client schema), emission is
/// unchanged: it uses the IVM `keyCmp[0]`. This documents the fallback and
/// reproduces the original bug shape when no client PK is provided.
#[test]
fn rowkey_falls_back_to_keycmp_best_without_client_pk() {
    let cols = emitted_rowkey_cols(None);
    assert_eq!(
        cols,
        vec!["id".to_string()],
        "without a client PK, emission uses the IVM keyCmp[0] surrogate `id` \
         (this is the shape that crashed the client before the fix)"
    );
}
