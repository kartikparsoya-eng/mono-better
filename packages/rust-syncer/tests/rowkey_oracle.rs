//! Row-key oracle — pins the exact table(s) whose stored CVR rowKey diverges
//! from the client's declared primary key.
//!
//! ## What bug this catches
//!
//! Sandbox incident (2026-08-22, rust image `rust-cvr-v1.0.0-478b00a`): the
//! client crash-looped on every poke with
//! `TypeError: Expected string, number or boolean. Got undefined` thrown from
//! `toPrimaryKeyString` (packages/zero-client/src/client/keys.ts). That error
//! means a rows-patch op carried a rowKey in which a **client primary-key
//! column was absent** (`value[pkCol] === undefined`).
//!
//! The stored rowKey is produced server-side by projecting rust's *chosen*
//! row-key columns out of each IVM row:
//!
//! ```text
//! sync_engine.rs:1541   rc.row_key copied verbatim into the CVR rows table
//! streamer/mod.rs:185   row_key = get_row_key(pk, node.row)
//! streamer/mod.rs:176   pk = primary_keys[table]  (== schema.primary_key)
//! replica_schema.rs:388 schema.primary_key = keyCmp[0] over candidate unique keys
//! ```
//!
//! The client then rebuilds the entity key by indexing that stored rowKey with
//! **its own declared `primaryKey`**. So the client crashes iff:
//!
//! ```text
//! ∃ col ∈ client.primaryKey  such that  col ∉ rust.chosen_primary_key
//! ```
//!
//! Crucially, `validate_client_schema` (replica_schema.rs:97) only checks that
//! the client PK is *some* replicated unique key — it never checks that the
//! client PK equals the `keyCmp[0]` key that is actually used to emit the
//! rowKey. A table can therefore pass validation and still poison the CVR. This
//! oracle closes exactly that gap by running the real production
//! `compute_zql_specs` and asserting the crash predicate per table.
//!
//! ## Running
//!
//! Both env vars are required; the test SKIPs (passes as a no-op) when either is
//! absent, matching the repo's env-gated integration-test convention.
//!
//! ```bash
//! # A copy of (or read-only handle to) the replica the syncer serves. Only the
//! # schema is read (sqlite_master + pragmas) — no table data is scanned, so a
//! # large replica is fine.
//! export TEST_REPLICA_DB=/path/to/replica.db
//! # The client-declared schema, in the same JSON shape validate_client_schema
//! # consumes: {"tables": {"<name>": {"columns": {...}, "primaryKey": [..]}}}.
//! # This is the schema the client sends on connect.
//! export TEST_CLIENT_SCHEMA=/path/to/client-schema.json
//!
//! # rust-syncer CI feature/env conventions (see verify-ci-locally memory):
//! unset SQLITE3_STATIC SQLITE3_LIB_DIR SQLITE3_INCLUDE_DIR PKG_CONFIG_LIBDIR
//! cargo test --locked --no-default-features --test rowkey_oracle -- --nocapture
//! ```

use std::collections::BTreeSet;

use rust_syncer::db::lite_tables::{compute_table_specs_from_path, validate_client_schema};
use rust_syncer::services::view_syncer::pipeline_driver::IvmTableSpec;

/// The client PK columns declared for a table, keyed by table name.
fn parse_client_pks(client_schema: &serde_json::Value) -> Vec<(String, Vec<String>)> {
    let tables = client_schema
        .get("tables")
        .and_then(serde_json::Value::as_object)
        .expect("client schema must have a `tables` object");
    let mut out: Vec<(String, Vec<String>)> = tables
        .iter()
        .map(|(name, t)| {
            let pk = t
                .get("primaryKey")
                .and_then(serde_json::Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            (name.clone(), pk)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

#[test]
fn rowkey_oracle_pins_diverging_table() {
    let (Ok(replica), Ok(schema_path)) = (
        std::env::var("TEST_REPLICA_DB"),
        std::env::var("TEST_CLIENT_SCHEMA"),
    ) else {
        eprintln!(
            "SKIP rowkey_oracle: set TEST_REPLICA_DB and TEST_CLIENT_SCHEMA to run \
             (see the module doc-comment)."
        );
        return;
    };

    // Real production key selection over the real replica schema.
    let specs: Vec<IvmTableSpec> =
        compute_table_specs_from_path(&replica).expect("compute_table_specs_from_path failed");
    let spec_by_name: std::collections::HashMap<&str, &IvmTableSpec> =
        specs.iter().map(|s| (s.table.as_str(), s)).collect();

    let client_schema: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&schema_path).expect("read TEST_CLIENT_SCHEMA"),
    )
    .expect("parse TEST_CLIENT_SCHEMA as JSON");
    let client_pks = parse_client_pks(&client_schema);

    // Show what the existing validation says — it is expected to PASS even for
    // the poisoned table, which is the whole point (the validation gap).
    match validate_client_schema(&client_schema, &specs) {
        Ok(()) => eprintln!("validate_client_schema: OK (as expected — it does not catch this)"),
        Err(e) => eprintln!("validate_client_schema: reported:\n{e}"),
    }

    let mut crashers: Vec<String> = Vec::new(); // client PK col missing from rust key → client crash
    let mut divergences: Vec<String> = Vec::new(); // set differs but not a crash (rust key ⊇ client PK)

    eprintln!("\n== per-table row-key report ==");
    for (table, client_pk) in &client_pks {
        let Some(spec) = spec_by_name.get(table.as_str()) else {
            eprintln!("  {table}: (client table not syncable / not in replica specs — skipped)");
            continue;
        };
        let rust_key: BTreeSet<&str> = spec.primary_key.iter().map(String::as_str).collect();
        let client_set: BTreeSet<&str> = client_pk.iter().map(String::as_str).collect();
        let all_unique = spec
            .unique_keys
            .as_ref()
            .map(|ks| {
                ks.iter()
                    .map(|k| format!("[{}]", k.join(",")))
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .unwrap_or_else(|| "<none>".to_string());

        // The exact crash predicate: a client PK column absent from rust's key.
        let missing: Vec<&str> = client_pk
            .iter()
            .map(String::as_str)
            .filter(|c| !rust_key.contains(c))
            .collect();

        let status = if !missing.is_empty() {
            crashers.push(format!(
                "{table}: client PK [{}] but rust rowKey is keyed by [{}] — missing column(s) {:?} \
                 → client toPrimaryKeyString gets `undefined`. candidate unique keys: {all_unique}",
                client_pk.join(","),
                spec.primary_key.join(","),
                missing,
            ));
            "CRASH"
        } else if client_set != rust_key {
            divergences.push(format!(
                "{table}: client PK [{}] vs rust chosen key [{}] (rust key is a superset — no crash, \
                 but diverges from TS). candidate unique keys: {all_unique}",
                client_pk.join(","),
                spec.primary_key.join(","),
            ));
            "DIVERGE"
        } else {
            "ok"
        };
        eprintln!(
            "  {table}: client=[{}] rust=[{}] {status}",
            client_pk.join(","),
            spec.primary_key.join(","),
        );
    }

    if !divergences.is_empty() {
        eprintln!("\n== divergences (non-crashing) ==");
        for d in &divergences {
            eprintln!("  - {d}");
        }
    }

    assert!(
        crashers.is_empty(),
        "\n\n>>> ROW-KEY POISON: {} table(s) whose stored CVR rowKey omits a client \
         primary-key column (this is the `Got undefined` crash source):\n{}\n\n\
         Fix: make rust's chosen row key for these tables match the client's declared \
         primary key (align compute_zql_specs' candidate-key enumeration / keyCmp[0] \
         with TS computeZqlSpecs), and add a write-time assertion in get_row_key that the \
         emitted rowKey contains the full client PK.\n",
        crashers.len(),
        crashers
            .iter()
            .map(|c| format!("  - {c}"))
            .collect::<Vec<_>>()
            .join("\n"),
    );

    eprintln!(
        "\nrowkey_oracle: no crashing table found across {} client tables ({} non-crashing \
         divergence(s)).",
        client_pks.len(),
        divergences.len(),
    );
}
