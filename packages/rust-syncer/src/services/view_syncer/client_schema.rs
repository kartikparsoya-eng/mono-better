//! Port of `zero-cache/src/services/view-syncer/client-schema.ts`.

use crate::db::lite_tables::lite_type_to_zql_value_type;
use crate::db::specs::LiteTableSpec;
use crate::protocol::{ErrorBody, ErrorKind};
use crate::services::view_syncer::pipeline_driver::IvmTableSpec;
use rust_cvr::shards::{ShardID, app_schema, upstream_schema};
use rust_cvr::shared::string_compare::string_compare;
use rust_ivm::snapshotter::ZERO_VERSION_COLUMN_NAME;
use std::collections::{HashMap, HashSet};

/// Port of TS `checkClientSchema(shardID, clientSchema, tableSpecs, fullTables)`
/// (client-schema.ts:15-155). `table_specs` are the syncable tables
/// (`Map<string, LiteAndZqlSpec>`: `zqlSpec` = `columns`, `tableSpec.
/// allPotentialPrimaryKeys` = `all_potential_primary_keys`); `full_tables` is
/// every replica table as `listTables` reports it. A TS `throw new
/// ProtocolError(...)` is an `Err(ErrorBody)` — `Internal` when nothing has
/// been synced, `SchemaVersionNotSupported` with the `\n`-joined error list
/// otherwise. Every message is byte-identical to TS (pinned by
/// `client-schema-fixture.json`).
// The Err IS the wire body TS throws (ProtocolError.errorBody); boxing it would
// only move the clone to the close_with_error call site.
#[allow(clippy::result_large_err)]
pub fn check_client_schema(
    shard_id: &ShardID,
    client_schema: &serde_json::Value,
    table_specs: &[IvmTableSpec],
    full_tables: &[LiteTableSpec],
) -> Result<(), ErrorBody> {
    if full_tables.is_empty() {
        return Err(ErrorBody::internal(
            "No tables have been synced from upstream. Please check that the ZERO_UPSTREAM_DB has been properly set.",
        ));
    }
    let table_specs: HashMap<&str, &IvmTableSpec> =
        table_specs.iter().map(|s| (s.table.as_str(), s)).collect();
    let full_tables: HashMap<&str, &LiteTableSpec> =
        full_tables.iter().map(|t| (t.name.as_str(), t)).collect();
    let empty = serde_json::Map::new();
    let client_tables_obj = client_schema
        .get("tables")
        .and_then(serde_json::Value::as_object)
        .unwrap_or(&empty);
    let mut errors: Vec<String> = Vec::new();
    let client_tables: HashSet<&str> = client_tables_obj.keys().map(String::as_str).collect();
    // TS `toSorted(difference(clientTables, tableSpecs))` — default sort = JS string order.
    let mut missing_tables: Vec<&str> = client_tables
        .iter()
        .copied()
        .filter(|t| !table_specs.contains_key(t))
        .collect();
    missing_tables.sort_by(|a, b| string_compare(a, b));
    for missing in missing_tables {
        if let Some(full_table) = full_tables.get(missing) {
            let data_type = |col: &str| full_table.column(col).map(|c| c.data_type.as_str());
            let unsupported_primary_key_columns: Vec<&String> = full_table
                .primary_key
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .filter(|col| lite_type_to_zql_value_type(data_type(col).unwrap_or("")).is_none())
                .collect();
            if !unsupported_primary_key_columns.is_empty() {
                errors.push(format!(
                    "The \"{missing}\" table's primary key contains unsupported columns: {}. These columns must use Zero-supported data types to sync the table to the client.",
                    unsupported_primary_key_columns
                        .iter()
                        .map(|col| format!("\"{col}\" ({})", data_type(col).unwrap_or("unknown")))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                continue;
            }
            errors.push(format!(
                "The \"{missing}\" table is missing a primary key or non-null unique index and thus cannot be synced to the client"
            ));
        } else {
            let app = format!("{}.", app_schema(shard_id));
            let shard = format!("{}.", upstream_schema(shard_id));
            let mut synced: Vec<&str> = table_specs
                .keys()
                .copied()
                .filter(|t| !t.starts_with(&app) && !t.starts_with(&shard))
                .collect();
            synced.sort_by(|a, b| string_compare(a, b));
            let synced_tables = synced
                .iter()
                .map(|t| format!("\"{t}\""))
                .collect::<Vec<_>>()
                .join(",");
            let schema_tip = if missing.contains('.') && !synced_tables.contains('.') {
                " Note that zero does not sync tables from non-public schemas by default. Make sure you have defined a custom ZERO_APP_PUBLICATION to sync tables from non-public schemas."
            } else {
                ""
            };
            errors.push(format!(
                "The \"{missing}\" table does not exist or is not one of the replicated tables: {synced_tables}.{schema_tip}"
            ));
        }
    }
    // TS `toSorted(intersection(tableSpecs, clientTables))`.
    let mut tables: Vec<&str> = table_specs
        .keys()
        .copied()
        .filter(|t| client_tables.contains(t))
        .collect();
    tables.sort_by(|a, b| string_compare(a, b));
    for table in tables {
        let client_spec = &client_tables_obj[table];
        let server_spec = table_specs[table]; // guaranteed by intersection
        let full_spec = full_tables
            .get(table)
            .unwrap_or_else(|| panic!("must: fullTables.get({table:?})"));
        let client_columns = client_spec
            .get("columns")
            .and_then(serde_json::Value::as_object)
            .unwrap_or(&empty);
        let synced_columns: HashSet<&str> =
            server_spec.columns.keys().map(String::as_str).collect();
        // TS `toSorted(difference(clientColumns, syncedColumns))`.
        let mut missing_columns: Vec<&str> = client_columns
            .keys()
            .map(String::as_str)
            .filter(|c| !synced_columns.contains(c))
            .collect();
        missing_columns.sort_by(|a, b| string_compare(a, b));
        for missing in missing_columns {
            if let Some(full_column) = full_spec.column(missing) {
                errors.push(format!(
                    "The \"{table}\".\"{missing}\" column cannot be synced because it is of an unsupported data type \"{}\"",
                    full_column.data_type
                ));
            } else {
                let mut columns: Vec<&str> = synced_columns
                    .iter()
                    .copied()
                    .filter(|c| *c != ZERO_VERSION_COLUMN_NAME)
                    .collect();
                columns.sort_by(|a, b| string_compare(a, b));
                let columns = columns
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(",");
                errors.push(format!(
                    "The \"{table}\".\"{missing}\" column does not exist or is not one of the replicated columns: {columns}."
                ));
            }
        }
        // TS `intersection(clientColumns, syncedColumns)` iterates in
        // clientColumns (client object insertion) order — NOT sorted.
        for (column, client_column) in client_columns {
            let Some(server_column) = server_spec.columns.get(column) else {
                continue;
            };
            let client_type = client_column
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let server_type = server_column.r#type.as_str();
            if client_type != server_type {
                errors.push(format!(
                    "The \"{table}\".\"{column}\" column's upstream type \"{server_type}\" does not match the client type \"{client_type}\""
                ));
            }
        }
        let client_primary_key: Option<Vec<&str>> = client_spec
            .get("primaryKey")
            .and_then(serde_json::Value::as_array)
            .map(|keys| keys.iter().filter_map(serde_json::Value::as_str).collect());
        match client_primary_key {
            None => errors.push(format!(
                "The \"{table}\" table's client schema does not specify a primary key."
            )),
            Some(primary_key) => {
                let client_key: HashSet<&str> = primary_key.iter().copied().collect();
                let matches = server_spec.all_potential_primary_keys.iter().any(|key| {
                    let key: HashSet<&str> = key.iter().map(String::as_str).collect();
                    key == client_key
                });
                if !matches {
                    errors.push(format!(
                        "The \"{table}\" table's primaryKey <{}> is not associated with a non-null unique index.",
                        primary_key.join(",")
                    ));
                }
            }
        }
    }
    if !errors.is_empty() {
        return Err(ErrorBody::basic(
            ErrorKind::SchemaVersionNotSupported,
            errors.join("\n"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::specs::LiteColumnSpec;
    use crate::services::view_syncer::pipeline_driver::IvmColumnSchema;
    use serde_json::Value;

    fn table_specs_from(fixture: &Value) -> Vec<IvmTableSpec> {
        fixture
            .as_object()
            .unwrap()
            .iter()
            .map(|(name, spec)| {
                let columns = spec["zqlSpec"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(col, v)| {
                        (
                            col.clone(),
                            IvmColumnSchema {
                                r#type: v["type"].as_str().unwrap().to_string(),
                                optional: false,
                            },
                        )
                    })
                    .collect();
                let keys: Vec<Vec<String>> = spec["tableSpec"]["allPotentialPrimaryKeys"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|k| {
                        k.as_array()
                            .unwrap()
                            .iter()
                            .map(|c| c.as_str().unwrap().to_string())
                            .collect()
                    })
                    .collect();
                IvmTableSpec {
                    table: name.clone(),
                    columns,
                    column_order: Vec::new(),
                    primary_key: keys.first().cloned().unwrap_or_default(),
                    unique_keys: None,
                    all_potential_primary_keys: keys,
                    min_row_version: None,
                }
            })
            .collect()
    }

    fn full_tables_from(fixture: &Value) -> Vec<LiteTableSpec> {
        fixture
            .as_object()
            .unwrap()
            .iter()
            .map(|(name, t)| LiteTableSpec {
                name: name.clone(),
                columns: t["columns"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .enumerate()
                    .map(|(i, (col, v))| {
                        (
                            col.clone(),
                            LiteColumnSpec {
                                pos: i + 1,
                                data_type: v["dataType"].as_str().unwrap().to_string(),
                                not_null: false,
                            },
                        )
                    })
                    .collect(),
                primary_key: t
                    .get("primaryKey")
                    .and_then(Value::as_array)
                    .map(|pk| pk.iter().map(|c| c.as_str().unwrap().to_string()).collect()),
            })
            .collect()
    }

    /// Layer-2 differential: every case is the REAL TS `checkClientSchema`
    /// run over the same inputs (`generate-client-schema-fixture.mjs`); the
    /// thrown `ProtocolError`'s `{kind, message}` — or `null` — must match
    /// exactly (message text, error ORDER, sorted vs insertion iteration,
    /// the non-public-schema tip, the `_0_version` exclusion).
    #[test]
    fn check_client_schema_parity_against_ts() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/agentic/parity/client-schema-fixture.json"
        );
        let fixture: Value =
            serde_json::from_slice(&std::fs::read(path).expect("read fixture")).unwrap();
        let cases = fixture["cases"].as_array().unwrap();
        assert!(cases.len() >= 12);
        for case in cases {
            let name = case["name"].as_str().unwrap();
            let shard = ShardID {
                app_id: case["shard"]["appID"].as_str().unwrap().to_string(),
                shard_num: case["shard"]["shardNum"].as_u64().unwrap() as u32,
            };
            let table_specs =
                table_specs_from(&fixture["tableSpecs"][case["tableSpecs"].as_str().unwrap()]);
            let full_tables =
                full_tables_from(&fixture["fullTables"][case["fullTables"].as_str().unwrap()]);
            let got = match check_client_schema(
                &shard,
                &case["clientSchema"],
                &table_specs,
                &full_tables,
            ) {
                Ok(()) => Value::Null,
                Err(body) => serde_json::json!({
                    "kind": serde_json::to_value(body.kind()).unwrap(),
                    "message": body.message(),
                }),
            };
            assert_eq!(got, case["expected"], "case {name}");
        }
    }
}
