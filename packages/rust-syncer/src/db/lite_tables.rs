//! Replica schema introspection (Part 1 of the ViewSyncer data path).
//!
//! Reads a Zero SQLite replica's schema and produces the [`IvmTableSpec`]s the
//! IVM engine needs. Port of the TS `computeZqlSpecs` / `liteTypeToZqlValueType`
//! (packages/zero-cache/src/db/lite-tables.ts + types/lite.ts + pg-data-type.ts).
//!
//! Column nullability and the upstream type are encoded in the SQLite declared
//! type string (the "lite type string"), e.g. `int8|NOT_NULL`, `nomz|TEXT_ENUM`,
//! `int8[]|TEXT_ARRAY` — NOT in the SQLite NOT NULL flag. Columns whose upstream
//! type ZQL doesn't support (returns `None`) are dropped from the spec, matching
//! TS.
//!
//! Unique indexes (`pragma_index_list`/`pragma_index_xinfo`) and per-table
//! `minRowVersion` (`_zero.tableMetadata`) are read here too — they flow into
//! the engine's `TableSpec` and drive snapshotter diff validation + the
//! streamer's `_0_version` bumping (ports of TS `listIndexes` /
//! `TableMetadataTracker.getMinRowVersions`).

use crate::services::view_syncer::pipeline_driver::{IvmColumnSchema, IvmTableSpec};
use rusqlite::Connection;
use std::collections::HashMap;

/// Immutable replica creation version and the live replication watermark.
/// These are deliberately distinct: a restored replica keeps its creation
/// version while its watermark advances on every replicated transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaVersions {
    pub replica_version: String,
    pub watermark: String,
}

/// Read the subscription versions from the same joined row used by the
/// TypeScript `getSubscriptionState()` implementation.
pub fn read_replica_versions(conn: &Connection) -> Result<ReplicaVersions, String> {
    conn.query_row(
        r#"SELECT c."replicaVersion", s."stateVersion"
           FROM "_zero.replicationConfig" AS c
           JOIN "_zero.replicationState" AS s ON c."lock" = s."lock""#,
        [],
        |row| {
            Ok(ReplicaVersions {
                replica_version: row.get(0)?,
                watermark: row.get(1)?,
            })
        },
    )
    .map_err(|e| format!("read replica subscription state: {e}"))
}

pub fn read_replica_versions_from_path(replica_path: &str) -> Result<ReplicaVersions, String> {
    let conn = open_replica_read_only(replica_path)?;
    read_replica_versions(&conn)
}

/// Open the replica strictly READ_ONLY. The default `Connection::open` is
/// READ_WRITE|CREATE, which silently creates an empty SQLite file on a
/// mistyped `REPLICA_FILE` — masking the misconfiguration behind later
/// "no such table" errors while leaving a stray db file behind. These readers
/// never write, so a missing file must fail fast here.
pub fn open_replica_read_only(replica_path: &str) -> Result<Connection, String> {
    Connection::open_with_flags(
        replica_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| format!("open replica {replica_path}: {e}"))
}

const NOT_NULL_ATTRIBUTE: &str = "|NOT_NULL";
const TEXT_ENUM_ATTRIBUTE: &str = "|TEXT_ENUM";
const TEXT_ARRAY_ATTRIBUTE: &str = "|TEXT_ARRAY";

/// Open the replica and compute its table specs.
pub fn compute_table_specs_from_path(replica_path: &str) -> Result<Vec<IvmTableSpec>, String> {
    let conn = open_replica_read_only(replica_path)?;
    compute_zql_specs(&conn)
}

/// Compute table specs from an open replica connection.
pub fn compute_zql_specs(conn: &Connection) -> Result<Vec<IvmTableSpec>, String> {
    let table_names = list_tables(conn)?;
    // Read unique indexes + minRowVersion once, then attach per table.
    let unique_indexes = list_unique_indexes(conn)?;
    let min_row_versions = read_min_row_versions(conn)?;
    let mut specs = Vec::with_capacity(table_names.len());
    for table in table_names {
        if let Some(spec) = read_table_spec(conn, &table, &unique_indexes, &min_row_versions)? {
            specs.push(spec);
        }
    }
    Ok(specs)
}

/// Validate the client-declared schema against the syncable replica schema.
/// This covers the read-path invariants enforced by TS `checkClientSchema`:
/// referenced tables/columns must exist, value types must agree, and the client
/// primary key must correspond to a replicated unique key.
pub fn validate_client_schema(
    client_schema: &serde_json::Value,
    specs: &[IvmTableSpec],
) -> Result<(), String> {
    let tables = client_schema
        .get("tables")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "client schema must contain a tables object".to_string())?;
    if specs.is_empty() {
        return Err(
            "No tables have been synced from upstream. Check the upstream replication setup."
                .to_string(),
        );
    }
    let by_name: HashMap<&str, &IvmTableSpec> =
        specs.iter().map(|s| (s.table.as_str(), s)).collect();
    let mut errors = Vec::new();
    for (table_name, client_table) in tables {
        let Some(server) = by_name.get(table_name.as_str()) else {
            errors.push(format!(
                "The \"{table_name}\" table is not replicated or syncable."
            ));
            continue;
        };
        let Some(columns) = client_table
            .get("columns")
            .and_then(serde_json::Value::as_object)
        else {
            errors.push(format!("The \"{table_name}\" table has no columns object."));
            continue;
        };
        for (column, client_column) in columns {
            match server.columns.get(column) {
                None => errors.push(format!(
                    "The \"{table_name}\".\"{column}\" column is not replicated or supported."
                )),
                Some(server_column) => {
                    let client_type = client_column
                        .get("type")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    if client_type != server_column.r#type {
                        errors.push(format!(
                            "The \"{table_name}\".\"{column}\" type \"{}\" does not match client type \"{client_type}\".",
                            server_column.r#type
                        ));
                    }
                }
            }
        }
        let client_pk: Option<Vec<String>> = client_table
            .get("primaryKey")
            .and_then(serde_json::Value::as_array)
            .map(|keys| {
                keys.iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_string)
                    .collect()
            });
        let Some(mut client_pk) = client_pk else {
            errors.push(format!(
                "The \"{table_name}\" table's client schema does not specify a primary key."
            ));
            continue;
        };
        client_pk.sort();
        let candidates: Vec<Vec<String>> = server
            .unique_keys
            .clone()
            .unwrap_or_else(|| vec![server.primary_key.clone()])
            .into_iter()
            .filter(|key| {
                key == &server.primary_key
                    || key.iter().all(|column| {
                        server
                            .columns
                            .get(column)
                            .is_some_and(|schema| !schema.optional)
                    })
            })
            .collect();
        if !candidates.iter().any(|key| {
            let mut key = key.clone();
            key.sort();
            key == client_pk
        }) {
            errors.push(format!(
                "The \"{table_name}\" table's primary key <{}> is not a replicated unique key.",
                client_pk.join(",")
            ));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

/// Read the UNIQUE indexes for every syncable table, grouped as
/// `table → [ [col, …], … ]`. Port of TS `listIndexes` (filtered to unique).
/// Includes SQLite auto-indexes for PRIMARY KEY / UNIQUE constraints, which
/// appear in `sqlite_master` with `type = 'index'`.
fn list_unique_indexes(conn: &Connection) -> Result<HashMap<String, Vec<Vec<String>>>, String> {
    let sql = "SELECT idx.name AS indexName, idx.tbl_name AS tableName, \
                      info.\"unique\" AS uniq, col.name AS column \
               FROM sqlite_master AS idx \
               JOIN pragma_index_list(idx.tbl_name) AS info ON info.name = idx.name \
               JOIN pragma_index_xinfo(idx.name) AS col \
               WHERE idx.type = 'index' AND col.key = 1 \
                 AND idx.tbl_name NOT LIKE '\\_zero.%' ESCAPE '\\' \
               ORDER BY idx.name, col.seqno ASC";
    let mut stmt = conn
        .prepare(sql)
        .map_err(|e| format!("prepare index list: {e}"))?;
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?, // index name
                row.get::<_, String>(1)?, // table name
                row.get::<_, i64>(2)?,    // unique flag
                row.get::<_, String>(3)?, // column name
            ))
        })
        .map_err(|e| format!("query index list: {e}"))?;

    // Accumulate columns per index (in seqno order), tracking table + uniqueness.
    let mut by_index: Vec<(String, String, bool, Vec<String>)> = Vec::new();
    for r in rows {
        let (index_name, table_name, uniq, column) = r.map_err(|e| format!("read index: {e}"))?;
        match by_index.last_mut() {
            Some((name, _, _, cols)) if *name == index_name => cols.push(column),
            _ => by_index.push((index_name, table_name, uniq != 0, vec![column])),
        }
    }

    let mut out: HashMap<String, Vec<Vec<String>>> = HashMap::new();
    for (_, table, is_unique, cols) in by_index {
        if is_unique {
            out.entry(table).or_default().push(cols);
        }
    }
    Ok(out)
}

/// Port of TS `liteTableName` (`types/names.ts`): the SQLite table name for a
/// `(schema, table)` pair — bare `table` for the `public` schema, `schema.table`
/// otherwise. This is the name the replica actually creates tables under and the
/// name `sqlite_master` reports, so any keyed lookup against `list_tables` output
/// MUST use this form.
fn lite_table_name(schema: &str, table: &str) -> String {
    if schema == "public" {
        table.to_string()
    } else {
        format!("{schema}.{table}")
    }
}

/// Read `table → minRowVersion` from `_zero.tableMetadata`. Port of TS
/// `TableMetadataTracker.getMinRowVersions` (the map key is the lite table name
/// via `liteTableName`). Returns an empty map when the metadata table is absent
/// (older replicas), so it degrades gracefully.
fn read_min_row_versions(conn: &Connection) -> Result<HashMap<String, String>, String> {
    let mut stmt = match conn
        .prepare("SELECT \"schema\", \"table\", \"minRowVersion\" FROM \"_zero.tableMetadata\"")
    {
        Ok(s) => s,
        // No metadata table (older replica) → no overrides.
        Err(_) => return Ok(HashMap::new()),
    };
    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?, // schema
                row.get::<_, String>(1)?, // table
                row.get::<_, String>(2)?, // minRowVersion
            ))
        })
        .map_err(|e| format!("query tableMetadata: {e}"))?;
    let mut out = HashMap::new();
    for r in rows {
        let (schema, table, min_row_version) = r.map_err(|e| format!("read tableMetadata: {e}"))?;
        // Key by the LITE table name so it matches the `sqlite_master` names used
        // at lookup time (`read_table_spec`). Port of TS `liteTableName` (which is
        // how `getMinRowVersions` keys its map): bare `name` for the `public`
        // schema, `schema.name` otherwise. Keying unconditionally by
        // `"{schema}.{table}"` silently missed EVERY public-schema table (the
        // common case), dropping the minRowVersion re-download override.
        out.insert(lite_table_name(&schema, &table), min_row_version);
    }
    Ok(out)
}

/// User (syncable) table names — excludes SQLite internal, `_zero.*`, and
/// `_litestream_*` bookkeeping tables (matches the TS `computeZqlSpecs` filter).
fn list_tables(conn: &Connection) -> Result<Vec<String>, String> {
    let mut stmt = conn
        .prepare(
            "SELECT name FROM sqlite_master \
             WHERE type = 'table' \
               AND name NOT LIKE 'sqlite_%' \
               AND name NOT LIKE '\\_zero.%' ESCAPE '\\' \
               AND name NOT LIKE '\\_litestream\\_%' ESCAPE '\\'",
        )
        .map_err(|e| format!("prepare table list: {e}"))?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|e| format!("query table list: {e}"))?;
    let mut names = Vec::new();
    for r in rows {
        names.push(r.map_err(|e| format!("read table name: {e}"))?);
    }
    Ok(names)
}

/// Build the spec for one table. Returns `None` if the table has no
/// ZQL-supported columns (e.g. it would be unsyncable).
fn read_table_spec(
    conn: &Connection,
    table: &str,
    unique_indexes: &HashMap<String, Vec<Vec<String>>>,
    min_row_versions: &HashMap<String, String>,
) -> Result<Option<IvmTableSpec>, String> {
    // pragma_table_info via table-valued function; bind the table name.
    let mut stmt = conn
        .prepare("SELECT name, type, pk FROM pragma_table_info(?1)")
        .map_err(|e| format!("prepare table_info({table}): {e}"))?;
    let rows = stmt
        .query_map([table], |row| {
            Ok((
                row.get::<_, String>(0)?, // column name
                row.get::<_, String>(1)?, // declared (lite) type string
                row.get::<_, i64>(2)?,    // pk position (0 = not part of pk)
            ))
        })
        .map_err(|e| format!("query table_info({table}): {e}"))?;

    let mut columns = std::collections::HashMap::new();
    // The kept columns in `pragma_table_info` (declared / `cid`) order. TS builds
    // its zqlSpec columns as an insertion-ordered object and emits the SELECT
    // list via `Object.keys(columns)` (query-builder.ts:37), so the SELECT column
    // order — which is client-observable in an analyzeQuery result's SQL keys —
    // is the DECLARED order, not the (nondeterministic) HashMap order.
    let mut column_order: Vec<String> = Vec::new();
    // Columns that are NOT NULL upstream (eligible to form a row key). A key over
    // a nullable column can't uniquely identify a row, so only these are usable.
    let mut not_null_columns: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Columns marked as PRIMARY KEY by `pragma_table_info` (`pk > 0`). These are
    // treated as non-null even if the lite type omitted `|NOT_NULL`, matching TS
    // `fullTable.primaryKey?.includes(col)`.
    let mut declared_pk: std::collections::HashSet<String> = std::collections::HashSet::new();

    for r in rows {
        let (col_name, lite_type, pk_pos) = r.map_err(|e| format!("read column: {e}"))?;
        if pk_pos > 0 {
            declared_pk.insert(col_name.clone());
        }
        let Some(zql_type) = lite_type_to_zql_value_type(&lite_type) else {
            // Unsupported upstream type — drop the column (matches TS visibleColumns).
            continue;
        };
        let optional = !lite_type.contains(NOT_NULL_ATTRIBUTE);
        if !optional {
            not_null_columns.insert(col_name.clone());
        }
        columns.insert(
            col_name.clone(),
            IvmColumnSchema {
                r#type: zql_type.to_string(),
                optional,
            },
        );
        column_order.push(col_name);
    }

    if columns.is_empty() {
        return Ok(None);
    }

    // A declared-PK column is non-null even if the lite type omitted `|NOT_NULL`
    // (only if it survived as a visible column). Port of TS `notNullColumns`.
    for col in &declared_pk {
        if columns.contains_key(col) {
            not_null_columns.insert(col.clone());
        }
    }

    // The primary key is chosen from the table's UNIQUE INDEXES — NOT from the
    // `pragma_table_info` pk, because Zero's replica encodes row keys as explicit
    // unique indexes and frequently declares no SQL PRIMARY KEY at all (pragma pk
    // would then be empty). This is a direct port of TS `computeZqlSpecs`.
    //
    // `unique_keys`: every unique index over still-visible (supported) columns.
    let all_unique: Vec<Vec<String>> = unique_indexes
        .get(table)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|key| key.iter().all(|c| columns.contains_key(c)))
        .collect();

    // Candidate keys: unique indexes whose columns are ALL non-null. Port of the
    // `keys` filter in TS `computeZqlSpecs`.
    let mut keys: Vec<Vec<String>> = all_unique
        .iter()
        .filter(|key| key.iter().all(|c| not_null_columns.contains(c)))
        .cloned()
        .collect();

    if keys.is_empty() {
        // No usable row key → table is not syncable (TS skips it with a debug log:
        // "not syncing table ... has no primary key").
        return Ok(None);
    }

    // Pick the "best" key: fewest columns, then lexicographic. Port of TS `keyCmp`.
    keys.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.iter().cmp(b.iter())));
    let primary_key = keys[0].clone();

    let unique_keys = if all_unique.is_empty() {
        None
    } else {
        Some(all_unique)
    };

    Ok(Some(IvmTableSpec {
        table: table.to_string(),
        columns,
        column_order,
        primary_key,
        unique_keys,
        min_row_version: min_row_versions.get(table).cloned(),
    }))
}

/// Map a lite type string to a ZQL value type, or `None` if unsupported.
/// Port of `liteTypeToZqlValueType` → `dataTypeToZqlValueType`.
pub fn lite_type_to_zql_value_type(lite_type: &str) -> Option<&'static str> {
    // Arrays (either `|TEXT_ARRAY` or a legacy `[]` suffix) → json.
    if lite_type.contains(TEXT_ARRAY_ATTRIBUTE) || lite_type.contains("[]") {
        return Some("json");
    }
    let is_enum = lite_type.contains(TEXT_ENUM_ATTRIBUTE);

    // upstreamDataType: strip attributes at the first '|', then normalize.
    let base = match lite_type.find('|') {
        Some(i) if i > 0 => &lite_type[..i],
        _ => lite_type,
    };
    match zql_type_for_upstream(base) {
        Some(t) => Some(t),
        None if is_enum => Some("string"),
        None => None,
    }
}

/// Port of `dataTypeToZqlValueType`'s `pgToZqlTypeMap` lookup, with
/// `formatTypeForLookup` (strip `(...)` args + lowercase).
fn zql_type_for_upstream(pg_type: &str) -> Option<&'static str> {
    let lower = pg_type.to_lowercase();
    let key = match lower.find('(') {
        Some(i) => &lower[..i],
        None => lower.as_str(),
    };
    let t = match key.trim() {
        // Numeric
        "smallint" | "integer" | "int" | "int2" | "int4" | "int8" | "bigint" | "smallserial"
        | "serial" | "serial2" | "serial4" | "serial8" | "bigserial" | "decimal" | "numeric"
        | "real" | "double precision" | "float" | "float4" | "float8" => "number",
        // Date/time (stored as numbers)
        "date"
        | "time"
        | "timetz"
        | "time with time zone"
        | "time without time zone"
        | "timestamp"
        | "timestamptz"
        | "timestamp with time zone"
        | "timestamp without time zone" => "number",
        // Native + text-represented string types
        "bpchar" | "character" | "character varying" | "text" | "varchar" | "cidr" | "ean13"
        | "inet" | "isbn" | "isbn13" | "ismn" | "ismn13" | "issn" | "issn13" | "macaddr"
        | "macaddr8" | "pg_lsn" | "upc" | "uuid" => "string",
        // Boolean
        "bool" | "boolean" => "boolean",
        // JSON
        "json" | "jsonb" => "json",
        _ => return None,
    };
    Some(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_mapping() {
        assert_eq!(lite_type_to_zql_value_type("text"), Some("string"));
        assert_eq!(lite_type_to_zql_value_type("text|NOT_NULL"), Some("string"));
        assert_eq!(lite_type_to_zql_value_type("int8|NOT_NULL"), Some("number"));
        assert_eq!(lite_type_to_zql_value_type("timestamptz"), Some("number"));
        assert_eq!(lite_type_to_zql_value_type("bool"), Some("boolean"));
        assert_eq!(lite_type_to_zql_value_type("jsonb"), Some("json"));
        assert_eq!(lite_type_to_zql_value_type("uuid|NOT_NULL"), Some("string"));
        assert_eq!(
            lite_type_to_zql_value_type("int8[]|TEXT_ARRAY"),
            Some("json")
        );
        assert_eq!(
            lite_type_to_zql_value_type("nomz|TEXT_ENUM"),
            Some("string")
        );
        assert_eq!(lite_type_to_zql_value_type("varchar(32)"), Some("string"));
        assert_eq!(lite_type_to_zql_value_type("bytea"), None);
    }

    #[test]
    fn reads_immutable_replica_version_separately_from_watermark() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE "_zero.replicationConfig" (
              lock INTEGER PRIMARY KEY, replicaVersion TEXT NOT NULL
            );
            CREATE TABLE "_zero.replicationState" (
              lock INTEGER PRIMARY KEY, stateVersion TEXT NOT NULL
            );
            INSERT INTO "_zero.replicationConfig" VALUES (1, 'base-01');
            INSERT INTO "_zero.replicationState" VALUES (1, 'head-99');
            "#,
        )
        .unwrap();
        assert_eq!(
            read_replica_versions(&conn).unwrap(),
            ReplicaVersions {
                replica_version: "base-01".to_string(),
                watermark: "head-99".to_string(),
            }
        );
    }

    #[test]
    fn validates_client_schema_against_replica_specs() {
        let spec = IvmTableSpec {
            table: "users".to_string(),
            column_order: Vec::new(),
            columns: HashMap::from([
                (
                    "id".to_string(),
                    IvmColumnSchema {
                        r#type: "string".to_string(),
                        optional: false,
                    },
                ),
                (
                    "email".to_string(),
                    IvmColumnSchema {
                        r#type: "string".to_string(),
                        optional: true,
                    },
                ),
            ]),
            primary_key: vec!["id".to_string()],
            unique_keys: Some(vec![vec!["id".to_string()], vec!["email".to_string()]]),
            min_row_version: None,
        };
        let valid = serde_json::json!({
            "tables": {"users": {"columns": {"id": {"type": "string"}}, "primaryKey": ["id"]}}
        });
        assert!(validate_client_schema(&valid, std::slice::from_ref(&spec)).is_ok());

        let nullable_unique_key = serde_json::json!({
            "tables": {"users": {
                "columns": {"email": {"type": "string"}},
                "primaryKey": ["email"]
            }}
        });
        assert!(
            validate_client_schema(&nullable_unique_key, std::slice::from_ref(&spec))
                .unwrap_err()
                .contains("not a replicated unique key")
        );

        let wrong = serde_json::json!({
            "tables": {"users": {"columns": {"id": {"type": "number"}}, "primaryKey": ["id"]}}
        });
        assert!(
            validate_client_schema(&wrong, &[spec])
                .unwrap_err()
                .contains("does not match")
        );
    }

    #[test]
    fn reads_replica_table_specs() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE "users" (
                "id" "text|NOT_NULL",
                "name" "varchar",
                "age" "int8|NOT_NULL",
                "meta" "jsonb",
                "raw" "bytea"
            );
            -- Real replicas declare NO SQL PRIMARY KEY (pragma pk = 0); the row
            -- key is an explicit UNIQUE INDEX. The PK must be derived from it.
            CREATE UNIQUE INDEX "users_pkey" ON "users" ("id");
            CREATE TABLE "_litestream_seq" ("y" "text");
            "#,
        )
        .unwrap();
        // A `_zero.*` table (name contains a dot) — must be filtered out.
        conn.execute_batch(r#"CREATE TABLE "_zero.clients" ("z" "text");"#)
            .unwrap();
        // A table with no unique index has no row key → not syncable (skipped),
        // matching TS `computeZqlSpecs` ("not syncing table ... has no primary key").
        conn.execute_batch(r#"CREATE TABLE "keyless" ("a" "text|NOT_NULL");"#)
            .unwrap();

        let specs = compute_zql_specs(&conn).unwrap();
        assert_eq!(
            specs.len(),
            1,
            "only the keyed user table should be included"
        );
        let users = &specs[0];
        assert_eq!(users.table, "users");
        // PK is derived from the unique index, not a declared SQL PRIMARY KEY.
        assert_eq!(users.primary_key, vec!["id".to_string()]);
        // `raw` (bytea) is unsupported → dropped; the other 4 remain.
        assert_eq!(users.columns.len(), 4);
        assert_eq!(users.columns["id"].r#type, "string");
        assert!(!users.columns["id"].optional);
        assert!(users.columns["name"].optional);
        assert_eq!(users.columns["age"].r#type, "number");
        assert!(!users.columns["age"].optional);
        assert_eq!(users.columns["meta"].r#type, "json");
        assert!(!users.columns.contains_key("raw"));
        // The unique index is surfaced as a unique key.
        assert_eq!(users.unique_keys, Some(vec![vec!["id".to_string()]]));
        // No metadata table → no minRowVersion override.
        assert_eq!(users.min_row_version, None);
    }

    #[test]
    fn reads_unique_indexes_and_min_row_version() {
        let conn = Connection::open_in_memory().unwrap();
        // A `public`-schema table is created under its BARE name in the replica
        // (`liteTableName`), so `sqlite_master` reports `"users"`, not
        // `"public.users"`. This is the production-realistic naming — an earlier
        // version of this test named the table `"public.users"`, which
        // coincidentally matched the (buggy) `"{schema}.{table}"` map key and thus
        // hid the minRowVersion keying bug. Keep it BARE so the lookup path is
        // actually exercised.
        conn.execute_batch(
            r#"
            CREATE TABLE "users" (
                "id" "text|NOT_NULL",
                "email" "text|NOT_NULL",
                "org" "text|NOT_NULL",
                "team" "text|NOT_NULL",
                "_0_version" "text"
            );
            CREATE UNIQUE INDEX "u_id" ON "users" ("id");
            CREATE UNIQUE INDEX "u_email" ON "users" ("email");
            CREATE UNIQUE INDEX "u_org_team" ON "users" ("org", "team");
            CREATE INDEX "nonunique" ON "users" ("team");

            CREATE TABLE "_zero.tableMetadata" (
                "schema" TEXT NOT NULL,
                "table" TEXT NOT NULL,
                "minRowVersion" TEXT NOT NULL DEFAULT "00",
                "upstreamMetadata" TEXT,
                "metadata" TEXT,
                PRIMARY KEY ("schema", "table")
            );
            INSERT INTO "_zero.tableMetadata" ("schema", "table", "minRowVersion")
                VALUES ('public', 'users', '2abc');
            "#,
        )
        .unwrap();

        let specs = compute_zql_specs(&conn).unwrap();
        assert_eq!(specs.len(), 1);
        let users = &specs[0];

        // minRowVersion is read from `_zero.tableMetadata`, keyed by the LITE
        // table name (`liteTableName`) so it matches the bare `sqlite_master`
        // name for `public`-schema tables. (Regression guard for the
        // `"{schema}.{table}"`-key vs bare-name-lookup mismatch.)
        assert_eq!(users.min_row_version.as_deref(), Some("2abc"));

        // Primary key = keyCmp winner: fewest columns, then lexicographically
        // first. Among the single-column keys [email] and [id], "email" sorts
        // first, so it wins over the composite [org, team]. (Matches TS keyCmp.)
        assert_eq!(users.primary_key, vec!["email".to_string()]);

        // unique_keys include every UNIQUE index; the plain index is excluded.
        let keys = users.unique_keys.clone().unwrap();
        assert!(keys.contains(&vec!["id".to_string()]), "id unique");
        assert!(
            keys.contains(&vec!["email".to_string()]),
            "single-col unique"
        );
        assert!(
            keys.contains(&vec!["org".to_string(), "team".to_string()]),
            "composite unique in order"
        );
        assert!(
            !keys.contains(&vec!["team".to_string()]),
            "non-unique index excluded"
        );
    }

    #[test]
    fn min_row_version_matches_non_public_schema_table() {
        // A non-`public` schema table is created under `"schema.table"` in the
        // replica, so both the `sqlite_master` name and the metadata map key are
        // `"myapp.widgets"`. Verifies the `else` branch of `lite_table_name`
        // keeps parity (the bug only ever affected the `public` schema).
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE "myapp.widgets" (
                "id" "text|NOT_NULL",
                "_0_version" "text"
            );
            CREATE UNIQUE INDEX "w_id" ON "myapp.widgets" ("id");

            CREATE TABLE "_zero.tableMetadata" (
                "schema" TEXT NOT NULL,
                "table" TEXT NOT NULL,
                "minRowVersion" TEXT NOT NULL DEFAULT "00",
                "upstreamMetadata" TEXT,
                "metadata" TEXT,
                PRIMARY KEY ("schema", "table")
            );
            INSERT INTO "_zero.tableMetadata" ("schema", "table", "minRowVersion")
                VALUES ('myapp', 'widgets', '3def');
            "#,
        )
        .unwrap();

        let specs = compute_zql_specs(&conn).unwrap();
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].table, "myapp.widgets");
        assert_eq!(specs[0].min_row_version.as_deref(), Some("3def"));
    }

    #[test]
    fn lite_table_name_public_is_bare_else_qualified() {
        assert_eq!(lite_table_name("public", "users"), "users");
        assert_eq!(lite_table_name("myapp", "widgets"), "myapp.widgets");
    }
}
