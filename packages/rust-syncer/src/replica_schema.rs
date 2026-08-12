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

use crate::pipeline_driver::{IvmColumnSchema, IvmTableSpec};
use rusqlite::Connection;
use std::collections::HashMap;

const NOT_NULL_ATTRIBUTE: &str = "|NOT_NULL";
const TEXT_ENUM_ATTRIBUTE: &str = "|TEXT_ENUM";
const TEXT_ARRAY_ATTRIBUTE: &str = "|TEXT_ARRAY";

/// Open the replica and compute its table specs.
pub fn compute_table_specs_from_path(replica_path: &str) -> Result<Vec<IvmTableSpec>, String> {
    let conn =
        Connection::open(replica_path).map_err(|e| format!("open replica {replica_path}: {e}"))?;
    compute_table_specs(&conn)
}

/// Compute table specs from an open replica connection.
pub fn compute_table_specs(conn: &Connection) -> Result<Vec<IvmTableSpec>, String> {
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

/// Read `table → minRowVersion` from `_zero.tableMetadata`. Port of TS
/// `TableMetadataTracker.getMinRowVersions` (the map key is the lite table name
/// `{schema}.{table}`). Returns an empty map when the metadata table is absent
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
        out.insert(format!("{schema}.{table}"), min_row_version);
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
    // Columns that are NOT NULL upstream (eligible to form a row key). A key over
    // a nullable column can't uniquely identify a row, so only these are usable.
    let mut not_null_columns: std::collections::HashSet<String> =
        std::collections::HashSet::new();
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

        let specs = compute_table_specs(&conn).unwrap();
        assert_eq!(specs.len(), 1, "only the keyed user table should be included");
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
        conn.execute_batch(
            r#"
            CREATE TABLE "public.users" (
                "id" "text|NOT_NULL",
                "email" "text|NOT_NULL",
                "org" "text|NOT_NULL",
                "team" "text|NOT_NULL",
                "_0_version" "text"
            );
            CREATE UNIQUE INDEX "u_id" ON "public.users" ("id");
            CREATE UNIQUE INDEX "u_email" ON "public.users" ("email");
            CREATE UNIQUE INDEX "u_org_team" ON "public.users" ("org", "team");
            CREATE INDEX "nonunique" ON "public.users" ("team");

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

        let specs = compute_table_specs(&conn).unwrap();
        assert_eq!(specs.len(), 1);
        let users = &specs[0];

        // minRowVersion is read from `_zero.tableMetadata` (keyed schema.table).
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
}
