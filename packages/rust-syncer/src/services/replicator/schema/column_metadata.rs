//! Port of `zero-cache/src/services/replicator/schema/column-metadata.ts` —
//! READ side only (`getInstance` / `getColumn` / `metadataToLiteTypeString`).
//! The write side (insert/update/delete/rename/clearBackfilling) belongs to the
//! replicator process, which is not ported.

use crate::db::lite_tables::lite_type_string;
use rusqlite::Connection;
use std::collections::HashMap;

/// Port of TS `ColumnMetadata` (column-metadata.ts:28-36).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnMetadata {
    pub upstream_type: String,
    pub is_not_null: bool,
    pub is_enum: bool,
    pub is_array: bool,
    pub character_max_length: Option<i64>,
    /// TS `isBackfilling: row.backfill !== null`.
    pub is_backfilling: bool,
}

/// Port of TS `ColumnMetadataStore` (column-metadata.ts:69). TS prepares a
/// per-column `SELECT … WHERE table_name = ? AND column_name = ?`; rust reads
/// the whole `_zero.column_metadata` table once per open (the replica
/// connection is a read-only snapshot, so the two are equivalent) — a
/// rust-only batching choice, not a behavior change.
#[derive(Debug, Default)]
pub struct ColumnMetadataStore {
    rows: HashMap<(String, String), ColumnMetadata>,
}

impl ColumnMetadataStore {
    /// Port of TS `ColumnMetadataStore.getInstance(db)` (column-metadata.ts:144):
    /// `None` when the `_zero.column_metadata` table does not exist yet.
    pub fn get_instance(conn: &Connection) -> Result<Option<Self>, String> {
        let exists: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_zero.column_metadata'",
                [],
                |row| row.get(0),
            )
            .ok();
        if exists.is_none() {
            return Ok(None);
        }
        let mut stmt = conn
            .prepare(
                "SELECT table_name, column_name, upstream_type, is_not_null, is_enum, is_array, \
                 character_max_length, backfill FROM \"_zero.column_metadata\"",
            )
            .map_err(|e| format!("prepare column_metadata: {e}"))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    ColumnMetadata {
                        upstream_type: row.get(2)?,
                        is_not_null: row.get::<_, i64>(3)? != 0,
                        is_enum: row.get::<_, i64>(4)? != 0,
                        is_array: row.get::<_, i64>(5)? != 0,
                        character_max_length: row.get(6)?,
                        is_backfilling: row.get::<_, Option<String>>(7)?.is_some(),
                    },
                ))
            })
            .map_err(|e| format!("query column_metadata: {e}"))?;
        let mut out = HashMap::new();
        for r in rows {
            let (table, column, metadata) = r.map_err(|e| format!("read column_metadata: {e}"))?;
            out.insert((table, column), metadata);
        }
        Ok(Some(Self { rows: out }))
    }

    /// Port of TS `getColumn(tableName, columnName)` (column-metadata.ts:226).
    pub fn get_column(&self, table_name: &str, column_name: &str) -> Option<&ColumnMetadata> {
        self.rows
            .get(&(table_name.to_string(), column_name.to_string()))
    }
}

/// Port of TS `metadataToLiteTypeString(metadata)` (column-metadata.ts:338):
/// `liteTypeString(upstreamType, isNotNull, isEnum, isArray)`.
pub fn metadata_to_lite_type_string(metadata: &ColumnMetadata) -> String {
    lite_type_string(
        &metadata.upstream_type,
        metadata.is_not_null,
        metadata.is_enum,
        metadata.is_array,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_instance_is_none_without_the_metadata_table() {
        let conn = Connection::open_in_memory().unwrap();
        assert!(ColumnMetadataStore::get_instance(&conn).unwrap().is_none());
    }

    /// TS row → ColumnMetadata mapping (column-metadata.ts:226-243) and the
    /// pipe-notation type string it yields (column-metadata.ts:338).
    #[test]
    fn maps_rows_and_builds_the_lite_type_string_like_ts() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r#"
            CREATE TABLE "_zero.column_metadata" (
                table_name TEXT NOT NULL, column_name TEXT NOT NULL, upstream_type TEXT NOT NULL,
                is_not_null INTEGER NOT NULL, is_enum INTEGER NOT NULL, is_array INTEGER NOT NULL,
                character_max_length INTEGER, backfill TEXT, PRIMARY KEY (table_name, column_name));
            INSERT INTO "_zero.column_metadata" VALUES ('users', 'id', 'text', 1, 0, 0, NULL, NULL);
            INSERT INTO "_zero.column_metadata" VALUES ('users', 'mood', 'mood', 0, 1, 1, 12, 'b1');
            "#,
        )
        .unwrap();
        let store = ColumnMetadataStore::get_instance(&conn).unwrap().unwrap();
        let id = store.get_column("users", "id").unwrap();
        assert_eq!(metadata_to_lite_type_string(id), "text|NOT_NULL");
        assert!(!id.is_backfilling);
        let mood = store.get_column("users", "mood").unwrap();
        assert_eq!(
            metadata_to_lite_type_string(mood),
            "mood|TEXT_ENUM|TEXT_ARRAY"
        );
        assert!(mood.is_backfilling);
        assert_eq!(mood.character_max_length, Some(12));
        assert!(store.get_column("users", "nope").is_none());
    }
}
