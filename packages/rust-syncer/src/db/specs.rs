//! Port of the replica table-spec types the view-syncer read path consumes
//! (`zero-cache/src/db/specs.ts` `LiteTableSpec` / `LiteColumnSpec`). Only the
//! fields `checkClientSchema` and `computeZqlSpecs` read are carried; the DDL /
//! zod half of specs.ts has no rust consumer.

/// Port of TS `LiteColumnSpec` (specs.ts) — one column of a replica table as
/// `listTables` (lite-tables.ts:47) reports it, BEFORE the ZQL visibility
/// filter: `data_type` is the raw lite type string (`text|NOT_NULL`, `bytea`,
/// …), so unsupported columns are still present here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiteColumnSpec {
    /// 1-based declared position (`pragma_table_info` cid + 1).
    pub pos: usize,
    pub data_type: String,
    /// SQLite `notnull` flag (distinct from the `|NOT_NULL` type attribute).
    pub not_null: bool,
}

/// Port of TS `LiteTableSpec` (specs.ts) as populated by `listTables`: every
/// replica table (syncable or not) with ALL its columns in declared order and
/// the SQL-declared primary key (by `pk` position, `''`-padded like TS).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct LiteTableSpec {
    pub name: String,
    pub columns: Vec<(String, LiteColumnSpec)>,
    pub primary_key: Option<Vec<String>>,
}

impl LiteTableSpec {
    /// TS `fullTable.columns[col]`.
    pub fn column(&self, name: &str) -> Option<&LiteColumnSpec> {
        self.columns.iter().find(|(c, _)| c == name).map(|(_, s)| s)
    }
}
