//! Port of `zero-protocol/src/mutation.ts`.
//!
//! Two parse modes meet here. `mutationSchema` (the entries of an upstream
//! `push` body, push.ts:8) is parsed at the client boundary in valita's
//! default STRICT mode — `deny_unknown_fields`, literal enums, exact tuples.
//! `mutationResponseSchema` is parsed back from the API server (through
//! `mutateResponseSchema`) in `passthrough` mode (custom/fetch.ts:260).
//!
//! The `mutationResultSchema` members are defined in rust-cvr's
//! `client_handler.rs`: that crate parses them too (`mutationRowSchema`) and
//! cannot depend on this one, so the single definition lives there and is
//! re-exported here under the TS names.

use serde::{Deserialize, Deserializer};
use serde_json::Value;

pub use rust_cvr::client_handler::{
    AppError, AppErrorLiteral, MutationError, MutationOk, MutationResult, ZeroError, ZeroErrorKind,
};

use super::JsNumber;
use super::data::Row;
use super::mutation_id::passthrough::MutationID;
use super::primary_key::{PrimaryKey, PrimaryKeyValueRecord};

/// `CRUD_MUTATION_NAME` (mutation.ts:9).
pub const CRUD_MUTATION_NAME: &str = "_zero_crud";

/// `v.literal(CRUD_MUTATION_NAME)` (mutation.ts:101).
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum CrudMutationName {
    #[serde(rename = "_zero_crud")]
    ZeroCrud,
}

/// `crudOpSchema` (mutation.ts:63-68): `insert | upsert | update | delete`,
/// told apart by the `op` literal (mutation.ts:39-62). A write op carries a
/// `rowSchema` value; `delete` carries a `primaryKeyValueRecordSchema`.
#[derive(Debug, Clone)]
pub enum CrudOp {
    Insert(CrudWriteOp),
    Upsert(CrudWriteOp),
    Update(CrudWriteOp),
    Delete(CrudDeleteOp),
}
crate::tagged_union!(CrudOp, "op", ["insert" => Insert(CrudWriteOp), "upsert" => Upsert(CrudWriteOp), "update" => Update(CrudWriteOp), "delete" => Delete(CrudDeleteOp)]);

/// The body shared by `insertOpSchema` / `upsertOpSchema` / `updateOpSchema`
/// (mutation.ts:39-56), minus the `op` tag.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CrudWriteOp {
    pub table_name: String,
    pub primary_key: PrimaryKey,
    pub value: Row,
}

/// `deleteOpSchema` (mutation.ts:57-62), minus the `op` tag.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CrudDeleteOp {
    pub table_name: String,
    pub primary_key: PrimaryKey,
    pub value: PrimaryKeyValueRecord,
}

/// `crudArgSchema` (mutation.ts:70-72).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrudArg {
    pub ops: Vec<CrudOp>,
}

/// `crudArgsSchema` (mutation.ts:73): `v.tuple([crudArgSchema])` — exactly one.
pub type CrudArgs = (CrudArg,);

/// `crudArgsSchema`: exactly one `crudArgSchema`.
fn crud_args<'de, D: Deserializer<'de>>(d: D) -> Result<CrudArgs, D::Error> {
    let value = Value::deserialize(d)?;
    super::exact_tuple::<CrudArgs, D::Error>(&value, 1, &[])
}

/// `crudMutationSchema` (mutation.ts:96-103), minus the `type` tag.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CrudMutation {
    pub id: JsNumber,
    #[serde(rename = "clientID")]
    pub client_id: String,
    pub name: CrudMutationName,
    #[serde(deserialize_with = "crud_args")]
    pub args: CrudArgs,
    pub timestamp: JsNumber,
}

/// `customMutationSchema` (mutation.ts:104-111), minus the `type` tag.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CustomMutation {
    pub id: JsNumber,
    #[serde(rename = "clientID")]
    pub client_id: String,
    pub name: String,
    pub args: Vec<Value>,
    pub timestamp: JsNumber,
}

/// `mutationSchema` (mutation.ts:112): `crudMutation | customMutation`, told
/// apart by the `type` literal (`MutationType.CRUD` / `MutationType.Custom`,
/// mutation-type-enum.ts).
#[derive(Debug, Clone)]
pub enum Mutation {
    Crud(CrudMutation),
    Custom(CustomMutation),
}
crate::tagged_union!(Mutation, "type", ["crud" => Crud(CrudMutation), "custom" => Custom(CustomMutation)]);

/// `deserialize_with` for `pushBodySchema.mutations` (`v.array(mutationSchema)`,
/// push.ts:8): every entry must satisfy the strict schema; the accepted JSON
/// is kept as-is because the push is relayed verbatim (I-3).
pub fn strict_mutations<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Value>, D::Error> {
    let entries = Vec::<Value>::deserialize(d)?;
    for (index, entry) in entries.iter().enumerate() {
        super::validate_nested::<Mutation, D::Error>(entry, &[super::valita::Key::Index(index)])?;
    }
    Ok(entries)
}

/// `mutationResponseSchema` (mutation.ts:144-147).
#[derive(Debug, Deserialize)]
pub struct MutationResponse {
    pub id: MutationID,
    pub result: MutationResult,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crud(ops: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "type": "crud", "id": 1, "clientID": "c1", "name": "_zero_crud",
            "args": [{"ops": ops}], "timestamp": 2,
        })
    }

    /// `mutationSchema` in valita's default strict mode: both members, then
    /// one rejection per rule — an unknown key at any level, a `type` or
    /// `name` outside its literal, `args` that is not a one-element tuple, a
    /// `delete` whose value carries a non-primitive, an empty primary key.
    #[test]
    fn strict_mutation_accepts_the_ts_shapes_and_rejects_each_rule() {
        let insert = crud(
            serde_json::json!([{"op": "insert", "tableName": "t", "primaryKey": ["id"], "value": {"id": 1, "x": null}}]),
        );
        let delete = crud(
            serde_json::json!([{"op": "delete", "tableName": "t", "primaryKey": ["id"], "value": {"id": "a"}}]),
        );
        let custom = serde_json::json!({"type": "custom", "id": 1, "clientID": "c1", "name": "n", "args": [1, {"a": 2}], "timestamp": 2});
        for ok in [&insert, &delete, &custom] {
            Mutation::deserialize(ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
        let mut extra_root = custom.clone();
        extra_root["extra"] = serde_json::json!(1);
        let mut extra_op = insert.clone();
        extra_op["args"][0]["ops"][0]["extra"] = serde_json::json!(1);
        let mut bad_type = custom.clone();
        bad_type["type"] = serde_json::json!("other");
        let mut bad_name = insert.clone();
        bad_name["name"] = serde_json::json!("_zero_other");
        let mut two_args = insert.clone();
        two_args["args"] = serde_json::json!([{"ops": []}, {"ops": []}]);
        let bad_delete = crud(
            serde_json::json!([{"op": "delete", "tableName": "t", "primaryKey": ["id"], "value": {"id": {"nested": 1}}}]),
        );
        let empty_pk = crud(
            serde_json::json!([{"op": "insert", "tableName": "t", "primaryKey": [], "value": {}}]),
        );
        let mut custom_args = custom.clone();
        custom_args["args"] = serde_json::json!({"not": "an array"});
        for bad in [
            &extra_root,
            &extra_op,
            &bad_type,
            &bad_name,
            &two_args,
            &bad_delete,
            &empty_pk,
            &custom_args,
        ] {
            assert!(Mutation::deserialize(bad).is_err(), "must reject: {bad}");
        }
    }
}
