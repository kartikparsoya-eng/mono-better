//! Port of `zero-protocol/src/ast.ts` — the valita schemas that define the
//! STRICT wire shape of a query AST as a client may send it.
//!
//! This module only validates. Once a message is accepted the syncer keeps the
//! AST as JSON (`serde_json::Value`), the same way TS keeps the parsed object,
//! and `parse_ts_ast` (pipeline_driver.rs) converts the accepted JSON into the
//! engine's `Ast` when the query is built.
//!
//! valita's default parse mode is `strict`: `v.object` / `v.readonlyObject`
//! reject unknown keys, `v.literalUnion` rejects any other value, and
//! `.optional()` is absent-or-value, never `null`. Hence `deny_unknown_fields`
//! on every object, enums for every literal union, and `optional_no_null` on
//! every optional field. TS rejects such a message at the upstream boundary
//! with an `InvalidMessage` error and closes the connection
//! (`Connection.#handleMessage`); rust reaches the same outcome through
//! `parse_upstream`.

use serde::{Deserialize, Deserializer};
use serde_json::Value;

use super::optional_no_null;

/// `astSchema` (ast.ts:181-197).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Ast {
    #[serde(default, deserialize_with = "optional_no_null")]
    pub schema: Option<String>,
    pub table: String,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub alias: Option<String>,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub r#where: Option<Condition>,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub related: Option<Vec<CorrelatedSubquery>>,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub limit: Option<serde_json::Number>,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub order_by: Option<Vec<OrderingElement>>,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub start: Option<Bound>,
}

/// `orderingElementSchema` (ast.ts:23-25): `[selector, 'asc' | 'desc']`.
pub type OrderingElement = (String, Direction);

/// `v.literalUnion('asc', 'desc')` (ast.ts:24).
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum Direction {
    #[serde(rename = "asc")]
    Asc,
    #[serde(rename = "desc")]
    Desc,
}

/// The `start` object of `astSchema` (ast.ts:190-195): `{row, exclusive}`.
/// `rowSchema` is `v.readonlyRecord(valueSchema)` (data.ts:6), any JSON value
/// per column.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bound {
    pub row: serde_json::Map<String, Value>,
    pub exclusive: bool,
}

/// `conditionSchema` (ast.ts:129-134), discriminated by `type`. Each variant
/// wraps its own object schema so `deny_unknown_fields` applies per object.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum Condition {
    #[serde(rename = "simple")]
    Simple(SimpleCondition),
    #[serde(rename = "and")]
    And(Conjunction),
    #[serde(rename = "or")]
    Or(Disjunction),
    #[serde(rename = "correlatedSubquery")]
    CorrelatedSubquery(CorrelatedSubqueryCondition),
}

/// `simpleConditionSchema` (ast.ts:108-113): `left` is any condition value,
/// `right` is a parameter or a literal — never a column.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimpleCondition {
    pub op: SimpleOperator,
    pub left: ConditionValue,
    pub right: RightValue,
}

/// `simpleOperatorSchema` (ast.ts:37-55): equality, order, like and in ops.
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum SimpleOperator {
    #[serde(rename = "=")]
    Eq,
    #[serde(rename = "!=")]
    Ne,
    #[serde(rename = "IS")]
    Is,
    #[serde(rename = "IS NOT")]
    IsNot,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = "<=")]
    Le,
    #[serde(rename = ">=")]
    Ge,
    #[serde(rename = "LIKE")]
    Like,
    #[serde(rename = "NOT LIKE")]
    NotLike,
    #[serde(rename = "ILIKE")]
    ILike,
    #[serde(rename = "NOT ILIKE")]
    NotILike,
    #[serde(rename = "IN")]
    In,
    #[serde(rename = "NOT IN")]
    NotIn,
}

/// `conditionValueSchema` (ast.ts:100-104): literal | column | parameter.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum ConditionValue {
    #[serde(rename = "literal")]
    Literal(LiteralReference),
    #[serde(rename = "column")]
    Column(ColumnReference),
    #[serde(rename = "static")]
    Static(ParameterReference),
}

/// The `right` side of `simpleConditionSchema` (ast.ts:112):
/// `v.union(parameterReferenceSchema, literalReferenceSchema)`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum RightValue {
    #[serde(rename = "static")]
    Static(ParameterReference),
    #[serde(rename = "literal")]
    Literal(LiteralReference),
}

/// `literalReferenceSchema` (ast.ts:57-66).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiteralReference {
    pub value: LiteralValue,
}

/// The `value` union of `literalReferenceSchema` (ast.ts:59-65): a string,
/// number, boolean, null, or an array of strings/numbers/booleans.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum LiteralValue {
    Str(String),
    Num(serde_json::Number),
    Bool(bool),
    Null(()),
    Array(Vec<LiteralScalar>),
}

/// An element of the array form of a literal (ast.ts:64).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum LiteralScalar {
    Str(String),
    Num(serde_json::Number),
    Bool(bool),
}

/// `columnReferenceSchema` (ast.ts:67-70).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnReference {
    pub name: String,
}

/// `parameterReferenceSchema` (ast.ts:89-98).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterReference {
    pub anchor: Anchor,
    pub field: StringOrStrings,
}

/// `v.literalUnion('authData', 'preMutationRow')` (ast.ts:96).
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum Anchor {
    #[serde(rename = "authData")]
    AuthData,
    #[serde(rename = "preMutationRow")]
    PreMutationRow,
}

/// `v.union(v.string(), v.array(v.string()))` (ast.ts:97).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum StringOrStrings {
    One(String),
    Many(Vec<String>),
}

/// `conjunctionSchema` (ast.ts:136-139).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Conjunction {
    pub conditions: Vec<Condition>,
}

/// `disjunctionSchema` (ast.ts:141-144).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Disjunction {
    pub conditions: Vec<Condition>,
}

/// `correlatedSubqueryConditionSchema` (ast.ts:121-127).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorrelatedSubqueryCondition {
    pub related: CorrelatedSubquery,
    pub op: CorrelatedSubqueryConditionOperator,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub flip: Option<bool>,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub scalar: Option<bool>,
}

/// `correlatedSubqueryConditionOperatorSchema` (ast.ts:118-119).
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum CorrelatedSubqueryConditionOperator {
    #[serde(rename = "EXISTS")]
    Exists,
    #[serde(rename = "NOT EXISTS")]
    NotExists,
}

/// `correlatedSubquerySchema` (ast.ts:170-179): the `OmitSubquery` object plus
/// the lazily-typed `subquery`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CorrelatedSubquery {
    pub correlation: Correlation,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub hidden: Option<bool>,
    #[serde(default, deserialize_with = "optional_no_null")]
    pub system: Option<System>,
    pub subquery: Box<Ast>,
}

/// `v.literalUnion('permissions', 'client', 'test')` (ast.ts:175).
#[derive(Debug, Clone, Copy, Deserialize)]
pub enum System {
    #[serde(rename = "permissions")]
    Permissions,
    #[serde(rename = "client")]
    Client,
    #[serde(rename = "test")]
    Test,
}

/// `correlationSchema` (ast.ts:161-164).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Correlation {
    #[serde(deserialize_with = "compound_key")]
    pub parent_field: Vec<String>,
    #[serde(deserialize_with = "compound_key")]
    pub child_field: Vec<String>,
}

/// `compoundKeySchema` (ast.ts:157-159):
/// `v.tuple([v.string()]).concat(v.array(v.string()))` — at least one string.
fn compound_key<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    let key = Vec::<String>::deserialize(d)?;
    if key.is_empty() {
        return Err(serde::de::Error::invalid_length(
            0,
            &"a compound key with at least one column",
        ));
    }
    Ok(key)
}

/// `astSchema.optional()` for a field that the syncer keeps as JSON: validate
/// the value against the strict schema, then hand back the JSON unchanged.
/// `null` is rejected like every valita `.optional()`.
pub fn optional_strict_ast<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Value>, D::Error> {
    let value = Value::deserialize(d)?;
    Ast::deserialize(&value).map_err(serde::de::Error::custom)?;
    Ok(Some(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strict(json: &str) -> Result<Ast, String> {
        serde_json::from_str::<Ast>(json).map_err(|e| e.to_string())
    }

    /// The shapes the client sends today all pass: a bare table, a full
    /// where/related/orderBy/limit/start query, and every condition kind.
    #[test]
    fn accepts_every_valid_ast_shape() {
        strict(r#"{"table":"issue"}"#).unwrap();
        strict(
            r#"{"schema":"","table":"issue","alias":"i","limit":10,"orderBy":[["id","asc"],["modified","desc"]],
                "start":{"row":{"id":"x","n":1,"j":{"a":[1]}},"exclusive":true},
                "where":{"type":"and","conditions":[
                  {"type":"simple","op":"=","left":{"type":"column","name":"ownerId"},"right":{"type":"static","anchor":"authData","field":"sub"}},
                  {"type":"or","conditions":[
                    {"type":"simple","op":"IN","left":{"type":"column","name":"state"},"right":{"type":"literal","value":["open","closed"]}},
                    {"type":"simple","op":"IS","left":{"type":"literal","value":null},"right":{"type":"literal","value":null}}]},
                  {"type":"correlatedSubquery","op":"NOT EXISTS","flip":false,"scalar":false,
                   "related":{"correlation":{"parentField":["id"],"childField":["issueId"]},"hidden":true,"system":"permissions",
                              "subquery":{"table":"comment","where":{"type":"simple","op":"LIKE","left":{"type":"column","name":"body"},"right":{"type":"literal","value":"%x%"}}}}}]},
                "related":[{"correlation":{"parentField":["id"],"childField":["issueId"]},"subquery":{"table":"comment","alias":"comments"}}]}"#,
        )
        .unwrap();
    }

    /// valita strict mode: an unknown key at ANY object level is rejected.
    #[test]
    fn rejects_unknown_keys_at_every_level() {
        assert!(strict(r#"{"table":"issue","bogus":1}"#).is_err());
        assert!(strict(r#"{"table":"issue","start":{"row":{},"exclusive":false,"x":1}}"#).is_err());
        assert!(
            strict(r#"{"table":"issue","where":{"type":"simple","op":"=","left":{"type":"column","name":"a","extra":1},"right":{"type":"literal","value":1}}}"#)
                .is_err()
        );
        assert!(
            strict(r#"{"table":"issue","related":[{"correlation":{"parentField":["id"],"childField":["a"],"z":1},"subquery":{"table":"t"}}]}"#)
                .is_err()
        );
    }

    /// Literal unions, tuples and `.optional()` are as strict as valita's.
    #[test]
    fn rejects_bad_literals_tuples_and_nulls() {
        assert!(strict(r#"{"table":"issue","orderBy":[["id","sideways"]]}"#).is_err());
        assert!(strict(r#"{"table":"issue","orderBy":[["id","asc","extra"]]}"#).is_err());
        assert!(
            strict(r#"{"table":"issue","where":{"type":"simple","op":"~","left":{"type":"column","name":"a"},"right":{"type":"literal","value":1}}}"#)
                .is_err()
        );
        // `right` is parameter | literal — a column is not admitted.
        assert!(
            strict(r#"{"table":"issue","where":{"type":"simple","op":"=","left":{"type":"column","name":"a"},"right":{"type":"column","name":"b"}}}"#)
                .is_err()
        );
        assert!(strict(r#"{"table":"issue","limit":null}"#).is_err());
        assert!(strict(r#"{"table":"issue","related":[{"correlation":{"parentField":[],"childField":["a"]},"subquery":{"table":"t"}}]}"#).is_err());
        assert!(strict(r#"{"table":"issue","related":[{"correlation":{"parentField":["id"],"childField":["a"]},"system":"other","subquery":{"table":"t"}}]}"#).is_err());
        assert!(strict(r#"{"table":"issue","where":{"type":"simple","op":"=","left":{"type":"static","anchor":"nowhere","field":"x"},"right":{"type":"literal","value":1}}}"#).is_err());
        assert!(strict(r#"{"table":"issue","where":{"type":"simple","op":"=","left":{"type":"column","name":"a"},"right":{"type":"literal","value":{"o":1}}}}"#).is_err());
    }
}
