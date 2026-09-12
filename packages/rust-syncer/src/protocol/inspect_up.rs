//! Port of `packages/zero-protocol/src/inspect-up.ts` — serde
//! equivalents of the valita schemas.

use serde::{Deserialize, Serialize};
use serde_json::Value;

// inspectQueriesUpBodySchema uses clientID (capital ID)
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "op")]
pub enum InspectUpBody {
    #[serde(rename = "queries")]
    Queries {
        id: String,
        #[serde(rename = "clientID")]
        client_id: Option<String>,
    },
    #[serde(rename = "metrics")]
    Metrics { id: String },
    #[serde(rename = "version")]
    Version { id: String },
    #[serde(rename = "authenticate")]
    Authenticate { id: String, value: String },
    #[serde(rename = "analyze-query")]
    AnalyzeQuery {
        id: String,
        // `astSchema.optional()` (inspect-up.ts:48/51): validated, kept as JSON.
        #[serde(
            default,
            deserialize_with = "crate::protocol::ast::optional_strict_ast"
        )]
        value: Option<Value>,
        options: Option<AnalyzeQueryOptions>,
        #[serde(
            default,
            deserialize_with = "crate::protocol::ast::optional_strict_ast"
        )]
        ast: Option<Value>,
        name: Option<String>,
        args: Option<Vec<Value>>,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AnalyzeQueryOptions {
    pub vended_rows: Option<bool>,
    pub synced_rows: Option<bool>,
    pub join_plans: Option<bool>,
}

// `inspectUpBodySchema` is a `v.union` told apart by `op` (inspect-up.ts).
// Deserialized by hand for the same reason as `tagged_union!`: serde's
// `#[serde(tag)]` buffers the content and loses every path below it, and
// with it the `at 1.ast.table` of the TS message. One private mirror struct
// per member carries the fields; the public enum keeps its shape.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueriesFields {
    id: String,
    #[serde(
        rename = "clientID",
        default,
        deserialize_with = "crate::protocol::optional_no_null"
    )]
    client_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdFields {
    id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthenticateFields {
    id: String,
    value: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AnalyzeQueryFields {
    id: String,
    #[serde(
        default,
        deserialize_with = "crate::protocol::ast::optional_strict_ast"
    )]
    value: Option<Value>,
    #[serde(default, deserialize_with = "crate::protocol::optional_no_null")]
    options: Option<AnalyzeQueryOptions>,
    #[serde(
        default,
        deserialize_with = "crate::protocol::ast::optional_strict_ast"
    )]
    ast: Option<Value>,
    #[serde(default, deserialize_with = "crate::protocol::optional_no_null")]
    name: Option<String>,
    #[serde(default, deserialize_with = "crate::protocol::optional_no_null")]
    args: Option<Vec<Value>>,
}

impl<'de> Deserialize<'de> for InspectUpBody {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use crate::protocol::valita::{Issue, Key, ValitaIssue, deserialize_at, encode_nested};
        use serde::de::Error as _;
        const OPS: [&str; 5] = [
            "queries",
            "metrics",
            "version",
            "authenticate",
            "analyze-query",
        ];
        let value = Value::deserialize(d)?;
        let fail = |issue: ValitaIssue| D::Error::custom(encode_nested(&issue));
        let Some(object) = value.as_object() else {
            return Err(fail(ValitaIssue {
                issue: Issue::InvalidType {
                    expected: vec!["object".to_string()],
                },
                path: vec![],
            }));
        };
        let op = object.get("op").and_then(Value::as_str);
        let mut inner = value.clone();
        if let Some(m) = inner.as_object_mut() {
            m.shift_remove("op");
        }
        match op {
            Some("queries") => deserialize_at::<QueriesFields>(&inner, &[], &inner)
                .map(|f| InspectUpBody::Queries {
                    id: f.id,
                    client_id: f.client_id,
                })
                .map_err(fail),
            Some("metrics") => deserialize_at::<IdFields>(&inner, &[], &inner)
                .map(|f| InspectUpBody::Metrics { id: f.id })
                .map_err(fail),
            Some("version") => deserialize_at::<IdFields>(&inner, &[], &inner)
                .map(|f| InspectUpBody::Version { id: f.id })
                .map_err(fail),
            Some("authenticate") => deserialize_at::<AuthenticateFields>(&inner, &[], &inner)
                .map(|f| InspectUpBody::Authenticate {
                    id: f.id,
                    value: f.value,
                })
                .map_err(fail),
            Some("analyze-query") => deserialize_at::<AnalyzeQueryFields>(&inner, &[], &inner)
                .map(|f| InspectUpBody::AnalyzeQuery {
                    id: f.id,
                    value: f.value,
                    options: f.options,
                    ast: f.ast,
                    name: f.name,
                    args: f.args,
                })
                .map_err(fail),
            _ => {
                let issue = if object.contains_key("op") {
                    Issue::InvalidLiteral {
                        expected: OPS.iter().map(|o| Value::from(*o)).collect(),
                    }
                } else {
                    Issue::MissingValue
                };
                Err(fail(ValitaIssue {
                    issue,
                    path: vec![Key::Prop("op".to_string())],
                }))
            }
        }
    }
}
