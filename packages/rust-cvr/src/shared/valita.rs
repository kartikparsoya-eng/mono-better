//! Port of `packages/shared/src/valita.ts` — the message text a schema
//! failure reaches the client with.
//!
//! TS's `parse` wraps valita: on failure it throws `new TypeError(message)`
//! where `message` is built by `getMessage` from the FIRST issue only —
//! `Expected number at 1.desiredQueriesPatch.0.ttl. Got null`,
//! `Unexpected property x at 1.desiredQueriesPatch.0`, `Missing property
//! hash at 1.desiredQueriesPatch.0`, … — and a union whose members all fail
//! at the same depth falls back to `Invalid union value: <JSON>`
//! (`getDeepestUnionParseError`). `Connection.#handleMessage` puts
//! `String(e)` — `TypeError: <message>` — into the `InvalidMessage` body
//! (connection.ts:207-209); `fetchFromAPIServer` puts `getErrorMessage(e)`
//! — the bare message — into its `parse` failure (custom/fetch.ts:305).
//!
//! The formatting functions here are the port (`toDisplay`, `displayList`,
//! `getMessage`, the union fallback). serde has no issue model, so the
//! second half is Rust-only glue that turns a `serde_json` error at a
//! `serde_path_to_error` path, plus the value being parsed, into the valita
//! issue TS would have produced — labelled as such below.

use std::fmt;

use serde::de::DeserializeOwned;
use serde_json::Value;

/// One element of an issue `path` (valita `Key = string | number`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key {
    Index(usize),
    Prop(String),
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Key::Index(i) => write!(f, "{i}"),
            Key::Prop(p) => f.write_str(p),
        }
    }
}

pub type Path = Vec<Key>;

/// The valita issue codes `getMessage` renders (valita.ts:63-124).
#[derive(Debug, Clone, PartialEq)]
pub enum Issue {
    /// `invalid_type` — `expected` are valita type names (`string`, `number`,
    /// `boolean`, `null`, `object`, `array`, …).
    InvalidType { expected: Vec<String> },
    /// `missing_value` — the path ends with the missing key.
    MissingValue,
    /// `invalid_literal` — `expected` are the literal values.
    InvalidLiteral { expected: Vec<Value> },
    /// `invalid_length` — `max` is `None` where TS's `maxLength` is undefined.
    InvalidLength { min: usize, max: Option<usize> },
    /// `unrecognized_keys` — every unknown key of the object at the path.
    UnrecognizedKeys { keys: Vec<String> },
    /// `invalid_union` reported by a nested `v.union` of objects whose members
    /// all fail: `Invalid union value at <path>` (valita.ts:113-116, the
    /// enclosing schema is not the union).
    InvalidUnion,
    /// `getDeepestUnionParseError`'s fallback for a ROOT-level union whose
    /// members tie: `Invalid union value: <JSON of the value at the path>`.
    UnionFallback,
    /// `custom_error`.
    CustomError { message: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValitaIssue {
    pub issue: Issue,
    pub path: Path,
}

/// `toDisplay` (valita.ts:6-27): primitives as JSON, `null`, `undefined`,
/// `array`, otherwise `typeof` — `object` for a JSON object.
pub fn to_display(value: Option<&Value>) -> String {
    match value {
        None => "undefined".to_string(),
        Some(Value::String(_)) | Some(Value::Bool(_)) => {
            serde_json::to_string(value.expect("checked")).expect("scalar serializes")
        }
        // `JSON.stringify(number)` prints the JS double: `-0` as `0`,
        // `100000` without a fraction, `9007199254740993` rounded to the
        // nearest double.
        Some(Value::Number(n)) => js_number_to_string(n.as_f64().unwrap_or(f64::NAN)),
        Some(Value::Null) => "null".to_string(),
        Some(Value::Array(_)) => "array".to_string(),
        Some(Value::Object(_)) => "object".to_string(),
    }
}

/// `Number.prototype.toString` for the values JSON can carry: integers up to
/// 1e21 print without a fraction, other finite doubles with Rust's shortest
/// round-trip digits (the same digits JS picks).
pub fn js_number_to_string(v: f64) -> String {
    if v == 0.0 {
        return "0".to_string();
    }
    if v.is_finite() && v.fract() == 0.0 && v.abs() < 1e21 {
        return format!("{}", v as i128);
    }
    format!("{v}")
}

/// `toDisplayAtPath` (valita.ts:31-40): walk `path` from the root; a step
/// that does not resolve reads as JS `undefined`.
pub fn to_display_at_path(root: &Value, path: &[Key]) -> String {
    to_display(value_at(root, path))
}

pub fn value_at<'a>(root: &'a Value, path: &[Key]) -> Option<&'a Value> {
    let mut cur = root;
    for key in path {
        cur = match (key, cur) {
            (Key::Index(i), Value::Array(items)) => items.get(*i)?,
            (Key::Prop(p), Value::Object(map)) => map.get(p)?,
            // JS indexes a string by position and everything else yields
            // undefined; none of our schemas walk into a scalar.
            _ => return None,
        };
    }
    Some(cur)
}

/// `displayList` (valita.ts:42-56): `a`, `a or b`, `a, b or c`.
pub fn display_list<T>(word: &str, expected: &[T], to_display: impl Fn(&T) -> String) -> String {
    match expected {
        [] => String::new(),
        [only] => to_display(only),
        _ => {
            let n = expected.len();
            let suffix = format!(
                "{} {word} {}",
                to_display(&expected[n - 2]),
                to_display(&expected[n - 1])
            );
            if n == 2 {
                suffix
            } else {
                let head: Vec<String> = expected[..n - 2].iter().map(&to_display).collect();
                format!("{}, {suffix}", head.join(", "))
            }
        }
    }
}

fn join_path(path: &[Key]) -> String {
    path.iter()
        .map(|k| k.to_string())
        .collect::<Vec<_>>()
        .join(".")
}

/// `getMessage` (valita.ts:63-124) for the first issue, `root` being the
/// value handed to `parse`.
///
/// Two TS quirks are kept: `invalid_length` reports `Got array with length
/// <root.length>` — the ROOT value's length, not the array at the path
/// (valita.ts:98-105; `undefined` when the root is not an array) — and
/// `missing_value` at the root reads `TODO Unknown missing property`.
pub fn get_message(first: &ValitaIssue, root: &Value) -> String {
    let path = &first.path;
    let at_path = if path.is_empty() {
        String::new()
    } else {
        format!(" at {}", join_path(path))
    };
    match &first.issue {
        Issue::InvalidType { expected } => format!(
            "Expected {}{at_path}. Got {}",
            display_list("or", expected, |s| s.clone()),
            to_display_at_path(root, path)
        ),
        Issue::MissingValue => {
            let at_path = if path.len() > 1 {
                format!(" at {}", join_path(&path[..path.len() - 1]))
            } else {
                String::new()
            };
            match path.last() {
                Some(key) => format!("Missing property {key}{at_path}"),
                None => format!("TODO Unknown missing property{at_path}"),
            }
        }
        Issue::InvalidLiteral { expected } => format!(
            "Expected literal value {}{at_path} Got {}",
            display_list("or", expected, |v| to_display(Some(v))),
            to_display_at_path(root, path)
        ),
        Issue::InvalidLength { min, max } => {
            let length = match max {
                Some(max) if max == min => min.to_string(),
                Some(max) => format!("between {min} and {max}"),
                None => format!("between {min} and undefined"),
            };
            let got = match root {
                Value::Array(items) => items.len().to_string(),
                _ => "undefined".to_string(),
            };
            format!("Expected array with length {length}{at_path}. Got array with length {got}")
        }
        Issue::UnrecognizedKeys { keys } => {
            if keys.len() == 1 {
                format!("Unexpected property {}{at_path}", keys[0])
            } else {
                format!(
                    "Unexpected properties {}{at_path}",
                    display_list("and", keys, |k| k.clone())
                )
            }
        }
        Issue::InvalidUnion => format!("Invalid union value{at_path}"),
        Issue::UnionFallback => invalid_union_message(value_at(root, path).unwrap_or(&Value::Null)),
        Issue::CustomError { message } => {
            format!("{message}{at_path}. Got {}", to_display_at_path(root, path))
        }
    }
}

/// `pathCmp` (valita.ts:150-165): the longer path sorts first; equal lengths
/// compare element-wise (JS `>` / `<` on the keys).
fn path_cmp(a: &ValitaIssue, b: &ValitaIssue) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if a.path.len() != b.path.len() {
        return b.path.len().cmp(&a.path.len());
    }
    for (x, y) in a.path.iter().zip(&b.path) {
        let ord = match (x, y) {
            (Key::Index(i), Key::Index(j)) => i.cmp(j),
            (Key::Prop(p), Key::Prop(q)) => p.cmp(q),
            // JS `>` between a number and a non-numeric string is false both
            // ways: equal.
            _ => Ordering::Equal,
        };
        match ord {
            Ordering::Less => return Ordering::Less,
            Ordering::Greater => return Ordering::Greater,
            Ordering::Equal => {}
        }
    }
    Ordering::Equal
}

/// `getDeepestUnionParseError` (valita.ts:127-147) for a ROOT-level `union`
/// whose members were each tried on `value`: the single deepest failure wins;
/// a tie between the two deepest is the [`Issue::UnionFallback`].
pub fn deepest_union_issue(mut failures: Vec<ValitaIssue>) -> ValitaIssue {
    failures.sort_by(path_cmp);
    match failures.as_slice() {
        [only] => only.clone(),
        [first, second, ..] if path_cmp(first, second) == std::cmp::Ordering::Less => first.clone(),
        _ => ValitaIssue {
            issue: Issue::UnionFallback,
            path: vec![],
        },
    }
}

/// `getDeepestUnionParseError`'s fallback (valita.ts:141-146): the members of
/// a `union` all failed at the same depth, so the message is the value
/// itself, `JSON.stringify`'d.
pub fn invalid_union_message(value: &Value) -> String {
    match serde_json::to_string(value) {
        Ok(json) => format!("Invalid union value: {json}"),
        Err(_) => "Invalid union value".to_string(),
    }
}

// ─── Rust-only glue: serde error → valita issue ──────────────────────────────
// serde reports one error with a fixed message shape and, through
// `serde_path_to_error`, the path it happened at. The issue TS would have
// produced follows from that message, the path and the value being parsed.
// The tables below are derived from the TS schemas each twin ports.

/// Wrap `serde_json::Error` so the path is carried alongside.
pub type PathError = serde_path_to_error::Error<serde_json::Error>;

/// Deserialize `value` as `T`, reporting a failure as the valita issue with
/// its path relative to `base` (the path of `value` within the value handed
/// to TS's `parse`, e.g. `[1]` for an upstream frame's body).
pub fn deserialize_at<T: DeserializeOwned>(
    value: &Value,
    base: &[Key],
    root: &Value,
) -> Result<T, ValitaIssue> {
    serde_path_to_error::deserialize(value).map_err(|err: PathError| {
        let err = missing_before_unknown::<T>(err, value);
        let mut path: Path = base.to_vec();
        path.extend(err.path().iter().map(|segment| match segment {
            serde_path_to_error::Segment::Seq { index } => Key::Index(*index),
            serde_path_to_error::Segment::Map { key } => Key::Prop(key.clone()),
            serde_path_to_error::Segment::Enum { variant } => Key::Prop(variant.clone()),
            serde_path_to_error::Segment::Unknown => Key::Prop("?".to_string()),
        }));
        // A `Segment::Unknown` is serde_path_to_error's marker for a position
        // it could not name — it never appears for our JSON input.
        path.retain(|k| !matches!(k, Key::Prop(p) if p == "?"));
        issue_from_serde(&err.into_inner(), path, root)
    })
}

/// valita reports a missing required key BEFORE an unrecognized one on the
/// same object (`Missing property auth at 1` for `{"au\u0000th": …}`);
/// serde stops at the first unknown key it meets. On an unknown-field error,
/// retry the object with its unknown keys removed: a `missing field` from
/// that retry is the issue valita would have put first.
fn missing_before_unknown<T: DeserializeOwned>(err: PathError, value: &Value) -> PathError {
    let msg = err.inner().to_string();
    let Some(rest) = msg.strip_prefix("unknown field `") else {
        return err;
    };
    let known: Vec<String> = rest
        .split_once("`, ")
        .map(|(_, tail)| backticked(tail))
        .unwrap_or_default();
    let offending = rest.split('`').next().unwrap_or_default();
    let mut relative: Path = err
        .path()
        .iter()
        .filter_map(|seg| match seg {
            serde_path_to_error::Segment::Seq { index } => Some(Key::Index(*index)),
            serde_path_to_error::Segment::Map { key } => Some(Key::Prop(key.clone())),
            _ => None,
        })
        .collect();
    if matches!(relative.last(), Some(Key::Prop(k)) if k == offending) {
        relative.pop();
    }
    let Some(Value::Object(map)) = value_at(value, &relative) else {
        return err;
    };
    if known.is_empty() || map.keys().all(|k| known.contains(k)) {
        return err;
    }
    let mut stripped = value.clone();
    if let Some(Value::Object(m)) = value_at_mut(&mut stripped, &relative) {
        m.retain(|k, _| known.contains(k));
    }
    // Only the retry's own `missing field` outranks the unknown key.
    match serde_path_to_error::deserialize::<_, T>(&stripped) {
        Err(retry) if retry.inner().to_string().starts_with("missing field `") => retry,
        _ => err,
    }
}

fn value_at_mut<'a>(root: &'a mut Value, path: &[Key]) -> Option<&'a mut Value> {
    let mut cur = root;
    for key in path {
        cur = match (key, cur) {
            (Key::Index(i), Value::Array(items)) => items.get_mut(*i)?,
            (Key::Prop(p), Value::Object(map)) => map.get_mut(p)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// A `deserialize_with` helper that validates a nested value on its own
/// (`optional_strict_ast` & co.) loses the outer path. It reports the inner
/// issue through this encoding; the outer [`deserialize_at`] splices the
/// paths back together.
const NESTED: &str = "\u{1}valita:";

pub fn encode_nested(issue: &ValitaIssue) -> String {
    let path: Vec<Value> = issue
        .path
        .iter()
        .map(|k| match k {
            Key::Index(i) => Value::from(*i),
            Key::Prop(p) => Value::from(p.as_str()),
        })
        .collect();
    let issue_json = match &issue.issue {
        Issue::InvalidType { expected } => {
            serde_json::json!({"code": "invalid_type", "expected": expected})
        }
        Issue::MissingValue => serde_json::json!({"code": "missing_value"}),
        Issue::InvalidLiteral { expected } => {
            serde_json::json!({"code": "invalid_literal", "expected": expected})
        }
        Issue::InvalidLength { min, max } => {
            serde_json::json!({"code": "invalid_length", "min": min, "max": max})
        }
        Issue::UnrecognizedKeys { keys } => {
            serde_json::json!({"code": "unrecognized_keys", "keys": keys})
        }
        Issue::InvalidUnion => serde_json::json!({"code": "invalid_union"}),
        Issue::UnionFallback => serde_json::json!({"code": "union_fallback"}),
        Issue::CustomError { message } => {
            serde_json::json!({"code": "custom_error", "message": message})
        }
    };
    format!(
        "{NESTED}{}",
        serde_json::json!({"issue": issue_json, "path": path})
    )
}

fn decode_nested(message: &str) -> Option<ValitaIssue> {
    let json: Value = serde_json::from_str(message.strip_prefix(NESTED)?).ok()?;
    let path = json["path"]
        .as_array()?
        .iter()
        .map(|k| match k {
            Value::Number(n) => Key::Index(n.as_u64().unwrap_or(0) as usize),
            other => Key::Prop(other.as_str().unwrap_or_default().to_string()),
        })
        .collect();
    let i = &json["issue"];
    let strings = |v: &Value| -> Vec<String> {
        v.as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|s| s.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default()
    };
    let issue = match i["code"].as_str()? {
        "invalid_type" => Issue::InvalidType {
            expected: strings(&i["expected"]),
        },
        "missing_value" => Issue::MissingValue,
        "invalid_literal" => Issue::InvalidLiteral {
            expected: i["expected"].as_array().cloned().unwrap_or_default(),
        },
        "invalid_length" => Issue::InvalidLength {
            min: i["min"].as_u64().unwrap_or(0) as usize,
            max: i["max"].as_u64().map(|m| m as usize),
        },
        "unrecognized_keys" => Issue::UnrecognizedKeys {
            keys: strings(&i["keys"]),
        },
        "invalid_union" => Issue::InvalidUnion,
        "union_fallback" => Issue::UnionFallback,
        "custom_error" => Issue::CustomError {
            message: i["message"].as_str().unwrap_or_default().to_string(),
        },
        _ => return None,
    };
    Some(ValitaIssue { issue, path })
}

/// Which key carries the literal that tells a `v.union` of objects apart,
/// keyed by that union's variant set — what serde's `#[serde(tag)]` knows
/// and its `unknown variant` message does not say. Each row cites the TS
/// union it comes from.
const UNION_TAGS: &[(&[&str], &str)] = &[
    // `conditionSchema` (ast.ts): simple | and | or | correlatedSubquery.
    (&["simple", "and", "or", "correlatedSubquery"], "type"),
    // `conditionValueSchema` (ast.ts): literal | column | static.
    (&["literal", "column", "static"], "type"),
    // `simpleCondition.right` (ast.ts): static | literal.
    (&["static", "literal"], "type"),
    // `upPatchOpSchema` (queries-patch.ts): put | del | clear.
    (&["put", "del", "clear"], "op"),
    // `crudOpSchema` (mutation.ts): insert | upsert | update | delete.
    (&["insert", "upsert", "update", "delete"], "op"),
    // `mutationSchema` (mutation.ts): crud | custom.
    (&["crud", "custom"], "type"),
    // `erroredQuerySchema` (custom-queries.ts): app | parse.
    (&["app", "parse"], "error"),
    // `inspectUpBodySchema` (inspect-up.ts): by `op`.
    (
        &[
            "queries",
            "metrics",
            "version",
            "authenticate",
            "analyze-query",
        ],
        "op",
    ),
];

/// valita folds a union of PRIMITIVE types into one `invalid_type` issue
/// listing every member (`Expected string, number, boolean, null or array`);
/// serde reports `data did not match any variant of untagged enum X`. The
/// member list per untagged twin, from the TS schema each ports.
const UNTAGGED_EXPECTED: &[(&str, &[&str])] = &[
    // `literalValueSchema` (ast.ts): string | number | boolean | null | array.
    (
        "LiteralValue",
        &["string", "number", "boolean", "null", "array"],
    ),
    // the array members of `literalValueSchema`: string | number | boolean.
    ("LiteralScalar", &["string", "number", "boolean"]),
    // `parameterReferenceSchema.field` (ast.ts): string | array.
    ("StringOrStrings", &["string", "array"]),
    // `primaryKeyValueSchema` (primary-key.ts): string | number | boolean.
    ("PrimaryKeyValue", &["string", "number", "boolean"]),
];

/// Untagged twins of `v.union`s whose members are all objects (or all
/// tuples). valita folds a non-object input into one `invalid_type`
/// (`Expected object …`); an object no member accepts is `invalid_union`,
/// which the wrapper renders `Invalid union value at <path>`.
const UNTAGGED_OBJECT_UNIONS: &[&str] = &[
    // `mutationResultSchema`, `mutationErrorSchema` (mutation.ts).
    "MutationResult",
    "MutationError",
    // `queryResultSchema` (query-server.ts), `legacyPushResponseSchema`
    // (mutate-server.ts).
    "QueryResult",
    "LegacyPushResponse",
];
const UNTAGGED_ARRAY_UNIONS: &[&str] = &[
    // `transformResponseMessageSchema` (custom-queries.ts): two tuples.
    "TransformResponseMessage",
];

/// serde's `expected …` wording for a scalar visitor → valita's type name.
fn expected_type_name(expected: &str) -> Vec<String> {
    let e = expected.trim();
    // A twin's own visitor may spell valita's list verbatim: `string or null`.
    const VALITA_TYPES: [&str; 8] = [
        "string",
        "number",
        "boolean",
        "null",
        "undefined",
        "object",
        "array",
        "bigint",
    ];
    let parts: Vec<&str> = e
        .split(" or ")
        .flat_map(|p| p.split(", "))
        .map(str::trim)
        .collect();
    if parts.len() > 1 && parts.iter().all(|p| VALITA_TYPES.contains(p)) {
        return parts.into_iter().map(str::to_string).collect();
    }
    let name = if e == "a string" || e == "string" {
        "string"
    } else if e == "a boolean" || e == "boolean" {
        "boolean"
    } else if e == "null" || e == "unit" {
        "null"
    } else if e.starts_with("a sequence")
        || e.starts_with("a tuple")
        || e.starts_with("an array")
        || e == "array"
    {
        "array"
    } else if e.starts_with("a map")
        || e.starts_with("struct ")
        || e.starts_with("an object")
        || e.contains("enum ")
        || e == "object"
    {
        "object"
    } else if e.starts_with('u') || e.starts_with('i') || e.starts_with('f') || e.contains("number")
    {
        "number"
    } else {
        // Unknown wording: surface it rather than guess.
        return vec![e.to_string()];
    };
    vec![name.to_string()]
}

fn backticked(s: &str) -> Vec<String> {
    s.split('`')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
}

/// Rust-only: map one serde error at `path` to the valita issue.
pub fn issue_from_serde(err: &serde_json::Error, path: Path, root: &Value) -> ValitaIssue {
    let msg = err.to_string();
    if let Some(nested) = decode_nested(&msg) {
        let mut full = path;
        full.extend(nested.path);
        return ValitaIssue {
            issue: nested.issue,
            path: full,
        };
    }
    if let Some(rest) = msg.strip_prefix("unknown field `") {
        // `unknown field `k`, expected one of `a`, `b`` / `expected `a`` /
        // `there are no fields`. serde_path_to_error's path ends at the
        // offending key; valita's issue sits on the OBJECT and lists EVERY
        // unknown key of it.
        let offending = rest.split('`').next().unwrap_or_default().to_string();
        let mut path = path;
        if matches!(path.last(), Some(Key::Prop(k)) if *k == offending) {
            path.pop();
        }
        let known: Vec<String> = rest
            .split_once("`, ")
            .map(|(_, tail)| backticked(tail))
            .unwrap_or_default();
        let keys: Vec<String> = match value_at(root, &path) {
            Some(Value::Object(map)) => {
                map.keys().filter(|k| !known.contains(k)).cloned().collect()
            }
            _ => Vec::new(),
        };
        let keys = if keys.is_empty() {
            rest.split('`')
                .next()
                .map(|k| vec![k.to_string()])
                .unwrap_or_default()
        } else {
            keys
        };
        return ValitaIssue {
            issue: Issue::UnrecognizedKeys { keys },
            path,
        };
    }
    if let Some(rest) = msg.strip_prefix("missing field `") {
        let key = rest.split('`').next().unwrap_or_default().to_string();
        let mut path = path;
        path.push(Key::Prop(key));
        return ValitaIssue {
            issue: Issue::MissingValue,
            path,
        };
    }
    if let Some(rest) = msg.strip_prefix("unknown variant `") {
        let expected: Vec<Value> = rest
            .split_once("`, ")
            .map(|(_, tail)| backticked(tail).into_iter().map(Value::from).collect())
            .unwrap_or_default();
        let names: Vec<&str> = expected.iter().filter_map(Value::as_str).collect();
        let mut path = path;
        if let Some((_, tag)) = UNION_TAGS
            .iter()
            .find(|(set, _)| set.len() == names.len() && set.iter().all(|s| names.contains(s)))
        {
            path.push(Key::Prop((*tag).to_string()));
        }
        return ValitaIssue {
            issue: Issue::InvalidLiteral { expected },
            path,
        };
    }
    if let Some(rest) = msg.strip_prefix("invalid length ") {
        // `invalid length N, expected a tuple of size M` / `… at least M …`.
        let tail = rest
            .split_once(", expected ")
            .map(|(_, t)| t)
            .unwrap_or_default();
        // A struct given a JSON array: serde's derived visitor accepts a
        // sequence and reports its length; valita reports the type.
        if tail.starts_with("struct ") {
            return ValitaIssue {
                issue: Issue::InvalidType {
                    expected: vec!["object".to_string()],
                },
                path,
            };
        }
        let number = |s: &str| {
            s.split(|c: char| !c.is_ascii_digit())
                .find(|p| !p.is_empty())
                .and_then(|p| p.parse().ok())
        };
        let issue = if let Some(min) = tail.split("at least ").nth(1).and_then(number) {
            Issue::InvalidLength { min, max: None }
        } else if let Some(n) = tail.split("size ").nth(1).and_then(number) {
            Issue::InvalidLength {
                min: n,
                max: Some(n),
            }
        } else {
            Issue::InvalidLength { min: 0, max: None }
        };
        return ValitaIssue { issue, path };
    }
    if let Some(rest) = msg.strip_prefix("data did not match any variant of untagged enum ") {
        let name = rest.trim();
        if UNTAGGED_OBJECT_UNIONS.contains(&name) || UNTAGGED_ARRAY_UNIONS.contains(&name) {
            let want_object = UNTAGGED_OBJECT_UNIONS.contains(&name);
            let shape_ok = match value_at(root, &path) {
                Some(Value::Object(_)) => want_object,
                Some(Value::Array(_)) => !want_object,
                _ => false,
            };
            let issue = if shape_ok {
                Issue::InvalidUnion
            } else {
                Issue::InvalidType {
                    expected: vec![if want_object { "object" } else { "array" }.to_string()],
                }
            };
            return ValitaIssue { issue, path };
        }
        let expected = UNTAGGED_EXPECTED
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, e)| e.iter().map(|s| s.to_string()).collect())
            .unwrap_or_else(|| vec![name.to_string()]);
        return ValitaIssue {
            issue: Issue::InvalidType { expected },
            path,
        };
    }
    if let Some(tail) = msg
        .strip_prefix("invalid type: ")
        .or_else(|| msg.strip_prefix("invalid value: "))
    {
        let expected = tail
            .rsplit_once(", expected ")
            .map(|(_, e)| e)
            .unwrap_or(tail);
        return ValitaIssue {
            issue: Issue::InvalidType {
                expected: expected_type_name(expected),
            },
            path,
        };
    }
    // `Error::custom(...)` text from a twin's own check, e.g. a non-empty
    // compound key reported as `invalid length` above; anything else lands
    // here as a custom error.
    ValitaIssue {
        issue: Issue::CustomError { message: msg },
        path,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TS goldens captured with node against `packages/shared/src/valita.ts`
    /// on 2026-09-12; each string is `getMessage`'s output for the issue.
    #[test]
    fn get_message_renders_each_issue_like_ts() {
        let root =
            serde_json::json!({"a": "s", "b": {"c": "no", "d": "w", "t": ["a", "b"]}, "z": 1});
        let prop = |s: &str| Key::Prop(s.to_string());
        let cases: Vec<(ValitaIssue, &str)> = vec![
            (
                ValitaIssue {
                    issue: Issue::UnrecognizedKeys {
                        keys: vec!["z".into()],
                    },
                    path: vec![],
                },
                "Unexpected property z",
            ),
            (
                ValitaIssue {
                    issue: Issue::UnrecognizedKeys {
                        keys: vec!["a".into(), "b".into()],
                    },
                    path: vec![prop("b")],
                },
                "Unexpected properties a and b at b",
            ),
            (
                ValitaIssue {
                    issue: Issue::InvalidType {
                        expected: vec!["number".into()],
                    },
                    path: vec![prop("b"), prop("c")],
                },
                "Expected number at b.c. Got \"no\"",
            ),
            (
                ValitaIssue {
                    issue: Issue::MissingValue,
                    path: vec![prop("a")],
                },
                "Missing property a",
            ),
            (
                ValitaIssue {
                    issue: Issue::MissingValue,
                    path: vec![prop("b"), prop("q")],
                },
                "Missing property q at b",
            ),
            (
                ValitaIssue {
                    issue: Issue::InvalidLiteral {
                        expected: vec!["x".into(), "y".into()],
                    },
                    path: vec![prop("b"), prop("d")],
                },
                "Expected literal value \"x\" or \"y\" at b.d Got \"w\"",
            ),
            (
                ValitaIssue {
                    issue: Issue::InvalidLength {
                        min: 1,
                        max: Some(1),
                    },
                    path: vec![prop("b"), prop("t")],
                },
                "Expected array with length 1 at b.t. Got array with length undefined",
            ),
            (
                ValitaIssue {
                    issue: Issue::InvalidLength { min: 1, max: None },
                    path: vec![prop("b"), prop("t")],
                },
                "Expected array with length between 1 and undefined at b.t. Got array with length undefined",
            ),
            (
                ValitaIssue {
                    issue: Issue::InvalidType {
                        expected: vec![
                            "string".into(),
                            "number".into(),
                            "boolean".into(),
                            "null".into(),
                            "array".into(),
                        ],
                    },
                    path: vec![prop("b")],
                },
                "Expected string, number, boolean, null or array at b. Got object",
            ),
            (
                ValitaIssue {
                    issue: Issue::InvalidType {
                        expected: vec!["string".into()],
                    },
                    path: vec![prop("b"), prop("t"), Key::Index(0)],
                },
                "Expected string at b.t.0. Got \"a\"",
            ),
        ];
        for (issue, expected) in cases {
            assert_eq!(get_message(&issue, &root), expected, "{issue:?}");
        }
        assert_eq!(
            get_message(
                &ValitaIssue {
                    issue: Issue::InvalidLength {
                        min: 1,
                        max: Some(1)
                    },
                    path: vec![Key::Index(1)]
                },
                &serde_json::json!(["ping", {}])
            ),
            "Expected array with length 1 at 1. Got array with length 2"
        );
        assert_eq!(
            invalid_union_message(&serde_json::json!(["ping", 5])),
            "Invalid union value: [\"ping\",5]"
        );
    }

    #[test]
    fn nested_issue_round_trips_with_its_path() {
        let issue = ValitaIssue {
            issue: Issue::InvalidLiteral {
                expected: vec!["asc".into(), "desc".into()],
            },
            path: vec![Key::Prop("orderBy".into()), Key::Index(0), Key::Index(1)],
        };
        let decoded = decode_nested(&encode_nested(&issue)).expect("decodes");
        assert_eq!(decoded, issue);
    }
}
