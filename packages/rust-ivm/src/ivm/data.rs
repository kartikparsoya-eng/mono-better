//! Core data types — port of `zql/src/ivm/data.ts`.

use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::ivm::stream::RelStream;

/// A column value. Maps to TS `Value`.
///
/// Uses a custom serde representation: plain JSON values (not tagged enums).
/// `null` → `Null`, `true`/`false` → `Bool`, `42` → `F64`, `"hello"` → `Str`,
/// `{...}`/`[...]` → `Json`. This matches the TS wire format and the
/// `json_to_value` mapping in `napi/src/lib.rs`.
#[derive(Clone, Debug, Default)]
pub enum Value {
    #[default]
    Null,
    Bool(bool),
    F64(f64),
    Str(Arc<str>),
    Json(Arc<str>),
}

// Manual Serialize: plain JSON (not tagged enum)
impl serde::Serialize for Value {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Value::Null => s.serialize_none(),
            Value::Bool(b) => s.serialize_bool(*b),
            Value::F64(n) => {
                if n.fract() == 0.0
                    && n.is_finite()
                    && *n >= i64::MIN as f64
                    && *n <= i64::MAX as f64
                {
                    s.serialize_i64(*n as i64)
                } else {
                    s.serialize_f64(*n)
                }
            }
            Value::Str(st) => s.serialize_str(st),
            Value::Json(st) => {
                let val: serde_json::Value =
                    serde_json::from_str(st).unwrap_or(serde_json::Value::String(st.to_string()));
                val.serialize(s)
            }
        }
    }
}

// Manual Deserialize: plain JSON → Value (matching json_to_value logic)
impl<'de> serde::Deserialize<'de> for Value {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = serde_json::Value::deserialize(d)?;
        Ok(match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    const MAX_SAFE: i64 = 9_007_199_254_740_991;
                    if !(-MAX_SAFE..=MAX_SAFE).contains(&i) {
                        return Err(serde::de::Error::custom(format!(
                            "integer {i} is outside of supported bounds"
                        )));
                    }
                    Value::F64(i as f64)
                } else if let Some(f) = n.as_f64() {
                    Value::F64(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::Str(Arc::from(s)),
            other => Value::Json(Arc::from(other.to_string())),
        })
    }
}

impl Value {
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::F64(a), Value::F64(b)) => a == b,
            (Value::Str(a), Value::Str(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

/// Compare two values — port of TS `compareValues` (data.ts:34).
#[inline]
pub fn compare_values(a: &Value, b: &Value) -> CmpOrdering {
    match (a, b) {
        (Value::Null, Value::Null) => CmpOrdering::Equal,
        (Value::Null, _) => CmpOrdering::Less,
        (_, Value::Null) => CmpOrdering::Greater,
        (Value::F64(x), Value::F64(y)) => x.partial_cmp(y).unwrap_or(CmpOrdering::Equal),
        (Value::Str(x), Value::Str(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Json(x), Value::Json(y)) => x.as_bytes().cmp(y.as_bytes()),
        _ => panic!(
            "Cannot compare values of different types: {:?} and {:?}",
            a, b
        ),
    }
}

/// Check if two values are equal — port of TS `valuesEqual` (data.ts:106).
/// null ≠ null (required for join semantics).
#[inline]
pub fn values_equal(a: &Value, b: &Value) -> bool {
    if a.is_null() || b.is_null() {
        return false;
    }
    a == b
}

/// A row of data. TS: `type Row = Record<string, Value>`.
pub type Row = Arc<FxHashMap<String, Value>>;

/// Create a row from key-value pairs.
pub fn row(pairs: impl IntoIterator<Item = (String, Value)>) -> Row {
    Arc::new(pairs.into_iter().collect())
}

/// Ordering specification: list of `[columnName, "asc"|"desc"]`.
/// Renamed from TS `Ordering` to avoid clash with `std::cmp::Ordering`.
pub type SortOrder = Arc<Vec<[String; 2]>>;

/// A comparator function for rows.
pub type Comparator = Rc<dyn Fn(&Row, &Row) -> CmpOrdering + 'static>;

/// Make a comparator from a sort order — port of TS `makeComparator`.
pub fn make_comparator(order: SortOrder, reverse: bool) -> Comparator {
    Rc::new(move |a: &Row, b: &Row| {
        for ord in order.iter() {
            let field = &ord[0];
            let a_val = a.get(field).cloned().unwrap_or(Value::Null);
            let b_val = b.get(field).cloned().unwrap_or(Value::Null);
            let cmp = compare_values(&a_val, &b_val);
            if cmp != CmpOrdering::Equal {
                let result = if ord[1] == "asc" { cmp } else { cmp.reverse() };
                return if reverse { result.reverse() } else { result };
            }
        }
        CmpOrdering::Equal
    })
}

/// Make a partial-bound comparator — comparison stops at the first
/// sort column ABSENT from `b`.
pub fn make_partial_bound_comparator(order: SortOrder, reverse: bool) -> Comparator {
    Rc::new(move |a: &Row, b: &Row| {
        for ord in order.iter() {
            let field = &ord[0];
            if !b.contains_key(field) {
                return CmpOrdering::Equal;
            }
            let a_val = a.get(field).cloned().unwrap_or(Value::Null);
            let b_val = b.get(field).cloned().unwrap_or(Value::Null);
            let cmp = compare_values(&a_val, &b_val);
            if cmp != CmpOrdering::Equal {
                let result = if ord[1] == "asc" { cmp } else { cmp.reverse() };
                return if reverse { result.reverse() } else { result };
            }
        }
        CmpOrdering::Equal
    })
}

/// A node flowing through the pipeline — port of TS `Node`.
#[derive(Clone)]
pub struct Node {
    pub row: Row,
    pub relationships: HashMap<String, RelStream>,
    pub rel_order: Vec<String>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("row", &self.row)
            .field("rel_order", &self.rel_order)
            .field("relationships", &self.relationships.len())
            .finish()
    }
}

impl Node {
    pub fn new(row: Row) -> Self {
        Node {
            row,
            relationships: HashMap::new(),
            rel_order: Vec::new(),
        }
    }

    /// `{...rels, [name]: fn}` — port of TS spread.
    pub fn set_relationship(self, name: &str, rel: RelStream) -> Self {
        let mut node = self;
        if !node.relationships.contains_key(name) {
            node.rel_order.push(name.to_string());
        }
        node.relationships.insert(name.to_string(), rel);
        node
    }
}

/// Drain all relationship streams recursively.
pub fn drain_streams(node: &Node) {
    for rel_fn in node.relationships.values() {
        let stream = rel_fn();
        for child in crate::ivm::stream::skip_yields(stream) {
            drain_streams(&child);
        }
    }
}
