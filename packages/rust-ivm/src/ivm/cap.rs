//! Cap operator — port of `zql/src/ivm/cap.ts`.
//!
//! Count-based limit for EXISTS subqueries that doesn't require ordering.
//! Tracks membership by primary key set rather than by sorted bound.
//! No comparator needed, no start/reverse support.
//!
//! During push, Cap tracks the PK set and either accepts adds (if under
//! limit), removes (refilling from input if possible), or forwards child
//! changes for tracked rows. Edit changes update the PK in the set if
//! the PK changed.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;

use crate::ivm::change::{Change, ChangeType, make_add_change};
use crate::ivm::constraint::Constraint;
use crate::ivm::data::{Node, Row, Value};
use crate::ivm::operator::{FetchRequest, Input, InputBase, Output, OutputHandle, Shared};
use crate::ivm::schema::SourceSchema;
use crate::ivm::stream::{NodeStream, StreamItem, from_vec};

/// Cap state — tracks count and PK set per partition.
#[derive(Clone, Debug)]
pub struct CapState {
    pub size: usize,
    pub pks: Vec<String>,
}

/// Storage for Cap state — tracks PK sets per partition.
#[derive(Default)]
pub struct CapStorage {
    states: std::collections::HashMap<String, CapState>,
}

impl CapStorage {
    pub fn new() -> Self {
        CapStorage {
            states: std::collections::HashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&CapState> {
        self.states.get(key)
    }

    pub fn set(&mut self, key: String, state: CapState) {
        self.states.insert(key, state);
    }

    pub fn del(&mut self, key: &str) {
        self.states.remove(key);
    }
}

/// RAII guard for `Cap::initial_fetch`, porting the TS `finally` block
/// (cap.ts:162-174). If the consumer drops the stream before the limit is
/// reached or the input is exhausted, the cap state would be under-hydrated.
/// TS persists the (partial) state then asserts `!downstreamEarlyReturn`
/// (throw -> reset). We mirror that on a clean early drop, and skip if a panic
/// is already unwinding (TS's `if (!exceptionThrown)`) to avoid double-panic.
struct CapInitialFetchGuard {
    persisted: Rc<Cell<bool>>,
    pks: Rc<RefCell<Vec<String>>>,
    storage: Shared<CapStorage>,
    key: String,
}

impl Drop for CapInitialFetchGuard {
    fn drop(&mut self) {
        if self.persisted.get() {
            return;
        }
        if std::thread::panicking() {
            return;
        }
        let pks_vec = self.pks.borrow().clone();
        let size = pks_vec.len();
        self.storage
            .borrow_mut()
            .set(self.key.clone(), CapState { size, pks: pks_vec });
        panic!("Cap: unexpected early return prevented full hydration");
    }
}

/// The Cap operator — port of TS `Cap` (cap.ts:36).
pub struct Cap {
    input: Shared<dyn Input>,
    storage: Shared<CapStorage>,
    limit: usize,
    partition_key: Option<Vec<String>>,
    primary_key: Vec<String>,
    schema: SourceSchema,
    output: Rc<RefCell<Option<OutputHandle>>>,
}

impl Cap {
    pub fn new(
        input: Shared<dyn Input>,
        storage: Shared<CapStorage>,
        limit: usize,
        partition_key: Option<Vec<String>>,
    ) -> Shared<Cap> {
        // limit is usize, always >= 0. TS asserts limit >= 0 but that's
        // trivially true for unsigned types.
        debug_assert!(limit < usize::MAX, "Limit must be reasonable");
        let schema = input.borrow().get_schema();
        let primary_key = schema.primary_key.clone();

        let cap = Rc::new(RefCell::new(Cap {
            input: input.clone(),
            storage,
            limit,
            partition_key,
            primary_key,
            schema,
            output: Rc::new(RefCell::new(None)),
        }));

        let cap_clone = cap.clone();
        input
            .borrow()
            .set_output(Rc::new(RefCell::new(CapOutput { cap: cap_clone })));

        cap
    }

    fn get_take_state_key(
        &self,
        row_or_constraint: Option<&Row>,
        constraint: Option<&Constraint>,
    ) -> String {
        match (&self.partition_key, row_or_constraint, constraint) {
            (Some(pk), Some(row), _) => {
                let mut parts = Vec::new();
                for col in pk {
                    let v = row.get(col).unwrap_or(&Value::Null);
                    parts.push(value_to_string(v));
                }
                format!("[\"cap\",{}]", parts.join(","))
            }
            (Some(pk), _, Some(c)) => {
                let mut parts = Vec::new();
                for col in pk {
                    let v = c.get(col).unwrap_or(&Value::Null);
                    parts.push(value_to_string(v));
                }
                format!("[\"cap\",{}]", parts.join(","))
            }
            _ => "[\"cap\"]".to_string(),
        }
    }

    fn serialize_pk(&self, row: &Row) -> String {
        let parts: Vec<String> = self
            .primary_key
            .iter()
            .map(|k| value_to_string(row.get(k).unwrap_or(&Value::Null)))
            .collect();
        format!("[{}]", parts.join(","))
    }

    #[allow(dead_code)]
    fn deserialize_pk_to_constraint(&self, pk_str: &str) -> Constraint {
        // Parse the serialized PK back into a constraint.
        // Format: [v1,v2,...] where each v is JSON-serialized.
        // Must handle quoted strings containing commas.
        let trimmed = pk_str.trim_start_matches('[').trim_end_matches(']');
        let parts = parse_json_array_elements(trimmed);
        let mut c = Constraint::default();
        for (i, part) in parts.iter().enumerate() {
            if i < self.primary_key.len() {
                c.insert(self.primary_key[i].clone(), parse_value(part));
            }
        }
        c
    }

    fn initial_fetch(&self, req: &FetchRequest) -> NodeStream {
        if self.limit == 0 {
            let state_key = self.get_take_state_key(None, req.constraint.as_ref());
            self.storage.borrow_mut().set(
                state_key,
                CapState {
                    size: 0,
                    pks: Vec::new(),
                },
            );
            return from_vec(Vec::new());
        }
        let mut stream = self.input.borrow().fetch(req);
        let limit = self.limit;
        let state_key = self.get_take_state_key(None, req.constraint.as_ref());
        let primary_key = self.primary_key.clone();
        let storage = self.storage.clone();

        // Lazy: yield nodes one at a time, recording PKs as a side effect.
        // State is persisted when the stream is exhausted or limit reached.
        // Port of TS Cap.#initialFetch (cap.ts:101-120).
        let pks: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
        let count = Rc::new(Cell::new(0usize));
        let persisted = Rc::new(Cell::new(false));

        let pks_c = pks.clone();
        let count_c = count.clone();
        let persisted_c = persisted.clone();
        let storage_c = storage.clone();
        let state_key_c = state_key.clone();
        let primary_key_c = primary_key.clone();

        // Fires the TS `finally` early-return assert when the stream is dropped
        // before limit/exhaustion. Captured by the closure so it drops with the
        // iterator.
        let early_return_guard = CapInitialFetchGuard {
            persisted: persisted.clone(),
            pks: pks.clone(),
            storage: storage.clone(),
            key: state_key.clone(),
        };

        Box::new(std::iter::from_fn(move || {
            let _ = &early_return_guard; // keep the guard owned by this closure
            if persisted_c.get() {
                return None;
            }
            match stream.next() {
                Some(StreamItem::Yield) => Some(StreamItem::Yield),
                Some(StreamItem::Data(node)) => {
                    let pk_parts: Vec<String> = primary_key_c
                        .iter()
                        .map(|k| {
                            crate::ivm::cap::value_to_string(
                                node.row.get(k).unwrap_or(&Value::Null),
                            )
                        })
                        .collect();
                    pks_c.borrow_mut().push(format!("[{}]", pk_parts.join(",")));
                    let c = count_c.get() + 1;
                    count_c.set(c);
                    if c >= limit {
                        let pks_vec = pks_c.borrow().clone();
                        storage_c.borrow_mut().set(
                            state_key_c.clone(),
                            CapState {
                                size: c,
                                pks: pks_vec,
                            },
                        );
                        persisted_c.set(true);
                    }
                    Some(StreamItem::Data(node))
                }
                None => {
                    let pks_vec = pks_c.borrow().clone();
                    let size = pks_vec.len();
                    storage_c
                        .borrow_mut()
                        .set(state_key_c.clone(), CapState { size, pks: pks_vec });
                    persisted_c.set(true);
                    None
                }
            }
        }))
    }
}

impl InputBase for Cap {
    fn get_schema(&self) -> SourceSchema {
        self.schema.clone()
    }

    fn destroy(&mut self) {
        self.input.borrow_mut().destroy();
        // Break the Rc cycle: clear the back-edge to the downstream output.
        *self.output.borrow_mut() = None;
    }
}

impl Input for Cap {
    fn set_output(&self, output: OutputHandle) {
        *self.output.borrow_mut() = Some(output);
    }

    fn fetch(&self, req: &FetchRequest) -> NodeStream {
        assert!(req.start.is_none(), "Cap does not support start");
        assert!(!req.reverse, "Cap does not support reverse");

        // v1.7.0: Cap is only built for non-flipped EXISTS subqueries, whose only
        // downstream consumer is a Join that always fetches with a constraint
        // built from the correlation's childField — which is Cap's partition
        // key. So either partitionKey is undefined, or constraint matches.
        if let Some(pk) = &self.partition_key
            && let Some(constraint) = &req.constraint
        {
            assert!(
                crate::ivm::take::constraint_matches_partition_key(constraint, pk),
                "Cap fetch: constraint must match partition key when partitioned"
            );
        }

        let state_key = self.get_take_state_key(None, req.constraint.as_ref());

        if let Some(cap_state) = self.storage.borrow().get(&state_key) {
            if cap_state.size == 0 {
                return from_vec(Vec::new());
            }
            let pks = cap_state.pks.clone();
            let input = self.input.clone();
            let req = req.clone();
            let primary_key = self.primary_key.clone();
            return Box::new(pks.into_iter().flat_map(move |pk| {
                let trimmed = pk.trim_start_matches('[').trim_end_matches(']');
                let parts = parse_json_array_elements(trimmed);
                let mut constraint = Constraint::default();
                for (i, part) in parts.iter().enumerate() {
                    if i < primary_key.len() {
                        constraint.insert(primary_key[i].clone(), parse_value(part));
                    }
                }
                let mut fetch_req = req.clone();
                fetch_req.constraint = Some(constraint);
                input.borrow().fetch(&fetch_req)
            }));
        }

        self.initial_fetch(req)
    }
}

impl Output for Cap {
    fn push(&mut self, _change: Change, _pusher: &dyn InputBase) {
        // Pushes arrive via CapOutput adapter
    }
}

/// Output adapter that receives pushes and applies the Cap limit logic.
struct CapOutput {
    cap: Shared<Cap>,
}

impl Output for CapOutput {
    fn push(&mut self, change: Change, pusher: &dyn InputBase) {
        let cap = self.cap.borrow();
        let output = cap.output.borrow().clone();

        let ct = change.change_type();

        match ct {
            ChangeType::Edit => {
                // Edit: check if partition key changed (should not for Cap).
                let (node, old_node) = match &change {
                    Change::Edit { node, old_node } => (node, old_node),
                    _ => unreachable!(),
                };

                // TS cap.ts:262 `assert(!partitionKeyComparator ||
                // partitionKeyComparator(old, new) === 0, 'Unexpected change of
                // partition key')` — a partition-key-changing edit is an invariant
                // violation (the source splits such edits into add/remove). Panic
                // (contained per-CG by catch_unwind → pipeline reset), matching TS.
                if let Some(pk_cols) = &cap.partition_key {
                    assert!(
                        pk_cols
                            .iter()
                            .all(|c| old_node.row.get(c) == node.row.get(c)),
                        "Unexpected change of partition key"
                    );
                }

                let state_key = cap.get_take_state_key(Some(&old_node.row), None);
                let cap_state = cap.storage.borrow().get(&state_key).cloned();

                if let Some(state) = cap_state {
                    let old_pk = cap.serialize_pk(&old_node.row);
                    let pk_set: HashSet<String> = state.pks.iter().cloned().collect();
                    if pk_set.contains(&old_pk) {
                        // Update the PK in our set if it changed.
                        let new_pk = cap.serialize_pk(&node.row);
                        if old_pk != new_pk {
                            let pks: Vec<String> = state
                                .pks
                                .iter()
                                .map(|p| {
                                    if p == &old_pk {
                                        new_pk.clone()
                                    } else {
                                        p.clone()
                                    }
                                })
                                .collect();
                            cap.storage.borrow_mut().set(
                                state_key,
                                CapState {
                                    size: state.size,
                                    pks,
                                },
                            );
                        }
                        drop(cap);
                        if let Some(output) = output {
                            output.borrow_mut().push(change, pusher);
                        }
                    }
                }
            }
            ChangeType::Add | ChangeType::Remove => {
                let node = change.node().clone();
                let state_key = cap.get_take_state_key(Some(&node.row), None);
                let cap_state = cap.storage.borrow().get(&state_key).cloned();

                let cap_state = match cap_state {
                    Some(s) => s,
                    None => return,
                };

                let pk = cap.serialize_pk(&node.row);

                if ct == ChangeType::Add {
                    if cap_state.size < cap.limit {
                        let mut pks = cap_state.pks.clone();
                        pks.push(pk);
                        cap.storage.borrow_mut().set(
                            state_key,
                            CapState {
                                size: cap_state.size + 1,
                                pks,
                            },
                        );
                        drop(cap);
                        if let Some(output) = output {
                            output.borrow_mut().push(change, pusher);
                        }
                    }
                    // Full — drop
                    return;
                }

                if ct == ChangeType::Remove {
                    let pk_index = cap_state.pks.iter().position(|p| p == &pk);
                    let pk_index = match pk_index {
                        Some(i) => i,
                        None => return, // Not in our set — drop
                    };

                    // Remove from set
                    let mut pks = cap_state.pks.clone();
                    pks.remove(pk_index);
                    let new_size = cap_state.size - 1;

                    // Try to refill: fetch from input with partition constraint,
                    // find first row NOT in PK set.
                    let pk_set: HashSet<String> = pks.iter().cloned().collect();
                    let constraint = cap.partition_key.as_ref().map(|pk_cols| {
                        let mut c = Constraint::default();
                        for col in pk_cols {
                            c.insert(
                                col.clone(),
                                node.row.get(col).cloned().unwrap_or(Value::Null),
                            );
                        }
                        c
                    });

                    let mut replacement: Option<Node> = None;
                    let fetch_req = FetchRequest {
                        constraint,
                        multi_constraints: Vec::new(),
                        start: None,
                        reverse: false,
                        ..Default::default()
                    };
                    for n in crate::ivm::stream::skip_yields(cap.input.borrow().fetch(&fetch_req)) {
                        let node_pk = cap.serialize_pk(&n.row);
                        if !pk_set.contains(&node_pk) {
                            replacement = Some(n);
                            break;
                        }
                    }

                    if let Some(rep) = replacement {
                        // Store state WITHOUT replacement during remove forward.
                        cap.storage.borrow_mut().set(
                            state_key.clone(),
                            CapState {
                                size: new_size,
                                pks: pks.clone(),
                            },
                        );
                        drop(cap);
                        if let Some(output) = &output {
                            output.borrow_mut().push(change.clone(), pusher);
                        }
                        // Now add replacement to set and forward the add.
                        let rep_pk = self.cap.borrow().serialize_pk(&rep.row);
                        pks.push(rep_pk);
                        self.cap.borrow_mut().storage.borrow_mut().set(
                            state_key,
                            CapState {
                                size: new_size + 1,
                                pks,
                            },
                        );
                        if let Some(output) = &output {
                            output.borrow_mut().push(make_add_change(rep), pusher);
                        }
                    } else {
                        cap.storage.borrow_mut().set(
                            state_key,
                            CapState {
                                size: new_size,
                                pks,
                            },
                        );
                        drop(cap);
                        if let Some(output) = output {
                            output.borrow_mut().push(change, pusher);
                        }
                    }
                }
            }
            ChangeType::Child => {
                let node = change.node().clone();
                let state_key = cap.get_take_state_key(Some(&node.row), None);
                let cap_state = cap.storage.borrow().get(&state_key).cloned();

                if let Some(state) = cap_state {
                    let pk = cap.serialize_pk(&node.row);
                    let pk_set: HashSet<String> = state.pks.iter().cloned().collect();
                    if pk_set.contains(&pk) {
                        drop(cap);
                        if let Some(output) = output {
                            output.borrow_mut().push(change, pusher);
                        }
                    }
                }
            }
        }
    }
}

/// Convert a Value to its JSON string representation for PK serialization.
fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::F64(n) => n.to_string(),
        // Escape backslashes and quotes so a string PK containing `"` (or `\`)
        // round-trips through `parse_json_array_elements` / `parse_value`
        // without corrupting element boundaries.
        Value::Str(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
        Value::Json(s) => s.to_string(),
    }
}

/// Inverse of the escaping in `value_to_string`: turn `\"` back into `"` and
/// `\\` back into `\` (any other `\x` yields `x`).
fn unescape_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next) = chars.next() {
                out.push(next);
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Parse a JSON string value back into a Value.
fn parse_value(s: &str) -> Value {
    let s = s.trim();
    if s == "null" {
        return Value::Null;
    }
    if s == "true" {
        return Value::Bool(true);
    }
    if s == "false" {
        return Value::Bool(false);
    }
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        return Value::Str(Arc::from(unescape_json_string(&s[1..s.len() - 1]).as_str()));
    }
    if let Ok(n) = s.parse::<f64>() {
        return Value::F64(n);
    }
    Value::Null
}

/// Parse JSON array elements, respecting quoted strings that may contain commas.
fn parse_json_array_elements(s: &str) -> Vec<String> {
    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut escape = false;
    for ch in s.chars() {
        if escape {
            current.push(ch);
            escape = false;
            continue;
        }
        if ch == '\\' && in_string {
            current.push(ch);
            escape = true;
            continue;
        }
        if ch == '"' {
            current.push(ch);
            in_string = !in_string;
            continue;
        }
        if ch == ',' && !in_string {
            elements.push(current.trim().to_string());
            current.clear();
            continue;
        }
        current.push(ch);
    }
    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() || !elements.is_empty() {
        elements.push(trimmed);
    }
    elements
}

// Rc removed — using Arc

#[cfg(test)]
mod pk_serialization_tests {
    use super::*;

    fn roundtrip(v: Value) -> Value {
        parse_value(&value_to_string(&v))
    }

    #[test]
    fn plain_string_pk_is_byte_identical_and_roundtrips() {
        // The common case (no special chars) must serialize exactly as before.
        assert_eq!(
            value_to_string(&Value::Str(Arc::from("abc-123"))),
            "\"abc-123\""
        );
        assert_eq!(
            roundtrip(Value::Str(Arc::from("abc-123"))),
            Value::Str(Arc::from("abc-123"))
        );
    }

    #[test]
    fn string_pk_with_quote_and_backslash_roundtrips() {
        for raw in [
            "a\"b",
            "a\\b",
            "he said \"hi\"",
            "c:\\path\\x",
            "trailing\\",
        ] {
            let v = Value::Str(Arc::from(raw));
            assert_eq!(roundtrip(v.clone()), v, "round-trip failed for {raw:?}");
        }
    }

    #[test]
    fn quoted_pk_does_not_break_array_element_split() {
        // Two string PKs, the first containing a comma and a quote, must split
        // into exactly two elements.
        let a = value_to_string(&Value::Str(Arc::from("x,\"y")));
        let b = value_to_string(&Value::Str(Arc::from("z")));
        let joined = format!("{a},{b}");
        let elems = parse_json_array_elements(&joined);
        assert_eq!(
            elems.len(),
            2,
            "expected 2 elements from {joined:?}, got {elems:?}"
        );
        assert_eq!(parse_value(&elems[0]), Value::Str(Arc::from("x,\"y")));
        assert_eq!(parse_value(&elems[1]), Value::Str(Arc::from("z")));
    }
}
