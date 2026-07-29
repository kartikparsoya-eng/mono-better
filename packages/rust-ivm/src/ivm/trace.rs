//! Env-gated pipeline trace harness for debugging push routing.
//!
//! Enable with `IVM_TRACE=1`. Zero cost when off (one cached bool check).
//! Instrument an operator's `push`/emit with:
//!     crate::ivm::trace::recv("FanIn", &change);
//!     crate::ivm::trace::emit("Exists", &change);
//! and read the flow as a top-to-bottom log of who received/emitted what.
//!
//! Intended for tracing the OR-with-EXISTS child-routing regressions
//! (agentic/fixtures/regressions). Not part of the production path.

use std::sync::OnceLock;

use crate::ivm::change::Change;
use crate::ivm::data::{Node, Value};

static ENABLED: OnceLock<bool> = OnceLock::new();

#[inline]
pub fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var("IVM_TRACE").is_ok())
}

fn id(n: &Node) -> String {
    match n.row.get("id") {
        Some(Value::Str(s)) => s.to_string(),
        Some(v) => format!("{v:?}"),
        None => "?".to_string(),
    }
}

/// Compact one-line description of a change (recurses into CHILD).
pub fn describe(change: &Change) -> String {
    match change {
        Change::Add(n) => format!("ADD({})", id(n)),
        Change::Remove(n) => format!("REMOVE({})", id(n)),
        Change::Edit { node, .. } => format!("EDIT({})", id(node)),
        Change::Child { node, child } => format!(
            "CHILD({} rel={} -> {})",
            id(node),
            child.relationship_name,
            describe(&child.change)
        ),
    }
}

/// Log a change an operator RECEIVED (from its upstream).
#[inline]
pub fn recv(op: &str, change: &Change) {
    if enabled() {
        eprintln!("[ivm-trace] {op:14} recv  {}", describe(change));
    }
}

/// Log a change an operator EMITTED (to its downstream).
#[inline]
pub fn emit(op: &str, change: &Change) {
    if enabled() {
        eprintln!("[ivm-trace] {op:14} EMIT  {}", describe(change));
    }
}

/// Log a free-form event.
#[inline]
pub fn note(op: &str, msg: &str) {
    if enabled() {
        eprintln!("[ivm-trace] {op:14} {msg}");
    }
}
