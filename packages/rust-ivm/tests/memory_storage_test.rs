//! Tests for memory-storage.ts — port of `zql/src/ivm/memory-storage.test.ts`.
//!
//! Tests: basics, default, other types, scan.

use rust_ivm::ivm::data::Value;
use rust_ivm::ivm::memory_storage::MemoryStorage;
use rust_ivm::ivm::operator::Storage;

// ---------------------------------------------------------------------------
// basics
// ---------------------------------------------------------------------------

#[test]
fn test_basics() {
    let mut ms = MemoryStorage::new();
    assert_eq!(ms.get("foo"), None);
    assert_eq!(ms.get("bar"), None);
    assert_eq!(ms.get("baz"), None);

    ms.set("foo".into(), Value::Str("bar".into()));
    ms.set("bar".into(), Value::Str("baz".into()));
    ms.set("baz".into(), Value::Str("monkey".into()));

    assert_eq!(ms.get("foo"), Some(Value::Str("bar".into())));
    assert_eq!(ms.get("bar"), Some(Value::Str("baz".into())));
    assert_eq!(ms.get("baz"), Some(Value::Str("monkey".into())));

    ms.del("foo");
    ms.del("bar");
    ms.del("baz");

    assert_eq!(ms.get("foo"), None);
    assert_eq!(ms.get("bar"), None);
    assert_eq!(ms.get("baz"), None);
}

// ---------------------------------------------------------------------------
// default — the Rust Storage trait has no default param, but we can
// emulate by checking get before/after set.
// ---------------------------------------------------------------------------

#[test]
fn test_default_returns_none_before_set() {
    let ms = MemoryStorage::new();
    // Before set: get returns None (no default param in Rust trait).
    assert_eq!(ms.get("foo"), None);
}

#[test]
fn test_default_returns_value_after_set() {
    let mut ms = MemoryStorage::new();
    ms.set("foo".into(), Value::Str("baz".into()));
    assert_eq!(ms.get("foo"), Some(Value::Str("baz".into())));
}

// ---------------------------------------------------------------------------
// other types
// ---------------------------------------------------------------------------

#[test]
fn test_other_types() {
    let mut ms = MemoryStorage::new();
    ms.set("foo".into(), Value::F64(1.0));
    ms.set("bar".into(), Value::Bool(true));
    ms.set("baz".into(), Value::Null);
    ms.set("qux".into(), Value::Json("{\"a\":1}".into()));
    ms.set("quux".into(), Value::Str("[1,2,3]".into()));

    assert_eq!(ms.get("foo"), Some(Value::F64(1.0)));
    assert_eq!(ms.get("bar"), Some(Value::Bool(true)));
    assert_eq!(ms.get("baz"), Some(Value::Null));
    assert!(matches!(
        ms.get("qux"),
        Some(Value::Json(value)) if value.as_ref() == "{\"a\":1}"
    ));
    assert_eq!(ms.get("quux"), Some(Value::Str("[1,2,3]".into())));
}

// ---------------------------------------------------------------------------
// scan
// ---------------------------------------------------------------------------

#[test]
fn test_scan_all() {
    let mut ms = MemoryStorage::new();
    ms.set("foo".into(), Value::F64(1.0));
    ms.set("bar".into(), Value::Bool(true));
    ms.set("baz".into(), Value::Null);
    ms.set("qux".into(), Value::Json("{\"a\":1}".into()));
    ms.set("quux".into(), Value::Str("[1,2,3]".into()));

    let result = ms.scan(None);
    // scan returns all entries — check count and that all keys are present
    assert_eq!(result.len(), 5);
    let keys: Vec<&str> = result.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"foo"));
    assert!(keys.contains(&"bar"));
    assert!(keys.contains(&"baz"));
    assert!(keys.contains(&"qux"));
    assert!(keys.contains(&"quux"));
}

#[test]
fn test_scan_prefix_ba() {
    let mut ms = MemoryStorage::new();
    ms.set("foo".into(), Value::F64(1.0));
    ms.set("bar".into(), Value::Bool(true));
    ms.set("baz".into(), Value::Null);
    ms.set("qux".into(), Value::Json("{\"a\":1}".into()));
    ms.set("quux".into(), Value::Str("[1,2,3]".into()));

    let result = ms.scan(Some("ba"));
    let keys: Vec<&str> = result.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"bar"));
    assert!(keys.contains(&"baz"));
    assert!(!keys.contains(&"foo"));
    assert!(!keys.contains(&"qux"));
    assert!(!keys.contains(&"quux"));
}

#[test]
fn test_scan_prefix_qu() {
    let mut ms = MemoryStorage::new();
    ms.set("foo".into(), Value::F64(1.0));
    ms.set("bar".into(), Value::Bool(true));
    ms.set("baz".into(), Value::Null);
    ms.set("qux".into(), Value::Json("{\"a\":1}".into()));
    ms.set("quux".into(), Value::Str("[1,2,3]".into()));

    let result = ms.scan(Some("qu"));
    let keys: Vec<&str> = result.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"qux"));
    assert!(keys.contains(&"quux"));
    assert!(!keys.contains(&"foo"));
}

#[test]
fn test_scan_prefix_quu() {
    let mut ms = MemoryStorage::new();
    ms.set("foo".into(), Value::F64(1.0));
    ms.set("quux".into(), Value::Str("[1,2,3]".into()));
    ms.set("qux".into(), Value::Json("{\"a\":1}".into()));

    let result = ms.scan(Some("quu"));
    let keys: Vec<&str> = result.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&"quux"));
    assert!(!keys.contains(&"qux"));
}
