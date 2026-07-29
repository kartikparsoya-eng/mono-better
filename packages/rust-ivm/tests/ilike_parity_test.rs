//! ILIKE parity test — port of TS `ilike-parity.test.ts` (v1.7.0).
//!
//! Verifies that the client-side IVM matcher (get_like_predicate, Rust
//! str::to_lowercase) and the SQLite-backed ILIKE (`lower(col) LIKE
//! lower(pattern)`) produce identical results.
//!
//! NOTE: Standard SQLite (without ICU extension) only handles ASCII in
//! lower(). Unicode case-folding tests are IVM-only since they require
//! ICU which rusqlite's bundled SQLite may not include.

use std::sync::Arc;
use rusqlite::Connection;

use rust_ivm::builder::like::get_like_predicate;
use rust_ivm::ivm::data::Value;

fn ivm_ilike(pattern: &str, input: &str) -> bool {
    let pred = get_like_predicate(&Value::Str(std::sync::Arc::from(pattern)), "i");
    pred(&Value::Str(std::sync::Arc::from(input)))
}

fn zqlite_ilike(conn: &Connection, pattern: &str, input: &str) -> bool {
    let sql = "SELECT (lower(name) LIKE lower(?) ESCAPE '\\') AS m FROM (SELECT ? AS name)";
    let result: rusqlite::Result<i64> = conn.query_row(sql, rusqlite::params![pattern, input], |row| {
        row.get(0)
    });
    result.unwrap_or(0) != 0
}

// Cases where both IVM and SQLite should agree (ASCII-only or no-case-folding)
const PARITY_CASES: &[(&str, &str)] = &[
    // Wildcards
    ("m%r", "MÜLLER"), // % matches ÜLLER (wildcard, no folding needed)
    ("x%", "MÜLLER"),  // no match
    // % and _ span newlines, matching SQLite LIKE.
    ("a%b", "a\nb"),
    ("a_b", "a\nb"),
    // Backslash escapes: \% and \_ are literal.
    (r"100\%", "100%"),
    (r"100\%", "100x"), // no match (literal %)
    (r"a\_b", "a_b"),
    (r"a\_b", "axb"), // no match (literal _)
    // ASCII case-insensitive
    ("foo", "FOO"),
    ("FOO", "foo"),
    ("foo", "bar"), // no match
];

// IVM-only cases: Unicode case-folding that requires ICU in SQLite.
// We verify the IVM matcher works correctly for these.
const IVM_ONLY_CASES: &[(&str, &str, bool)] = &[
    ("müller", "MÜLLER", true),
    ("MÜLLER", "müller", true),
    ("café", "CAFÉ", true),
    ("привет", "ПРИВЕТ", true),
    ("σιγμα", "ΣΙΓΜΑ", true),
    ("müller", "schmidt", false),
    ("å", "Ä", false),
    ("m_ller", "müller", true),  // _ matches ü
    ("%Ü%", "müller", true),     // wildcard + case-insensitive
    ("straße", "STRASSE", false), // ß vs SS: lower differs from fold
];

#[test]
fn test_ilike_parity_ascii() {
    let conn = Connection::open_in_memory().unwrap();
    for &(pattern, input) in PARITY_CASES {
        let ivm = ivm_ilike(pattern, input);
        let sqlite = zqlite_ilike(&conn, pattern, input);
        assert_eq!(
            ivm, sqlite,
            "ILIKE mismatch: pattern={:?} input={:?} → ivm={} sqlite={}",
            pattern, input, ivm, sqlite
        );
    }
}

#[test]
fn test_ilike_ivm_unicode() {
    for &(pattern, input, expected) in IVM_ONLY_CASES {
        let result = ivm_ilike(pattern, input);
        assert_eq!(
            result, expected,
            "IVM ILIKE: pattern={:?} input={:?} → got={} expected={}",
            pattern, input, result, expected
        );
    }
}
