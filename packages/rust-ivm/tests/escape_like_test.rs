//! Tests for escape-like.ts — port of `zql/src/query/escape-like.test.ts`.

use rust_ivm::builder::escape_like::escape_like;

#[test]
fn test_escape_like_basics() {
    let cases: &[(&str, &str)] = &[
        ("", ""),
        ("foo", "foo"),
        ("%", "\\%"),
        ("%_", "\\%\\_"),
        ("%_foo_%", "\\%\\_foo\\_\\%"),
    ];

    for (input, expected) in cases {
        assert_eq!(
            escape_like(input),
            *expected,
            "input: {:?}",
            input
        );
    }
}
