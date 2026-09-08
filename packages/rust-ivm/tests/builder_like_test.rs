//! Tests for like.ts — port of `zql/src/builder/like.test.ts`.
//!
//! Uses the same test cases as `like-test-cases.ts`.

use rust_ivm::builder::like::get_like_predicate;
use rust_ivm::ivm::data::Value;

/// A test case: pattern, flags, and (input, expected) pairs.
struct Case {
    pattern: &'static str,
    flags: &'static str,
    inputs: &'static [(&'static str, bool)],
}

const CASES: &[Case] = &[
    Case {
        pattern: "foo",
        flags: "",
        inputs: &[
            ("foo", true),
            ("bar", false),
            ("Foo", false),
            ("FOO", false),
            ("fo", false),
            ("fooa", false),
            ("afoo", false),
            ("afoob", false),
        ],
    },
    Case {
        pattern: "foo",
        flags: "i",
        inputs: &[
            ("foo", true),
            ("bar", false),
            ("Foo", true),
            ("FOO", true),
            ("fo", false),
            ("fooa", false),
            ("afoo", false),
            ("afoob", false),
        ],
    },
    Case {
        pattern: "foo%",
        flags: "",
        inputs: &[
            ("foo", true),
            ("foobar", true),
            ("bar", false),
            ("Foo", false),
            ("FOO", false),
            ("fo", false),
            ("fooa", true),
            ("afoo", false),
            ("afoob", false),
        ],
    },
    Case {
        pattern: "foo%",
        flags: "i",
        inputs: &[
            ("foo", true),
            ("foobar", true),
            ("bar", false),
            ("Foo", true),
            ("FOO", true),
            ("fo", false),
            ("fooa", true),
            ("afoo", false),
            ("afoob", false),
        ],
    },
    Case {
        pattern: "foo_",
        flags: "",
        inputs: &[
            ("foo", false),
            ("foobar", false),
            ("foob", true),
            ("bar", false),
            ("Foo", false),
            ("FOO", false),
            ("fo", false),
            ("afoo", false),
            ("afoob", false),
        ],
    },
    Case {
        pattern: "foo\\%",
        flags: "",
        inputs: &[
            ("foo%", true),
            ("foobar", false),
            ("bar", false),
            ("Foo", false),
            ("FOO", false),
            ("fo", false),
            ("fooa", false),
            ("afoo", false),
            ("afoob", false),
        ],
    },
    Case {
        pattern: "foo\\%",
        flags: "i",
        inputs: &[
            ("foo%", true),
            ("FOO%", true),
            ("foobar", false),
            ("bar", false),
            ("Foo", false),
            ("FOO", false),
            ("fo", false),
            ("fooa", false),
            ("afoob", false),
        ],
    },
    Case {
        pattern: "foo\\_",
        flags: "",
        inputs: &[
            ("foo_", true),
            ("FOO_", false),
            ("foobar", false),
            ("bar", false),
            ("Foo", false),
            ("FOO", false),
            ("fo", false),
            ("fooa", false),
            ("afoo", false),
            ("afoob", false),
        ],
    },
    Case {
        pattern: "%foo",
        flags: "",
        inputs: &[
            ("foo", true),
            ("foobar", false),
            ("bar", false),
            ("Foo", false),
            ("FOO", false),
            ("fo", false),
            ("fooa", false),
            ("afoo", true),
            ("afoob", false),
        ],
    },
    Case {
        pattern: "%foo%",
        flags: "",
        inputs: &[
            ("foo", true),
            ("foobar", true),
            ("bar", false),
            ("Foo", false),
            ("FOO", false),
            ("fo", false),
            ("fooa", true),
            ("afoo", true),
            ("afoob", true),
        ],
    },
];

#[test]
fn test_like_basics() {
    for case in CASES {
        let pred = get_like_predicate(&Value::Str(case.pattern.into()), case.flags);
        for (input, expected) in case.inputs {
            let result = pred(&Value::Str((*input).into()));
            assert_eq!(
                result, *expected,
                "pattern={:?}, flags={:?}, input={:?}",
                case.pattern, case.flags, input
            );
        }
    }
}
