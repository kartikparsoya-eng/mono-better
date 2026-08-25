//! LIKE predicate — port of `zql/src/builder/like.ts`.
//!
//! Converts SQL LIKE/ILIKE patterns to predicates:
//! - `%` → `.*`
//! - `_` → `.`
//! - `\x` → escaped literal
//! - No wildcards → simple string comparison

/// A predicate function type.
pub type SimplePredicate = Box<dyn Fn(&crate::ivm::data::Value) -> bool>;

/// Get a LIKE predicate.
/// `flags` = "" for LIKE (case-sensitive), "i" for ILIKE (case-insensitive).
/// Port of TS `getLikePredicate` (like.ts:4).
pub fn get_like_predicate(pattern: &crate::ivm::data::Value, flags: &str) -> SimplePredicate {
    let pattern_str = match pattern {
        crate::ivm::data::Value::Str(s) => s.to_string(),
        _ => panic!("LIKE pattern must be a string"),
    };

    let op = get_like_op(&pattern_str, flags);
    Box::new(move |lhs: &crate::ivm::data::Value| {
        let lhs_str = match lhs {
            crate::ivm::data::Value::Str(s) => s.to_string(),
            _ => return false,
        };
        op(&lhs_str)
    })
}

fn get_like_op(pattern: &str, flags: &str) -> Box<dyn Fn(&str) -> bool + 'static> {
    let has_wildcards = pattern.contains('%') || pattern.contains('_') || pattern.contains('\\');

    if !has_wildcards {
        if flags == "i" {
            let rhs_lower = pattern.to_lowercase();
            return Box::new(move |lhs: &str| lhs.to_lowercase() == rhs_lower);
        }
        let pattern = pattern.to_string();
        return Box::new(move |lhs: &str| lhs == pattern);
    }

    // For ILIKE, TS builds `new RegExp(pattern, 'i')` and tests the ORIGINAL
    // strings, so the match uses Unicode case FOLDING (e.g. capital Σ folds with
    // final sigma ς). The `regex` crate's case-insensitive mode uses the same
    // Unicode simple case folding as JS regex `i`, so we build the regex from the
    // un-lowercased pattern and match the un-lowercased input — NOT `to_lowercase`
    // both sides, which diverges from folding for chars whose lowercase ≠ their
    // fold (final sigma, etc.). Port of TS `patternToRegExp(pattern, flags)`.
    let re = pattern_to_regex(pattern, flags);
    Box::new(move |lhs: &str| re.is_match(lhs))
}

fn pattern_to_regex(source: &str, flags: &str) -> regex::Regex {
    let mut pattern = String::from("^");
    let chars: Vec<char> = source.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '%' => pattern.push_str(".*"),
            '_' => pattern.push('.'),
            '\\' => {
                i += 1;
                if i >= chars.len() {
                    panic!("LIKE pattern must not end with escape character");
                }
                let escaped = chars[i];
                if is_special_regex_char(escaped) {
                    pattern.push('\\');
                }
                pattern.push(escaped);
            }
            _ => {
                if is_special_regex_char(c) {
                    pattern.push('\\');
                }
                pattern.push(c);
            }
        }
        i += 1;
    }
    pattern.push('$');

    // TS `new RegExp(pattern + '$', flags + 's')`: dotall always on; `i` when
    // ILIKE. `regex` (unlike regex-lite) does Unicode-aware `i` case folding.
    regex::RegexBuilder::new(&pattern)
        .case_insensitive(flags == "i")
        .dot_matches_new_line(true)
        .build()
        .expect("invalid LIKE pattern regex")
}

fn is_special_regex_char(c: char) -> bool {
    matches!(
        c,
        '$' | '(' | ')' | '*' | '+' | '.' | '?' | '[' | ']' | '\\' | '^' | '{' | '|' | '}'
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ivm::data::Value;
    use std::sync::Arc;

    fn ilike(pattern: &str, lhs: &str) -> bool {
        let p = get_like_predicate(&Value::Str(Arc::from(pattern)), "i");
        p(&Value::Str(Arc::from(lhs)))
    }

    #[test]
    fn ilike_uses_unicode_case_folding_like_ts() {
        // Values verified against the REAL TS `getLikePredicate` (F-LIKE-1).
        // Discriminator: capital Σ folds with final sigma ς under JS regex `i`,
        // so this is TRUE — the OLD `to_lowercase` impl returned FALSE (the
        // lowercase of Σ is σ, not ς). Non-vacuous per HARD RULE #7.
        assert!(ilike("a%Σ", "aXς"));
        assert!(ilike("%ΣΟΣ%", "xx σος yy"));
        // ß does NOT fold to `ss` under simple case folding (matches JS regex `i`).
        assert!(!ilike("STRASSE", "straße"));
        // No-wildcard ILIKE fast path uses Unicode lowercase on both sides.
        assert!(ilike("σος", "ΣΟΣ"));
        // ASCII sanity.
        assert!(ilike("HELLO%", "hello world"));
        assert!(!ilike("bye%", "hello"));
    }
}
