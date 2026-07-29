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
pub fn get_like_predicate(
    pattern: &crate::ivm::data::Value,
    flags: &str,
) -> SimplePredicate {
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

    // For ILIKE, lowercase the pattern before building the regex and
    // lowercase the input before matching, since regex-lite may not
    // support Unicode case-insensitive matching.
    let effective_pattern = if flags == "i" {
        pattern.to_lowercase()
    } else {
        pattern.to_string()
    };
    let re = pattern_to_regex(&effective_pattern, flags);
    if flags == "i" {
        Box::new(move |lhs: &str| re.is_match(&lhs.to_lowercase()))
    } else {
        Box::new(move |lhs: &str| re.is_match(lhs))
    }
}

fn pattern_to_regex(source: &str, flags: &str) -> regex_lite::Regex {
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

    let re_flags = if flags == "i" { "is" } else { "s" };
    regex_lite::Regex::new(&format!("(?{}){}", re_flags, pattern))
        .expect("invalid LIKE pattern regex")
}

fn is_special_regex_char(c: char) -> bool {
    matches!(c, '$' | '(' | ')' | '*' | '+' | '.' | '?' | '[' | ']' | '\\' | '^' | '{' | '|' | '}')
}
