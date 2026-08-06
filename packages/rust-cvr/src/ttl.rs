//! Port of `packages/zql/src/query/ttl.ts`.
//!
//! TTL (Time To Live) for query expiration. All values are in milliseconds.
//! Negative values mean "forever" (never expire).

pub const DEFAULT_TTL_MS: i64 = 1_000 * 60 * 5; // 5 minutes
pub const MAX_TTL_MS: i64 = 1_000 * 60 * 10; // 10 minutes

/// TTL can be a number (milliseconds), or a string like "5m", "1h", "forever", "none".
/// In the CVR context, TTL is always already parsed to a number by the time it reaches
/// the updater. This module provides the parsing and comparison for completeness.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TTL {
    Ms(i64),
    Forever,
    None,
}

const MULT_S: i64 = 1000;
const MULT_M: i64 = 60 * 1000;
const MULT_H: i64 = 60 * 60 * 1000;
const MULT_D: i64 = 24 * 60 * 60 * 1000;
const MULT_Y: i64 = 365 * 24 * 60 * 60 * 1000;

/// Parse a TTL value into milliseconds.
/// Returns -1 for "forever", 0 for "none".
pub fn parse_ttl(ttl: TTL) -> i64 {
    match ttl {
        TTL::Ms(n) => {
            if n < 0 {
                -1
            } else {
                n
            }
        }
        TTL::Forever => -1,
        TTL::None => 0,
    }
}

/// Compare two TTL values. Returns positive if a > b (a lives longer).
pub fn compare_ttl(a: TTL, b: TTL) -> i64 {
    let ap = parse_ttl(a);
    let bp = parse_ttl(b);
    if ap == -1 && bp != -1 {
        1
    } else if ap != -1 && bp == -1 {
        -1
    } else {
        ap - bp
    }
}

/// Clamp TTL to max 10 minutes. "forever" becomes 10 minutes.
/// Returns the clamped TTL in milliseconds.
pub fn clamp_ttl(ttl: TTL) -> i64 {
    let parsed = parse_ttl(ttl);
    if parsed == -1 || parsed > MAX_TTL_MS {
        MAX_TTL_MS
    } else {
        parsed
    }
}

/// Parse a string TTL like "5m", "1h", "forever", "none" into a TTL enum.
pub fn parse_ttl_string(s: &str) -> TTL {
    if s == "forever" {
        return TTL::Forever;
    }
    if s == "none" {
        return TTL::None;
    }
    let last = s.chars().last();
    if let Some(unit) = last {
        let multi = match unit {
            's' => MULT_S,
            'm' => MULT_M,
            'h' => MULT_H,
            'd' => MULT_D,
            'y' => MULT_Y,
            _ => {
                // Pure number
                if let Ok(n) = s.parse::<i64>() {
                    return TTL::Ms(n);
                }
                return TTL::Ms(0);
            }
        };
        let num_str = &s[..s.len() - 1];
        if let Ok(n) = num_str.parse::<i64>() {
            return TTL::Ms(n * multi);
        }
    }
    TTL::Ms(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ttl_numbers() {
        assert_eq!(parse_ttl(TTL::Ms(5000)), 5000);
        assert_eq!(parse_ttl(TTL::Ms(0)), 0);
        assert_eq!(parse_ttl(TTL::Ms(-1)), -1);
        assert_eq!(parse_ttl(TTL::Ms(-100)), -1);
        assert_eq!(parse_ttl(TTL::Forever), -1);
        assert_eq!(parse_ttl(TTL::None), 0);
    }

    #[test]
    fn test_compare_ttl() {
        assert!(compare_ttl(TTL::Forever, TTL::Ms(5000)) > 0);
        assert!(compare_ttl(TTL::Ms(5000), TTL::Forever) < 0);
        assert_eq!(compare_ttl(TTL::Ms(5000), TTL::Ms(5000)), 0);
        assert!(compare_ttl(TTL::Ms(10000), TTL::Ms(5000)) > 0);
    }

    #[test]
    fn test_clamp_ttl() {
        assert_eq!(clamp_ttl(TTL::Ms(5000)), 5000);
        assert_eq!(clamp_ttl(TTL::Forever), MAX_TTL_MS);
        assert_eq!(clamp_ttl(TTL::Ms(MAX_TTL_MS + 1)), MAX_TTL_MS);
        assert_eq!(clamp_ttl(TTL::Ms(MAX_TTL_MS)), MAX_TTL_MS);
        assert_eq!(clamp_ttl(TTL::Ms(0)), 0);
    }

    #[test]
    fn test_parse_ttl_string() {
        assert_eq!(parse_ttl_string("5m"), TTL::Ms(300_000));
        assert_eq!(parse_ttl_string("1s"), TTL::Ms(1000));
        assert_eq!(parse_ttl_string("1h"), TTL::Ms(3_600_000));
        assert_eq!(parse_ttl_string("forever"), TTL::Forever);
        assert_eq!(parse_ttl_string("none"), TTL::None);
        assert_eq!(parse_ttl_string("100"), TTL::Ms(100));
    }
}
