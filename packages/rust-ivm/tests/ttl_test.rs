//! Tests for ttl.ts — port of `zql/src/query/ttl.test.ts`.
//!
//! Tests: parseTTL, compareTTL, normalizeTTL, clampTTL.

use rust_ivm::builder::ttl::{
    clamp_ttl, compare_ttl, parse_ttl, MAX_TTL_MS,
};

// ---------------------------------------------------------------------------
// parseTTL
// ---------------------------------------------------------------------------

#[test]
fn test_parse_ttl_none() {
    assert_eq!(parse_ttl("none"), 0);
}

#[test]
fn test_parse_ttl_forever() {
    assert_eq!(parse_ttl("forever"), -1);
}

#[test]
fn test_parse_ttl_seconds() {
    assert_eq!(parse_ttl("1s"), 1000);
}

#[test]
fn test_parse_ttl_minutes() {
    assert_eq!(parse_ttl("1m"), 60 * 1000);
}

#[test]
fn test_parse_ttl_hours() {
    assert_eq!(parse_ttl("1h"), 60 * 60 * 1000);
}

#[test]
fn test_parse_ttl_days() {
    assert_eq!(parse_ttl("1d"), 24 * 60 * 60 * 1000);
}

#[test]
fn test_parse_ttl_years() {
    assert_eq!(parse_ttl("1y"), 365 * 24 * 60 * 60 * 1000);
}

#[test]
fn test_parse_ttl_fractional_seconds() {
    assert_eq!(parse_ttl("1.5s"), 1500);
}

#[test]
fn test_parse_ttl_fractional_minutes() {
    assert_eq!(parse_ttl("1.5m"), (1.5 * 60.0 * 1000.0) as i64);
}

#[test]
fn test_parse_ttl_fractional_hours() {
    assert_eq!(parse_ttl("1.5h"), (1.5 * 60.0 * 60.0 * 1000.0) as i64);
}

#[test]
fn test_parse_ttl_fractional_days() {
    assert_eq!(parse_ttl("1.5d"), (1.5 * 24.0 * 60.0 * 60.0 * 1000.0) as i64);
}

#[test]
fn test_parse_ttl_fractional_years() {
    assert_eq!(parse_ttl("1.5y"), (1.5 * 365.0 * 24.0 * 60.0 * 60.0 * 1000.0) as i64);
}

// ---------------------------------------------------------------------------
// compareTTL
// ---------------------------------------------------------------------------

#[test]
fn test_compare_ttl_equal() {
    assert_eq!(compare_ttl("none", "none"), 0);
    assert_eq!(compare_ttl("forever", "forever"), 0);
    assert_eq!(compare_ttl("1s", "1s"), 0);
}

#[test]
fn test_compare_ttl_none_vs_forever() {
    assert_eq!(compare_ttl("none", "forever"), -1);
}

#[test]
fn test_compare_ttl_none_vs_zero() {
    assert_eq!(compare_ttl("none", "0"), 0);
}

#[test]
fn test_compare_ttl_forever_vs_forever() {
    assert_eq!(compare_ttl("forever", "forever"), 0);
}

#[test]
fn test_compare_ttl_1_vs_2() {
    assert_eq!(compare_ttl("1", "2"), -1);
}

#[test]
fn test_compare_ttl_1000_vs_1s() {
    assert_eq!(compare_ttl("1000", "1s"), 0);
}

#[test]
fn test_compare_ttl_1s_vs_1m() {
    assert_eq!(compare_ttl("1s", "1m"), 1000 - 60 * 1000);
}

// ---------------------------------------------------------------------------
// clampTTL
// ---------------------------------------------------------------------------

#[test]
fn test_clamp_ttl_none() {
    assert_eq!(clamp_ttl("none"), 0);
}

#[test]
fn test_clamp_ttl_forever_clamped() {
    assert_eq!(clamp_ttl("forever"), MAX_TTL_MS as i64);
}

#[test]
fn test_clamp_ttl_zero() {
    assert_eq!(clamp_ttl("0"), 0);
}

#[test]
fn test_clamp_ttl_minus_one_clamped() {
    // Rust TTL takes &str, so -1 is passed as string "-1"
    // parse_ttl("-1") → tries to parse as number → -1
    // clamp_ttl: parsed == -1 || parsed > MAX → returns MAX
    assert_eq!(clamp_ttl("-1"), MAX_TTL_MS as i64);
}

#[test]
fn test_clamp_ttl_within_bounds() {
    assert_eq!(clamp_ttl("1"), 1);
}

#[test]
fn test_clamp_ttl_1000() {
    assert_eq!(clamp_ttl("1000"), 1000);
}

#[test]
fn test_clamp_ttl_exactly_at_max() {
    assert_eq!(clamp_ttl("10m"), MAX_TTL_MS as i64);
}

#[test]
fn test_clamp_ttl_just_above_max() {
    // 1h is above MAX_TTL (10m)
    assert_eq!(clamp_ttl("1h"), MAX_TTL_MS as i64);
}

#[test]
fn test_clamp_ttl_1m_within_bounds() {
    assert_eq!(clamp_ttl("1m"), 60 * 1000);
}
