//! TTL — port of `zql/src/query/ttl.ts`.
//!
//! Time To Live for query expiration. Parsed to milliseconds.

/// Default TTL: 5 minutes.
pub const DEFAULT_TTL_MS: usize = 1_000 * 60 * 5;
/// Max TTL: 10 minutes.
pub const MAX_TTL_MS: usize = 1_000 * 60 * 10;

/// Parse a TTL string (e.g. "5m", "1h", "forever", "none") into milliseconds.
/// Port of TS `parseTTL` (ttl.ts:40).
pub fn parse_ttl(ttl: &str) -> i64 {
    if ttl == "none" {
        return 0;
    }
    if ttl == "forever" {
        return -1;
    }

    let unit = ttl.chars().last().unwrap_or('0');
    let multiplier = match unit {
        's' => 1000.0,
        'm' => 60.0 * 1000.0,
        'h' => 60.0 * 60.0 * 1000.0,
        'd' => 24.0 * 60.0 * 60.0 * 1000.0,
        'y' => 365.0 * 24.0 * 60.0 * 60.0 * 1000.0,
        _ => return ttl.parse::<i64>().unwrap_or(0),
    };

    let num_str = &ttl[..ttl.len() - 1];
    let num: f64 = num_str.parse::<f64>().unwrap_or(0.0);
    (num * multiplier) as i64
}

/// Clamp TTL to the maximum allowed (10 minutes).
/// Port of TS `clampTTL` (ttl.ts:95).
pub fn clamp_ttl(ttl: &str) -> i64 {
    let parsed = parse_ttl(ttl);
    if parsed == -1 || parsed > MAX_TTL_MS as i64 {
        return MAX_TTL_MS as i64;
    }
    parsed
}

/// Compare two TTL values by their parsed millisecond values.
/// Returns positive if a > b, negative if a < b, 0 if equal.
/// Port of TS `compareTTL` (ttl.ts:63).
pub fn compare_ttl(a: &str, b: &str) -> i64 {
    let ap = parse_ttl(a);
    let bp = parse_ttl(b);
    if ap == -1 && bp != -1 {
        return 1;
    }
    if ap != -1 && bp == -1 {
        return -1;
    }
    ap - bp
}
