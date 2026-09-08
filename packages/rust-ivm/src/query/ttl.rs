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
        // No unit suffix: outside the TS TTL type contract (TS computes
        // `Number(slice) * undefined` = NaN there). Both Rust ports extend
        // this defensively and MUST agree with each other — f64, matching the
        // LIVE `rust_cvr::ttl` impl's `Number(...)` semantics (the previous
        // i64 parse dropped fractional inputs to 0 — see cross_impl_tests).
        _ => return ttl.parse::<f64>().map(|n| n as i64).unwrap_or(0),
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

#[cfg(test)]
mod cross_impl_tests {
    /// The same TS origin (`zql/src/query/ttl.ts`) is ported twice — here (the
    /// 1:1 file) and in `rust_cvr::ttl` (the LIVE path: clamp/parse ran 99x in
    /// the L8 traffic capture while this file ran 0x). Two copies of one spec
    /// is exactly the drift shape that produced the FxHasher signature bug, so
    /// pin them to each other over the whole input space (unit strings, float
    /// quantities, keywords, and the out-of-TS-type unit-less fallback where
    /// the copies had ALREADY drifted: "1500.5" parsed to 0 here vs 1500 in
    /// rust_cvr before this test's fix).
    #[test]
    fn parse_and_clamp_agree_with_the_live_rust_cvr_impl() {
        let corpus = [
            "none", "forever", "5s", "5m", "1.5m", "2h", "3d", "1y", "0s", "0.25h", "12345",
            "1500.5", "-1", "", "10m", "11m", "600001",
        ];
        for s in corpus {
            let cvr_parsed = rust_cvr::ttl::parse_ttl(rust_cvr::ttl::parse_ttl_string(s));
            assert_eq!(
                super::parse_ttl(s),
                cvr_parsed,
                "parse_ttl diverges from rust_cvr::ttl on {s:?}"
            );
            let cvr_clamped = rust_cvr::ttl::clamp_ttl(rust_cvr::ttl::parse_ttl_string(s));
            assert_eq!(
                super::clamp_ttl(s),
                cvr_clamped,
                "clamp_ttl diverges from rust_cvr::ttl on {s:?}"
            );
        }
    }
}
