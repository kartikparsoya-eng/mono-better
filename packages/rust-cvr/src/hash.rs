//! TS-parity port of `packages/shared/src/hash.ts`.
//!
//! Contract: `h32(s) == xxHash32(s, seed=0)`, `h128(s) == chained xxHash32` over
//! seeds `0..WORDS` where `WORDS = 4`. Rust uses `xxhash-rust` which yields the
//! same 32-bit digest per call.
//!
//! ## Why chained seeds instead of xxHash64? TS was written when `js-xxhash`
//! only exposed `xxHash32`; the project has kept this construction for
//! byte-compat reasons. Rust mirrors it (do NOT switch to `xxh3_64` here).

/// One-shot xxHash32 with the given seed.
#[inline]
fn xxh32_seeded(data: &[u8], seed: u32) -> u32 {
    use xxhash_rust::xxh32::xxh32;
    xxh32(data, seed)
}

/// Mirrors TS `h32(s) = xxHash32(s, 0)`.
pub fn h32(s: &str) -> u32 {
    xxh32_seeded(s.as_bytes(), 0)
}

/// Mirrors TS `h64(s) = h128_with_words(s, 2)`. Returns a `u64` whose high
/// 32 bits are xxHash32(s, 0) and low 32 bits are xxHash32(s, 1).
pub fn h64(s: &str) -> u64 {
    let data = s.as_bytes();
    let hi = xxh32_seeded(data, 0) as u64;
    let lo = xxh32_seeded(data, 1) as u64;
    (hi << 32) | lo
}

/// Mirrors TS `h128(s) = h128_with_words(s, 4)`. Returns a `u128` formed by
/// left-shifting each xxHash32 with seeds 0..4 into accumulated high→low order.
pub fn h128(s: &str) -> u128 {
    let data = s.as_bytes();
    let mut hash: u128 = 0;
    for i in 0..4u32 {
        hash = (hash << 32) | (xxh32_seeded(data, i) as u128);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reference values from `js-xxhash` for `xxHash32(s, 0)` on the inputs
    /// below. When adding this module, validate against the TS hash on CI.
    #[test]
    fn test_h32_smoke() {
        // Smoke values unrelated to production data; verified independent of TS.
        // Just ensures the algorithm runs end-to-end.
        assert_eq!(h32(""), 0x02cc5d05);
        assert_eq!(h32("a"), 0x550d7456);
        assert_eq!(h32("hello"), 0xfb0077f9);
    }

    #[test]
    fn test_h64_smoke() {
        // h64 is concatenation of xxHash32(s, 0) and xxHash32(s, 1).
        let s = "hello";
        let hi = xxh32_seeded(s.as_bytes(), 0) as u64;
        let lo = xxh32_seeded(s.as_bytes(), 1) as u64;
        assert_eq!(h64(s), (hi << 32) | lo);
    }

    #[test]
    fn test_h128_smoke() {
        // Take first 64 bits of h128 and verify it matches h64's upper bits.
        let s = "hello";
        let h128v = h128(s);
        let h64v = h64(s) as u128;
        assert_eq!(h128v >> 64, h64v);
    }
}
