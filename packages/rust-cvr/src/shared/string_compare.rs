//! Port of `packages/shared/src/string-compare.ts`.

use std::cmp::Ordering;

/// Port of TS `stringCompare(a, b)` (string-compare.ts:1-9): the JS `<` / `>`
/// string relation, i.e. UTF-16 code-unit order.
///
/// UTF-8 byte order (Rust `str::cmp`) equals Unicode code-point order, and the
/// two agree EXCEPT when the first differing characters straddle the surrogate
/// range: a supplementary character (U+10000+, high surrogate D800–DBFF) sorts
/// BEFORE any BMP character in U+E000–U+FFFF under UTF-16 but AFTER it under
/// code-point order. Callers that mirror a TS `.sort(stringCompare)` /
/// `a < b ? -1 : …` must use this, not `cmp` (cvr.ts desiredQueryIDs,
/// row-key.ts normalizedKeyOrder, constraint.ts constraint keys).
pub fn string_compare(a: &str, b: &str) -> Ordering {
    let (ab, bb) = (a.as_bytes(), b.as_bytes());
    let n = ab.iter().zip(bb).take_while(|(x, y)| x == y).count();
    if n == ab.len() || n == bb.len() {
        return ab.len().cmp(&bb.len());
    }
    // Both differing bytes ASCII → byte order is UTF-16 order.
    if ab[n] < 0x80 && bb[n] < 0x80 {
        return ab[n].cmp(&bb[n]);
    }
    // Back up to the char boundary shared by both strings (the prefix bytes are
    // identical, so the boundary is the same) and compare the first differing
    // chars by their leading UTF-16 code unit, then by code point (which is the
    // low-surrogate order when the leading units tie).
    let mut start = n;
    while !a.is_char_boundary(start) {
        start -= 1;
    }
    let x = a[start..].chars().next().expect("differing char");
    let y = b[start..].chars().next().expect("differing char");
    utf16_lead(x).cmp(&utf16_lead(y)).then(x.cmp(&y))
}

fn utf16_lead(c: char) -> u32 {
    let cp = c as u32;
    if cp >= 0x10000 {
        0xD800 + ((cp - 0x10000) >> 10)
    } else {
        cp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Golden from `node -e` with TS `stringCompare`'s body
    /// (`a === b ? 0 : a < b ? -1 : 1`): U+FF01 (BMP, unit FF01) sorts AFTER
    /// U+1F600 (units D83D DE00) in JS, but BEFORE it in UTF-8/code-point
    /// order — the divergence this port exists for.
    #[test]
    fn matches_js_utf16_order_across_the_surrogate_boundary() {
        assert_eq!(string_compare("\u{FF01}", "\u{1F600}"), Ordering::Greater);
        assert_eq!("\u{FF01}".cmp("\u{1F600}"), Ordering::Less);
        assert_eq!(string_compare("a\u{1F600}", "a\u{FF01}"), Ordering::Less);
        assert_eq!(string_compare("\u{1F600}", "\u{1F601}"), Ordering::Less);
        assert_eq!(string_compare("\u{E9}", "z"), Ordering::Greater);
        assert_eq!(string_compare("ab", "abc"), Ordering::Less);
        assert_eq!(string_compare("abd", "abc"), Ordering::Greater);
        assert_eq!(string_compare("", ""), Ordering::Equal);
        assert_eq!(string_compare("same", "same"), Ordering::Equal);
        let mut v = vec!["\u{FF01}", "\u{1F600}", "z", "A"];
        v.sort_by(|a, b| string_compare(a, b));
        assert_eq!(v, vec!["A", "z", "\u{1F600}", "\u{FF01}"]);
    }
}
