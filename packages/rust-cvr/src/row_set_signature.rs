//! TS-parity port of `packages/zero-cache/src/services/view-syncer/row-set-signature.ts`.
//!
//! ## Contract
//!
//! - `signatureUnit(id) = h64(rowIDString(id))`
//! - `parseSignature(hex) = BigInt('0x'+hex)` (identity when undefined/null/empty)
//! - `formatSignature(sig) = sig.toString(16)` (lowercase)
//!
//! ## Notes
//!
//! TS functions are thin wrappers. Rust versions are direct.

use crate::hash::h64;
use crate::schema::types::RowID;

/// Mirrors TS `rowIDSignatureUnit(id) = h64(rowIDString(id))`.
pub fn row_id_signature_unit(id: &RowID) -> u64 {
    let s = crate::row_key::row_id_string_cached(id);
    h64(&s)
}

/// Mirrors TS `parseSignature(hex)`. Empty/None -> 0.
pub fn parse_signature(hex: Option<&str>) -> Result<u64, std::num::ParseIntError> {
    match hex {
        None | Some("") => Ok(0),
        Some(s) => u64::from_str_radix(s, 16),
    }
}

/// Mirrors TS `formatSignature(sig) -> hex lowercase`.
pub fn format_signature(sig: u64) -> String {
    format!("{:x}", sig)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::types::RowID;

    fn make_row_id(schema: &str, table: &str, row_key_json: serde_json::Value) -> RowID {
        let row_key = row_key_json.as_object().unwrap().clone();
        RowID {
            schema: schema.to_string(),
            table: table.to_string(),
            row_key,
        }
    }

    #[test]
    fn test_signature_unit_consistency() {
        let id = make_row_id("public", "users", serde_json::json!({"id": 1}));
        let a = row_id_signature_unit(&id);
        let b = row_id_signature_unit(&id);
        assert_eq!(a, b);
        // Must be non-trivially non-zero.
        assert_ne!(a, 0);
    }

    #[test]
    fn test_parse_and_format_roundtrip() {
        let values: Vec<u64> = vec![0, 1, 0xff, u64::MAX / 2, u64::MAX];
        for v in values {
            let hex = format_signature(v);
            let parsed = parse_signature(Some(&hex)).expect("roundtrip failed");
            assert_eq!(parsed, v, "roundtrip mismatch for {}", v);
        }
    }

    #[test]
    fn test_parse_none_is_zero() {
        assert_eq!(parse_signature(None).unwrap(), 0);
        assert_eq!(parse_signature(Some("")).unwrap(), 0);
    }

    #[test]
    fn test_parse_invalid_hex_fails() {
        assert!(parse_signature(Some("zz")).is_err());
        assert!(parse_signature(Some("0x10")).is_err());
    }
}
