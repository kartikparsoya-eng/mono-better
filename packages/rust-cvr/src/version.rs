//! TS-parity port of `packages/zero-cache/src/services/view-syncer/schema/types.ts`
//! subset (CVRVersion and utilities) plus
//! `packages/zero-cache/src/types/lexi-version.ts` (LexiVersion utilities).
//!
//! ## Contract
//!
//! - `versionString(v)` formats with `stateVersion[:lexi(configVersion)]`
//! - `versionFromString(s)` parses; panics on >2 colon parts
//! - `cmpVersions(a, b)` matches the TS ordering: null < anything, then lex on
//!   stateVersion, then numeric on configVersion
//! - LexiVersion: base36(then-prefixed by base36(length-1)) representation.
//!
//! Note the checked constraint in `versionToLexi`: callers must pass values
//! that fit in safe integer range and whose base36 length ≤ 37 chars
//! (i.e. up to ~1.06e+56 — effectively unlimited).

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::sync::LazyLock;

/// Mirrors TS `CVRVersion` — `{stateVersion: string, configVersion?: number}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CVRVersion {
    #[serde(rename = "stateVersion")]
    pub state_version: String,
    #[serde(rename = "configVersion", skip_serializing_if = "Option::is_none")]
    pub config_version: Option<u64>,
}

/// Mirrors TS `oneAfter(v)`. Bumps the configVersion, or starts at 0 for null.
pub fn one_after(v: &NullableCVRVersion) -> CVRVersion {
    match v {
        // TS: `oneAfter(null) === EMPTY_CVR_VERSION`. The zero major version is
        // Lexi-encoded as "00"; it is not an empty string/config minor pair.
        None => EMPTY_CVR_VERSION.clone(),
        Some(v) => CVRVersion {
            state_version: v.state_version.clone(),
            config_version: Some(v.config_version.unwrap_or(0) + 1),
        },
    }
}

pub static EMPTY_CVR_VERSION: LazyLock<CVRVersion> = LazyLock::new(CVRVersion::empty);

impl CVRVersion {
    pub fn empty() -> Self {
        CVRVersion {
            // `majorVersionToString(0)` in the TypeScript source of truth.
            state_version: "00".to_string(),
            config_version: None,
        }
    }
}

/// Mirrors TS `NullableCVRVersion = CVRVersion | null`.
pub type NullableCVRVersion = Option<CVRVersion>;

/// Mirrors TS `cmpVersions(a, b)`.
pub fn cmp_versions(a: &NullableCVRVersion, b: &NullableCVRVersion) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => a
            .state_version
            .cmp(&b.state_version)
            .then_with(|| (a.config_version.unwrap_or(0)).cmp(&b.config_version.unwrap_or(0))),
    }
}

/// Mirrors TS `maxVersion(a, b)`. `b` is optional (like TS's spread-optional).
pub fn max_version(a: CVRVersion, b: Option<CVRVersion>) -> CVRVersion {
    match b {
        None => a,
        Some(b) => {
            if cmp_versions(&Some(b.clone()), &Some(a.clone())) == Ordering::Greater {
                b
            } else {
                a
            }
        }
    }
}

/// Mirrors TS `versionToCookie` / `versionToNullableCookie`.
pub fn version_to_cookie(v: &CVRVersion) -> String {
    version_string(v)
}

pub fn version_to_nullable_cookie(v: &NullableCVRVersion) -> Option<String> {
    v.as_ref().map(version_to_cookie)
}

pub fn cookie_to_version(cookie: Option<&str>) -> NullableCVRVersion {
    cookie.map(version_from_string)
}

/// Mirrors TS `versionString(v)`.
pub fn version_string(v: &CVRVersion) -> String {
    // TS checks `v.configVersion ? ... : v.stateVersion`, so a configVersion of
    // `0` is FALSY and serializes as the bare stateVersion (no `:00` suffix).
    // `Some(0)` is never produced internally (bumps start at 1), but it is
    // reachable by parsing an externally-supplied `"<state>:00"` cookie — so the
    // zero case must collapse to `None`'s behavior to stay byte-identical to TS.
    match v.config_version {
        Some(cv) if cv != 0 => format!("{}:{}", v.state_version, version_to_lexi(cv)),
        _ => v.state_version.clone(),
    }
}

/// Mirrors TS `versionFromString(s)`.
pub fn version_from_string(s: &str) -> CVRVersion {
    let parts: Vec<&str> = s.split(':').collect();
    match parts.len() {
        1 => CVRVersion {
            state_version: parts[0].to_string(),
            config_version: None,
        },
        2 => {
            let config_version = version_from_lexi(parts[1])
                .unwrap_or_else(|e| panic!("invalid lexi version {}: {}", parts[1], e));
            assert!(
                config_version <= u64::from(u32::MAX) as u128,
                "_configVersion {} exceeds max safe integer_",
                parts[1]
            );
            CVRVersion {
                state_version: parts[0].to_string(),
                config_version: Some(config_version as u64),
            }
        }
        _ => panic!("Invalid version string {}", s),
    }
}

// ---- LexiVersion utilities ----
//
// Mirrors TS `versionToLexi` / `versionFromLexi` from
// `packages/zero-cache/src/types/lexi-version.ts`.

pub fn version_to_lexi(v: u64) -> String {
    let base36 = to_base36_u64(v);
    assert!(
        !base36.is_empty() && base36.len() <= 37,
        "Value too large for LexiVersion: {}",
        v
    );
    // Length is the base36 of (base36.len - 1).
    let length_char = to_base36_u64((base36.len() - 1) as u64);
    assert!(
        length_char.len() == 1,
        "Value is too large to be encoded as a LexiVersion: {}",
        v
    );
    format!("{}{}", length_char, base36)
}

pub fn version_from_lexi(lexi_version: &str) -> Result<u128, &'static str> {
    if lexi_version.len() < 2 {
        return Err("LexiVersion must have at least 2 characters");
    }
    let length_char = &lexi_version[0..1];
    let base36 = &lexi_version[1..];
    let expected_length = from_base36_u64(length_char)?;
    if (base36.len() as u64) != expected_length + 1 {
        return Err("Invalid LexiVersion: length prefix mismatch");
    }
    u128::from_str_radix(base36, 36).map_err(|_| "Invalid base36 encoded value")
}

fn to_base36_u64(mut n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut buf = Vec::new();
    while n > 0 {
        buf.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    buf.reverse();
    String::from_utf8(buf).expect("to_base36 produced invalid UTF-8")
}

fn from_base36_u64(s: &str) -> Result<u64, &'static str> {
    if s.is_empty() {
        return Err("empty base36 string");
    }
    let mut value: u64 = 0;
    for c in s.chars() {
        let d = c.to_digit(36).ok_or("invalid base36 digit")? as u64;
        value = value
            .checked_mul(36)
            .and_then(|v| v.checked_add(d))
            .ok_or("base36 value overflows u64")?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_to_lexi_examples() {
        // Examples from lexi-version.ts doc comment.
        assert_eq!(version_to_lexi(0), "00");
        assert_eq!(version_to_lexi(10), "0a");
        assert_eq!(version_to_lexi(35), "0z");
        assert_eq!(version_to_lexi(36), "110");
        assert_eq!(version_to_lexi(46655), "2zzz");
    }

    #[test]
    fn test_version_from_lexi_inverts() {
        for v in [0u64, 1, 10, 35, 36, 100, 46655, u32::MAX as u64] {
            let lexi = version_to_lexi(v);
            assert_eq!(
                version_from_lexi(&lexi),
                Ok(v as u128),
                "roundtrip failed for {}",
                v
            );
        }
    }

    #[test]
    fn test_version_string_no_config() {
        let v = CVRVersion {
            state_version: "01".to_string(),
            config_version: None,
        };
        assert_eq!(version_string(&v), "01");
    }

    #[test]
    fn empty_and_one_after_null_match_typescript_zero_major() {
        assert_eq!(version_string(&EMPTY_CVR_VERSION), "00");
        assert_eq!(one_after(&None), *EMPTY_CVR_VERSION);
    }

    #[test]
    fn test_version_string_with_config() {
        let v = CVRVersion {
            state_version: "01".to_string(),
            config_version: Some(1),
        };
        assert_eq!(version_string(&v), "01:01");
    }

    #[test]
    fn test_version_from_string_parses() {
        assert_eq!(
            version_from_string("01"),
            CVRVersion {
                state_version: "01".to_string(),
                config_version: None
            }
        );
        assert_eq!(
            version_from_string("01:01"),
            CVRVersion {
                state_version: "01".to_string(),
                config_version: Some(1)
            }
        );
        assert_eq!(
            version_from_string("01:02"),
            CVRVersion {
                state_version: "01".to_string(),
                config_version: Some(2)
            }
        );
    }

    #[test]
    fn test_cmp_versions_null_semantics() {
        let v1: NullableCVRVersion = None;
        let v2: NullableCVRVersion = Some(CVRVersion {
            state_version: "01".into(),
            config_version: None,
        });
        assert_eq!(cmp_versions(&v1, &v1), Ordering::Equal);
        assert_eq!(cmp_versions(&v1, &v2), Ordering::Less);
        assert_eq!(cmp_versions(&v2, &v1), Ordering::Greater);
    }

    #[test]
    fn test_cmp_versions_numeric_ordering() {
        let a = Some(CVRVersion {
            state_version: "01".into(),
            config_version: None,
        });
        let b = Some(CVRVersion {
            state_version: "02".into(),
            config_version: None,
        });
        let c = Some(CVRVersion {
            state_version: "01".into(),
            config_version: Some(1),
        });
        let d = Some(CVRVersion {
            state_version: "01".into(),
            config_version: Some(2),
        });
        assert_eq!(cmp_versions(&a, &b), Ordering::Less);
        assert_eq!(cmp_versions(&b, &a), Ordering::Greater);
        assert_eq!(cmp_versions(&a, &c), Ordering::Less);
        assert_eq!(cmp_versions(&c, &d), Ordering::Less);
    }
}
