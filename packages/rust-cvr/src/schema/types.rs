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
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::LazyLock;

use crate::cvr::RefCounts;
use crate::row_key::RowKey;
use crate::schema::cvr::QueriesRow;
use crate::ttl_clock::TTLClock;

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
/// Order two concrete versions. Absent `configVersion` counts as 0 (TS `?? 0`).
/// Borrowing sibling of [`cmp_versions`]: compare owned `CVRVersion`s directly
/// instead of wrapping-and-cloning each into `Some(_)`.
///
/// NB: this deliberately differs from the derived `PartialEq` on `CVRVersion`,
/// which treats `configVersion: None` and `Some(0)` as unequal. Ordering follows
/// TS's `?? 0` (so `None` and `Some(0)` are Equal here), which is why `CVRVersion`
/// does NOT implement `Ord` — an `Ord` consistent with this would violate the
/// `Ord`/`Eq` contract against the derived `Eq`.
pub fn cmp_cvr(a: &CVRVersion, b: &CVRVersion) -> Ordering {
    a.state_version.cmp(&b.state_version).then_with(|| {
        a.config_version
            .unwrap_or(0)
            .cmp(&b.config_version.unwrap_or(0))
    })
}

pub fn cmp_versions(a: &NullableCVRVersion, b: &NullableCVRVersion) -> Ordering {
    match (a, b) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(a), Some(b)) => cmp_cvr(a, b),
    }
}

/// Mirrors TS `maxVersion(a, b)`. `b` is optional (like TS's spread-optional).
pub fn max_version(a: CVRVersion, b: Option<CVRVersion>) -> CVRVersion {
    match b {
        None => a,
        Some(b) => {
            if cmp_cvr(&b, &a) == Ordering::Greater {
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

// NOTE: there is deliberately no `cookie_to_version` here. It used to wrap the
// PANICKING `version_from_string`, which is a foot-gun on any client-supplied
// cookie path. All cookie parsing must go through the fallible
// `maybe_version_string` (used by every real caller). Do not re-add an
// infallible cookie→version helper.

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

/// Error from parsing a version string. Mirrors the cases where TS
/// `versionFromString` throws.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VersionError {
    #[error("invalid version string {0:?}: more than one ':' separator")]
    TooManyParts(String),
    #[error("invalid lexi configVersion {lexi:?}: {reason}")]
    BadLexi { lexi: String, reason: &'static str },
    #[error("configVersion {0:?} exceeds max safe integer")]
    ConfigTooLarge(String),
    #[error("invalid stateVersion {ver:?}: {reason}")]
    BadStateVersion { ver: String, reason: &'static str },
    #[error("malformed query row {query_hash:?}: {reason}")]
    MalformedQuery {
        query_hash: String,
        reason: &'static str,
    },
}

/// Mirror of TS `stateVersionFromString`, used purely to VALIDATE that a state
/// version is a well-formed LexiVersion (or `major.minor` of two). TS's
/// `versionFromString` runs this in the 1-part case, so untrusted cookies with a
/// malformed stateVersion are rejected rather than silently accepted.
fn validate_state_version(ver: &str) -> Result<(), VersionError> {
    let bad = |reason| VersionError::BadStateVersion {
        ver: ver.to_string(),
        reason,
    };
    if !ver.contains('.') {
        version_from_lexi(ver).map_err(bad)?;
    } else {
        let parts: Vec<&str> = ver.split('.').collect();
        if parts.len() != 2 {
            return Err(bad("expected major.minor"));
        }
        version_from_lexi(parts[0]).map_err(bad)?;
        version_from_lexi(parts[1]).map_err(bad)?;
    }
    Ok(())
}

/// Fallible sibling of [`version_from_string`]: returns a [`VersionError`]
/// instead of panicking. Use this for untrusted / DB-sourced / client-cookie
/// strings, so a corrupt value surfaces as a recoverable error (mirroring TS
/// `versionFromString` throwing, which the caller catches) rather than aborting
/// the thread.
pub fn maybe_version_string(s: &str) -> Result<CVRVersion, VersionError> {
    let parts: Vec<&str> = s.split(':').collect();
    match parts.len() {
        1 => {
            // TS validates the stateVersion in the 1-part case (case 2 does not).
            validate_state_version(parts[0])?;
            Ok(CVRVersion {
                state_version: parts[0].to_string(),
                config_version: None,
            })
        }
        2 => {
            let config_version =
                version_from_lexi(parts[1]).map_err(|reason| VersionError::BadLexi {
                    lexi: parts[1].to_string(),
                    reason,
                })?;
            // TS bound (types.ts:332): `configVersion > BigInt(Number.MAX_SAFE_INTEGER)`
            // → 2^53-1, NOT u32::MAX (F-TYPES-1: a version in (2^32-1, 2^53-1]
            // must parse like TS, not be rejected as malformed).
            const MAX_SAFE_INTEGER: u128 = 9_007_199_254_740_991;
            if config_version > MAX_SAFE_INTEGER {
                return Err(VersionError::ConfigTooLarge(parts[1].to_string()));
            }
            Ok(CVRVersion {
                state_version: parts[0].to_string(),
                config_version: Some(config_version as u64),
            })
        }
        _ => Err(VersionError::TooManyParts(s.to_string())),
    }
}

/// Mirrors TS `versionFromString(s)`. Panics on malformed input — retained for
/// internally-produced strings (round-tripped through [`version_string`], hence
/// provably well-formed) and tests. For untrusted input use
/// [`maybe_version_string`].
pub fn version_from_string(s: &str) -> CVRVersion {
    maybe_version_string(s).unwrap_or_else(|e| panic!("{e}"))
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
    // The byte-slices below require byte 1 to be a char boundary. A valid lexi
    // version is all base36 (ASCII), so a multi-byte first char is malformed —
    // but slicing at a non-boundary PANICS. This parser runs on the untrusted
    // client cookie (maybe_version_string), so that panic is a per-CG DoS
    // (fuzz crash d1b161 = "ѱa"). TS indexes UTF-16 units and never panics,
    // returning a parse error instead; mirror that with a clean Err.
    if !lexi_version.is_char_boundary(1) {
        return Err("Invalid base36 encoded value");
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

// ─── CVR record + query types (schema/types.ts) ───

/// RowRecord — a row's CVR metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowRecord {
    pub id: RowID,
    pub row_version: String,
    pub patch_version: CVRVersion,
    /// None = tombstone (row removed from view)
    pub ref_counts: Option<RefCounts>,
}
/// ClientRecord — per-client metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientRecord {
    pub id: String,
    pub desired_query_ids: Vec<String>,
}
/// ClientState — per-client, per-query state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientState {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inactivated_at: Option<TTLClock>,
    pub ttl: i64,
    pub version: CVRVersion,
}
/// AST is stored as opaque JSON.
pub type AST = Value;

/// ClientSchema is stored as opaque JSON.
pub type ClientSchema = Value;
/// QueryRecord — discriminated union (internal/client/custom).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum QueryRecord {
    #[serde(rename = "internal")]
    Internal(InternalQueryRecord),
    #[serde(rename = "client")]
    Client(ClientQueryRecord),
    #[serde(rename = "custom")]
    Custom(CustomQueryRecord),
}

/// Fields common to all query variants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseQueryRecord {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transformation_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transformation_version: Option<CVRVersion>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_set_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InternalQueryRecord {
    #[serde(flatten)]
    pub base: BaseQueryRecord,
    pub ast: AST,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientQueryRecord {
    #[serde(flatten)]
    pub base: BaseQueryRecord,
    pub ast: AST,
    pub client_state: BTreeMap<String, ClientState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_version: Option<CVRVersion>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CustomQueryRecord {
    #[serde(flatten)]
    pub base: BaseQueryRecord,
    pub name: String,
    pub args: Vec<Value>,
    pub client_state: BTreeMap<String, ClientState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch_version: Option<CVRVersion>,
}

// Convenience accessors on QueryRecord
impl QueryRecord {
    pub fn id(&self) -> &str {
        match self {
            QueryRecord::Internal(r) => &r.base.id,
            QueryRecord::Client(r) => &r.base.id,
            QueryRecord::Custom(r) => &r.base.id,
        }
    }

    pub fn base(&self) -> &BaseQueryRecord {
        match self {
            QueryRecord::Internal(r) => &r.base,
            QueryRecord::Client(r) => &r.base,
            QueryRecord::Custom(r) => &r.base,
        }
    }

    pub fn base_mut(&mut self) -> &mut BaseQueryRecord {
        match self {
            QueryRecord::Internal(r) => &mut r.base,
            QueryRecord::Client(r) => &mut r.base,
            QueryRecord::Custom(r) => &mut r.base,
        }
    }

    pub fn is_internal(&self) -> bool {
        matches!(self, QueryRecord::Internal(_))
    }

    pub fn client_state(&self) -> Option<&BTreeMap<String, ClientState>> {
        match self {
            QueryRecord::Internal(_) => None,
            QueryRecord::Client(r) => Some(&r.client_state),
            QueryRecord::Custom(r) => Some(&r.client_state),
        }
    }

    pub fn client_state_mut(&mut self) -> Option<&mut BTreeMap<String, ClientState>> {
        match self {
            QueryRecord::Internal(_) => None,
            QueryRecord::Client(r) => Some(&mut r.client_state),
            QueryRecord::Custom(r) => Some(&mut r.client_state),
        }
    }

    pub fn patch_version(&self) -> Option<&CVRVersion> {
        match self {
            QueryRecord::Internal(_) => None,
            QueryRecord::Client(r) => r.patch_version.as_ref(),
            QueryRecord::Custom(r) => r.patch_version.as_ref(),
        }
    }

    pub fn patch_version_mut(&mut self) -> &mut Option<CVRVersion> {
        match self {
            QueryRecord::Internal(_) => panic!("internal queries have no patch_version"),
            QueryRecord::Client(r) => &mut r.patch_version,
            QueryRecord::Custom(r) => &mut r.patch_version,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum QueryPatch {
    #[serde(rename = "put")]
    Put {
        id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        client_id: Option<String>,
    },
    #[serde(rename = "del")]
    Del {
        id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        client_id: Option<String>,
    },
}

// ─── RowID (schema/types.ts) ───

/// A RowID is the composite primary key used to identify a row across tables.
/// TS: `{schema: string, table: string, rowKey: RowKey}`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RowID {
    pub schema: String,
    pub table: String,
    #[serde(rename = "rowKey")]
    pub row_key: RowKey,
}

/// Convert a QueryRecord to a QueriesRow for storage.
/// Mirrors TS `queryRecordToQueryRow` from schema/types.ts.
pub fn query_record_to_query_row(cvr_id: &str, query: &QueryRecord) -> QueriesRow {
    match query {
        QueryRecord::Internal(r) => QueriesRow {
            client_group_id: cvr_id.to_string(),
            query_hash: r.base.id.clone(),
            client_ast: Some(r.ast.clone()),
            query_name: None,
            query_args: None,
            patch_version: None,
            transformation_hash: r.base.transformation_hash.clone(),
            transformation_version: r.base.transformation_version.as_ref().map(version_string),
            internal: Some(true),
            deleted: Some(false),
            row_set_signature: r.base.row_set_signature.clone(),
        },
        QueryRecord::Client(r) => QueriesRow {
            client_group_id: cvr_id.to_string(),
            query_hash: r.base.id.clone(),
            client_ast: Some(r.ast.clone()),
            query_name: None,
            query_args: None,
            patch_version: r.patch_version.as_ref().map(version_string),
            transformation_hash: r.base.transformation_hash.clone(),
            transformation_version: r.base.transformation_version.as_ref().map(version_string),
            internal: None,
            deleted: Some(false),
            row_set_signature: r.base.row_set_signature.clone(),
        },
        QueryRecord::Custom(r) => QueriesRow {
            client_group_id: cvr_id.to_string(),
            query_hash: r.base.id.clone(),
            client_ast: None,
            query_name: Some(r.name.clone()),
            query_args: Some(Value::Array(r.args.clone())),
            patch_version: r.patch_version.as_ref().map(version_string),
            transformation_hash: r.base.transformation_hash.clone(),
            transformation_version: r.base.transformation_version.as_ref().map(version_string),
            internal: None,
            deleted: Some(false),
            row_set_signature: r.base.row_set_signature.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// F-TYPES-1 regression: the configVersion bound is TS's
    /// `Number.MAX_SAFE_INTEGER` (2^53-1, types.ts:332), NOT u32::MAX. A
    /// version in (u32::MAX, 2^53-1] must parse; above 2^53-1 must reject.
    /// Pre-fix (u32::MAX bound) the first two asserts failed with
    /// ConfigTooLarge — proven by temp-revert.
    #[test]
    fn maybe_version_string_config_version_bound_matches_ts() {
        let max_safe = CVRVersion {
            state_version: "1a9".to_string(),
            config_version: Some(9_007_199_254_740_991),
        };
        assert_eq!(
            maybe_version_string(&version_string(&max_safe)).expect("MAX_SAFE_INTEGER parses"),
            max_safe
        );
        let above_u32 = CVRVersion {
            state_version: "1a9".to_string(),
            config_version: Some(u32::MAX as u64 + 1),
        };
        assert_eq!(
            maybe_version_string(&version_string(&above_u32)).expect("(u32::MAX, 2^53) parses"),
            above_u32
        );
        // Above MAX_SAFE_INTEGER: TS throws "exceeds max safe integer".
        let too_large = CVRVersion {
            state_version: "1a9".to_string(),
            config_version: Some(9_007_199_254_740_992),
        };
        assert!(matches!(
            maybe_version_string(&version_string(&too_large)),
            Err(VersionError::ConfigTooLarge(_))
        ));
    }

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
    fn test_try_version_from_string_errors() {
        // >2 colon parts → TooManyParts (TS throws)
        assert_eq!(
            maybe_version_string("a:b:c"),
            Err(VersionError::TooManyParts("a:b:c".to_string()))
        );
        // Malformed lexi config → BadLexi
        assert!(matches!(
            maybe_version_string("01:x"),
            Err(VersionError::BadLexi { .. })
        ));
        // Well-formed still parses
        assert_eq!(
            maybe_version_string("01:01"),
            Ok(CVRVersion {
                state_version: "01".to_string(),
                config_version: Some(1)
            })
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

    // ④ Property-based invariants — catch edge cases the handpicked golden
    // vectors miss (base36 length boundaries, tie-breaks, large u64).
    use proptest::prelude::*;

    proptest! {
        // LexiVersion round-trips losslessly for every u64.
        #[test]
        fn prop_lexi_round_trip(n in any::<u64>()) {
            let lexi = version_to_lexi(n);
            prop_assert_eq!(version_from_lexi(&lexi).unwrap(), n as u128);
        }

        // The DEFINING LexiVersion property: lexicographic string order equals
        // numeric order. This is what lets version strings sort as keys; a
        // length-prefix encoding bug at a base36 boundary (36, 36^2, ...) would
        // break it. Includes cross-length pairs.
        #[test]
        fn prop_lexi_order_matches_numeric(a in any::<u64>(), b in any::<u64>()) {
            let (la, lb) = (version_to_lexi(a), version_to_lexi(b));
            prop_assert_eq!(a.cmp(&b), la.cmp(&lb));
        }

        // versionString ∘ versionFromString is identity for well-formed
        // versions (configVersion > 0; 0 normalizes to None by design).
        // Range widened to the TS bound (MAX_SAFE_INTEGER, not u32::MAX) with
        // the F-TYPES-1 fix.
        #[test]
        fn prop_version_string_round_trip(
            sv in any::<u64>(),
            cv in proptest::option::of(1u64..=9_007_199_254_740_991u64),
        ) {
            let v = CVRVersion { state_version: version_to_lexi(sv), config_version: cv };
            prop_assert_eq!(version_from_string(&version_string(&v)), v);
        }

        // cmp_versions is a consistent total order: antisymmetric under swap
        // (Some(0) and None compare equal, which preserves antisymmetry).
        #[test]
        fn prop_cmp_antisymmetric(
            sa in any::<u64>(), ca in proptest::option::of(0u64..1000),
            sb in any::<u64>(), cb in proptest::option::of(0u64..1000),
        ) {
            let a = Some(CVRVersion { state_version: version_to_lexi(sa), config_version: ca });
            let b = Some(CVRVersion { state_version: version_to_lexi(sb), config_version: cb });
            prop_assert_eq!(cmp_versions(&a, &b), cmp_versions(&b, &a).reverse());
        }

        // Total-order transitivity: the missing half of the order laws (with
        // antisymmetry above). If a <= b and b <= c then a <= c — a broken
        // configVersion tie-break or a non-lexi stateVersion compare would let
        // versions sort inconsistently as CVR keys. Small state domain so triples
        // actually collide on stateVersion and exercise the configVersion tie-break.
        #[test]
        fn prop_cmp_transitive(
            sa in 0u64..6, ca in proptest::option::of(0u64..4),
            sb in 0u64..6, cb in proptest::option::of(0u64..4),
            sc in 0u64..6, cc in proptest::option::of(0u64..4),
        ) {
            let a = Some(CVRVersion { state_version: version_to_lexi(sa), config_version: ca });
            let b = Some(CVRVersion { state_version: version_to_lexi(sb), config_version: cb });
            let c = Some(CVRVersion { state_version: version_to_lexi(sc), config_version: cc });
            if cmp_versions(&a, &b) != Ordering::Greater
                && cmp_versions(&b, &c) != Ordering::Greater
            {
                prop_assert_ne!(cmp_versions(&a, &c), Ordering::Greater);
            }
        }
    }
}
