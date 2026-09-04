//! Port of `packages/zero-protocol/src/version.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

/// A CVR version (cookie). Always a string like "00" or "0123abc".
pub type Version = String;
/// Port of TS `nullableVersionSchema = v.union(versionSchema, v.null())`
/// (version.ts:4) — a version that may be `null` (a base cookie is null before
/// the first request) but whose KEY is still required.
///
/// Not `Option<String>`, and not a `#[serde(transparent)]` newtype over one:
/// serde fills a missing field via `missing_field`, whose deserializer answers
/// `deserialize_option` with `visit_none`, so BOTH of those accept a body with
/// no `cookie` at all — where TS rejects it (M13 R2). The hand-written impl
/// below routes through `deserialize_any`, which `missing_field` refuses, so an
/// absent key is an error while an explicit `null` still parses.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize)]
#[serde(transparent)]
pub struct NullableVersion(pub Option<String>);

impl From<Option<String>> for NullableVersion {
    fn from(v: Option<String>) -> Self {
        NullableVersion(v)
    }
}

impl<'de> serde::Deserialize<'de> for NullableVersion {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = NullableVersion;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a version string or null")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(NullableVersion(Some(v.to_string())))
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(NullableVersion(None))
            }

            fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(NullableVersion(None))
            }
        }
        d.deserialize_any(V)
    }
}
