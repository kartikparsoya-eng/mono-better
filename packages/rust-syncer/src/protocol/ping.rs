//! Port of `packages/zero-protocol/src/ping.ts`.

use serde::{Deserialize, Serialize};

/// Port of TS `pingBodySchema = v.object({})` (ping.ts:3).
///
/// An EMPTY object — valita `v.object` accepts only an object and, being
/// strict, rejects any key in it. Rust previously ignored the ping body, so
/// `["ping", null]`, `["ping", "x"]` and `["ping", []]` were all accepted where
/// TS rejects them (M13 R5).
///
/// The `Deserialize` impl is hand-written because a derived empty struct also
/// accepts an empty SEQUENCE (serde's struct visitor implements `visit_seq`),
/// which would let `["ping", []]` through.
#[derive(Debug, Clone, Default, Serialize)]
pub struct PingBody {}

impl<'de> Deserialize<'de> for PingBody {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let map = serde_json::Map::<String, serde_json::Value>::deserialize(d)?;
        if let Some(key) = map.keys().next() {
            return Err(serde::de::Error::custom(format!(
                "unexpected key `{key}` in ping body"
            )));
        }
        Ok(PingBody {})
    }
}
