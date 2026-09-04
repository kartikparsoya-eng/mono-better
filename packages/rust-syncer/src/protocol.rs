//! Zero protocol message types — Rust serde equivalents of the TypeScript
//! valita schemas in `packages/zero-protocol/src/`, mirrored file-for-file
//! (L9 Stage 5a): each submodule ports its same-named TS file. The re-exports
//! keep every `crate::protocol::X` path stable.
//!
//! Wire format: all messages are JSON tuples `["messageType", bodyObject]`.
//! We use untagged enums + `#[serde(tag = "op")]` to match the TS union types.

/// A TS `v.number()` — a JS number, in both directions.
///
/// JS has ONE numeric type, so a `v.number()` field accepts `1`, `1.5`, `1E5`,
/// `-0`, `1e309` (-> Infinity) and `1e-330` (-> 0) alike. Typing such a field
/// `i64` in rust rejected all of those and CLOSED the connection where TS
/// served the query — `1E5` is an ordinary way to write a timestamp (M13 R1).
///
/// Plain `f64` fixes the inbound half and breaks the outbound half: serde
/// renders `404.0` where `JSON.stringify` renders `404`. `JsNumber` does both —
/// it deserializes from any JSON number and serializes the way
/// `JSON.stringify` renders a JS number (integral values as integers,
/// non-finite as `null`, exactly as JS does).
///
/// This is the same rule already hand-written in three places —
/// `rust_ivm::ivm::data::Value`'s `Serialize`, `tdigest::number_to_value`, and
/// `view_syncer::value_to_serde_json` — whose integral cutoffs had already
/// drifted apart (`9.007e15` vs `i64::MIN/MAX`). New protocol fields use this.
///
/// Rust-only type (AGENTS.md rule 5): it has no TS twin because it exists to
/// give rust the single JS numeric type TS gets from the language.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Default)]
pub struct JsNumber(pub f64);

impl JsNumber {
    pub fn as_f64(self) -> f64 {
        self.0
    }
}

impl From<f64> for JsNumber {
    fn from(v: f64) -> Self {
        JsNumber(v)
    }
}

impl From<i64> for JsNumber {
    fn from(v: i64) -> Self {
        JsNumber(v as f64)
    }
}

impl serde::Serialize for JsNumber {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let n = self.0;
        if !n.is_finite() {
            // `JSON.stringify(Infinity)` and `JSON.stringify(NaN)` are `null`.
            return s.serialize_none();
        }
        // `JSON.stringify(1)` is `1`, never `1.0`; `-0` renders as `0`.
        if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
            s.serialize_i64(n as i64)
        } else {
            s.serialize_f64(n)
        }
    }
}

impl<'de> serde::Deserialize<'de> for JsNumber {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        f64::deserialize(d).map(JsNumber)
    }
}

/// Deserialize a valita `.optional()` field: absent-or-value, NEVER an explicit
/// `null`.
///
/// TS `v.object({x: v.string().optional()})` REJECTS `{"x": null}` — `.optional()`
/// widens the field to "may be missing", not "may be null" (verified by the M13
/// oracle: `wrongtype/*/null` frames are rejected by valita). serde's
/// `Option<T>` accepts BOTH a missing key and an explicit `null`, so every
/// `.optional()` field needs this to match.
///
/// Serde invokes a `deserialize_with` only when the key is PRESENT, so `null`
/// reaches `T::deserialize` and fails there, while an absent key falls back to
/// `#[serde(default)]` -> `None`. Pair it with `#[serde(default)]`.
///
/// Rust-only helper (AGENTS.md rule 5): it has no TS twin because it exists to
/// close the serde-vs-valita gap in optional-field semantics, reproducing TS
/// behavior rather than adding any.
pub fn optional_no_null<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    T::deserialize(d).map(Some)
}

pub mod analyze_query_result;
pub mod change_desired_queries;
pub mod close_connection;
pub mod connect;
pub mod delete_clients;
pub mod down;
pub mod error;
pub mod error_kind_enum;
pub mod error_origin_enum;
pub mod error_reason_enum;
pub mod inspect_up;
pub mod mutation_id;
pub mod mutations_patch;
pub mod ping;
pub mod poke;
pub mod pong;
pub mod protocol_version;
pub mod pull;
pub mod push;
pub mod queries_patch;
pub mod row_patch;
pub mod up;
pub mod update_auth;
pub mod version;

pub use analyze_query_result::*;
pub use change_desired_queries::*;
pub use close_connection::*;
pub use connect::*;
pub use delete_clients::*;
pub use down::*;
pub use error::*;
pub use error_kind_enum::*;
pub use error_origin_enum::*;
pub use error_reason_enum::*;
pub use inspect_up::*;
pub use mutation_id::*;
pub use mutations_patch::*;
pub use ping::*;
pub use poke::*;
pub use pong::*;
pub use protocol_version::*;
pub use pull::*;
pub use push::*;
pub use queries_patch::*;
pub use row_patch::*;
pub use up::*;
pub use update_auth::*;
pub use version::*;

#[cfg(test)]
mod tests {
    use super::*;

    /// G36 malformed-init: TS valita-parses every ws message against
    /// `upstreamSchema` (connection.ts `#handleMessage`), so an initConnection
    /// body with a non-array `desiredQueriesPatch` is rejected at PARSE time
    /// (→ InvalidMessage), never reaching init handling (which used to
    /// surface a misleading InvalidConnectionRequest for it).
    #[test]
    fn parse_upstream_rejects_malformed_init_connection_body() {
        let err = parse_upstream(r#"["initConnection",{"desiredQueriesPatch":"not-a-list"}]"#);
        assert!(
            err.is_err(),
            "malformed initConnection body must fail upstream parse"
        );
    }

    #[test]
    fn parse_upstream_accepts_valid_init_connection_body() {
        let ok = parse_upstream(
            r#"["initConnection",{"desiredQueriesPatch":[],"clientSchema":{"tables":{}}}]"#,
        );
        assert!(matches!(ok, Ok(Upstream::InitConnection(_))), "{ok:?}");
    }

    #[test]
    fn parse_upstream_rejects_unknown_message_type() {
        // TS: unknown tag fails the valita union → InvalidMessage.
        assert!(parse_upstream(r#"["definitelyNotAThing",{}]"#).is_err());
    }

    /// TS parity, verified against `Connection.#handleMessage`
    /// (zero-cache/src/workers/connection.ts:203-204): TS runs `JSON.parse`
    /// then `valita.parse(value, upstreamSchema)`, and NEITHER rejects an
    /// unpaired surrogate — JS strings are UTF-16, so `JSON.parse` on
    /// `{"q":"\ud800"}` yields a 1-char string with `charCodeAt(0) === 0xd800`
    /// (checked against node), and valita's string check is a `typeof` test.
    /// `serde_json` rejects the same bytes, so rust answered a real client with
    /// `InvalidMessage` + close where TS served the query. Browser clients emit
    /// lone surrogates by slicing mid-astral-pair (`"👍".slice(0, 1)`) — what a
    /// length-capped search box does. Seen in production as
    /// `InvalidMessage: unexpected end of hex escape`.
    #[test]
    fn parse_upstream_accepts_unpaired_surrogate_like_ts_json_parse() {
        let parsed = parse_upstream(r#"["updateAuth",{"auth":"\ud800"}]"#)
            .expect("TS JSON.parse accepts a lone surrogate; rust must too");
        let Upstream::UpdateAuth(body) = parsed else {
            panic!("expected UpdateAuth, got {parsed:?}")
        };
        // U+FFFD is what TS itself stores: node re-encodes a lone surrogate to
        // UTF-8 as the replacement character at every boundary it crosses.
        assert_eq!(body.auth, "\u{FFFD}");
    }

    /// A lone TRAILING surrogate is equally legal to `JSON.parse`.
    #[test]
    fn parse_upstream_accepts_unpaired_low_surrogate() {
        let parsed = parse_upstream(r#"["updateAuth",{"auth":"a\udc4db"}]"#).unwrap();
        let Upstream::UpdateAuth(body) = parsed else {
            panic!("expected UpdateAuth")
        };
        assert_eq!(body.auth, "a\u{FFFD}b");
    }

    /// The repair must not over-reach: a WELL-FORMED pair is a real character
    /// and must survive as that character, not decay into two U+FFFDs — which
    /// is what a naive "replace every surrogate escape" fix would do. The lone
    /// `\ud800` is what forces the repair to run at all: a frame holding only a
    /// valid pair parses on the first attempt and never reaches it, so the pair
    /// here is genuinely under the scanner.
    #[test]
    fn parse_upstream_preserves_well_formed_surrogate_pairs() {
        let parsed = parse_upstream(r#"["updateAuth",{"auth":"\ud83d\udc4d \ud800"}]"#).unwrap();
        let Upstream::UpdateAuth(body) = parsed else {
            panic!("expected UpdateAuth")
        };
        assert_eq!(body.auth, "👍 \u{FFFD}");
    }

    /// `\\ud800` is an escaped BACKSLASH plus the literal text `ud800`, not a
    /// unicode escape; consuming `\\` as one unit is what keeps ordinary text
    /// from being corrupted into U+FFFD. The trailing lone surrogate is what
    /// puts this frame through the repair in the first place.
    #[test]
    fn parse_upstream_does_not_treat_escaped_backslash_as_a_unicode_escape() {
        let parsed = parse_upstream(r#"["updateAuth",{"auth":"\\ud800|\ud800"}]"#).unwrap();
        let Upstream::UpdateAuth(body) = parsed else {
            panic!("expected UpdateAuth")
        };
        assert_eq!(body.auth, "\\ud800|\u{FFFD}");
    }

    /// The handler re-reads the RAW frame text for the `initConnection`,
    /// `updateAuth` and `push` bodies. A bare `serde_json::from_str(..)` there
    /// still fails on a frame `parse_upstream` now accepts, collapsing the body
    /// to `Null` — a silently EMPTY `initConnection` context, which breaks push
    /// auth and custom queries downstream. Those sites route through
    /// `parse_frame_json` so the body survives.
    #[test]
    fn parse_frame_json_keeps_the_body_of_a_surrogate_bearing_init_frame() {
        let frame = r#"["initConnection",{"desiredQueriesPatch":[],"tag":"\ud800"}]"#;
        assert!(
            serde_json::from_str::<Vec<serde_json::Value>>(frame).is_err(),
            "precondition: a bare serde parse must reject this frame",
        );
        let arr = parse_frame_json(frame).expect("frame parses after repair");
        let body = arr.get(1).cloned().unwrap_or(serde_json::Value::Null);
        assert!(!body.is_null(), "init body collapsed to null: {body}");
        assert_eq!(body["tag"], serde_json::json!("\u{FFFD}"));
    }

    /// Port of TS `errorBodySchema` wire shapes (zero-protocol/src/error.ts +
    /// error-origin-enum.ts: `ZeroCache = 'zeroCache'`). G36 error-semantics
    /// surface: the serialized `["error", body]` frames for the ClientNotFound
    /// and VersionNotSupported constructors must be byte-exact — kind is the
    /// PascalCase ErrorKind string, origin the camelCase `zeroCache`, and the
    /// frame is a TUPLE (`["error", {...}]`), not an object.
    #[test]
    fn client_not_found_error_wire_shape_matches_ts() {
        let body = ErrorBody::client_not_found("Client not found");
        assert_eq!(body.kind(), &ErrorKind::ClientNotFound);
        assert_eq!(body.message(), "Client not found");
        assert_eq!(
            serde_json::to_string(&error_message(&body)).unwrap(),
            r#"["error",{"kind":"ClientNotFound","message":"Client not found","origin":"zeroCache"}]"#,
        );
    }

    /// Byte-exact VersionNotSupported body, with the exact message TS
    /// `Connection.init()` builds (connection.ts) for a below-minimum client.
    #[test]
    fn version_not_supported_error_wire_shape_matches_ts() {
        let message = format!(
            "server is at sync protocol v{PROTOCOL_VERSION} and does not support v29. The client must be updated to a newer release."
        );
        let body = ErrorBody::version_not_supported(message.clone());
        assert_eq!(body.kind(), &ErrorKind::VersionNotSupported);
        assert_eq!(
            serde_json::to_string(&error_message(&body)).unwrap(),
            format!(
                r#"["error",{{"kind":"VersionNotSupported","message":"{message}","origin":"zeroCache"}}]"#
            ),
        );
    }
}
