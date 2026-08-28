//! Zero protocol message types — Rust serde equivalents of the TypeScript
//! valita schemas in `packages/zero-protocol/src/`, mirrored file-for-file
//! (L9 Stage 5a): each submodule ports its same-named TS file. The re-exports
//! keep every `crate::protocol::X` path stable.
//!
//! Wire format: all messages are JSON tuples `["messageType", bodyObject]`.
//! We use untagged enums + `#[serde(tag = "op")]` to match the TS union types.

pub mod change_desired_queries;
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
pub mod poke;
pub mod pong;
pub mod protocol_version;
pub mod push;
pub mod queries_patch;
pub mod row_patch;
pub mod up;
pub mod update_auth;
pub mod version;

pub use change_desired_queries::*;
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
pub use poke::*;
pub use pong::*;
pub use protocol_version::*;
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
