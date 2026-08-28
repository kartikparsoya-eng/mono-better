//! Port of `packages/zero-protocol/src/up.ts` — serde
//! equivalents of the valita schemas (L9 Stage 5a split of the
//! former single-file `protocol.rs`).

use super::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

//
// All upstream messages are `["messageType", body]` tuples.
// We deserialize the tag first, then the body.

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Upstream {
    /// `["initConnection", body]` — body is parsed separately because it may
    /// arrive in the sec-websocket-protocol header.
    InitConnection(Value),
    /// `["ping", {}]`
    Ping,
    /// `["deleteClients", body]`
    DeleteClients(DeleteClientsBody),
    /// `["changeDesiredQueries", body]`
    ChangeDesiredQueries(ChangeDesiredQueriesBody),
    /// `["pull", body]` — not supported by Zero
    Pull(Value),
    /// `["updateAuth", body]`
    UpdateAuth(UpdateAuthBody),
    /// `["push", body]`
    Push(PushBody),
    /// `["closeConnection", body]` — deprecated, no-op
    CloseConnection,
    /// `["inspect", body]`
    Inspect(InspectUpBody),
    /// `["ackMutationResponses", body]`
    AckMutationResponses(AckMutationResponsesBody),
}

/// Parse an upstream message from a JSON array `["type", body]`.
pub fn parse_upstream(text: &str) -> Result<Upstream, serde_json::Error> {
    let arr: Vec<Value> = serde_json::from_str(text)?;
    parse_upstream_array(&arr)
}

/// Validate + dispatch an already-parsed `["type", body]` array. Split out of
/// [`parse_upstream`] so a caller that also needs the raw array (e.g. the
/// router's inbound dispatch) can parse the frame's JSON exactly once.
pub fn parse_upstream_array(arr: &[Value]) -> Result<Upstream, serde_json::Error> {
    if arr.len() < 2 {
        return Err(serde::de::Error::custom(
            "message must be a tuple [type, body]",
        ));
    }
    let msg_type = arr[0]
        .as_str()
        .ok_or_else(|| serde::de::Error::custom("message type must be a string"))?;
    let body = &arr[1];

    let result = match msg_type {
        "initConnection" => {
            // TS parity: `Connection.#handleMessage` valita-parses EVERY
            // ws-delivered message against `upstreamSchema` (connection.ts),
            // so a malformed initConnection body (e.g. a non-array
            // desiredQueriesPatch) is an `InvalidMessage` error — it must
            // never reach the init handling (which would otherwise fail
            // later with a misleading InvalidConnectionRequest). Keep the
            // raw Value: the header-delivered init path parses it itself.
            serde_json::from_value::<InitConnectionBody>(body.clone())?;
            Upstream::InitConnection(body.clone())
        }
        "ping" => Upstream::Ping,
        "deleteClients" => {
            Upstream::DeleteClients(serde_json::from_value::<DeleteClientsBody>(body.clone())?)
        }
        "changeDesiredQueries" => Upstream::ChangeDesiredQueries(serde_json::from_value::<
            ChangeDesiredQueriesBody,
        >(body.clone())?),
        "pull" => Upstream::Pull(body.clone()),
        "updateAuth" => {
            Upstream::UpdateAuth(serde_json::from_value::<UpdateAuthBody>(body.clone())?)
        }
        "push" => Upstream::Push(serde_json::from_value::<PushBody>(body.clone())?),
        "closeConnection" => Upstream::CloseConnection,
        "inspect" => Upstream::Inspect(serde_json::from_value::<InspectUpBody>(body.clone())?),
        "ackMutationResponses" => Upstream::AckMutationResponses(serde_json::from_value::<
            AckMutationResponsesBody,
        >(body.clone())?),
        other => {
            return Err(serde::de::Error::custom(format!(
                "unknown message type: {other}"
            )));
        }
    };
    Ok(result)
}
