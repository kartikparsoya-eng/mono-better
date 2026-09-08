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

/// Rewrite every UNPAIRED surrogate escape to `�`, or `None` if there is
/// none. Split out only so [`parse_frame_json`] stays readable.
fn replace_unpaired_surrogate_escapes(text: &str) -> Option<String> {
    fn hex4(b: &[u8], at: usize) -> Option<u32> {
        let end = at.checked_add(4)?;
        u32::from_str_radix(std::str::from_utf8(b.get(at..end)?).ok()?, 16).ok()
    }
    const ESC: usize = 6; // `\uXXXX`

    let b = text.as_bytes();
    let mut repaired: Option<String> = None;
    let (mut copied, mut i) = (0usize, 0usize);

    while i < b.len() {
        if b[i] != b'\\' {
            i += 1;
            continue;
        }
        // Consume the escaped byte together with its backslash, so `\\` in
        // ordinary text can never be misread as the start of a `\u` escape.
        let Some(&next) = b.get(i + 1) else { break };
        if next != b'u' {
            i += 2;
            continue;
        }
        let Some(unit) = hex4(b, i + 2) else {
            i += 2;
            continue;
        };
        if (0xD800..0xDC00).contains(&unit) {
            // Leading surrogate: well-formed only when a trailing surrogate
            // escape follows IMMEDIATELY — skip the whole pair when it does.
            let after = i + ESC;
            if b.get(after) == Some(&b'\\')
                && b.get(after + 1) == Some(&b'u')
                && hex4(b, after + 2).is_some_and(|lo| (0xDC00..0xE000).contains(&lo))
            {
                i = after + ESC;
                continue;
            }
        } else if !(0xDC00..0xE000).contains(&unit) {
            i += ESC; // an ordinary escape, e.g. `A`
            continue;
        }
        // Unpaired: a leading surrogate with no trailing partner, or a bare
        // trailing surrogate.
        let out = repaired.get_or_insert_with(|| String::with_capacity(text.len()));
        out.push_str(&text[copied..i]);
        out.push(char::REPLACEMENT_CHARACTER);
        i += ESC;
        copied = i;
    }

    let mut out = repaired?;
    out.push_str(&text[copied..]);
    Some(out)
}

/// Rust twin of the `JSON.parse(data)` in TS `Connection.#handleMessage`
/// (zero-cache/src/workers/connection.ts:203).
///
/// TS parses each ws frame with `JSON.parse` and then `valita.parse(value,
/// upstreamSchema)` (connection.ts:204). JS strings are UTF-16, so an unpaired
/// surrogate is a legal string value: `JSON.parse` accepts `"\ud800"`, and
/// valita's string check is a `typeof` test, so NEITHER layer rejects it. Rust
/// `String` is UTF-8 and cannot hold a lone surrogate, so `serde_json` rejects
/// the whole frame — and we answered a real client with `InvalidMessage` +
/// close where TS served the query. Browser clients produce lone surrogates
/// routinely by slicing a string mid-astral-pair (`"👍".slice(0, 1)`), which is
/// what a length-capped search box does.
///
/// U+FFFD is not a choice made here — it is the value TS itself ends up with.
/// Node re-encodes a lone surrogate to UTF-8 as the replacement character at
/// every boundary the string crosses (the PG driver, better-sqlite3), so
/// U+FFFD is what TS stores in the CVR, compares against the replica, and
/// returns to the client. `serde_json` implements the same JS rule internally
/// (read.rs `parse_unicode_escape`, WTF-8 when `validate` is false) but exposes
/// it only through `deserialize_bytes`, which `Value`'s `deserialize_any` never
/// reaches — hence the repair here rather than a parser flag.
///
/// This is a UTF-16-vs-UTF-8 string-model bridge (AGENTS.md rule 5): it exists
/// to REPRODUCE TS-observable behavior, not to change it. Only the error path
/// runs it, so well-formed frames — all of normal traffic — pay nothing.
///
/// EVERY site that turns a raw frame into JSON must go through this, not
/// `serde_json::from_str`: the handler re-reads the raw text for the
/// `initConnection`, `updateAuth` and `push` bodies, and a bare `from_str`
/// there would still fail on the frames this now accepts, collapsing the body
/// to `Null` — an empty `initConnection` context breaks push auth downstream.
pub fn parse_frame_json(text: &str) -> Result<Vec<Value>, serde_json::Error> {
    match serde_json::from_str(text) {
        Ok(arr) => Ok(arr),
        Err(e) => match replace_unpaired_surrogate_escapes(text) {
            Some(fixed) => serde_json::from_str(&fixed),
            None => Err(e),
        },
    }
}

/// Parse an upstream message from a JSON array `["type", body]`.
pub fn parse_upstream(text: &str) -> Result<Upstream, serde_json::Error> {
    let arr = parse_frame_json(text)?;
    parse_upstream_array(&arr)
}

/// Validate + dispatch an already-parsed `["type", body]` array. Split out of
/// [`parse_upstream`] so a caller that also needs the raw array (e.g. the
/// router's inbound dispatch) can parse the frame's JSON exactly once.
pub fn parse_upstream_array(arr: &[Value]) -> Result<Upstream, serde_json::Error> {
    // TS `v.tuple([v.literal(...), bodySchema])` pins the frame to EXACTLY two
    // elements — a 3-element array fails the tuple, it is not truncated. Rust
    // checked only `< 2` and ignored the extras (M13 R5).
    if arr.len() != 2 {
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
        "ping" => {
            // TS `pingBodySchema = v.object({})` (ping.ts:3) — the body must be
            // an object, and valita rejects any key in it. Rust ignored the
            // ping body entirely (M13 R5).
            serde_json::from_value::<PingBody>(body.clone())?;
            Upstream::Ping
        }
        "deleteClients" => {
            Upstream::DeleteClients(serde_json::from_value::<DeleteClientsBody>(body.clone())?)
        }
        "changeDesiredQueries" => Upstream::ChangeDesiredQueries(serde_json::from_value::<
            ChangeDesiredQueriesBody,
        >(body.clone())?),
        "pull" => {
            // TS validates the body against `pullRequestBodySchema`
            // (pull.ts:5). Rust kept the raw `Value` and validated NOTHING, so
            // wrong types, missing fields and null fields all passed (M13 R2).
            // Keep the raw Value afterwards: the handler forwards it verbatim.
            serde_json::from_value::<PullRequestBody>(body.clone())?;
            Upstream::Pull(body.clone())
        }
        "updateAuth" => {
            Upstream::UpdateAuth(serde_json::from_value::<UpdateAuthBody>(body.clone())?)
        }
        "push" => Upstream::Push(serde_json::from_value::<PushBody>(body.clone())?),
        "closeConnection" => {
            // TS `closeConnectionBodySchema = v.array(v.unknown())`
            // (close-connection.ts:3) — the body must be an ARRAY. Rust ignored
            // it (M13 R5).
            serde_json::from_value::<CloseConnectionBody>(body.clone())?;
            Upstream::CloseConnection
        }
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
