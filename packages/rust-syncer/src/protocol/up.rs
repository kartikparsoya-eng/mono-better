//! Port of `packages/zero-protocol/src/up.ts` — serde
//! equivalents of the valita schemas.

use super::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::valita;

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
/// upstreamSchema)` (workers/connection.ts:204). JS strings are UTF-16, so an unpaired
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
/// (the serde_json crate's own read.rs, `parse_unicode_escape`, WTF-8 when
/// `validate` is false) but exposes
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
    let value: Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(e) => match replace_unpaired_surrogate_escapes(text) {
            Some(fixed) => serde_json::from_str(&fixed)
                .map_err(|e| serde::de::Error::custom(format!("SyntaxError: {e}")))?,
            None => return Err(serde::de::Error::custom(format!("SyntaxError: {e}"))),
        },
    };
    match value {
        Value::Array(arr) => Ok(arr),
        // Valid JSON that is not an array fails `upstreamSchema`'s tuple union
        // at the root: valita folds it into `Expected array. Got <value>`.
        other => Err(serde::de::Error::custom(format!(
            "TypeError: Expected array. Got {}",
            valita::to_display(Some(&other))
        ))),
    }
}

/// Parse an upstream message from a JSON array `["type", body]`.
pub fn parse_upstream(text: &str) -> Result<Upstream, serde_json::Error> {
    // `Connection.#handleMessage` (connection.ts:203-209) puts `String(e)` in
    // the `InvalidMessage` body: `SyntaxError: …` from `JSON.parse` (the text
    // is V8's — serde's wording differs, the class does not), `TypeError: …`
    // from `valita.parse` (rendered 1:1 by `shared::valita`).
    let arr = parse_frame_json(text)?;
    parse_upstream_array(&arr)
}

/// The frame-level `v.union` of `upstreamSchema` (up.ts): when every member
/// fails at the same depth — the type literal, the tuple arity, the body's
/// own type, or an unknown key directly on the body — TS's message is the
/// union fallback, `Invalid union value: <frame>` (shared/src/valita.ts:141).
fn invalid_union(arr: &[Value]) -> serde_json::Error {
    serde::de::Error::custom(format!(
        "TypeError: {}",
        valita::invalid_union_message(&Value::Array(arr.to_vec()))
    ))
}

/// Parse a frame's body (`arr[1]`) as `T`; a failure renders TS's message —
/// the first issue at its path from the frame root, or the union fallback
/// when the issue sits directly on the body (path `1`).
fn parse_body<T: serde::de::DeserializeOwned>(
    body: &Value,
    arr: &[Value],
) -> Result<T, serde_json::Error> {
    valita::deserialize_at::<T>(body, &[], body).map_err(|mut issue| {
        issue.path.insert(0, valita::Key::Index(1));
        if issue.path.len() == 1 {
            return invalid_union(arr);
        }
        let frame = Value::Array(arr.to_vec());
        serde::de::Error::custom(format!(
            "TypeError: {}",
            valita::get_message(&issue, &frame)
        ))
    })
}

/// Validate + dispatch an already-parsed `["type", body]` array. Split out of
/// [`parse_upstream`] so a caller that also needs the raw array (e.g. the
/// router's inbound dispatch) can parse the frame's JSON exactly once.
pub fn parse_upstream_array(arr: &[Value]) -> Result<Upstream, serde_json::Error> {
    // TS `v.tuple([v.literal(...), bodySchema])` pins the frame to EXACTLY two
    // elements — a 3-element array fails the tuple, it is not truncated. Rust
    // checked only `< 2` and ignored the extras.
    if arr.len() != 2 {
        return Err(invalid_union(arr));
    }
    let msg_type = arr[0].as_str().ok_or_else(|| invalid_union(arr))?;
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
            parse_body::<InitConnectionBody>(body, arr)?;
            Upstream::InitConnection(body.clone())
        }
        "ping" => {
            // TS `pingBodySchema = v.object({})` (ping.ts:3) — the body must be
            // an object, and valita rejects any key in it. Rust ignored the
            // ping body entirely.
            parse_body::<PingBody>(body, arr)?;
            Upstream::Ping
        }
        "deleteClients" => Upstream::DeleteClients(parse_body::<DeleteClientsBody>(body, arr)?),
        "changeDesiredQueries" => {
            Upstream::ChangeDesiredQueries(parse_body::<ChangeDesiredQueriesBody>(body, arr)?)
        }
        "pull" => {
            // TS validates the body against `pullRequestBodySchema`
            // (pull.ts:5). Rust kept the raw `Value` and validated NOTHING, so
            // wrong types, missing fields and null fields all passed.
            // Keep the raw Value afterwards: the handler forwards it verbatim.
            parse_body::<PullRequestBody>(body, arr)?;
            Upstream::Pull(body.clone())
        }
        "updateAuth" => Upstream::UpdateAuth(parse_body::<UpdateAuthBody>(body, arr)?),
        "push" => Upstream::Push(parse_body::<PushBody>(body, arr)?),
        "closeConnection" => {
            // TS `closeConnectionBodySchema = v.array(v.unknown())`
            // (close-connection.ts:3) — the body must be an ARRAY. Rust ignored
            // it.
            parse_body::<CloseConnectionBody>(body, arr)?;
            Upstream::CloseConnection
        }
        "inspect" => Upstream::Inspect(parse_body::<InspectUpBody>(body, arr)?),
        "ackMutationResponses" => {
            Upstream::AckMutationResponses(parse_body::<AckMutationResponsesBody>(body, arr)?)
        }
        _ => return Err(invalid_union(arr)),
    };
    Ok(result)
}
