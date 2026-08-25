//! Connect parameters — port of `packages/zero-cache/src/workers/connect-params.ts`.
//!
//! Parsed from URL query params + `sec-websocket-protocol` header.

use crate::protocol::{InitConnectionMessage, SecProtocols, decode_sec_protocols};
use std::collections::HashMap;

/// All connect parameters extracted from a WebSocket upgrade request.
#[derive(Debug, Clone)]
pub struct ConnectParams {
    pub protocol_version: u32,
    pub client_id: String,
    pub client_group_id: String,
    pub profile_id: Option<String>,
    pub base_cookie: Option<String>,
    pub timestamp: i64,
    pub lm_id: i64,
    pub ws_id: String,
    pub debug_perf: bool,
    pub auth: Option<String>,
    pub user_id: Option<String>,
    pub init_connection_msg: Option<InitConnectionMessage>,
    pub http_cookie: Option<String>,
    pub origin: Option<String>,
    /// All incoming HTTP request headers (lowercased names, multi-values joined
    /// with `, `). Forwarded to the query API filtered by the
    /// `query-allowed-request-headers` allowlist. Port of `requestHeaders`
    /// added in zero/v1.9.0 (#6144).
    pub request_headers: HashMap<String, String>,
}

/// Error during connect-param parsing.
#[derive(Debug, thiserror::Error)]
pub enum ConnectParamsError {
    #[error("invalid querystring - missing {0}")]
    MissingParam(&'static str),
    #[error("invalid querystring parameter {name}, got: {value}")]
    InvalidInt { name: &'static str, value: String },
    #[error("sec-websocket-protocol header missing")]
    MissingSecProtocol,
    #[error("sec-websocket-protocol decode error: {0}")]
    DecodeError(#[from] crate::protocol::DecodeError),
}

/// Parse connect params from a URL + headers.
///
/// `protocol_version` is extracted from the URL path (e.g. `/sync/v51/connect`).
/// `sec_websocket_protocol` is the raw header value.
/// `cookie` and `origin` are optional HTTP headers.
pub fn get_connect_params(
    protocol_version: u32,
    url: &str,
    sec_websocket_protocol: Option<&str>,
    cookie: Option<&str>,
    origin: Option<&str>,
    request_headers: HashMap<String, String>,
) -> Result<ConnectParams, ConnectParamsError> {
    let parsed = url::Url::parse(url).map_err(|_| ConnectParamsError::MissingParam("url"))?;
    let params: HashMap<String, String> = parsed
        .query_pairs()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();

    let client_id = get_string(&params, "clientID", true)?.expect("required");
    let client_group_id = get_string(&params, "clientGroupID", true)?.expect("required");
    let profile_id = get_string(&params, "profileID", false)?;
    let base_cookie = get_string(&params, "baseCookie", false)?;
    let timestamp = get_integer(&params, "ts", true)?;
    let lm_id = get_integer(&params, "lmid", true)?;
    let ws_id = get_string(&params, "wsid", false)?.unwrap_or_default();
    let user_id = get_string(&params, "userID", false)?;
    let debug_perf = get_boolean(&params, "debugPerf");

    let sec_protocol = sec_websocket_protocol.ok_or(ConnectParamsError::MissingSecProtocol)?;
    let SecProtocols {
        init_connection_message,
        auth_token,
    } = decode_sec_protocols(sec_protocol)?;

    Ok(ConnectParams {
        protocol_version,
        client_id,
        client_group_id,
        profile_id,
        base_cookie,
        timestamp,
        lm_id,
        ws_id,
        debug_perf,
        auth: auth_token,
        user_id,
        init_connection_msg: init_connection_message,
        http_cookie: cookie.map(|s| s.to_string()),
        origin: origin.map(|s| s.to_string()),
        request_headers,
    })
}

/// Extract the protocol version from the URL path.
/// E.g. `/sync/v51/connect` → `Some(51)`.
pub fn extract_protocol_version(path: &str) -> Option<u32> {
    let parts: Vec<&str> = path.split('/').collect();
    for part in parts {
        if let Some(num_str) = part.strip_prefix('v')
            && let Ok(num) = num_str.parse::<u32>()
        {
            return Some(num);
        }
    }
    None
}

// ─── URL parameter helpers (port of url-params.ts) ─────────────────────────

fn get_string(
    params: &HashMap<String, String>,
    name: &'static str,
    required: bool,
) -> Result<Option<String>, ConnectParamsError> {
    let value = params.get(name).map(|s| s.as_str());
    match value {
        Some(v) if !v.is_empty() => Ok(Some(v.to_string())),
        _ => {
            if required {
                Err(ConnectParamsError::MissingParam(name))
            } else {
                Ok(None)
            }
        }
    }
}

fn get_integer(
    params: &HashMap<String, String>,
    name: &'static str,
    required: bool,
) -> Result<i64, ConnectParamsError> {
    match get_string(params, name, required)? {
        // TypeScript's URLParams.getInteger() uses parseInt(), which accepts a
        // numeric prefix (notably fractional performance.now()-style `ts`
        // values). Match that behavior instead of Rust's strict i64 parser.
        Some(v) => parse_js_integer(&v).ok_or(ConnectParamsError::InvalidInt { name, value: v }),
        None => Ok(0),
    }
}

/// The subset of JavaScript `parseInt(value)` relevant to URL parameters:
/// trim leading whitespace/sign, honor the optional `0x` prefix, and stop at
/// the first invalid digit. This deliberately parses `"123.9"` as `123`.
fn parse_js_integer(value: &str) -> Option<i64> {
    let value = value.trim_start();
    let (negative, unsigned) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    let (radix, digits) = if unsigned.starts_with("0x") || unsigned.starts_with("0X") {
        (16, &unsigned[2..])
    } else {
        (10, unsigned)
    };
    let digit_len = digits
        .bytes()
        .take_while(|byte| match radix {
            16 => byte.is_ascii_hexdigit(),
            _ => byte.is_ascii_digit(),
        })
        .count();
    if digit_len == 0 {
        return None;
    }
    let magnitude = i128::from_str_radix(&digits[..digit_len], radix).ok()?;
    let signed = if negative { -magnitude } else { magnitude };
    i64::try_from(signed).ok()
}

fn get_boolean(params: &HashMap<String, String>, name: &str) -> bool {
    params.get(name).map(|s| s.as_str()) == Some("true")
}

#[cfg(test)]
mod tests {
    use super::parse_js_integer;

    #[test]
    fn integer_parsing_matches_typescript_parse_int() {
        assert_eq!(parse_js_integer("1786564382909.802"), Some(1786564382909));
        assert_eq!(parse_js_integer("  -42tail"), Some(-42));
        assert_eq!(parse_js_integer("0x10"), Some(16));
        assert_eq!(parse_js_integer("1e3"), Some(1));
        assert_eq!(parse_js_integer("not-a-number"), None);
    }

    /// Layer-2 body-differential: `parse_js_integer` (the `ts`/`lmid` reader,
    /// TS `URLParams.getInteger` == `parseInt`) must match the REAL JS `parseInt`
    /// for every string in `parse-int-fixture.json` (generated by
    /// `generate-parse-int-fixture.mjs`) — pinning the quirks (truncate-at-`.`,
    /// leading ws + sign, stop-at-junk, auto-hex, stop-at-`e`, NaN → None) to JS.
    #[test]
    fn parse_int_parity_against_ts() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/agentic/parity/parse-int-fixture.json"
        );
        let bytes = std::fs::read(path)
            .unwrap_or_else(|e| panic!("failed to read parse-int fixture {path}: {e}"));
        let fixture: serde_json::Value =
            serde_json::from_slice(&bytes).expect("parse-int fixture is not valid JSON");
        let cases = fixture["cases"].as_array().expect("fixture.cases missing");
        assert!(!cases.is_empty());
        for case in cases {
            let input = case["input"].as_str().unwrap();
            let want = if case["result"].is_null() {
                None
            } else {
                Some(case["result"].as_i64().unwrap())
            };
            assert_eq!(
                parse_js_integer(input),
                want,
                "parse_js_integer divergence for input {input:?}"
            );
        }
    }
}
