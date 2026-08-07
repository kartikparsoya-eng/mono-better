//! Connect parameters — port of `packages/zero-cache/src/workers/connect-params.ts`.
//!
//! Parsed from URL query params + `sec-websocket-protocol` header.

use crate::protocol::{decode_sec_protocols, InitConnectionMessage, SecProtocols};
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
) -> Result<ConnectParams, ConnectParamsError> {
    let parsed = url::Url::parse(url).map_err(|_| ConnectParamsError::MissingParam("url"))?;
    let params: HashMap<String, String> = parsed.query_pairs().map(|(k, v)| (k.to_string(), v.to_string())).collect();

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
    })
}

/// Extract the protocol version from the URL path.
/// E.g. `/sync/v51/connect` → `Some(51)`.
pub fn extract_protocol_version(path: &str) -> Option<u32> {
    let parts: Vec<&str> = path.split('/').collect();
    for part in parts {
        if let Some(num_str) = part.strip_prefix('v') {
            if let Ok(num) = num_str.parse::<u32>() {
                return Some(num);
            }
        }
    }
    None
}

// ─── URL parameter helpers (port of url-params.ts) ─────────────────────────

fn get_string(params: &HashMap<String, String>, name: &'static str, required: bool) -> Result<Option<String>, ConnectParamsError> {
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

fn get_integer(params: &HashMap<String, String>, name: &'static str, required: bool) -> Result<i64, ConnectParamsError> {
    match get_string(params, name, required)? {
        Some(v) => v.parse::<i64>().map_err(|_| ConnectParamsError::InvalidInt { name, value: v }),
        None => Ok(0),
    }
}

fn get_boolean(params: &HashMap<String, String>, name: &str) -> bool {
    params.get(name).map(|s| s.as_str()) == Some("true")
}
