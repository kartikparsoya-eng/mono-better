//! WebSocket server — port of the WebSocket acceptance + connection lifecycle
//! from `workers/syncer.ts` and `workers/connection.ts`.
//!
//! Uses `tokio-tungstenite` for the WebSocket protocol. No handoff mechanism
//! (the server accepts directly, unlike the TS handoff model).

use crate::connect_params::{ConnectParams, extract_protocol_version, get_connect_params};
use crate::protocol::{
    ErrorBody, MIN_SERVER_SUPPORTED_SYNC_PROTOCOL, PROTOCOL_VERSION, error_message, pong_message,
};
use crate::ws_sink::{DirectWebSocketSink, WsCommand};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::accept_hdr_async_with_config;
use tokio_tungstenite::tungstenite::Error as WebSocketError;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

/// Downstream message interval: slightly longer than client's 5s PING_INTERVAL.
const DOWNSTREAM_MSG_INTERVAL_MS: u64 = 6000;
/// Keepalive pong check interval: half of DOWNSTREAM_MSG_INTERVAL_MS.
const KEEPALIVE_CHECK_INTERVAL_MS: u64 = 3000;
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Configuration for the WebSocket server.
#[derive(Clone)]
pub struct WsServerConfig {
    /// Port to listen on.
    pub port: u16,
    /// Max WebSocket payload size in bytes.
    pub max_payload_bytes: usize,
    /// Whether to enable per-message-deflate compression.
    pub compression: bool,
}

/// Connection context passed to the handler.
pub struct ConnectionContext {
    pub params: ConnectParams,
    pub sink: DirectWebSocketSink,
    /// Channel for receiving parsed upstream messages on the CG thread.
    /// The WS read task sends messages here; the CG thread receives them.
    pub upstream_rx: mpsc::Receiver<String>,
}

/// Accept a single WebSocket connection.
///
/// This function:
/// 1. Completes the WS handshake (extracting path + headers via callback).
/// 2. Parses connect params from the URL + headers.
/// 3. Validates the protocol version.
/// 4. Sends the `connected` message.
/// 5. Spawns the WS read task (forwards messages to a channel).
/// 6. Returns the `ConnectionContext` for the CG thread to use.
pub async fn accept_connection(stream: tokio::net::TcpStream) -> Option<ConnectionContext> {
    accept_connection_with_limit(stream, DEFAULT_MAX_PAYLOAD_BYTES).await
}

/// Accept a connection while enforcing the configured message and frame cap.
/// Applying the limit at the tungstenite layer bounds allocation before a
/// payload reaches the router or its per-connection channels.
pub async fn accept_connection_with_limit(
    stream: tokio::net::TcpStream,
    max_payload_bytes: usize,
) -> Option<ConnectionContext> {
    // Capture the request path + headers during the handshake callback.
    let path = Arc::new(std::sync::Mutex::new(String::new()));
    let headers = Arc::new(std::sync::Mutex::new(
        tokio_tungstenite::tungstenite::http::HeaderMap::new(),
    ));

    let path_clone = path.clone();
    let headers_clone = headers.clone();

    let callback = move |req: &Request, mut response: Response| {
        // Capture the path (with query string).
        let uri = req.uri();
        let full_path = if let Some(query) = uri.query() {
            format!("{}?{}", uri.path(), query)
        } else {
            uri.path().to_string()
        };
        *path_clone.lock().unwrap() = full_path;
        *headers_clone.lock().unwrap() = req.headers().clone();

        // Echo the first offered `Sec-WebSocket-Protocol` back in the handshake
        // response. The zero client passes its encoded initConnection/auth as a
        // WS subprotocol, and per RFC 6455 the client fails the connection if
        // the server does not select one. (The `ws` server the TS syncer uses
        // does this automatically — `protocols.values().next().value`.)
        if let Some(proto) = req
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            && let Ok(value) = proto.parse()
        {
            response
                .headers_mut()
                .insert("sec-websocket-protocol", value);
        }
        Ok(response)
    };

    let ws_config = WebSocketConfig {
        max_message_size: Some(max_payload_bytes.max(1)),
        max_frame_size: Some(max_payload_bytes.max(1)),
        ..WebSocketConfig::default()
    };
    let ws_stream = match accept_hdr_async_with_config(stream, callback, Some(ws_config)).await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::warn!("WebSocket handshake failed: {e}");
            return None;
        }
    };

    // Extract captured path + headers.
    let path = path.lock().unwrap().clone();
    let headers = headers.lock().unwrap().clone();

    // Extract protocol version from the URL path.
    let protocol_version = extract_protocol_version(&path).unwrap_or(0);

    // Extract sec-websocket-protocol header.
    let sec_protocol = headers
        .get("sec-websocket-protocol")
        .and_then(|v| v.to_str().ok());

    let origin = headers.get("origin").and_then(|v| v.to_str().ok());

    // Normalize ALL incoming request headers (lowercased names) so allowlisted
    // ones can be forwarded to the query API. Port of connect-params.ts
    // `normalizeHeaders` (#6144), whose input is Node's parsed header map — so
    // duplicate handling must match Node http: `cookie` duplicates join with
    // "; ", Node's discard-duplicates singleton headers keep the FIRST value,
    // everything else joins with ", ".
    let request_headers: std::collections::HashMap<String, String> = {
        // Node _addHeaderLine's non-array headers (first value wins).
        const NODE_SINGLETON_HEADERS: [&str; 15] = [
            "age",
            "authorization",
            "content-length",
            "content-type",
            "etag",
            "expires",
            "from",
            "host",
            "if-modified-since",
            "if-unmodified-since",
            "last-modified",
            "location",
            "max-forwards",
            "referer",
            "user-agent",
        ];
        let mut normalized = std::collections::HashMap::new();
        for (name, value) in headers.iter() {
            if let Ok(v) = value.to_str() {
                let name = name.as_str().to_string();
                normalized
                    .entry(name.clone())
                    .and_modify(|existing: &mut String| {
                        if NODE_SINGLETON_HEADERS.contains(&name.as_str()) {
                            // keep first
                        } else if name == "cookie" {
                            existing.push_str("; ");
                            existing.push_str(v);
                        } else {
                            existing.push_str(", ");
                            existing.push_str(v);
                        }
                    })
                    .or_insert_with(|| v.to_string());
            }
        }
        normalized
    };

    // The forwarded cookie comes from the normalized map so duplicate Cookie
    // headers reach the API "; "-joined (Node `headers.cookie` semantics),
    // not first-only.
    let cookie = request_headers.get("cookie").map(|c| c.to_string());

    // Build the full URL for parsing (path + query).
    let full_url = format!("http://localhost{}", path);

    // Parse connect params.
    let params = match get_connect_params(
        protocol_version,
        &full_url,
        sec_protocol,
        cookie.as_deref(),
        origin,
        request_headers,
    ) {
        Ok(params) => params,
        Err(e) => {
            tracing::warn!("connect params error: {e}");
            let error = ErrorBody::invalid_message(e.to_string());
            send_error_and_close(ws_stream, error).await;
            return None;
        }
    };

    // Validate protocol version.
    if !(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL..=PROTOCOL_VERSION).contains(&protocol_version) {
        let error = ErrorBody::version_not_supported(format!(
            "Server supports protocol versions {MIN_SERVER_SUPPORTED_SYNC_PROTOCOL} to {PROTOCOL_VERSION}, but client requested {protocol_version}"
        ));
        send_error_and_close(ws_stream, error).await;
        return None;
    }

    // Split the WebSocket into read and write halves.
    let (ws_writer, ws_reader) = ws_stream.split();

    // Channel: CG thread receives upstream messages from the WS reader.
    let (upstream_tx, upstream_rx) = mpsc::channel::<String>(256);

    // Channel: CG thread sends downstream messages to the WS writer.
    let (downstream_tx, downstream_rx) = mpsc::channel::<WsCommand>(256);
    let sink = DirectWebSocketSink::new(downstream_tx);

    // The `connected` message is sent by `Connection::init()` on the CG thread
    // (TS parity — the connection handler owns it, and it needs the server's
    // app id / shard for the client's direct-mutation addressing). Sending it
    // here too would double-send.

    // Spawn the WS writer task.
    tokio::spawn(run_ws_writer(ws_writer, downstream_rx));

    // Spawn the WS reader task.
    tokio::spawn(run_ws_reader(ws_reader, upstream_tx, params.ws_id.clone()));

    Some(ConnectionContext {
        params,
        sink,
        upstream_rx,
    })
}

/// WS writer task: drains `WsCommand`s from the channel and writes to the WebSocket.
async fn run_ws_writer(
    mut ws_writer: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        Message,
    >,
    mut rx: mpsc::Receiver<WsCommand>,
) {
    let mut last_downstream_msg_time = Instant::now();

    let mut keepalive_interval =
        tokio::time::interval(Duration::from_millis(KEEPALIVE_CHECK_INTERVAL_MS));

    loop {
        tokio::select! {
            cmd = rx.recv() => {
                match cmd {
                    Some(WsCommand::Send(value)) => {
                        let text = serde_json::to_string(&value).unwrap_or_else(|e| {
                            tracing::error!("serialization error: {e}");
                            r#"["error",{"kind":"Internal","message":"serialization failed"}]"#.to_string()
                        });
                        if ws_writer.send(Message::Text(text)).await.is_err() {
                            break;
                        }
                        last_downstream_msg_time = Instant::now();
                    }
                    Some(WsCommand::Fail(error)) => {
                        let msg = error_message(&error);
                        let text = serde_json::to_string(&msg).unwrap_or_else(|_| {
                            r#"["error",{"kind":"Internal","message":"error"}]"#.to_string()
                        });
                        let _ = ws_writer.send(Message::Text(text)).await;
                        let _ = ws_writer.send(Message::Close(Some(
                            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                code: CloseCode::from(3000u16),
                                reason: error.message().to_string().into(),
                            }
                        ))).await;
                        break;
                    }
                    Some(WsCommand::Close(reason)) => {
                        let _ = ws_writer.send(Message::Close(Some(
                            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                code: CloseCode::from(1000u16),
                                reason: reason.into(),
                            }
                        ))).await;
                        break;
                    }
                    None => break,
                }
            }
            _ = keepalive_interval.tick() => {
                if last_downstream_msg_time.elapsed()
                    > Duration::from_millis(DOWNSTREAM_MSG_INTERVAL_MS)
                {
                    let pong = serde_json::to_string(&pong_message()).unwrap_or_default();
                    if ws_writer.send(Message::Text(pong)).await.is_err() {
                        break;
                    }
                    last_downstream_msg_time = Instant::now();
                }
            }
        }
    }
}

/// WS reader task: reads WS messages, forwards raw text to upstream channel.
async fn run_ws_reader(
    mut ws_reader: futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    >,
    upstream_tx: mpsc::Sender<String>,
    ws_id: String,
) {
    while let Some(msg_result) = ws_reader.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                if upstream_tx.send(text.to_string()).await.is_err() {
                    break;
                }
            }
            Ok(Message::Binary(data)) => {
                if let Ok(text) = std::str::from_utf8(&data)
                    && upstream_tx.send(text.to_string()).await.is_err()
                {
                    break;
                }
            }
            Ok(Message::Close(_)) => {
                tracing::info!("WebSocket {ws_id} closed by client");
                break;
            }
            Ok(Message::Ping(_)) => {}
            Ok(Message::Pong(_)) => {}
            Ok(Message::Frame(_)) => {}
            Err(e) => {
                // Abrupt tab closes, mobile-network changes, and the client's
                // intentional reconnect path commonly end without an RFC 6455
                // close handshake. Node's `ws` reports these as ordinary
                // connection closure; do the same instead of polluting the
                // production warning/error signal during normal lifecycle churn.
                if is_expected_disconnect(&e) {
                    tracing::debug!("WebSocket {ws_id} disconnected: {e}");
                } else {
                    tracing::warn!("WebSocket {ws_id} read error: {e}");
                }
                break;
            }
        }
    }
}

fn is_expected_disconnect(error: &WebSocketError) -> bool {
    match error {
        WebSocketError::ConnectionClosed | WebSocketError::AlreadyClosed => true,
        WebSocketError::Protocol(ProtocolError::ResetWithoutClosingHandshake) => true,
        WebSocketError::Io(error) => matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::UnexpectedEof
        ),
        _ => false,
    }
}

/// Send an error message and close the WebSocket.
async fn send_error_and_close(
    ws_stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    error: ErrorBody,
) {
    let (mut writer, _reader) = ws_stream.split();
    let msg = error_message(&error);
    let text = serde_json::to_string(&msg).unwrap_or_default();
    let _ = writer.send(Message::Text(text)).await;
    let _ = writer
        .send(Message::Close(Some(
            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                code: CloseCode::from(3000u16),
                reason: error.message().to_string().into(),
            },
        )))
        .await;
}

/// Start the WebSocket server. Accepts connections and dispatches them to
/// the provided handler.
/// Bind the WebSocket TCP listener without serving. Split out so the caller can
/// confirm the port is bound (and emit its process-ready signal) BEFORE the
/// blocking accept loop begins.
pub async fn bind_ws_listener(port: u16) -> Result<TcpListener, std::io::Error> {
    let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!("WebSocket server listening on port {}", port);
    Ok(listener)
}

pub async fn run_ws_server<F>(config: WsServerConfig, handler: F) -> Result<(), std::io::Error>
where
    F: Fn(ConnectionContext) + Send + Sync + 'static,
{
    let listener = bind_ws_listener(config.port).await?;
    serve_ws_with_config(listener, config, handler).await
}

/// Serve the accept loop on an already-bound listener (see `bind_ws_listener`).
pub async fn serve_ws<F>(listener: TcpListener, handler: F) -> Result<(), std::io::Error>
where
    F: Fn(ConnectionContext) + Send + Sync + 'static,
{
    serve_ws_with_config(
        listener,
        WsServerConfig {
            port: 0,
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            compression: false,
        },
        handler,
    )
    .await
}

/// Serve an already-bound listener with explicit WebSocket resource limits.
pub async fn serve_ws_with_config<F>(
    listener: TcpListener,
    config: WsServerConfig,
    handler: F,
) -> Result<(), std::io::Error>
where
    F: Fn(ConnectionContext) + Send + Sync + 'static,
{
    if config.compression {
        tracing::warn!("WebSocket compression requested but is not supported by this server");
    }
    let handler = Arc::new(handler);

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                tracing::debug!("TCP connection from {addr}");
                let handler = handler.clone();
                let max_payload_bytes = config.max_payload_bytes;
                tokio::spawn(async move {
                    if let Some(ctx) = accept_connection_with_limit(stream, max_payload_bytes).await
                    {
                        handler(ctx);
                    }
                });
            }
            Err(e) => {
                tracing::warn!("TCP accept error: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_normal_transport_disconnects_as_expected() {
        assert!(is_expected_disconnect(&WebSocketError::ConnectionClosed));
        assert!(is_expected_disconnect(&WebSocketError::Protocol(
            ProtocolError::ResetWithoutClosingHandshake,
        )));
        assert!(is_expected_disconnect(&WebSocketError::Io(
            std::io::Error::from(std::io::ErrorKind::ConnectionReset),
        )));
    }

    #[test]
    fn preserves_warnings_for_real_websocket_protocol_errors() {
        assert!(!is_expected_disconnect(&WebSocketError::Protocol(
            ProtocolError::InvalidOpcode(3),
        )));
    }
}
