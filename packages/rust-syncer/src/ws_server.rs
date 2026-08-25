//! WebSocket server — port of the WebSocket acceptance + connection lifecycle
//! from `workers/syncer.ts` and `workers/connection.ts`.
//!
//! Uses `tokio-tungstenite` for the WebSocket protocol. No handoff mechanism
//! (the server accepts directly, unlike the TS handoff model).

use crate::protocol::{
    ErrorBody, MIN_SERVER_SUPPORTED_SYNC_PROTOCOL, PROTOCOL_VERSION, error_message, pong_message,
};
use crate::workers::connect_params::{ConnectParams, extract_protocol_version, get_connect_params};
use crate::ws_sink::{DirectWebSocketSink, SinkLimits, WsCommand};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
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
/// TS parity: zero-config `websocketMaxPayloadBytes` defaults to 10MB and
/// rejects larger messages before parsing.
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 10 * 1024 * 1024;

/// Slow-client shed: max queued downstream commands before the connection is
/// force-closed (the client reconnects + rehydrates). Each `Send` command is
/// one WS frame; poke parts batch up to 100 patches per frame, so 4096 frames
/// is a very large in-flight backlog — a healthy client never approaches it,
/// while a stalled TCP window would otherwise buffer pokes in process memory
/// without bound. Env override: `ZERO_WS_DOWNSTREAM_HWM`.
const DEFAULT_DOWNSTREAM_QUEUE_HWM: i64 = 4096;

/// Primary memory bound: estimated serialized bytes queued downstream for one
/// connection before it is shed. A single command can be a multi-MB tree, so the
/// frame-count HWM alone can't bound memory. 256MB estimated-serialized ≈ up to
/// ~0.75GB in-memory worst case per pinned connection (a `Value` tree is 2–4× its
/// serialized size). Generous by design — the HWM is a safety valve against a
/// *stalled* client, not a slow one on a big hydrate. Env override:
/// `ZERO_WS_DOWNSTREAM_BYTE_HWM` (0 disables byte shedding — rollout escape hatch).
const DEFAULT_DOWNSTREAM_BYTE_HWM: i64 = 256 * 1024 * 1024;

/// Server-side liveness: close a connection that has sent NOTHING for this
/// long. zero-client pings every ~5s, so 60s = 12 missed pings; a half-open
/// socket (pulled cable, sleeping laptop) otherwise queues pokes until the OS
/// TCP timeout (~15-30 min) — or forever against a zero-window peer. Env
/// override: `ZERO_WS_LIVENESS_TIMEOUT_MS` (0 disables).
const DEFAULT_LIVENESS_TIMEOUT_MS: u64 = 60_000;

fn downstream_queue_hwm() -> i64 {
    std::env::var("ZERO_WS_DOWNSTREAM_HWM")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &i64| *v > 0)
        .unwrap_or(DEFAULT_DOWNSTREAM_QUEUE_HWM)
}

/// Byte HWM for the downstream queue. Unlike the frame HWM, `0` is a legal value
/// here (disables byte shedding), so a parsed `0` is honored rather than filtered.
fn downstream_byte_hwm() -> i64 {
    std::env::var("ZERO_WS_DOWNSTREAM_BYTE_HWM")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &i64| *v >= 0)
        .unwrap_or(DEFAULT_DOWNSTREAM_BYTE_HWM)
}

fn liveness_timeout_ms() -> u64 {
    std::env::var("ZERO_WS_LIVENESS_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_LIVENESS_TIMEOUT_MS)
}

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

    // The tungstenite-layer limit sits at 2× the advertised cap; the CAP
    // itself is enforced in `run_ws_reader` on cleanly-read messages. A
    // tungstenite Capacity error aborts mid-frame WITHOUT consuming the
    // payload, leaving the stream misaligned — the subsequent close
    // handshake then races the poisoned read state and the client
    // nondeterministically sees 1006 instead of 1009 (observed live at
    // 12MB). Reading the frame fully (bounded by 2×cap) and rejecting
    // above tungstenite keeps the stream aligned so the Node-parity 1009
    // close + drain is deterministic. Frames beyond 2×cap still hit the
    // tungstenite Capacity guard (allocation bound), with best-effort 1009.
    let ws_config = WebSocketConfig {
        max_message_size: Some(max_payload_bytes.max(1).saturating_mul(2)),
        max_frame_size: Some(max_payload_bytes.max(1).saturating_mul(2)),
        ..WebSocketConfig::default()
    };
    let ws_stream = match accept_hdr_async_with_config(stream, callback, Some(ws_config)).await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::warn!("WebSocket handshake failed: {e}");
            crate::metrics::record_ws_connection_failure(0, "handshake");
            return None;
        }
    };

    // Extract captured path + headers.
    let path = path.lock().unwrap().clone();
    let headers = headers.lock().unwrap().clone();

    // Extract protocol version from the URL path.
    let protocol_version = extract_protocol_version(&path).unwrap_or(0);
    crate::metrics::record_ws_connection_attempt(protocol_version);

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
            crate::metrics::record_ws_connection_failure(protocol_version, "configuration");
            let error = ErrorBody::invalid_message(e.to_string());
            send_error_and_close(ws_stream, error).await;
            return None;
        }
    };

    // Validate protocol version.
    if !(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL..=PROTOCOL_VERSION).contains(&protocol_version) {
        crate::metrics::record_ws_connection_failure(protocol_version, "protocol_version");
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

    // Channel: CG thread sends downstream messages to the WS writer. Unbounded
    // to preserve poke frame order from the sync, in-runtime `push` path (see
    // `ws_sink` module docs); memory is bounded by the slow-client shed policy
    // (queue-depth HWM trips `kill`, which the writer reacts to immediately).
    let (downstream_tx, downstream_rx) = mpsc::unbounded_channel::<WsCommand>();
    let (kill_tx, kill_rx) = watch::channel(false);
    let limits = Arc::new(SinkLimits {
        depth: Arc::new(AtomicI64::new(0)),
        hwm: downstream_queue_hwm(),
        bytes: Arc::new(AtomicI64::new(0)),
        byte_hwm: downstream_byte_hwm(),
        kill: kill_tx,
        shed_counted: std::sync::atomic::AtomicBool::new(false),
    });
    let sink = DirectWebSocketSink::with_limits(downstream_tx, limits.clone());
    // Wall-clock of the last frame received FROM the client (liveness).
    let last_inbound = Arc::new(AtomicI64::new(now_epoch_ms()));

    // The `connected` message is sent by `Connection::init()` on the CG thread
    // (TS parity — the connection handler owns it, and it needs the server's
    // app id / shard for the client's direct-mutation addressing). Sending it
    // here too would double-send.

    // Spawn the WS writer task.
    tokio::spawn(run_ws_writer(
        ws_writer,
        downstream_rx,
        limits,
        kill_rx,
        last_inbound.clone(),
    ));

    // Open-connections gauge: +1 here, -1 when the reader task exits (the
    // reader always terminates on close/error, so the decrement fires exactly
    // once per accepted connection — TS pairs the same way via `ws.once('close')`).
    crate::metrics::record_ws_open_delta(1, protocol_version);

    // Spawn the WS reader task. The sink clone lets the reader relay
    // protocol-level rejections (oversized frame → Close 1009) through the
    // writer, since the split read half cannot write.
    tokio::spawn(run_ws_reader(
        ws_reader,
        upstream_tx,
        params.ws_id.clone(),
        last_inbound,
        protocol_version,
        sink.clone(),
        max_payload_bytes,
    ));

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
    mut rx: mpsc::UnboundedReceiver<WsCommand>,
    limits: Arc<SinkLimits>,
    mut kill_rx: watch::Receiver<bool>,
    last_inbound: Arc<AtomicI64>,
) {
    let mut last_downstream_msg_time = Instant::now();
    let liveness_timeout = liveness_timeout_ms();

    let mut keepalive_interval =
        tokio::time::interval(Duration::from_millis(KEEPALIVE_CHECK_INTERVAL_MS));

    loop {
        tokio::select! {
            // Slow-client shed tripped: close IMMEDIATELY, ahead of the queued
            // backlog (which is exactly what crossed the limit).
            _ = kill_rx.changed() => {
                tracing::warn!("closing slow client: downstream queue exceeded HWM");
                let error = ErrorBody::rehome("Server buffer overflow (slow connection)");
                let msg = error_message(&error);
                if let Ok(text) = serde_json::to_string(&msg) {
                    let _ = ws_writer.send(Message::Text(text)).await;
                }
                let _ = ws_writer.send(Message::Close(Some(
                    tokio_tungstenite::tungstenite::protocol::CloseFrame {
                        code: CloseCode::from(3000u16),
                        reason: "slow client".into(),
                    }
                ))).await;
                break;
            }
            cmd = rx.recv() => {
                if let Some(ref c) = cmd {
                    limits.depth.fetch_sub(1, Ordering::SeqCst);
                    crate::metrics::record_ws_queued_delta(-1);
                    // Symmetric byte accounting: subtract EXACTLY what the sink
                    // added for this command (only `Send` carries bytes).
                    if let WsCommand::Send { est_bytes, .. } = c {
                        limits.bytes.fetch_sub(*est_bytes as i64, Ordering::SeqCst);
                        crate::metrics::record_ws_queued_bytes_delta(-(*est_bytes as i64));
                    }
                }
                match cmd {
                    Some(WsCommand::Send { msg, .. }) => {
                        let text = serde_json::to_string(&msg).unwrap_or_else(|e| {
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
                    Some(WsCommand::CloseWithCode { code, reason }) => {
                        let _ = ws_writer.send(Message::Close(Some(
                            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                code: CloseCode::from(code),
                                reason: reason.into(),
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
                // Liveness: a client that has sent NOTHING (zero-client pings
                // every ~5s) for the timeout is half-open — close it rather than
                // queue pokes against a dead socket until the OS TCP timeout.
                if liveness_timeout > 0 {
                    let idle_ms = now_epoch_ms() - last_inbound.load(Ordering::Relaxed);
                    if idle_ms > liveness_timeout as i64 {
                        tracing::info!(
                            "closing unresponsive client (no inbound frame for {idle_ms}ms)"
                        );
                        crate::metrics::record_ws_shed("liveness");
                        let _ = ws_writer.send(Message::Close(Some(
                            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                code: CloseCode::from(1001u16),
                                reason: "liveness timeout".into(),
                            }
                        ))).await;
                        break;
                    }
                }
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

    // The writer is exiting (shed/Fail/Close/socket error/liveness). Commands
    // still queued were counted into the process-global queued-frames/bytes
    // gauges at enqueue but will never be dequeued here — drain them for metrics
    // accounting so the gauges don't monotonically inflate under connection
    // churn (worst on the slow-client kill path, where the backlog is largest).
    // The per-connection `limits` counters need no fixup: the whole
    // `SinkLimits` Arc drops with this task.
    while let Ok(cmd) = rx.try_recv() {
        crate::metrics::record_ws_queued_delta(-1);
        if let WsCommand::Send { est_bytes, .. } = cmd {
            crate::metrics::record_ws_queued_bytes_delta(-(est_bytes as i64));
        }
    }
}

/// Wall-clock millis (liveness bookkeeping shared between reader and writer).
fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// WS reader task: reads WS messages, forwards raw text to upstream channel.
/// Node `ws` closeTimeout parity: keep CONSUMING the socket after queueing a
/// close, so a client caught mid-upload can finish writing and then read our
/// close frame. Dropping the read half immediately RSTs the still-writing
/// client and it observes 1006 with the close frame discarded. Bounded grace;
/// parse errors are tolerated (a mid-frame abort leaves garbage "headers").
async fn drain_until_peer_close(
    ws_reader: &mut futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    >,
) {
    let _ = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(res) = ws_reader.next().await {
            match res {
                Ok(Message::Close(_)) => break,
                Ok(_) => {}
                Err(
                    WebSocketError::ConnectionClosed
                    | WebSocketError::AlreadyClosed
                    | WebSocketError::Io(_),
                ) => break,
                Err(_) => {}
            }
        }
    })
    .await;
}

async fn run_ws_reader(
    mut ws_reader: futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    >,
    upstream_tx: mpsc::Sender<String>,
    ws_id: String,
    last_inbound: Arc<AtomicI64>,
    protocol_version: u32,
    sink: DirectWebSocketSink,
    max_payload_bytes: usize,
) {
    while let Some(msg_result) = ws_reader.next().await {
        // Any frame from the client — including protocol-level ping/pong —
        // counts as liveness.
        last_inbound.store(now_epoch_ms(), Ordering::Relaxed);
        // TS parity: Node `ws` rejects a message above `maxPayload` with
        // Close 1009 "Max payload size exceeded". Enforced HERE, on a
        // fully-read message (tungstenite's own limit is 2× — see the
        // accept-time config comment), so the stream stays aligned and the
        // close handshake + drain is deterministic.
        let too_big = match &msg_result {
            Ok(Message::Text(t)) => t.len() > max_payload_bytes,
            Ok(Message::Binary(b)) => b.len() > max_payload_bytes,
            _ => false,
        };
        if too_big {
            tracing::warn!("WebSocket {ws_id} message over max payload");
            crate::metrics::record_websocket_error("error_event", protocol_version);
            sink.close_with_code(1009, "Max payload size exceeded".to_string());
            drain_until_peer_close(&mut ws_reader).await;
            break;
        }
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
                // TS parity: Node `ws` rejects a frame above `maxPayload` by
                // sending Close 1009 "Max payload size exceeded" (ws
                // receiver.js RangeError → 1009). tungstenite surfaces it as
                // a Capacity error on the READ half, which cannot write —
                // relay the 1009 close through the writer's queue so the
                // client sees the same close code as TS, not an abnormal
                // 1006 teardown.
                if matches!(e, WebSocketError::Capacity(_)) {
                    tracing::warn!("WebSocket {ws_id} frame over max payload: {e}");
                    crate::metrics::record_websocket_error("error_event", protocol_version);
                    sink.close_with_code(1009, "Max payload size exceeded".to_string());
                    // Frames beyond 2× the cap abort mid-frame, so this
                    // drain chews a misaligned stream (best-effort 1009);
                    // the ≤2× path above is the deterministic one.
                    drain_until_peer_close(&mut ws_reader).await;
                    break;
                }
                // Abrupt tab closes, mobile-network changes, and the client's
                // intentional reconnect path commonly end without an RFC 6455
                // close handshake. Node's `ws` reports these as ordinary
                // connection closure; do the same instead of polluting the
                // production warning/error signal during normal lifecycle churn.
                if is_expected_disconnect(&e) {
                    tracing::debug!("WebSocket {ws_id} disconnected: {e}");
                    // A reset/abrupt close without an RFC 6455 handshake — TS
                    // `#handleClose` with `wasClean === false`.
                    crate::metrics::record_websocket_error("unclean_close", protocol_version);
                } else {
                    tracing::warn!("WebSocket {ws_id} read error: {e}");
                    // A real transport/protocol error — TS `#handleError`.
                    crate::metrics::record_websocket_error("error_event", protocol_version);
                }
                break;
            }
        }
    }
    crate::metrics::record_ws_open_delta(-1, protocol_version);
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

    /// G36 oversized-payload: a frame above the payload cap must close with
    /// RFC 6455 code 1009 "Max payload size exceeded" — Node `ws` behavior
    /// (receiver RangeError → 1009), which TS zero-cache inherits via
    /// `maxPayload`. Before the fix the reader just dropped the transport and
    /// clients observed an abnormal 1006 with no close frame.
    #[tokio::test]
    async fn oversized_frame_closes_with_1009_like_node_ws() {
        use futures_util::{SinkExt as _, StreamExt as _};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            // Mirror production: tungstenite limit = 2× the enforced cap.
            let cfg = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
                max_message_size: Some(2048),
                ..Default::default()
            };
            let ws = tokio_tungstenite::accept_async_with_config(stream, Some(cfg))
                .await
                .unwrap();
            let (w, r) = ws.split();
            let (tx, rx) = mpsc::unbounded_channel::<WsCommand>();
            let (kill_tx, kill_rx) = watch::channel(false);
            let limits = Arc::new(SinkLimits {
                depth: Arc::new(AtomicI64::new(0)),
                hwm: 1_000_000,
                bytes: Arc::new(AtomicI64::new(0)),
                byte_hwm: i64::MAX,
                kill: kill_tx,
                shed_counted: std::sync::atomic::AtomicBool::new(false),
            });
            let sink = DirectWebSocketSink::with_limits(tx, limits.clone());
            let (up_tx, up_rx) = mpsc::channel::<String>(16);
            let last = Arc::new(AtomicI64::new(now_epoch_ms()));
            tokio::spawn(run_ws_writer(w, rx, limits, kill_rx, last.clone()));
            run_ws_reader(
                r,
                up_tx,
                "test-ws".into(),
                last,
                PROTOCOL_VERSION,
                sink,
                1024,
            )
            .await;
            drop(up_rx);
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut ws, _) = tokio_tungstenite::client_async("ws://localhost/", stream)
            .await
            .unwrap();
        // 1500 bytes: above the enforced cap (1024) but under the
        // tungstenite layer's 2× limit, so the frame is read CLEANLY and
        // rejected by the reader's own size check — the deterministic 1009
        // path (the mid-frame Capacity abort path is best-effort only; its
        // regression net is the live xyne-art G36 oversized-payload case).
        let _ = ws.send(Message::Text("x".repeat(1500).into())).await;

        let mut close_code = None;
        let deadline = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = &mut deadline => break,
                m = ws.next() => match m {
                    Some(Ok(Message::Close(Some(cf)))) => {
                        close_code = Some(u16::from(cf.code));
                        break;
                    }
                    Some(Ok(_)) => {}
                    _ => break, // closed without a close frame (the old bug)
                }
            }
        }
        assert_eq!(
            close_code,
            Some(1009),
            "oversized frame must close 1009 like Node ws, got {close_code:?}"
        );
        server.abort();
    }
}
