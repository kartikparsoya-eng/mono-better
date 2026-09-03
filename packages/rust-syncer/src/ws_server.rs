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

/// Server-side liveness close — Rust-only, OPT-IN, OFF by default
/// (parity/INVENTIONS.md I-14). TS never closes an idle client socket: the
/// syncer's `connection.ts` only keeps the 6s downstream `pong` keepalive
/// (`DOWNSTREAM_MSG_INTERVAL_MS`, ported below), and TS's heartbeat close
/// (`sendPingsForLiveness`, types/ws.ts:26) is applied only to its internal
/// streams (types/streams.ts:155/264), never to clients. A 60s default here
/// closed 50 of 344 sessions (code 1001 "liveness timeout") in the 2026-09-03
/// ART replay — clients that send no app-level `ping` — and cost 1/3 of the
/// pokes vs TS. `ZERO_WS_LIVENESS_TIMEOUT_MS=<ms>` opts in for deployments that
/// want half-open sockets (pulled cable, sleeping laptop, zero-window peer)
/// closed before the OS TCP timeout; zero-client pings every ~5s, so 60000 =
/// 12 missed pings. 0 (the default) disables it.
const DEFAULT_LIVENESS_TIMEOUT_MS: u64 = 0;

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

    // Validate protocol version — TS `Connection.init()` (connection.ts) gate.
    // In TS this runs inside `init()` on the accept handler; the Rust syncer
    // builds `Connection` on the CG thread, so the gate is applied here on the
    // accept path instead. The error message is byte-identical to TS `init()`'s
    // `VersionNotSupported` (server-vs-client phrasing by which bound was
    // crossed) so a rejected client sees the same wire error as under TS.
    if !(MIN_SERVER_SUPPORTED_SYNC_PROTOCOL..=PROTOCOL_VERSION).contains(&protocol_version) {
        crate::metrics::record_ws_connection_failure(protocol_version, "protocol_version");
        let error = ErrorBody::version_not_supported(format!(
            "server is at sync protocol v{PROTOCOL_VERSION} and does not support v{protocol_version}. The {} must be updated to a newer release.",
            if protocol_version > PROTOCOL_VERSION {
                "server"
            } else {
                "client"
            }
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
                        // TS: the shed is a downstream failure → closeWithError → 1011.
                        code: CloseCode::from(1011u16),
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
                        // TS closeWithError (types/ws.ts:11-24): warn log, then
                        // close INTERNAL_ERROR 1011 with the error text elided to
                        // 123 bytes (`endpoint` = `ws.url ?? 'client'`; server-side
                        // sockets have no url).
                        tracing::warn!(
                            kind = ?error.kind(),
                            error = %error.message(),
                            "closing connection to client with error"
                        );
                        let _ = ws_writer.send(Message::Close(Some(
                            tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                code: CloseCode::from(1011u16),
                                reason: elide(error.message(), 123).into(),
                            }
                        ))).await;
                        break;
                    }
                    Some(WsCommand::FailWithCode { error, code }) => {
                        let msg = error_message(&error);
                        let text = serde_json::to_string(&msg).unwrap_or_else(|_| {
                            r#"["error",{"kind":"Internal","message":"error"}]"#.to_string()
                        });
                        let _ = ws_writer.send(Message::Text(text)).await;
                        let close = match code {
                            // connect-time rejection: TS syncer.ts `ws.close(3000, message)`.
                            // NOTE (flagged divergence): TS passes the message
                            // un-elided and the `ws` lib throws RangeError past 123
                            // bytes; rust elides so the close is always delivered.
                            Some(code) => Message::Close(Some(
                                tokio_tungstenite::tungstenite::protocol::CloseFrame {
                                    code: CloseCode::from(code),
                                    reason: elide(error.message(), 123).into(),
                                },
                            )),
                            // TS Connection#closeWithError → ws.close(): no status
                            None => Message::Close(None),
                        };
                        let _ = ws_writer.send(close).await;
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
                        // TS Connection.close() → ws.close() with no status
                        // (workers/connection.ts:182); the reason is logged, not sent.
                        tracing::info!("closing connection: {reason}");
                        let _ = ws_writer.send(Message::Close(None)).await;
                        break;
                    }
                    None => break,
                }
            }
            _ = keepalive_interval.tick() => {
                // Opt-in liveness close (I-14; off by default = TS behaviour):
                // a client that has sent NOTHING (zero-client pings every ~5s)
                // for the timeout is half-open — close it rather than queue
                // pokes against a dead socket until the OS TCP timeout.
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
                reason: elide(error.message(), 123).into(),
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
                // Port of Node `ws` (websocket.js `socket.setNoDelay()`):
                // every accepted sync socket disables Nagle. Without it the
                // multi-frame poke burst (pokeStart → pokePart → pokeEnd as
                // separate writes) stalls ~40-50ms on Nagle + delayed-ACK —
                // measured live as a constant +50ms push-ack penalty
                // (G42 push class 2.0x vs TS).
                let _ = stream.set_nodelay(true);
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

/// Port of TS `elide` (types/strings.ts:1), folded into its consumer — the
/// crate has no `types/` twin. Close-frame reasons must be ≤ 123 BYTES
/// (RFC 6455 control-frame payload ≤ 125 incl. the 2-byte code); TS
/// `closeWithError` elides the error text to 123 (types/ws.ts:23). Byte-aware:
/// trims whole chars until `val + "..."` fits.
pub(crate) fn elide(val: &str, max_bytes: usize) -> String {
    if val.len() <= max_bytes {
        return val.to_string();
    }
    // TS: `val.substring(0, maxBytes - 3)` — a CHAR count, then shrink by bytes.
    let mut val: String = val.chars().take(max_bytes.saturating_sub(3)).collect();
    while val.len() + 3 > max_bytes {
        val.pop();
    }
    val + "..."
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TS-golden (types/strings.test.ts "elide byte count"): ASCII elides to
    /// exactly 123 chars; full-width (3-byte) chars trim to ≤ 123 BYTES.
    #[test]
    fn elide_byte_count() {
        let ascii = elide(&format!("fo{}", "o".repeat(150)), 123);
        assert_eq!(ascii, format!("fo{}...", "o".repeat(118)));
        assert_eq!(ascii.len(), 123);
        let full = elide(&format!("こんにちは{}", "あ".repeat(150)), 123);
        assert_eq!(full, format!("こんにちは{}...", "あ".repeat(35)));
        assert!(full.len() <= 123);
        assert_eq!(elide("short", 123), "short");
    }

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
        let _ = ws.send(Message::Text("x".repeat(1500))).await;

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

    /// Port of TS `closeWithProtocolError` (connect.ts `closeWithError`
    /// ordering): a pre-CG connect failure sends the `["error", body]` frame
    /// FIRST, then the close frame with code 3000 carrying the error message
    /// as the reason — never a bare close (the client would see an opaque
    /// 1005/1006 and could not classify the failure). G36 error surface.
    #[tokio::test]
    async fn send_error_and_close_sends_error_frame_then_close_3000() {
        use futures_util::StreamExt as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            send_error_and_close(ws, ErrorBody::invalid_message("invalid connection params")).await;
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut ws, _) = tokio_tungstenite::client_async("ws://localhost/", stream)
            .await
            .unwrap();

        // Frame 1: the ["error", body] tuple with the exact kind + message.
        let first = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for the error frame")
            .expect("stream ended before the error frame")
            .unwrap();
        let text = match first {
            Message::Text(t) => t.to_string(),
            other => panic!("expected the error TEXT frame first, got {other:?}"),
        };
        let frame: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(frame[0], "error");
        assert_eq!(frame[1]["kind"], "InvalidMessage");
        assert_eq!(frame[1]["message"], "invalid connection params");

        // Frame 2: the close frame — code 3000, reason = the error message.
        let second = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for the close frame")
            .expect("stream ended before the close frame")
            .unwrap();
        match second {
            Message::Close(Some(cf)) => {
                assert_eq!(u16::from(cf.code), 3000);
                assert_eq!(cf.reason.as_ref(), "invalid connection params");
            }
            other => panic!("expected a close frame with code 3000, got {other:?}"),
        }
        let _ = server.await;
    }

    /// I-4 slow-client shed parity (INVENTIONS.md I-4 — closes its GAP).
    /// The ws_sink HWM shed is a Rust invention (TS relies on runtime
    /// backpressure); its contract is that a shed is *observationally* the SAME
    /// backoff TS emits for a connection it can no longer serve —
    /// `ErrorKind::Rehome` (view-syncer.ts:473 / cvr-store.ts:1373), telling the
    /// client to reconnect to a fresh assignment. When `run_ws_writer`'s `kill`
    /// watch trips (depth crossed the HWM in `DirectWebSocketSink`), it MUST send
    /// an `["error", {kind:"Rehome"}]` frame FIRST, then a close frame (code
    /// 3000) — never a bare 1006, which the client cannot classify.
    ///
    /// NON-VACUOUS: change `ErrorBody::rehome(...)` at the shed arm to any other
    /// kind, or drop the error frame, and the `kind == "Rehome"` / frame-present
    /// assertions fail. (Verified by reverting to a bare close.)
    #[tokio::test]
    async fn slow_client_shed_closes_with_rehome_error_then_close_1011() {
        use futures_util::StreamExt as _;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let (w, _r) = ws.split();
            let (_tx, rx) = mpsc::unbounded_channel::<WsCommand>();
            let (kill_tx, kill_rx) = watch::channel(false);
            let limits = Arc::new(SinkLimits {
                depth: Arc::new(AtomicI64::new(0)),
                hwm: 1_000_000,
                bytes: Arc::new(AtomicI64::new(0)),
                byte_hwm: i64::MAX,
                kill: kill_tx,
                shed_counted: std::sync::atomic::AtomicBool::new(false),
            });
            let last = Arc::new(AtomicI64::new(now_epoch_ms()));
            let writer = tokio::spawn(run_ws_writer(w, rx, limits.clone(), kill_rx, last));
            // Trip the shed exactly as `DirectWebSocketSink` does when the
            // downstream queue depth crosses the HWM.
            let _ = limits.kill.send(true);
            let _ = writer.await;
        });

        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut ws, _) = tokio_tungstenite::client_async("ws://localhost/", stream)
            .await
            .unwrap();

        // Frame 1: ["error", {kind:"Rehome", ...}] — the shed backoff.
        let first = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for the shed error frame")
            .expect("stream ended before the error frame")
            .unwrap();
        let text = match first {
            Message::Text(t) => t.to_string(),
            other => panic!("expected the error TEXT frame first, got {other:?}"),
        };
        let frame: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(frame[0], "error");
        assert_eq!(
            frame[1]["kind"], "Rehome",
            "a slow-client shed must be observationally a Rehome (I-4), got {frame:?}"
        );

        // Frame 2: the close frame — code 3000.
        let second = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("timed out waiting for the close frame")
            .expect("stream ended before the close frame")
            .unwrap();
        match second {
            Message::Close(Some(cf)) => assert_eq!(
                u16::from(cf.code),
                1011,
                "shed goes through TS downstream.fail → closeWithError → INTERNAL_ERROR 1011 \
                 (types/streams.ts:91, types/ws.ts:7), got {:?}",
                cf.code
            ),
            other => panic!("expected close 1011 after the Rehome error, got {other:?}"),
        }
        let _ = server.await;
    }

    /// Close codes are per TS path: a downstream `Fail` closes 1011 (TS
    /// `closeWithError` default INTERNAL_ERROR, types/ws.ts:7 via
    /// types/streams.ts:91); a connect-time rejection closes 3000 (TS
    /// syncer.ts:610/639 `ws.close(3000, message)`); a connection-level error
    /// or a normal close sends NO status (TS `Connection.close()` →
    /// `ws.close()`, workers/connection.ts:182, → peer sees 1005).
    #[tokio::test]
    async fn writer_close_codes_follow_the_ts_path() {
        use futures_util::StreamExt as _;
        async fn run(cmds: Vec<WsCommand>) -> Vec<Message> {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                let (w, _r) = ws.split();
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
                let last = Arc::new(AtomicI64::new(now_epoch_ms()));
                let writer = tokio::spawn(run_ws_writer(w, rx, limits, kill_rx, last));
                for c in cmds {
                    let _ = tx.send(c);
                }
                let _ = writer.await;
            });
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let (mut ws, _) = tokio_tungstenite::client_async("ws://localhost/", stream)
                .await
                .unwrap();
            let mut out = Vec::new();
            while let Ok(Some(Ok(m))) =
                tokio::time::timeout(Duration::from_secs(5), ws.next()).await
            {
                let done = matches!(m, Message::Close(_));
                out.push(m);
                if done {
                    break;
                }
            }
            let _ = server.await;
            out
        }
        let err = ErrorBody::basic(crate::protocol::ErrorKind::Internal, "boom".to_string());
        // downstream fail → error frame + 1011
        let fr = run(vec![WsCommand::Fail(err.clone())]).await;
        assert!(
            matches!(&fr[0], Message::Text(_)),
            "error frame first, got {fr:?}"
        );
        match &fr[1] {
            Message::Close(Some(cf)) => assert_eq!(
                u16::from(cf.code),
                1011,
                "Fail must close 1011, got {:?}",
                cf.code
            ),
            other => panic!("expected close 1011, got {other:?}"),
        }
        // TS closeWithError: `ws.close(code, elide(errMsg, 123))` — a long
        // error text must NOT produce an over-long close reason (RFC 6455:
        // control payload ≤ 125 bytes; browsers fail the socket with 1002
        // instead of delivering our close).
        let long = "x".repeat(300);
        let fr = run(vec![WsCommand::Fail(ErrorBody::basic(
            crate::protocol::ErrorKind::Internal,
            long.clone(),
        ))])
        .await;
        match &fr[1] {
            Message::Close(Some(cf)) => {
                assert_eq!(cf.reason.as_ref(), elide(&long, 123));
                assert!(cf.reason.len() <= 123, "reason {} bytes", cf.reason.len());
            }
            other => panic!("expected close 1011 with an elided reason, got {other:?}"),
        }
        // connect-time rejection → error frame + 3000
        let fr = run(vec![WsCommand::FailWithCode {
            error: err.clone(),
            code: Some(3000),
        }])
        .await;
        match &fr[1] {
            Message::Close(Some(cf)) => assert_eq!(
                u16::from(cf.code),
                3000,
                "connect-time must close 3000, got {:?}",
                cf.code
            ),
            other => panic!("expected close 3000, got {other:?}"),
        }
        // connection-level error → error frame + close with NO status
        let fr = run(vec![WsCommand::FailWithCode {
            error: err,
            code: None,
        }])
        .await;
        assert!(
            matches!(&fr[0], Message::Text(_)),
            "error frame first, got {fr:?}"
        );
        assert!(
            matches!(&fr[1], Message::Close(None)),
            "Connection#closeWithError → ws.close() with no status, got {:?}",
            fr[1]
        );
        // normal close → NO status (TS Connection.close() → ws.close())
        let fr = run(vec![WsCommand::Close("bye".to_string())]).await;
        assert!(
            matches!(&fr[0], Message::Close(None)),
            "Connection.close() → ws.close() with no status, got {:?}",
            fr[0]
        );
    }
}

#[cfg(test)]
mod liveness_default_tests {
    use super::*;

    /// I-14: with no operator opt-in the server never closes an idle client
    /// (TS parity — connection.ts has no idle close). Non-vacuous: a 60_000
    /// default makes the first assertion fail. Env is read at call time, so
    /// the opt-in parse is checked in the same test (no cross-test env race).
    #[test]
    fn liveness_close_is_disabled_by_default_and_opt_in_via_env() {
        // SAFETY: single-threaded within this test; restored below.
        unsafe { std::env::remove_var("ZERO_WS_LIVENESS_TIMEOUT_MS") };
        assert_eq!(
            liveness_timeout_ms(),
            0,
            "default must be 0 (disabled): TS never closes an idle client socket"
        );
        unsafe { std::env::set_var("ZERO_WS_LIVENESS_TIMEOUT_MS", "250") };
        assert_eq!(
            liveness_timeout_ms(),
            250,
            "operator opt-in must be honoured"
        );
        unsafe { std::env::set_var("ZERO_WS_LIVENESS_TIMEOUT_MS", "0") };
        assert_eq!(liveness_timeout_ms(), 0, "explicit 0 disables");
        unsafe { std::env::set_var("ZERO_WS_LIVENESS_TIMEOUT_MS", "junk") };
        assert_eq!(
            liveness_timeout_ms(),
            0,
            "unparseable falls back to the default (disabled)"
        );
        unsafe { std::env::remove_var("ZERO_WS_LIVENESS_TIMEOUT_MS") };
    }
}
