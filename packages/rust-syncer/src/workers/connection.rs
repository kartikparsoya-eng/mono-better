//! Connection — port of `workers/connection.ts` (Connection class, ~457 LOC).
//!
//! Handles incoming messages on a WebSocket connection and dispatches them
//! to the correct service. Manages keepalive pongs, error classification,
//! and connection lifecycle (close, cleanup).
//!
//! In the Rust syncer, the Connection runs on the CG (client group) thread.
//! The WS I/O is handled by tokio tasks — the CG thread receives parsed
//! upstream messages via a channel and sends downstream messages via the
//! `DirectWebSocketSink`.

use crate::protocol::{
    self, ErrorBody, ErrorKind, MIN_SERVER_SUPPORTED_SYNC_PROTOCOL, PROTOCOL_VERSION,
    connected_message, error_message, pong_message,
};
use crate::ws_sink::DirectWebSocketSink;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

/// Result of handling an upstream message.
/// Matches the TS `HandlerResult` type.
#[derive(Debug)]
pub enum HandlerResult {
    /// Message processed successfully.
    Ok,
    /// Fatal error — connection should be closed.
    Fatal { error: ErrorBody },
    /// Transient errors — sent to client but connection stays open.
    Transient { errors: Vec<ErrorBody> },
}

/// Trait for message handlers (port of TS `MessageHandler` interface).
///
/// Implemented by `SyncerWsMessageHandler`, which dispatches to ViewSyncer,
/// Mutagen, Pusher. `async` + `?Send` (L9 Stage 3d): the live ViewSyncer
/// dispatch executes the message body INLINE on the CG task (TS `#lock` is
/// FIFO-at-arrival — re-enqueueing would reorder), so the handler awaits it
/// there and holds CG-local (`!Send`) state.
#[async_trait::async_trait(?Send)]
pub trait MessageHandler {
    /// Handle a parsed upstream message.
    /// Returns a list of `HandlerResult`s.
    async fn handle_message(&self, msg: &str) -> Vec<HandlerResult>;
}

/// Connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsState {
    Connecting,
    Open,
    Closing,
    Closed,
}

/// A connection between a client and the server.
///
/// Port of `Connection` class in `connection.ts`.
/// Runs on the CG thread. The WS I/O is handled by tokio tasks.
pub struct Connection {
    /// WebSocket sink for sending downstream messages.
    sink: DirectWebSocketSink,
    /// Protocol version negotiated during handshake.
    protocol_version: u32,
    /// WebSocket ID.
    ws_id: String,
    /// Client ID.
    client_id: String,
    /// Client group ID.
    client_group_id: String,
    /// Server app id + shard number, echoed in the `connected` message so a
    /// direct-mutation client can address the mutate endpoint.
    app_id: String,
    shard_num: u32,
    /// Whether the connection has been closed.
    closed: AtomicBool,
    /// Time of last downstream message sent.
    last_downstream_msg_time: std::sync::Mutex<Instant>,
    /// The message handler for dispatching upstream messages.
    handler: Box<dyn MessageHandler>,
    /// Called when the connection is closed.
    on_close: Box<dyn Fn() + Send + Sync>,
    /// Live-instance census guard (leak hunt): inc on construct, dec on drop.
    _census: crate::live_count::Guard,
}

impl Connection {
    /// Create a new connection.
    ///
    /// In the TS code, the constructor sets up event listeners and starts
    /// proxying inbound messages. In Rust, the WS reader task already forwards
    /// messages to a channel — the CG thread calls `handle_inbound()` for each.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sink: DirectWebSocketSink,
        protocol_version: u32,
        ws_id: String,
        client_id: String,
        client_group_id: String,
        app_id: String,
        shard_num: u32,
        handler: Box<dyn MessageHandler>,
        on_close: Box<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            sink,
            protocol_version,
            ws_id,
            client_id,
            client_group_id,
            app_id,
            shard_num,
            closed: AtomicBool::new(false),
            last_downstream_msg_time: std::sync::Mutex::new(Instant::now()),
            handler,
            on_close,
            _census: crate::live_count::Guard::new(&crate::live_count::CONNECTION),
        }
    }

    /// Check the protocol version and send the `connected` message.
    ///
    /// Port of `Connection.init()`.
    /// Returns `true` if the version is supported, `false` if the connection
    /// was closed with a `VersionNotSupported` error.
    pub fn init(&self) -> bool {
        if self.protocol_version > PROTOCOL_VERSION
            || self.protocol_version < MIN_SERVER_SUPPORTED_SYNC_PROTOCOL
        {
            let error = ErrorBody::version_not_supported(format!(
                "server is at sync protocol v{PROTOCOL_VERSION} and does not support v{}. The {} must be updated to a newer release.",
                self.protocol_version,
                if self.protocol_version > PROTOCOL_VERSION {
                    "server"
                } else {
                    "client"
                }
            ));
            self.close_with_error(error);
            false
        } else {
            self.send(connected_message(&self.ws_id, &self.app_id, self.shard_num));
            true
        }
    }

    /// Handle an inbound message (raw JSON text from the WebSocket).
    ///
    /// Port of `Connection.#handleMessage()`.
    /// Returns `true` if the connection is still open, `false` if closed.
    pub async fn handle_inbound(&self, data: &str) -> bool {
        if self.closed.load(Ordering::Relaxed) {
            tracing::debug!("Ignoring message received after closed: {data}");
            return false;
        }

        // Parse the message.
        let parsed = match protocol::parse_upstream(data) {
            Ok(msg) => msg,
            Err(e) => {
                let error = ErrorBody::invalid_message(e.to_string());
                self.close_with_error(error);
                return false;
            }
        };

        // Handle ping immediately — don't go through the message handler.
        if matches!(parsed, protocol::Upstream::Ping) {
            self.send(pong_message());
            return true;
        }

        // Dispatch to the message handler.
        let results = self.handler.handle_message(data).await;
        for result in results {
            if !self.handle_result(result) {
                return false;
            }
        }
        true
    }

    /// Process a HandlerResult.
    ///
    /// Port of `Connection.#handleMessageResult()`.
    /// Returns `true` if the connection is still open.
    fn handle_result(&self, result: HandlerResult) -> bool {
        match result {
            HandlerResult::Ok => true,
            HandlerResult::Fatal { error } => {
                self.close_with_error(error);
                false
            }
            HandlerResult::Transient { errors } => {
                for error in errors {
                    self.send_error(error);
                }
                true
            }
        }
    }

    /// Handle a close event from the WebSocket.
    ///
    /// Port of `Connection.#handleClose()`.
    pub fn handle_close(&self, code: u16, reason: &str) {
        self.close(&format!(
            "WebSocket close event: code={code}, reason={reason}"
        ));
    }

    /// Handle an error event from the WebSocket.
    ///
    /// Port of `Connection.#handleError()`.
    pub fn handle_error(&self, message: &str) {
        tracing::warn!(
            client_id = %self.client_id,
            ws_id = %self.ws_id,
            "WebSocket error event: {message}"
        );
    }

    /// Close the connection with an error (TS `Connection.#closeWithError`):
    /// send the error downstream, then close.
    ///
    /// This is the CONNECTION-level close, for a failure TS raises without a
    /// `ClientHandler` in hand (a message-parse throw, a rejected initConnection
    /// before the handler exists). When TS instead fails a REGISTERED client —
    /// `client.fail(e)` — the ported entry point is [`Connection::fail`], which
    /// logs the line TS's `ClientHandler.fail` logs before closing.
    pub fn close_with_error(&self, error: ErrorBody) {
        self.close_with_error_thrown(error, None);
    }

    /// Port of TS `ClientHandler.fail(e)` (client-handler.ts:175-181):
    ///
    /// ```ts
    /// fail(e: unknown) {
    ///   this.#lc[getLogLevel(e)]?.(
    ///     `view-syncer closing connection with error: ${String(e)}`, e);
    ///   this.#downstream.fail(wrapWithProtocolError(e));
    /// }
    /// ```
    ///
    /// STRUCTURAL NOTE (HARD RULES 3 + 5). TS fails one client through its
    /// `ClientHandler`, and the wrapped error travels that client's subscription
    /// down to `Connection`, which sends the frame and closes. Rust's view-syncer
    /// holds the `Connection` directly on the failure path, because rust-cvr's
    /// `WebSocketSink::fail` takes a message `String` and cannot carry the error
    /// KIND — a `Rehome` / `ClientNotFound` / `Unauthorized` routed through it
    /// would reach the client as `Internal` and change what the client DOES. So
    /// the ported `fail` lives with the type that owns the `ErrorBody`, and TS's
    /// single `e: unknown` becomes the (`error`, `thrown`) pair this file already
    /// uses for `sendError` / `#closeWithError`: `error` is
    /// `wrapWithProtocolError(e)`, `thrown` is `e` itself — the value
    /// `getLogLevel` reads. `thrown: None` is the common case where `e` ALREADY
    /// is the ProtocolError carrying `error`: `wrapWithProtocolError` returns it
    /// unchanged and `getLogLevel` yields `warn`.
    ///
    /// The two levels this emits differ ON PURPOSE, matching TS. This line reads
    /// the RAW `e`; the `Sending error on WebSocket` line below it reads the
    /// WRAPPED ProtocolError, because `#closeWithThrown` receives the wrapped
    /// value (connection.ts:319) and so takes `isProtocolError` → `warn`. That is
    /// why a hydrate throw logs `error` here and `warn` there.
    ///
    /// Why it exists: rust emitted ONE operator-visible line per client failure
    /// where TS emits two — TS logs in `#runInLockForClient`'s catch
    /// (view-syncer.ts:1243) and again here. Measured 2026-09-08 on the G44
    /// runtime log differential: rust 47 `closing connection with error` against
    /// TS 92, plus 22 TS-only INFO lines from the shutdown-race `Rehome`.
    pub fn fail(&self, error: ErrorBody, thrown: Option<Thrown<'_>>) {
        // TS `getLogLevel(e)` over the RAW caught value; a bare ProtocolError
        // (`thrown: None`) is the `isProtocolError` → `warn` branch.
        let level = thrown.map_or(LogLevel::Warn, Thrown::get_log_level);
        // TS `String(e)`: the caught value's own text when there is one, else the
        // ProtocolError body's message.
        let message = thrown
            .and_then(Thrown::message)
            .map(str::to_string)
            .unwrap_or_else(|| error.message().to_string());
        match level {
            LogLevel::Warn => tracing::warn!(
                client_id = %self.client_id,
                ws_id = %self.ws_id,
                "view-syncer closing connection with error: {message}"
            ),
            LogLevel::Error => tracing::error!(
                client_id = %self.client_id,
                ws_id = %self.ws_id,
                "view-syncer closing connection with error: {message}"
            ),
            LogLevel::Info => tracing::info!(
                client_id = %self.client_id,
                ws_id = %self.ws_id,
                "view-syncer closing connection with error: {message}"
            ),
        }
        // TS `#downstream.fail(wrapWithProtocolError(e))`. `wrapWithProtocolError`
        // wraps a RAW value into a new `ProtocolError` (→ `sendError` classifies
        // it at `warn`), but returns a value that already IS a ProtocolError
        // UNCHANGED (types/error-with-level.ts:33-35) — so a
        // `ProtocolErrorWithLevel` reaches `sendError` still carrying its own
        // level and takes the `instanceof ProtocolErrorWithLevel` branch. Until
        // 2026-09-08 this re-wrapped everything as a plain `Protocol`, which
        // logged an OwnershipError's frame at WARN where TS logs INFO
        // (`ownership_transfer_fails_clients_at_info_like_ts_ownership_error`).
        let wrapped = match thrown {
            Some(Thrown::WithLevel(level)) => Thrown::WithLevel(level),
            _ => Thrown::Protocol(&message),
        };
        self.close_with_error_thrown(error, Some(wrapped));
    }

    /// Port of TS `#closeWithError(errorBody, thrown?)` (connection.ts:331-337).
    pub fn close_with_error_thrown(&self, error: ErrorBody, thrown: Option<Thrown<'_>>) {
        self.send_error_with_thrown(error.clone(), thrown);
        self.close(&format!("{:?}: {}", error.kind(), error.message()));
    }

    /// Close the connection.
    ///
    /// Port of `Connection.close()`.
    pub fn close(&self, reason: &str) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }
        tracing::info!(
            client_id = %self.client_id,
            client_group_id = %self.client_group_id,
            ws_id = %self.ws_id,
            "closing connection: {reason}"
        );
        (self.on_close)();
        self.sink.close(reason.to_string());
    }

    /// Send a downstream message.
    ///
    /// Port of `Connection.send()`.
    pub fn send(&self, msg: serde_json::Value) {
        *self.last_downstream_msg_time.lock().unwrap() = Instant::now();
        self.sink.push(msg);
    }

    /// Send an error message to the client.
    ///
    /// Port of `sendError()` — classifies log level and sends `["error", body]`.
    pub fn send_error(&self, error: ErrorBody) {
        self.send_error_with_thrown(error, None);
    }

    /// Port of TS `sendError(errorBody, thrown)` (connection.ts:356/396) — the
    /// full form. `send_error` is the `thrown === undefined` call.
    pub fn send_error_with_thrown(&self, error: ErrorBody, thrown: Option<Thrown<'_>>) {
        let log_level = classify_error_log_level(&error, thrown);
        let frame = error_message(&error);
        // TS `lc[logLevel]?.('Sending error on WebSocket', errorBody, thrown ?? '')`
        // (workers/connection.ts:429): the MESSAGE is exactly
        // `Sending error on WebSocket` and the body rides as a separate argument,
        // which the JSON formatter renders as the wire body. Interpolating rust's
        // `{:?}` printed the Rust enum instead
        // (`Basic(BasicErrorBody { kind: Internal, .. })`), so the same line read
        // as a different one on each arm and no operator query matched both
        // (G44 runtime log differential, 2026-09-08).
        let error_body = frame
            .get(1)
            .map(ToString::to_string)
            .unwrap_or_else(|| "null".to_string());
        match log_level {
            LogLevel::Warn => {
                tracing::warn!(
                    client_id = %self.client_id,
                    error_kind = ?error.kind(),
                    error_body = %error_body,
                    "Sending error on WebSocket"
                );
            }
            LogLevel::Error => {
                tracing::error!(
                    client_id = %self.client_id,
                    error_kind = ?error.kind(),
                    error_body = %error_body,
                    "Sending error on WebSocket"
                );
            }
            LogLevel::Info => {
                tracing::info!(
                    client_id = %self.client_id,
                    error_kind = ?error.kind(),
                    error_body = %error_body,
                    "Sending error on WebSocket"
                );
            }
        }
        self.send(frame);
    }

    /// Handle an initConnection message that was piggybacked in the
    /// sec-websocket-protocol header.
    ///
    /// Port of `Connection.handleInitConnection()`.
    pub async fn handle_init_connection(&self, init_msg_json: &str) -> bool {
        self.handle_inbound(init_msg_json).await
    }

    /// Whether the connection is closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// Get the client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Get the WS ID.
    pub fn ws_id(&self) -> &str {
        &self.ws_id
    }

    /// The negotiated protocol version. Rust-only accessor (rule 5): TS reads
    /// `connCtx.protocolVersion` at `initConnection` for the `#activeClients`
    /// gauge (view-syncer.ts:888-890); rust reads it back off the socket
    /// accepted earlier.
    pub fn protocol_version(&self) -> u32 {
        self.protocol_version
    }

    /// The downstream sink. Rust-only accessor (rule 5): the CG service builds
    /// the poke-target `ClientHandler` when the initConnection message arrives
    /// (TS view-syncer.ts:903-910, `downstream`), from the socket accepted
    /// earlier.
    pub fn sink(&self) -> &DirectWebSocketSink {
        &self.sink
    }
}

// ─── Error log level classification ────────────────────────────────────────
//
// Port of `sendError()` log level logic in `connection.ts`.

/// Log level for error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}

/// System error codes that indicate transient socket conditions. Port of TS
/// `TRANSIENT_SOCKET_ERROR_CODES` (`connection.ts`); lowercased for matching.
const TRANSIENT_SOCKET_ERROR_CODES: [&str; 6] = [
    "epipe",
    "econnreset",
    "ecanceled",
    // `std::io::ErrorKind` Display forms — rust's stand-in for reading the
    // thrown error's `.code`, which is what TS inspects (F-CON-3). They live
    // HERE, on the thrown-side check, not in the message-pattern list: TS runs
    // `hasTransientSocketCode` against the THROWN and only
    // `isTransientSocketMessage` against the error body.
    "connection reset",
    "broken pipe",
    "operation canceled",
];

/// Error-message fragments that indicate transient socket conditions without a
/// standard code. 1:1 with TS `TRANSIENT_SOCKET_MESSAGE_PATTERNS`
/// (connection.ts:462-464) — a single pattern; the errno spellings live on
/// `TRANSIENT_SOCKET_ERROR_CODES`, which is checked against the THROWN.
const TRANSIENT_SOCKET_MESSAGE_PATTERNS: [&str; 1] =
    ["socket was closed while data was being compressed"];

/// Port of TS `hasTransientSocketCode`. TS reads the thrown error's `.code`;
/// Rust has no thrown object at this boundary, so we scan the (lowercased) error
/// message for the errno spelling — the only channel available (F-CON-3).
fn has_transient_socket_code(msg_lower: &str) -> bool {
    TRANSIENT_SOCKET_ERROR_CODES
        .iter()
        .any(|c| msg_lower.contains(c))
}

/// Port of TS `isTransientSocketMessage`.
fn is_transient_socket_message(msg_lower: &str) -> bool {
    TRANSIENT_SOCKET_MESSAGE_PATTERNS
        .iter()
        .any(|p| msg_lower.contains(p))
}

/// Port of TS `hasErrno` (connection.ts:443-450): whether the THROWN value
/// carries an `errno` property. TS inspects the object; rust has only the
/// caught error's message at this boundary, so it scans for the errno spelling
/// — the same channel `has_transient_socket_code` uses (F-CON-3).
fn has_errno(msg_lower: &str) -> bool {
    msg_lower.contains("errno")
        || msg_lower.contains("os error ")
        || TRANSIENT_SOCKET_ERROR_CODES
            .iter()
            .any(|c| msg_lower.contains(c))
}

/// Port of TS `sendError`'s `thrown?: unknown` parameter (connection.ts:396).
/// TS branches on what the CAUGHT value IS; rust models exactly the three
/// properties the classification reads, so no call site has to carry a JS value.
///
/// `None` at a call site means TS's `thrown === undefined` — a body the server
/// SYNTHESIZED rather than caught, which TS logs at `info`.
#[derive(Clone, Copy, Debug)]
pub enum Thrown<'a> {
    /// TS `thrown instanceof ProtocolErrorWithLevel` — the explicit level wins
    /// over every other branch.
    WithLevel(LogLevel),
    /// TS `isProtocolError(thrown)` — `getLogLevel` yields `warn`.
    Protocol(&'a str),
    /// Any other caught value (a plain `Error`) — `getLogLevel` yields `error`.
    Other(&'a str),
}

impl<'a> Thrown<'a> {
    /// The caught value's message, for the `hasErrno` / `hasTransientSocketCode`
    /// checks TS runs against the THROWN (not against the error body).
    fn message(self) -> Option<&'a str> {
        match self {
            Thrown::WithLevel(_) => None,
            Thrown::Protocol(m) | Thrown::Other(m) => Some(m),
        }
    }

    /// Port of TS `getLogLevel(error)` (types/error-with-level.ts:24-30).
    fn get_log_level(self) -> LogLevel {
        match self {
            Thrown::WithLevel(level) => level,
            Thrown::Protocol(_) => LogLevel::Warn,
            Thrown::Other(_) => LogLevel::Error,
        }
    }
}

/// Classify the log level for an error body.
///
/// Port of the `sendError()` logic:
/// - `ClientNotFound` → warn
/// - `TransformFailed` → warn
/// - transient socket condition (EPIPE/ECONNRESET/ECANCELED, etc.) → warn
/// - Otherwise → info (or error for `Internal`)
pub fn classify_error_log_level(error: &ErrorBody, thrown: Option<Thrown<'_>>) -> LogLevel {
    // TS `sendError` (connection.ts:400-427), branch for branch and IN ORDER:
    //
    //   if (thrown instanceof ProtocolErrorWithLevel)          -> thrown.logLevel
    //   else if (hasErrno(thrown) || hasTransientSocketCode(thrown)
    //            || isTransientSocketMessage(errorBody.message)) -> 'warn'
    //   else if (kind is ClientNotFound | TransformFailed)      -> 'warn'
    //   else                                                    -> thrown ? getLogLevel(thrown) : 'info'
    //
    // Two things rust got wrong before the `thrown` parameter existed:
    //   * it ran the errno / socket-code checks against the ERROR BODY, where TS
    //     runs them against the THROWN and only `isTransientSocketMessage`
    //     against the body; and
    //   * its fallback mapped `Internal` to `error`, where TS's fallback is
    //     `info` whenever nothing was caught. That made rust page an operator on
    //     bodies the server synthesized itself — 47 rust `error` lines against
    //     TS's `info`/`warn` for the same replay (G44 runtime log differential,
    //     2026-09-08).
    if let Some(Thrown::WithLevel(level)) = thrown {
        return level;
    }
    let thrown_lower = thrown.and_then(|t| t.message()).map(str::to_lowercase);
    if thrown_lower
        .as_deref()
        .is_some_and(|m| has_errno(m) || has_transient_socket_code(m))
        || is_transient_socket_message(&error.message().to_lowercase())
    {
        return LogLevel::Warn;
    }
    match error.kind() {
        ErrorKind::ClientNotFound | ErrorKind::TransformFailed => LogLevel::Warn,
        // TS `thrown ? getLogLevel(thrown) : 'info'`.
        _ => thrown.map_or(LogLevel::Info, |t| t.get_log_level()),
    }
}

// ─── Free functions (ported from connection.ts) ────────────────────────────

/// Send a message on a WebSocket-like sink.
///
/// Port of the exported `send()` function.
/// If the WS is not open, the message is dropped (with a debug log).
pub fn send(sink: &DirectWebSocketSink, data: serde_json::Value) {
    sink.push(data);
}

/// Send an error message on a WebSocket.
///
/// Port of the exported `sendError()` function.
pub fn send_error(sink: &DirectWebSocketSink, error: ErrorBody) {
    // No caught error at this boundary — TS's `thrown === undefined` call.
    let log_level = classify_error_log_level(&error, None);
    match log_level {
        LogLevel::Warn => tracing::warn!("Sending error: {:?}", error),
        LogLevel::Error => tracing::error!("Sending error: {:?}", error),
        LogLevel::Info => tracing::info!("Sending error: {:?}", error),
    }
    sink.push(error_message(&error));
}

// ─── Tests ─────────────────────────────────────────────────────────────────
//
// Port of the log-level classification cases from TS `connection.test.ts`
// (`sendError` log level: ClientNotFound/TransformFailed → warn, compressed-
// socket-closed → warn, internal → error, protocol errors → info).

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::BasicErrorBody;

    fn basic(kind: ErrorKind, message: &str) -> ErrorBody {
        ErrorBody::Basic(BasicErrorBody {
            kind,
            message: message.to_string(),
            origin: None,
        })
    }

    #[test]
    fn client_not_found_and_transform_failed_are_warn() {
        assert_eq!(
            classify_error_log_level(&ErrorBody::client_not_found("gone"), None),
            LogLevel::Warn
        );
        assert_eq!(
            classify_error_log_level(&basic(ErrorKind::TransformFailed, "bad transform"), None),
            LogLevel::Warn
        );
    }

    /// The client-failure path wraps before it closes: TS
    /// `ClientHandler.fail(e)` -> `#downstream.fail(wrapWithProtocolError(e))`
    /// (client-handler.ts:175-181), and the pipeline hands THAT wrapped
    /// ProtocolError to `#closeWithThrown` (connection.ts:319). So
    /// `getLogLevel(thrown)` takes the `isProtocolError` branch -> `warn`.
    ///
    /// NON-VACUOUS: classify this as `Thrown::Other` (a plain error) and it
    /// reports `error` — which is exactly what rust shipped, logging 47 Internal
    /// bodies at ERROR where TS logged WARN for the identical events.
    #[test]
    fn a_wrapped_client_failure_is_a_protocol_error_and_warns_like_ts() {
        let body = ErrorBody::internal("probe SQL contains NUL byte");
        assert_eq!(
            classify_error_log_level(&body, Some(Thrown::Protocol("probe SQL contains NUL byte"))),
            LogLevel::Warn,
            "a wrapped ProtocolError is TS `getLogLevel` = 'warn', not 'error'"
        );
    }

    #[test]
    fn internal_errors_take_their_level_from_the_thrown_value_like_ts() {
        // TS `sendError`'s fallback is `thrown ? getLogLevel(thrown) : 'info'`
        // (connection.ts:426). A SYNTHESIZED Internal body — nothing caught — is
        // therefore `info`, NOT `error`: rust's old kind-based mapping paged an
        // operator on bodies the server built itself (47 such lines in one
        // replay where TS logged info/warn).
        assert_eq!(
            classify_error_log_level(&ErrorBody::internal("boom"), None),
            LogLevel::Info,
            "a synthesized Internal body is TS `: 'info'`"
        );
        // A CAUGHT plain error keeps `error` (TS `getLogLevel` default).
        assert_eq!(
            classify_error_log_level(&ErrorBody::internal("boom"), Some(Thrown::Other("boom"))),
            LogLevel::Error
        );
    }

    #[test]
    fn protocol_errors_default_to_info() {
        assert_eq!(
            classify_error_log_level(&ErrorBody::invalid_message("nope"), None),
            LogLevel::Info
        );
        assert_eq!(
            classify_error_log_level(&ErrorBody::version_not_supported("old"), None),
            LogLevel::Info
        );
    }

    #[test]
    fn compressed_socket_close_is_downgraded_to_warn() {
        // A transient "socket was closed while data was being compressed" is a
        // benign disconnect, not an internal error — downgraded to warn even
        // though its kind would otherwise be Internal.
        let err = basic(
            ErrorKind::Internal,
            "The socket was closed while data was being compressed",
        );
        assert_eq!(classify_error_log_level(&err, None), LogLevel::Warn);
    }

    #[test]
    fn transient_socket_errors_are_downgraded_to_warn() {
        // TS downgrades EPIPE/ECONNRESET/ECANCELED (transient peer disconnects)
        // to warn by reading the THROWN error's `.code`
        // (`hasTransientSocketCode`), never the error body — the body is only
        // matched against the single compression pattern. Rust has no thrown
        // object, so it scans the caught error's MESSAGE, which carries the
        // io::Error Display (F-CON-3).
        //
        // (The real socket-failure path does not reach this classifier at all:
        // `WsCommand::Fail` is logged by `ws_server` at warn, mirroring TS
        // `closeWithError` in types/ws.ts.)
        for msg in [
            "write failed: Broken pipe (os error 32)",
            "Connection reset by peer (os error 54)",
            "io error: ECONNRESET",
            "operation canceled",
        ] {
            let err = basic(ErrorKind::Internal, msg);
            assert_eq!(
                classify_error_log_level(&err, Some(Thrown::Other(msg))),
                LogLevel::Warn,
                "a CAUGHT transient socket error should warn: {msg:?}"
            );
            // Nothing caught: TS has no `.code` to read and the body matches no
            // message pattern, so the fallback is `info`.
            assert_eq!(
                classify_error_log_level(&err, None),
                LogLevel::Info,
                "a synthesized body is TS `: 'info'`: {msg:?}"
            );
        }
        // A genuine CAUGHT internal error still logs at error.
        assert_eq!(
            classify_error_log_level(
                &basic(ErrorKind::Internal, "assertion failed: x == y"),
                Some(Thrown::Other("assertion failed: x == y"))
            ),
            LogLevel::Error
        );
    }

    // ─── Connection::handle_inbound / init — port of TS `Connection`
    // (`#handleMessage`, `#handleMessageResult`, `init`, connection.ts) ──────

    use crate::ws_sink::WsCommand;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    /// Drive an async connection call on a current-thread runtime (the CG task
    /// twin for these unit tests).
    fn block_on_local<F: std::future::Future>(fut: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    }

    /// MessageHandler mock: records every dispatched message and returns a
    /// configured list of `HandlerResult`s.
    struct MockHandler {
        results: Mutex<Vec<HandlerResult>>,
        calls: std::sync::Arc<Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait(?Send)]
    impl MessageHandler for MockHandler {
        async fn handle_message(&self, msg: &str) -> Vec<HandlerResult> {
            self.calls.lock().unwrap().push(msg.to_string());
            std::mem::take(&mut *self.results.lock().unwrap())
        }
    }

    #[allow(clippy::type_complexity)]
    fn test_connection(
        protocol_version: u32,
        results: Vec<HandlerResult>,
    ) -> (
        Connection,
        tokio::sync::mpsc::UnboundedReceiver<WsCommand>,
        std::sync::Arc<Mutex<Vec<String>>>,
        std::sync::Arc<AtomicUsize>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<WsCommand>();
        let calls = std::sync::Arc::new(Mutex::new(Vec::new()));
        let closes = std::sync::Arc::new(AtomicUsize::new(0));
        let closes_in = closes.clone();
        let conn = Connection::new(
            DirectWebSocketSink::new(tx),
            protocol_version,
            "ws1".to_string(),
            "c1".to_string(),
            "cg1".to_string(),
            "zero".to_string(),
            0,
            Box::new(MockHandler {
                results: Mutex::new(results),
                calls: calls.clone(),
            }),
            Box::new(move || {
                closes_in.fetch_add(1, Ordering::SeqCst);
            }),
        );
        (conn, rx, calls, closes)
    }

    fn drain(rx: &mut tokio::sync::mpsc::UnboundedReceiver<WsCommand>) -> Vec<WsCommand> {
        std::iter::from_fn(|| rx.try_recv().ok()).collect()
    }

    /// Port of TS `Connection.init()` (connection.ts): an in-range protocol
    /// version sends the `connected` message `{wsid, timestamp, appID,
    /// shardNum}` and returns true.
    #[test]
    fn init_in_range_sends_connected_with_ws_id_app_id_and_shard() {
        let (conn, mut rx, _calls, _closes) = test_connection(PROTOCOL_VERSION, Vec::new());
        assert!(conn.init());
        let cmds = drain(&mut rx);
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            WsCommand::Send { msg, .. } => {
                assert_eq!(msg[0], "connected");
                assert_eq!(msg[1]["wsid"], "ws1");
                assert_eq!(msg[1]["appID"], "zero");
                assert_eq!(msg[1]["shardNum"], 0);
                assert!(msg[1]["timestamp"].is_i64());
            }
            _ => panic!("expected the connected Send frame"),
        }
    }

    /// Port of TS `Connection.init()` version gate: below the minimum → the
    /// exact "client must be updated" VersionNotSupported message; above the
    /// server's version → the "server must be updated" variant. Both close.
    #[test]
    fn init_out_of_range_closes_with_exact_version_not_supported_message() {
        for (version, who) in [
            (MIN_SERVER_SUPPORTED_SYNC_PROTOCOL - 1, "client"),
            (PROTOCOL_VERSION + 1, "server"),
        ] {
            let (conn, mut rx, _calls, closes) = test_connection(version, Vec::new());
            assert!(!conn.init());
            assert!(conn.is_closed());
            assert_eq!(closes.load(Ordering::SeqCst), 1, "on_close must fire");
            let cmds = drain(&mut rx);
            let error = cmds
                .iter()
                .find_map(|c| match c {
                    WsCommand::Send { msg, .. } if msg[0] == "error" => Some(msg[1].clone()),
                    _ => None,
                })
                .expect("expected an error frame before the close");
            assert_eq!(error["kind"], "VersionNotSupported");
            assert_eq!(
                error["message"],
                format!(
                    "server is at sync protocol v{PROTOCOL_VERSION} and does not support v{version}. The {who} must be updated to a newer release."
                )
            );
            assert!(
                cmds.iter().any(|c| matches!(c, WsCommand::Close(_))),
                "the connection must close after the error"
            );
        }
    }

    /// Port of TS `#handleMessage`'s parse catch (connection.ts): unparseable
    /// JSON closes the connection with an InvalidMessage error — sent BEFORE
    /// the close — and the handler is never consulted.
    #[test]
    fn handle_inbound_invalid_json_closes_with_invalid_message() {
        let (conn, mut rx, calls, closes) = test_connection(PROTOCOL_VERSION, Vec::new());
        assert!(!block_on_local(conn.handle_inbound("{not json")));
        assert!(conn.is_closed());
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert!(calls.lock().unwrap().is_empty(), "handler must not run");
        let cmds = drain(&mut rx);
        match &cmds[0] {
            WsCommand::Send { msg, .. } => {
                assert_eq!(msg[0], "error");
                assert_eq!(msg[1]["kind"], "InvalidMessage");
            }
            _ => panic!("expected the error frame first"),
        }
        assert!(matches!(cmds[1], WsCommand::Close(_)));
    }

    /// Port of TS `#handleMessage`'s ping fast-path: `["ping",{}]` answers
    /// `["pong",{}]` directly, WITHOUT dispatching to the message handler.
    #[test]
    fn handle_inbound_ping_answers_pong_without_handler() {
        let (conn, mut rx, calls, _closes) = test_connection(
            PROTOCOL_VERSION,
            vec![HandlerResult::Fatal {
                error: ErrorBody::internal("must never be reached"),
            }],
        );
        assert!(block_on_local(conn.handle_inbound(r#"["ping",{}]"#)));
        assert!(!conn.is_closed());
        assert!(
            calls.lock().unwrap().is_empty(),
            "ping must bypass the handler"
        );
        let cmds = drain(&mut rx);
        assert_eq!(cmds.len(), 1);
        match &cmds[0] {
            WsCommand::Send { msg, .. } => {
                assert_eq!(msg, &serde_json::json!(["pong", {}]));
            }
            _ => panic!("expected the pong frame"),
        }
    }

    /// Port of TS `#handleMessageResult` 'fatal': the error is sent and the
    /// connection closes (handle_inbound returns false).
    #[test]
    fn handle_inbound_fatal_result_sends_error_and_closes() {
        let (conn, mut rx, calls, closes) = test_connection(
            PROTOCOL_VERSION,
            vec![HandlerResult::Fatal {
                error: ErrorBody::unauthorized("bad token"),
            }],
        );
        assert!(!block_on_local(
            conn.handle_inbound(r#"["updateAuth",{"auth":"t"}]"#)
        ));
        assert!(conn.is_closed());
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert_eq!(calls.lock().unwrap().len(), 1, "handler dispatched once");
        let cmds = drain(&mut rx);
        match &cmds[0] {
            WsCommand::Send { msg, .. } => {
                assert_eq!(msg[0], "error");
                assert_eq!(msg[1]["kind"], "Unauthorized");
                assert_eq!(msg[1]["message"], "bad token");
            }
            _ => panic!("expected the error frame"),
        }
        assert!(matches!(cmds[1], WsCommand::Close(_)));
    }

    /// Port of TS `#handleMessageResult` 'transient': every error is sent to
    /// the client but the connection STAYS OPEN — the branch difference vs
    /// 'fatal' that G36 pins.
    #[test]
    fn handle_inbound_transient_errors_keep_connection_open() {
        let (conn, mut rx, _calls, closes) = test_connection(
            PROTOCOL_VERSION,
            vec![HandlerResult::Transient {
                errors: vec![
                    ErrorBody::basic(ErrorKind::MutationFailed, "m1 failed".to_string()),
                    ErrorBody::basic(ErrorKind::MutationRateLimited, "slow down".to_string()),
                ],
            }],
        );
        assert!(block_on_local(
            conn.handle_inbound(r#"["updateAuth",{"auth":"t"}]"#)
        ));
        assert!(!conn.is_closed());
        assert_eq!(closes.load(Ordering::SeqCst), 0, "no close for transient");
        let kinds: Vec<String> = drain(&mut rx)
            .iter()
            .filter_map(|c| match c {
                WsCommand::Send { msg, .. } if msg[0] == "error" => {
                    Some(msg[1]["kind"].as_str().unwrap().to_string())
                }
                _ => None,
            })
            .collect();
        assert_eq!(kinds, vec!["MutationFailed", "MutationRateLimited"]);
    }

    /// Port of TS `#handleMessage`'s closed guard: frames arriving after close
    /// are ignored (no pong, no dispatch), and `close` is idempotent — the
    /// on_close callback fires exactly once.
    #[test]
    fn messages_after_close_are_ignored_and_close_is_idempotent() {
        let (conn, mut rx, calls, closes) = test_connection(PROTOCOL_VERSION, Vec::new());
        conn.close("test close");
        assert!(!block_on_local(conn.handle_inbound(r#"["ping",{}]"#)));
        conn.close("second close");
        assert_eq!(closes.load(Ordering::SeqCst), 1, "on_close fires once");
        assert!(calls.lock().unwrap().is_empty());
        let sends = drain(&mut rx)
            .iter()
            .filter(|c| matches!(c, WsCommand::Send { .. }))
            .count();
        assert_eq!(sends, 0, "no frame may be sent after close");
    }
}
