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
    self, ErrorBody, ErrorKind, ErrorOrigin, MIN_SERVER_SUPPORTED_SYNC_PROTOCOL, PROTOCOL_VERSION,
    connected_message, error_message, pong_message,
};
use crate::ws_sink::DirectWebSocketSink;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Downstream message interval: slightly longer than client's 5s PING_INTERVAL.
const DOWNSTREAM_MSG_INTERVAL_MS: u64 = 6000;

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
/// In Phase 2, this is implemented by `SyncerWsMessageHandler`.
/// In the full implementation, it dispatches to ViewSyncer, Mutagen, Pusher.
pub trait MessageHandler: Send {
    /// Handle a parsed upstream message.
    /// Returns a list of `HandlerResult`s.
    fn handle_message(&self, msg: &str) -> Vec<HandlerResult>;
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
    /// Whether the connection has been closed.
    closed: AtomicBool,
    /// Time of last downstream message sent.
    last_downstream_msg_time: std::sync::Mutex<Instant>,
    /// The message handler for dispatching upstream messages.
    handler: Box<dyn MessageHandler>,
    /// Called when the connection is closed.
    on_close: Box<dyn Fn() + Send + Sync>,
}

impl Connection {
    /// Create a new connection.
    ///
    /// In the TS code, the constructor sets up event listeners and starts
    /// proxying inbound messages. In Rust, the WS reader task already forwards
    /// messages to a channel — the CG thread calls `handle_inbound()` for each.
    pub fn new(
        sink: DirectWebSocketSink,
        protocol_version: u32,
        ws_id: String,
        client_id: String,
        client_group_id: String,
        handler: Box<dyn MessageHandler>,
        on_close: Box<dyn Fn() + Send + Sync>,
    ) -> Self {
        Self {
            sink,
            protocol_version,
            ws_id,
            client_id,
            client_group_id,
            closed: AtomicBool::new(false),
            last_downstream_msg_time: std::sync::Mutex::new(Instant::now()),
            handler,
            on_close,
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
            self.send(connected_message(&self.ws_id));
            true
        }
    }

    /// Handle an inbound message (raw JSON text from the WebSocket).
    ///
    /// Port of `Connection.#handleMessage()`.
    /// Returns `true` if the connection is still open, `false` if closed.
    pub fn handle_inbound(&self, data: &str) -> bool {
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
        let results = self.handler.handle_message(data);
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

    /// Close the connection with an error (TS `Connection.#closeWithError` /
    /// `client.fail`): send the error downstream, then close.
    pub fn close_with_error(&self, error: ErrorBody) {
        self.send_error(error.clone());
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
        let log_level = classify_error_log_level(&error);
        match log_level {
            LogLevel::Warn => {
                tracing::warn!(
                    client_id = %self.client_id,
                    error_kind = ?error.kind(),
                    "Sending error on WebSocket: {:?}",
                    error
                );
            }
            LogLevel::Error => {
                tracing::error!(
                    client_id = %self.client_id,
                    error_kind = ?error.kind(),
                    "Sending error on WebSocket: {:?}",
                    error
                );
            }
            LogLevel::Info => {
                tracing::info!(
                    client_id = %self.client_id,
                    error_kind = ?error.kind(),
                    "Sending error on WebSocket: {:?}",
                    error
                );
            }
        }
        self.send(error_message(&error));
    }

    /// Check if a pong should be sent (keepalive).
    ///
    /// Port of `Connection.#maybeSendPong()`.
    /// Called on a timer every `DOWNSTREAM_MSG_INTERVAL_MS / 2` (3s).
    pub fn maybe_send_pong(&self) {
        let last = *self.last_downstream_msg_time.lock().unwrap();
        if last.elapsed().as_millis() as u64 > DOWNSTREAM_MSG_INTERVAL_MS {
            tracing::debug!(
                ws_id = %self.ws_id,
                "manually sending pong"
            );
            self.send(pong_message());
        }
    }

    /// Handle an initConnection message that was piggybacked in the
    /// sec-websocket-protocol header.
    ///
    /// Port of `Connection.handleInitConnection()`.
    pub fn handle_init_connection(&self, init_msg_json: &str) -> bool {
        self.handle_inbound(init_msg_json)
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

/// Classify the log level for an error body.
///
/// Port of the `sendError()` logic:
/// - `ClientNotFound` → warn
/// - `TransformFailed` → warn
/// - Otherwise → info (for thrown errors, falls back to `getLogLevel`)
pub fn classify_error_log_level(error: &ErrorBody) -> LogLevel {
    match error.kind() {
        ErrorKind::ClientNotFound | ErrorKind::TransformFailed => LogLevel::Warn,
        _ => {
            // Check for transient socket message patterns.
            let msg = error.message().to_lowercase();
            if msg.contains("socket was closed while data was being compressed") {
                return LogLevel::Warn;
            }
            // Default: info for protocol errors, error for internal errors.
            match error.kind() {
                ErrorKind::Internal => LogLevel::Error,
                _ => LogLevel::Info,
            }
        }
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
    let log_level = classify_error_log_level(&error);
    match log_level {
        LogLevel::Warn => tracing::warn!("Sending error: {:?}", error),
        LogLevel::Error => tracing::error!("Sending error: {:?}", error),
        LogLevel::Info => tracing::info!("Sending error: {:?}", error),
    }
    sink.push(error_message(&error));
}

/// Current time in milliseconds since Unix epoch.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
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
            classify_error_log_level(&ErrorBody::client_not_found("gone")),
            LogLevel::Warn
        );
        assert_eq!(
            classify_error_log_level(&basic(ErrorKind::TransformFailed, "bad transform")),
            LogLevel::Warn
        );
    }

    #[test]
    fn internal_errors_are_error_level() {
        assert_eq!(
            classify_error_log_level(&ErrorBody::internal("boom")),
            LogLevel::Error
        );
    }

    #[test]
    fn protocol_errors_default_to_info() {
        assert_eq!(
            classify_error_log_level(&ErrorBody::invalid_message("nope")),
            LogLevel::Info
        );
        assert_eq!(
            classify_error_log_level(&ErrorBody::version_not_supported("old")),
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
        assert_eq!(classify_error_log_level(&err), LogLevel::Warn);
    }
}
