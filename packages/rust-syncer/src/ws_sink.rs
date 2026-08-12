//! DirectWebSocketSink — replaces `NapiWebSocketSink` + TSFN.
//!
//! The CG thread writes poke frames to a bounded channel; a tokio task
//! drains the channel and writes to the WebSocket. Backpressure is natural
//! (bounded channel — the CG thread blocks when the channel is full, exactly
//! like the TS `ws.send()` backpressure model).

use crate::protocol::{BasicErrorBody, ErrorBody, ErrorKind};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc;

/// Messages sent from the CG thread to the WS writer task.
pub enum WsCommand {
    /// Send a JSON text message.
    Send(Value),
    /// Send an error message and close with code 3000.
    Fail(ErrorBody),
    /// Close the WebSocket (graceful).
    Close(String),
}

/// The sink that the CG thread uses to push downstream messages.
///
/// `push()` is **synchronous** — it blocks the CG thread if the channel
/// is full. This matches the TS `ws.send()` backpressure behavior where
/// a slow client slows down the server.
#[derive(Clone)]
pub struct DirectWebSocketSink {
    tx: mpsc::Sender<WsCommand>,
}

impl DirectWebSocketSink {
    pub fn new(tx: mpsc::Sender<WsCommand>) -> Self {
        Self { tx }
    }

    /// Push a downstream message. Blocks if the channel is full (backpressure).
    pub fn push(&self, msg: Value) {
        let _ = self.tx.blocking_send(WsCommand::Send(msg));
    }

    /// Push a downstream message, serialized from any Serialize type.
    pub fn push_serializable(&self, msg: &impl Serialize) {
        let value = serde_json::to_value(msg).unwrap_or_else(|_| {
            serde_json::json!(["error", {"kind": "Internal", "message": "serialization failed"}])
        });
        self.push(value);
    }

    /// Send an error message and close the connection with code 3000.
    pub fn fail(&self, error: ErrorBody) {
        let _ = self.tx.blocking_send(WsCommand::Fail(error));
    }

    /// Close the connection gracefully.
    pub fn close(&self, reason: String) {
        let _ = self.tx.blocking_send(WsCommand::Close(reason));
    }
}

/// Adapt `DirectWebSocketSink` to `rust-cvr`'s `WebSocketSink` trait so
/// `ClientHandler` / `PokeHandler` can push poke frames straight to the WS
/// writer task. Replaces `NapiWebSocketSink` (the one napi-specific piece of
/// the CVR hot path) with no TSFN — the bounded channel is the backpressure.
impl rust_cvr::client_handler::WebSocketSink for DirectWebSocketSink {
    fn push(&self, msg: Value) -> Result<(), String> {
        self.tx
            .blocking_send(WsCommand::Send(msg))
            .map_err(|e| format!("ws sink closed: {e}"))
    }

    fn fail(&self, e: String) {
        // rust-cvr passes a plain message; the accompanying `["error", ..]`
        // frame is delivered separately via `push`. Close with code 3000.
        let _ = self
            .tx
            .blocking_send(WsCommand::Fail(ErrorBody::Basic(BasicErrorBody {
                kind: ErrorKind::Internal,
                message: e,
                origin: None,
            })));
    }

    fn cancel(&self) {
        // Poke-chain cancel: the `pokeEnd {cancel:true}` frame is already sent
        // via `push` by `PokeHandler::cancel`, so nothing to send here.
    }
}
