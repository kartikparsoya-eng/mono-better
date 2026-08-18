//! DirectWebSocketSink — replaces `NapiWebSocketSink` + TSFN.
//!
//! The CG thread writes poke frames to a channel; a tokio task drains it and
//! writes to the WebSocket.
//!
//! ## Ordering
//!
//! Poke frame order is a hard protocol invariant (pokeStart → pokePart* →
//! pokeEnd). `push()` is synchronous and is invoked from inside the CG's
//! `current_thread` runtime, where `blocking_send` panics and there is no way to
//! `.await`. The channel is therefore **unbounded**: `UnboundedSender::send` is
//! non-blocking, never fails on capacity, and preserves send order.
//!
//! A bounded channel cannot be used here without breaking ordering: on a full
//! bounded channel the only non-panicking, non-blocking option is to defer the
//! overflow frame (which lets a later frame overtake it — a reorder bug) or to
//! drop the connection mid-hydration. Unbounded avoids both. Backpressure now
//! lives at the socket/writer layer rather than in this channel's depth; this is
//! no worse than the previous behavior, which spawned an unbounded number of
//! deferred-send tasks under sustained overload.

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
/// `push()` is **synchronous** and non-blocking: it enqueues onto an unbounded
/// channel, so frames are delivered to the writer task in exact send order and
/// the call can never panic on a full channel (see module docs on ordering).
#[derive(Clone)]
pub struct DirectWebSocketSink {
    tx: mpsc::UnboundedSender<WsCommand>,
}

impl DirectWebSocketSink {
    pub fn new(tx: mpsc::UnboundedSender<WsCommand>) -> Self {
        Self { tx }
    }

    /// Push a downstream message. Blocks if the channel is full (backpressure).
    pub fn push(&self, msg: Value) {
        let _ = self.send_command(WsCommand::Send(msg));
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
        let _ = self.send_command(WsCommand::Fail(error));
    }

    /// Close the connection gracefully.
    pub fn close(&self, reason: String) {
        let _ = self.send_command(WsCommand::Close(reason));
    }

    /// Enqueue a command onto the unbounded, order-preserving channel. Never
    /// blocks and never panics; the only failure is a dropped receiver (the WS
    /// writer task has exited), reported as "ws sink closed".
    fn send_command(&self, command: WsCommand) -> Result<(), String> {
        self.tx
            .send(command)
            .map_err(|_| "ws sink closed".to_string())
    }
}

/// Adapt `DirectWebSocketSink` to `rust-cvr`'s `WebSocketSink` trait so
/// `ClientHandler` / `PokeHandler` can push poke frames straight to the WS
/// writer task. Replaces `NapiWebSocketSink` (the one napi-specific piece of
/// the CVR hot path) with no TSFN — the bounded channel is the backpressure.
impl rust_cvr::client_handler::WebSocketSink for DirectWebSocketSink {
    fn push(&self, msg: Value) -> Result<(), String> {
        self.send_command(WsCommand::Send(msg))
    }

    fn fail(&self, e: String) {
        // rust-cvr passes a plain message; the accompanying `["error", ..]`
        // frame is delivered separately via `push`. Close with code 3000.
        let _ = self.send_command(WsCommand::Fail(ErrorBody::Basic(BasicErrorBody {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn runtime_error_send_never_uses_blocking_send() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = DirectWebSocketSink::new(tx);

        sink.push(serde_json::json!(["first"]));
        sink.fail(ErrorBody::basic(
            ErrorKind::Unauthorized,
            "rejected".to_string(),
        ));

        assert!(matches!(rx.recv().await, Some(WsCommand::Send(_))));
        assert!(matches!(rx.recv().await, Some(WsCommand::Fail(_))));
    }

    /// A burst larger than any previous bounded capacity must arrive in exact
    /// send order (no reorder under "backpressure") — the invariant the old
    /// try_send-then-spawn path violated.
    #[tokio::test]
    async fn burst_preserves_frame_order() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let sink = DirectWebSocketSink::new(tx);

        // Push far more than the old 256-slot bound, synchronously, before the
        // reader drains anything.
        const N: i64 = 1000;
        for i in 0..N {
            sink.push(serde_json::json!(["pokePart", i]));
        }
        sink.close("done".to_string());

        for i in 0..N {
            match rx.recv().await {
                Some(WsCommand::Send(v)) => assert_eq!(v[1], serde_json::json!(i)),
                _ => panic!("expected Send({i}) in order"),
            }
        }
        assert!(matches!(rx.recv().await, Some(WsCommand::Close(_))));
    }
}
