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
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::{mpsc, watch};

/// Messages sent from the CG thread to the WS writer task.
pub enum WsCommand {
    /// Send a JSON text message.
    Send(Value),
    /// Send an error message and close with code 3000.
    Fail(ErrorBody),
    /// Close the WebSocket (graceful).
    Close(String),
}

/// Shed policy for the unbounded downstream channel: the channel itself must be
/// unbounded (ordering — see module docs), so the memory bound is enforced by
/// DISCONNECTING a client whose queue depth crosses the high-water mark. The
/// client's reconnect + rehydrate protocol makes this safe; unbounded growth
/// against a stalled TCP window is not.
pub struct SinkLimits {
    /// Commands queued but not yet drained by the writer task (sink increments,
    /// writer decrements).
    pub depth: Arc<AtomicI64>,
    /// Queue-depth high-water mark; crossing it trips `kill`.
    pub hwm: i64,
    /// Fired once when the HWM is crossed. The WRITER selects on this and
    /// closes the socket immediately — sending a `Fail` through the queue would
    /// deliver it only after the very backlog that tripped the limit.
    pub kill: watch::Sender<bool>,
}

/// The sink that the CG thread uses to push downstream messages.
///
/// `push()` is **synchronous** and non-blocking: it enqueues onto an unbounded
/// channel, so frames are delivered to the writer task in exact send order and
/// the call can never panic on a full channel (see module docs on ordering).
#[derive(Clone)]
pub struct DirectWebSocketSink {
    tx: mpsc::UnboundedSender<WsCommand>,
    limits: Option<Arc<SinkLimits>>,
}

impl DirectWebSocketSink {
    pub fn new(tx: mpsc::UnboundedSender<WsCommand>) -> Self {
        Self { tx, limits: None }
    }

    /// A sink with a slow-client shed policy (production WS path). `new` (no
    /// limits) is retained for in-process sinks and tests, where the consumer
    /// is not a client socket.
    pub fn with_limits(tx: mpsc::UnboundedSender<WsCommand>, limits: Arc<SinkLimits>) -> Self {
        Self {
            tx,
            limits: Some(limits),
        }
    }

    /// Push a downstream message. Never blocks (unbounded, ordered channel);
    /// a sink with limits sheds the connection past the high-water mark.
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
    /// blocks and never panics; failures are a dropped receiver (the WS writer
    /// task has exited) or a tripped slow-client high-water mark.
    fn send_command(&self, command: WsCommand) -> Result<(), String> {
        if let Some(limits) = &self.limits {
            let depth = limits.depth.fetch_add(1, Ordering::SeqCst) + 1;
            if depth > limits.hwm {
                // Signal the writer to close NOW (it selects on `kill`), and
                // reject the command — the queue is already `hwm` deep.
                limits.depth.fetch_sub(1, Ordering::SeqCst);
                let _ = limits.kill.send(true);
                return Err(format!(
                    "ws downstream queue overflow (depth > {}): slow client shed",
                    limits.hwm
                ));
            }
            if self.tx.send(command).is_err() {
                limits.depth.fetch_sub(1, Ordering::SeqCst);
                return Err("ws sink closed".to_string());
            }
            crate::metrics::record_ws_queued_delta(1);
            return Ok(());
        }
        self.tx
            .send(command)
            .map_err(|_| "ws sink closed".to_string())
    }
}

/// Adapt `DirectWebSocketSink` to `rust-cvr`'s `WebSocketSink` trait so
/// `ClientHandler` / `PokeHandler` can push poke frames straight to the WS
/// writer task. Replaces `NapiWebSocketSink` (the one napi-specific piece of
/// the CVR hot path) with no TSFN — ordering comes from the unbounded channel,
/// memory bounds from the slow-client shed policy (see module docs).
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

    /// Crossing the downstream high-water mark rejects the frame and trips the
    /// kill signal (the writer closes the socket immediately); frames under the
    /// mark are unaffected.
    #[tokio::test]
    async fn hwm_overflow_trips_kill_and_rejects() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (kill_tx, kill_rx) = watch::channel(false);
        let limits = Arc::new(SinkLimits {
            depth: Arc::new(AtomicI64::new(0)),
            hwm: 3,
            kill: kill_tx,
        });
        let sink = DirectWebSocketSink::with_limits(tx, limits.clone());

        for i in 0..3 {
            assert!(
                sink.send_command(WsCommand::Send(serde_json::json!([i])))
                    .is_ok(),
                "frame {i} under the HWM must be accepted"
            );
        }
        assert!(!*kill_rx.borrow(), "kill must not fire under the HWM");
        // 4th frame crosses hwm=3 → rejected + kill fired.
        assert!(
            sink.send_command(WsCommand::Send(serde_json::json!([3])))
                .is_err()
        );
        assert!(*kill_rx.borrow(), "kill fires on overflow");
        assert_eq!(
            limits.depth.load(Ordering::SeqCst),
            3,
            "rejected frame not counted"
        );
        // The queued (accepted) frames are still there, in order.
        for i in 0..3 {
            match rx.recv().await {
                Some(WsCommand::Send(v)) => assert_eq!(v[0], serde_json::json!(i)),
                _ => panic!("expected queued frame {i}"),
            }
        }
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
