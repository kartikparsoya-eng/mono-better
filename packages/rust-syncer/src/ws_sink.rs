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
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use tokio::sync::{mpsc, watch};

/// Messages sent from the CG thread to the WS writer task.
pub enum WsCommand {
    /// Send a JSON text message. `est_bytes` is the approximate serialized size
    /// recorded at enqueue; the writer subtracts EXACTLY this value from the
    /// byte counter on dequeue (symmetric accounting → no drift).
    Send { msg: Value, est_bytes: usize },
    /// Send an error message and close with code 3000.
    Fail(ErrorBody),
    /// Close the WebSocket (graceful).
    Close(String),
    /// Close with an explicit RFC 6455 code (e.g. 1009 Message Too Big).
    /// Needed because the split READ half cannot write: the reader detects
    /// the condition and relays the close through the writer's queue.
    CloseWithCode { code: u16, reason: String },
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
    /// Estimated serialized bytes queued but not yet drained. A single command
    /// can be a multi-MB `Value` tree, so the frame-count HWM alone can't bound
    /// memory; this is the primary bound (sink adds the command's `est_bytes`,
    /// writer subtracts exactly it).
    pub bytes: Arc<AtomicI64>,
    /// Queued-bytes high-water mark; crossing it trips `kill`. `0` disables
    /// byte-based shedding (frame HWM still applies).
    pub byte_hwm: i64,
    /// Fired once when either HWM is crossed. The WRITER selects on this and
    /// closes the socket immediately — sending a `Fail` through the queue would
    /// deliver it only after the very backlog that tripped the limit.
    pub kill: watch::Sender<bool>,
    /// Guards the shed METRIC so it counts exactly once per connection: after
    /// the HWM trips, the CG keeps calling `send_command` (re-crossing the mark)
    /// until the writer drains/closes, so without this the shed counter would
    /// over-count. Set on the first crossing via compare_exchange.
    pub shed_counted: AtomicBool,
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
    /// a sink with limits sheds the connection past the high-water mark. The
    /// byte estimate is computed here for callers that don't already have one.
    pub fn push(&self, msg: Value) {
        let est_bytes = rust_cvr::client_handler::estimate_json_bytes(&msg);
        let _ = self.send_command(WsCommand::Send { msg, est_bytes });
    }

    /// Push a downstream message whose approximate serialized size is already
    /// known (poke parts, where the assembler accumulated the estimate for
    /// free) — skips the estimator walk.
    pub fn push_sized(&self, msg: Value, est_bytes: usize) {
        let _ = self.send_command(WsCommand::Send { msg, est_bytes });
    }

    // NOTE: a `push_serializable(&impl Serialize)` convenience once lived here;
    // it had no TS twin (TS `send()` stringifies the Downstream tuple directly)
    // and no callers — removed as dead drift. `push` / `push_sized` are the
    // live paths.

    /// Send an error message and close the connection with code 3000.
    pub fn fail(&self, error: ErrorBody) {
        let _ = self.send_command(WsCommand::Fail(error));
    }

    /// Close the connection gracefully.
    pub fn close(&self, reason: String) {
        let _ = self.send_command(WsCommand::Close(reason));
    }

    /// Close with an explicit RFC 6455 close code (see `WsCommand::CloseWithCode`).
    pub fn close_with_code(&self, code: u16, reason: String) {
        let _ = self.send_command(WsCommand::CloseWithCode { code, reason });
    }

    /// Record a slow-client shed exactly once per connection (the CG re-crosses
    /// the HWM on every subsequent push until the writer closes).
    fn count_shed_once(limits: &SinkLimits, reason: &'static str) {
        if limits
            .shed_counted
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            crate::metrics::record_ws_shed(reason);
        }
    }

    /// Enqueue a command onto the unbounded, order-preserving channel. Never
    /// blocks and never panics; failures are a dropped receiver (the WS writer
    /// task has exited) or a tripped slow-client high-water mark.
    fn send_command(&self, command: WsCommand) -> Result<(), String> {
        if let Some(limits) = &self.limits {
            // Only `Send` frames carry queue bytes; `Fail`/`Close` terminate the
            // stream and are accounted as depth only.
            let est = match &command {
                WsCommand::Send { est_bytes, .. } => *est_bytes as i64,
                _ => 0,
            };
            let depth = limits.depth.fetch_add(1, Ordering::SeqCst) + 1;
            if depth > limits.hwm {
                // Signal the writer to close NOW (it selects on `kill`), and
                // reject the command — the queue is already `hwm` deep.
                limits.depth.fetch_sub(1, Ordering::SeqCst);
                let _ = limits.kill.send(true);
                Self::count_shed_once(limits, "frame_hwm");
                return Err(format!(
                    "ws downstream queue overflow (depth > {}): slow client shed",
                    limits.hwm
                ));
            }
            // Primary bound: queued bytes. A single command can be a multi-MB
            // tree the frame HWM would never catch. `byte_hwm == 0` disables.
            if limits.byte_hwm > 0 {
                let bytes = limits.bytes.fetch_add(est, Ordering::SeqCst) + est;
                if bytes > limits.byte_hwm {
                    limits.bytes.fetch_sub(est, Ordering::SeqCst);
                    limits.depth.fetch_sub(1, Ordering::SeqCst);
                    let _ = limits.kill.send(true);
                    Self::count_shed_once(limits, "byte_hwm");
                    return Err(format!(
                        "ws downstream queue overflow (bytes > {}): slow client shed",
                        limits.byte_hwm
                    ));
                }
            }
            if self.tx.send(command).is_err() {
                limits.depth.fetch_sub(1, Ordering::SeqCst);
                if limits.byte_hwm > 0 {
                    limits.bytes.fetch_sub(est, Ordering::SeqCst);
                }
                return Err("ws sink closed".to_string());
            }
            crate::metrics::record_ws_queued_delta(1);
            crate::metrics::record_ws_queued_bytes_delta(est);
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
        let est_bytes = rust_cvr::client_handler::estimate_json_bytes(&msg);
        self.send_command(WsCommand::Send { msg, est_bytes })
    }

    fn push_sized(&self, msg: Value, est_bytes: usize) -> Result<(), String> {
        self.send_command(WsCommand::Send { msg, est_bytes })
    }

    fn fail(&self, e: String) {
        // rust-cvr passes a plain message; the accompanying `["error", ..]`
        // frame is delivered separately via `push`. Close with code 3000.
        // TS `ClientHandler.fail(e)` → `wrapWithProtocolError(e)`: Internal with
        // origin ZeroCache (types/error-with-level.ts).
        let _ = self.send_command(WsCommand::Fail(ErrorBody::Basic(BasicErrorBody {
            kind: ErrorKind::Internal,
            message: e,
            origin: Some(crate::protocol::ErrorOrigin::ZeroCache),
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

        assert!(matches!(rx.recv().await, Some(WsCommand::Send { .. })));
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
            bytes: Arc::new(AtomicI64::new(0)),
            byte_hwm: 0, // frame-HWM test: byte shedding disabled
            kill: kill_tx,
            shed_counted: AtomicBool::new(false),
        });
        let sink = DirectWebSocketSink::with_limits(tx, limits.clone());

        for i in 0..3 {
            assert!(
                sink.send_command(WsCommand::Send {
                    msg: serde_json::json!([i]),
                    est_bytes: 0,
                })
                .is_ok(),
                "frame {i} under the HWM must be accepted"
            );
        }
        assert!(!*kill_rx.borrow(), "kill must not fire under the HWM");
        // 4th frame crosses hwm=3 → rejected + kill fired.
        assert!(
            sink.send_command(WsCommand::Send {
                msg: serde_json::json!([3]),
                est_bytes: 0,
            })
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
                Some(WsCommand::Send { msg, .. }) => assert_eq!(msg[0], serde_json::json!(i)),
                _ => panic!("expected queued frame {i}"),
            }
        }
    }

    fn byte_limits(byte_hwm: i64) -> (Arc<SinkLimits>, watch::Receiver<bool>) {
        let (kill_tx, kill_rx) = watch::channel(false);
        let limits = Arc::new(SinkLimits {
            depth: Arc::new(AtomicI64::new(0)),
            hwm: 1_000_000, // frame HWM out of the way — exercise the byte bound
            bytes: Arc::new(AtomicI64::new(0)),
            byte_hwm,
            kill: kill_tx,
            shed_counted: AtomicBool::new(false),
        });
        (limits, kill_rx)
    }

    /// Crossing the byte HWM trips kill and rejects the frame; both counters are
    /// rolled back so the rejected frame is not accounted.
    #[tokio::test]
    async fn byte_hwm_overflow_trips_kill_and_rejects() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let (limits, kill_rx) = byte_limits(1000);
        let sink = DirectWebSocketSink::with_limits(tx, limits.clone());

        // Two 400-byte frames fit; the third (would be 1200 > 1000) is rejected.
        for _ in 0..2 {
            assert!(
                sink.send_command(WsCommand::Send {
                    msg: serde_json::json!(["x"]),
                    est_bytes: 400,
                })
                .is_ok()
            );
        }
        assert!(!*kill_rx.borrow(), "kill must not fire under the byte HWM");
        assert!(
            sink.send_command(WsCommand::Send {
                msg: serde_json::json!(["x"]),
                est_bytes: 400,
            })
            .is_err()
        );
        assert!(*kill_rx.borrow(), "kill fires on byte overflow");
        assert_eq!(
            limits.bytes.load(Ordering::SeqCst),
            800,
            "rejected bytes rolled back"
        );
        assert_eq!(
            limits.depth.load(Ordering::SeqCst),
            2,
            "rejected frame rolled back"
        );
    }

    /// Push N sized frames then drain them all → both counters return to zero
    /// (symmetric accounting: writer subtracts exactly what the sink added).
    #[tokio::test]
    async fn byte_accounting_is_symmetric() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let (limits, _kill_rx) = byte_limits(1_000_000);
        let sink = DirectWebSocketSink::with_limits(tx, limits.clone());

        for i in 0..10 {
            sink.send_command(WsCommand::Send {
                msg: serde_json::json!([i]),
                est_bytes: 123,
            })
            .unwrap();
        }
        assert_eq!(limits.bytes.load(Ordering::SeqCst), 1230);
        // Drain: subtract exactly the est each command carried (writer parity).
        while let Ok(cmd) = rx.try_recv() {
            if let WsCommand::Send { est_bytes, .. } = cmd {
                limits.bytes.fetch_sub(est_bytes as i64, Ordering::SeqCst);
                limits.depth.fetch_sub(1, Ordering::SeqCst);
            }
        }
        assert_eq!(
            limits.bytes.load(Ordering::SeqCst),
            0,
            "no byte drift after full drain"
        );
        assert_eq!(
            limits.depth.load(Ordering::SeqCst),
            0,
            "no frame drift after full drain"
        );
    }

    /// `byte_hwm == 0` disables byte shedding entirely; a huge frame is accepted.
    #[tokio::test]
    async fn byte_hwm_zero_disables_shedding() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let (limits, kill_rx) = byte_limits(0);
        let sink = DirectWebSocketSink::with_limits(tx, limits.clone());
        assert!(
            sink.send_command(WsCommand::Send {
                msg: serde_json::json!(["x"]),
                est_bytes: 10_000_000_000,
            })
            .is_ok()
        );
        assert!(!*kill_rx.borrow(), "byte_hwm=0 must never shed on bytes");
        assert_eq!(
            limits.bytes.load(Ordering::SeqCst),
            0,
            "disabled: bytes not accounted"
        );
    }

    /// `Fail`/`Close` commands terminate the stream and must not perturb the
    /// byte counter (they carry no bytes).
    #[tokio::test]
    async fn fail_and_close_do_not_touch_byte_counter() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let (limits, _kill_rx) = byte_limits(1000);
        let sink = DirectWebSocketSink::with_limits(tx, limits.clone());
        sink.send_command(WsCommand::Fail(ErrorBody::basic(
            ErrorKind::Internal,
            "x".to_string(),
        )))
        .unwrap();
        sink.send_command(WsCommand::Close("bye".to_string()))
            .unwrap();
        assert_eq!(
            limits.bytes.load(Ordering::SeqCst),
            0,
            "Fail/Close carry no bytes"
        );
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
                Some(WsCommand::Send { msg, .. }) => assert_eq!(msg[1], serde_json::json!(i)),
                _ => panic!("expected Send({i}) in order"),
            }
        }
        assert!(matches!(rx.recv().await, Some(WsCommand::Close(_))));
    }
}
