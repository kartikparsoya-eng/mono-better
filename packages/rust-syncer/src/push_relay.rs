//! HTTP relay for custom-mutation pushes.
//!
//! The Rust syncer keeps the sync connection read-only and runs ZERO mutation
//! logic. When a client sends a custom `push` over the sync WebSocket, this
//! relay forwards the push body plus the connection's raw auth/header material
//! to the TS dispatcher's push endpoint (`PUSHER_URL`). The TS side rebuilds the
//! `userPushURL` request via the existing `fetchFromAPIServer('push', …)` path
//! (the real pusher), so mutation handling stays entirely in TS.
//!
//! The mutation RESULT is NOT returned over this relay — it flows back to the
//! client through the CVR's `lmids` (→ `lastMutationIDChanges`) and
//! `mutationResults` (→ `mutationsPatch`) queries the [`crate::sync_engine`]
//! already hydrates and pokes. So the relay is one-directional: forward the
//! push, let the app apply it, and the normal sync poke delivers the outcome.
//!
//! Ordering: pushes are drained from a FIFO channel by a single background task
//! that POSTs them ONE AT A TIME. A client's mutations must reach the app in
//! id-order (the app rejects an out-of-order `lastMutationID`); a single
//! sequential drainer preserves that. `enqueue_push` only enqueues (never
//! blocks), so it is safe to call from the async CG-executor threads — unlike a
//! `block_on`, which panics inside a tokio worker.

use std::time::Duration;

use tokio::sync::mpsc;

use crate::connection::HandlerResult;
use crate::message_handler::{ConnectionSelector, PushRelayHeaders, PusherDispatch};

/// How long a single relay POST may take before it is abandoned.
const RELAY_TIMEOUT: Duration = Duration::from_secs(10);

/// Relays custom pushes to the TS push endpoint over HTTP, in order.
pub struct HttpRelayPusher {
    tx: mpsc::UnboundedSender<serde_json::Value>,
}

impl HttpRelayPusher {
    /// `relay_url` is the TS dispatcher's push endpoint (`PUSHER_URL`);
    /// `relay_token` is the shared secret gating it (`PUSHER_AUTH_TOKEN`).
    pub fn new(
        relay_url: String,
        relay_token: Option<String>,
        tokio_handle: tokio::runtime::Handle,
    ) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<serde_json::Value>();
        // Single sequential drainer: builds the reqwest client inside the
        // runtime (so its pool/timers bind to the reactor) and POSTs each queued
        // push one at a time, preserving per-connection mutation order.
        tokio_handle.spawn(async move {
            let client = reqwest::Client::new();
            while let Some(payload) = rx.recv().await {
                let mut req = client
                    .post(&relay_url)
                    .timeout(RELAY_TIMEOUT)
                    .json(&payload);
                if let Some(token) = &relay_token {
                    req = req.header("x-relay-auth", token);
                }
                match req.send().await {
                    Ok(resp) if resp.status().is_success() => {}
                    Ok(resp) => {
                        // Non-2xx: the app/endpoint rejected or errored. The
                        // client's lmid won't advance, so it re-pushes the
                        // pending mutation on its next attempt/reconnect.
                        tracing::warn!(status = %resp.status(), "push relay returned non-2xx");
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "push relay request failed");
                    }
                }
            }
        });
        Self { tx }
    }

    fn relay_body(
        selector: &ConnectionSelector,
        body: &serde_json::Value,
        headers: &PushRelayHeaders,
        client_group_id: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "clientGroupID": client_group_id,
            "clientID": selector.client_id,
            "push": body,
            "auth": headers.auth,
            "cookie": headers.cookie,
            "origin": headers.origin,
            "requestHeaders": headers.request_headers,
            "userID": headers.user_id,
        })
    }
}

impl PusherDispatch for HttpRelayPusher {
    fn enqueue_push(
        &self,
        selector: &ConnectionSelector,
        body: &serde_json::Value,
        headers: &PushRelayHeaders,
        client_group_id: &str,
    ) -> HandlerResult {
        let payload = Self::relay_body(selector, body, headers, client_group_id);
        // Enqueue only — never blocks, so this is safe on the async CG-executor
        // threads. The drainer POSTs sequentially (see `new`).
        if let Err(e) = self.tx.send(payload) {
            tracing::warn!(error = %e, "push relay channel closed; mutation dropped");
        }
        HandlerResult::Ok
    }

    fn init_connection(&self, _selector: &ConnectionSelector) {}

    fn ack_mutation_responses(&self, _selector: &ConnectionSelector, _body: &serde_json::Value) {}

    fn delete_client_mutations(&self, _selector: &ConnectionSelector, _client_ids: &[String]) {}
}
