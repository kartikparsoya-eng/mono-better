//! Pusher — custom-mutation push path. Port of
//! `services/mutagen/pusher.ts` (`PusherService` + the sequential worker loop,
//! TS `PushWorker`), adapted to the Option-A relay architecture (registered
//! invention): TS's in-process pusher POSTs straight to the user's API server
//! via `fetchFromAPIServer`; rust forwards to the TS dispatcher's push
//! endpoint, which rebuilds the `userPushURL` request. Client-observable
//! contract (ordering, PushFailed semantics, results-via-poke) is identical.
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

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use tokio::sync::mpsc;

use crate::protocol::{
    ErrorKind, ErrorOrigin, ErrorReason, MutationID, PushFailedHttpBody, PushFailedZeroCacheBody,
};
use crate::router::ConnectionSinks;
use crate::workers::connection::HandlerResult;
use crate::workers::syncer_ws_message_handler::{
    ConnectionSelector, PushRelayHeaders, PusherDispatch,
};

/// Max bytes of a failing relay response body echoed back in a `PushFailed`
/// frame (TS `bodyPreview` parity — never buffer an unbounded error body).
const BODY_PREVIEW_CAP: usize = 1024;

/// A queued relay POST plus the metadata needed to surface a failure. Real
/// client pushes carry a `target`; synthetic cleanup pushes are fire-and-forget
/// (`None`) — TS emits no `PushFailed` for those either.
struct QueuedPush {
    payload: serde_json::Value,
    target: Option<PushTarget>,
    /// TS `combinePushes` grouping key `${clientID}:${wsID}:${revision}`
    /// (pusher.ts:655). Rust has no connCtx revision in the queue — the relay
    /// bakes the auth/header snapshot into the payload at enqueue — so the
    /// revision component is the snapshot ITSELF: exactly the fields TS
    /// `assertAreCompatiblePushes` requires equal (auth, cookie, origin,
    /// userID, push overrides, pushVersion, schemaVersion). Same-key ⇒
    /// TS-compatible ⇒ safe to merge.
    combine_key: String,
}

/// Who to notify (and about which mutations) when a real push's POST fails.
struct PushTarget {
    client_id: String,
    /// The socket that sent the push. A failure is delivered only if this is
    /// still the client's current socket (see `send_error_if_current`).
    ws_id: String,
    mutation_ids: Vec<MutationID>,
}

/// Extract `{id, clientID}` pairs from a push body's `mutations` array. Shared
/// by the enqueue-drop path and the drainer-failure path so both report the
/// same ids. Reads the *push body* (what lands under `payload["push"]`).
fn mutation_ids_of(push_body: &serde_json::Value) -> Vec<MutationID> {
    push_body
        .get("mutations")
        .and_then(|m| m.as_array())
        .map(|muts| {
            muts.iter()
                .filter_map(|m| {
                    Some(MutationID {
                        id: m.get("id")?.as_i64()?,
                        client_id: m.get("clientID")?.as_str()?.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Read up to `cap` bytes of a response body as a lossy UTF-8 preview. Bounded
/// so a huge error page can't be buffered into the `PushFailed` frame.
async fn read_body_preview(resp: reqwest::Response, cap: usize) -> Option<String> {
    let bytes = resp.bytes().await.ok()?;
    if bytes.is_empty() {
        return None;
    }
    let end = bytes.len().min(cap);
    Some(String::from_utf8_lossy(&bytes[..end]).into_owned())
}

/// How long a single relay POST may take before it is abandoned.
const RELAY_TIMEOUT: Duration = Duration::from_secs(10);

/// Internal mutation name `zero-server` handles directly (no user dispatch) by
/// deleting stored mutation results. Mirror of
/// `zero-protocol/src/mutation.ts CLEANUP_RESULTS_MUTATION_NAME`.
const CLEANUP_RESULTS_MUTATION_NAME: &str = "_zero_cleanupResults";

/// Max queued pushes before new ones are dropped (client re-pushes by design —
/// its lmid doesn't advance until the mutation lands). Bounds relay memory
/// during a TS-loopback outage: the drainer POSTs one-at-a-time with a 10s
/// timeout, so an outage otherwise grows the queue at push-rate × duration
/// while clients re-push into it. Dropping the NEWEST keeps the queue a
/// contiguous, in-order prefix. Env override: `PUSHER_QUEUE_CAP`.
const DEFAULT_QUEUE_CAP: i64 = 1024;

fn queue_cap() -> i64 {
    std::env::var("PUSHER_QUEUE_CAP")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &i64| *v > 0)
        .unwrap_or(DEFAULT_QUEUE_CAP)
}

/// Build the TS `combinePushes` grouping key for a queued relay push. See
/// `QueuedPush::combine_key` for the revision-twin rationale.
fn combine_key_of(
    selector: &ConnectionSelector,
    body: &serde_json::Value,
    payload: &serde_json::Value,
) -> String {
    // \u{1f} separators: unambiguous even if field values contain ':'.
    let sep = '\u{1f}';
    let mut key = String::new();
    for part in [selector.client_id.as_str(), selector.ws_id.as_str()] {
        key.push_str(part);
        key.push(sep);
    }
    for field in [
        "auth",
        "cookie",
        "origin",
        "userID",
        "userPushURL",
        "userPushHeaders",
        "requestHeaders",
    ] {
        key.push_str(
            &payload
                .get(field)
                .map(|v| v.to_string())
                .unwrap_or_default(),
        );
        key.push(sep);
    }
    for field in ["pushVersion", "schemaVersion"] {
        key.push_str(&body.get(field).map(|v| v.to_string()).unwrap_or_default());
        key.push(sep);
    }
    key
}

/// Port of TS `combinePushes` (pusher.ts:626): merge queued pushes with the
/// same `clientID:wsID:revision` snapshot into one composite push (mutations
/// arrays concatenated in order), preserving first-seen group order. Pushes
/// for different clients/sockets/snapshots stay separate. TS's `'stop'`
/// sentinel maps to the rust channel close (the drainer loop ends); there is
/// no in-band sentinel to handle here.
fn combine_pushes(entries: Vec<QueuedPush>) -> Vec<QueuedPush> {
    let mut order: Vec<String> = Vec::new();
    let mut groups: std::collections::HashMap<String, QueuedPush> =
        std::collections::HashMap::new();
    for entry in entries {
        match groups.entry(entry.combine_key.clone()) {
            std::collections::hash_map::Entry::Vacant(v) => {
                order.push(entry.combine_key.clone());
                v.insert(entry);
            }
            std::collections::hash_map::Entry::Occupied(mut o) => {
                let composite = o.get_mut();
                // Composite push body: concatenate `push.mutations` (TS
                // `composite.push.mutations.push(...entry.push.mutations)`).
                let extra = entry
                    .payload
                    .get("push")
                    .and_then(|p| p.get("mutations"))
                    .and_then(|m| m.as_array())
                    .cloned()
                    .unwrap_or_default();
                if let Some(muts) = composite
                    .payload
                    .get_mut("push")
                    .and_then(|p| p.get_mut("mutations"))
                    .and_then(|m| m.as_array_mut())
                {
                    muts.extend(extra);
                }
                // Failure target: a composite covering any real push must
                // report ALL its mutation ids on a failed POST.
                match (&mut composite.target, entry.target) {
                    (Some(t), Some(extra_t)) => t.mutation_ids.extend(extra_t.mutation_ids),
                    (t @ None, Some(extra_t)) => *t = Some(extra_t),
                    (_, None) => {}
                }
            }
        }
    }
    order
        .into_iter()
        .filter_map(|k| groups.remove(&k))
        .collect()
}

/// Port of TS `PusherService` (Option-A relay seat): relays custom pushes
/// to the TS push endpoint over HTTP, in order.
pub struct PusherService {
    tx: mpsc::UnboundedSender<QueuedPush>,
    /// Queued-but-not-yet-POSTed pushes (enqueue increments, drainer decrements).
    depth: Arc<AtomicI64>,
    cap: i64,
    /// Live-instance census guard (leak hunt): inc on construct, dec on drop.
    _census: crate::live_count::Guard,
}

impl PusherService {
    /// `relay_url` is the TS dispatcher's push endpoint (`PUSHER_URL`);
    /// `relay_token` is the shared secret gating it (`PUSHER_AUTH_TOKEN`);
    /// `sinks` lets a failed POST surface a `PushFailed` frame back to the
    /// originating client's socket.
    pub fn new(
        relay_url: String,
        relay_token: Option<String>,
        tokio_handle: tokio::runtime::Handle,
        sinks: ConnectionSinks,
    ) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<QueuedPush>();
        let depth = Arc::new(AtomicI64::new(0));
        let drainer_depth = depth.clone();
        // Single sequential drainer: builds the reqwest client inside the
        // runtime (so its pool/timers bind to the reactor) and POSTs each queued
        // push one at a time, preserving per-connection mutation order.
        tokio_handle.spawn(async move {
            // Explicit connect timeout so a half-open TS-loopback socket can't
            // wedge the drainer (parity with the JWKS/transform clients, which
            // set one). The per-request .timeout(RELAY_TIMEOUT) bounds the whole
            // POST; this bounds just the TCP/TLS connect.
            let client = reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(10))
                .build()
                .unwrap_or_default();
            while let Some(first) = rx.recv().await {
                // TS PushWorker.run: `task = dequeue(); rest = drain();
                // combinePushes([task, ...rest])` — pull everything already
                // queued behind the first item and merge same-snapshot pushes
                // into composite POSTs (order preserved).
                drainer_depth.fetch_sub(1, Ordering::SeqCst);
                let mut batch = vec![first];
                while let Ok(more) = rx.try_recv() {
                    drainer_depth.fetch_sub(1, Ordering::SeqCst);
                    batch.push(more);
                }
                for QueuedPush {
                    payload, target, ..
                } in combine_pushes(batch)
                {
                    let mut req = client
                        .post(&relay_url)
                        .timeout(RELAY_TIMEOUT)
                        .json(&payload);
                    if let Some(token) = &relay_token {
                        req = req.header("x-relay-auth", token);
                    }
                    match req.send().await {
                        Ok(resp) if resp.status().is_success() => {
                            // Drain the body: hyper only returns the connection
                            // to reqwest's pool once the response is consumed —
                            // otherwise every push pays a fresh loopback connect.
                            let _ = resp.bytes().await;
                        }
                        Ok(resp) => {
                            // Non-2xx: the app/endpoint rejected or errored. The
                            // client's lmid won't advance, so it re-pushes the
                            // pending mutation on its next attempt/reconnect — but
                            // surface a PushFailed now so it doesn't hang (TS parity:
                            // pusher.ts fails the downstream on a non-OK response).
                            let status = resp.status().as_u16() as i64;
                            let preview = read_body_preview(resp, BODY_PREVIEW_CAP).await;
                            tracing::warn!(status, "push relay returned non-2xx");
                            if let Some(t) = target {
                                let err = crate::protocol::ErrorBody::PushFailedHttp(
                                    PushFailedHttpBody {
                                        kind: ErrorKind::PushFailed,
                                        details: None,
                                        mutation_ids: t.mutation_ids,
                                        message: format!(
                                            "Fetch from API server returned non-OK status {status}"
                                        ),
                                        origin: ErrorOrigin::ZeroCache,
                                        reason: ErrorReason::Http,
                                        status,
                                        body_preview: preview,
                                    },
                                );
                                sinks.send_error_if_current(&t.client_id, &t.ws_id, &err);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "push relay request failed");
                            if let Some(t) = target {
                                let err = crate::protocol::ErrorBody::PushFailedZeroCache(
                                    PushFailedZeroCacheBody {
                                        kind: ErrorKind::PushFailed,
                                        details: None,
                                        mutation_ids: t.mutation_ids,
                                        // TS parity: pusher.ts catch → "Failed to push: …".
                                        message: format!("Failed to push: {e}"),
                                        origin: ErrorOrigin::ZeroCache,
                                        reason: ErrorReason::Internal,
                                    },
                                );
                                sinks.send_error_if_current(&t.client_id, &t.ws_id, &err);
                            }
                        }
                    }
                }
            }
        });
        Self {
            tx,
            depth,
            cap: queue_cap(),
            _census: crate::live_count::Guard::new(&crate::live_count::PUSHER),
        }
    }

    fn relay_body(
        selector: &ConnectionSelector,
        body: &serde_json::Value,
        headers: &PushRelayHeaders,
        client_group_id: &str,
    ) -> serde_json::Value {
        // The CURRENT forwarded token: each call site fills `headers.auth` fresh
        // from `mustGetConnectionContext(selector).auth` immediately before relaying
        // (mirrors TS reading it per push, pusher.ts), so an expired token never
        // reaches the API server as a stale 401.
        let auth = headers.auth.clone();
        let mut payload = serde_json::json!({
            "clientGroupID": client_group_id,
            "clientID": selector.client_id,
            "push": body,
            "auth": auth,
            "cookie": headers.cookie,
            "origin": headers.origin,
            "requestHeaders": headers.request_headers,
            "userID": headers.user_id,
        });
        // Client push overrides (initConnection userPushURL/userPushHeaders).
        // The TS relay validates the URL against the configured allowlist and
        // filters the headers through `allowedClientHeaders` — exactly what the
        // in-process TS pusher does per connection.
        if let Ok(guard) = headers.push_override.lock()
            && let Some(ov) = guard.as_ref()
        {
            let obj = payload.as_object_mut().expect("relay body is an object");
            if let Some(url) = &ov.url {
                obj.insert("userPushURL".into(), serde_json::json!(url));
            }
            if let Some(hdrs) = &ov.headers {
                let map: serde_json::Map<String, serde_json::Value> = hdrs
                    .iter()
                    .map(|(k, v)| (k.clone(), serde_json::json!(v)))
                    .collect();
                obj.insert("userPushHeaders".into(), serde_json::Value::Object(map));
            }
        }
        payload
    }

    /// Cap-checked enqueue shared by real pushes and synthetic cleanup pushes.
    /// Returns false when the payload was dropped (queue full / channel closed).
    fn enqueue_payload(&self, push: QueuedPush, what: &str) -> bool {
        if self.depth.load(Ordering::SeqCst) >= self.cap {
            tracing::warn!(cap = self.cap, "push relay queue full; dropping {what}");
            return false;
        }
        self.depth.fetch_add(1, Ordering::SeqCst);
        if let Err(e) = self.tx.send(push) {
            self.depth.fetch_sub(1, Ordering::SeqCst);
            tracing::warn!(error = %e, "push relay channel closed; {what} dropped");
            return false;
        }
        true
    }

    /// Build the synthetic `_zero_cleanupResults` push body the TS
    /// `PusherService` sends for mutation-result cleanup. `args` is the single
    /// element of the mutation's `args` array (`{type:"single"|"bulk", …}`).
    fn cleanup_push_body(
        client_group_id: &str,
        sender_client_id: &str,
        request_id: String,
        args: serde_json::Value,
    ) -> serde_json::Value {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        serde_json::json!({
            "clientGroupID": client_group_id,
            "mutations": [{
                "type": "custom",
                // Not tracked — fire-and-forget, same as TS (`id: 0`).
                "id": 0,
                "clientID": sender_client_id,
                "name": CLEANUP_RESULTS_MUTATION_NAME,
                "args": [args],
                "timestamp": now_ms,
            }],
            "pushVersion": 1,
            "timestamp": now_ms,
            "requestID": request_id,
        })
    }
}

impl PusherDispatch for PusherService {
    fn enqueue_push(
        &self,
        selector: &ConnectionSelector,
        body: &serde_json::Value,
        headers: &PushRelayHeaders,
        client_group_id: &str,
    ) -> HandlerResult {
        // Queue cap: during a relay-endpoint outage the drainer stalls (10s per
        // item); dropping the NEWEST push keeps the queue an in-order prefix and
        // bounds memory. The client's lmid never advances for a dropped push, so
        // it re-pushes — the same recovery as a failed POST.
        // Capture the failure target BEFORE moving the payload into the queue:
        // a drainer-side POST failure (non-2xx / network) surfaces a PushFailed
        // frame to this exact socket via `send_error_if_current`.
        let mutation_ids = mutation_ids_of(body);
        let payload = Self::relay_body(selector, body, headers, client_group_id);
        let combine_key = combine_key_of(selector, body, &payload);
        let target = PushTarget {
            client_id: selector.client_id.clone(),
            ws_id: selector.ws_id.clone(),
            mutation_ids: mutation_ids.clone(),
        };
        // Enqueue only — never blocks, so this is safe on the async CG-executor
        // threads. The drainer POSTs sequentially (see `new`).
        if self.enqueue_payload(
            QueuedPush {
                payload,
                target: Some(target),
                combine_key,
            },
            "push (client will re-push)",
        ) {
            return HandlerResult::Ok;
        }
        // The push was DROPPED (queue cap / relay shut down). TS surfaces a
        // failed push as a PushFailed error frame; a silent drop here left the
        // client believing the mutation was in flight until its lmid stalled.
        // Transient: the connection stays open and the client re-pushes.
        HandlerResult::Transient {
            errors: vec![crate::protocol::ErrorBody::PushFailedZeroCache(
                PushFailedZeroCacheBody {
                    kind: ErrorKind::PushFailed,
                    details: None,
                    mutation_ids,
                    message: "push relay queue is full; retry".to_string(),
                    origin: ErrorOrigin::ZeroCache,
                    reason: ErrorReason::Internal,
                },
            )],
        }
    }

    fn init_connection(&self, _selector: &ConnectionSelector) {}

    fn ack_mutation_responses(
        &self,
        selector: &ConnectionSelector,
        body: &serde_json::Value,
        headers: &PushRelayHeaders,
        client_group_id: &str,
    ) {
        // Port of `PusherService.ackMutationResponses`: relay a synthetic
        // `_zero_cleanupResults` push so the API server deletes stored mutation
        // results up to the acked ID. Fire-and-forget (a drop only delays
        // cleanup until the next ack).
        let (client_id, up_to_id) = match (
            body.get("clientID").and_then(|v| v.as_str()),
            body.get("id").and_then(|v| v.as_i64()),
        ) {
            (Some(c), Some(i)) => (c.to_string(), i),
            _ => {
                tracing::warn!("ackMutationResponses body missing clientID/id; skipped");
                return;
            }
        };
        let push = Self::cleanup_push_body(
            client_group_id,
            &client_id,
            format!("cleanup-{client_group_id}-{client_id}-{up_to_id}"),
            serde_json::json!({
                "type": "single",
                "clientGroupID": client_group_id,
                "clientID": client_id,
                "upToMutationID": up_to_id,
            }),
        );
        let payload = Self::relay_body(selector, &push, headers, client_group_id);
        let combine_key = combine_key_of(selector, &push, &payload);
        self.enqueue_payload(
            QueuedPush {
                payload,
                target: None,
                combine_key,
            },
            "mutation-results cleanup (single)",
        );
    }

    fn delete_client_mutations(
        &self,
        selector: &ConnectionSelector,
        client_ids: &[String],
        headers: &PushRelayHeaders,
        client_group_id: &str,
    ) {
        // Port of `PusherService.deleteClientMutations`: bulk-clean the stored
        // mutation results of explicitly deleted clients.
        if client_ids.is_empty() {
            return;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let push = Self::cleanup_push_body(
            client_group_id,
            // TS uses the first deleted client as the nominal sender.
            &client_ids[0],
            format!("cleanup-bulk-{client_group_id}-{now_ms}"),
            serde_json::json!({
                "type": "bulk",
                "clientGroupID": client_group_id,
                "clientIDs": client_ids,
            }),
        );
        let payload = Self::relay_body(selector, &push, headers, client_group_id);
        let combine_key = combine_key_of(selector, &push, &payload);
        self.enqueue_payload(
            QueuedPush {
                payload,
                target: None,
                combine_key,
            },
            "mutation-results cleanup (bulk)",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ws_sink::{DirectWebSocketSink, WsCommand};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// `mutation_ids_of` extracts `{id, clientID}` from a push body's
    /// `mutations` array (shared by the drop path and the drainer-failure path).
    #[test]
    fn mutation_ids_of_extracts_pairs() {
        let body = serde_json::json!({"mutations": [
            {"id": 7, "clientID": "cA"},
            {"id": 8, "clientID": "cB"},
            {"id": 9},                    // missing clientID → skipped
        ]});
        let ids = mutation_ids_of(&body);
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0].id, 7);
        assert_eq!(ids[0].client_id, "cA");
        assert_eq!(ids[1].id, 8);
        // No mutations key → empty, not a panic.
        assert!(mutation_ids_of(&serde_json::json!({})).is_empty());
    }

    /// A one-shot TCP server that returns `status` with `body` then closes.
    /// Returns the bound address to point the relay at.
    async fn oneshot_http(status: u16, body: &'static str) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 2048];
                let _ = sock.read(&mut buf).await;
                let resp = format!(
                    "HTTP/1.1 {status} X\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            }
        });
        addr
    }

    fn selector(client_id: &str, ws_id: &str) -> ConnectionSelector {
        ConnectionSelector {
            client_id: client_id.to_string(),
            ws_id: ws_id.to_string(),
        }
    }

    /// Port of the TS `combinePushes` unit semantics (pusher.test.ts): pushes
    /// with the same clientID/wsID/snapshot merge into ONE composite with the
    /// mutations concatenated in order; different sockets/snapshots stay
    /// separate; first-seen group order is preserved.
    #[test]
    fn combine_pushes_merges_same_connection_snapshot() {
        let hdrs = PushRelayHeaders::default();
        let mk = |ws: &str, id: i64| {
            let sel = selector("cA", ws);
            let body = serde_json::json!({"pushVersion": 1, "mutations": [
                {"id": id, "clientID": "cA", "type": "custom", "name": "m"}
            ]});
            let payload = PusherService::relay_body(&sel, &body, &hdrs, "cg1");
            let combine_key = combine_key_of(&sel, &body, &payload);
            QueuedPush {
                payload,
                target: Some(PushTarget {
                    client_id: "cA".into(),
                    ws_id: ws.to_string(),
                    mutation_ids: mutation_ids_of(&body),
                }),
                combine_key,
            }
        };
        let combined = combine_pushes(vec![mk("ws1", 1), mk("ws1", 2), mk("ws2", 3), mk("ws1", 4)]);
        assert_eq!(combined.len(), 2, "ws1 group merges; ws2 stays separate");
        let muts = |p: &QueuedPush| {
            p.payload["push"]["mutations"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m["id"].as_i64().unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(muts(&combined[0]), vec![1, 2, 4], "in-order concat");
        assert_eq!(muts(&combined[1]), vec![3]);
        let t = combined[0].target.as_ref().unwrap();
        assert_eq!(
            t.mutation_ids.iter().map(|m| m.id).collect::<Vec<_>>(),
            vec![1, 2, 4],
            "failure target covers every merged mutation"
        );
    }

    /// A counting HTTP server: first request is held for `first_delay_ms`, all
    /// requests recorded (body) and answered 200. Lets a batch build up behind
    /// the sequential drainer.
    async fn counting_http(
        first_delay_ms: u64,
    ) -> (
        std::net::SocketAddr,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen: std::sync::Arc<std::sync::Mutex<Vec<String>>> = Default::default();
        let record = seen.clone();
        tokio::spawn(async move {
            let mut first = true;
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let mut buf = vec![0u8; 65536];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).into_owned();
                let body = req
                    .split_once("\r\n\r\n")
                    .map(|(_, b)| b.to_string())
                    .unwrap_or_default();
                record.lock().unwrap().push(body);
                if first {
                    first = false;
                    tokio::time::sleep(Duration::from_millis(first_delay_ms)).await;
                }
                let resp = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";
                let _ = sock.write_all(resp.as_bytes()).await;
                let _ = sock.flush().await;
            }
        });
        (addr, seen)
    }

    /// NON-VACUOUS wiring proof for `combinePushes`: with the first POST held,
    /// pushes that queue up behind it for the SAME connection snapshot must be
    /// drained as ONE merged POST (TS PushWorker: dequeue + drain →
    /// combinePushes → process). Written BEFORE the drainer wiring — it fails
    /// with one POST per push — and passes once the drainer combines.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drainer_combines_queued_pushes_per_connection() {
        let (addr, seen) = counting_http(500).await;
        let sinks = ConnectionSinks::new();
        let pusher = PusherService::new(
            format!("http://{addr}/push"),
            None,
            tokio::runtime::Handle::current(),
            sinks,
        );
        let hdrs = PushRelayHeaders::default();
        let push = |id: i64| {
            serde_json::json!({"pushVersion": 1, "mutations": [
                {"id": id, "clientID": "cA", "type": "custom", "name": "m"}
            ]})
        };
        // p1 is picked up by the drainer and held at the server.
        let _ = pusher.enqueue_push(&selector("cA", "ws1"), &push(1), &hdrs, "cg1");
        tokio::time::sleep(Duration::from_millis(150)).await;
        // These three queue up behind p1: two share ws1's snapshot, one is ws2.
        let _ = pusher.enqueue_push(&selector("cA", "ws1"), &push(2), &hdrs, "cg1");
        let _ = pusher.enqueue_push(&selector("cA", "ws1"), &push(3), &hdrs, "cg1");
        let _ = pusher.enqueue_push(&selector("cA", "ws2"), &push(4), &hdrs, "cg1");

        // Wait for the drain to complete (3 POSTs expected: p1, merged p2+p3, p4).
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let n = seen.lock().unwrap().len();
            if n >= 3 && std::time::Instant::now() > deadline - Duration::from_secs(4) {
                // Give a beat for any (incorrect) 4th POST to land, then stop.
                tokio::time::sleep(Duration::from_millis(300)).await;
                break;
            }
            if std::time::Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let bodies = seen.lock().unwrap().clone();
        let ids_of = |b: &str| -> Vec<i64> {
            serde_json::from_str::<serde_json::Value>(b)
                .ok()
                .and_then(|v| {
                    v["push"]["mutations"]
                        .as_array()
                        .map(|muts| muts.iter().filter_map(|m| m["id"].as_i64()).collect())
                })
                .unwrap_or_default()
        };
        assert_eq!(
            bodies.len(),
            3,
            "same-snapshot pushes must merge into one POST (TS combinePushes); got {:?}",
            bodies.iter().map(|b| ids_of(b)).collect::<Vec<_>>()
        );
        assert_eq!(ids_of(&bodies[0]), vec![1]);
        assert_eq!(ids_of(&bodies[1]), vec![2, 3], "ws1 batch merged in order");
        assert_eq!(ids_of(&bodies[2]), vec![4], "ws2 stays separate");
    }

    /// A non-2xx relay response surfaces a `PushFailedHttp` frame to the
    /// originating socket, carrying the status, mutation ids, and a body preview
    /// — without closing the connection.
    #[tokio::test]
    async fn drainer_surfaces_push_failed_http_on_non_2xx() {
        let addr = oneshot_http(500, "nope").await;
        let sinks = ConnectionSinks::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        sinks.insert_for_test("cA", "ws1", DirectWebSocketSink::new(tx));

        let pusher = PusherService::new(
            format!("http://{addr}/push"),
            None,
            tokio::runtime::Handle::current(),
            sinks,
        );
        let body = serde_json::json!({"mutations": [{"id": 7, "clientID": "cA"}]});
        let _ = pusher.enqueue_push(
            &selector("cA", "ws1"),
            &body,
            &PushRelayHeaders::default(),
            "cg1",
        );

        let frame = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("push failure frame not delivered in time")
            .expect("channel closed");
        match frame {
            WsCommand::Send { msg, .. } => {
                assert_eq!(msg[0], "error");
                assert_eq!(msg[1]["kind"], "PushFailed");
                assert_eq!(msg[1]["status"], 500);
                assert_eq!(msg[1]["mutationIDs"][0]["id"], 7);
                assert_eq!(msg[1]["body_preview"], "nope");
            }
            _ => panic!("expected a non-closing error Send frame"),
        }
    }

    /// A network-level failure (nothing listening) surfaces a
    /// `PushFailedZeroCache` frame with a "Failed to push:" message.
    #[tokio::test]
    async fn drainer_surfaces_push_failed_zerocache_on_network_error() {
        // Bind then drop to get an almost-certainly-closed port.
        let dead = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap()
        };
        let sinks = ConnectionSinks::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        sinks.insert_for_test("cA", "ws1", DirectWebSocketSink::new(tx));

        let pusher = PusherService::new(
            format!("http://{dead}/push"),
            None,
            tokio::runtime::Handle::current(),
            sinks,
        );
        let body = serde_json::json!({"mutations": [{"id": 3, "clientID": "cA"}]});
        let _ = pusher.enqueue_push(
            &selector("cA", "ws1"),
            &body,
            &PushRelayHeaders::default(),
            "cg1",
        );

        let frame = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("push failure frame not delivered in time")
            .expect("channel closed");
        match frame {
            WsCommand::Send { msg, .. } => {
                assert_eq!(msg[1]["kind"], "PushFailed");
                assert!(
                    msg[1]["message"]
                        .as_str()
                        .unwrap()
                        .starts_with("Failed to push:"),
                    "got: {}",
                    msg[1]["message"]
                );
            }
            _ => panic!("expected an error Send frame"),
        }
    }

    /// A push whose target socket has been superseded (client reconnected under
    /// a new ws_id) delivers NOTHING — the replacement re-pushes on its own.
    #[tokio::test]
    async fn drainer_drops_frame_for_superseded_socket() {
        let addr = oneshot_http(500, "x").await;
        let sinks = ConnectionSinks::new();
        let (tx, mut rx) = mpsc::unbounded_channel();
        // Registered under ws2, but the push targets ws1 (the old socket).
        sinks.insert_for_test("cA", "ws2", DirectWebSocketSink::new(tx));

        let pusher = PusherService::new(
            format!("http://{addr}/push"),
            None,
            tokio::runtime::Handle::current(),
            sinks,
        );
        let body = serde_json::json!({"mutations": [{"id": 1, "clientID": "cA"}]});
        let _ = pusher.enqueue_push(
            &selector("cA", "ws1"),
            &body,
            &PushRelayHeaders::default(),
            "cg1",
        );

        // Give the drainer time to POST + fail; ws2 must receive nothing.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            rx.try_recv().is_err(),
            "superseded socket must not be notified"
        );
    }

    /// Client push overrides captured at initConnection must reach the relay
    /// payload (the TS endpoint applies the allowlist/filters); without an
    /// override the fields are absent entirely (not null).
    #[test]
    fn relay_body_carries_user_push_overrides() {
        let selector = ConnectionSelector {
            client_id: "cA".to_string(),
            ws_id: "ws1".to_string(),
        };
        let push = serde_json::json!({"mutations": []});
        let headers = PushRelayHeaders::default();

        let body = PusherService::relay_body(&selector, &push, &headers, "cg1");
        assert!(body.get("userPushURL").is_none());
        assert!(body.get("userPushHeaders").is_none());

        *headers.push_override.lock().unwrap() =
            Some(crate::workers::syncer_ws_message_handler::PushOverride {
                url: Some("https://api.example.com/push".to_string()),
                headers: Some(vec![("x-tenant".to_string(), "acme".to_string())]),
            });
        let body = PusherService::relay_body(&selector, &push, &headers, "cg1");
        assert_eq!(body["userPushURL"], "https://api.example.com/push");
        assert_eq!(body["userPushHeaders"]["x-tenant"], "acme");
    }

    /// The synthetic cleanup push must match the TS `PusherService` shape
    /// exactly — zero-server dispatches on `type: "custom"` +
    /// `name: "_zero_cleanupResults"` and validates `cleanupResultsArgSchema`.
    #[test]
    fn cleanup_push_body_matches_ts_shape() {
        let body = PusherService::cleanup_push_body(
            "cg1",
            "cA",
            "cleanup-cg1-cA-7".to_string(),
            serde_json::json!({
                "type": "single",
                "clientGroupID": "cg1",
                "clientID": "cA",
                "upToMutationID": 7,
            }),
        );
        assert_eq!(body["clientGroupID"], "cg1");
        assert_eq!(body["pushVersion"], 1);
        assert_eq!(body["requestID"], "cleanup-cg1-cA-7");
        let m = &body["mutations"][0];
        assert_eq!(m["type"], "custom");
        assert_eq!(m["id"], 0);
        assert_eq!(m["clientID"], "cA");
        assert_eq!(m["name"], CLEANUP_RESULTS_MUTATION_NAME);
        assert_eq!(m["args"][0]["type"], "single");
        assert_eq!(m["args"][0]["upToMutationID"], 7);
        assert!(body["timestamp"].as_i64().unwrap() > 0);
    }
}
