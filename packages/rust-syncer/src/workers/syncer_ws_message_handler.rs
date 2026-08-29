//! Message handler — port of `syncer-ws-message-handler.ts` (~283 LOC).
//!
//! Routes upstream messages to the appropriate service (ViewSyncer, Mutagen,
//! Pusher). Each message type is handled according to the TS behavior.
//!
//! In Phase 2, the handler dispatches via trait objects. The actual ViewSyncer
//! implementation comes in Phase 4. Mutagen/Pusher stay in TS (HTTP forwarding
//! in the full binary).

use crate::protocol::{self, ErrorBody, ErrorKind, ErrorOrigin, PushBody, Upstream};
use crate::workers::connection::{HandlerResult, MessageHandler};
use std::rc::Rc;
use std::sync::{Arc, Mutex};

/// Connection selector — identifies a connection by clientID + wsID.
#[derive(Debug, Clone)]
pub struct ConnectionSelector {
    pub client_id: String,
    pub ws_id: String,
}

/// Traceparent propagation (W3C trace context).
///
/// In the TS code, this uses OTel context propagation. In Rust, we extract
/// the traceparent but don't propagate it via OTel context yet (Phase 4).
fn with_traceparent<F, R>(traceparent: Option<&str>, f: F) -> R
where
    F: FnOnce() -> R,
{
    if let Some(tp) = traceparent {
        tracing::debug!(traceparent = tp, "processing with traceparent");
    }
    f()
}

/// Trait for the ViewSyncer dispatch interface.
///
/// Implemented live by the CG-side `CgViewSyncer` adapter (L9 Stage 3d), which
/// executes each message body INLINE on the CG task — the twin of TS
/// `viewSyncer.<method>` awaiting the view-syncer `#lock`. `async` + `?Send`
/// (not `Send + Sync`): the live impl holds CG-local (`!Send`) state and must
/// run to completion within the dispatch call so message handling stays
/// strictly serial in arrival order (re-enqueueing would reorder vs TS's
/// FIFO-at-arrival lock; `spawn_local` would interleave at await points).
#[async_trait::async_trait(?Send)]
pub trait ViewSyncerDispatch {
    /// Handle `changeDesiredQueries` message.
    async fn change_desired_queries(&self, selector: &ConnectionSelector, msg: &str);

    /// Handle `updateAuth` message.
    async fn update_auth(
        &self,
        selector: &ConnectionSelector,
        msg: &str,
        auth_revision_changed: bool,
    );

    /// Handle `deleteClients` message. Returns deleted client IDs.
    async fn delete_clients(&self, selector: &ConnectionSelector, msg: &str) -> Vec<String>;

    /// Handle `initConnection` message. Returns true if the connection
    /// was accepted (stream started).
    async fn init_connection(&self, selector: &ConnectionSelector, msg: &str) -> bool;

    /// Handle `inspect` message.
    async fn inspect(&self, selector: &ConnectionSelector, msg: &str);
}

/// Trait for the connection context manager interface.
pub trait ConnContextManagerDispatch: Send + Sync {
    /// Get connection context — MUST semantics: `Err` (the TS
    /// `mustGetConnectionContext` throw, an `InvalidConnectionRequest`
    /// ProtocolError) when no context is registered for `selector`. Callers on
    /// the push/CRUD paths surface the error to the client; they must NEVER
    /// proceed with a defaulted/empty context — a silently-defaulted
    /// `auth: None` is how the 2026-08-29 prod relay POSTed pushes with no
    /// Authorization at all ("No token provided" 401s).
    fn must_get_connection_context(
        &self,
        selector: &ConnectionSelector,
    ) -> Result<ConnContextInfo, Box<ErrorBody>>;

    /// Initialize/update connection from initConnection body.
    fn init_connection(&self, selector: &ConnectionSelector, body: &serde_json::Value);

    /// Update auth. Returns whether the auth revision changed.
    fn update_auth(&self, selector: &ConnectionSelector, body: &serde_json::Value) -> bool;
}

/// Info about a connection context needed by the message handler.
#[derive(Debug)]
pub struct ConnContextInfo {
    pub auth: Option<String>,
    /// Whether `auth` is an OPAQUE token (CCM `Auth::Opaque`, TS
    /// `auth.type === 'opaque'`). The CRUD path rejects opaque auth exactly
    /// like the TS assert (syncer-ws-message-handler.ts:152-155).
    pub is_opaque: bool,
    pub revision: u32,
}

/// Trait for the Mutagen interface (CRUD mutation processing).
pub trait MutagenDispatch: Send + Sync {
    /// Process a single mutation. Returns (kind, message) if error.
    fn process_mutation(
        &self,
        mutation: &serde_json::Value,
        auth: Option<&serde_json::Value>,
        has_pusher: bool,
    ) -> Option<(ErrorKind, String)>;
}

/// Per-connection header/auth material forwarded with a relayed push, so the TS
/// relay endpoint can rebuild the `userPushURL` request (Bearer auth, Cookie,
/// Origin, forwarded request headers) the same way `fetchFromAPIServer` does for
/// the query path. These are the RAW values captured at `initConnection`; the TS
/// side applies its own push-config allowlist/composition. The Rust syncer runs
/// zero mutation logic — it only relays these bytes.
#[derive(Clone, Default)]
pub struct PushRelayHeaders {
    /// Bearer auth token forwarded to the TS relay endpoint. Read FRESH from the
    /// ConnectionContextManager (the single owner of per-connection auth) at each
    /// relay, mirroring TS `pusher.ts enqueuePush` reading
    /// `mustGetConnectionContext(selector).auth?.raw` per push. Each relay call
    /// site sets this from `ConnContextManagerDispatch::must_get_connection_context`
    /// immediately before relaying, so a token refreshed mid-session via
    /// `updateAuth` is always current — a connect-time snapshot would go stale and
    /// the API server would reject it with 401 (the 2026-08-27 push-relay
    /// incident). No parallel auth copy is kept (I-8: one owner).
    pub auth: Option<String>,
    /// The connection-context revision `auth` was read at (TS
    /// `connCtx.revision`, captured per push at enqueue like
    /// `entry.connCtx.revision` in pusher.ts). The pusher's auth-failure
    /// invalidation passes it to `failConnection(selector, revision)` so a
    /// 401 on an OLD token can never tear down a connection that has since
    /// re-authed (the CCM's stale-revision guard).
    pub revision: u32,
    pub cookie: Option<String>,
    pub origin: Option<String>,
    pub request_headers: Vec<(String, String)>,
    pub user_id: Option<String>,
    /// Client-supplied `userPushURL`/`userPushHeaders` from `initConnection`
    /// (TS `ConnectionContextManager` applies these per connection). Shared
    /// via `Arc` because the router and the message handler each hold a clone
    /// of this struct, and the override arrives AFTER both were constructed
    /// (the initConnection body is the first post-handshake message). The TS
    /// relay endpoint enforces the push-URL allowlist and the
    /// `allowedClientHeaders` filter — the rust side only relays the bytes.
    pub push_override: std::sync::Arc<std::sync::Mutex<Option<PushOverride>>>,
}

/// The client's per-connection push overrides (see `PushRelayHeaders`).
#[derive(Clone, Default)]
pub struct PushOverride {
    pub url: Option<String>,
    pub headers: Option<Vec<(String, String)>>,
}

/// Trait for the Pusher interface (custom mutation forwarding).
pub trait PusherDispatch: Send + Sync {
    /// Enqueue a push message. Returns a HandlerResult.
    fn enqueue_push(
        &self,
        selector: &ConnectionSelector,
        body: &serde_json::Value,
        headers: &PushRelayHeaders,
        client_group_id: &str,
    ) -> HandlerResult;

    /// Initialize connection for pusher.
    fn init_connection(&self, selector: &ConnectionSelector);

    /// Ack mutation responses: relay a `_zero_cleanupResults` push (type
    /// `single`) so the API server prunes stored mutation results up to the
    /// acked mutation ID. Port of `PusherService.ackMutationResponses`.
    fn ack_mutation_responses(
        &self,
        selector: &ConnectionSelector,
        body: &serde_json::Value,
        headers: &PushRelayHeaders,
        client_group_id: &str,
    );

    /// Delete client mutations: relay a `_zero_cleanupResults` push (type
    /// `bulk`) for explicitly deleted clients. Port of
    /// `PusherService.deleteClientMutations`.
    fn delete_client_mutations(
        &self,
        selector: &ConnectionSelector,
        client_ids: &[String],
        headers: &PushRelayHeaders,
        client_group_id: &str,
    );

    /// Install the push-auth-failure invalidation hook. Port of the TS pusher's
    /// `isAuthErrorBody(response)` → `#connContextManager.failConnection(
    /// entry.connCtx, entry.connCtx.revision)` (pusher.ts:539/569): when the
    /// relayed push comes back 401/403, the connection's context is removed at
    /// the captured revision, so the connection's next message must-fails and
    /// the client reconnects with FRESH auth instead of relaying a dead token
    /// forever (the 2026-08-29 401 storm). Default no-op for dispatch impls
    /// without a connection-context owner (tests).
    fn set_auth_fail_hook(&self, _hook: AuthFailHook) {}
}

/// Callback invoked by the pusher's drainer on a 401/403 relay response:
/// `(selector, revision)` → `ConnectionContextManager::fail_connection`.
pub type AuthFailHook = std::sync::Arc<dyn Fn(&ConnectionSelector, u32) + Send + Sync>;

/// The message handler — port of `SyncerWsMessageHandler`.
///
/// Routes upstream messages to ViewSyncer, Mutagen, Pusher.
pub struct SyncerWsMessageHandler {
    /// `Rc`, not `Arc` (L9 Stage 3d): the live dispatch is the CG-local
    /// `CgViewSyncer` (`!Send`); the handler lives and dies on the CG task.
    view_syncer: Rc<dyn ViewSyncerDispatch>,
    conn_context_manager: Arc<dyn ConnContextManagerDispatch>,
    mutagen: Option<Arc<dyn MutagenDispatch>>,
    pusher: Option<Arc<dyn PusherDispatch>>,
    /// Per-connection mutation lock (ordering within a connection).
    mutation_lock: Mutex<()>,
    client_group_id: String,
    connection_selector: ConnectionSelector,
    /// Raw auth/header material for this connection, forwarded on a relayed
    /// push so the TS endpoint can rebuild the `userPushURL` request.
    push_relay_headers: PushRelayHeaders,
    /// Live-instance census guard (leak hunt): inc on construct, dec on drop.
    _census: crate::live_count::Guard,
}

impl SyncerWsMessageHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        view_syncer: Rc<dyn ViewSyncerDispatch>,
        conn_context_manager: Arc<dyn ConnContextManagerDispatch>,
        mutagen: Option<Arc<dyn MutagenDispatch>>,
        pusher: Option<Arc<dyn PusherDispatch>>,
        client_group_id: String,
        client_id: String,
        ws_id: String,
        push_relay_headers: PushRelayHeaders,
    ) -> Self {
        Self {
            view_syncer,
            conn_context_manager,
            mutagen,
            pusher,
            mutation_lock: Mutex::new(()),
            client_group_id,
            connection_selector: ConnectionSelector { client_id, ws_id },
            push_relay_headers,
            _census: crate::live_count::Guard::new(&crate::live_count::WS_MESSAGE_HANDLER),
        }
    }

    /// The relay headers with `auth` (+ its revision) filled FRESH from the
    /// connection context manager for this relay — TS reads
    /// `mustGetConnectionContext(selector).auth` per push (pusher.ts), so a
    /// token refreshed mid-session via `updateAuth` is always current. The
    /// static cookie/origin/request_headers/push_override are carried from the
    /// connect-time base (TS also fixes these per connection). `Err` = no
    /// context registered — the TS mustGet throw; callers surface it, never
    /// relay headerless.
    fn relay_headers_for(
        &self,
        selector: &ConnectionSelector,
    ) -> Result<PushRelayHeaders, Box<ErrorBody>> {
        let ctx = self
            .conn_context_manager
            .must_get_connection_context(selector)?;
        let mut headers = self.push_relay_headers.clone();
        headers.auth = ctx.auth;
        headers.revision = ctx.revision;
        Ok(headers)
    }
}

#[async_trait::async_trait(?Send)]
impl MessageHandler for SyncerWsMessageHandler {
    async fn handle_message(&self, msg: &str) -> Vec<HandlerResult> {
        let parsed = match protocol::parse_upstream(msg) {
            Ok(m) => m,
            Err(e) => {
                return vec![HandlerResult::Fatal {
                    error: ErrorBody::invalid_message(e.to_string()),
                }];
            }
        };

        let selector = &self.connection_selector;

        match parsed {
            Upstream::Ping => {
                tracing::error!("Ping is not supported at this layer by Zero");
                vec![HandlerResult::Ok]
            }

            Upstream::Pull(_) => {
                tracing::error!("Pull is not supported by Zero");
                vec![HandlerResult::Ok]
            }

            Upstream::Push(body) => {
                vec![self.handle_push(selector, &body, msg)]
            }

            Upstream::ChangeDesiredQueries(body) => {
                // `with_traceparent` only logs the traceparent before running
                // the body; the dispatch itself is awaited inline (the closure
                // cannot hold an await).
                with_traceparent(body.traceparent.as_deref(), || {});
                self.view_syncer.change_desired_queries(selector, msg).await;
                vec![HandlerResult::Ok]
            }

            Upstream::UpdateAuth(_) => {
                let body_value: serde_json::Value = {
                    let arr: Vec<serde_json::Value> = serde_json::from_str(msg).unwrap_or_default();
                    arr.get(1).cloned().unwrap_or(serde_json::Value::Null)
                };
                // TS reads mustGetConnectionContext before updateAuth (revision
                // compare); advisory here — a missing context surfaces via
                // update_auth itself, so the Result is intentionally discarded.
                let _ = self
                    .conn_context_manager
                    .must_get_connection_context(selector);
                let auth_revision_changed =
                    self.conn_context_manager.update_auth(selector, &body_value);
                self.view_syncer
                    .update_auth(selector, msg, auth_revision_changed)
                    .await;
                vec![HandlerResult::Ok]
            }

            Upstream::DeleteClients(_) => {
                let deleted_client_ids = self.view_syncer.delete_clients(selector, msg).await;
                if let Some(pusher) = &self.pusher
                    && !deleted_client_ids.is_empty()
                {
                    // TS deleteClientMutations uses the SOFT read
                    // (`getConnectionContext`, pusher.ts:181) and skips the
                    // cleanup when the context is gone — mirror: skip, never
                    // relay headerless.
                    if let Ok(headers) = self.relay_headers_for(selector) {
                        pusher.delete_client_mutations(
                            selector,
                            &deleted_client_ids,
                            &headers,
                            &self.client_group_id,
                        );
                    }
                }
                vec![HandlerResult::Ok]
            }

            Upstream::InitConnection(_) => {
                // This arm IS the production dispatch (L9 Stage 3d removed the
                // CG-thread interception): `connContextManager.initConnection`
                // records the connection context, then the ViewSyncer dispatch
                // runs the config/hydrate pass inline on the CG task.
                let body_value: serde_json::Value = {
                    let arr: Vec<serde_json::Value> = serde_json::from_str(msg).unwrap_or_default();
                    arr.get(1).cloned().unwrap_or(serde_json::Value::Null)
                };
                self.conn_context_manager
                    .init_connection(selector, &body_value);

                let traceparent = body_value
                    .get("traceparent")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                with_traceparent(traceparent.as_deref(), || {});
                let accepted = self.view_syncer.init_connection(selector, msg).await;

                if accepted && let Some(pusher) = &self.pusher {
                    pusher.init_connection(selector);
                }
                // The TS code returns stream results here. In Rust, the stream
                // is implicit — the ViewSyncer writes directly to the sink.
                vec![HandlerResult::Ok]
            }

            Upstream::CloseConnection => {
                // Deprecated, no-op.
                vec![HandlerResult::Ok]
            }

            Upstream::Inspect(_) => {
                self.view_syncer.inspect(selector, msg).await;
                vec![HandlerResult::Ok]
            }

            Upstream::AckMutationResponses(body) => {
                if let Some(pusher) = &self.pusher {
                    let body_value = serde_json::to_value(&body).unwrap_or(serde_json::Value::Null);
                    // TS ackMutationResponses uses the SOFT read
                    // (`getConnectionContext`, pusher.ts:120) and silently
                    // skips the fire-and-forget cleanup when the context is
                    // gone — mirror that: skip, never relay headerless.
                    if let Ok(headers) = self.relay_headers_for(selector) {
                        pusher.ack_mutation_responses(
                            selector,
                            &body_value,
                            &headers,
                            &self.client_group_id,
                        );
                    }
                }
                vec![HandlerResult::Ok]
            }
        }
    }
}

impl SyncerWsMessageHandler {
    /// Handle a `push` message.
    ///
    /// Port of the `push` case in `SyncerWsMessageHandler.handleMessage()`.
    fn handle_push(
        &self,
        selector: &ConnectionSelector,
        body: &PushBody,
        raw_msg: &str,
    ) -> HandlerResult {
        with_traceparent(body.traceparent.as_deref(), || {
            // Validate clientGroupID.
            if body.client_group_id != self.client_group_id {
                return HandlerResult::Fatal {
                    error: ErrorBody::invalid_push(format!(
                        "clientGroupID in mutation \"{}\" does not match clientGroupID of connection \"{}\"",
                        body.client_group_id, self.client_group_id
                    )),
                };
            }

            // Empty mutations — no-op.
            if body.mutations.is_empty() {
                return HandlerResult::Ok;
            }

            // Determine mutation type from the first mutation.
            let first_mutation = &body.mutations[0];
            let is_custom = first_mutation.get("type").and_then(|v| v.as_str()) == Some("custom");

            if is_custom {
                // Custom mutation → relay to the TS push endpoint (which runs
                // the real pusher → `userPushURL`). The Rust syncer runs zero
                // mutation logic: it only forwards the push body plus this
                // connection's auth/header material. The mutation RESULT flows
                // back to the client through the CVR's `lmids`/`mutationResults`
                // queries this syncer already hydrates and pokes — no WS
                // response needed here.
                if let Some(pusher) = &self.pusher {
                    let body_value: serde_json::Value = serde_json::from_str(raw_msg)
                        .ok()
                        .and_then(|arr: Vec<serde_json::Value>| arr.into_iter().nth(1))
                        .unwrap_or(serde_json::Value::Null);
                    // TS enqueuePush reads `mustGetConnectionContext(selector)`
                    // (pusher.ts:107) — a missing context THROWS and the
                    // view-syncer fails the connection with the error. Mirror:
                    // Fatal with the mustGet error; never relay headerless.
                    let headers = match self.relay_headers_for(selector) {
                        Ok(h) => h,
                        Err(error) => return HandlerResult::Fatal { error: *error },
                    };
                    return pusher.enqueue_push(
                        selector,
                        &body_value,
                        &headers,
                        &self.client_group_id,
                    );
                }
                // No relay endpoint configured → the sync connection stays
                // read-only. Surface a clear error but keep the (read)
                // connection open rather than tearing it down.
                return HandlerResult::Transient {
                    errors: vec![ErrorBody::invalid_push(
                        "This server does not process mutations over the sync connection. \
                         Configure the push relay (PUSHER_URL) or enable direct mutations \
                         on the client so mutations POST to your API server.",
                    )],
                };
            }

            // CRUD mutation → forward to mutagen.
            let mutagen = match &self.mutagen {
                Some(m) => m,
                None => {
                    return HandlerResult::Fatal {
                        error: ErrorBody::invalid_push(
                            "Support for legacy CRUD mutations is disabled",
                        ),
                    };
                }
            };

            // Get auth from connection context — must semantics (TS mustGet
            // throws; the connection is failed with the error).
            let conn_ctx = match self
                .conn_context_manager
                .must_get_connection_context(selector)
            {
                Ok(ctx) => ctx,
                Err(error) => return HandlerResult::Fatal { error: *error },
            };
            // Port of TS `assert(auth?.type !== 'opaque', 'Only JWT auth is
            // supported for CRUD mutations')` (syncer-ws-message-handler.ts:
            // 152-155). The TS assert THROW closes the connection with an
            // Internal error; absent auth passes (undefined !== 'opaque').
            if conn_ctx.is_opaque {
                return HandlerResult::Fatal {
                    error: ErrorBody::internal("Only JWT auth is supported for CRUD mutations"),
                };
            }

            // Process mutations under the connection-level lock. The lock only
            // serializes (it guards `()`), so recovering from a poisoned mutex is
            // safe and avoids turning one panicked batch into a dead connection.
            let _lock =
                crate::services::view_syncer::view_syncer::lock_unpoisoned(&self.mutation_lock);

            let mut errors: Vec<ErrorBody> = Vec::new();
            let auth_value = conn_ctx
                .auth
                .as_ref()
                .map(|s| serde_json::json!({"token": s}));

            for mutation in &body.mutations {
                if let Some((kind, message)) =
                    mutagen.process_mutation(mutation, auth_value.as_ref(), self.pusher.is_some())
                {
                    errors.push(ErrorBody::Basic(protocol::BasicErrorBody {
                        kind,
                        message,
                        origin: Some(ErrorOrigin::ZeroCache),
                    }));
                }
            }

            if errors.is_empty() {
                HandlerResult::Ok
            } else {
                HandlerResult::Transient { errors }
            }
        })
    }
}
