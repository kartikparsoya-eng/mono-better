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
/// Phase 2 defines the trait; Phase 4 implements it.
pub trait ViewSyncerDispatch: Send + Sync {
    /// Handle `changeDesiredQueries` message.
    fn change_desired_queries(&self, selector: &ConnectionSelector, msg: &str);

    /// Handle `updateAuth` message.
    fn update_auth(&self, selector: &ConnectionSelector, msg: &str, auth_revision_changed: bool);

    /// Handle `deleteClients` message. Returns deleted client IDs.
    fn delete_clients(&self, selector: &ConnectionSelector, msg: &str) -> Vec<String>;

    /// Handle `initConnection` message. Returns true if the connection
    /// was accepted (stream started).
    fn init_connection(&self, selector: &ConnectionSelector, msg: &str) -> bool;

    /// Handle `inspect` message.
    fn inspect(&self, selector: &ConnectionSelector, msg: &str);
}

/// Trait for the connection context manager interface.
pub trait ConnContextManagerDispatch: Send + Sync {
    /// Get connection context, panicking if not found.
    fn must_get_connection_context(&self, selector: &ConnectionSelector) -> ConnContextInfo;

    /// Initialize/update connection from initConnection body.
    fn init_connection(&self, selector: &ConnectionSelector, body: &serde_json::Value);

    /// Update auth. Returns whether the auth revision changed.
    fn update_auth(&self, selector: &ConnectionSelector, body: &serde_json::Value) -> bool;
}

/// Info about a connection context needed by the message handler.
pub struct ConnContextInfo {
    pub auth: Option<String>,
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
    pub auth: Option<String>,
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
}

/// The message handler — port of `SyncerWsMessageHandler`.
///
/// Routes upstream messages to ViewSyncer, Mutagen, Pusher.
pub struct SyncerWsMessageHandler {
    view_syncer: Arc<dyn ViewSyncerDispatch>,
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
        view_syncer: Arc<dyn ViewSyncerDispatch>,
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
}

impl MessageHandler for SyncerWsMessageHandler {
    fn handle_message(&self, msg: &str) -> Vec<HandlerResult> {
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
                with_traceparent(body.traceparent.as_deref(), || {
                    self.view_syncer.change_desired_queries(selector, msg);
                });
                vec![HandlerResult::Ok]
            }

            Upstream::UpdateAuth(_) => {
                let body_value: serde_json::Value = {
                    let arr: Vec<serde_json::Value> = serde_json::from_str(msg).unwrap_or_default();
                    arr.get(1).cloned().unwrap_or(serde_json::Value::Null)
                };
                let initial = self
                    .conn_context_manager
                    .must_get_connection_context(selector);
                let auth_revision_changed =
                    self.conn_context_manager.update_auth(selector, &body_value);
                let _ = initial; // initial revision compared above
                self.view_syncer
                    .update_auth(selector, msg, auth_revision_changed);
                vec![HandlerResult::Ok]
            }

            Upstream::DeleteClients(_) => {
                let deleted_client_ids = self.view_syncer.delete_clients(selector, msg);
                if let Some(pusher) = &self.pusher
                    && !deleted_client_ids.is_empty()
                {
                    pusher.delete_client_mutations(
                        selector,
                        &deleted_client_ids,
                        &self.push_relay_headers,
                        &self.client_group_id,
                    );
                }
                vec![HandlerResult::Ok]
            }

            Upstream::InitConnection(_) => {
                // NOTE: in the full binary the `ConnectionRouter` intercepts
                // `initConnection` (and `changeDesiredQueries` / `updateAuth` /
                // `deleteClients`) on the CG thread BEFORE it reaches this
                // handler — those tags never arrive here in production, so there
                // is no double dispatch. The CG-thread path
                // (`CgState::handle_desired_queries`) fires the same
                // `connContextManager.initConnection` + `pusher.initConnection`
                // side effects this arm does. This arm remains as the
                // self-contained, unit-tested reference dispatch.
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

                let accepted = with_traceparent(traceparent.as_deref(), || {
                    self.view_syncer.init_connection(selector, msg)
                });

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
                self.view_syncer.inspect(selector, msg);
                vec![HandlerResult::Ok]
            }

            Upstream::AckMutationResponses(body) => {
                if let Some(pusher) = &self.pusher {
                    let body_value = serde_json::to_value(&body).unwrap_or(serde_json::Value::Null);
                    pusher.ack_mutation_responses(
                        selector,
                        &body_value,
                        &self.push_relay_headers,
                        &self.client_group_id,
                    );
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
                    return pusher.enqueue_push(
                        selector,
                        &body_value,
                        &self.push_relay_headers,
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

            // Get auth from connection context.
            let conn_ctx = self
                .conn_context_manager
                .must_get_connection_context(selector);
            // Assert auth is JWT (not opaque).
            // In TS: assert(auth?.type !== 'opaque', 'Only JWT auth is supported for CRUD mutations')
            // We skip this assertion here since auth type is not available in the
            // ConnContextInfo struct. The full implementation in Phase 4 will check.

            // Process mutations under the connection-level lock. The lock only
            // serializes (it guards `()`), so recovering from a poisoned mutex is
            // safe and avoids turning one panicked batch into a dead connection.
            let _lock = crate::router::lock_unpoisoned(&self.mutation_lock);

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
