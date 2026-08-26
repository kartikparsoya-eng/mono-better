//! Connection Context Manager — port of `connection-context-manager.ts`.
//!
//! ## STATUS: REFERENCE IMPLEMENTATION — NOT WIRED INTO PRODUCTION
//!
//! Production installs `PlaceholderConnContextManager` (see `main.rs`); the
//! live auth model is the simplified per-CG state in `CgState`
//! (`pinned_user_id` / `client_raw_auth` + the folded revalidate/retransform
//! tick in `router.rs`). This module is the full TS state machine kept as a
//! tested reference for a future promotion — behavior changes to auth
//! maintenance belong in `router.rs`, NOT here.
//!
//! State machine for the auth state of a single `ViewSyncerService` (one CG).
//! Connections are registered as `provisional`, optionally backfilled with
//! `initConnection` metadata, and then promoted to `validated` once their
//! effective `userID` is confirmed as valid. The manager also tracks which
//! validated connection currently serves as the group's background connection.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::protocol::{ErrorBody, ErrorKind, ErrorOrigin};

// ─── Types ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionState {
    Provisional,
    Validated,
}

/// Normalized user identity. `None` means logged out.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserState {
    pub id: Option<String>,
}

/// Delineates the two paths for validating a connection.
#[derive(Debug, Clone)]
pub enum ConnectionValidation {
    ClientFallback,
    ServerValidated { validated_user_id: Option<String> },
}

/// Identifies one live websocket for a client slot.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectionSelector {
    #[serde(rename = "clientID")]
    pub client_id: String,
    #[serde(rename = "wsID")]
    pub ws_id: String,
}

/// NOTE: the outgoing query-API request headers (including the #6144
/// incoming-request-header forwarding) are actually built by `router.rs`
/// (`default_query_context` + `filtered_query_headers`) directly from
/// [`FetchConfig`] and [`ConnectParams`]. This `HeaderOptions`/`build_fetch_context`
/// port of `connection-context-manager.ts` is retained for structural parity but
/// is not on the runtime fetch path, so it keeps the pre-#6144 `allowed_client_headers`
/// shape rather than the renamed `requestHeaders` record.
#[derive(Debug, Clone, Default)]
pub struct HeaderOptions {
    pub api_key: Option<String>,
    pub custom_headers: Option<HashMap<String, String>>,
    pub allowed_client_headers: Option<Vec<String>>,
    pub cookie: Option<String>,
    pub origin: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct ConnectionFetchContext {
    pub url: Option<String>,
    pub allowed_url_patterns: Option<Vec<String>>, // compiled patterns stored as strings
    pub header_options: HeaderOptions,
}

/// Auth types — port of `auth.ts` types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Auth {
    Opaque { raw: String },
    Jwt { raw: String, decoded: JwtPayload },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct JwtPayload {
    pub sub: Option<String>,
    pub iat: Option<u64>,
}

/// A snapshot of one live connection tracked by the manager.
#[derive(Debug, Clone)]
pub struct ConnectionContext {
    pub state: ConnectionState,
    pub client_id: String,
    pub ws_id: String,
    pub user: UserState,
    pub auth: Option<Auth>,
    pub profile_id: Option<String>,
    pub base_cookie: Option<String>,
    pub protocol_version: u32,
    pub revision: u32,
    pub revalidate_at: Option<i64>,
    pub insertion_order: u32,
    pub query_context: ConnectionFetchContext,
    pub mutate_context: ConnectionFetchContext,
}

/// Group-scoped auth state shared across live connections.
#[derive(Debug, Clone, Default)]
pub struct GroupAuthState {
    pub pinned_user: Option<UserState>,
    pub background_connection: Option<ConnectionSelector>,
    pub retransform_at: Option<i64>,
    pub maintenance_not_before_at: Option<i64>,
}

/// Connect params needed for registration.
#[derive(Debug, Clone)]
pub struct ConnectParamsForRegistration {
    pub client_id: String,
    pub ws_id: String,
    pub user_id: Option<String>,
    pub profile_id: Option<String>,
    pub base_cookie: Option<String>,
    pub protocol_version: u32,
    pub http_cookie: Option<String>,
    pub origin: Option<String>,
}

/// Fetch config for query/push endpoints.
#[derive(Debug, Clone, Default)]
pub struct FetchConfig {
    pub url: Option<Vec<String>>,
    pub api_key: Option<String>,
    /// Allowlist for headers provided in the client's `initConnection` options
    /// (`userQueryHeaders` / `userPushHeaders`).
    pub allowed_client_headers: Option<Vec<String>>,
    /// Allowlist for headers forwarded from the incoming HTTP request (e.g.
    /// `x-forwarded-for`). Port of the `query-/mutate-allowed-request-headers`
    /// config added in zero/v1.9.0 (#6144).
    pub allowed_request_headers: Option<Vec<String>>,
    pub forward_cookies: bool,
}

/// InitConnection body fields.
#[derive(Debug, Clone, Default)]
pub struct InitConnectionBody {
    pub user_query_url: Option<String>,
    pub user_query_headers: Option<HashMap<String, String>>,
    pub user_push_url: Option<String>,
    pub user_push_headers: Option<HashMap<String, String>>,
}

/// UpdateAuth body.
#[derive(Debug, Clone)]
pub struct UpdateAuthBody {
    pub auth: Option<String>, // None or empty = clearing auth
}

// ─── Error type ────────────────────────────────────────────────────────────

/// Errors from the connection context manager.
/// In TS these are `ProtocolErrorWithLevel` thrown exceptions.
/// In Rust we return them as `Result` variants.
#[derive(Debug, Clone)]
pub enum CCMError {
    /// `ProtocolError(InvalidConnectionRequest, ...)` — connection not found.
    InvalidConnectionRequest(String),
    /// `ProtocolError(Unauthorized, ...)` — auth/userID mismatch.
    Unauthorized(String),
    /// `ProtocolError(AuthInvalidated, ...)` — JWT validation failure.
    AuthInvalidated(String),
}

impl CCMError {
    pub fn to_error_body(&self) -> ErrorBody {
        match self {
            CCMError::InvalidConnectionRequest(msg) => {
                ErrorBody::Basic(crate::protocol::BasicErrorBody {
                    kind: ErrorKind::InvalidConnectionRequest,
                    message: msg.clone(),
                    origin: Some(ErrorOrigin::ZeroCache),
                })
            }
            CCMError::Unauthorized(msg) => ErrorBody::unauthorized(msg),
            CCMError::AuthInvalidated(msg) => ErrorBody::Basic(crate::protocol::BasicErrorBody {
                kind: ErrorKind::AuthInvalidated,
                message: msg.clone(),
                origin: Some(ErrorOrigin::ZeroCache),
            }),
        }
    }
}

/// Result of validation.
#[derive(Debug)]
pub struct ValidationResult {
    pub connection: ConnectionContext,
    pub group: GroupAuthState,
}

/// Result of maintenance planning.
#[derive(Debug, Default)]
pub struct MaintenancePlan {
    pub due_revalidations: Vec<ConnectionContext>,
    pub due_retransform: bool,
    pub earliest_deadline_at: Option<i64>,
}

pub type LegacyJwtValidator = dyn Fn(&str, Option<&str>) -> Result<Auth, CCMError> + Send + Sync;

// ─── Auth resolution ───────────────────────────────────────────────────────

/// Port of `resolveAuth()` from `auth.ts`.
/// Resolves one auth snapshot transition.
pub fn resolve_auth(
    previous_auth: Option<&Auth>,
    user_id: Option<&str>,
    wire_auth: Option<&str>,
    validate_legacy_jwt: Option<&LegacyJwtValidator>,
) -> Result<Option<Auth>, CCMError> {
    let has_provided_auth = wire_auth.is_some_and(|a| !a.is_empty());

    if !has_provided_auth && previous_auth.is_some() {
        return Err(CCMError::Unauthorized(
            "No token provided. An unauthenticated client cannot connect to an authenticated client group."
                .to_string(),
        ));
    }

    if !has_provided_auth {
        return Ok(None);
    }

    let wire = wire_auth.unwrap();

    if user_id.is_none() {
        return Err(CCMError::Unauthorized(
            "Authenticated connections require a userID.".to_string(),
        ));
    }

    if let Some(validate) = validate_legacy_jwt {
        let verified = validate(wire, user_id)?;
        let next = pick_token(previous_auth, &verified)?;
        return Ok(next);
    }

    // No legacy JWT validator
    if let Some(prev) = previous_auth {
        if matches!(prev, Auth::Jwt { .. }) {
            return Err(CCMError::Unauthorized(
                "Token type cannot change from JWT to opaque. Connections are pinned to a single token type."
                    .to_string(),
            ));
        }
        if let Auth::Opaque { raw } = prev
            && raw == wire
        {
            return Ok(previous_auth.cloned());
        }
    }

    Ok(Some(Auth::Opaque {
        raw: wire.to_string(),
    }))
}

/// Port of `pickToken()` from `auth.ts`.
fn pick_token(previous: Option<&Auth>, new: &Auth) -> Result<Option<Auth>, CCMError> {
    let previous = match previous {
        None => return Ok(Some(new.clone())),
        Some(p) => p,
    };

    let prev_type = match previous {
        Auth::Opaque { .. } => "opaque",
        Auth::Jwt { .. } => "jwt",
    };
    let new_type = match new {
        Auth::Opaque { .. } => "opaque",
        Auth::Jwt { .. } => "jwt",
    };

    if prev_type != new_type {
        return Err(CCMError::Unauthorized(
            "Token type cannot change. Client groups are pinned to a single token type."
                .to_string(),
        ));
    }

    if let Auth::Opaque { .. } = new {
        return Ok(Some(new.clone()));
    }

    // Both are JWT
    let Auth::Jwt {
        decoded: prev_decoded,
        ..
    } = previous
    else {
        unreachable!()
    };
    let Auth::Jwt {
        decoded: new_decoded,
        ..
    } = new
    else {
        unreachable!()
    };

    if prev_decoded.sub != new_decoded.sub {
        return Err(CCMError::Unauthorized(
            "The user id in the new token does not match the previous token. Client groups are pinned to a single user."
                .to_string(),
        ));
    }

    match (prev_decoded.iat, new_decoded.iat) {
        (None, _) => Ok(Some(new.clone())),
        (Some(_), None) => Err(CCMError::Unauthorized(
            "The new token does not have an issued at time but the prior token does. Tokens for a client group must either all have issued at times or all not have issued at times".to_string(),
        )),
        (Some(prev_iat), Some(new_iat)) => {
            if new_iat > prev_iat {
                Ok(Some(new.clone()))
            } else {
                // New token is older or the same — keep existing token.
                Ok(Some(previous.clone()))
            }
        }
    }
}

/// Port of `authEquals()` from `auth.ts`.
pub fn auth_equals(a: Option<&Auth>, b: Option<&Auth>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => {
            let a_type = match a {
                Auth::Opaque { .. } => "opaque",
                Auth::Jwt { .. } => "jwt",
            };
            let b_type = match b {
                Auth::Opaque { .. } => "opaque",
                Auth::Jwt { .. } => "jwt",
            };
            a_type == b_type && a.raw() == b.raw()
        }
        _ => false,
    }
}

impl Auth {
    pub fn raw(&self) -> &str {
        match self {
            Auth::Opaque { raw } => raw,
            Auth::Jwt { raw, .. } => raw,
        }
    }
}

// ─── ConnectionContextManager ──────────────────────────────────────────────

pub struct ConnectionContextManager {
    connections: HashMap<String, ConnectionContext>,
    group: GroupAuthState,
    validate_legacy_jwt: Option<Box<LegacyJwtValidator>>,
    now: Box<dyn Fn() -> i64 + Send + Sync>,
    revalidate_interval_ms: Option<i64>,
    retransform_interval_ms: Option<i64>,
    query_config: Option<FetchConfig>,
    push_config: Option<FetchConfig>,
    shared_retransform_ready: bool,
    next_insertion_order: u32,
}

impl ConnectionContextManager {
    pub fn new(
        revalidate_interval_seconds: Option<u64>,
        retransform_interval_seconds: Option<u64>,
        query_config: Option<FetchConfig>,
        push_config: Option<FetchConfig>,
        validate_legacy_jwt: Option<Box<LegacyJwtValidator>>,
        now: Option<Box<dyn Fn() -> i64 + Send + Sync>>,
    ) -> Self {
        Self {
            connections: HashMap::new(),
            group: GroupAuthState::default(),
            validate_legacy_jwt,
            now: now.unwrap_or(Box::new(now_ms)),
            revalidate_interval_ms: revalidate_interval_seconds.map(|s| (s as i64) * 1000),
            retransform_interval_ms: retransform_interval_seconds.map(|s| (s as i64) * 1000),
            query_config,
            push_config,
            shared_retransform_ready: false,
            next_insertion_order: 0,
        }
    }

    fn now(&self) -> i64 {
        (self.now)()
    }

    // ─── 5.2 registerConnection ────────────────────────────────────────────

    pub fn register_connection(
        &mut self,
        selector: &ConnectionSelector,
        params: &ConnectParamsForRegistration,
        auth: Option<Auth>,
    ) -> ConnectionContext {
        self.remove_connection_internal(selector, None);

        let query_context = self.build_fetch_context(params, true);
        let mutate_context = self.build_fetch_context(params, false);

        self.next_insertion_order += 1;
        let connection = ConnectionContext {
            state: ConnectionState::Provisional,
            client_id: params.client_id.clone(),
            ws_id: params.ws_id.clone(),
            revision: 0,
            user: UserState {
                id: params.user_id.clone(),
            },
            auth,
            profile_id: params.profile_id.clone(),
            base_cookie: params.base_cookie.clone(),
            protocol_version: params.protocol_version,
            revalidate_at: None,
            insertion_order: self.next_insertion_order,
            query_context,
            mutate_context,
        };
        self.store_connection(connection.clone());
        self.refresh_background_connection_context(None);
        self.update_background_retransform_deadline(false);
        connection
    }

    fn build_fetch_context(
        &self,
        params: &ConnectParamsForRegistration,
        is_query: bool,
    ) -> ConnectionFetchContext {
        let config = if is_query {
            &self.query_config
        } else {
            &self.push_config
        };

        ConnectionFetchContext {
            url: config
                .as_ref()
                .and_then(|c| c.url.as_ref().and_then(|urls| urls.first().cloned())),
            allowed_url_patterns: config.as_ref().and_then(|c| c.url.clone()),
            header_options: HeaderOptions {
                custom_headers: None,
                origin: params.origin.clone(),
                api_key: config.as_ref().and_then(|c| c.api_key.clone()),
                allowed_client_headers: config
                    .as_ref()
                    .and_then(|c| c.allowed_client_headers.clone()),
                cookie: if config.as_ref().is_some_and(|c| c.forward_cookies) {
                    params.http_cookie.clone()
                } else {
                    None
                },
            },
        }
    }

    // ─── 5.3 initConnection ────────────────────────────────────────────────

    pub fn init_connection(
        &mut self,
        selector: &ConnectionSelector,
        body: &InitConnectionBody,
    ) -> Result<ConnectionContext, CCMError> {
        let mut connection = self.must_get_connection_context(selector)?;

        if let Some(ref url) = body.user_query_url {
            connection.query_context.url = Some(url.clone());
        }
        if let Some(ref headers) = body.user_query_headers {
            connection.query_context.header_options.custom_headers = Some(headers.clone());
        }
        if let Some(ref url) = body.user_push_url {
            connection.mutate_context.url = Some(url.clone());
        }
        if let Some(ref headers) = body.user_push_headers {
            connection.mutate_context.header_options.custom_headers = Some(headers.clone());
        }

        connection.revision += 1;
        Ok(self.demote_connection(connection))
    }

    // ─── 5.4 updateAuth ────────────────────────────────────────────────────

    pub fn update_auth(
        &mut self,
        selector: &ConnectionSelector,
        body: &UpdateAuthBody,
    ) -> Result<ConnectionContext, CCMError> {
        let connection = self.must_get_connection_context(selector)?;

        let wire_auth = body.auth.as_deref();
        let next_auth = resolve_auth(
            connection.auth.as_ref(),
            connection.user.id.as_deref(),
            wire_auth,
            self.validate_legacy_jwt.as_deref(),
        )?;

        let auth_changed = !auth_equals(connection.auth.as_ref(), next_auth.as_ref());
        if auth_changed {
            let mut updated = connection.clone();
            updated.auth = next_auth;
            updated.revision += 1;
            return Ok(self.demote_connection(updated));
        }

        // Same identity but different object — store the new one
        if next_auth.as_ref() != connection.auth.as_ref() {
            let mut updated = connection.clone();
            updated.auth = next_auth;
            return Ok(self.store_connection(updated));
        }

        Ok(connection)
    }

    // ─── 5.5 validateConnection ────────────────────────────────────────────

    pub fn validate_connection(
        &mut self,
        selector: &ConnectionSelector,
        revision: u32,
        validation: &ConnectionValidation,
    ) -> Result<Option<ValidationResult>, CCMError> {
        let connection = match self.get_connection_context(selector) {
            Some(c) => c,
            None => return Ok(None),
        };

        if connection.revision != revision {
            tracing::debug!(
                "Skipping validateConnection for stale revision: {:?} attempted={} current={}",
                selector,
                revision,
                connection.revision
            );
            return Ok(None);
        }

        let mut validated_user_state: Option<UserState> = None;

        if let ConnectionValidation::ServerValidated { validated_user_id } = validation {
            validated_user_state = Some(UserState {
                id: validated_user_id.clone(),
            });
            if connection.user.id != validated_user_state.as_ref().unwrap().id {
                return Err(CCMError::Unauthorized(
                    "Connection userID does not match validated server userID.".to_string(),
                ));
            }
        }

        let incoming_user_state = validated_user_state.unwrap_or_else(|| connection.user.clone());

        if let Some(ref pinned) = self.group.pinned_user
            && pinned.id != incoming_user_state.id
        {
            return Err(CCMError::Unauthorized(
                    "Client groups are pinned to a single userID. Connection userID does not match existing client group userID."
                        .to_string(),
                ));
        }

        if self.group.pinned_user.is_none() {
            self.group.pinned_user = Some(incoming_user_state);
        }

        let mut validated = connection.clone();
        validated.state = ConnectionState::Validated;
        validated.revalidate_at = self.next_revalidate_at();
        self.store_connection(validated.clone());
        self.refresh_background_connection_context(Some(&validated));
        self.update_background_retransform_deadline(false);

        Ok(Some(ValidationResult {
            connection: validated,
            group: self.group.clone(),
        }))
    }

    // ─── 5.6 failConnection ────────────────────────────────────────────────

    pub fn fail_connection(
        &mut self,
        selector: &ConnectionSelector,
        revision: u32,
    ) -> Option<ConnectionContext> {
        self.remove_connection_internal(selector, Some(revision))
    }

    // ─── 5.7 closeConnection ───────────────────────────────────────────────

    pub fn close_connection(&mut self, selector: &ConnectionSelector) -> Option<ConnectionContext> {
        self.remove_connection_internal(selector, None)
    }

    // ─── 5.8 markBackgroundRetransformSuccess ──────────────────────────────

    pub fn mark_background_retransform_success(
        &mut self,
        selector: &ConnectionSelector,
        revision: u32,
    ) {
        let bg = match self.get_background_connection_context() {
            Some(c) => c,
            None => return,
        };
        if bg.client_id != selector.client_id
            || bg.ws_id != selector.ws_id
            || bg.revision != revision
        {
            return;
        }
        self.update_background_retransform_deadline(true);
    }

    // ─── 5.9 setSharedRetransformReady ─────────────────────────────────────

    pub fn set_shared_retransform_ready(&mut self, ready: bool) {
        if self.shared_retransform_ready == ready {
            return;
        }
        self.shared_retransform_ready = ready;
        self.update_background_retransform_deadline(true);
    }

    // ─── 5.10 deferMaintenance ─────────────────────────────────────────────

    pub fn defer_maintenance(&mut self, kind: MaintenanceKind) {
        let interval_ms = match kind {
            MaintenanceKind::Revalidate => self.revalidate_interval_ms,
            MaintenanceKind::Retransform => self.retransform_interval_ms,
        };
        let interval_ms = match interval_ms {
            Some(v) => v,
            None => return,
        };
        let now = self.now();
        let current = self.group.maintenance_not_before_at.unwrap_or(0);
        self.group.maintenance_not_before_at = Some(current.max(now + interval_ms));
    }

    // ─── Getters ───────────────────────────────────────────────────────────

    pub fn get_connection_context(
        &self,
        selector: &ConnectionSelector,
    ) -> Option<ConnectionContext> {
        let connection = self.connections.get(&selector.client_id)?;
        if connection.ws_id != selector.ws_id {
            return None;
        }
        Some(connection.clone())
    }

    pub fn must_get_connection_context(
        &self,
        selector: &ConnectionSelector,
    ) -> Result<ConnectionContext, CCMError> {
        self.get_connection_context(selector).ok_or_else(|| {
            CCMError::InvalidConnectionRequest(
                "Connection auth state was not available for this websocket.".to_string(),
            )
        })
    }

    pub fn get_background_connection_context(&self) -> Option<ConnectionContext> {
        let bg = self.group.background_connection.as_ref()?;
        self.get_connection_context(bg)
    }

    pub fn must_get_background_connection_context(&self) -> Result<ConnectionContext, CCMError> {
        self.get_background_connection_context().ok_or_else(|| {
            CCMError::InvalidConnectionRequest(
                "No validated connection is available for shared query work.".to_string(),
            )
        })
    }

    pub fn get_group_state(&self) -> &GroupAuthState {
        &self.group
    }

    // ─── 5.11 planMaintenance ──────────────────────────────────────────────

    pub fn plan_maintenance(&self) -> MaintenancePlan {
        let mut due_revalidations = Vec::new();
        let now = self.now();
        let mut earliest_deadline_at = self.group.retransform_at;

        for connection in self.connections.values() {
            if connection.state != ConnectionState::Validated || connection.revalidate_at.is_none()
            {
                continue;
            }
            let revalidate_at = connection.revalidate_at.unwrap();
            if revalidate_at <= now {
                due_revalidations.push(connection.clone());
            }
            earliest_deadline_at = min_defined(earliest_deadline_at, Some(revalidate_at));
        }

        let due_retransform = self.group.retransform_at.is_some_and(|at| at <= now);

        let maintenance_not_before_at = self.group.maintenance_not_before_at;

        if let Some(not_before) = maintenance_not_before_at
            && not_before > now
            && earliest_deadline_at.is_some()
        {
            return MaintenancePlan {
                due_revalidations: Vec::new(),
                due_retransform: false,
                earliest_deadline_at: Some(earliest_deadline_at.unwrap().max(not_before)),
            };
        }

        due_revalidations.sort_by(compare_by_insertion_order);

        MaintenancePlan {
            due_revalidations,
            due_retransform,
            earliest_deadline_at,
        }
    }

    // ─── Internal helpers ──────────────────────────────────────────────────

    fn store_connection(&mut self, connection: ConnectionContext) -> ConnectionContext {
        self.connections
            .insert(connection.client_id.clone(), connection.clone());
        connection
    }

    fn remove_connection_internal(
        &mut self,
        selector: &ConnectionSelector,
        revision: Option<u32>,
    ) -> Option<ConnectionContext> {
        let connection = self.get_connection_context(selector)?;

        if let Some(rev) = revision
            && connection.revision != rev
        {
            tracing::debug!(
                "Ignoring removeConnection for stale revision: {:?} attempted={} current={}",
                selector,
                rev,
                connection.revision
            );
            return None;
        }

        self.connections.remove(&connection.client_id);
        self.refresh_background_connection_context(None);
        self.update_background_retransform_deadline(false);
        Some(connection)
    }

    fn demote_connection(&mut self, connection: ConnectionContext) -> ConnectionContext {
        let mut demoted = connection;
        demoted.state = ConnectionState::Provisional;
        demoted.revalidate_at = None;
        let result = self.store_connection(demoted);
        self.refresh_background_connection_context(None);
        self.update_background_retransform_deadline(false);
        result
    }

    // ─── 5.12 refreshBackgroundConnectionContext ───────────────────────────

    fn refresh_background_connection_context(&mut self, preferred: Option<&ConnectionContext>) {
        if let Some(preferred) = preferred
            && preferred.state == ConnectionState::Validated
        {
            let current_bg = self.get_background_connection_context();
            if let Some(ref bg) = current_bg
                && bg.client_id == preferred.client_id
                && bg.ws_id == preferred.ws_id
            {
                return;
            }
            if current_bg.is_some() {
                return;
            }
            self.set_background_connection(Some(ConnectionSelector {
                client_id: preferred.client_id.clone(),
                ws_id: preferred.ws_id.clone(),
            }));
            return;
        }

        let current_bg = self.get_background_connection_context();
        if let Some(ref bg) = current_bg
            && bg.state == ConnectionState::Validated
        {
            return;
        }

        // Find newest validated connection
        let mut candidates: Vec<&ConnectionContext> = self
            .connections
            .values()
            .filter(|c| c.state == ConnectionState::Validated)
            .collect();

        candidates.sort_by(|a, b| compare_preferred_validated_connection(a, b));

        if let Some(next) = candidates.first() {
            self.set_background_connection(Some(ConnectionSelector {
                client_id: next.client_id.clone(),
                ws_id: next.ws_id.clone(),
            }));
        } else {
            self.set_background_connection(None);
        }
    }

    fn set_background_connection(&mut self, bg: Option<ConnectionSelector>) {
        let same = match (&self.group.background_connection, &bg) {
            (Some(a), Some(b)) => a.client_id == b.client_id && a.ws_id == b.ws_id,
            (None, None) => true,
            _ => false,
        };
        if same {
            return;
        }
        self.group.background_connection = bg;
    }

    // ─── 5.13 updateBackgroundRetransformDeadline ──────────────────────────

    fn update_background_retransform_deadline(&mut self, reset: bool) {
        let bg = self.get_background_connection_context();
        if bg.is_none() || self.retransform_interval_ms.is_none() || !self.shared_retransform_ready
        {
            if self.group.retransform_at.is_some() {
                self.group.retransform_at = None;
            }
            return;
        }

        if reset || self.group.retransform_at.is_none() {
            self.group.retransform_at = Some(self.now() + self.retransform_interval_ms.unwrap());
        }
    }

    fn next_revalidate_at(&self) -> Option<i64> {
        self.revalidate_interval_ms.map(|ms| self.now() + ms)
    }
}

// ─── Utility types ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
pub enum MaintenanceKind {
    Revalidate,
    Retransform,
}

// ─── Helper functions ──────────────────────────────────────────────────────

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn min_defined(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (None, b) => b,
        (a, None) => a,
        (Some(a), Some(b)) => Some(a.min(b)),
    }
}

/// Ascending by `insertion_order`, then `ws_id` ascending.
fn compare_by_insertion_order(a: &ConnectionContext, b: &ConnectionContext) -> std::cmp::Ordering {
    a.insertion_order
        .cmp(&b.insertion_order)
        .then(a.ws_id.cmp(&b.ws_id))
}

/// Descending by `insertion_order`, then `ws_id` descending.
fn compare_preferred_validated_connection(
    a: &ConnectionContext,
    b: &ConnectionContext,
) -> std::cmp::Ordering {
    b.insertion_order
        .cmp(&a.insertion_order)
        .then(b.ws_id.cmp(&a.ws_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Layer-2 body-differential: `CCMError::to_error_body` must serialize to the
    /// exact wire error body TS emits for each kind (built from the real
    /// zero-protocol `ErrorKind`/`ErrorOrigin` enums in
    /// `generate-error-body-fixture.mjs`) — pinning the kind string, the
    /// `origin: "zeroCache"` value, and the flat `{kind,message,origin}` shape.
    #[test]
    fn to_error_body_parity_against_ts() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/agentic/parity/error-body-fixture.json"
        );
        let bytes = std::fs::read(path).expect("read error-body-fixture.json");
        let cases: serde_json::Value =
            serde_json::from_slice(&bytes).expect("fixture is valid JSON");
        for case in cases.as_array().expect("fixture is an array") {
            let variant = case["variant"].as_str().expect("variant");
            let message = case["message"].as_str().expect("message").to_string();
            let err = match variant {
                "InvalidConnectionRequest" => CCMError::InvalidConnectionRequest(message),
                "Unauthorized" => CCMError::Unauthorized(message),
                "AuthInvalidated" => CCMError::AuthInvalidated(message),
                other => panic!("unknown CCMError variant in fixture: {other}"),
            };
            let got = serde_json::to_value(err.to_error_body()).expect("serialize");
            assert_eq!(got, case["body"], "to_error_body mismatch for {variant}");
        }
    }
}
