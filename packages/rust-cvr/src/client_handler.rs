//! Port of `packages/zero-cache/src/services/view-syncer/client-handler.ts`.
//!
//! The ClientHandler is the bridge between CVR state changes and a WebSocket.
//! It serializes pokes per-connection and assembles poke bodies with
//! special handling for the clients and mutations tables.
//!
//! ## Threading
//!
//! All methods are **synchronous**. The ClientHandler runs on the engine's
//! actor thread (a dedicated OS thread, not a tokio runtime). The only
//! async edge is `WebSocketSink::push`, which uses a TSFN `Blocking` call
//! that blocks the OS thread until JS processes the frame — identical
//! backpressure to TS's `#pokeTail` promise chain.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex};

use serde::Serialize;
#[cfg(test)]
use serde_json::Map;
use serde_json::Value;

use crate::types::*;
use crate::version::{CVRVersion, NullableCVRVersion, cmp_versions, version_string};
use std::cmp::Ordering;

const PART_COUNT_FLUSH_THRESHOLD: usize = 100;

/// Abstract WebSocket sink. The napi implementation proxies to TS's WS via
/// a ThreadsafeFunction with `Blocking` call mode.
pub trait WebSocketSink: Send + Sync {
    fn push(&self, msg: Value) -> Result<(), String>;
    fn fail(&self, e: String);
    fn cancel(&self);
}

// ─── Poke body types ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct QueryPatchEntry {
    pub op: String,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RowPatchOp {
    pub op: String,
    #[serde(rename = "tableName")]
    pub table_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MutationPatchEntry {
    pub op: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutation: Option<MutationPatchMutation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<MutationPatchId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MutationPatchMutation {
    pub id: MutationPatchId,
    pub result: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct MutationPatchId {
    #[serde(rename = "clientID")]
    pub client_id: String,
    pub id: i64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct PokePartBody {
    #[serde(rename = "pokeID")]
    pub poke_id: String,
    #[serde(rename = "gotQueriesPatch", skip_serializing_if = "Option::is_none")]
    pub got_queries_patch: Option<Vec<QueryPatchEntry>>,
    #[serde(
        rename = "desiredQueriesPatches",
        skip_serializing_if = "Option::is_none"
    )]
    pub desired_queries_patches: Option<BTreeMap<String, Vec<QueryPatchEntry>>>,
    #[serde(rename = "rowsPatch", skip_serializing_if = "Option::is_none")]
    pub rows_patch: Option<Vec<RowPatchOp>>,
    #[serde(
        rename = "lastMutationIDChanges",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_mutation_id_changes: Option<BTreeMap<String, i64>>,
    #[serde(rename = "mutationsPatch", skip_serializing_if = "Option::is_none")]
    pub mutations_patch: Option<Vec<MutationPatchEntry>>,
}

// ─── Poke state ────────────────────────────────────────────────────────────

struct PokeState {
    poke_id: String,
    base_cookie: Option<String>,
    started: bool,
    body: Option<PokePartBody>,
    part_count: usize,
    poke_in_progress: bool,
}

impl PokeState {
    fn new(poke_id: String, base_cookie: Option<String>) -> Self {
        Self {
            poke_id,
            base_cookie,
            started: false,
            body: None,
            part_count: 0,
            poke_in_progress: false,
        }
    }
}

// ─── PokeHandler ───────────────────────────────────────────────────────────

/// Returned by `start_poke()`. Serializes patches into poke frames.
pub struct PokeHandler {
    state: Arc<StdMutex<PokeState>>,
    downstream: Arc<dyn WebSocketSink>,
    base_version: Arc<StdMutex<NullableCVRVersion>>,
    poke_chain: Arc<AtomicBool>,
    /// Shared with the owning `ClientHandler`: set true once this client has
    /// received a completed poke. Mirrors TS ClientHandler `#everPoked`.
    ever_poked: Arc<AtomicBool>,
    /// `!ever_poked` captured at `start_poke` time (TS `forceInitialPoke`).
    /// Forces one (empty) poke on connect even when already caught up, so the
    /// client learns its got-queries state was reconciled with the server.
    force_initial_poke: bool,
    zero_clients_table: String,
    zero_mutations_table: String,
    client_group_id: String,
    /// Wall-clock start of this poke transaction — TS `const start =
    /// performance.now()` in `startPoke`. Read once in `end()` for
    /// `zero.sync.poke.time`.
    start: std::time::Instant,
    /// True for the handler returned when the client is already at/ahead of the
    /// tentative version: a genuinely inert NOOP whose `add_patch`/`end`/
    /// `cancel` do nothing, matching TS's do-nothing-methods NOOP object
    /// (client-handler.ts). Without this, an `end(final != base)` on the
    /// "Greater" case emitted a fabricated `pokeStart {baseCookie: null}` +
    /// `pokeEnd` that REGRESSED the client's cookie.
    noop: bool,
}

impl PokeHandler {
    pub fn add_patch(&self, patch_to_version: &PatchToVersion) -> Result<(), String> {
        if self.noop {
            return Ok(());
        }
        let to_version = &patch_to_version.to_version;
        let base = self.base_version.lock().unwrap();

        if cmp_versions(&Some(to_version.clone()), &base) != Ordering::Greater {
            return Ok(());
        }
        drop(base);

        let mut state = self.state.lock().unwrap();
        self.ensure_body(&mut state)?;

        let result: Result<(), String> = (|| {
            match &patch_to_version.patch {
                Patch::Query(qp) => {
                    let body = state.body.as_mut().unwrap();
                    match qp {
                        QueryPatch::Put { id, client_id } => {
                            let entry = QueryPatchEntry {
                                op: "put".to_string(),
                                hash: id.clone(),
                            };
                            match client_id {
                                Some(cid) => {
                                    let dqp = body
                                        .desired_queries_patches
                                        .get_or_insert_with(BTreeMap::new);
                                    dqp.entry(cid.clone()).or_default().push(entry);
                                }
                                None => {
                                    body.got_queries_patch
                                        .get_or_insert_with(Vec::new)
                                        .push(entry);
                                }
                            }
                        }
                        QueryPatch::Del { id, client_id } => {
                            let entry = QueryPatchEntry {
                                op: "del".to_string(),
                                hash: id.clone(),
                            };
                            match client_id {
                                Some(cid) => {
                                    let dqp = body
                                        .desired_queries_patches
                                        .get_or_insert_with(BTreeMap::new);
                                    dqp.entry(cid.clone()).or_default().push(entry);
                                }
                                None => {
                                    body.got_queries_patch
                                        .get_or_insert_with(Vec::new)
                                        .push(entry);
                                }
                            }
                        }
                    }
                }
                Patch::Row(rp) => {
                    // TS `#pokedRows.add(1)` fires for every `type === 'row'`
                    // patch delivered to the poker (client-handler.ts:297).
                    crate::otel_metrics::record_poked_row();
                    let table = match rp {
                        RowPatch::Put { id, .. } => &id.table,
                        RowPatch::Del { id } => &id.table,
                    };

                    if table == &self.zero_clients_table {
                        self.update_lmids(&mut state, rp)?;
                    } else if table == &self.zero_mutations_table {
                        self.add_mutation_patch(&mut state, rp)?;
                    } else {
                        let body = state.body.as_mut().unwrap();
                        body.rows_patch
                            .get_or_insert_with(Vec::new)
                            .push(make_row_patch(rp)?);
                    }
                }
            }

            state.part_count += 1;
            if state.part_count >= PART_COUNT_FLUSH_THRESHOLD {
                self.flush_body(&mut state)?;
            }
            Ok(())
        })();

        // Once a frame cannot be assembled or delivered, this poke is dead.
        // Match TS's per-poker addPatch wrapper (client-handler.ts:463), which
        // catches the throw and calls `downstream.fail(...)` — failing THIS
        // client's connection so it reconnects and rehydrates, rather than
        // silently dropping the row and completing the poke. (MultiPoker then
        // continues to the other clients, mirroring Promise.allSettled.)
        // Releasing the chain is essential too: a later catch-up poke shares
        // the same per-client chain and would otherwise spin in acquire_chain.
        if let Err(e) = &result {
            self.downstream.fail(e.clone());
            self.release_chain(&mut state);
        }
        result
    }

    pub fn cancel(&self) -> Result<(), String> {
        if self.noop {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        let result = if state.started {
            self.downstream.push(serde_json::json!([
                "pokeEnd",
                {"pokeID": state.poke_id, "cookie": "", "cancel": true}
            ]))
        } else {
            Ok(())
        };
        // Socket delivery errors do not change ownership of the chain.
        // Always unlock it before returning the error.
        self.release_chain(&mut state);
        result
    }

    pub fn end(&self, final_version: CVRVersion) -> Result<(), String> {
        // The NOOP handler sends nothing and must not touch `ever_poked` or
        // `base_version` — TS's NOOP `end` is an empty function.
        if self.noop {
            return Ok(());
        }
        let mut state = self.state.lock().unwrap();
        let cookie = version_string(&final_version);

        if !state.started {
            let base = self.base_version.lock().unwrap();
            // Force the initial empty poke even when nothing changed; see
            // `force_initial_poke`. Mirrors TS ClientHandler `end` (zero/v1.9.0).
            if cmp_versions(&base, &Some(final_version.clone())) == Ordering::Equal
                && !self.force_initial_poke
            {
                self.release_chain(&mut state);
                return Ok(());
            }
            drop(base);
            self.acquire_chain(&mut state);
            if let Err(error) = self.downstream.push(serde_json::json!([
                "pokeStart",
                {"pokeID": state.poke_id, "baseCookie": state.base_cookie}
            ])) {
                self.release_chain(&mut state);
                return Err(error);
            }
            state.started = true;
        } else {
            let base = self.base_version.lock().unwrap();
            if cmp_versions(&base, &Some(final_version.clone())) != Ordering::Less {
                let error = format!(
                    "Patches were sent but finalVersion {:?} is not greater than baseVersion {:?}",
                    final_version, *base
                );
                drop(base);
                self.release_chain(&mut state);
                return Err(error);
            }
            drop(base);
        }

        self.flush_body(&mut state)?;
        if let Err(error) = self.downstream.push(serde_json::json!([
            "pokeEnd",
            {"pokeID": state.poke_id, "cookie": cookie}
        ])) {
            self.release_chain(&mut state);
            return Err(error);
        }

        let mut base = self.base_version.lock().unwrap();
        *base = Some(final_version);
        drop(base);
        // TS `this.#everPoked = true` — after this, caught-up pokes NOOP again.
        self.ever_poked.store(true, AtomicOrdering::SeqCst);

        self.release_chain(&mut state);

        // OTLP: this poke transaction completed (pokeEnd pushed). Canceled/noop
        // pokes return before reaching here, matching TS `#pokeTime.recordMs` /
        // `#pokeTransactions.add(1)` at the end of `ClientHandler` `end()`.
        crate::otel_metrics::record_poke(self.start.elapsed().as_secs_f64() * 1000.0);
        Ok(())
    }

    fn ensure_body(&self, state: &mut PokeState) -> Result<(), String> {
        if !state.started {
            self.acquire_chain(state);
            if let Err(error) = self.downstream.push(serde_json::json!([
                "pokeStart",
                {"pokeID": state.poke_id, "baseCookie": state.base_cookie}
            ])) {
                self.release_chain(state);
                return Err(error);
            }
            state.started = true;
        }
        if state.body.is_none() {
            state.body = Some(PokePartBody {
                poke_id: state.poke_id.clone(),
                ..Default::default()
            });
        }
        Ok(())
    }

    fn flush_body(&self, state: &mut PokeState) -> Result<(), String> {
        if let Some(body) = state.body.take() {
            if let Err(error) = self.downstream.push(serde_json::json!(["pokePart", body])) {
                self.release_chain(state);
                return Err(error);
            }
            state.part_count = 0;
        }
        Ok(())
    }

    fn acquire_chain(&self, state: &mut PokeState) {
        while self
            .poke_chain
            .compare_exchange(false, true, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
            .is_err()
        {
            std::thread::yield_now();
        }
        state.poke_in_progress = true;
    }

    fn release_chain(&self, state: &mut PokeState) {
        if state.poke_in_progress {
            state.poke_in_progress = false;
            self.poke_chain.store(false, AtomicOrdering::SeqCst);
        }
    }

    fn update_lmids(&self, state: &mut PokeState, patch: &RowPatch) -> Result<(), String> {
        if let RowPatch::Put { id: _, contents } = patch {
            let cg = contents.get("clientGroupID").and_then(|v| v.as_str());
            let cid = contents.get("clientID").and_then(|v| v.as_str());
            let lmid = contents.get("lastMutationID").and_then(|v| v.as_i64());

            if let (Some(cg), Some(cid), Some(lmid)) = (cg, cid, lmid) {
                if cg != self.client_group_id {
                    eprintln!(
                        "Received clients row for wrong clientGroupID. Ignoring. {}",
                        cg
                    );
                } else {
                    let body = state.body.as_mut().unwrap();
                    let lmids = body
                        .last_mutation_id_changes
                        .get_or_insert_with(BTreeMap::new);
                    lmids.insert(cid.to_string(), lmid);
                }
            }
        }
        // del/constrain ops for clients are ignored
        Ok(())
    }

    fn add_mutation_patch(&self, state: &mut PokeState, patch: &RowPatch) -> Result<(), String> {
        let body = state.body.as_mut().unwrap();
        let patches = body.mutations_patch.get_or_insert_with(Vec::new);

        match patch {
            RowPatch::Put { id: _, contents } => {
                // TS: `normalizeMutationResult(ensureSafeJSON(patch.contents))`
                // (client-handler.ts:410) — the mutations path is subject to the
                // same unsafe-integer guard as the rows path.
                ensure_safe_json(contents)?;
                let normalized = normalize_mutation_result(contents);
                let client_id = normalized
                    .get("clientID")
                    .and_then(|v| v.as_str())
                    .ok_or("clientID missing in mutation row")?
                    .to_string();
                let mutation_id = normalized
                    .get("mutationID")
                    .and_then(|v| v.as_i64())
                    .ok_or("mutationID missing in mutation row")?;
                let result = normalized.get("result").cloned().unwrap_or(Value::Null);

                patches.push(MutationPatchEntry {
                    op: "put".to_string(),
                    mutation: Some(MutationPatchMutation {
                        id: MutationPatchId {
                            client_id,
                            id: mutation_id,
                        },
                        result,
                    }),
                    id: None,
                });
            }
            RowPatch::Del { id } => {
                let client_id = id
                    .row_key
                    .get("clientID")
                    .and_then(|v| v.as_str())
                    .ok_or("clientID missing in mutation rowKey")?
                    .to_string();
                let mutation_id = id
                    .row_key
                    .get("mutationID")
                    .and_then(|v| v.as_i64())
                    .ok_or("mutationID missing in rowKey")?;

                patches.push(MutationPatchEntry {
                    op: "del".to_string(),
                    mutation: None,
                    id: Some(MutationPatchId {
                        client_id,
                        id: mutation_id,
                    }),
                });
            }
        }
        Ok(())
    }
}

impl Drop for PokeHandler {
    fn drop(&mut self) {
        // If the poke was started but end() was never called,
        // release the poke chain so future pokes can proceed.
        let mut state = self.state.lock().unwrap();
        if state.poke_in_progress {
            state.poke_in_progress = false;
            self.poke_chain.store(false, AtomicOrdering::SeqCst);
        }
    }
}

/// Defense-in-depth: if `result` arrives as a JSON string, parse it.
fn normalize_mutation_result(row: &Value) -> Value {
    if let Value::Object(map) = row
        && let Some(result) = map.get("result")
        && let Value::String(s) = result
        && let Ok(parsed) = serde_json::from_str::<Value>(s)
    {
        let mut cloned = map.clone();
        cloned.insert("result".to_string(), parsed);
        return Value::Object(cloned);
    }
    row.clone()
}

/// The largest integer JS can represent exactly (`Number.MAX_SAFE_INTEGER`).
const MAX_SAFE_INTEGER: i128 = 9_007_199_254_740_991;

/// Port of TS `ensureSafeJSON`: a top-level integer column outside
/// ±MAX_SAFE_INTEGER cannot be represented by the JS client without silent
/// precision loss, so TS throws (failing the connection) rather than send it.
/// serde_json has no bigint type, so we check integer `Number`s directly;
/// floats and nested values are left alone (matching TS, which only walks the
/// row's own entries).
fn ensure_safe_json(contents: &Value) -> Result<(), String> {
    if let Some(obj) = contents.as_object() {
        for (k, v) in obj {
            let n: Option<i128> = v
                .as_i64()
                .map(i128::from)
                .or_else(|| v.as_u64().map(i128::from));
            if let Some(n) = n
                && !(-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&n)
            {
                return Err(format!(
                    "Value of \"{}\" exceeds safe Number range ({})",
                    k, n
                ));
            }
        }
    }
    Ok(())
}

pub(crate) fn make_row_patch(patch: &RowPatch) -> Result<RowPatchOp, String> {
    match patch {
        RowPatch::Put { id, contents } => {
            ensure_safe_json(contents)?;
            Ok(RowPatchOp {
                op: "put".to_string(),
                table_name: id.table.clone(),
                value: Some(contents.clone()),
                id: None,
            })
        }
        RowPatch::Del { id } => Ok(RowPatchOp {
            op: "del".to_string(),
            table_name: id.table.clone(),
            value: None,
            id: Some(Value::Object(id.row_key.clone())),
        }),
    }
}

// ─── ClientHandler ─────────────────────────────────────────────────────────

pub struct ClientHandler {
    client_group_id: String,
    pub client_id: String,
    pub ws_id: String,
    zero_clients_table: String,
    zero_mutations_table: String,
    downstream: Arc<dyn WebSocketSink>,
    base_version: Arc<StdMutex<NullableCVRVersion>>,
    poke_chain: Arc<AtomicBool>,
    /// Set true once this client has received a completed poke. On the first
    /// poke after connect we force an (empty) poke even when caught up. Mirrors
    /// TS ClientHandler `#everPoked` (zero/v1.9.0).
    ever_poked: Arc<AtomicBool>,
}

impl ClientHandler {
    pub fn new(
        client_group_id: &str,
        client_id: &str,
        ws_id: &str,
        shard: &ShardID,
        base_cookie: Option<&str>,
        downstream: Arc<dyn WebSocketSink>,
    ) -> Self {
        let us = upstream_schema(shard);
        Self {
            client_group_id: client_group_id.to_string(),
            client_id: client_id.to_string(),
            ws_id: ws_id.to_string(),
            zero_clients_table: format!("{}.clients", us),
            zero_mutations_table: format!("{}.mutations", us),
            downstream,
            base_version: Arc::new(StdMutex::new(
                base_cookie.map(crate::version::version_from_string),
            )),
            poke_chain: Arc::new(AtomicBool::new(false)),
            ever_poked: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set the base version (for testing).
    #[doc(hidden)]
    pub fn set_base_version_for_test(&self, version: CVRVersion) {
        *self.base_version.lock().unwrap() = Some(version);
    }

    pub fn version(&self) -> NullableCVRVersion {
        self.base_version.lock().unwrap().clone()
    }

    pub fn fail(&self, e: &str) {
        self.downstream.fail(e.to_string());
    }

    pub fn close(&self, reason: &str) {
        eprintln!("view-syncer closing connection: {}", reason);
        self.downstream.cancel();
    }

    pub fn start_poke(&self, tentative_version: CVRVersion) -> PokeHandler {
        let poke_id = version_string(&tentative_version);

        let base = self.base_version.clone();
        let base_val = self.base_version.lock().unwrap().clone();

        // Force one (empty) poke on connect even when caught up, so the client
        // learns its got-queries state was reconciled; thereafter only poke when
        // behind. Mirrors TS ClientHandler.startPoke (zero/v1.9.0).
        let force_initial_poke = !self.ever_poked.load(AtomicOrdering::SeqCst);
        let cmp = cmp_versions(&base_val, &Some(tentative_version.clone()));
        if cmp == Ordering::Greater || (cmp == Ordering::Equal && !force_initial_poke) {
            // Genuinely inert NOOP handler (TS returns an object whose
            // addPatch/end/cancel are empty functions): every method
            // early-returns on `noop`, so a later `end(final != base)` cannot
            // emit a fabricated baseCookie-null poke or regress the cookie.
            return PokeHandler {
                state: Arc::new(StdMutex::new(PokeState::new(poke_id, None))),
                downstream: self.downstream.clone(),
                base_version: base,
                poke_chain: self.poke_chain.clone(),
                ever_poked: self.ever_poked.clone(),
                force_initial_poke,
                zero_clients_table: self.zero_clients_table.clone(),
                zero_mutations_table: self.zero_mutations_table.clone(),
                client_group_id: self.client_group_id.clone(),
                start: std::time::Instant::now(),
                noop: true,
            };
        }

        let base_cookie = base_val.as_ref().map(version_string);

        PokeHandler {
            state: Arc::new(StdMutex::new(PokeState::new(poke_id.clone(), base_cookie))),
            downstream: self.downstream.clone(),
            base_version: base,
            poke_chain: self.poke_chain.clone(),
            ever_poked: self.ever_poked.clone(),
            force_initial_poke,
            zero_clients_table: self.zero_clients_table.clone(),
            zero_mutations_table: self.zero_mutations_table.clone(),
            client_group_id: self.client_group_id.clone(),
            start: std::time::Instant::now(),
            noop: false,
        }
    }

    pub fn send_delete_clients(
        &self,
        client_ids: Vec<String>,
        client_group_ids: Vec<String>,
    ) -> Result<(), String> {
        let mut body = serde_json::Map::new();
        if !client_ids.is_empty() {
            body.insert(
                "clientIDs".to_string(),
                Value::Array(client_ids.into_iter().map(Value::String).collect()),
            );
        }
        if !client_group_ids.is_empty() {
            body.insert(
                "clientGroupIDs".to_string(),
                Value::Array(client_group_ids.into_iter().map(Value::String).collect()),
            );
        }
        self.downstream
            .push(serde_json::json!(["deleteClients", body]))
    }

    pub fn send_query_transform_application_errors(
        &self,
        errors: Vec<Value>,
    ) -> Result<(), String> {
        self.downstream
            .push(serde_json::json!(["transformError", errors]))
    }

    pub fn send_inspect_response(&self, response: Value) {
        // Fire-and-forget like TS. On the actor thread, push is sync.
        // If push fails, there's nothing to do — the WS is already broken.
        let _ = self
            .downstream
            .push(serde_json::json!(["inspect", response]));
    }

    /// Send a query transform failed error to the client.
    /// Port of `sendQueryTransformFailedError` from TS.
    pub fn send_query_transform_failed_error(&self, error: &Value) {
        // In TS, this calls `this.fail(new ProtocolError(error))`.
        // ProtocolError is serialized as ["error", errorBody].
        let _ = self.downstream.push(serde_json::json!(["error", error]));
        self.fail("query transform failed");
    }
}

// ─── Multi-client poke fanout ──────────────────────────────────────────────

/// Wraps PokeHandlers for multiple clients, mirroring TS `startPoke()`.
/// Unlike TS's `Promise.allSettled`, on the actor thread each poke is
/// sequential. A failed client's error is logged but does not stop
/// the remaining clients — matching TS's allSettled semantics.
pub struct MultiPoker {
    pokers: Vec<PokeHandler>,
}

impl MultiPoker {
    pub fn new(clients: &[&ClientHandler], tentative_version: CVRVersion) -> Self {
        let pokers = clients
            .iter()
            .map(|c| c.start_poke(tentative_version.clone()))
            .collect();
        Self { pokers }
    }

    pub fn add_patch(&self, patch: &PatchToVersion) {
        for poker in &self.pokers {
            if let Err(e) = poker.add_patch(patch) {
                eprintln!("Poke add_patch failed for client: {}", e);
            }
        }
    }

    pub fn cancel(&self) {
        for poker in &self.pokers {
            if let Err(e) = poker.cancel() {
                eprintln!("Poke cancel failed: {}", e);
            }
        }
    }

    pub fn end(&self, final_version: CVRVersion) {
        for poker in &self.pokers {
            if let Err(e) = poker.end(final_version.clone()) {
                eprintln!("Poke end failed: {}", e);
            }
        }
    }
}

fn upstream_schema(shard: &ShardID) -> String {
    format!("{}_{}", shard.app_id, shard.shard_num)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    struct MockSink {
        messages: Arc<StdMutex<Vec<Value>>>,
        failed: Arc<StdMutex<Option<String>>>,
        cancelled: Arc<StdMutex<bool>>,
    }

    impl MockSink {
        fn new() -> (Self, Arc<StdMutex<Vec<Value>>>) {
            let messages = Arc::new(StdMutex::new(Vec::new()));
            let sink = Self {
                messages: messages.clone(),
                failed: Arc::new(StdMutex::new(None)),
                cancelled: Arc::new(StdMutex::new(false)),
            };
            (sink, messages)
        }
    }

    impl WebSocketSink for MockSink {
        fn push(&self, msg: Value) -> Result<(), String> {
            self.messages.lock().unwrap().push(msg);
            Ok(())
        }
        fn fail(&self, e: String) {
            *self.failed.lock().unwrap() = Some(e);
        }
        fn cancel(&self) {
            *self.cancelled.lock().unwrap() = true;
        }
    }

    struct FailingSink {
        fail_tag: &'static str,
    }

    impl WebSocketSink for FailingSink {
        fn push(&self, msg: Value) -> Result<(), String> {
            if msg
                .as_array()
                .and_then(|parts| parts.first())
                .and_then(Value::as_str)
                == Some(self.fail_tag)
            {
                Err(format!("intentional {} failure", self.fail_tag))
            } else {
                Ok(())
            }
        }

        fn fail(&self, _e: String) {}

        fn cancel(&self) {}
    }

    fn make_failing_handler(fail_tag: &'static str) -> ClientHandler {
        ClientHandler::new(
            "cg1",
            "client1",
            "ws1",
            &ShardID {
                app_id: "app".to_string(),
                shard_num: 0,
            },
            None,
            Arc::new(FailingSink { fail_tag }),
        )
    }

    fn assert_chain_released(handler: &ClientHandler) {
        assert!(
            !handler.poke_chain.load(AtomicOrdering::SeqCst),
            "failed poke must release the per-client chain"
        );
    }

    fn make_handler() -> (ClientHandler, Arc<StdMutex<Vec<Value>>>) {
        let (sink, messages) = MockSink::new();
        let handler = ClientHandler::new(
            "cg1",
            "client1",
            "ws1",
            &ShardID {
                app_id: "app".to_string(),
                shard_num: 0,
            },
            None,
            Arc::new(sink),
        );
        (handler, messages)
    }

    fn make_row_patch_put(table: &str, contents: Value) -> PatchToVersion {
        PatchToVersion {
            patch: Patch::Row(RowPatch::Put {
                id: RowID {
                    schema: "s".to_string(),
                    table: table.to_string(),
                    row_key: Map::new(),
                },
                contents,
            }),
            to_version: CVRVersion {
                state_version: "v2".to_string(),
                config_version: Some(1),
            },
        }
    }

    #[test]
    fn make_row_patch_rejects_unsafe_integer() {
        // Safe integers (<= MAX_SAFE_INTEGER) pass through unchanged.
        let safe = RowPatch::Put {
            id: RowID {
                schema: "s".into(),
                table: "t".into(),
                row_key: Map::new(),
            },
            contents: serde_json::json!({"id": "1", "big": 9_007_199_254_740_991_i64}),
        };
        assert!(make_row_patch(&safe).is_ok());

        // A column beyond the safe range (e.g. a snowflake id) must be rejected
        // — matching TS ensureSafeJSON, which throws to fail the connection
        // rather than let the JS client silently truncate the value.
        let unsafe_i = RowPatch::Put {
            id: RowID {
                schema: "s".into(),
                table: "t".into(),
                row_key: Map::new(),
            },
            contents: serde_json::json!({"id": "1", "big": 9_007_199_254_740_993_u64}),
        };
        let err = make_row_patch(&unsafe_i).unwrap_err();
        assert!(err.contains("exceeds safe Number range"), "got: {err}");
    }

    #[test]
    fn test_noop_poke_sends_nothing() {
        let (handler, messages) = make_handler();
        let v2 = CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        };
        // Set base version to v2 (client is caught up).
        *handler.base_version.lock().unwrap() = Some(v2.clone());
        // The first poke on connect is forced (an empty poke) even when caught
        // up — see `force_initial_poke`. Consume it, then verify a *subsequent*
        // caught-up poke is a true NOOP.
        handler.start_poke(v2.clone()).end(v2.clone()).unwrap();
        messages.lock().unwrap().clear();

        let poke = handler.start_poke(v2.clone());
        poke.end(v2).unwrap();
        assert!(messages.lock().unwrap().is_empty());
    }

    /// The client is AHEAD of the tentative version (Greater case): the
    /// returned handler must be a true NOOP even when `end` is called with a
    /// final version different from the client's base. Before the fix it was a
    /// live handler with `baseCookie: None` — the mismatched `end` emitted a
    /// fabricated from-scratch `pokeStart {baseCookie: null}` + `pokeEnd` and
    /// REGRESSED the client's cookie. TS returns an object whose
    /// addPatch/end/cancel are empty functions (client-handler.ts).
    #[test]
    fn test_noop_poke_inert_on_mismatched_end() {
        let (handler, messages) = make_handler();
        let v2 = CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        };
        let v3 = CVRVersion {
            state_version: "v3".to_string(),
            config_version: None,
        };
        *handler.base_version.lock().unwrap() = Some(v3.clone());

        let poke = handler.start_poke(v2.clone());
        poke.end(v2).unwrap();
        assert!(
            messages.lock().unwrap().is_empty(),
            "NOOP handler must send nothing on a mismatched end"
        );
        assert_eq!(
            *handler.base_version.lock().unwrap(),
            Some(v3.clone()),
            "NOOP end must not regress the client's base version"
        );

        // The NOOP end must not have consumed `ever_poked`: the first REAL
        // caught-up poke is still forced.
        handler.start_poke(v3.clone()).end(v3).unwrap();
        let msgs = messages.lock().unwrap();
        assert_eq!(msgs.len(), 2, "forced initial poke still fires");
        assert_eq!(msgs[0][0], "pokeStart");
        assert_eq!(msgs[1][0], "pokeEnd");
    }

    #[test]
    fn test_initial_poke_forced_when_caught_up() {
        // Even when the client connects already caught up (base == tentative),
        // the FIRST poke sends an empty pokeStart/pokeEnd so the client learns
        // its got-queries state has been reconciled with the server. Mirrors TS
        // ClientHandler `#everPoked` (zero/v1.9.0).
        let (handler, messages) = make_handler();
        let v2 = CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        };
        *handler.base_version.lock().unwrap() = Some(v2.clone());
        handler.start_poke(v2.clone()).end(v2).unwrap();
        let msgs = messages.lock().unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0][0], "pokeStart");
        assert_eq!(msgs[1][0], "pokeEnd");
    }

    #[test]
    fn test_empty_poke_sends_start_and_end() {
        let (handler, messages) = make_handler();
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        poke.end(CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        })
        .unwrap();
        let msgs = messages.lock().unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0][0], "pokeStart");
        assert_eq!(msgs[1][0], "pokeEnd");
    }

    #[test]
    fn test_poke_flushes_at_100_parts() {
        let (handler, messages) = make_handler();
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        // Add 101 row patches
        for _ in 0..101 {
            poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": 1})))
                .unwrap();
        }
        poke.end(CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        })
        .unwrap();
        let msgs = messages.lock().unwrap();
        // pokeStart + 1 pokePart (at 100) + 1 pokePart (remaining 1) + pokeEnd
        assert_eq!(msgs.len(), 4);
        assert_eq!(msgs[0][0], "pokeStart");
        assert_eq!(msgs[1][0], "pokePart");
        assert_eq!(msgs[2][0], "pokePart");
        assert_eq!(msgs[3][0], "pokeEnd");
    }

    #[test]
    fn test_poke_lmids_interception() {
        let (handler, messages) = make_handler();
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        poke.add_patch(&make_row_patch_put(
            "app_0.clients",
            serde_json::json!({
                "clientGroupID": "cg1",
                "clientID": "clientA",
                "lastMutationID": 42,
            }),
        ))
        .unwrap();
        poke.end(CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        })
        .unwrap();
        let msgs = messages.lock().unwrap();
        // pokeStart + pokePart (with lastMutationIDChanges) + pokeEnd
        assert_eq!(msgs.len(), 3);
        let part = &msgs[1][1];
        assert!(part.get("lastMutationIDChanges").is_some());
        assert_eq!(part["lastMutationIDChanges"]["clientA"], 42);
    }

    #[test]
    fn test_mutations_patch_shape() {
        let (handler, messages) = make_handler();
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        poke.add_patch(&make_row_patch_put(
            "app_0.mutations",
            serde_json::json!({
                "clientGroupID": "cg1",
                "clientID": "clientA",
                "mutationID": 5,
                "result": {"ok": true},
            }),
        ))
        .unwrap();
        poke.end(CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        })
        .unwrap();
        let msgs = messages.lock().unwrap();
        let part = &msgs[1][1];
        let mp = part.get("mutationsPatch").unwrap();
        assert_eq!(mp[0]["op"], "put");
        assert_eq!(mp[0]["mutation"]["id"]["clientID"], "clientA");
        assert_eq!(mp[0]["mutation"]["id"]["id"], 5);
        assert_eq!(mp[0]["mutation"]["result"]["ok"], true);
    }

    #[test]
    fn test_cancel_releases_chain() {
        let (handler, messages) = make_handler();
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        // Start the poke by adding a patch
        poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": 1})))
            .unwrap();
        poke.cancel().unwrap();
        {
            let msgs = messages.lock().unwrap();
            // pokeStart + pokeEnd (cancel)
            assert_eq!(msgs.len(), 2);
            assert_eq!(msgs[1][0], "pokeEnd");
            assert_eq!(msgs[1][1]["cancel"], true);
        }

        // Verify chain is released — next poke should work
        let poke2 = handler.start_poke(CVRVersion {
            state_version: "v3".to_string(),
            config_version: None,
        });
        poke2
            .end(CVRVersion {
                state_version: "v3".to_string(),
                config_version: Some(1),
            })
            .unwrap();
    }

    #[test]
    fn failed_poke_start_releases_chain() {
        let handler = make_failing_handler("pokeStart");
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        assert!(
            poke.end(CVRVersion {
                state_version: "v2".to_string(),
                config_version: Some(1),
            })
            .is_err()
        );
        assert_chain_released(&handler);
    }

    #[test]
    fn failed_poke_part_releases_chain() {
        let handler = make_failing_handler("pokePart");
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": 1})))
            .unwrap();
        assert!(
            poke.end(CVRVersion {
                state_version: "v2".to_string(),
                config_version: Some(1),
            })
            .is_err()
        );
        assert_chain_released(&handler);
    }

    #[test]
    fn failed_poke_end_and_cancel_release_chain() {
        let handler = make_failing_handler("pokeEnd");
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        assert!(
            poke.end(CVRVersion {
                state_version: "v2".to_string(),
                config_version: Some(1),
            })
            .is_err()
        );
        assert_chain_released(&handler);

        let poke = handler.start_poke(CVRVersion {
            state_version: "v3".to_string(),
            config_version: None,
        });
        poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": 1})))
            .unwrap();
        assert!(poke.cancel().is_err());
        assert_chain_released(&handler);
    }

    #[test]
    fn patch_assembly_error_releases_chain() {
        let (handler, _messages) = make_handler();
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        let malformed_mutation = make_row_patch_put(
            "app_0.mutations",
            serde_json::json!({"mutationID": 1, "result": {"ok": true}}),
        );
        assert!(poke.add_patch(&malformed_mutation).is_err());
        assert_chain_released(&handler);
    }

    #[test]
    fn invalid_final_version_after_patches_releases_chain() {
        let (handler, _messages) = make_handler();
        *handler.base_version.lock().unwrap() = Some(CVRVersion {
            state_version: "v1".to_string(),
            config_version: None,
        });
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": 1})))
            .unwrap();

        // Once frames have started, ending at the current base is invalid and
        // must not strand the per-client serialization guard.
        assert!(
            poke.end(CVRVersion {
                state_version: "v1".to_string(),
                config_version: None,
            })
            .is_err()
        );
        assert_chain_released(&handler);
    }

    #[test]
    fn test_end_advances_base_version() {
        let (handler, _messages) = make_handler();
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        let final_v = CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        };
        poke.end(final_v.clone()).unwrap();
        let bv = handler.version();
        assert_eq!(bv, Some(final_v));
    }

    #[test]
    fn test_patches_below_base_version_skipped() {
        let (handler, messages) = make_handler();
        // Set base version
        {
            let mut bv = handler.base_version.lock().unwrap();
            *bv = Some(CVRVersion {
                state_version: "v2".to_string(),
                config_version: Some(1),
            });
        }
        let poke = handler.start_poke(CVRVersion {
            state_version: "v3".to_string(),
            config_version: None,
        });
        // This patch has to_version <= base_version, should be skipped
        poke.add_patch(&PatchToVersion {
            patch: Patch::Row(RowPatch::Put {
                id: RowID {
                    schema: "s".to_string(),
                    table: "t".to_string(),
                    row_key: Map::new(),
                },
                contents: serde_json::json!({"id": 1}),
            }),
            to_version: CVRVersion {
                state_version: "v2".to_string(),
                config_version: Some(1),
            },
        })
        .unwrap();
        poke.end(CVRVersion {
            state_version: "v3".to_string(),
            config_version: None,
        })
        .unwrap();
        let msgs = messages.lock().unwrap();
        // Only pokeStart + pokeEnd, no pokePart
        assert_eq!(msgs.len(), 2);
    }

    #[test]
    fn test_normalize_mutation_result_string() {
        let row = serde_json::json!({
            "clientID": "c1",
            "mutationID": 1,
            "result": "{\"ok\":true}",
        });
        let normalized = normalize_mutation_result(&row);
        assert!(normalized.get("result").unwrap().is_object());
        assert_eq!(normalized["result"]["ok"], true);
    }

    #[test]
    fn test_normalize_mutation_result_object() {
        let row = serde_json::json!({
            "clientID": "c1",
            "mutationID": 1,
            "result": {"ok": true},
        });
        let normalized = normalize_mutation_result(&row);
        assert!(normalized.get("result").unwrap().is_object());
    }
}
