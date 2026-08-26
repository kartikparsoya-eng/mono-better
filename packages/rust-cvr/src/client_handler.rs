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

use serde::{Deserialize, Serialize};
// ─── wire patch types (client-handler.ts) ───

/// Patches — sent to clients to update their view.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Patch {
    #[serde(rename = "row")]
    Row(RowPatch),
    #[serde(rename = "query")]
    Query(QueryPatch),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum RowPatch {
    #[serde(rename = "put")]
    Put {
        id: RowID,
        contents: std::sync::Arc<Value>,
    },
    #[serde(rename = "del")]
    Del { id: RowID },
}
/// Patch tagged with the version it applies to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PatchToVersion {
    pub patch: Patch,
    pub to_version: CVRVersion,
}
/// RowPatchInfo — internal tracking for dedup.
#[derive(Debug, Clone, PartialEq)]
pub struct RowPatchInfo {
    /// None for a row-del
    pub row_version: Option<String>,
    pub to_version: CVRVersion,
}

#[cfg(test)]
use serde_json::Map;
use serde_json::Value;

use crate::schema::types::*;
use crate::schema::types::{CVRVersion, NullableCVRVersion, cmp_cvr, cmp_versions, version_string};
use crate::shards::ShardID;
use std::cmp::Ordering;

const PART_COUNT_FLUSH_THRESHOLD: usize = 100;

/// Default per-`pokePart` byte cap (estimated serialized bytes). A part flushes
/// early once its accumulated estimate crosses this, in addition to the 100-row
/// count — bounding single-frame size so a burst of large rows can't build a
/// multi-MB frame (which would also strain proxies and the client's inbound
/// payload cap). Env override: `ZERO_POKE_PART_MAX_BYTES` (0 disables the byte
/// cap, leaving only the count threshold).
const DEFAULT_POKE_PART_MAX_BYTES: usize = 256 * 1024;

/// Envelope overhead added to a flushed part's estimate: `["pokePart",{...}]`
/// framing plus the poke-id string. A constant is enough for accounting.
const POKE_PART_ENVELOPE_EST: usize = 48;

/// Cached `ZERO_POKE_PART_MAX_BYTES`. Read once — this is on the per-row hot
/// path, so never re-parse the env per call.
fn poke_part_max_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("ZERO_POKE_PART_MAX_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_POKE_PART_MAX_BYTES)
    })
}

/// Abstract WebSocket sink. The napi implementation proxies to TS's WS via
/// a ThreadsafeFunction with `Blocking` call mode.
pub trait WebSocketSink: Send + Sync {
    fn push(&self, msg: Value) -> Result<(), String>;
    /// Push a frame whose approximate serialized byte size is already known
    /// (poke parts, where the assembler accumulated the estimate for free).
    /// Default forwards to `push`; the production sink overrides it to feed the
    /// byte-aware slow-client shed without re-walking the tree. Test mocks keep
    /// the default and are unaffected.
    fn push_sized(&self, msg: Value, _est_bytes: usize) -> Result<(), String> {
        self.push(msg)
    }
    fn fail(&self, e: String);
    fn cancel(&self);
}

/// Approximate serialized JSON size of `v` in bytes. Deliberately cheap and
/// deterministic — pointer-chasing with no allocation, strictly dominated by
/// the per-client `to_value` deep conversion `flush_body` already performs on
/// the same data. Used only for queue accounting, never for protocol
/// decisions, so exact escaping/number widths don't matter.
///
/// Depth-guarded: values reaching this function are already bounded by serde's
/// 128-level parse limit (client JSON is always parsed, never built with
/// `disable_recursion_limit`), but as this is a recursive walker we cap descent
/// regardless of caller so it can never stack-overflow — accounting only, so
/// stopping at the cap merely under-counts a pathological subtree.
pub fn estimate_json_bytes(v: &Value) -> usize {
    /// Comfortably above serde's 128 parse limit; a backstop, not a functional
    /// bound. Reaching it means the input bypassed the parser (a programmatic
    /// build) — under-count rather than blow the stack.
    const MAX_DEPTH: u32 = 300;
    fn go(v: &Value, depth: u32) -> usize {
        if depth >= MAX_DEPTH {
            return 0;
        }
        match v {
            Value::Null => 4,
            Value::Bool(_) => 5,
            Value::Number(_) => 12,
            Value::String(s) => s.len() + 2, // no escape accounting — fine for an estimate
            Value::Array(a) => 2 + a.len() + a.iter().map(|e| go(e, depth + 1)).sum::<usize>(),
            Value::Object(m) => {
                2 + m
                    .iter()
                    .map(|(k, v)| k.len() + 4 + go(v, depth + 1))
                    .sum::<usize>()
            }
        }
    }
    go(v, 0)
}

/// Estimated serialized size of one row patch, envelope included. Del patches
/// carry only the row key; Put patches the full contents.
fn estimate_row_patch_bytes(rp: &RowPatch) -> usize {
    const ROW_PATCH_ENVELOPE_EST: usize = 32; // {"op":"put","tableName":"...","id":{}}
    match rp {
        RowPatch::Put { id, contents } => {
            id.table.len() + estimate_json_bytes(contents) + ROW_PATCH_ENVELOPE_EST
        }
        RowPatch::Del { id } => {
            let key_bytes: usize = id
                .row_key
                .iter()
                .map(|(k, v)| k.len() + 4 + estimate_json_bytes(v))
                .sum();
            id.table.len() + key_bytes + ROW_PATCH_ENVELOPE_EST
        }
    }
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
    // Arc-shared with the originating `RowPatch::Put` (serde's `rc` feature
    // serializes through the Arc transparently) — no per-client deep clone.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<std::sync::Arc<Value>>,
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
    /// Accumulated estimated serialized bytes of the current (unflushed) body.
    /// Reset to 0 whenever the body is flushed (taken).
    body_est_bytes: usize,
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
            body_est_bytes: 0,
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
    /// Live-instance census guard (leak hunting). A `PokeHandler` is transient
    /// (one per `start_poke`), not `Clone`, so a plain field guard is correct;
    /// the census should return to 0 between pokes.
    _census: crate::live_count::Guard,
}

impl PokeHandler {
    pub fn add_patch(&self, patch_to_version: &PatchToVersion) -> Result<(), String> {
        if self.noop {
            return Ok(());
        }
        let to_version = &patch_to_version.to_version;
        let base = self.base_version.lock().unwrap();

        // Skip when to_version is not strictly greater than base (a None base
        // means "no floor", so nothing is skipped) — matches the old
        // `cmp_versions(&Some(to_version), &base) != Greater`, without cloning.
        if base
            .as_ref()
            .is_some_and(|b| cmp_cvr(to_version, b) != Ordering::Greater)
        {
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
                    // Byte accounting for the slow-client shed + part cap. Add
                    // the row's estimate regardless of which sub-table it routes
                    // to — lmid/mutation rows are tiny, regular rows are the ones
                    // that can build a large frame.
                    state.body_est_bytes += estimate_row_patch_bytes(rp);
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
            let byte_cap = poke_part_max_bytes();
            if state.part_count >= PART_COUNT_FLUSH_THRESHOLD
                || (byte_cap > 0 && state.body_est_bytes >= byte_cap)
            {
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

        // Release the chain on failure — like every other error path here. A
        // `?` propagation would leave the chain held and the NEXT poke for this
        // client spinning forever in `acquire_chain`.
        if let Err(error) = self.flush_body(&mut state) {
            self.release_chain(&mut state);
            return Err(error);
        }
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
        let elapsed_ms = self.start.elapsed().as_secs_f64() * 1000.0;
        crate::otel_metrics::record_poke(elapsed_ms);
        if crate::tracer::enabled() {
            crate::tracer::note(
                "PokeHandler",
                &format!(
                    "end poke_id={} cookie={} elapsed_ms={:.2}",
                    state.poke_id, cookie, elapsed_ms,
                ),
            );
        }
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
            let est = state.body_est_bytes + POKE_PART_ENVELOPE_EST;
            if let Err(error) = self
                .downstream
                .push_sized(serde_json::json!(["pokePart", body]), est)
            {
                self.release_chain(state);
                return Err(error);
            }
            state.part_count = 0;
            state.body_est_bytes = 0;
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
            // TS `#updateLMIDs` (client-handler.ts:376-390): `v.parse(row,
            // lmidRowSchema, 'passthrough')` — clientGroupID/clientID (string)
            // and lastMutationID (number) are REQUIRED; a malformed clients row
            // THROWS (failing the poke downstream), it is not silently ignored
            // (F-CH-1). Only the wrong-clientGroupID case is log-and-ignore.
            let cg = contents
                .get("clientGroupID")
                .and_then(|v| v.as_str())
                .ok_or("clients row: clientGroupID must be a string")?;
            let cid = contents
                .get("clientID")
                .and_then(|v| v.as_str())
                .ok_or("clients row: clientID must be a string")?;
            let lmid = contents
                .get("lastMutationID")
                .and_then(|v| v.as_i64())
                .ok_or("clients row: lastMutationID must be a number")?;

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
        RowPatch::Del { id } => {
            // TS `makeRowPatch` del: `v.parse(id, primaryKeyValueRecordSchema)`
            // (client-handler.ts:434, primary-key.ts:10-20) — every rowKey
            // value must be string | number | boolean; anything else (null,
            // nested object/array) THROWS rather than reaching the client
            // (F-CH-1). The put arm's rowSchema parse is structurally
            // guaranteed here: `contents` is already a JSON object by type.
            for (col, val) in id.row_key.iter() {
                if !(val.is_string() || val.is_number() || val.is_boolean()) {
                    return Err(format!(
                        "rowKey column {col:?} is not a primary key value (string|number|boolean)"
                    ));
                }
            }
            Ok(RowPatchOp {
                op: "del".to_string(),
                table_name: id.table.clone(),
                value: None,
                id: Some(Value::Object(id.row_key.clone())),
            })
        }
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
    /// Live-instance census guard (leak hunting). Inc on `new`, dec on Drop.
    /// `ClientHandler` is not `Clone`, so a plain field guard is correct here.
    _census: crate::live_count::Guard,
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
            base_version: Arc::new(StdMutex::new(base_cookie.and_then(|c| {
                // base_cookie is client-supplied. A malformed one must not panic
                // connection setup; treat it as no base version (client re-syncs
                // from scratch) and record it via the env-gated trace. Well-behaved
                // clients only ever send cookies we produced.
                match crate::schema::types::maybe_version_string(c) {
                    Ok(v) => Some(v),
                    Err(e) => {
                        crate::tracer::note(
                            "ClientHandler",
                            &format!("ignoring malformed base cookie {c:?}: {e}"),
                        );
                        None
                    }
                }
            }))),
            poke_chain: Arc::new(AtomicBool::new(false)),
            ever_poked: Arc::new(AtomicBool::new(false)),
            _census: crate::live_count::Guard::new(&crate::live_count::CLIENT_HANDLER),
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
                _census: crate::live_count::Guard::new(&crate::live_count::POKE_HANDLER),
            };
        }

        let base_cookie = base_val.as_ref().map(version_string);

        if crate::tracer::enabled() {
            crate::tracer::note(
                "PokeHandler",
                &format!(
                    "start client_id={} poke_id={} force_initial={}",
                    self.client_id, poke_id, force_initial_poke,
                ),
            );
        }

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
            _census: crate::live_count::Guard::new(&crate::live_count::POKE_HANDLER),
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
///
/// The first failure for a client marks its poker **dead** (`dead[i]`) for the
/// remainder of this poke. `PokeHandler::add_patch` already fails the client's
/// downstream terminally on error (`downstream.fail`), mirroring TS where a
/// caught `addPatch` throw calls `#downstream.fail()` and puts the subscription
/// in a terminal state so every subsequent `#push` is silently absorbed. Once
/// dead, we stop re-invoking the poker: this avoids both the wasted `send` into
/// a closed sink AND the per-patch log flood (a big hydration poke is thousands
/// of patches; without this, one disconnected client logs once per patch).
/// Net result: one log line per dead client per poke, matching TS.
pub struct MultiPoker {
    pokers: Vec<PokeHandler>,
    dead: Vec<AtomicBool>,
}

impl MultiPoker {
    pub fn new(clients: &[&ClientHandler], tentative_version: CVRVersion) -> Self {
        let pokers: Vec<PokeHandler> = clients
            .iter()
            .map(|c| c.start_poke(tentative_version.clone()))
            .collect();
        let dead = pokers.iter().map(|_| AtomicBool::new(false)).collect();
        Self { pokers, dead }
    }

    pub fn add_patch(&self, patch: &PatchToVersion) {
        for (poker, dead) in self.pokers.iter().zip(&self.dead) {
            if dead.load(AtomicOrdering::Relaxed) {
                continue;
            }
            if let Err(e) = poker.add_patch(patch) {
                // First failure: PokeHandler has already failed the downstream
                // terminally. Mark dead so the remaining patches skip this
                // poker (no re-push, no re-log) — TS-faithful terminal state.
                dead.store(true, AtomicOrdering::Relaxed);
                eprintln!(
                    "Poke add_patch failed for client, dropping from poke: {}",
                    e
                );
            }
        }
    }

    pub fn cancel(&self) {
        for (poker, dead) in self.pokers.iter().zip(&self.dead) {
            if dead.load(AtomicOrdering::Relaxed) {
                continue;
            }
            if let Err(e) = poker.cancel() {
                dead.store(true, AtomicOrdering::Relaxed);
                eprintln!("Poke cancel failed: {}", e);
            }
        }
    }

    pub fn end(&self, final_version: CVRVersion) {
        for (poker, dead) in self.pokers.iter().zip(&self.dead) {
            if dead.load(AtomicOrdering::Relaxed) {
                continue;
            }
            if let Err(e) = poker.end(final_version.clone()) {
                // A client whose poke cannot complete (delivery failure, or the
                // "finalVersion not greater" invariant) is mid-poke with no
                // `pokeEnd`: its cookie hasn't advanced and the next poke would
                // nest a second `pokeStart`. Fail the connection — the client
                // reconnects and rehydrates — matching the per-client failure
                // handling in `add_patch` (TS Promise.allSettled semantics: the
                // other clients' pokes proceed).
                dead.store(true, AtomicOrdering::Relaxed);
                eprintln!("Poke end failed: {}", e);
                poker.downstream.fail(e);
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

    /// Sink that always errors on `push` and counts how many times it was
    /// invoked, so a test can assert a dead poker is not re-pushed per patch.
    struct CountingFailSink {
        pushes: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl WebSocketSink for CountingFailSink {
        fn push(&self, _msg: Value) -> Result<(), String> {
            self.pushes.fetch_add(1, AtomicOrdering::SeqCst);
            Err("sink closed".to_string())
        }
        fn fail(&self, _e: String) {}
        fn cancel(&self) {}
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

    // Build a handler alongside the sink's fail/cancel observation handles.
    #[allow(clippy::type_complexity)] // test helper: a one-off observation tuple
    fn make_handler_observing_lifecycle() -> (
        ClientHandler,
        Arc<StdMutex<Option<String>>>,
        Arc<StdMutex<bool>>,
    ) {
        let failed = Arc::new(StdMutex::new(None));
        let cancelled = Arc::new(StdMutex::new(false));
        let sink = MockSink {
            messages: Arc::new(StdMutex::new(Vec::new())),
            failed: failed.clone(),
            cancelled: cancelled.clone(),
        };
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
        (handler, failed, cancelled)
    }

    // Port of TS client-handler.ts:175 `fail`: forwards to downstream.fail and
    // does NOT cancel.
    #[test]
    fn fail_forwards_to_downstream_fail_only() {
        let (handler, failed, cancelled) = make_handler_observing_lifecycle();
        handler.fail("boom");
        assert_eq!(*failed.lock().unwrap(), Some("boom".to_string()));
        assert!(!*cancelled.lock().unwrap(), "fail must not cancel");
    }

    // Port of TS client-handler.ts:183 `close`: invokes downstream.cancel (a
    // clean close), NOT fail.
    #[test]
    fn close_forwards_to_downstream_cancel_not_fail() {
        let (handler, failed, cancelled) = make_handler_observing_lifecycle();
        handler.close("done");
        assert!(*cancelled.lock().unwrap(), "close must cancel");
        assert_eq!(*failed.lock().unwrap(), None, "close must not fail");
    }

    fn make_row_patch_put(table: &str, contents: Value) -> PatchToVersion {
        PatchToVersion {
            patch: Patch::Row(RowPatch::Put {
                id: RowID {
                    schema: "s".to_string(),
                    table: table.to_string(),
                    row_key: Map::new(),
                },
                contents: std::sync::Arc::new(contents),
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
            contents: std::sync::Arc::new(
                serde_json::json!({"id": "1", "big": 9_007_199_254_740_991_i64}),
            ),
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
            contents: std::sync::Arc::new(
                serde_json::json!({"id": "1", "big": 9_007_199_254_740_993_u64}),
            ),
        };
        let err = make_row_patch(&unsafe_i).unwrap_err();
        assert!(err.contains("exceeds safe Number range"), "got: {err}");
    }

    /// F-CH-1: TS `makeRowPatch` del runs `v.parse(id,
    /// primaryKeyValueRecordSchema)` (client-handler.ts:434) — rowKey values
    /// must be string|number|boolean; null/nested values THROW instead of
    /// reaching the client. Pre-fix, Rust passed them through (proven by
    /// temp-revert: the unwrap_err below panicked on Ok).
    #[test]
    fn make_row_patch_del_rejects_non_primitive_row_key() {
        let mut row_key = Map::new();
        row_key.insert("id".to_string(), serde_json::json!("1"));
        row_key.insert("bad".to_string(), Value::Null);
        let del = RowPatch::Del {
            id: RowID {
                schema: "s".into(),
                table: "t".into(),
                row_key,
            },
        };
        let err = make_row_patch(&del).unwrap_err();
        assert!(err.contains("not a primary key value"), "got: {err}");

        // Primitive-only keys still pass (bool/number/string all legal).
        let mut ok_key = Map::new();
        ok_key.insert("id".to_string(), serde_json::json!(7));
        ok_key.insert("flag".to_string(), serde_json::json!(true));
        let ok = RowPatch::Del {
            id: RowID {
                schema: "s".into(),
                table: "t".into(),
                row_key: ok_key,
            },
        };
        assert!(make_row_patch(&ok).is_ok());
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

    /// Load-bearing invariant: client JSON is parsed via serde, whose default
    /// 128-level recursion limit rejects pathologically-nested input BEFORE any
    /// of our unguarded recursive walks (AST transform, hash, estimate) run. If
    /// a future change ever calls `disable_recursion_limit()` on a client path,
    /// this test fails — a deliberate tripwire.
    #[test]
    fn serde_rejects_deeply_nested_client_json() {
        // 200 open brackets ≫ serde's 128 default depth.
        let deep = "[".repeat(200) + &"]".repeat(200);
        let parsed = serde_json::from_str::<Value>(&deep);
        assert!(
            parsed.is_err(),
            "serde must reject >128-deep JSON at parse time (recursion-limit tripwire)"
        );
    }

    /// The estimator must not stack-overflow even on nesting deeper than its
    /// depth cap (defense-in-depth: a Value built programmatically, bypassing
    /// the parser). Build the tree directly (not via parse) to exceed the cap.
    #[test]
    fn estimate_json_bytes_is_depth_bounded() {
        let mut v = serde_json::json!(0);
        for _ in 0..1000 {
            v = Value::Array(vec![v]);
        }
        // Must return without overflowing; value is not asserted (accounting only).
        let _ = estimate_json_bytes(&v);
    }

    #[test]
    fn estimate_json_bytes_tracks_serialized_size() {
        // The estimate must stay within a small factor of the real serialized
        // length across scalars, unicode, and nested JSON — it's an accounting
        // approximation, not exact, but must never wildly under/over-count.
        let samples = [
            serde_json::json!({"id": "1", "n": 42, "b": true, "z": null}),
            serde_json::json!({"name": "héllo wörld", "tags": ["a", "b", "c"]}),
            serde_json::json!({"nested": {"deep": {"arr": [1, 2, 3, {"k": "v"}]}}}),
        ];
        for s in samples {
            let actual = serde_json::to_string(&s).unwrap().len();
            let est = estimate_json_bytes(&s);
            assert!(
                est as f64 >= actual as f64 * 0.5 && est as f64 <= actual as f64 * 2.0,
                "estimate {est} not within 0.5x–2x of actual {actual} for {s}"
            );
        }
    }

    #[test]
    fn poke_flushes_early_on_byte_cap() {
        // Large rows must flush into several parts BEFORE the 100-count
        // threshold — bounding single-frame size. Deterministic against the
        // shipped 256KB default cap (no env/OnceLock dependency): 8 rows of
        // ~50KB ≈ 400KB, well over the cap but only 8 rows (≪ 100).
        let (handler, messages) = make_handler();
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        let big = "x".repeat(50 * 1024);
        for _ in 0..8 {
            poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"blob": big})))
                .unwrap();
        }
        poke.end(CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        })
        .unwrap();
        let msgs = messages.lock().unwrap();
        let parts = msgs.iter().filter(|m| m[0] == "pokePart").count();
        // 8 rows never reach the 100-count path, so ≥2 parts proves the byte
        // cap flushed mid-stream. (256KB / ~50KB ⇒ a flush around row 5.)
        assert!(
            parts >= 2,
            "byte cap must split ~400KB into multiple parts, got {parts}"
        );
    }

    #[test]
    fn single_oversized_row_still_ships() {
        // A single row larger than the cap is never split or dropped — it ships
        // as one oversized part.
        let (handler, messages) = make_handler();
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        let huge = "y".repeat(300 * 1024); // > 256KB default cap
        poke.add_patch(&make_row_patch_put("t1", serde_json::json!({"blob": huge})))
            .unwrap();
        poke.end(CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        })
        .unwrap();
        let msgs = messages.lock().unwrap();
        assert!(
            msgs.iter().any(|m| m[0] == "pokePart"),
            "oversized single row must still be delivered as a pokePart"
        );
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

    /// F-CH-1: TS `#updateLMIDs` parses the clients row against lmidRowSchema
    /// (client-handler.ts:379-383) — a row missing `lastMutationID` (or with
    /// wrong types) THROWS, failing the poke; it is not silently skipped.
    /// Pre-fix, Rust's `if let (Some, Some, Some)` swallowed it (proven by
    /// temp-revert: add_patch returned Ok).
    #[test]
    fn test_poke_lmids_malformed_clients_row_fails() {
        let (handler, _messages) = make_handler();
        let poke = handler.start_poke(CVRVersion {
            state_version: "v2".to_string(),
            config_version: None,
        });
        let err = poke
            .add_patch(&make_row_patch_put(
                "app_0.clients",
                serde_json::json!({
                    "clientGroupID": "cg1",
                    "clientID": "clientA",
                    // lastMutationID missing
                }),
            ))
            .unwrap_err();
        assert!(err.contains("lastMutationID"), "got: {err}");
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
    fn multipoker_drops_dead_client_after_first_failure() {
        // One client whose sink always errors, alongside a healthy client. The
        // failing sink must be pushed to at most ONCE across the whole poke (it
        // dies on the first patch's pokeStart), not once per patch — proving the
        // per-poker `dead` short-circuit that mirrors TS's terminal downstream
        // fail. Without it, N patches would produce N pushes + N log lines.
        let pushes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let failing = ClientHandler::new(
            "cg1",
            "bad",
            "ws-bad",
            &ShardID {
                app_id: "app".to_string(),
                shard_num: 0,
            },
            None,
            Arc::new(CountingFailSink {
                pushes: pushes.clone(),
            }),
        );
        let (healthy, healthy_msgs) = make_handler();

        let tentative = CVRVersion {
            state_version: "v2".to_string(),
            config_version: Some(1),
        };
        let poker = MultiPoker::new(&[&failing, &healthy], tentative.clone());

        // Fan out several patches; the failing client dies on the first.
        for i in 0..5 {
            poker.add_patch(&make_row_patch_put("t1", serde_json::json!({"id": i})));
        }
        poker.end(tentative);

        assert_eq!(
            pushes.load(AtomicOrdering::SeqCst),
            1,
            "dead client must be pushed exactly once, not per patch"
        );
        assert!(
            !healthy_msgs.lock().unwrap().is_empty(),
            "healthy client keeps receiving patches after the other dies"
        );
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
                contents: std::sync::Arc::new(serde_json::json!({"id": 1})),
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

    /// Port of TS `sendQueryTransformFailedError` (client-handler.ts:368):
    /// `this.fail(new ProtocolError(error))`. The ProtocolError body reaches the
    /// client as EXACTLY one `["error", body]` frame, and the downstream is
    /// failed terminally (`downstream.fail`, NOT `cancel` — `close()` is the
    /// cancel path, client-handler.ts:183). Caller: rust-syncer sync_engine.rs.
    #[test]
    fn send_query_transform_failed_error_emits_exact_error_frame_and_fails() {
        let messages = Arc::new(StdMutex::new(Vec::new()));
        let failed = Arc::new(StdMutex::new(None));
        let cancelled = Arc::new(StdMutex::new(false));
        let sink = MockSink {
            messages: messages.clone(),
            failed: failed.clone(),
            cancelled: cancelled.clone(),
        };
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

        // A TransformFailedBody-shaped error (zero-protocol error body).
        let body = serde_json::json!({
            "kind": "TransformFailed",
            "origin": "zero-cache",
            "message": "failed to transform query",
            "queryHashes": ["qh1"],
        });
        handler.send_query_transform_failed_error(&body);

        // Exactly one frame, byte-shape ["error", body] — not wrapped, not
        // re-keyed, no other frames before/after.
        let msgs = messages.lock().unwrap();
        assert_eq!(
            *msgs,
            vec![serde_json::json!(["error", body])],
            "wire frame must be exactly [\"error\", body]"
        );
        // TS fail() puts the subscription in a terminal failed state.
        assert_eq!(
            failed.lock().unwrap().as_deref(),
            Some("query transform failed"),
            "downstream.fail must fire"
        );
        assert!(
            !*cancelled.lock().unwrap(),
            "must fail the downstream, not cancel it (cancel is the close() path)"
        );
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
