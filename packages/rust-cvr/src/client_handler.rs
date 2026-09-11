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

/// Abstract WebSocket sink. The production implementation is rust-syncer's
/// `DirectWebSocketSink` (ws_sink.rs), which owns the socket's writer task.
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
    /// Send a `pokePart` frame from its TYPED body, so the JSON text is
    /// produced by whoever owns the socket rather than on the client-group
    /// thread.
    ///
    /// This is TS's shape, not an optimization on top of it:
    /// `this.#push(['pokePart', body])` (client-handler.ts:220) hands the typed
    /// tuple to the outstream, and the `Transform` stringifies it AT THE SINK
    /// (types/streams.ts:126-130). Building a `serde_json::Value` tree first —
    /// a full allocation pass over every row patch in the part, on the serial
    /// CG thread, then walked a second time by the writer — is the rust-only
    /// step.
    ///
    /// The default keeps the `Value` route so a sink that only implements
    /// `push`/`push_sized` (mocks, the in-process test sinks) needs no change;
    /// `poke_part_serializes_identically_as_a_value_tree_and_as_a_typed_body`
    /// pins that both routes emit the same bytes.
    fn push_poke_part(&self, body: PokePartBody, est_bytes: usize) -> Result<(), String> {
        self.push_sized(serde_json::json!(["pokePart", body]), est_bytes)
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
    // `&'static str`, not `String`: every construction site passes the literal
    // `"put"` or `"del"` (TS `putOpSchema` / `delOpSchema`,
    // zero-protocol/src/queries-patch.ts:6,21), so a `String` heap-allocated a
    // two/three-byte constant for every patch in every poke. Serializes
    // byte-identically — serde writes a `&str` and a `String` the same way.
    pub op: &'static str,
    pub hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RowPatchOp {
    // `&'static str` for the same reason as `QueryPatchEntry::op`. NOT modelled
    // as a rust enum: TS's `rowPatchOpSchema` is a union of FOUR variants with
    // different field sets — `put{tableName,value}`, `update{tableName,id,
    // merge?,constrain?}`, `del{tableName,id}` and `clear{}` (no tableName) —
    // (zero-protocol/src/row-patch.ts:6-34), while rust only ever emits `put`
    // and `del`. A two-variant enum would misstate the protocol type; this
    // struct is the permissive superset that carries exactly those two.
    pub op: &'static str,
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
    // `&'static str` for the same reason as `QueryPatchEntry::op`: every
    // construction site passes the literal `"put"` or `"del"` (TS
    // `putOpSchema` / `delOpSchema`, zero-protocol/src/mutations-patch.ts:13,17),
    // so a `String` heap-allocated a constant per patch. Serializes identically.
    pub op: &'static str,
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
    /// `to_version` of the FIRST patch admitted into this poke — i.e. the patch
    /// that made `ensure_body` push `pokeStart` and set `started`. Diagnostic
    /// for the `Patches were sent but finalVersion ...` close: that error means
    /// a patch was admitted (`to_version > base`) and `end()` then ran with
    /// `final <= base`, so this is the version that says WHICH patch opened a
    /// poke the flush went on to discard.
    first_patch_version: Option<CVRVersion>,
    /// Which `MultiPoker::new` call site opened this poke. There are five, and
    /// the `Patches were sent but finalVersion ...` close could come from any —
    /// trace adjacency was the only way to guess, which is how the
    /// already-refuted hypotheses got their plausibility. Naming the origin
    /// makes the failing site a FACT.
    origin: &'static str,
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
            first_patch_version: None,
            origin: "unknown",
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
        if state.first_patch_version.is_none() {
            state.first_patch_version = Some(to_version.clone());
        }
        self.ensure_body(&mut state)?;

        let result: Result<(), String> = (|| {
            match &patch_to_version.patch {
                Patch::Query(qp) => {
                    let body = state.body.as_mut().unwrap();
                    match qp {
                        QueryPatch::Put { id, client_id } => {
                            let entry = QueryPatchEntry {
                                op: "put",
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
                                op: "del",
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

    /// Whether this poke has emitted `pokeStart` — i.e. at least one patch was
    /// admitted and sent downstream. The 1:1 read of TS `pokeStarted`
    /// (client-handler.ts:280), which is the flag `end()` branches on: only a
    /// STARTED poke can raise `Patches were sent but finalVersion ... is not
    /// greater than baseVersion` (:327-334). An unstarted poke either no-ops or
    /// opens a fresh `pokeStart`, and can never raise it.
    pub fn started(&self) -> bool {
        !self.noop && self.state.lock().unwrap().started
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
                // TS `end` returns SILENTLY here — `return; // Nothing changed
                // and nothing was sent.` (client-handler.ts:319-325). It logs
                // NOTHING at this site. Rust used to emit
                // `already caught up, not sending poke.` here, misattributed to
                // client-handler.ts:196, which is the `startPoke` branch below
                // — a different site with a different comparison (TENTATIVE vs
                // base, not FINAL vs base). On the advance path the tentative
                // version is always ahead, so TS never takes that branch, while
                // rust took THIS one on every no-change advance: 1.7M INFO
                // lines/hour of JSON serialization on the serving path against
                // TS's 2,983, for identical (empty) client output.
                self.release_chain(&mut state);
                return Ok(());
            }
            drop(base);
            self.acquire_chain(&mut state)?;
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
                // `state.poke_id` IS the tentative version this poke was
                // started with (`start_poke` sets it to `version_string(
                // tentative_version)`), and it is the field that says WHICH way
                // this fired: `poke_id == final` means the client's base moved
                // after `start_poke`'s caught-up guard passed, while
                // `poke_id > final` means the CVR ended below the version the
                // poke was opened for. The message carried only final+base, so
                // production could not tell them apart. TS keeps the same
                // context (`lc.withContext('pokeID', pokeID)`,
                // client-handler.ts:190).
                let error = format!(
                    "Patches were sent but finalVersion {:?} is not greater than baseVersion {:?} \
                     (pokeID {}, firstPatch {:?}, origin {})",
                    final_version, *base, state.poke_id, state.first_patch_version, state.origin
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
            self.acquire_chain(state)?;
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
            if let Err(error) = self.downstream.push_poke_part(body, est) {
                self.release_chain(state);
                return Err(error);
            }
            state.part_count = 0;
            state.body_est_bytes = 0;
        }
        Ok(())
    }

    /// Take this client's poke chain, or fail rather than hang.
    ///
    /// Rust-only mutual exclusion (AGENTS rule 5/10; see `poke_chain` and
    /// parity/INVENTIONS.md I-15). TS needs none: its `startPoke` keeps
    /// `pokeStarted`/`body` as plain locals (client-handler.ts:208-210) and the
    /// view-syncer's `#lock` plus JS's single thread already make two pokes for
    /// one client impossible to interleave. Rust's pokers are reached from the
    /// serial CG thread, so the flag exists to turn any accidental overlap into
    /// a detectable state instead of two interleaved `pokeStart`s on one socket.
    ///
    /// It must NOT spin unboundedly, which is what it used to do:
    /// `std::thread::yield_now()` yields the OS THREAD, while the only code
    /// that can release this chain is another `PokeHandler` on the SAME thread
    /// — reachable only through an `.await` point this loop never reaches. So a
    /// contended chain was not a slow path, it was a permanent 100%-CPU wedge
    /// of the CG thread, and every client of the group stopped receiving acks
    /// and pokes (the shape of the connect-ack outage).
    ///
    /// A bounded retry is kept rather than failing on the first miss purely as
    /// insurance against a cross-thread holder this analysis has not foreseen;
    /// it costs microseconds and cannot mask the same-thread case, which no
    /// number of retries can resolve. On give-up the caller's `add_patch`
    /// routes the error to `downstream.fail`, failing THIS client so it
    /// reconnects and rehydrates — recoverable, unlike a wedged thread.
    fn acquire_chain(&self, state: &mut PokeState) -> Result<(), String> {
        /// Enough to cover a genuine cross-thread hand-off, far too few to
        /// spend meaningful CPU on the unresolvable same-thread case.
        const MAX_SPINS: u32 = 1024;
        for _ in 0..MAX_SPINS {
            if self
                .poke_chain
                .compare_exchange(false, true, AtomicOrdering::SeqCst, AtomicOrdering::SeqCst)
                .is_ok()
            {
                state.poke_in_progress = true;
                return Ok(());
            }
            std::thread::yield_now();
        }
        // Deliberately do NOT set `poke_in_progress`: this handler never took
        // the chain, so its `release_chain`/`Drop` must not clear the flag out
        // from under the handler that actually holds it.
        Err(format!(
            "poke chain already held for client group {}: refusing to block the \
             client-group thread",
            self.client_group_id
        ))
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
                // TS `this.#lc.error?.('Received clients row for wrong
                // clientGroupID. Ignoring.', clientGroupID)`
                // (client-handler.ts:385-388). Must go through `tracing`, not
                // `eprintln!`: a raw stderr write has no level, so it ignores
                // ZERO_LOG_LEVEL and is invisible to every error-count alert and
                // to the release gate's error-volume watch, while TS's twin is a countable
                // ERROR (pinned by `parity/log_differential.py`).
                tracing::error!(
                    client_group_id = %cg,
                    "Received clients row for wrong clientGroupID. Ignoring."
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
                    op: "put",
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
                // Port of TS client-handler.ts:267-284: `assert(typeof
                // clientID === 'string', 'client id must be a string')` covers
                // missing AND wrong-typed keys with ONE message; `Number(
                // mutationID)` + `assert(finite && id >= 0, 'mutation id must
                // be a finite number')` REJECTS a negative id (previously the
                // rust arm accepted it — divergence).
                let client_id = id
                    .row_key
                    .get("clientID")
                    .and_then(|v| v.as_str())
                    .ok_or("client id must be a string")?
                    .to_string();
                let mutation_id = id
                    .row_key
                    .get("mutationID")
                    .and_then(|v| v.as_i64())
                    .filter(|id| *id >= 0)
                    .ok_or("mutation id must be a finite number")?;

                patches.push(MutationPatchEntry {
                    op: "del",
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
                op: "put",
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
                op: "del",
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

    /// Port of TS `ClientHandler.fail` (client-handler.ts:175-181):
    ///
    /// ```ts
    /// fail(e: unknown) {
    ///   this.#lc[getLogLevel(e)]?.(
    ///     `view-syncer closing connection with error: ${String(e)}`, e);
    ///   this.#downstream.fail(wrapWithProtocolError(e));
    /// }
    /// ```
    ///
    /// WARN is `getLogLevel(e)` for this function's only caller,
    /// [`ClientHandler::send_query_transform_failed_error`]: TS passes
    /// `new ProtocolError(error)` (client-handler.ts:368), which takes
    /// `getLogLevel`'s `isProtocolError` branch (types/error-with-level.ts:24-30).
    /// A caller that passes a RAW thrown value would be `error` in TS and must
    /// carry its level here rather than reuse this one.
    ///
    /// The view-syncer's own client failures do NOT come through here — rust
    /// routes them through `Connection::fail`, which owns the `ErrorBody` the
    /// wire needs; see that function's structural note.
    pub fn fail(&self, e: &str) {
        tracing::warn!("view-syncer closing connection with error: {}", e);
        self.downstream.fail(e.to_string());
    }

    pub fn close(&self, reason: &str) {
        // TS `this.#lc.debug?.(`view-syncer closing connection: ${reason}`)`
        // (client-handler.ts:184) — DEBUG, so it must be suppressible by
        // ZERO_LOG_LEVEL. As an `eprintln!` it printed unconditionally.
        tracing::debug!("view-syncer closing connection: {}", reason);
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
            // TS client-handler.ts:196 `lc.info?.(`already caught up, not
            // sending poke.`)` — logged where TS logs it, on the NOOP branch of
            // `startPoke`, comparing the TENTATIVE version to the base.
            tracing::info!("already caught up, not sending poke.");
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
    /// `origin` labels the call site so the `finalVersion` close names where its
    /// poke was opened. See `PokeState::origin`. Stamped here rather than
    /// threaded through `start_poke`, which stays 1:1 with TS `startPoke`.
    pub fn new(
        clients: &[&ClientHandler],
        tentative_version: CVRVersion,
        origin: &'static str,
    ) -> Self {
        let pokers: Vec<PokeHandler> = clients
            .iter()
            .map(|c| {
                let poker = c.start_poke(tentative_version.clone());
                poker.state.lock().unwrap().origin = origin;
                poker
            })
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
                // DEBUG, not stderr: TS logs NOTHING here. Its fan-out is
                // `Promise.allSettled(pokers.map(p => p.addPatch(patch)))`
                // (client-handler.ts:96) and the per-poker `addPatch` catch
                // calls `this.#downstream.fail(...)` (:306-308) — the failure
                // reaches the operator through `sendError`, which IS ported.
                // Keeping it visible at default level would invent an
                // operator-facing event TS does not have (pinned by `parity/log_differential.py`).
                tracing::debug!(
                    "Poke add_patch failed for client, dropping from poke: {}",
                    e
                );
            }
        }
    }

    /// Whether ANY of this group's pokes has emitted `pokeStart` (patches were
    /// admitted). Dead pokers are INCLUDED on purpose: `dead` means "already
    /// failed terminally, push nothing more", which does not un-send the frames
    /// that were already delivered — and this predicate asks what went out, not
    /// what may still go out.
    ///
    /// Rust-only accessor (HARD RULE 5) over the rust-only `MultiPoker` fan-out;
    /// the flag it reads is the 1:1 port of TS `pokeStarted`. It exists so the
    /// view-syncer can tell a DANGEROUS discarded version bump (patches already
    /// sent → the next `end()` raises `Patches were sent but finalVersion ...`)
    /// from the benign one (nothing sent → `end()` cannot raise).
    pub fn any_started(&self) -> bool {
        self.pokers.iter().any(|p| p.started())
    }

    pub fn cancel(&self) {
        for (poker, dead) in self.pokers.iter().zip(&self.dead) {
            if dead.load(AtomicOrdering::Relaxed) {
                continue;
            }
            if let Err(e) = poker.cancel() {
                dead.store(true, AtomicOrdering::Relaxed);
                // TS `Promise.allSettled(pokers.map(p => p.cancel()))` swallows
                // this (client-handler.ts:99); see the `add_patch` note above.
                tracing::debug!("Poke cancel failed: {}", e);
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
                // TS `Promise.allSettled(pokers.map(p => p.end(v)))` swallows
                // this (client-handler.ts:102); the `fail` below is the ported,
                // operator-visible half. See the `add_patch` note above.
                tracing::debug!("Poke end failed: {}", e);
                poker.downstream.fail(e);
            }
        }
    }
}

fn upstream_schema(shard: &ShardID) -> String {
    format!("{}_{}", shard.app_id, shard.shard_num)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests/client_handler_tests.rs"]
mod tests;
