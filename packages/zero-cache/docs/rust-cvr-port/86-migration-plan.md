# 86 — Migration Plan

Per-phase rollout with concrete code deliverables, CI gates, and rollback criteria.

## Naming conventions

- Env flag: `USE_RUST_CVR_<PHASE>` where `<PHASE> ∈ {SIGNATURE, ROW_CACHE, UPDATERS, POKER, STORE}`.
- Rust crate name: `packages/rust-cvr/` (or `rust-cvr-poker/` etc. if we split).
- napi crate name: `packages/rust-cvr/napi/`.
- All flags default **off**.

## Phase A — Row-set signature port (1 day)

**Goal.** Replace `row-set-signature.ts` with a Rust napi binding. Also, as a smoke, the `h64` hash from `shared/src/hash.ts`. Doesn't change behavior; validates the FFI path.

**Code deliverables:**

- `packages/rust-cvr/Cargo.toml` (workspace member)
- `packages/rust-cvr/src/row_set_signature.rs` — `fn signature_unit(row_id: &RowID) -> u64`, `fn parse_signature(hex: &str) -> u64`, `fn format_signature(sig: u64) -> String`
- `packages/rust-cvr/napi/src/lib.rs` — `#[napi] pub fn row_id_signature_unit(...) -> BigInt`, `#[napi] pub fn parse_signature(...) -> BigInt`, `#[napi] pub fn format_signature(...) -> String`
- `packages/zero-cache/src/services/view-syncer/row-set-signature.ts` — replaced by rust call, but **old TS path kept** in `row-set-signature-legacy.ts` for diffing

**CI gates:**

- `pnpm --filter=rust-cvr build` passes
- `cargo test --package=rust-cvr` passes (10 unit tests, mirroring the TS property tests)
- Diff test: TS vs Rust over the existing `row-set-signature.test.ts` fixture set, byte-equal
- Existing integration tests (`cvr.pg.test.ts`) run against BOTH implementations — assert identical outputs

**Rollback:** `USE_RUST_CVR_SIGNATURE=0` + restart.

---

## Phase B — RowRecordCache port (2-3 days)

**Goal.** Port the write-through/write-back LRU.

**Code deliverables:**

- `packages/rust-cvr/src/row_record_cache.rs` (~500 LOC Rust)
- napi surface: `RowRecordCacheHandle` with `load`, `apply`, `flushed`, `catchupRowPatches`, `executeRowUpdates`, `hasPendingUpdates`, `clear`
- TS `row-record-cache.ts` becomes a thin wrapper that delegates to the handle when `USE_RUST_CVR_ROW_CACHE=1`.

**CI gates:**

- All 10 tests from `83-row-record-cache-port.md` pass in Rust
- `cvr-store.pg.test.ts` passes with the flag on
- **Deferral latch test**: push 1 batch > 100 rows, then push another — second must defer. Verify by checking `rowsVersion` advancing only once.

**Rollback:** `USE_RUST_CVR_ROW_CACHE=0` + restart.

---

## Phase C — Updaters port (4-7 days)

**Goal.** Both `CVRConfigDrivenUpdater` and `CVRQueryDrivenUpdater` become Rust. This is the largest chunk.

**Code deliverables:**

- `packages/rust-cvr/src/cvr.rs` (~400 LOC)
- `packages/rust-cvr/src/updater.rs` (~600 LOC, both updaters)
- napi surface: `CVRConfigDrivenUpdaterHandle`, `CVRQueryDrivenUpdaterHandle` with all the public methods declared in doc 82
- TS `cvr.ts` retains the classes but their bodies proxy to Rust when the flag is on

**CI gates:**

- All `cvr.pg.test.ts` tests pass with flag on (regression suite)
- **Merge-ref-counts property test** — `proptest!` invariant `mergeRefCounts(x, null) == normalize(x)` over 100k random (x, y) pairs
- **Signature-flush ordering test** — simulate `rowSetSignatureProvider` returning drift; assert signature write happens before base flush SQL

**Rollback:** `USE_RUST_CVR_UPDATERS=0` + restart.

---

## Phase D — ClientHandler port (3-5 days)

**Goal.** Per-connection poke chain in Rust.

**Code deliverables:**

- `packages/rust-cvr/src/client_handler.rs` (~500 LOC Rust)
- napi surface: `ClientHandlerHandle`, `PokeHandler`, `startPoke(version) -> PokeHandler`
- The Rust `WebSocketSink` trait with `NapiWebSocketSink` impl that proxies back to TS's WS
- TS `client-handler.ts` becomes a wrapper

**CI gates:**

- **CRITICAL: poke-chain interleave regression test** — set up two rapid advances, verify frames from different pokes NEVER interleave in the wire stream. Hard requirement.
- Body-shape byte equality test — 1000 fixture rows through TS path and Rust path, byte-equal JSON
- Bigint / JSON edge cases — `i64::MAX`, `i64::MIN`, deeply nested objects, JSON strings-as-columns, mutation results as both object and string

**Rollback:** `USE_RUST_CVR_POKER=0` + restart.

---

## Phase E — CVRStore port (2-3 days)

**Goal.** Postgres writer in Rust. Final layer.

**Code deliverables:**

- `packages/rust-cvr/src/store.rs` (~600 LOC)
- `sqlx` setup, transaction wrapper
- TS `cvr-store.ts` fully replaced

**CI gates:**

- All `cvr-store.pg.test.ts` tests pass
- Ownership-void-write integration test
- Catchup-row-patch stream cancellation under client disconnect

**Rollback:** `USE_RUST_CVR_STORE=0` + restart.

---

## Phase F (final) — TS cleanup + legacy removal

After phase E soaks for **two weeks in production without a CVR-related rollback**, delete:

- `packages/zero-cache/src/services/view-syncer/row-set-signature-legacy.ts`
- The TS-side bodies of `cvr-store.ts`, `cvr.ts`, `client-handler.ts`, `row-record-cache.ts` (keep names as thin re-export layers for backward compat in PRs)
- The `USE_RUST_CVR_*` flags → consolidate to single `USE_RUST_CVR=1`

Then a final removal PR deletes the legacy TS.

**Total wall-clock estimate:** 14-21 days of focused work, one dedicated engineer. (Verified with the per-phase estimates above.)

---

## Soaking rules between phases

Do **not** proceed to the next phase until:

1. The flag has been at `1` in sandbox for **24 hours** with zero CVR-related alerts.
2. Production has the flag at `1` for **72 hours** with no rollback triggers (see below).
3. The `cvr.flush-time` OTEL histogram shows no regression >10% P95.

## Rollback triggers (any one forces immediate flag-off)

| Trigger | Detection | Action |
|---|---|---|
| CVR load failures spike | `sync.cvr.load.count{error=~"true"}` > 5/min sustained for 2 min | Flag to 0, page on-call |
| Poke body byte-diffs | Differential smoke test in CI fails | Halt rollout, fix forward |
| Row refCounts divergence | `sync.cvr.rows-flushed` jumps >20% without query-set change | Flag to 0, investigate |
| Poke chain hang | `sync.poke.time` P99 > 30s | Flag to 0, page on-call |
| Bigint out-of-range crash | Rust panic log contains `out of safe integer range` | Flag to 0, patch, retry |

## Risk register

| Risk | Phase | Mitigation |
|---|---|---|
| Byte-diff in poke bodies | D | Diff-test against the existing body-shape snapshot fixtures |
| Slower than TS | All | Benchmarks per phase; do not ship if >10% regression on `sync.cvr.flush-time` or `sync.poke.time` |
| Bigint coercion drift | B, C, D | Proptest invariants on `merge-ref-counts` and `RowID` keying |
| Deadlock in poke chain | D | Drop-impl releases chain; watchdog in TS side closes connection after 60s without frame |
| Postgres silent corruption | E | All writes in single transaction; ownership check on every flush |
| Bigint literal in prepared-statement bind | E | Verify `sqlx` correctly serializes `i64::MAX`; if not, fall back to `String` bind and `CAST` |

## What does NOT ship with this port

- Full WebSocket server in Rust (separate future initiative)
- Auth / cookie parsing (stays in TS at the route layer)
- Schema-migration runner (separate initiative)
- Change-streamer side (covered elsewhere)
- Replicator (covered elsewhere)
