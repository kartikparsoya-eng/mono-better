# Rust Syncer — Gate Observations Backlog

Every non-fatal thing an ART gate surfaces, captured so it gets a real fix instead
of a shrug. "Benign" here means "did not fail the gate / clients saw `errs=0`" — it
does **not** mean "correct" or "nothing to do". Each row is a codebase task.

Update this file on every gate run: bump `Last seen`, adjust frequency, close rows
when the underlying code is fixed (link the commit).

## Run verdict — 2026-08-25, image rev `b1f6025de` (release gate, sandbox rust-test)

`LOCAL ART: FAIL` — **24 PASS / 4 SKIP / 2 WATCH / 3 FAIL**. Every **correctness** gate passed
(mutations 37/37, mut-matrix 220/220, determinism byte-identical, negative 8/8, wedge 11/11,
protocol, coverage 67/67, upgrade zero-loss, capacity candidate≥ref). The refactor is
**correctness-clean**. The 3 FAILs classify as:

- **G8 diff-oracle** (`channels` +5 rows) → **CLOSED benign** (B0). All 5 rows are `PUBLIC`,
  `createdBy`=the oracle user → user must see them; the rust candidate has them, the TS mirror
  is stale (behind on the 54GB-lagged slot, B-ENV). Rust is the *more correct* side.
- **G13 log-health** (64 unknown signatures) → **gate not blessed**: `signature_counts.baseline=0`,
  so every distinct signature is "unknown". Not 64 failure modes (B-LOG). Bless a baseline.
- **G25 latency-parity** (43/51 queries 3–5s, 35–73× TS) → **B-POOL, but NOT a pool-sizing issue**.
  CORRECTION: pool=30 was proven sufficient before — prior G25 history at pool=30: **28 shards → 0
  violations**, 14 → 17, 56 → 4. This run's 43 (worse than any prior config) is caused by the
  **548 `catchup … pool timed out`** — catchup hanging on the 54GB-stalled slot (B-ENV), holding
  connections the full 10s → the 30-pool exhausts. Fix the slot + set shards≈clients, NOT the pool
  size. Container exited cleanly (code 0, no OOM).

WATCH: **G7 cvr-gc** client-group instances 4730→4780→6456 growing (B-GC). **G29** 84 prod shapes
not exercised (coverage gap). SKIP: G5/G5b (no blessed latency baseline for `50c-life-prod-7d`).

Severity legend:
- `harness` — the finding is in the ART harness, not the syncer; fix is test-side.
- `perf` — correct output, but slower than it should be.
- `robustness` — self-healed under load but indicates a real failure mode.
- `security` — safe in the sandbox only because the sandbox is permissive.
- `config` — a default that is wrong for the deployed shape.

---

## Open

| # | Symptom (verbatim) | Where it surfaces | Freq (release, ~4min) | Severity | Why non-fatal | Proposed fix |
|---|---|---|---|---|---|---|
| **B-POOL** | `config_and_hydrate failed: catchup rows page / catchup_config_patches: pool timed out while waiting for an open connection` → `CG thread …: terminating after fatal synchronization error` | `rust_syncer::router` during 200c sweep + G25 parity (08:31–08:41) | **548** pool-timeouts, **314** CG terminations | **HIGH — but triggered by B-ENV, NOT sizing** | Drove G25's 3–5s latencies. **`cvr_max_conns=30` is NOT too small** — prior runs at pool=30 hit **0 violations at 28 shards** (14→17, 56→4). The regression this run is that catchup queries (`catchup rows page`, `catchup_config_patches`) hang against the **54GB-stalled replica slot** (B-ENV) and hold pool connections until the 10s `acquire_timeout`, exhausting 30 → later hydrates starve → 43 violations + CG terminations. Prior clean runs had `error_level_lines=0` (no timeouts). Clean exit (code 0, no OOM). | 1) PRIMARY: fix B-ENV (unstall slot) — catchup then completes fast, connections release, 30 suffices. 2) Set `ZERO_SYNCER_SHARDS ≈ num clients` (1 CG/shard; 28→0 proven) — the harness should pass this for the cpus=4 container. 3) Add TS's boot guard (B-POOL-GUARD) so an undersized pool fails fast instead of silently timing out. 4) Bound catchup so a stalled slot can't hold a pool connection for 10s. Re-run G25 healthy-slot + shards≈clients to confirm 0. |
| **B-HEALTHZ** | Rust serves **only `/readyz`**, no `/healthz`; TS `runner/zero-dispatcher.ts:37` serves `fastify.get('/healthz', → 'OK')` (always-200 liveness). | `rust-syncer/src/http_server.rs` (routes: only `/readyz`) | n/a (user-found, code-level) | **real parity gap** | A liveness probe (LB / k8s livenessProbe / harness) hitting `/healthz` on the rust target gets 404/refusal where TS returns 200 → the orchestrator may consider rust perpetually-unhealthy and kill/never-route it. `/readyz` is readiness (PG+replica probe), a different contract from always-OK liveness. | Add a `/healthz` route to `http_server.rs` returning 200 `OK` unconditionally (match TS semantics: liveness = "process is up", independent of PG). Keep `/readyz` as the PG/replica readiness probe. Trivial, high-value for prod LB/k8s parity. |
| **B-POOL-GUARD** | No boot-time validation that the CVR pool is large enough for the concurrency; TS throws at startup: `Insufficient cvr connections (N) for M syncers … Increase ZERO_CVR_MAX_CONNS` (main.ts:121). | `rust-syncer/src/main.rs` pool init | n/a (TS-comparison gap) | robustness parity gap | Rust silently accepts `cvr_max_conns=30` regardless of shard/worker fan-out and only reveals the shortfall as `pool timed out` under load (B-POOL). TS fails fast with an actionable message. | Add an equivalent startup check/warn in rust: validate `cvr_max_conns` against `num_shards` (or the real concurrency), and log/throw with a "raise CVR_MAX_CONNS or lower ZERO_SYNCER_SHARDS" message mirroring TS. |
| **B-GC** | G7 cvr-gc WATCH: art client groups `4730 → 4780` (still growing at run end); CVR `instances` table at **6456** after the run | `evaluate_gates.py` G7 + CVR `instances` table | growing, not plateauing | robustness/leak-suspect | The 200c capacity sweep created a burst of transient client groups; if CVRPurger/GC doesn't reclaim them promptly they pin CVR rows AND (per B-POOL) possibly PG connections, feeding the pool exhaustion. Not proven a leak yet — could be GC lag within the run window. | Verify CVRPurger reclaims idle CGs: sample `instances` count over time post-load; confirm it drains to baseline. If it plateaus high, it's a GC/retention gap (distinct from the fixed G6 engine-Drop leak [[rust-g6-leak-hunt-dhat]] — this is the CVR-instance layer). Correlate reclaim with connection release for B-POOL. |
| **B-LOG** | `log gate: FAIL — unknown-signatures: 64 new ERROR/WARN signatures (>5 threshold) — possible new failure mode` | `evaluate_gates.py` log gate | 64 sigs (1 run) | **GATE FAIL — classify all 64** | The only failing gate. Samples are known-benign (`no read-permissions…`=B3; `User ID mismatch pinned/incoming`=the wrong-user-pin case the negative suite PASSES). Strong hypothesis: our **1:1 rename refactor changed log `target:` paths + message wording**, so signatures fell off the gate's known-good allowlist. BUT 64 is a lot — must diff each against baseline; a real new error could hide among renames. | 1) Read `reports/*log*`/gate report for the full 64. 2) Bucket: (a) renamed-target/reworded-but-equivalent (benign → refresh the gate's signature baseline), (b) expected-WARN-not-yet-allowlisted (B3, user-id-mismatch → allowlist), (c) genuinely NEW error (real regression → fix). 3) Only refresh baseline after (c) is empty. Do NOT rubber-stamp. |
| **B0** | oracle `pair 1: mismatches=5` on `channels` — `{'only_primary': 5, 'only_mirror': 0, 'value_mismatch': 0}`; rust candidate has 5 channel rows TS reference lacks | `evaluate_gates.py` end-of-run differential oracle | 1 pair of 4 (others 0) | **CLOSED — benign, verified** | Extra rows on the rust side, values identical, isolated to one pair — same signature as the known G8 convergence-skew ([[rust-dm-channel-leak-g8]]). **ROOT CAUSE SUSPECT FOUND same run:** the log gate reported `pg-slot-lag: PG slot total lag hit 54.42GB — change-streamer likely falling behind or stalled slot` — the identical 54GB stalled-slot condition from G8. Two caches pinned at different replica versions ⇒ superset-on-one-side skew, NOT a rust eval bug. Still verify. | 1) Dump the 5 rowIDs + the CG's userID; `SELECT` participation for each in PG — participant ⇒ skew confirmed (benign), non-participant ⇒ real leak (blocker). 2) Fix the stalled local PG slot (see B-ENV) and re-run oracle — expect 0 mismatches. Do NOT close until the participant check is done. |
| **B-ENV** | `pg-slot-lag: PG slot total lag hit 54.42GB — change-streamer likely falling behind or stalled slot` | `evaluate_gates.py` log gate | 1 | environment (not code) | Local sandbox PG logical-replication slot is 54GB behind → the two caches (rust `-art`, TS `-ts`) replicate at different rates and drift in `stateVersion`. Manifests as B0. Not a syncer bug, but it **poisons the differential oracle** by comparing two caches at different versions. | Unstall/advance the local PG slot (or recreate the sandbox replica) before trusting oracle diffs. Longer-term: the ART oracle should **gate on cache stateVersion convergence** before diffing (refuse to compare caches >N versions apart) so slot lag can't masquerade as a correctness mismatch. This is the real fix — it would have auto-classified B0. |
| B1 | `heap snapshot failed / cpu profile failed / trace capture failed: <urlopen error [Errno 61] Connection refused>` | ART harness startup, probing the candidate's profiling endpoints | 3 (once each, at start) | harness | Rust syncer doesn't expose Node's inspector/`/heapsnapshot`/`/cpuprofile`/trace HTTP endpoints; harness catches and continues. Same class as the known **G17** scrape-port false-fail (:4849 vs rust :8081). | Either (a) expose equivalent Rust profiling endpoints (pprof/`tokio-console`/dhat-on-signal) on the syncer's status port so the gate captures real candidate profiles, or (b) teach the harness to detect a Rust candidate and skip Node-only captures instead of emitting scary lines. Prefer (a) — we lose candidate profiling otherwise. |
| B2 | `query coverage shadow summary … coveredHydratedQueries=0 uncoveredHydratedQueries=N` | `rust_syncer::services::view_syncer::query_covering`, every hydration | 76 | perf/feature | Query-covering runs in **shadow** mode (observe-only), so 0 coverage never affects results. But coverage is 0/N *always* → the covering index matches nothing in this workload, i.e. the optimization is dead weight here. | Investigate why every hydrated query is "uncovered": is the covering set empty, mis-keyed, or not populated for this app? Confirm whether coverage is expected to be non-zero for prod-7d queries; if the feature is not ready, gate the shadow-summary log behind a debug flag so it stops dominating the log. |
| B3 | `CG …: no read-permissions deployed — queries pass through` | `rust_syncer` per client-group load | 71 | security | The sandbox app ships **no permission rules**, so the syncer correctly (and by design) passes queries through unfiltered. Safe here; would be a data-exposure hole if it ever happened in prod. | (a) Add an assertion/telemetry that this NEVER happens in a prod-shaped deploy (fail closed or loud-alert when permissions are absent but expected). (b) Deploy a real permission ruleset into the ART sandbox so the gate actually **exercises** permission enforcement (right now that whole path is untested by ART). |
| B4 | `Slow query materialization: config_and_hydrate took Nms for client X` | `rust_syncer::router` | 45 | perf | Hydration completes correctly, just slow; `errs=0`. Threshold-based log. | Profile `config_and_hydrate`: is the cost config-resolution or the initial fetch? Ties to the columnar-rows / hydrate hot-path work ([[phase1-columnar-rows]]). Capture the worst offenders' ASTs and see if they overlap B7's slow SQLite queries. |
| B5 | `pipeline reset (advancement-timeout); re-initializing engine + rehydrating` | `rust_syncer` per CG | 5 | **robustness** | Clients saw `errs=0` — the reset+rehydrate recovered transparently. But an advancement timeout that forces a full engine rebuild 5× in 4min is a real stall, not cosmetic. | Root-cause the advancement timeout: which operator/CG stalls, and why advancement doesn't complete within the deadline. This is the highest-value row — it's the one "benign" that is actually a latent correctness/latency risk under load. Check for lock contention on the shared runtime ([[rust-efficiency-audit-2026-08]]). |
| B6 | `cgroup cpu quota is far below the host core count; the 14-shard default may oversubscribe — consider tuning ZERO_SYNCER_SHARDS` | `rust_syncer` startup | 2 | config | Release runs the container at `cpus=4` but the syncer defaults to 14 shards (host*2) → oversubscription; runs anyway. | Auto-clamp the default shard count to the **cgroup** quota (not host core count) so a CPU-limited container self-tunes. Until then, set `ZERO_SYNCER_SHARDS` in the release harness to match `--cpus`. Directly relevant to the sublinear-scaling finding ([[rust-vs-ts-1core-knee-experiment]], [[rust-efficiency-audit-2026-08]]). |
| B7 | `Slow SQLite query 352.6ms / 135.1ms / 103.2ms` | `rust_syncer` (SQLite read path) | 3 | perf | Correct results; occasional slow reads under 50-conn load. | Capture the offending SQL + EXPLAIN QUERY PLAN. Watch for the **NULL+OR full-table-scan** gotcha (AGENTS.md) and missing indexes on the replica. Likely correlated with B4's slow hydrations. |

## Closed

_(none yet — link commits here as rows are fixed)_

---

## Provenance

- First captured: release gate on image rev `b1f6025deb070005aaf4b56b2781d47e6e601ef9` (`zero-cache-rust-syncer:local`), 2026-08-25, sandbox `rust-test`, prod-7d profile, 50 conns / 900s.
- Harvest command (re-runnable):
  ```sh
  docker logs xyne-sandbox-rust-test-zero-cache-art 2>&1 \
    | grep -iE '"level":"WARN"|"level":"ERROR"|slow|uncovered|pass through|advancement|shard' \
    | sed -E 's/"timestamp":"[^"]*"//; s/art-[a-z0-9]+/art-CG/g; s/took [0-9]+ms/took Nms/g' \
    | grep -oE '"message":"[^"]*"' | sort | uniq -c | sort -rn
  ```
