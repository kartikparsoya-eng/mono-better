# Rust Syncer — Production Operations Runbook

Operations guide for running zero-cache with `ZERO_SYNCER=rust`. Audience: SREs
operating the TS zero-cache today. Everything below is verified against the
source; maintainer citations are in HTML comments.

---

## 1. Architecture in 10 lines

<!-- Sources: packages/zero-cache/src/server/main.ts (loadRustSyncer, notify fan-out,
     push relay), packages/zero-cache/src/server/rust-syncer-bridge.ts,
     packages/rust-syncer/src/main.rs -->

1. The **TS process stays in charge**: runner, dispatcher, replicator,
   change-streamer, the custom-push endpoint, and `/statz` on the main port are
   unchanged TS code.
2. The **rust binary owns the read path**: sync WebSockets, view-syncer/IVM,
   and the CVR (Postgres reads/flushes).
3. One rust process is spawned per syncer index `i`, listening on
   `ws = 3100 + i` and `http = 3200 + i` (override:
   `ZERO_RUST_SYNCER_BASE_PORT` / `ZERO_RUST_SYNCER_HTTP_BASE_PORT`).
4. **Client upgrades**: the dispatcher reverse-proxies the raw upgrade socket
   over TCP to the rust WS port (no fd-passing; `proxyUpgradeToRust`).
5. **Replica commits**: the TS replicator's `version-ready` is relayed as
   `POST /notify` to every rust HTTP port (retried 5x with backoff, 5s/attempt
   timeout, token-gated via `NOTIFY_AUTH_TOKEN`).
6. **Custom mutations**: rust runs ZERO mutation logic — WS pushes are relayed
   to a TS loopback endpoint (`PUSHER_URL` + `PUSHER_AUTH_TOKEN`), which runs
   the real pusher → `userPushURL`. Results flow back via normal CVR pokes.
7. Legacy CRUD mutations are rejected (`create_mutagen` returns `None`).
8. Config is resolved **TS-side** (single source of truth) and handed to rust
   as env vars; the child also inherits the parent's full environment.
9. Readiness: rust prints `["ready", {"ready": true}]` on stdout only after
   binding BOTH ports; the ProcessManager gates the dispatcher on it.
10. Rust log lines are forwarded verbatim into the parent's stdout stream.

---

## 2. Configuration

<!-- Source of truth: packages/rust-syncer/src/main.rs SyncerConfig::from_env,
     packages/zero-cache/src/server/rust-syncer-bridge.ts rustSyncerEnv(),
     packages/zero-cache/src/server/main.ts loadRustSyncer().
     "Bridge" = set automatically by rustSyncerEnv/main.ts from the normalized
     TS config. "Inherit" = NOT set by the bridge; reaches rust only because the
     child spawn uses {...env, ...rustSyncerEnv(...)} — i.e. it must exist as a
     raw ENV VAR on the zero-cache process (a TS CLI flag is NOT enough). -->

### Set automatically by the TS bridge

| Env | Value forwarded | Rust default if absent |
|---|---|---|
| `PORT` / `HTTP_PORT` | per-syncer ws/http ports | 8080 / 8081 |
| `REPLICA_FILE` | replica path with file-mode applied (`-serving-copy`) | `replica.db` |
| `CVR_PG_URI` | `cvr.db ?? upstream.db` | `postgres://localhost/zero` |
| `CVR_MAX_CONNS` | `cvr.maxConns / numSyncers` (one **shared** pool per process) | 30 |
| `TASK_ID`, `SHARD`, `ZERO_APP_ID` | resolved task/shard/app id | `task-0` / `0` / `zero` |
| `AUTH_SECRET` / `AUTH_JWK` / `AUTH_JWKS_URL` / `AUTH_ISSUER` / `AUTH_AUDIENCE` | from `config.auth` | unset → opaque tokens pass unverified |
| `AUTH_REVALIDATE_INTERVAL_SECONDS` | from config | 300 (0/negative disables periodic revalidation) |
| `QUERY_URLS_JSON`, `QUERY_API_KEY`, `QUERY_ALLOWED_CLIENT_HEADERS_JSON`, `QUERY_ALLOWED_REQUEST_HEADERS_JSON`, `QUERY_FORWARD_COOKIES` | normalized `query` (or legacy `getQueries`) config | no custom-query fetch config |
| `ENABLE_QUERY_COVERING` | only an explicit `false` is forwarded | `true` (shadow/log-only) |
| `ZERO_LOG_FORMAT` / `ZERO_LOG_LEVEL` | normalized log config | plaintext / `info` |
| `ZERO_SLOW_HYDRATE_THRESHOLD_MS` | `log.slowHydrateThreshold` | 1000 ms (NB: TS default is 100 ms) |
| `NOTIFY_AUTH_TOKEN` | per-dispatcher random UUID gating `/notify` | unset → `/notify` open |
| `PUSHER_URL` / `PUSHER_AUTH_TOKEN` | loopback push-relay URL + token (only when a push/mutate URL is configured) | unset → custom pushes rejected read-only |

### Must be set as raw environment variables (inherited only)

| Env | Default | Notes |
|---|---|---|
| `ZERO_SYNCER_SHARDS` | `(host_cores * 2).clamp(16, 64)` | See "Why 2x host cores" below. |
| `MAX_CLIENT_GROUPS` | 1000 | Memory **backstop**, not a normal-operation limit. Overflow → client gets a retryable `Rehome`, never a hard reject. Tune to per-instance memory budget. <!-- main.rs:111-122, router.rs:585-607 --> |
| `ZERO_WS_DOWNSTREAM_HWM` | 4096 frames | Slow-client shed threshold (per connection, frame count). <!-- ws_server.rs:34-55 --> |
| `ZERO_WS_LIVENESS_TIMEOUT_MS` | 0 (off) | Opt-in (no TS twin, INVENTIONS.md I-14): close a connection that sent nothing for this long, e.g. `60000` ≈ 12 missed client pings. Default `0` = TS behaviour, never close an idle client. <!-- ws_server.rs DEFAULT_LIVENESS_TIMEOUT_MS --> |
| `PUSHER_QUEUE_CAP` | 1024 | Max queued relay pushes; newest dropped past cap. <!-- push_relay.rs:40-54 --> |
| `ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES` | 10 MiB | Same env the TS config layer reads — one knob for both syncers; enforced at the tungstenite layer. <!-- main.rs:495-503, ws_server.rs:30-32 --> |
| `ZERO_ADMIN_PASSWORD` | unset | Gates `/statz`, `/heapz`, and the inspector protocol. **Caveat**: must be an ENV VAR on the zero-cache process. The TS `--admin-password` CLI flag never becomes an env var, so it does NOT reach rust — with no password, production requests are denied (`NODE_ENV=development` allows). <!-- main.rs:123-125, http_server.rs:65-108 --> |
| `ZERO_SERVER_VERSION` | crate version | Reported by the inspector `version` op and as OTel `service.version`. |
| `RUST_LOG` | unset | Full tracing filter syntax; see precedence below. |
| `OTEL_*` | see §3 | Standard OTel envs, inherited. |
| `SYNCER_TRACE` | unset | Debug event-trace harness to stderr (`trace.rs`); not for production. |

`MUTAGEN_URL` is parsed but unused (mutagen is never constructed).

### Log precedence

<!-- main.rs:359-384 -->
`RUST_LOG` (full targeting syntax) → else `ZERO_LOG_LEVEL` → else `info`.
`ZERO_LOG_FORMAT=json` emits one JSON object per line — **required** when your
log pipeline parses the container stream as JSON, since the parent forwards
rust stdout verbatim and a plaintext line there is unparseable (you would drop
exactly the error lines you alert on). ANSI is always off.

### Why the shard default is 2x HOST cores (and quota-sized pools are wrong)

<!-- main.rs:132-163 (num_shards comment), 183-258 (host_parallelism,
     warn_if_quota_capped) — read these before changing the default. -->

- Each shard is a `current_thread` executor that **serializes its client
  groups**: any CG sharing a shard eats the full latency of its neighbor's
  hydrations (a single 12k-row hydrate + poke serialization holds the thread
  ~200 ms). Shards bound **tail fairness, not throughput**.
- Threads beyond the CPU count are cheap (idle shards are parked), so the
  default is sized for **CG-per-shard isolation**, not core count. Measured A/B
  on a 4-cpu-capped container (ART G25): 4 shards → 41+/51 queries breach 2x
  parity; 28 shards → 0 violations; 56 shards regressed slightly (burstier
  concurrent pokes per socket). 2x host is the measured sweet spot.
- `std::thread::available_parallelism` is **cgroup-quota-aware** on Linux (it
  returns 4 in a `--cpus 4` container), which silently recreates the
  quota-sized pool this default exists to avoid. Rust therefore reads the CPU
  **affinity mask** (`nproc` semantics, quota-independent).
- If the cgroup cpu quota is ≤ 1/3 of host cores, rust logs a warning at
  startup — it deliberately does NOT auto-shrink; tune `ZERO_SYNCER_SHARDS`
  yourself if you want a smaller pool.

---

## 3. Observability

### OTLP export mechanics

<!-- packages/rust-syncer/src/otel.rs -->

| Aspect | Behavior |
|---|---|
| Enable condition | any of `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, `OTEL_METRICS_EXPORTER` non-empty (same gating as TS `otelMetricsEnabled`) |
| Transport | **always OTLP HTTP/protobuf** to `:4318/v1/metrics`. `OTEL_EXPORTER_OTLP_PROTOCOL` is **NOT honored** — do not point rust at a gRPC-only (`:4317`) receiver. The `/v1/metrics` signal path is appended to `OTEL_EXPORTER_OTLP_ENDPOINT` by rust itself (the SDK would otherwise POST the base URL and 404). |
| Export interval | `OTEL_METRIC_EXPORT_INTERVAL` (ms) **is honored**; default 10 s |
| Resource | `service.name=zero-cache`, `service.version=$ZERO_SERVER_VERSION` |
| Histograms | `zero.sync.e2e_serving_lag` and `zero.sync.view_syncer_hydration` export as base2 **exponential** histograms (TS native-histogram parity); latency histograms use the same explicit boundaries as TS `LATENCY_HISTOGRAM_BOUNDARIES_S` |

**Changed-instruments-only export quirk**: the Rust exporter only ships series
that recorded measurements in the interval — a quiet counter simply does not
appear in that OTLP batch (unlike the TS NodeSDK, which re-reports).
Consequences for the collector/backend:
- set the collector's Prometheus-exporter `metric_expiration` **much larger**
  than the export interval (e.g. ≥ 10 min for a 10 s interval), or quiet
  counters will vanish and rate() windows will break;
- treat an **absent** series in a scrape as "no change", not zero/reset.

### Prometheus pull endpoints (rust-only, per-process `:HTTP_PORT`)

<!-- packages/rust-syncer/src/http_server.rs, metrics.rs render_prometheus -->

| Endpoint | What |
|---|---|
| `GET :HTTP_PORT/metrics` | Prometheus text: `zero_sync_active_client_groups`, `zero_sync_{hydrations,advances,pipeline_resets,expired_queries,auth_changes,client_deletions,permission_reloads,auth_revalidations,auth_revalidation_failures}_total`, `zero_sync_{hydration,advance}_time_seconds` histograms. Unauthenticated. |
| `GET :HTTP_PORT/census` | Plaintext live-object census across the 3 rust crates (leak hunting: watch `cg=` after disconnects). |
| `GET :HTTP_PORT/readyz` | 200/503; probes CVR PG (`SELECT 1`, 2 s timeout) + replica-file existence. Use as the k8s readiness probe — the stdout ready handshake can lie about PG. |
| `GET :HTTP_PORT/statz` | **Flat JSON, NOT the TS statz schema**: `{activeClientGroups, activeConnections, totalMessagesReceived, totalMessagesSent, uptimeMs, metrics{...}}`. Admin-gated (Basic auth vs `ZERO_ADMIN_PASSWORD`). ⚠️ `activeConnections`/`totalMessagesReceived`/`totalMessagesSent` are currently NOT wired (always `0`) — use the OTLP `websocket.open_connections` gauge for live connection count, not `/statz`. The TS `/statz` on the main port still exists and is unchanged. |
| `GET :HTTP_PORT/heapz` | **Stub** — returns a minimal JSON placeholder, not a V8 heap snapshot. Use `/census` (and the `dhat-heap` build feature) for memory work. |

### Metric parity table (OTLP, meter `zero`)

<!-- packages/rust-syncer/src/metrics.rs — names/types/boundaries mirror TS
     observability/metrics.ts, custom/metrics.ts, cvr-store.ts -->

**Same name + semantics as TS (dashboards keep working):**

| Instrument | Notes |
|---|---|
| `zero.sync.hydration`, `zero.sync.hydration-time`, `zero.sync.advance-time` | identical boundaries |
| `zero.sync.pipeline-resets{reason}` | |
| `zero.sync.query.transformations{result}`, `.transformation-time`, `.transformation-hash-changes`, `.transformation-no-ops` | |
| `zero.sync.e2e_serving_lag` (+ `_clamps`) | exponential histogram, same as TS |
| `zero.sync.view_syncer_hydration` | |
| `zero.server.api.requests/request_duration/attempts/attempt_duration/in_flight` | `operation="query"` only (no push/mutate on this path) |
| `zero.sync.active-clients{protocol.version}` | |
| `zero.sync.cvr.load_attempts{result}`, `cvr.load_duration`, `cvr.flush_attempts{result,flush.type}` | `flush.type` is always `sync` in rust (no deferred flush) |

**Rust-only (add to dashboards):**

| Instrument | Why |
|---|---|
| `zero.sync.websocket.{open_connections,connection_attempts,connection_successes,connection_failures{reason}}` | front-door connect SLO (`reason`: `auth`, `protocol_version`, `configuration`, `internal`, `handshake`, `rehome`, …) |
| `zero.sync.cvr.flush-failures` | leading indicator of fail_group storms |
| `zero.sync.failed-client-groups` | CGs torn down by `fail_group` (all clients rehomed) |
| `zero.sync.websocket.queued-frames` | aggregate downstream WS backlog, frames (gauge) |
| `zero.sync.websocket.queued-bytes` | aggregate downstream WS backlog, estimated bytes (gauge) |
| `zero.sync.websocket.sheds{reason}` | slow-client disconnects by reason (`byte_hwm`/`frame_hwm`/`liveness`) — alert on this rate |
| `zero.sync.cvr.pool-connections` / `pool-idle-connections` | shared CVR PgPool gauges — the prime capacity-cliff suspect |

**MISSING vs TS (dashboard migration required):**

| TS signal | Status in rust |
|---|---|
| `zero.sync.view_syncer_lag` (periodic per-CG backlog sampler) | intentionally not ported; use completion-based `e2e_serving_lag` instead <!-- metrics.rs:205-214 comment --> |
| replication/serving-lag gauges emitted by other TS workers | unchanged — those workers are still TS; only *syncer-worker* metrics moved |
| OTel **trace spans** | none — rust exports metrics only (`trace.rs` is a stderr debug harness, not OTel) |
| Anonymous product telemetry | not emitted by the rust process |
| TS `/statz` schema on the syncer | replaced by the flat JSON above |

---

## 4. Alerting starters

<!-- Signal sources: metrics.rs; failure paths: sync_engine.rs:370-410 (flush
     retry), router.rs fail_group, ws_sink.rs (shed), push_relay.rs -->

| Rule sketch | Meaning / action |
|---|---|
| `rate(zero.sync.cvr.flush_attempts{result="error"}) > 0` for 5m | CVR PG trouble / pool exhaustion / ownership churn. Each terminal flush failure rehomes+rehydrates a whole CG — this is the leading indicator of the self-amplifying reconnect storm. Check pool gauges + PG. |
| `rate(zero.sync.failed-client-groups) > ~0.1/s` sustained | CGs are being torn down (flush failures, load errors, panics). Correlate with logs `"Unable to load the client view state"` / `"Client view synchronization failed"`. |
| `rate(zero.sync.websocket.connection_failures) by (reason)` spike | `auth` → IdP/JWKS issue; `rehome` → at `MAX_CLIENT_GROUPS` or draining; `handshake`/`configuration` → LB or client rollout problem. |
| `zero.sync.websocket.queued-frames` growing without recovery | slow/stalled clients backing up; sheds (Rehome) follow when a connection crosses the HWM. Persistent growth across many conns = egress bottleneck. |
| `zero.sync.cvr.pool-idle-connections == 0` sustained (minutes) | pool acquire convoy forming; next stage is 10 s acquire timeouts → flush errors → fail_groups. Raise `CVR_MAX_CONNS` budget or shed load. |
| `up`/absent on `:HTTP_PORT/metrics`, or `/readyz` 503 | process wedged or PG/replica probe failing. |

---

## 5. Lifecycle

<!-- main.rs:282-312, 560-594 (signal handling); router.rs:905-975 (drain);
     drain.rs; main.ts:185-206 (spawn), life-cycle.ts via comments -->

| Event | Behavior |
|---|---|
| **SIGTERM** (deploys; sent by the ProcessManager) | **Staggered drain**: refuse new connections, then rehome ONE client group per interval (interval = `25s * 0.6 / total_CGs`, DrainCoordinator pacing at 60% target utilization). Whole staggered phase bounded at **25 s** (`MAX_DRAIN_MS`), then a final sweep rehomes the rest and joins executors. Fits inside a 30 s orchestrator stop-grace. |
| **SIGINT** (dev ctrl-C) | Immediate shutdown: every connection failed with `Rehome`, CG threads joined. |
| **Rehome semantics** | The client receives a retryable `Rehome` error and reconnects; the LB places it on another instance where it re-hydrates from its CVR. Used for: drain, `MAX_CLIENT_GROUPS` overflow, slow-client shed, generic `fail_group`. It is *reconnect elsewhere*, not *reset* (contrast `ClientNotFound`, §6). |
| **Deploy guidance** | Drain at the LB **first** (stop routing new upgrades), then SIGTERM. All rehomed clients reconnect through the dispatcher; if this instance is still in rotation they can land right back on it. Keep stop-grace ≥ 30 s. |
| **Orphaned-child caveat** | The rust child is spawned `detached` (own process group). If the **dispatcher is SIGKILLed** (or dies before signaling children), the rust processes are NOT killed — they keep running and keep holding the 3100+/3200+ ports, so the restarted zero-cache fails to bind. Recovery: `pkill rust-syncer` / kill the PIDs holding the ports before restart. In containers this only bites when zero-cache restarts *inside* the same container/pod. <!-- main.ts:186 `detached: process.platform !== 'win32'` --> |
| **Readiness** | `["ready",…]` on stdout after both ports bind; wire orchestrator readiness to `GET /readyz` instead (it actually probes PG + replica). |

---

## 6. Failure modes and recovery

| Failure | Behavior (verified) | Operator action |
|---|---|---|
| **JWKS outage** <!-- auth.rs:26-263 --> | JWKS cached per URL, TTL 300 s, refetch cooldown 30 s (unknown-`kid` storms cannot hammer the IdP). On refetch **failure**, rust verifies against the **stale cached keyset** (stale-grace) — an IdP blip does not disconnect-storm the tokened population at the next revalidation tick. Genuinely revoked keys still fail signature verification. First-ever fetch failing = tokens rejected (nothing cached). | Alert on `connection_failures{reason="auth"}`; no restart needed — recovery is automatic when the IdP returns. |
| **Query-API (custom query transform) hang/outage** <!-- custom_query.rs:256-400 --> | Per-attempt 30 s timeout (10 s connect), up to 4 attempts; 5xx/network errors retry with jittered backoff. A query that still fails is surfaced to that client as a `transformError` **without** dropping its healthy queries. `zero.server.api.*` instruments record every attempt with status. | Watch `api.requests{result!="success"}` and `api.in_flight`. Fix the API server; clients self-heal on next transform. |
| **Push relay outage** <!-- push_relay.rs --> | Single sequential drainer, 10 s per POST. Queue cap 1024 (`PUSHER_QUEUE_CAP`); past it, the NEWEST push is dropped and the client receives a `PushFailed` error frame — the connection stays open and the client **re-pushes** (its lmid never advanced). Non-2xx/timeout POSTs are logged; same re-push recovery. | Restore the TS side / `userPushURL`. No rust restart needed; mutations are never silently lost, only delayed. |
| **CVR pool saturation** <!-- sync_engine.rs:370-410, metrics.rs pool gauges --> | Pool acquire timeout is 10 s. A failed flush is retried **once** with 100–300 ms jitter (flush is one PG transaction — retry is safe); a second failure → `fail_group` → all the group's clients get `Rehome`. | Alert on idle==0 + flush errors (see §4). Increase `cvr.maxConns` (TS flag; becomes `CVR_MAX_CONNS`) or scale out syncers. |
| **Slow clients** <!-- ws_sink.rs, ws_server.rs --> | Per-connection downstream queue crossing the BYTE HWM (256 MiB, primary) or FRAME HWM (4096, secondary) trips `kill` — the connection is shed with `Rehome`, bounding process memory. Separately, a connection silent for 60 s (liveness) is closed. | Alert on `websocket.sheds{reason}` rate (the actual drops, by cause); watch `websocket.queued-bytes`/`queued-frames` gauges as leading indicators. Tune `ZERO_WS_DOWNSTREAM_BYTE_HWM`/`ZERO_WS_DOWNSTREAM_HWM` with a memory budget in hand (§7). |
| **Replica swap / older replica** <!-- router.rs:78-92, 1507-1520 --> | If a CVR's state version is **newer** than the serving replica (replica rolled back / restored from an older backup), the group is failed with **`ClientNotFound`** ("Cannot sync from older replica: CVR=…, DB=…") — the client **wipes local state and re-syncs fresh**. This is expected, one-time behavior after a replica restore, not a bug. | Expect a re-sync burst after restoring an older replica. No action unless it repeats without a restore. |
| **CG task panic** <!-- router.rs:3195-3210 --> | Counted in `failed-client-groups`; clients rehome. | Investigate the panic log line; file a bug. |

---

## 7. Known gaps / roadmap pointers

<!-- Verify current status in the cited files before relying on this section. -->

| Gap | Detail |
|---|---|
| **Byte-based shed** | Slow-client shed is now BYTE-aware (primary) plus frame-count (secondary). `ZERO_WS_DOWNSTREAM_BYTE_HWM` (default 256 MiB estimated-serialized, `0` disables) bounds per-connection queued bytes; `ZERO_WS_DOWNSTREAM_HWM=4096` frames is the secondary bound; `ZERO_POKE_PART_MAX_BYTES` (default 256 KiB) caps single-frame size. Live gauge `websocket.queued-bytes`; shed counter `websocket.sheds{reason}`. <!-- ws_server.rs, ws_sink.rs --> |
| **Inspector `metrics` op is a placeholder** | Returns empty TDigests (`[1000]`) for `query-materialization-server` / `query-update-server` so the client's schema parse succeeds; no real server digests are tracked. <!-- router.rs:2475-2489 --> |
| **`analyze-query` unsupported** | Inspector op returns an explicit error frame ("not supported by the rust syncer yet"). <!-- router.rs:2490-2496 --> |
| **No OTel traces** | Metrics-only OTLP. `SYNCER_TRACE=1` is a stderr debug harness, not a tracing exporter. <!-- otel.rs, trace.rs --> |
| **RSS / snapshot-close** | A periodic glibc `malloc_trim` thread (30 s cadence, Linux/gnu only) returns freed arena memory to the OS so TTL-expiry/CG-teardown churn does not read as a leak. Residual RSS-growth investigation (G6) points at SQLite-side snapshot connection holders, invisible to Rust heap profiling; `/census` + the `dhat-heap` build feature are the tools. Not fully closed — track before long-uptime rollouts. <!-- main.rs:534-551; pipeline_driver.rs destroy() --> |
| **Placeholder dispatch traits** | `ViewSyncerDispatch`/`ConnContextManagerDispatch` in `main.rs` are inert placeholders; the CG-thread path in `router.rs` owns the real logic. Cosmetic, but confusing when reading `main.rs`. |

---

## 8. Rollback

The rust syncer is opt-in behind a single switch, so rollback is a config flip,
not a code change. There is nothing rust-specific to un-migrate: the CVR
(Postgres) and replica (SQLite) are the SAME artifacts the TS syncer reads.

| Rollback | Procedure | Blast radius |
|---|---|---|
| **Rust → TS syncer (fastest)** | Unset `ZERO_SYNCER` (or set it to anything other than `rust`) and restart zero-cache. `main.ts:62` `useRustSyncer = process.env.ZERO_SYNCER === 'rust'` gates the entire rust child spawn; with it off, the process is the pure-TS view-syncer. | All clients reconnect once (LB/dispatcher rehome) and re-hydrate from their existing CVRs. No data migration. |
| **Image rollback** | Redeploy the previous zero-cache image tag. If the previous image predates the rust syncer, this also removes `ZERO_SYNCER=rust`. | Same one-time reconnect burst. |
| **Replica rollback / restore from older backup** | Covered in §6 "Replica swap / older replica": a CVR newer than the restored replica trips `ClientNotFound` and the client wipes + re-syncs fresh. Expect a re-sync burst; it is one-time and self-healing. | Fresh re-sync for clients whose CVR outran the restored replica. |

**Poisoned-CVR caveat** (see the Row-key invariant section below): a bad rowKey
written into the SHARED CVR Postgres by a buggy rust build **survives an image
revert** — the TS syncer reads the same poisoned rows and the client keeps
crash-looping. A code rollback is NOT sufficient here; the fix is a fresh client
group (client re-login → new CG → new CVR) or purging the poisoned CVR rows.
This is the one rollback that is not "just flip the switch."

**Pre-rollback checklist:** confirm the target image/config is the last known-good
(don't roll back onto another broken build), drain at the LB first (§5 Deploy
guidance) so the reconnect storm lands on healthy instances, and keep stop-grace
≥ 30 s.

---

## 9. Profiling

All rust-side profiling is feature-gated and off the hot path unless explicitly
requested, so the tools can be baked into non-prod (initial-testing) images. The
Node dispatcher's own inspector endpoints cover the TS side; these are the rust
analogs.

| Tool | How to enable | How to collect | Reads |
|---|---|---|---|
| **CPU flamegraph** (`profiling` feature, pprof-rs) | Build with `RUST_SYNCER_FEATURES=profiling` (safe in initial-testing images — the sampler only runs during an active request). <!-- Cargo.toml [features] profiling --> | `curl 'http://<HTTP_PORT>/debug/pprof/flamegraph?seconds=30' > flame.svg` — samples the WHOLE process at 99 Hz for `seconds` (default 10, cap 120) and returns an SVG. <!-- http_server.rs:170-190 --> | On-CPU time — where the executor threads actually spend cycles (serialization, IVM advance, SQLite). |
| **Heap / RSS attribution** (`dhat-heap` feature) | Build with `--features dhat-heap` (real per-allocation overhead — NOT for prod). <!-- main.rs:339-353 --> | Run a load, then SIGTERM for a graceful shutdown; dhat dumps `dhat-heap.json` (path via `ZERO_DHAT_OUT`). Open at [dh_view](https://nnethercote.github.io/dh_view/dh_view.html). | Rust-side allocations only. **SQLite page-cache / mmap / C-side allocations are invisible to dhat** — a retained snapshot connection leaks RSS while dhat stays flat (the G6 class). |
| **Live-object census** (`/census`, always on) | No feature flag. <!-- http_server.rs:326, live_count.rs --> | `curl http://<HTTP_PORT>/census` during a load run — one line per tracked counter across all three crates (engines, connections, CVR instances, …). | Which counter climbs during a load = the leak's object class. Pair with `trace.rs` (`SYNCER_TRACE=1` stderr harness) to correlate a climbing counter with the event that created it. |
| **Sanitizers** (ASan/LSan/TSan) | `parity/sanitize.sh` (pins nightly, builds WAL2 SQLite into the multiarch libdir; rust-cvr + rust-ivm are the leak-carrying crates). | Runs the unit suites under the sanitizer; TSan uses the `deadlock:unix*` suppression for SQLite's internal VFS lock-order. | Use-after-free / leaks (ASan+LSan) and data races (TSan) as a regression gate. |

**Typical workflow for an RSS climb (the recurring hard case):** `/census`
first to identify the growing object class; if it's a Rust object, `dhat-heap`
to attribute the allocation site; if census is flat but RSS still climbs, the
retention is SQLite-side (snapshot connection holders) — check `pipeline_driver.rs`
`destroy()` and the `Drop for Engine` cascade (the G6 fix), not the Rust heap.

---

*Maintainers: keep this document in sync with `SyncerConfig::from_env`
(main.rs), `rustSyncerEnv` (rust-syncer-bridge.ts), and the metric definitions
in metrics.rs/otel.rs — those are the sources of truth this runbook was
verified against.*

## Row-key invariant & ART gates (client-PK poison class)

**Invariant:** the client-facing CVR rowKey for a table MUST be keyed by that
table's **client-declared primary key** — NOT the IVM `keyCmp[0]` (the shortest
replicated unique key). When they differ (e.g. a junction table with a compound
client PK plus a shorter surrogate unique index), keying by `keyCmp[0]` stores a
rowKey missing a client-PK column, and the client crash-loops every poke with
`toPrimaryKeyString: Expected string, number or boolean. Got undefined`. The
poisoned rows persist in the shared CVR (Postgres) and survive an image revert.
See the fix in `engine::apply_client_primary_keys` (one logical key map =
`keyCmp[0]` overlaid with the client PK, threaded through source identity,
ordering, advance edit-classification, and emission — TS `#primaryKeys` parity).

**In-CI gates (this repo, always run):**
- `tests/rowkey_invariant_test.rs` — self-contained; asserts emission == client PK
  across several `client PK != keyCmp[0]` schema SHAPES. No replica/PG needed.
- `tests/rowkey_repro.rs` — hydrate-path regression.
- `tests/pg_harness.rs::pg_advance_client_pk_col_update_emits_remove_add` —
  advance-path (PG-gated): a client-PK-column update must emit REMOVE+ADD, not a
  single EDIT.

**ART gates to wire sandbox-side (the corpus that missed this used only
`id`-keyed tables where client PK == keyCmp[0]):**
1. Feed the REAL app schema into the differential oracle (golden fixtures from TS
   on the actual xyne schema, esp. compound-PK + surrogate-unique-index tables) —
   not just the reference schema.
2. Run `tests/rowkey_oracle.rs` against the live replica + client schema:
   `TEST_REPLICA_DB=... TEST_CLIENT_SCHEMA=... cargo test --no-default-features
   --test rowkey_oracle` — per-table assert emitted rowKey cols == client PK.
3. Post-run CVR poison probe: `scripts/cvr_rowkey_probe.sql` against the CVR
   Postgres; fail the run if any table shows a rowKey column-set that omits a
   client-PK column (or >1 set).
4. Make the diff-oracle compare RAW stored rowKey bytes, not just logical row
   identity (which normalizes a key-shape divergence away).
