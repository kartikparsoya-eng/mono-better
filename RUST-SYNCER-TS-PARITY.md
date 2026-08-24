# Rust Syncer — TS ↔ Rust Behavior Parity

> **Companion to** [`RUST-SYNCER-ARCHITECTURE.md`](./RUST-SYNCER-ARCHITECTURE.md) (§15).
> The main doc maps *which Rust module ports which TS file*. This doc maps **behavior**: where the Rust port matches TS exactly, where it deliberately differs (and why), and where a TS feature is intentionally absent.
>
> Harvested from parity/divergence comments in the actual code across `rust-syncer`, `rust-ivm`, `rust-cvr` on branch `rust-cvr-v1.0.0`. Line numbers are anchors.

---

## How to read this

Every entry is tagged:

- 🟰 **EXACT PARITY** — Rust reproduces TS behavior on purpose.
- ⚠️ **INTENTIONAL DIVERGENCE** — Rust deliberately differs (usually stricter/safer). These are the ones to know.
- ⛔ **NOT IMPLEMENTED** — a TS feature intentionally absent.
- 🚧 **KNOWN DIVERGENCE / GATED** — a real difference that is documented and release-gated (mostly benign or fixture-blocked).

---

## ⚠️ The divergences that matter most (read these first)

These are the deliberate places Rust is **stricter or safer than TS**. If you're reasoning about correctness differences between the engines, start here.

| # | Divergence | Where | TS behavior → Rust behavior | Why it's safer |
|---|---|---|---|---|
| 1 | **`ws_id`-guarded push failure** | `router.rs:460-461` | TS routes a failed push error by `clientID` only → Rust guards by **both `clientID` and `ws_id`** | The client may have reconnected (new socket) by the time a relay POST fails; TS would spuriously kill the *new* socket for the *old* push. Rust drops the stale frame. |
| 2 | **Fail-closed permissions (load)** | `permissions.rs:32-56` | An existing-but-unloadable permissions doc → TS **throws** (tears down CG) → Rust substitutes **deny-all** and keeps serving | No unauthorized row is ever served; the rest of the CG stays up. |
| 3 | **Fail-closed permissions (reload)** | `permissions.rs:905-912` | Transient hash-read error → TS lets it propagate → Rust returns **`Unchanged`** (keeps working perms) | A blip doesn't clobber a working permission set; persistent problems still surface via the reset path. |
| 4 | **Unique `task_id` fallback** | `main.rs:101-122` | `TASK_ID` unset → TS **asserts** presence → Rust generates a unique `task-auto-{pid}-{nanos}` | A shared constant `TASK_ID` would collapse the CVR **ownership lease** (two instances each satisfy `owner==task_id`), permitting interleaved lost updates. |
| 5 | **Bounded downstream/handoff queues** | `sync_engine.rs:380`, `ws_server.rs` | TS uses **unbounded** queues → Rust bounds them (frame + byte HWM) and sheds a slow client | Caps per-connection memory against a stalled peer instead of buffering pokes without bound. |
| 6 | **JWT `nbf` + `leeway` fixed for parity** | `auth.rs:84-99` | `jsonwebtoken` defaults (`validate_nbf=false`, `leeway=60s`) would be **laxer** than TS's `jose` → Rust forces `validate_nbf=true`, `leeway=0` | Without these, Rust would accept future-dated / 60s-expired tokens that TS rejects — a real auth hole, closed. |

Everything else below is grouped by subsystem.

---

## File map: TS → Rust

Which Rust file(s) port each TypeScript source. Directions is **TS → Rust** (the inverse of the main doc's Rust→TS map). Every mapping below was extracted from the actual `.ts` paths cited in the Rust sources — grep a Rust file for the TS filename to find the exact call sites. A Rust file can appear under several TS sources (it fuses them); a TS file can map to several Rust files (it was split).

### `packages/zero-cache/src/workers/` — connection front door

| TS file | Rust file(s) |
|---|---|
| `syncer.ts` | `rust-syncer/src/router.rs`, `ws_server.rs`, `lib.rs`, `main.rs` |
| `connection.ts` | `rust-syncer/src/connection.rs`, `ws_server.rs` |
| `connect-params.ts` | `rust-syncer/src/connect_params.rs`, `ws_server.rs` |
| `url-params.ts` | `rust-syncer/src/connect_params.rs` |
| `syncer-ws-message-handler.ts` | `rust-syncer/src/message_handler.rs` |
| `connection-context-manager.ts` | `rust-syncer/src/connection_context.rs`, `router.rs` |
| `dispatcher.ts` | `rust-syncer/src/lib.rs` |

### `packages/zero-cache/src/services/view-syncer/` — the view syncer + CVR

| TS file | Rust file(s) |
|---|---|
| `view-syncer.ts` | `rust-syncer/src/sync_engine.rs`, `change_processor.rs`*, `pipeline_driver.rs`, `lib.rs` |
| `pipeline-driver.ts` | `rust-syncer/src/pipeline_driver.rs`; `rust-ivm/src/engine/mod.rs`, `ivm/source.rs`, `streamer/mod.rs`, `snapshotter/snapshotter.rs` |
| `cvr.ts` | `rust-cvr/src/cvr.rs`, `updater.rs`, `types.rs`, `row_record_cache.rs`; `rust-syncer/src/sync_engine.rs` |
| `cvr-store.ts` | `rust-cvr/src/store.rs` |
| `client-handler.ts` | `rust-cvr/src/client_handler.rs`, `otel_metrics.rs` |
| `row-record-cache.ts` | `rust-cvr/src/row_record_cache.rs`, `otel_metrics.rs` |
| `row-set-signature.ts` | `rust-cvr/src/row_set_signature.rs` |
| `schema/types.ts` | `rust-cvr/src/types.rs` |
| `query-covering.ts` | `rust-syncer/src/query_covering.rs` |
| `e2e-serving-lag.ts` | `rust-syncer/src/e2e_serving_lag.rs` |
| `drain-coordinator.ts` | `rust-syncer/src/drain.rs` |

\* `change_processor.rs` ports `ViewSyncer.#processChanges` specifically.

### `packages/zero-cache/src/auth/` — auth & permissions

| TS file | Rust file(s) |
|---|---|
| `jwt.ts` | `rust-syncer/src/auth.rs` |
| `auth.ts` | `rust-syncer/src/auth.rs`, `connection_context.rs`, `router.rs` |
| `read-authorizer.ts` | `rust-syncer/src/permissions.rs` |

### `packages/zero-cache/src/` — mutations, custom queries, types, db, config, observability

| TS file | Rust file(s) |
|---|---|
| `pusher.ts` / `mutation.ts` | `rust-syncer/src/push_relay.rs` |
| `custom-queries/transform-query.ts` | `rust-syncer/src/custom_query.rs` |
| `custom-queries/fetch.ts` | `rust-syncer/src/custom_query.rs`, `metrics.rs` |
| `types/row-key.ts` | `rust-cvr/src/row_key.rs`, `lib.rs` |
| `types/lexi-version.ts` | `rust-cvr/src/version.rs` |
| `db/lite-tables.ts` · `db/lite.ts` · `db/pg-data-type.ts` | `rust-syncer/src/replica_schema.rs` |
| `observability/metrics.ts` | `rust-syncer/src/metrics.rs`, `otel.rs`; `rust-cvr/src/otel_metrics.rs` |
| `server/otel-start.ts` · `…/enabled.ts` | `rust-syncer/src/otel.rs`, `main.rs` |
| `config/zero-config.ts` | `rust-syncer/src/main.rs` |

### `packages/zero-protocol/src/` and `packages/shared/src/`

| TS file | Rust file(s) |
|---|---|
| `zero-protocol/src/protocol-version.ts` | `rust-syncer/src/protocol.rs` |
| `zero-protocol/src/error.ts` | `rust-syncer/src/protocol.rs` |
| `shared/src/hash.ts` | `rust-cvr/src/hash.rs`, `lib.rs` |

### `packages/zql/src/ivm/` — IVM operators (mostly 1:1 by name)

| TS file | Rust file | | TS file | Rust file |
|---|---|---|---|---|
| `filter.ts` | `rust-ivm/src/ivm/filter.rs` | | `fan-out.ts` | `ivm/fan_out.rs` |
| `filter-operators.ts` | `ivm/filter_operators.rs` | | `union-fan-in.ts` | `ivm/union_fan_in.rs` |
| `filter-push.ts` | `ivm/filter_push.rs` | | `union-fan-out.ts` | `ivm/union_fan_out.rs` |
| `join.ts` | `ivm/join.rs` | | `view.ts` · `view-apply-change.ts` · `format.ts` | `ivm/view.rs` |
| `flipped-join.ts` | `ivm/flipped_join.rs` | | `array-view.ts` | `ivm/array_view.rs` |
| `join-utils.ts` | `ivm/join_utils.rs` | | `catch.ts` | `ivm/catch.rs` |
| `take.ts` | `ivm/take.rs` | | `snitch.ts` | `ivm/snitch.rs` |
| `cap.ts` | `ivm/cap.rs` | | `change.ts` · `change-type-enum.ts` | `ivm/change.rs` |
| `exists.ts` | `ivm/exists.rs`, `ivm/node_filter.rs` | | `source.ts` | `ivm/source.rs`, `ivm/change.rs` |
| `skip.ts` | `ivm/skip.rs` | | `memory-source.ts` | `ivm/source.rs`, `sqlite/table_source.rs`, `ivm/join_utils.rs` |
| `fan-in.ts` | `ivm/fan_in.rs` | | `memory-storage.ts` | `ivm/memory_storage.rs` |
| `operator.ts` | `ivm/operator.rs`, `ivm/constraint.rs` | | `constraint.ts` | `ivm/constraint.rs` |
| `data.ts` | `ivm/data.rs`, `ivm/stream.rs` | | `stream.ts` · `skip-yields.ts` | `ivm/stream.rs` |
| `schema.ts` | `ivm/schema.rs` | | `push-accumulated.ts` | `ivm/push_accumulated.rs` |
| `stopable-iterator.ts` | `ivm/stopable_iterator.rs` | | | |

### `packages/zql/src/builder/` and `planner/`

| TS file | Rust file(s) |
|---|---|
| `builder.ts` | `rust-ivm/src/builder/builder.rs` |
| `ast.ts` | `rust-ivm/src/builder/ast.rs`; `rust-ivm/src/streamer/mod.rs`; `rust-syncer/src/permissions.rs` |
| `expression.ts` | `rust-ivm/src/builder/expression.rs`; `rust-syncer/src/permissions.rs` |
| `complete-ordering.ts` · `escape-like.ts` · `like.ts` · `named.ts` · `validate-input.ts` · `typed-view.ts` · `query-impl.ts` · `query-internals.ts` · `query-registry.ts` · `query-delegate*.ts` · `static-query.ts` · `runnable-query-impl.ts` · `create-builder.ts` · `schema-query.ts` · `measure-push-operator.ts` · `metrics-delegate.ts` | matching `rust-ivm/src/builder/*.rs` (1:1 by name) |
| `planner-*.ts` (`builder`, `connection`, `constraint`, `fan-in`, `fan-out`, `graph`, `join`, `node`, `source`, `terminus`) | matching `rust-ivm/src/planner/*.rs` (1:1 by name) |

### `packages/zql/src/` — SQLite table source, query, snapshotter

| TS file | Rust file(s) |
|---|---|
| `ivm/table-source.ts` | `rust-ivm/src/sqlite/table_source.rs` |
| `ivm/query-builder.ts` | `rust-ivm/src/sqlite/query_builder.rs` |
| `ivm/database-storage.ts` | `rust-ivm/src/sqlite/database_storage.rs` |
| `db.ts` · `sql-inline.ts` · `sqlite-cost-model.ts` · `sqlite-stat-fanout.ts` | `rust-ivm/src/sqlite/{db,sqlite_cost_model,sqlite_stat_fanout}.rs` |
| `explain-queries.ts` · `options.ts` · `query-delegate.ts` · `resolve-scalar-subqueries.ts` | matching `rust-ivm/src/sqlite/*.rs` |
| `query/ttl.ts` | `rust-ivm/src/builder/ttl.rs`; `rust-cvr/src/ttl.rs` |
| `snapshotter.ts` · `change-log.ts` · `constants.ts` · `specs.ts` | `rust-ivm/src/snapshotter/{snapshotter,diff,mod,spec}.rs` |

> **Note on directory prefixes:** filenames are exact (cited in the Rust code); a few TS *directory* prefixes (`observability/`, `server/`, `config/`, `custom-queries/`) are best-effort — grep the mono TS tree for the filename if you need the canonical path. The `types/`, `services/view-syncer/`, `workers/`, `db/`, `auth/` prefixes are confirmed from full paths cited in the Rust sources.

---

## A. Connection / WebSocket lifecycle

| Tag | Item | Where |
|---|---|---|
| 🟰 | WS acceptance + connection lifecycle ported from `syncer.ts` + `connection.ts` | `ws_server.rs:1` |
| 🟰 | `websocketMaxPayloadBytes` = 10MB, reject before parse | `ws_server.rs:30` |
| 🟰 | Reconnects that end without an RFC 6455 close handshake treated as normal (Node `ws` behavior) | `ws_server.rs:513` |
| ⚠️ | JavaScript `parseInt` truncation replicated (`"123.9"` → `123`) for connect-param parity | `connect_params.rs:149` |
| 🟰 | `checkClientAndCVRVersions` equivalent — distinguishes purged/missing CVR from stale | `router.rs:52` |
| ⚠️ | **`ws_id`-guarded push failure** (see divergence #1) | `router.rs:460-461` |
| 🟰 | ActiveClients GC + idempotent shutdown cleanup match TS | `router.rs:2760`, `:4701` |
| ⚠️ | `PipelineDriver` intentionally **not** `Send`/`Sync` (Engine holds `Rc<RefCell<..>>`), mirroring TS's single-threaded mutable class instances | `pipeline_driver.rs:7` |

---

## B. Auth & permissions

| Tag | Item | Where |
|---|---|---|
| 🟰 | JWT config precedence `jwk → secret → jwksUrl` | `auth.rs:1-2` |
| 🟰 | JWKS caching mirrors `createRemoteJWKSet` singleton; TTL 300s; refetch cooldown 30s | `auth.rs:29`, `:35`, `:42` |
| ⚠️ | **`validate_nbf=true`, `leeway=0`, explicit required-claims** to match `jose`/TS (see divergence #6) | `auth.rs:84-99` |
| 🟰 | Fail-closed JWKS refetch within cooldown (serve cached, don't hammer IdP) | `auth.rs:189` |
| 🟰 | Algorithm-confusion prevention: reject HMAC on kid-miss | `auth.rs:355` |
| ⚠️ | **Fail-closed permissions on load** (see divergence #2) | `permissions.rs:32-56` |
| ⚠️ | **Fail-closed permissions on reload** (see divergence #3) | `permissions.rs:905-912` |
| 🟰 | Deny-by-default, allow-rule WHERE merging, static-parameter binding | `permissions.rs:1058` |
| ⚠️ | **Unique `task_id` fallback** (see divergence #4) | `main.rs:101-122` |
| 🟰 | Existing-but-unloadable permissions → deny all queries, keep CG serving | `main.rs:774-778` |

---

## C. Protocol / message handling

| Tag | Item | Where |
|---|---|---|
| 🟰 | Message dispatch ported from `syncer-ws-message-handler.ts` | `message_handler.rs:1`, `:152` |
| 🟰 | Ack mutation responses via `_zero_cleanupResults` push (type `single`) | `message_handler.rs:131` |
| 🟰 | Delete-client mutations + cleanup-results paths | `message_handler.rs:141-152` |
| 🟰 | Admin-password gate: `isAdminPasswordValid` shape + 401 + constant-time compare | `http_server.rs:63`, `:84`, `:412` |

---

## D. View-syncer / CVR hot path

| Tag | Item | Where |
|---|---|---|
| 🟰 | `hydrate_and_sync` / `advance_and_sync` port napi `HydrateAndSyncTask` / `AdvanceAndSyncTask` | `sync_engine.rs:1140`, `:1252` |
| 🟰 | One shared CVR pool, one store created once (TS one-pool-per-worker) | `sync_engine.rs:200`, `:214` |
| 🟰 | Row-write dedup (TS `#flush` pending-row dedup): drop no-op row ops | `sync_engine.rs:329` |
| ⚠️ | Handoff queues **bounded** vs TS unbounded (see divergence #5) | `sync_engine.rs:380` |
| 🟰 | `#syncQueryPipelineSet`, `#catchupClients`, version-min across clients | `sync_engine.rs:619`, `:898`, `:994` |
| 🟰 | Row-set-signature snapshot + seed from prior full signature | `sync_engine.rs:1109`, `:1124` |
| 🟰 | `contentsAndVersion` (drops `_0_version`), row-op no-op detection (both TS drop conditions) | `sync_engine.rs:1657`, `:1672` |
| ⚠️ | Row-set-signature maintenance is **caller-driven, not operator-driven** (matches napi path) | `pipeline_driver.rs:395-396` |
| 🟰 | Engine-panic caught → surfaced as error for TS teardown parity (not abort) | `pipeline_driver.rs:451-454`, `:526` |
| 🟰 | `change_processor` ports `ViewSyncer.#processChanges` | `change_processor.rs:1` |
| 🟰 | `row_record_cache` invariants preserved verbatim; identical SQL template | `row_record_cache.rs:3-14`, `:166` |
| 🚧 | **`merge_ref_counts` drops literal zeros** where TS retains them — benign, no functional impact documented | `cvr.rs:38` |
| ⚠️ | `cmp_cvr` treats `configVersion None` vs `Some(0)` as unequal in `Eq` but equal in `Ord` (TS `?? 0`); `CVRVersion` deliberately does **not** impl `Ord` | `version.rs:63-67` |
| ⚠️ | No `cookie_to_version` wrapper (it wrapped a panicking `version_from_string` — a foot-gun) | `version.rs:108` |
| 🚧 | **BigInt rowKey > u64::MAX**: TS preserves full-precision decimal strings; `serde_json::Number` can't. **Phase A release blocker** — add deref-bigint-in-rowkey fixtures before Phase B | `row_key.rs:11-21` |
| 🚧 | rowKey cache is explicit-clear, **not** a `WeakMap` (different eviction model) — documented | `row_key.rs:156` |
| 🚧 | Eviction tie-break order: TS insertion-order vs Rust `BTreeMap` order — benign (consumers are order-independent) | `parity_check.rs:449` |

---

## E. IVM engine

| Tag | Item | Where |
|---|---|---|
| 🟰 | Faithful port: `Iterable`→`Iterator`, `yield` token dropped, `Input`/`Output` traits, single-threaded graph | `lib.rs:1-8` |
| 🟰 | Row-set-signature tracking (`#rowSetSignatures`) | `engine/mod.rs:80-84` |
| 🟰 | Advance-abort economic circuit breaker (`#shouldAdvanceYieldMaybeAbortAdvance`); shared `MIN_ADVANCEMENT_TIME_LIMIT_MS` | `engine/mod.rs:103-104`, `:111` |
| 🟰 | Scalar-subquery companion pipelines | `engine/mod.rs:131` |
| ⚠️ | **Take boundary asserts kept as raw panics** matching TS exactly — deliberately **not** converted to `-2` in-place resets (reset re-hydrates anyway; no WAL benefit; TS reserves resets for scalar-subquery/permissions/schema-change/truncation) | `take.rs:37-48`, `:695` |
| 🚧 | **Streaming-hydrate snapshot divergence unique to Rust**: async hydrate may read an empty-looking partition, then a later advance carries an Edit for it. TS's *synchronous* hydrate reads the same frame it advances from, so this can't happen there. Handled with a **deliberate panic (TS teardown parity)**; kept rare by the streaming-hydrate completeness fix. Observed on prod (`take.rs:670` "Bound should be set") | `take.rs:698-702` |
| 🟰 | **Take-bound divergence fix (2026-08-12)**: VALUE-aware NULL guard — a NULL start value takes NULL-aware SQL branches even on a declared non-optional column, fixing an always-false `(col > ?) OR (col = ?)` that returned EMPTY | `table_source.rs:1130`, `query_builder.rs:312` |
| 🟰 | Swallowed SQLite write would silently diverge persisted operator state → Rust panics to match better-sqlite3 `.run()` throw | `database_storage.rs:103` |

---

## F. Mutations / push

| Tag | Item | Where |
|---|---|---|
| ⛔ | **CRUD mutations not processed** — `create_mutagen` returns `None`; legacy CRUD hits the read-only rejection | `main.rs:715` |
| ⚠️ | **Custom mutations relayed, not run** — with `PUSHER_URL` set, a custom push is forwarded (with auth/headers) to the TS push endpoint; results flow back through the `lmids`/`mutationResults` queries this syncer already hydrates + pokes | `main.rs:726-734`, `push_relay.rs` |
| 🟰 | Sequential (ordered) pokes vs TS `Promise.allSettled` — Rust actor-thread pokes are strictly ordered | `client_handler.rs:880` |

---

## G. Config / readiness / draining / metrics

| Tag | Item | Where |
|---|---|---|
| 🟰 | **CVR readiness not hardened** — reports ready even with CVR unreachable, connects lazily (TS `warmupConnections` + `Promise.allSettled` tolerance); `/readyz` reports true health | `main.rs:476-485` |
| ⚠️ | **Shard pool sized from host parallelism (affinity mask), not cgroup quota** — quota-sized `current_thread` shards serialize whole CGs and destroy tail latency (A/B: ART G25). Deliberately no auto-shrink; operators tune via `ZERO_SYNCER_SHARDS` | `main.rs:69`, `:232-238` |
| ⚠️ | Replica **creation version** and live **replication watermark** deliberately distinct (a restored replica keeps creation version while the watermark advances) | `replica_schema.rs:24` |
| 🟰 | `otelMetricsEnabled()` port; OTLP push to the same collector as TS | `metrics.rs:24`, `otel.rs:24` |
| ⛔ | **`zero.sync.view_syncer_lag` not ported** — TS samples backlog periodically over a central CG registry; Rust has no such registry (per-CG dedicated executors), and adding cross-executor shared state on the advance hot path isn't justified for a purely-observational metric. Completion-based `e2e_serving_lag` (ported) already captures served-version lag | `metrics.rs:205-214` |
| ⛔ | Advance-cost placement metric is a deliberate V2/V3 follow-up (V1 balances by group count) | `router.rs:917` |
| 🟰 | Conservative query-covering analysis — any un-understood case returns `false` (shadow-mode, log-only) | `query_covering.rs:7` |
| 🟰 | `e2e-serving-lag.ts` ported (#6157/#6312) | `e2e_serving_lag.rs:1` |

---

## Release-gating summary

The parity items that are **not yet closed** (track these before promoting):

| Item | Status | Action |
|---|---|---|
| BigInt rowKey > u64::MAX (`row_key.rs:11-21`) | 🚧 Phase A blocker (no fixtures exercise it yet) | Add deref-bigint-in-rowkey fixtures **before Phase B** |
| Streaming-hydrate Take divergence (`take.rs:698-702`) | 🚧 rare, panics on divergence (TS teardown parity) | Kept rare by completeness fix; monitor prod panic counters |
| `view_syncer_lag` metric (`metrics.rs:205-214`) | ⛔ intentionally absent | None — `e2e_serving_lag` covers it |

Everything else is either exact parity or a documented, deliberately-safer divergence.
