# Layer-2 differential coverage — `syncer` crate

_Which behavioral Rust fns have their BODY pinned to REAL-TS output via a golden
fixture (drive the real TS impl → JSON → Rust replays + asserts byte-parity).
Layer-1 (parity/MAP-syncer.md) only proves a fn EXISTS on both sides; the bugs
hide in matched-fn BODIES, which is what these differentials pin._

Fixtures + generators live in `packages/rust-syncer/agentic/parity/`; each
differential is an in-crate `#[test]` gated on nothing (reads the checked-in
golden — no tsx needed at test time). Regenerate a golden with
`npx tsx agentic/parity/generate-<x>-fixture.mjs > agentic/parity/<x>-fixture.json`.

## ✅ COVERED — differential exists

| Surface (TS origin) | Rust fn | Fixture / test |
|---|---|---|
| JWT verify (auth/jwt.ts) | `auth.rs` verify_sync/verify_with_jwk(s) | auth-fixture · `jwt_parity_against_ts` |
| Read-permission transform (auth/read-authorizer.ts) | `permissions.rs` transform_query | perms-fixture · perms test |
| Serving-lag stats (workers/syncer.ts) | `serving_lag.rs` compute_serving_lag_stats_ms / _max / percentile / bounds / find_first_unserved | serving-lag-fixture · `serving_lag_parity_against_ts` |
| E2E serving-lag tracker (e2e-serving-lag.ts) | `e2e_serving_lag.rs` on_version_ready / on_version_served | e2e-serving-lag-fixture · `e2e_serving_lag_parity_against_ts` |
| Covered-query detection (query-covering.ts) | `query_covering.rs` is_query_covered_by | query-covering-fixture · `query_covering_parity_against_ts` |
| Custom-query URL allowlist (custom/fetch.ts urlMatch) | `custom_query.rs` url_match | url-match-fixture · `url_match_parity_against_ts` |
| JS-parseInt for ts/lmid (connect-params.ts getInteger) | `connect_params.rs` parse_js_integer | parse-int-fixture · `parse_int_parity_against_ts` |

The two serving-lag differentials + query-covering + url_match + parse_js_integer
were added 2026-08-25 to close the differentiable-but-unpinned gaps found when
asking "is differential coverage full?". Combined they exercise the whole
serving-lag chain (percentile/prune/watermark bounds), the covered-query
implication algebra (filter-subset / IN-range / OR / limit-paging / recursive
related / NOT-EXISTS reversal), the security-relevant URL allowlist (glob vs the
native WHATWG URLPattern — agreed on all subset cases), and the JS-quirk integer
parse (auto-hex, stop-at-`e`, truncate-at-`.`, NaN).

## ⛔ NON-DIFFERENTIABLE — byte-parity is NOT the contract (documented, not a gap)

| Surface | Why not a byte-differential |
|---|---|
| `custom_query.rs` get_cache_key / normalized_headers | Per-process in-memory dedup key; TS uses `JSON.stringify(...)`, Rust uses `url\|auth\|user\|h64(headers)\|id`. Divergent representations, each deterministic + injective per side; the key never crosses the TS↔Rust boundary. Property (determinism/injectivity), not bytes. |
| `custom_query.rs` get_backoff_delay_ms | Non-deterministic — both sides add `Math.random()` / RNG jitter to the backoff. Only the deterministic base `min(1000, 100·2^(n-1))` is comparable; the jitter breaks byte-parity by design. |

## ↪ COVERED BY A SIBLING CRATE'S ORACLE (cross-crate)

| Surface | Where it's differentiated |
|---|---|
| IVM advance-gate / pipeline ops (pipeline-driver.ts) | rust-ivm's autonomous oracle — 1822 fixtures + property tests (packages/rust-ivm/agentic/). |
| CVR flush / inspect / catchup / stateful sequence | rust-cvr's PG differentials + the stateful sequence fuzzer (packages/rust-cvr/agentic/parity/). |

## 🔩 INFRA / IO / thin-plumbing — no body to differentiate

- Transport & lifecycle: `ws_server.rs`, `ws_sink.rs`, `http_server.rs`, `router.rs`
  (CG dispatch), `connection.rs` (socket lifecycle), `otel.rs`, `metrics.rs`
  (Prometheus rendering — unit-tested), `live_count.rs`, `trace.rs`.
- `replica_schema.rs` compute_zql_specs / lite-type mapping — reads a SQLite
  replica (IO); pinned via the integration tests (`stage_e_test`, `rowkey_*`).
- `connect_params.rs` get_connect_params end-to-end — thin URL-param plumbing +
  header-forwarding (a representation detail); its one divergence-prone core,
  `parse_js_integer`, IS differentiated above; the rest is integration-tested
  (`phase2`/`phase3`/`phase6`).

## 🕳 Residual small pure candidates (low value, not yet differentiated)

- `connection.rs` classify_error_log_level / find_protocol_error — a
  message-substring log-level classifier (a few branches; unit-testable).
- `drain.rs` drain state machine — tiny; no TS test to lift cases from.
- `connection_context.rs` token pinning / pickToken — stateful auth logic
  (single-user pin); partly exercised by the auth integration tests.

**Verdict:** the crate's *differentiable pure logic* is now covered end-to-end by
7 TS-golden differentials. The remainder is genuinely non-differentiable
(transport/representation/RNG), owned by a sibling crate's oracle, or a couple of
tiny classifier helpers whose behavior is low-risk and unit-/integration-tested.
