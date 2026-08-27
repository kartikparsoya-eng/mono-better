# L8 first-run triage — 2026-08-27 (capture: full-catalog diff-oracle + mutations, TS `:local` vs rust `l8cov`)

52 confirmed TS-HOT/RUST-COLD fn-pairs (after fixing two joiner blind spots:
v0 generic-arg segments and `Cs<hash>_` tokens — now rustfilt-demangled).
Every pair dispositioned below. Verdict codes:

- **FIXED** — real divergence/drift-hazard, fixed this session (commit refs in git log)
- **RELOCATED** — the live twin runs elsewhere with ~count parity; the 1:1-file copy is dead. Not a behavior bug; tracked as drift hazard where a real duplicate impl exists
- **INVENTION** — covered by a registered rust-only invention contract (INVENTIONS.md)
- **BAD-PAIR** — ledger fuzzy/name-collision artifact, not a real twin
- **BRANCH** — rust caller exists at the mirrored site; this traffic never took the branch
- **GAP** — genuine structural divergence, tracked for its own work item

## cvr

| TS symbol (ts#) | verdict | disposition |
|---|---|---|
| rowIDSignatureUnit (325) | **FIXED** | live path (`rust_ivm::row_signature_unit` adapter, 335 runs) re-composed `h64(row_id_string)` inline; now delegates to the 1:1 `row_id_signature_unit` — one impl on the live path |
| versionToCookie (209) | **FIXED** | poke-cookie sites in `sync_engine.rs` called raw `version_string`; now call the 1:1 `version_to_cookie` wrapper |
| versionToNullableCookie (53) | BRANCH | nullable-cookie formatting: rust call sites format Some-only paths; primitive (`version_string` 982 runs) shared — no duplicate impl |
| maxVersion (232) | BRANCH | caller exists in `cvr.rs`; `cmp_versions` (the primitive, 529 runs) is shared. Watch |
| getTTLClock (110) | RELOCATED | live: `CgState::get_ttl_clock` (69 runs, syncer) |
| rowCount (48) | RELOCATED | live: `CgState::row_count` (68 runs, syncer) |
| clear (121), executeRowUpdates (19) | INVENTION | I-7 CVR write-behind flush actor replaces the row-record-cache write path; byte-parity pinned by the flush PG differential |
| close (43), cancel (17), fail (1) | INVENTION | client-handler lifecycle is owned by the pokers/ws_sink model; error semantics pinned by G36 + shed tests |
| deleteUnreferencedRows (2) | *(joiner false-cold)* | ran 2× — exactly TS's count (generic symbol, fixed demangler) |

## ivm

| TS symbol (ts#) | verdict | disposition |
|---|---|---|
| assert (110520) | BAD-PAIR | TS `assert` import binding fuzzy-paired to `assert_matches` (replay harness) |
| run (25884), createStorage (162), genPush (96), stop (136), has (232), valuesEqual (90), applyAnd (82), serializePK (142), flush/array-view (613) | INVENTION | TS server runs the zql client engine (memory sources, array views, stopable iterators) for query execution; rust replaces this layer with the SQLite-backed engine (registered architecture invention; value parity = G8/ART 0-mismatch) |
| beginFilter (138), endFilter (138), setFilterOutput (121), buildFilterPipeline (118), assertOrderingIncludesPK (337), assertNoNotExists (24) | **GAP** | TS builds WHERE/EXISTS through the filter-pipeline operator protocol (FanIn/FanOut/FilterStart/FilterEnd); rust builder uses `apply_filter` chains (284 runs) and rust `exists` does not implement the begin/end filter push protocol. Value-space converged on the full catalog (G8, exists-flip G8fix validated), but the operator-graph structure diverges from in-repo TS. Needs its own port work item — see ZERO-DIVERGENCE-PLAN Part 3 L8 follow-ups |
| simplifyCondition (450), flatten (108), isAlwaysFalse (163), isAlwaysTrue (82) | BRANCH/GAP-lite | rust callers exist (`query_impl.rs`, `read_authorizer.rs`); TS runs DNF simplification during query transform where rust's permissions path (`permissions.rs`) transforms without the simplification pass. Same results on this catalog; simplification affects transformed-AST shape/hash compat. Bundled into the filter-pipeline follow-up |
| clampTTL (99), parseTTL (99) | **FIXED**(guard) | live twins are `rust_cvr::ttl` (99 runs each — exact count parity). The dead 1:1 copies here had ALREADY drifted on out-of-contract input ("1500.5" → 0 vs 1500); fallback aligned + a cross-impl agreement test now pins the two ports to each other (proven failing pre-fix) |
| addMetric (160), isServerMetric (160) | INVENTION | rust metrics go through the otel `metrics.rs` layer, not a per-query metrics delegate |

## syncer

| TS symbol (ts#) | verdict | disposition |
|---|---|---|
| planMaintenance (52), minDefined (52), validateConnection (4), getGroupState (6), setSharedRetransformReady (4), mustGetBackgroundConnectionContext (2) | **FIXED** | the auth-maintenance loop existed but planned with its own logic and never recorded into the ported CCM. Migrated: connect/updateAuth now record `validate_connection` (client-fallback), `arm_auth_maintenance` derives the deadline from `plan_maintenance().earliest_deadline_at`, the tick executes the plan (fail → `fail_connection`, transient → `defer_maintenance`, success → `validate_connection`), and the group retransform is ONE background-connection pass gated on `due_retransform` (+ `set_shared_retransform_ready` lifecycle). Non-vacuous tests: `maintenance_honors_ccm_revalidate_deadlines`, `periodic_revalidation_disabled_never_arms_but_unauthed_is_scheduled` |
| pickToken (2) | RELOCATED | called on the CCM update path (ccm:252); TS also calls it inside `resolveAuth` at connect — rust `resolve_auth` inlines the selection (golden-tested) |
| shouldDrain (52), drainNextIn (4), draining (2) | INVENTION | rust drain = staggered SIGTERM drain + idle reaper (registered); TS elective drain-after-hydrate consult not wired — contract note added to INVENTIONS |
| reset (6609) | BAD-PAIR | fuzzy-paired to `record_reset` (metrics). TS `reset` is the pipeline-driver reset — count feeds task #145 (advancement-timeout resets) |
| initialized (309), removeQuery (103), getRow (48), destroy (2) | RELOCATED | live twins in `rust_ivm` engine/snapshotter (`Engine::remove_query` 103 runs — exact count parity; `diff::get_row` 128) |
| transformAndHashQuery (4) | BRANCH | rust caller at the mirrored site (`sync_engine.rs:730`, Client-AST + permissions); catalog drives custom queries only. Follow-up: add an AST+permissions case to the oracle catalog |
| literalArrayIncludes (2), boundsCoveredBy (1) | BRANCH | query-covering edge branches this traffic didn't hit on rust; covering decisions value-safe (over/under-retransform only) |
| compute_serving_lag_distribution_ms (336) | *(joiner false-cold)* | ran 74×+225× — TS 336 vs rust 74 computes explained by the registered 200ms scrape cache |

## Count-ratio anomalies

None ≥100× after the demangler fix.

## Recapture verification

After the fixes, a fresh capture must show hot: `row_id_signature_unit`,
`version_to_cookie`, `plan_maintenance`, `min_defined`, and (on connect)
`validate_connection` + `set_shared_retransform_ready`. That recapture is the
non-vacuous proof for the wiring fixes — the pre-fix run IS the failing state.

## Recapture RESULT (image l8cov2, same traffic, 2026-08-27)

Value space: 0 mismatches, 99/99 catalog again. Wired symbols went hot —
`row_id_signature_unit` 335x, `version_to_cookie` 66x, `plan_maintenance`/
`min_defined`/`set_shared_retransform_ready` firing, `validate_connection`
4x (exactly TS's 4). `must_get_background_connection_context` remains 0
legitimately (gated on `due_retransform`; no retransform deadline elapsed in
the 5-minute window). Cold pairs: syncer 18→14, cvr 10→8, ivm 24 (tracked
gap + guarded-dead ttl copies). The pre-fix capture is the proven-failing
state; this run is the fix proof.
