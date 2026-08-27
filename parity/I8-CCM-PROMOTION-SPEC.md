# I-8 CCM Promotion — Staged 1:1 Spec (task #155)

**Divergence:** TS keeps ONE owner of per-connection state,
`ConnectionContextManager` (connection-context-manager.ts), read at use time via
`mustGetConnectionContext(selector)`. Rust splits it across a dead
`PlaceholderConnContextManager` (returns `auth:None`) + four `CgState` maps
(`client_raw_auth`, `client_auth`, `client_query_ctx`, `PushRelayHeaders`). The
ported `connection_context_manager.rs` exists but is a "tested reference," not the
live owner. This split is the soil bug-2 grew in.

**1:1 target:** the live path uses `connection_context_manager.rs`'s existing
types/methods (`ConnectionContextManager`, `ConnectionContext`,
`must_get_connection_context`, `init_connection`, `update_auth`) — SAME file,
SAME function names as TS. No new invented struct. Every consumer reads context
at USE time (rule 9).

## Why it's staged (not one commit)
Each stage keeps ALL existing tests green and is independently ART-gateable. Never
leave the live auth path half-rewired.

### Stage 0 — parity map — DONE (2026-08-27)
Enumerate every read/write of the four maps → TS `ConnectionContext` field/op.

`client_raw_auth` (TS `auth.raw`):
| router.rs | op | TS op |
|---|---|---|
| 2111 | insert @ connect | `init_connection` |
| 2749 / 2730 | write / unchanged-check @ updateAuth | `updateAuth` (`authEquals`) |
| 2430 | read for custom-query Bearer | `mustGetConnectionContext().queryContext` |
| 1859 / 1895 | presence read | context read |
| 2082 / 2982 / 3381 | remove / clear | `deleteConnection` / teardown |

`client_auth` (TS `auth` decoded claims):
| 2107 | insert @ connect | `init_connection` |
| 2748 | insert @ updateAuth | `updateAuth` |
| 2549 / 3281 | read for transform | `mustGetConnectionContext()` |
| 2981 / 3380 | remove / clear | teardown |

`client_query_ctx` (TS `queryContext`):
| 2115 | insert @ connect | `init_connection` |
| 2437 | modify @ initConnection (userQueryURL) | `handleInitConnection` |
| 2751 | auth refresh @ updateAuth | `updateAuth` |
| 1967 / 2582 / 3301 | read for transform | `mustGetConnectionContext()` |
| 2084 / 2984 / 3382 | remove / clear | teardown |

**Use-time reads that must go through `must_get_connection_context` post-promotion:**
router.rs 2430 (query Bearer), 2549 / 3281 (claims), 1967 / 2582 / 3301 (queryCtx),
+ push_relay auth cell, + mutagen path (already reads the CCM). Writes concentrate
at connect (`init_connection`), `handle_update_auth` (`updateAuth`),
`handle_desired_queries` (`handleInitConnection`); removals at
`on_connection_closed` + group teardown. No read/write is outside these TS ops —
the promotion is mechanical, not behavioral.

### Stage 1 — ported CCM as parallel live owner (dual-write)
- **1.0 — DONE (commit da8b55583):** `CgState` holds the ported
  `ConnectionContextManager` (`Arc<Mutex>`, uncontended on the CG thread).
  `on_new_connection` dual-writes `register_connection`; `handle_update_auth`
  dual-writes `update_auth`. Maps stay AUTHORITATIVE → zero behavioral change
  (dual-write best-effort, logged-on-error). Non-vacuous test
  `i8_stage1_ccm_tracks_connection_and_auth_via_dual_write`.
- **Stage-1.0 note CORRECTED (2026-08-27):** the learning "CCM `resolve_auth`
  requires a userID … anonymous/opaque paths need a userID or the CCM rejects"
  was half-wrong. Verified against TS `resolveAuth` (auth.ts:49-123) AND the rust
  1:1 port (connection_context_manager.rs:219-270): a userID is required **only
  when a token is provided** (auth.ts:79-85). With **no** token, `resolveAuth`
  returns `undefined` (anonymous ALLOWED, auth.ts:74-77). So the rust router
  admitting anonymous (no-token) connections is FAITHFUL to TS — there is **no
  anonymous-opaque divergence**. The CCM promotion is therefore pure
  state-ownership de-duplication of the LATENT I-8 split, not a correctness fix.
  Pinned by `resolve_auth_matches_ts_anonymous_and_userid_branches` (non-vacuous).
- **1.1 — DONE (this session):** the CCM is now a COMPLETE, verified shadow of
  the live maps.
  - `ccm_register` seeds connect-time auth via `resolve_auth(None, user_id, wire,
    None)` → `Opaque{raw}` (modern path). `push_config`/`validate_legacy_jwt`/`now`
    stay `None` by design (see below).
  - `ccm_init_connection` dual-writes `init_connection` (userQueryURL/Headers,
    userPushURL/Headers) in `handle_desired_queries` on a real initConnection.
  - `ccm_close_connection` dual-writes `close_connection` in `on_connection_closed`.
  - Golden test `i8_stage1_ccm_tracks_connection_and_auth_via_dual_write` now
    asserts the CCM's seeded auth == live `client_raw_auth` at connect, updateAuth
    flows through, and close drops the CCM entry. Non-vacuous (seed→None fails it).
  - **`push_config`/`validate_legacy_jwt`/`now` intentionally `None`:** no Stage-2
    consumer reads the CCM's `mutate_context` (the push relay is invention I-3 and
    keeps its own connect-time relay fields; only its AUTH cell maps to the CCM);
    `now` defaults to `now_ms`; the modern path has no legacy JWT validator (TS
    `validateLegacyJWT` is undefined → `resolveAuth` returns `opaque`, auth.ts:108).
  - **KEY FINDING that de-risked authData:** TS passes `ctx.auth?.raw` (the RAW
    token) to the transform (view-syncer-test-util.ts:861/1040) and decodes
    `authData` at use time — the CCM stores **opaque** for modern JWTs, NOT decoded
    claims. So NO `JwtPayload` restructure is needed: the authData consumer will
    decode `must_get_connection_context(sel).auth.raw()` at use time, matching the
    live `client_auth = decode_jwt_claims(token)`.

### Stage 2/3 — PROGRESS (this session)
Code cleaned to a 1:1 form first (user directive): the `ccm_*` wrapper methods
and all "Stage N"/"dual-write" labels were removed from the source; call sites
now invoke the CCM's TS-named methods inline (`register_connection`,
`update_auth`, `init_connection`, `close_connection`,
`must_get_connection_context`). Staging lives ONLY in this doc.

Consumers migrated to read the CCM at use time + maps deleted:
- **authData** → `must_get_connection_context(sel).auth?.raw` decoded at use time
  (TS `ctx.auth?.raw`). `69dfb21e2`.
- **auth maintenance / revalidation** (arm "any auth", the revalidation set, the
  updateAuth unchanged-check) → the CCM. Register now precedes arm (TS order).
  `073670c49`.
- **`client_auth` map DELETED** — first parallel auth map gone. `9d21de9f4`.
Each migration is CI-green with a non-vacuous test; the userID-bearing (JWT) path
is byte-identical, the opaque-no-userID edge is the ART-validated delta.

**REMAINING + a design fork on `client_query_ctx`:** the CCM's `query_context`
(`ConnectionFetchContext`) is, per connection_context_manager.rs:56-62, a
STRUCTURAL port that is NOT on the live query-fetch path. The real
`CustomQueryContext` is built inline in `default_query_context` from FetchConfig +
ConnectParams and carries fields the CCM does not model — notably the #6144
forwarded `request_headers` (allowlisted `x-forwarded-*`), plus `user_id`. So
`client_query_ctx` cannot be flipped to the CCM's `query_context` as-is. Options:
  (a) port the #6144 header-forwarding into the CCM's `build_fetch_context` so the
      CCM owns the full query context, then flip; or
  (b) keep `client_query_ctx` as a rust query-fetch structure (an I-3-class
      invention) and migrate ONLY its `auth` field to the CCM (sourcing the Bearer
      from `must_get_connection_context(sel).auth` at build time), which then frees
      the last `client_raw_auth` read for deletion.
Option (b) is smaller and keeps auth single-owned; (a) is the fuller 1:1. Pick per
review. The push-relay `PushRelayHeaders.auth` flip is independent (needs the
`Arc<Mutex<CCM>>` + selector plumbed into the message handler).

**DECISION: Option A. CCM hardened for it (this session):**
- `bc0396582` port #6144 request-header forwarding into the CCM (HeaderOptions
  `request_headers` + ConnectParamsForRegistration `request_headers` +
  `filter_headers` = TS `filterHeaders`); the CCM now owns the FULL query context.
- `83b2bede7` fix: `init_connection` filters `customHeaders` by the allowlist
  (TS `filterHeaders(userQueryHeaders, allowedClientHeaders)`, :306/:324) — a
  latent divergence in the reference CCM, corrected before it goes live.
Both behaviorally inert today (query_context not yet read on the live path).

**Step 2/3 — DONE (query-context migration commit):**
- Step 2: the 3 `client_query_ctx` read sites (router.rs config_and_hydrate x2 +
  on_auth_maintenance_tick validate) now build `CustomQueryContext` from
  `must_get_connection_context(sel)` via the new `custom_query_context_from`
  adapter + `CgState::query_context_for` helper — maps url/allowed_urls/api_key/
  cookie/origin from `query_context`, `client_headers`←`custom_headers`,
  `request_headers`←`request_headers` (both HashMap→sorted Vec), `auth`←`ctx.auth
  .raw()`, `user_id`←`ctx.user.id`; returns `None` when `query_context.url` is
  None. `custom_query_context_from`/`query_context_for` are labeled rust-only
  adapters (no TS twin: TS `transform-query.ts` reads `ctx.queryContext` fields
  inline; rust flattens onto the ported `CustomQueryContext`).
- Step 3: DELETED `client_query_ctx`, `client_raw_auth`, `default_query_context`,
  `filtered_query_headers`, and the dead `CgState.query_config` field (the CCM
  owns the fetch config). The register-time inserts, the initConnection
  client_query_ctx block, the updateAuth mutation, and all removes/clears are gone.
- Golden: `configured_query_context_matches_typescript_defaults_and_header_filtering`
  + `forwards_allowlisted_incoming_request_headers` now drive a real register +
  initConnection through the CCM and assert `custom_query_context_from` reproduces
  every TS-config-derived field. NON-VACUOUS proven (break the `auth` mapping →
  golden FAILS).
- Teardown/auth-maintenance/authData tests repointed off the deleted maps to the
  CCM (`ccm_raw_auth` test helper; teardown asserts the CCM connection is gone).

**Follow-up discovered (separate focused commit) — opaque-token updateAuth pin
divergence:** `handle_update_auth` decodes the token (`decode_jwt_claims`) and does
a **sub-based** single-user pin check, which wrongly CLOSES any opaque-token
refresh once the group is pinned (opaque tokens carry no `sub`). TS (modern path,
`validateLegacyJWT` undefined) stores ALL tokens as `opaque` (auth.ts:94-112) and
does NO sub-pin on updateAuth — the single-user pin is the connection's fixed
`userID` (auth.ts:79 rejects a token without one; a refresh keeps the existing
userID). Both opaque tests currently MASK this: `..._skips_retransform` passes only
because the connection is closed (authChanges stays 0), and `..._change_retransforms`
was kept in its no-userID form (unpinned) so it isn't tripped. Fix = route
updateAuth pin enforcement through the CCM's `update_auth`/`resolve_auth` and drop
the router's `decode_jwt_claims` sub-pin; then make BOTH opaque tests non-vacuous
(userID present, assert retransform-vs-skip WITHOUT a close). Then push-relay
`PushRelayHeaders.auth` flip + delete the cell; then I-6 + ART.

### Stage 2 — migrate consumers to read the CCM at use time (REMAINING — ART-gated)
One consumer per commit, each reverting-proven. Consume sites (router.rs):
- authData (`client_auth`): 2585, 3429 → decode `ccm.auth.raw()` at use time.
- Bearer/`has-auth` (`client_raw_auth`): 1887, 1923, 2462, 2872 → `ccm.auth.raw()`.
- query ctx (`client_query_ctx`): 1995, 2469, 2618, 3449 → `ccm.query_context`.
- push relay (`PushRelayHeaders.auth`): needs the `Arc<Mutex<CCM>>`+selector
  plumbed into the message handler (currently holds the auth cell).
- revalidation/pin: read `user`/`auth` from the CCM.

**BEHAVIORAL RISK discovered — why Stage 2 MUST be ART-gated before shipping:**
an *anonymous-opaque* connection (an opaque token but NO userID) is admitted by
the live rust path (`client_raw_auth` holds the token), but `resolve_auth` rejects
it (token + no userID → Unauthorized, auth.ts:79-85) so the CCM seeds `auth:None`.
Flipping the Bearer/authData reads to the CCM would therefore change such a
connection's forwarded token from the opaque token to `None`. Per TS this is the
*more* faithful behavior (TS `resolveAuth` would reject it too), but it is a
LIVE behavior change that must be validated by the ART re-gate (mutation/auth
gates + G-ttl) before it ships. This is exactly why the user sequenced "ART run"
AFTER the I-8 migration. Until Stage 1.1's shadow is confirmed byte-equal to the
maps across a full ART run, do NOT flip the live reads — the maps stay
authoritative (a failed best-effort dual-write must never reach a client).

### Stage 2 — migrate consumers to read the CCM at use time
One consumer per commit, each reverting-proven:
1. push relay → read `must_get_connection_context(selector).auth` (delete the
   `PushRelayHeaders.auth` shared cell; the CCM is now the single owner).
2. custom-query fetch → same for `queryContext` (url + customHeaders).
3. mutagen CRUD path → already reads `must_get_connection_context`; it now gets
   real auth (closes the latent I-8 bug even if mutagen is enabled).
4. revalidation / pin → read `user`/`auth` from the CCM.

### Stage 3 — delete the shims
Remove `client_raw_auth`, `client_auth`, `client_query_ctx`, and the
`PushRelayHeaders.auth` cell once no consumer reads them. `PushRelayHeaders`
keeps only the connect-time fields (cookie/origin/headers/user) + `push_override`.

### Stage 4 — re-gate
Full ART (G8 diff-oracle, G-ttl once #154 lands, mutation/auth/reconnect gates)
+ the L3/L4 guards. Only then is I-8 closed.

## Risks + guards
- **Auth pinning** (`pickToken`/`resolveAuth`) must stay 1:1 — the ported CCM
  already has `resolve_auth`/`pick_token`; use them, don't reimplement.
- **updateAuth re-transform** must still fire (`auth_changes` metric) — pin with
  the existing opaque-token tests.
- Each stage: `cp file /tmp/bak`, revert, confirm the new assertion FAILS, restore.

## Estimate
~2–3 focused days + one ART cycle. NOT a single-session change; attempting it
half-way is worse than the current benign-latent state (mutagen is off in prod,
so I-8 is dead today — INVENTIONS.md I-8 forbids shipping a new placeholder-CCM
consumer until this lands).
