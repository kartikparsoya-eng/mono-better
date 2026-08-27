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
  `i8_stage1_ccm_tracks_connection_and_auth_via_dual_write`. Learned: CCM
  `resolve_auth` requires a userID (TS parity) — anonymous/opaque paths need a
  userID or the CCM rejects (see also the anonymous-opaque question in Stage 1.1).
- **1.1 — TODO:** seed connect-time auth into `register_connection` (Stage 1.0
  registers with `auth: None`); wire `push_config` + `validate_legacy_jwt` +
  `now` into `ConnectionContextManager::new`; dual-write `init_connection`
  (userQueryURL/userPushURL) in `handle_desired_queries`; dual-write
  `close_connection` on teardown. Open question to resolve 1:1: does TS
  `resolveAuth` require a userID for OPAQUE tokens? Rust router allows anonymous
  opaque (unpinned group) but the ported CCM requires a userID — reconcile
  against auth.ts before Stage 2.

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
