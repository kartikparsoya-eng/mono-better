# L4 — State-Ownership & Freshness Sweep (rust-syncer)

Goal: every rust struct field that stores connection/auth/config state must
match TS's read pattern. If TS reads through a manager at USE time, rust must
read a shared cell at use time; a constructor-time snapshot is legal ONLY if TS
also snapshots (cite the TS line).

Method: enumerate the fields the message-handler / push-relay / CG-state read
per message, and cross-check `connection-context-manager.ts` for what
`updateAuth` (line 344) and `handleInitConnection` (line 290) mutate post-connect.

## TS post-connect-mutable connection state (the only things that can go stale)
From connection-context-manager.ts:
- `auth` — mutated by `updateAuth`.
- `queryContext.url` (`userQueryURL`), `queryContext.customHeaders`
  (`userQueryHeaders`) — mutated by `handleInitConnection`.
- `mutateContext.url` (`userPushURL`), `mutateContext.customHeaders`
  (`userPushHeaders`) — mutated by `handleInitConnection`.
- `revision` — bumped on any of the above.

Everything else (`requestHeaders`, `cookie`, `origin`, `user`) is set once at
connect (lines 242-260) and never mutated → snapshotting is faithful.

## Field-by-field verdict

| Rust field | Holds | TS reads | Verdict |
|---|---|---|---|
| `PushRelayHeaders.auth` | Bearer token | fresh per push (`mustGetConnectionContext`) | **WAS SNAPSHOT → BUG-2. FIXED** `97440d021` (now `Arc<Mutex>`, refreshed in `handle_update_auth`) |
| `PushRelayHeaders.push_override` (userPushURL/Headers) | push overrides | re-set on `handleInitConnection` | OK — already `Arc<Mutex>`, refreshed in `handle_desired_queries` (router.rs:2371) |
| `PushRelayHeaders.cookie/origin/request_headers/user_id` | connect headers | connect-time (never mutated in TS) | OK — faithful snapshot |
| `CustomQueryContext.auth` | Bearer for named-query fetch | fresh (re-transform on updateAuth) | OK — refreshed in `handle_update_auth` (router.rs:2749) |
| `CustomQueryContext.url/client_headers/allowed_urls` | userQueryURL/Headers | re-set on `handleInitConnection` | OK — REPLACED on each initConnection (router.rs:2440-2445) |
| `client_raw_auth` / `client_auth` (CgState) | raw token / claims | fresh | OK — refreshed in `handle_update_auth` |
| `client_base_versions` (CgState) | client base cookie | connect param, connect-time in TS | OK — faithful snapshot |
| `SyncerWsMessageHandler.connection_selector` | client/ws ids | immutable | OK |
| `ConnContextInfo.auth` via `PlaceholderConnContextManager` | — | fresh in TS | **LATENT (I-8):** placeholder returns `None`; only reader is the mutagen CRUD path, which is disabled in prod (`create_mutagen`→None). If enabled → bug-2-class. |

## Findings
1. **BUG-2 (auth) — the only ACTIVE stale-snapshot divergence — fixed.**
2. **I-8 latent:** connection/auth state is split — TS has one owner
   (`ConnectionContextManager`); rust has a placeholder CCM (returns `None`) +
   four `CgState` maps. Not a live bug (only the disabled CRUD path reads the
   placeholder), but it is exactly the soil bug-2 grew in. **Structural fix =
   plan item 7** (promote the ported CCM to single live owner). Until then,
   INVENTIONS.md I-8 forbids shipping a new consumer of the placeholder CCM.
3. No other active snapshot divergence found: every post-connect-mutable TS
   field maps to a rust cell that is refreshed on the same trigger.

## Standing rule (AGENTS.md amendment)
Storing a clone of any connection/auth/config value in a struct requires a
doc-comment citing the TS line proving TS ALSO captures-at-construction. Default
is read-through-shared-state at use time.
