# L7 — TS Prose-Invariant Checklist

TS comments are spec text (AGENTS.md rule 1). Ordering/timing/liveness prose in
zero-cache/zql is mined here; each row must map to a rust test reference or an
explicit N/A with citation. New ports add rows for such comments in the ported
file.

Mining query (re-run to refresh):
```
grep -rnE '//.*(immediately|before .*hydrat|must not block|per push|per connection|
  early in the|without waiting|ahead of|synchronously|as soon as|independent of|
  does not wait|returned immediately)' packages/zero-cache/src/{workers,services/view-syncer,services/mutagen,custom}
```

| TS site | Invariant | Rust honoring | Test |
|---|---|---|---|
| `connection.ts:135` | "`connected` ... early in the connection lifecycle" (before initConnection handling) | `handle_connection` emits `connected` on accept task before CG dispatch | `connected_ack_is_decoupled_from_a_blocked_cg_hydrate` |
| `view-syncer.ts:896` | "initConnection must be synchronous so that the downstream subscription is returned immediately" | rust registers the client + sink synchronously in `on_new_connection` before hydrate; ack is off-thread | I-1/I-2 tests |
| `view-syncer.ts:916` | "initConnection() must be synchronous so that `downstream` is immediately returned to the caller (connection.ts) ... so it can be canceled even if #runInLockForClient has not run" | hydration is the awaited body but the SINK/registration exist first; cancel path = `on_connection_closed` removes the client independent of hydrate | I-1 test; **GAP:** cancel-during-hydrate test |
| `pusher.ts:107` | `enqueuePush` reads `mustGetConnectionContext(selector)` fresh (implicit per-push freshness) | push auth is a shared cell refreshed on updateAuth | `update_auth_refreshes_the_forwarded_push_relay_token` |
| `view-syncer.ts:717` | "All lock tasks check for shutdown so that queued work is immediately [drained]" | CG loop checks `terminal`/shutdown between messages | drain/shutdown tests |
| `row-record-cache.ts:171` | "Set this.#cache immediately (before await) so that only one db [load runs]" | rust row-record-cache single-flight | row-record-cache tests |

## Open gaps (become L5 tests)
- **cancel-during-hydrate:** a connection closed while its hydrate is in flight
  must cancel cleanly (no poke to a dead sink, no leak). TS guarantees this via
  the sync `downstream` return (view-syncer.ts:916).
- **pong/error independence:** a `ping` or a protocol error on connection B must
  be answered even while the CG thread is blocked on A's hydrate (I-1 contract).
