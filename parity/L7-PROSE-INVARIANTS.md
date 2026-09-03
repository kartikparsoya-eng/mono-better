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
| `view-syncer.ts:916` | "initConnection() must be synchronous so that `downstream` is immediately returned to the caller (connection.ts) ... so it can be canceled even if #runInLockForClient has not run" | sink + registration exist synchronously; a mid-hydrate close reaches the serial CG thread as `ConnectionClosed` enqueued AFTER `NewConnection` (FIFO), processed once the hydrate releases → `on_connection_closed` fully tears down; dropped sink ⇒ no post-close frames | `a_close_fully_tears_down_all_per_client_state` (teardown completeness) + `connected_ack_is_decoupled_from_a_blocked_cg_hydrate` (serial channel drains queued msg after a block) |
| `pusher.ts:107` | `enqueuePush` reads `mustGetConnectionContext(selector)` fresh (implicit per-push freshness) | push auth is a shared cell refreshed on updateAuth | `update_auth_refreshes_the_forwarded_push_relay_token` |
| `view-syncer.ts:717` | "All lock tasks check for shutdown so that queued work is immediately [drained]" | CG loop checks `terminal`/shutdown between messages | drain/shutdown tests |
| `row-record-cache.ts:171` | "Set this.#cache immediately (before await) so that only one db [load runs]" | rust row-record-cache single-flight | row-record-cache tests |

## Closed gaps (now pinned)
- **cancel-during-hydrate** (view-syncer.ts:916): CLOSED — the FIFO serial
  channel delivers the queued close after the block
  (`connected_ack_is_decoupled_from_a_blocked_cg_hydrate` proves the channel
  drains a message queued behind a blocked hydrate), and
  `a_close_fully_tears_down_all_per_client_state` pins that `on_connection_closed`
  then clears EVERY per-client map (auth/raw-auth/query-ctx/push-headers/
  profile/base-version/sink) — no leaked subscription or stale auth. Dropped sink
  ⇒ no poke to a dead client.
- **pong/error independence** (I-1): RESOLVED — the precise invariant is that
  pong LIVENESS comes from the decoupled writer-task keepalive (ws_server.rs:474,
  mirrors TS `#maybeSendPong`), NOT the CG-thread ping reply; connect-time errors
  are on the accept path; the shed error is on the writer task. See INVENTIONS.md
  I-1's "three decoupled emissions". Tests: `on_inbound_ping_answers_pong`,
  `send_error_and_close_sends_error_frame_then_close_3000`,
  `slow_client_shed_closes_with_rehome_error_then_close_1011`. A protocol error
  generated while PROCESSING client A's message is serialized on the CG thread —
  faithful (TS also runs `handleMessage` before it can throw, and the throw only
  closes A's own connection, not B's).
