# 85 — Open Design Decisions

Two decisions we paused on. These gate phase B and D of the rollout.

---

## D1: Void-flush semantics on ownership signal

### The TS behavior

`packages/zero-cache/src/services/view-syncer/cvr-store.ts:388-405`:

```ts
if (owner !== this.#taskID) {
  if ((grantedAt ?? 0) > lastConnectTime) {
    throw new OwnershipError(owner, grantedAt, lastConnectTime);
  } else {
    // Fire-and-forget an ownership change to signal the current owner.
    void this.#db`
      UPDATE cvr.instances 
      SET owner=${this.#taskID}, grantedAt=${lastConnectTime}
      WHERE clientGroupID=${this.#id} AND (
        grantedAt IS NULL OR grantedAt <= to_timestamp(${lastConnectTime / 1000})
      )`
      .execute()
      .catch(this.#failService);
  }
}
```

The UPDATE is fire-and-forget (`void`). Rationale: if it fails, the next `load()` retry will see the stale owner and re-signal; no correctness is lost because the load loop is idempotent and retrying already.

### The options for Rust

**Option A — Preserve the void-write.** The Rust port spawns the UPDATE as an unmonitored task and proceeds. Errors are caught by a global handler that calls `#failService`.

- Pros: byte-for-byte parity with TS behavior. No new failure modes.
- Cons: genuinely silent failures. If the OWNER-update has a persistent reason to fail (Postgres connection loss), the load-retry loop never exits cleanly. Matches TS, so this is actually correct but ugly.

**Option B — Synchronous + fallible.** The Rust port awaits the UPDATE. Failure surfaces as `Err(OwnershipError)` propagated back through `load()` to the ViewSyncer. The retry loop continues.

- Pros: explicit error propagation, clearer in logs.
- Cons: changes the failure timing. A failed vote can block load-retry for longer than TS does, increasing chance the CVR is considered stale.

### Decision criteria

The TS behavior was chosen deliberately because:

1. **Anti-flapping.** A transient DB issue in the middle of an ownership handoff shouldn't kill the ViewSyncer that the CVR is being handed TO.
2. **Cleanup is someone else's job.** The `cvr-purger` (TS) reaps stale CVRs on intervals; a CVR whose owner-update failed is the same as if the owner had crashed. Eventually cleaned up.

The Rust port will observe the same Postgres error rate. There's no evidence that the void-write ever caused a real-world bug. The pathological case is Postgres hard-down — but then EVERYTHING is failing, including the CVR load itself.

### Recommendation

**Option A: Preserve void-write semantics.** Then revisit once we have sandbox data. If loads become noisier, switch to option B. The change is a 5-line diff in the Rust once written.

### Implementation

```rust
// cvr-store.rs
if owner != self.task_id {
    if granted_at.unwrap_or(0) > last_connect_time {
        return Err(OwnershipError { owner, granted_at, last_connect_time });
    } else {
        // Fire-and-forget. TS comment quoted verbatim.
        let task_id = self.task_id.clone();
        let last_connect_time_s = last_connect_time / 1000;
        let pool = self.pool.clone();
        let cvr_id = self.cvr_id.clone();
        let schema = self.schema.clone();
        let fail_service = self.fail_service.clone();
        tokio::spawn(async move {
            let r = sqlx::query(&format!(r#"UPDATE "{}".instances SET owner=$1, granted_at=$2 WHERE clientGroupID=$3 AND (grantedAt IS NULL OR grantedAt <= to_timestamp($4))"#, schema))
                .bind(&task_id)
                .bind(last_connect_time_s)
                .bind(&cvr_id)
                .execute(&pool)
                .await;
            if let Err(e) = r {
                fail_service(e);
            }
        });
    }
}
```

---

## D2: Poke backpressure exactness

### The TS behavior

`client-handler.ts` doesn't have a server-side "backpressure" channel beyond the per-poke `#pokeTail` promise. **The only backpressure signal is the WebSocket itself** — if the client doesn't ACK frames, the WebSocket send queue grows, and the next `#push` (the WS send) will block on TCP-level flow control once the socket buffer is full. At that point the `#pokeTail` chain naturally stops because the current poke's body-flushes stall.

### The Rust options

**Option A — Identical.** Poke chain per client, WS frame sends block on the underlying socket buffer.

- Pros: parity. No new failure modes.
- Cons: no explicit cancellation point in the middle of a poke body flush.

**Option B — Explicit per-frame ACK.** Each `pokePart` frame requires an explicit client ACK before the next is pushed.

- Pros: precise backpressure, allows the server to pause mid-poke.
- Cons: requires client-side protocol changes; not supported by `zero-client`.

**Option C — Bounded on-disk queue.** Poke parts go to a per-connection bounded queue. If the queue is full, the poke stalls (like the WS buffer) but with an explicit fail-fast limit instead of an unbounded socket buffer.

- Pros: predictable memory footprint.
- Cons: introduces a new class of full-rank failures that TS doesn't have — without client-side support, a stalled queue wedges the CG.

### Decision criteria

Existing stock TS behaves like Option A. Client-side handles backpressure implicitly via WS frame queue + flow-control ACKs (WebSocket protocol itself). The CVR port should match this unless we have measured overflow events.

### Recommendation

**Option A: Identical.** No protocol changes; poke chain + WS-buffer blocking is the backpressure mechanism. Document explicitly so future maintenance doesn't conflate this with actual client-side flow control.

### Implementation note

The Rust `WebSocketSink::push` should be `async` and capable of yielding when the socket's write-buffer is congested — a `tokio-tungstenite` "send with backpressure" style. This is what gives us the equivalent of TS's queue-stall, with no extra protocol.

---

## Both decisions have **test coverage requirements** before phase B lands:

- `test-ownership-void-write-surfaces-on-fail` — assert that `fail_service` is called when the UPDATE fails, but `load()` still retries.
- `test-poke-chain-stall-when-socket-busy` — assert that a slow consumer stalls the chain without deadlock.
