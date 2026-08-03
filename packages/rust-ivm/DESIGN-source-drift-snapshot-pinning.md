# Source-drift root cause + fix: TableSource snapshot-pinning during advance

Owner: Kartik. Written 2026-07-24. Status: FIXED in rust-ivm master.

## Symptom
Under the ART mutation-matrix (writes), the rust pod logs ~8
`engine panic: source drift: Add duplicate row in <table>`
(mutations, clients, bookmarks, channel_sections, canvas_participants).
The **TS reference pod hits ZERO** source-drift under the identical load → this
was a real port bug, not expected behaviour.

## Root cause (historical)
`TableSource.validate_change` asserts, for a SET that the diff classified as an
ADD (`prev_values` empty), that the row does NOT already exist — via
`check_exists(self.db, row)`.

In TS, during advance the TableSource reads the **prev** snapshot; it is switched
to **curr** only AFTER all changes are processed
(`pipeline-driver.ts:790` `table.setDB(curr.db.db)`). So for a genuinely-new row
the prev-read returns "absent" → the ADD assert passes.

Before the fix, the Rust port did not wire `set_db` to the snapshotter: the
TableSource held its own read-only `Rc<RefCell<Connection>>` opened at napi init,
separate from the snapshotter's pinned prev/curr connections. That connection
floated at the latest committed head, so under write bursts `check_exists` saw
the just-inserted row → "Add duplicate" panic.

This connection was ALSO wrong for advance-time fetches (Exists re-fetch, Take):
they read head instead of prev.

## Implementation today
- `snapshotter.rs` exposes `prev_conn()` and `current_conn()` as `SharedConn`
  (`Rc<RefCell<rusqlite::Connection>>`).
- `TableSource` implements `set_snapshot_db` (Source trait) by calling
  `set_db(db)`, which swaps its `Rc<RefCell<Connection>>`.
- `Engine::advance_to_head_streaming` (engine/mod.rs) calls
  `set_snapshot_db(prev_conn.clone())` on every source before iterating the
  diff, and `set_snapshot_db(curr_conn.clone())` afterwards — exactly mirroring
  TS `pipeline-driver.ts`.
- napi init wires the TableSources to the snapshotter's current connection.

## Fix applied
1. ✅ `Snapshot.conn` is `Rc<RefCell<rusqlite::Connection>>` (`SharedConn`);
   `prev_conn()`/`current_conn()` return clones of it.
2. ✅ napi init creates the `Snapshotter` first, then builds each `TableSource`
   with the snapshotter's current connection.
3. ✅ `Engine::advance_to_head_streaming` switches sources to `prev` before the
   diff loop and back to `curr` after. The `Source` trait exposes
   `set_snapshot_db` with a default no-op so `MemorySource` ignores it.
4. ✅ `TableSource.validate_change` is left unchanged — it now reads the prev
   snapshot during advance, matching TS's invariant check.

## Validation
- Differential fuzzers (serial and advance) and the 1832-fixture
  replay suite stay green.
- ART `--oracle --mutation-matrix` is the final gate: expect **0 source-drift**
  resets (parity with TS reference), G8 still ≤1 benign.
- Hydration reads the curr snapshot consistently.

## Risk
The fix is in production. The main residual risk is ART-scale load exposing
remaining race windows; monitoring source-drift reset metrics validates it.
