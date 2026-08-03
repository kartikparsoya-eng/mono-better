# DESIGN: wal2-aware snapshot isolation (match TS `BEGIN CONCURRENT`)

**Status:** implemented.
**Owner:** (implementing agent)
**Severity:** release blocker. Independent of the G8 `uniqueKeys` fix.

---

## 1. Problem

Under heavy write + checkpoint churn (ART `--mutation-matrix --negative`), the Rust
engine emits bursts of:

```
advance failed: get_rows next: database disk image is malformed
```

- `PRAGMA integrity_check` on the replica returns **`ok`** → the file is NOT corrupt.
  This is a **transient torn read**: a snapshot connection read a wal2 frame/page
  that the checkpointer recycled out from under it.
- It occurs in `snapshotter/diff.rs::get_rows` / `get_row`, reading the **`prev_conn`
  snapshot connection** during the advance diff, not the scalar-uniqueKeys path.
- Effect today: `iterate_diff` returns `DiffError::Other` → the engine surfaces
  `advance failed` → the view-syncer **closes the connection** (`Internal` error to
  the client) → the client reconnects and rehydrates (this is most of the observed
  `ClientNotFound` churn). It self-heals, but ungracefully.
- Observed as a single ~700ms burst at load ramp (6 CGs), amplified by a
  `docker restart` catch-up; zero recurrence afterward. No panics.

## 2. Root cause — divergence from the TS reference

The reference is TS `packages/zero-cache/src/services/view-syncer/snapshotter.ts`.

**TS (correct):** each `Snapshot` opens a **read-write, wal2-aware** connection
(`new Database(...)` → better-sqlite3 linked against the wal2 fork) and pins with
**`BEGIN CONCURRENT`** + the mandatory `_zero.replicationState` read
(`snapshotter.ts:307` `db.beginConcurrent()`; `db/statements.ts:55`
`beginConcurrent() → run('BEGIN CONCURRENT')`). A read-write connection registers a
proper wal2 **read-mark** in the `-shm`, which the checkpointer respects (it will
not recycle frames ≤ the minimum reader read-mark). Therefore TS **never sees a
torn read**, and its diff `catch (e)` (`snapshotter.ts:542`) is just
`cleanup(); throw e;` — **no corrupt/malformed handling exists because none is
needed.**

**Rust (diverged):** `snapshotter.rs::Snapshot::create` opens the connection
**`SQLITE_OPEN_READ_ONLY`** and pins with plain **`BEGIN`**
(`begin_and_pin`, `reset_to_head`). Rationale in the code comment: "Standard SQLite
(rusqlite) doesn't support WAL2 … read-write connections hold incompatible locks
that block the write-worker's COMMITs" + BEGIN CONCURRENT "requires write access".

That rationale is only true for **local tests** (system SQLite, plain WAL). In the
**deployed image** `rusqlite` is linked against the wal2 fork
(`build.rs`: "The Dockerfile installs WAL2 SQLite as system SQLite"), so read-write +
`BEGIN CONCURRENT` is available and is exactly the TS-correct pattern.

**Mechanism (confirm empirically during impl):** a **read-only** open under the wal2
fork does not register a checkpoint-blocking read-mark in the `-shm` (it can't write
the shared-memory index), so the aggressive checkpointer ignores it and recycles
frames the reader still needs → torn page. A **read-write** connection with
`BEGIN CONCURRENT` registers the read-mark → protected. This is why TS (rw) is safe
and Rust (ro) tears.

## 3. The fix

Make the Rust snapshot connections match TS: **open read-write and pin with
`BEGIN CONCURRENT` when the replica is in `wal2` mode**. For non-wal2 local test
replicas, use the same read-write connection with plain `BEGIN`.

Do **not** add a `SQLITE_CORRUPT`→reset catch — that would be a divergence from TS
and only masks the symptom.

### 3a. Connection open — read-write, wal2-gated

Snapshot connections are opened at these sites; all currently use
`SQLITE_OPEN_READ_ONLY`:

| Site | File:fn | Current |
|------|---------|---------|
| Snapshot open | `snapshotter.rs::Snapshot::create` (~297) | `open_with_flags(READ_ONLY …)` |
| Snapshot pin | `snapshotter.rs::begin_and_pin` (~335) | `execute_batch("BEGIN")` |
| Snapshot re-pin | `snapshotter.rs::reset_to_head` (~392) | `ROLLBACK` + `execute_batch("BEGIN")` |

Change to:

- **Open flags:** `SQLITE_OPEN_READ_WRITE | SQLITE_OPEN_NO_MUTEX | SQLITE_OPEN_URI`
  (drop `READ_ONLY`). Read-write open of a plain-WAL db is also fine, so this is
  safe for both prod and tests. Keep page-cache behavior unchanged and publish
  interrupt handles for the live connections after every snapshot swap.
- **Pin verb:** detect journal mode once after open (`PRAGMA journal_mode` is a
  read, returns `wal2` / `wal` / `memory` …). Pin with:
  - `BEGIN CONCURRENT` if `journal_mode == "wal2"`,
  - `BEGIN` otherwise (tests: plain WAL / memory).
  Centralize this in one helper, e.g. `begin_snapshot_tx(conn, is_wal2)` used by
  `begin_and_pin` and `reset_to_head`.
- **Never COMMIT.** The snapshotter only ever reads; every path already `ROLLBACK`s.
  `BEGIN CONCURRENT` that only reads + rollbacks acquires the read-mark and releases
  it on rollback — it never takes the wal2 write slot. (This is exactly how TS uses
  it: a read-only workload under a concurrent-write transaction primitive.)
- **Reuse exactly two connections.** The older snapshot is rolled back and
  re-pinned by `reset_to_head`, then `prev` and `curr` leapfrog. Allocating a new
  connection per advance diverges from TS and increases wal2 read-mark churn.

### 3b. journal-mode detection

Open is read-write; then `let mode: String = conn.query_row("PRAGMA journal_mode", …)`.
Store `is_wal2 = mode.eq_ignore_ascii_case("wal2")` on the `Snapshot` so re-pin
(`reset_to_head`) reuses it without re-querying.

### 3c. Pragmas now apply (bonus correctness)

`synchronous=OFF`, `case_sensitive_like=ON`, and `cache_size` are issued on the
read-write connection. Every PRAGMA error is propagated, matching TS. In
particular, **`case_sensitive_like=ON`** is required for query correctness.

## 4. Writer-contention safety

The original read-only choice feared blocking the JS write-worker's COMMITs. This is
disproved by TS, which runs the identical pattern (read-write wal2 conn +
`BEGIN CONCURRENT`) alongside the same replicator with no blocking — that is the
entire purpose of `BEGIN CONCURRENT` in wal2. Validate during impl that under load
the replicator's commit latency is unchanged (VictoriaMetrics `zero_sync_*` or the
ART G4/G5 gates). A read-write open failure is propagated exactly like TS; there
is no unsafe read-only fallback.

## 5. Local-test compatibility (must-keep)

Existing tests create replicas with `PRAGMA journal_mode=WAL` (plain WAL) on **system
SQLite**, where `BEGIN CONCURRENT` is unsupported. The wal2 gate in §3a keeps these
on `BEGIN`, so no test setup changes are required. Add one wal2-specific test only if
a wal2-capable SQLite is available in the test env (it is not for `cargo test` on
macOS — the wal2 build is Docker-only), otherwise cover wal2 behaviour in the ART
integration run (§7).

## 6. Change checklist

1. `snapshotter.rs`:
   - `Snapshot::create`: open `READ_WRITE`; read `journal_mode`; store `is_wal2`.
   - `begin_and_pin`: `BEGIN CONCURRENT` if `is_wal2` else `BEGIN`.
   - `reset_to_head`: `ROLLBACK` then `BEGIN CONCURRENT`/`BEGIN` per `is_wal2`.
   - Reuse `prev` via `reset_to_head` so only two connections leapfrog, matching TS.
2. Shared helper `begin_snapshot_tx(conn, is_wal2)` to avoid drift between pin
   sites.
3. Republish interrupt handles after every leapfrog so cancellation always targets
   the two live snapshot connections.

## 7. Validation

- **Rust unit/integration:** `cargo test` (plain-WAL test feature) stays green.
  Production NAPI disables that feature and requires wal2. Run `cargo fmt --check`
  and `cargo clippy --all-targets -D warnings`.
- **napi build** + **zero-cache `check-types`** (no TS change expected here).
- **ART, the real gate:** rebuild `zero-cache-rust-ivm`, run
  `./run-art-local.sh --oracle --mutations --mutation-matrix --negative`.
  Acceptance: **zero `database disk image is malformed` lines** in
  `docker logs xyne-sandbox-rust-test-zero-cache` across the whole run (grep it),
  G8 still `mismatches=0`, G4/G5 latency within baseline (no writer-contention
  regression), no new panics, G13 log-health not worse.
- Compare replicator commit latency before/after to confirm §4.

## 8. Rollout / risk

- **No behavior flag:** correctness-critical snapshot semantics must not silently
  fall back to an unsafe mode. Initialization fails if the read-write wal2 snapshot
  cannot be established, as it does in TS.
- **Risk:** a read-write open contending with the writer (mitigated by
  `BEGIN CONCURRENT` + read-only-in-practice). This is the TS production behavior.
- **Do NOT** pursue the `SQLITE_CORRUPT`→`ResetPipelinesSignal` catch — it diverges
  from TS and hides the real isolation gap.

## 9. Acceptance criteria

1. Deployed wal2 snapshot connections open read-write and pin with
   `BEGIN CONCURRENT`; tests (plain WAL) still pin with `BEGIN`; exactly two
   connections leapfrog via rollback and re-pin.
2. ART torture run (`--mutation-matrix --negative`) produces **0**
   `database disk image is malformed` / `advance failed: … malformed` log lines.
3. No regression: G8=0, G4/G5 within baseline, replicator commit latency unchanged,
   Rust suite + clippy + fmt + check-types green.
4. `case_sensitive_like=ON` is effective on snapshot reads (LIKE correctness).
