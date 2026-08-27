# Zero-Divergence Plan (post-incident 2026-08-27)

Two prod bugs (connect-ack serialized behind hydrate; push-relay relaying a
stale auth token) escaped ALL five existing parity layers. This document is the
post-mortem of *why the tools missed them* and the plan that closes each hole.

Goal restated (the standing rule): **the only thing Rust is allowed to invent is
the thread/parallelism implementation — and even those inventions must be
observationally equivalent to TS.** Everything else is a 1:1 port.

---

## Part 1 — Why every existing layer missed both bugs

| Layer | What it audits | Why it missed bug 1 (connect-ack) | Why it missed bug 2 (stale push auth) |
|---|---|---|---|
| **L1 symbol/file ledger** (`parity_ledger.py`) | A ported symbol *exists* in the mirrored file | `Connection::init()` existed, 1:1 named, right file. The bug was *where it was called from* (serial CG thread vs concurrent accept path) — L1 does not model call sites | `PushRelayHeaders` is part of the push-relay **invention** (Option-A), exempted from the ledger as "no TS twin" |
| **L2 body-differential** (`layer2_coverage.py`) | Function *bodies* branch-match TS | `init()`'s body was a perfect port. The divergence was in the **caller topology**, which L2 never looks at | TS `pusher.ts enqueuePush` body says `mustGetConnectionContext(selector)` — a *use-time read*. Rust's relay used a stored field. L2 never diffed this body because the pusher is an invention, not a port |
| **TS-golden fixtures** | Byte-equal outputs for chosen inputs | The `connected` frame was byte-identical on both sides. Fixtures compare **values**, never **when/on which thread** a value is produced | A fixture would need a *token refresh followed by a push* to expose it; nobody fixtures state-freshness over time |
| **diff-oracle / ART** | End-to-end result sets TS-vs-rust | Results were identical. The oracle is **time-blind**: it can't see "ack arrived 254s late". And the workload had no multi-second hydrates and no mid-hydrate reconnects, so the pathology never fired | Sessions were shorter than any token TTL and never refreshed auth mid-session. Value-space testing, time-space bug |
| **Ported-test audit** (view-syncer.pg.test.ts etc.) | Rust reproduces TS's own test outcomes | TS has no test asserting "connected is independent of hydrate latency" — it's guaranteed *structurally* (per-connection concurrency), stated only in a prose comment ("This is early in the connection lifecycle") | TS's freshness is also structural (`mustGetConnectionContext` per push); no TS test pins it, so the audit had nothing to port |

**The single root cause:** both bugs live at the **seam between ported code and
the invented concurrency architecture**. Every tool audits the *interior* of
ported functions; nothing audits the seam:

1. **Execution-context placement** — which thread/task a ported call runs on,
   and what ordering that placement implies (bug 1).
2. **State ownership & freshness** — TS keeps one copy of connection state in
   `ConnectionContextManager` and *reads it at use time*; rust smeared it into
   four parallel copies (`client_raw_auth`, `client_auth`,
   `client_query_ctx.auth`, `PushRelayHeaders.auth`) and one went stale (bug 2).
3. **Inventions were exempt** — AGENTS.md rule 5 says "justified + labeled" but
   requires no *contract*: no statement of the TS-observable behavior the
   invention must preserve, and no test pinning it.
4. **Time-space is untested** — all differential testing compares values;
   nothing compares ordering/latency-independence under adversarial timing
   (slow hydrates, token expiry, mid-hydrate reconnects).

---

## Part 2 — The plan: five new layers + two process rules

### L3 — Call-topology & execution-context ledger  *(catches bug-1 class)*

Extend `parity_ledger.py` from "symbol exists" to "symbol is **called from the
mapped context**":

- Extract call edges (caller → callee) for every ported symbol on both sides
  (regex/tree-sitter on TS, `syn`/regex on Rust — same content-derived approach
  as L1).
- Maintain a checked-in **execution-context map**: every TS context → rust
  context, e.g.
  - TS per-connection accept handler (`syncer.ts#handleConnection`) → rust
    accept task (`router::handle_connection`)
  - TS view-syncer `#lock` tasks → rust serial CG thread (`dispatch_cg_message`)
  - TS setTimeout/interval callbacks → rust CG-loop deadline arms
- **Rule:** a ported call edge whose rust context is *more serialized* than its
  TS context is a divergence unless a contract entry (L6) proves
  order-equivalence. Bug 1 is exactly this: `Connection.init()` moved from the
  concurrent context into the serial one.
- One-time full-edge audit of rust-syncer (the seam crate), then the ledger
  enforces on change like L1 does.

### L4 — State-ownership & freshness audit  *(catches bug-2 class)*

Mirror TS's **state topology**, not just its functions:

- **One-time sweep (highest yield, do first):** enumerate every rust struct
  field that stores a *constructor-time snapshot* of connection/auth/config
  state. For each, find the TS read pattern. If TS reads through a
  manager/getter at use time, rust must read a shared cell at use time. A
  snapshot is only legal if TS also snapshots — cite the TS line.
  Known candidates to check now: `PushRelayHeaders.{cookie, origin,
  request_headers, user_id}`, `CustomQueryContext` fields, `ConnContextInfo`
  consumers, `client_base_versions`, anything cloned into
  `SyncerWsMessageHandler`/`push_relay`.
- **Eliminate duplicated state:** auth existed in four places; TS has one
  (`ConnectionContextManager`). The ported `connection_context_manager.rs`
  exists but is a "tested reference" while the live path uses simplified
  `CgState` maps — that split is itself a divergence and is what made bug 2
  possible. Plan item: **promote the ported CCM to be the single live owner**
  of connection context; everything (pushes, queries, revalidation) reads
  through it, exactly like TS.
- Extend rule 3 in AGENTS.md: 1:1 files and 1:1 **state ownership** — a TS
  class's fields live in exactly one rust struct; no parallel copies.

### L5 — Temporal differential oracle  *(catches both classes end-to-end)*

Today's oracle compares result sets. Add **time-space** comparison:

- **Injected-delay harness:** the two new regression tests (block the CG thread,
  assert `connected` still flows; refresh auth, assert the forwarded token
  flips) are instances of a general pattern. Build it out: a `BlockingCcm`-style
  hook at each seam (hydrate, advance, flush, relay) + assertions for every
  client-observable liveness/ordering invariant.
- **ART adversarial-timing gates (new):**
  - *G-slow*: seed one deliberately expensive query (or a sleep-injecting cost
    hook), then run a reconnect storm mid-hydrate. Assert: connect-ack p99 is
    independent of hydrate time, on BOTH TS and rust, and equal frame sequences.
  - *G-ttl*: mint JWTs with TTL shorter than the session; client refreshes via
    `updateAuth`; mutations continue. Assert: zero 401s, mutation results equal
    TS.
  - *Frame-sequence oracle*: per client, record the ordered downstream frame
    *types* (connected, pokeStart/parts/End, error) with coarse timing classes,
    diff TS vs rust. Values were always compared; now the *order and latency
    envelope* is too.

### L6 — Invention contract registry  *(closes the exemption hole)*

Upgrade AGENTS.md rule 5 from "justified + labeled" to **"justified + labeled +
contracted + tested"**:

- `parity/INVENTIONS.md`: enumerate every Rust-only invention — CG
  thread/executor model, ws_sink writer/reader tasks, push relay (Option-A),
  CVR write-behind, Drop-based teardown, offload runtime, shed policy…
- Each entry states its **TS-observable contract**, e.g.:
  - *CG serial thread*: "clients must observe the same frame ordering AND the
    same independence guarantees as TS's per-connection concurrency — in
    particular, connect-ack, pong, and error frames must never be delayed by
    another message's processing."
  - *Push relay*: "the relayed request must be byte-equivalent to what TS's
    in-process `fetchFromAPIServer('push', ctx)` would send **at push time** —
    including the current (not connect-time) auth."
- Each contract maps to at least one test (the L5 harness). The ledger fails if
  an invention exists in code without a registry entry.

### L7 — TS prose-invariant mining  *(the cheap one nobody does)*

TS comments are spec text under rule 1. Both bugs were *written down in TS*:
`connection.ts:135` "This is early in the connection lifecycle";
`pusher.ts` reads context per-push by construction. One-time sweep of
zero-cache/zql for ordering/timing/liveness prose ("immediately", "before",
"must not block", "per push/connection", "paced", "early") → each becomes a
checklist row: rust test reference, or an explicit N/A with citation. New ports
must add rows for any such comments in the ported file.

### Process rules (AGENTS.md amendments)

1. **Rule 6 extension:** when porting or moving a call site, re-read the TS
   *caller and its execution context*, not just the function body. Porting a
   function without porting its placement is a divergence.
2. **Snapshot rule:** storing a clone of any connection/auth/config value in a
   struct requires a doc-comment citing the TS line proving TS also
   captures-at-construction. Default is read-through-shared-state at use time.

---

## Part 3 — Execution order (inline, no agent fan-out)

| # | Item | Effort | Would have caught |
|---|---|---|---|
| 1 | **L4 snapshot sweep** of rust-syncer (all constructor-captured state vs TS read patterns) | ~½ day | bug 2 + any siblings lurking now |
| 2 | **L6 registry** `parity/INVENTIONS.md` with contracts for the ~8 existing inventions | ~½ day | both (as review checklist) |
| 3 | **L5 injected-delay unit harness**: generalize BlockingCcm; one liveness test per seam (ack, pong, error, poke during blocked hydrate/flush/relay) | ~1 day | bug 1 + siblings |
| 4 | **L7 prose-invariant sweep** of zero-cache (view-syncer, workers, mutagen, custom) | ~½ day | both, cheaply |
| 5 | **L3 call-edge ledger** extension + one-time full-edge audit of rust-syncer | ~1–2 days | bug 1 class, permanently |
| 6 | **L5 ART gates** G-slow + G-ttl + frame-sequence oracle (xyne-art) | ~1–2 days | both, end-to-end, forever |
| 7 | **L4 CCM promotion**: single state owner for connection context (the ported CCM), delete the parallel CgState auth maps | ~2–3 days, re-gate | bug 2 class, structurally |

Items 1–4 are fast and close the immediate holes; 5–7 make it structural.

## Status (executed 2026-08-27)

**Fixes shipped (branch rust-cvr-v1.0.0, unpushed):**
- Bug 1: `5e71e24f4` (connected on accept task) — non-vacuous test proven.
- Bug 2: `97440d021` (push auth Arc refreshed on updateAuth) — non-vacuous test.

**Plan items — done:**
- **L4 snapshot sweep** → `L4-SNAPSHOT-SWEEP.md`. Verdict: auth was the ONLY
  active stale-snapshot divergence (fixed x2). All other post-connect-mutable TS
  fields (query/push URL + customHeaders) map to rust cells refreshed on the same
  trigger. One LATENT finding: I-8 placeholder CCM (dead in prod, mutagen off).
- **L6 invention registry** → `INVENTIONS.md` (I-1..I-8 with contracts + tests).
- **L7 prose sweep** → `L7-PROSE-INVARIANTS.md`. Confirmed view-syncer.ts:896/916
  is bug-1's spec; confirmed the pong keepalive is ALREADY decoupled (writer task,
  ws_server.rs:464 — mirrors TS `#maybeSendPong`), so no pong-behind-hydrate bug.
- **L3 call-topology guard** → `call_topology.py`, wired into `local-rust-ci.sh`.
  Passes clean; proven to catch a re-introduced bug-1 (connected in on_new_connection).
- **AGENTS.md** amended with rules 8 (call-site/context), 9 (state ownership +
  freshness), 10 (invention contract), + the divergence-layer index.

**Plan items — remaining (tracked as tasks):**
- **L5 ART temporal gates** (xyne-art): G-slow (injected slow query + reconnect
  storm → ack latency hydrate-independent), G-ttl (JWT TTL < session → zero 401s),
  frame-sequence oracle. — the end-to-end backstop.
- **L7/I-8 CCM promotion**: make the ported `connection_context_manager.rs` the
  single live owner of connection/auth state; delete the parallel `CgState` auth
  maps + placeholder CCM. Structural removal of the bug-2 soil. Multi-day + re-gate.
- Minor test GAPs noted in INVENTIONS.md: shed-error parity (I-4),
  durability-ordering oracle (I-6), cancel-during-hydrate (L7).
