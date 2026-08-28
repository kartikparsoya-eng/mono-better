# L3 — Execution-Context Map (TS → Rust)

The bug-1 class (connect-ack serialized behind hydrate) was a ported call moving
from a **concurrent** TS execution context into the **serial** rust CG thread.
L1/L2 audit function interiors; they cannot see which context a call runs in.
This map is the reference `parity/call_topology.py` enforces.

## TS context → Rust context

| TS context | What runs there | Rust context | Rust root fn(s) | Concurrency |
|---|---|---|---|---|
| per-connection accept handler (`workers/syncer.ts#handleConnection`) | `new Connection` → `connection.init()` (version gate + `connected`), connect-time error/close | **accept task** | `workers::syncer::Syncer::create_connection`, `ws_server::accept_connection`, `ws_server::send_error_and_close` | one per connection, NOT behind any CG lock |
| view-syncer `#lock` tasks (`view-syncer.ts`) | `config_and_hydrate`, advance, poke, message processing (`handleMessage`), close | **serial CG thread** | `router::dispatch_cg_message`, `router::run_cg_thread` (→ `on_new_connection` / `on_inbound` / `on_connection_closed`) | one OS thread per CG hash-shard, one message to completion |
| `setInterval(#maybeSendPong)` (`connection.ts:341`) + `ws.send` drain | keepalive `pong`, downstream frame writes, slow-client shed close | **writer task** | `ws_server::run_ws_writer` | independent tokio task per connection — decoupled from the CG thread |
| ws `'message'` reader | JSON parse + valita validate, size cap, forward inbound | **reader task** | `ws_server::run_ws_reader` → `router::forward_inbound` | independent tokio task; forwards to the CG channel, emits no frames |
| CVR I/O awaited inline (`cvr-store.ts`) | CVR flush / persistence | **offload runtime** (invention I-6) | `ViewSyncerService::offload` (services/view_syncer/view_syncer.rs), CVR flush actor | pool runtime; must preserve durability-before-poke ordering |

## Client-observable frame emissions — sanctioned sites

`call_topology.py` Tier-2 pins each frame primitive to a (file, enclosing-fn)
allowlist. A call site anywhere else fails CI.

| Primitive | Sanctioned site(s) | Context | Why exactly here |
|---|---|---|---|
| `connected_message` | `workers/syncer.rs::create_connection` | accept task | the live emission — decoupled from hydrate (bug-1 fix `5e71e24f4`) |
| `connected_message` | `connection.rs::init` | accept (by contract) | the 1:1 TS port of `connection.init()`; NOT called on the prod path (unit-tested only) — I-2 |
| `pong_message` | `ws_server.rs::run_ws_writer` | writer task | decoupled keepalive (mirrors TS `#maybeSendPong`) — keeps pong live under a blocked CG thread |
| `pong_message` | `connection.rs::handle_inbound` | serial CG thread | explicit `ping→pong` reply, faithful to TS `connection.ts:220` (answered on the per-connection handler) |

## The rule (why bug-1 is now un-reintroducible)

A ported call whose rust context is **more serialized** than its TS context is a
divergence unless an INVENTIONS.md contract proves order-equivalence. Concretely:
`connected_message` reachable from the serial CG thread = the bug-1 pathology.
Tier-1 guards the direct enclosing fn in its pinned file; Tier-2 extends the same rule
to **every file**, so the emission cannot be smuggled onto the CG thread (or the
CVR offload runtime) via a helper in another module. Proven non-vacuous: injecting
a `connected_message` call into the CG serving core (view_syncer.rs) fails Tier-2 (a re-coupling
Tier-1's router-only scan would miss).

## Extending

When a new client-observable frame primitive gains a "must not be serialized
behind another message" contract (INVENTIONS.md), add it to `EMISSIONS` in
`call_topology.py` with its sanctioned (file, fn) sites and a row here.
