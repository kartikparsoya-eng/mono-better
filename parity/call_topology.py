#!/usr/bin/env python3
"""
L3 — Call-topology / execution-context guard.

Bug-1 class (connect-ack serialized behind hydrate) was a ported call moving
from a concurrent TS context into the serial rust CG thread. The body-level
ledgers (L1/L2) cannot see this — they audit function interiors, not which
context a call runs in.

Two tiers:

  TIER 1 (enclosing-fn, precise): for each ORDERING-SENSITIVE emission the
    enclosing rust function must be in the sanctioned set (and NOT in a
    forbidden serialized context). Catches a DIRECT re-coupling.

  TIER 2 (cross-file emitter-site allowlist, generalized): scan EVERY source
    file for call sites of the client-observable frame primitives
    (`connected_message`, `pong_message`) and assert each site's enclosing
    function is in that primitive's sanctioned (file, fn) allowlist. Tier 1 only
    guards router.rs; Tier 2 catches the same class in ANY file — e.g. a
    `connected_message` emitted from a new helper on the CVR offload runtime or
    the CG dispatch path. (An earlier draft tried whole-crate call-graph
    reachability, but rust's pervasive method-name collisions — `init`, `new`,
    `handle_*` — make name-based edges too coarse to be sound; the file-qualified
    emitter-site allowlist is the reliable, low-false-positive form.)

The execution-context map (TS context -> rust context/root/fn) is documented in
parity/L3-CONTEXT-MAP.md and encoded in EMISSIONS below; keep them in sync.

Exit non-zero on any violation. Wired into scripts/local-rust-ci.sh.
"""
import re
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "packages" / "rust-syncer" / "src"

# ---------------------------------------------------------------------------
# TIER 1 — enclosing-fn rules (precise, single-file)
# ---------------------------------------------------------------------------
CRITICAL = [
    {
        "symbol": r"connected_message\s*\(",
        "file": "router.rs",
        "allowed": {"handle_connection"},
        "forbidden": {"on_new_connection", "dispatch_cg_message", "handle_desired_queries"},
        "why": "I-2: `connected` must be emitted on the accept task, never on the "
               "serial CG thread (else the ack is queued behind config_and_hydrate).",
    },
    {
        # `Connection::init()` (version gate + `connected`) is TS's accept-handler
        # call; in rust its effects are on the accept path (accept_connection +
        # handle_connection). It must NEVER be called on the serial CG thread,
        # which would re-couple the `connected` send to config_and_hydrate.
        "symbol": r"\.init\s*\(\s*\)",
        "file": "router.rs",
        "allowed": set(),           # init() must not be called from router.rs at all
        "forbidden": {"on_new_connection"},
        "why": "I-2: `connected`/version gate must not run on the serial CG thread; "
               "`on_new_connection` must not call `.init()`.",
    },
]

# ---------------------------------------------------------------------------
# TIER 2 — cross-file emitter-site allowlist
# ---------------------------------------------------------------------------
# For each client-observable frame primitive: the sanctioned (file, enclosing-fn)
# sites, each annotated with its execution context. A call site anywhere else is
# a violation. This mirrors parity/L3-CONTEXT-MAP.md — keep them in sync.
EMISSIONS = [
    {
        "symbol": "connected_message",
        # (file, fn): context — WHY this site is a sanctioned `connected` source.
        "allowed": {
            ("router.rs", "handle_connection"):
                "accept task — the live emission (prod path); decoupled from the "
                "CG thread, this is the bug-1 fix.",
            ("connection.rs", "init"):
                "accept context by contract (I-2): `Connection::init()` is the 1:1 "
                "TS port; it is NOT called on the prod path (only unit tests), its "
                "live effect runs via handle_connection.",
        },
        "why": "I-1/I-2: `connected` must be emitted on the accept task, never on "
               "the serial CG thread (else the ack is queued behind "
               "config_and_hydrate — prod bug-1).",
    },
    {
        "symbol": "pong_message",
        "allowed": {
            ("ws_server.rs", "run_ws_writer"):
                "writer task — the decoupled keepalive (mirrors TS #maybeSendPong); "
                "this is what keeps pong live under a blocked CG thread.",
            ("connection.rs", "handle_inbound"):
                "CG thread — the explicit ping->pong reply, faithful to TS "
                "connection.ts:220 (answered on the per-connection handler).",
        },
        "why": "I-1: pong liveness is the writer keepalive (decoupled); the ping "
               "reply on the CG thread is faithful. Any OTHER emitter is an "
               "unsanctioned pong source.",
    },
]

FN_RE = re.compile(r"^(\s*)(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(\w+)")


def _test_module_start(lines):
    for i, l in enumerate(lines):
        if "#[cfg(test)]" in l:
            nxt = next((lines[j] for j in range(i + 1, min(i + 3, len(lines)))
                        if lines[j].strip()), "")
            if re.match(r"\s*mod\s+\w+", nxt):
                return i
    return len(lines)


def check_tier1():
    violations = []
    for rule in CRITICAL:
        path = SRC / rule["file"]
        text = path.read_text().splitlines()
        test_start = _test_module_start(text)
        owner = _enclosing_fns(text)
        sym = re.compile(rule["symbol"])
        for i, ln in enumerate(text):
            if i >= test_start:
                break
            code = ln.split("//", 1)[0]
            if sym.search(code):
                fn = owner[i]
                if fn in rule["forbidden"] or (rule["allowed"] and fn not in rule["allowed"]):
                    violations.append(
                        f"[T1] {rule['file']}:{i+1}  `{ln.strip()}`\n"
                        f"    enclosing fn = {fn!r}; must be in {rule['allowed'] or 'NONE'}, "
                        f"never {rule['forbidden']}\n    {rule['why']}"
                    )
    return violations


def _enclosing_fns(lines):
    owner = [None] * len(lines)
    cur = None
    for i, ln in enumerate(lines):
        m = FN_RE.match(ln)
        if m:
            cur = m.group(2)
        owner[i] = cur
    return owner


def check_tier2():
    """Cross-file emitter-site allowlist. For each frame primitive, every call
    site (excluding the primitive's own definition and test modules) must sit in
    a sanctioned (file, enclosing-fn)."""
    violations = []
    files = sorted(SRC.rglob("*.rs"))
    for em in EMISSIONS:
        sym = em["symbol"]
        call_re = re.compile(r"\b" + re.escape(sym) + r"\s*\(")
        def_re = re.compile(r"\bfn\s+" + re.escape(sym) + r"\b")
        for path in files:
            lines = path.read_text().splitlines()
            test_start = _test_module_start(lines)
            owner = _enclosing_fns(lines)
            for i, ln in enumerate(lines):
                if i >= test_start:
                    break
                code = ln.split("//", 1)[0]
                if def_re.search(code):        # the primitive's own definition
                    continue
                if not call_re.search(code):
                    continue
                if "import" in code or ("use " in code and "::" in code):
                    continue                    # a `use ...{connected_message}` line
                site = (path.name, owner[i])
                if site not in em["allowed"]:
                    allowed_str = ", ".join(f"{f}::{fn}" for (f, fn) in sorted(em["allowed"]))
                    violations.append(
                        f"[T2] {sym}() call at {path.name}:{i+1} in "
                        f"`{owner[i]}`  `{ln.strip()}`\n"
                        f"    not a sanctioned emitter site. Sanctioned: {allowed_str}\n"
                        f"    {em['why']}"
                    )
    return violations


def main():
    v = check_tier1() + check_tier2()
    if v:
        print("L3 CALL-TOPOLOGY VIOLATIONS:\n")
        print("\n\n".join(v))
        print(f"\n{len(v)} violation(s).")
        return 1
    print("L3 call-topology guard: OK "
          "(Tier-1 enclosing-fn + Tier-2 cross-file emitter-site allowlist; all "
          "ordering-sensitive emissions in sanctioned execution context).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
