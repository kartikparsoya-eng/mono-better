#!/usr/bin/env python3
"""
L3 — Call-topology / execution-context guard.

Bug-1 class (connect-ack serialized behind hydrate) was a ported call moving
from a concurrent TS context into the serial rust CG thread. The body-level
ledgers (L1/L2) cannot see this — they audit function interiors, not which
context a call runs in.

This guard pins the execution context of ORDERING-SENSITIVE emissions: for each
critical call it asserts the enclosing rust function is in the sanctioned set
(and NOT in a forbidden, serialized context). It is a targeted mechanical guard,
not a full cross-language call graph.

Add a row to CRITICAL when a new client-observable frame/emission has a strict
"must not be serialized behind another message" contract (see parity/INVENTIONS.md).

Exit non-zero on any violation. Wire into CI next to repo-coverage.sh.
"""
import re
import sys
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "packages" / "rust-syncer" / "src"

# symbol regex : {file, allowed enclosing fns, forbidden enclosing fns, why}
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
        # The version gate on the CG thread must be check_version (no sink),
        # NOT init() (which would re-send connected → double-send / re-couple).
        "symbol": r"\.init\s*\(\s*\)",
        "file": "router.rs",
        "allowed": set(),           # init() must not be called from router.rs at all
        "forbidden": {"on_new_connection"},
        "why": "I-2: on_new_connection must call check_version (no `connected`), not init().",
    },
]

FN_RE = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(\w+)")


def enclosing_fns(lines):
    """For each line index, the name of the nearest preceding `fn` declaration
    at column <= that line's fn (approximate: nearest preceding fn decl)."""
    owner = [None] * len(lines)
    cur = None
    for i, ln in enumerate(lines):
        m = FN_RE.match(ln)
        if m:
            cur = m.group(1)
        owner[i] = cur
    return owner


def check():
    violations = []
    for rule in CRITICAL:
        path = SRC / rule["file"]
        text = path.read_text().splitlines()
        # Only guard production paths. The test MODULE is a `#[cfg(test)]` line
        # immediately followed by `mod ` (scattered `#[cfg(test)]` on helper
        # fns/impls are NOT the boundary). Guard applies above the first such
        # module.
        test_start = len(text)
        for i, l in enumerate(text):
            if "#[cfg(test)]" in l:
                nxt = next((text[j] for j in range(i + 1, min(i + 3, len(text)))
                            if text[j].strip()), "")
                if re.match(r"\s*mod\s+\w+", nxt):
                    test_start = i
                    break
        owner = enclosing_fns(text)
        sym = re.compile(rule["symbol"])
        for i, ln in enumerate(text):
            if i >= test_start:
                break
            code = ln.split("//", 1)[0]  # ignore line comments (prose mentions .init())
            if sym.search(code):
                fn = owner[i]
                if fn in rule["forbidden"] or (rule["allowed"] and fn not in rule["allowed"]):
                    violations.append(
                        f"{rule['file']}:{i+1}  `{ln.strip()}`\n"
                        f"    enclosing fn = {fn!r}; must be in {rule['allowed'] or 'NONE'}, "
                        f"never {rule['forbidden']}\n    {rule['why']}"
                    )
    return violations


def main():
    v = check()
    if v:
        print("L3 CALL-TOPOLOGY VIOLATIONS:\n")
        print("\n\n".join(v))
        print(f"\n{len(v)} violation(s).")
        return 1
    print("L3 call-topology guard: OK (all ordering-sensitive emissions in sanctioned context).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
