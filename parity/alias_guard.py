#!/usr/bin/env python3
"""M9 — alias-note guard: every ledger alias (the hand-written 📌 exceptions in
`parity_ledger.py` CRATES[*]["aliases"]) must be VERIFIABLE, not prose.

An alias maps a TS symbol to either
  * a rust TARGET — must name an existing `.rs` file under the crate (or a
    `CROSS-CRATE rust-<crate>` file), and every snake_case identifier it names
    must occur in that file (so a renamed/removed twin fails the guard); or
  * a non-code resolution (`N/A`, `INLINED`, `ABSENT`, `IDENTITY`, …) — then the
    NOTE must cite something checkable: an INVENTIONS `I-n` / PARITY-EXCEPTIONS
    `D-n` id, a `task #n`, an L2 finding `F-…`, a `.rs` file, or a `.ts:LINE`.

Origin (2026-09-03): three aliases were factually wrong and hid real bugs —
`#fanOutResponses` "no per-connection response fan-out by design",
`#failDownstream` "relay-hop failure path", `closeWithError` mapped to the
connection-level method while `types/ws.ts` was never ledgered. Prose that
nothing checks is how divergences survive the ledger.

Usage: python3 parity/alias_guard.py [--list]   (exit 1 when failures > BASELINE)
"""
import os, re, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "parity"))
from parity_ledger import CRATES  # noqa: E402

# Ratchet: may only go DOWN. 2026-09-03 seed after the first sweep.
BASELINE = 0

NON_CODE = re.compile(r"^(N/A|INLINED|ABSENT|IDENTITY|Rust |JS |TS |—)", re.I)
CITATION = re.compile(r"\bI-\d+\b|\bD-\d+\b|\bGAP-\d+\b|task #\d+|\bF-[A-Z0-9-]+\b|[\w/-]+\.rs\b|[\w/-]+\.ts:\d+|\b[0-9a-f]{7,10}\b|\(cross-crate\)|INVENTIONS|PARITY-EXCEPTIONS")
FILE_RE = re.compile(r"([A-Za-z0-9_/-]+\.rs)")
IDENT_RE = re.compile(r"\b([a-z][a-z0-9]*(?:_[a-z0-9]+)+|[A-Z][A-Za-z0-9]+)\b")
SKIP_IDENTS = {"CROSS", "CRATE", "INLINED", "ABSENT", "IDENTITY", "Rust", "N"}

def crate_src(crate):
    return os.path.join(ROOT, "packages", f"rust-{crate}", "src")

_TREE = {}
def tree(crate):
    """All .rs files of a crate (path -> body), cached."""
    if crate not in _TREE:
        files = {}
        for dp, _, fns in os.walk(crate_src(crate)):
            for fn in fns:
                if fn.endswith(".rs"):
                    q = os.path.join(dp, fn)
                    files[q] = open(q, errors="replace").read()
        _TREE[crate] = files
    return _TREE[crate]

def candidates(crate, rel):
    """Files whose path ends with `rel` (a basename or a suffix path), in this
    crate first, then the other rust crates (`CROSS-CRATE rust-<x>` targets)."""
    out = []
    for c in ([crate] if crate else []) + [x for x in ("cvr", "ivm", "syncer") if x != crate]:
        for q in tree(c):
            if q.endswith("/" + rel) or q.endswith("/" + os.path.basename(rel)):
                out.append((c, q))
    return out

def crates_named(target):
    return re.findall(r"rust-(cvr|ivm|syncer)", target)

def check(crate, ts, target, note):
    t = target.strip()
    if NON_CODE.match(t):
        if CITATION.search(note) or CITATION.search(target):
            return None
        return f"non-code alias without a checkable citation: {ts!r} -> {target!r} | {note!r}"
    files = FILE_RE.findall(t)
    stripped = FILE_RE.sub(" ", t)
    stripped = re.sub(r"rust-(cvr|ivm|syncer)", " ", stripped)
    idents = [i for i in IDENT_RE.findall(stripped) if i not in SKIP_IDENTS]
    search_crates = crates_named(t) or ([crate] if crate else ["cvr", "ivm", "syncer"])
    if files:
        cands = candidates(crate, files[0])
        if not cands:
            return f"target file missing: {ts!r} -> {files[0]!r}"
        bodies = [b for c, q in cands for b in [tree(c)[q]]]
        missing = [i for i in idents if not any(i in b for b in bodies)]
        if missing:
            return f"target symbol(s) not in {files[0]}: {ts!r} -> {missing}"
        return None
    if re.search(r"macro|idiom", t + " " + note) and not idents:
        return None
    # No file named: a `rust-<crate> module/symbol` mention — every identifier must
    # exist somewhere in that crate (as text, or as a module/dir/file stem).
    missing = []
    for i in idents:
        ok = False
        for c in search_crates:
            for q, b in tree(c).items():
                if i in b or f"/{i}.rs" in q or f"/{i}/" in q:
                    ok = True
                    break
            if ok:
                break
        if not ok:
            missing.append(i)
    if missing:
        return f"target names no .rs file and symbol(s) not found in rust-{'/'.join(search_crates)}: {ts!r} -> {missing}"
    return None

def main():
    failures = []
    total = 0
    for crate, spec in CRATES.items():
        for ts, (target, note) in spec.get("aliases", {}).items():
            total += 1
            f = check(crate, ts, target, note)
            if f:
                failures.append(f"[{crate}] {f}")
    if "--list" in sys.argv or failures:
        for f in failures:
            print("  " + f)
    print(f"M9 alias guard: {len(failures)} unverifiable of {total} aliases (baseline {BASELINE})")
    if len(failures) > BASELINE:
        print("FAIL: alias notes must cite a rust file/symbol or an I-/D-/task/F- id")
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
