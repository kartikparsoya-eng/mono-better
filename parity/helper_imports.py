#!/usr/bin/env python3
"""M10 — helper-import ledger: every VALUE symbol a ported TS file imports from
an UNLEDGERED TS file (shared/src/*, zero-cache/src/types/*, zero-protocol/src/*,
…) must have a rust twin (a definition whose canon() name matches, in any of the
three crates) or a verifiable entry in `parity_ledger.HELPER_ALIASES`
(checked by the M9 rules: a named rust file + identifiers that exist there, or a
non-code resolution citing an I-/D-/GAP-/task/F- id).

Origin (2026-09-03/04): the L1 ledger only sees files listed in CRATES[*]
["ts_files"]. `types/ws.ts::elide` (1011 close reason truncation) and
`types/strings.ts` were never in scope, so a whole helper class was invisible —
and the same sweep then surfaced `client-schema.ts::checkClientSchema` (ported
under another name with different client-visible messages), `shared/src/
string-compare.ts` (UTF-16 vs UTF-8 ordering), and `like.ts`'s `String(pattern)`
/`assertString(lhs)` coercions. Type-only imports (`import type`, `type X`)
are skipped: they become struct shapes, which L2/M8 cover.

Usage: python3 parity/helper_imports.py [--list]   (exit 1 when failures > BASELINE)
"""
import os, re, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "parity"))
from parity_ledger import CRATES, HELPER_ALIASES, canon, expand_ts_files  # noqa: E402
import alias_guard  # noqa: E402

# Ratchet: may only go DOWN.
BASELINE = 0

IMPORT_NAMED = re.compile(r"import\s+(type\s+)?\{([^}]*)\}\s+from\s+'([^']+)'", re.S)
IMPORT_OTHER = re.compile(r"import\s+(type\s+)?(\*\s+as\s+\w+|\w+)\s+from\s+'([^']+)'")
DEF = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:unsafe\s+)?"
                 r"(?:fn|struct|enum|const|static|type|trait|macro_rules!)\s+([A-Za-z_][A-Za-z0-9_]*)")

def rust_defs():
    """canon(name) -> {relative .rs paths} over all three crates."""
    out = {}
    for crate in CRATES:
        for path, body in alias_guard.tree(crate).items():
            for line in body.split("\n"):
                m = DEF.match(line)
                if m:
                    out.setdefault(canon(m.group(1)), set()).add(
                        os.path.relpath(path, os.path.join(ROOT, "packages")))
    return out

def rust_module_stems():
    stems = set()
    for crate in CRATES:
        for path in alias_guard.tree(crate):
            stems.add(os.path.basename(path)[:-3])
    return stems

def helper_imports():
    """(target ts path relative to packages/, symbol) -> {importing crate}."""
    ledgered = set()
    for spec in CRATES.values():
        for f in expand_ts_files(spec):
            ledgered.add(os.path.normpath(os.path.join(ROOT, f)))
    rows = {}
    for crate, spec in CRATES.items():
        for f in expand_ts_files(spec):
            path = os.path.join(ROOT, f)
            src = open(path, encoding="utf-8").read()
            for m in IMPORT_NAMED.finditer(src):
                spec_path = m.group(3)
                if not spec_path.startswith("."):
                    continue
                target = os.path.normpath(os.path.join(os.path.dirname(path), spec_path))
                if target in ledgered or not os.path.exists(target):
                    continue
                rel = os.path.relpath(target, os.path.join(ROOT, "packages"))
                for item in m.group(2).split(","):
                    item = item.strip()
                    if not item or m.group(1) or item.startswith("type "):
                        continue
                    name = item.split(" as ")[0].strip()
                    rows.setdefault((rel, name), set()).add(crate)
            for m in IMPORT_OTHER.finditer(src):
                spec_path = m.group(3)
                if m.group(1) or not spec_path.startswith("."):
                    continue
                target = os.path.normpath(os.path.join(os.path.dirname(path), spec_path))
                if target in ledgered or not os.path.exists(target):
                    continue
                rel = os.path.relpath(target, os.path.join(ROOT, "packages"))
                rows.setdefault((rel, "*"), set()).add(crate)
    return rows

def main():
    defs = rust_defs()
    stems = rust_module_stems()
    rows = helper_imports()
    failures, twins, aliased = [], 0, 0
    for (rel, name), crates in sorted(rows.items()):
        key = f"{rel}::{name}"
        if name == "*":
            stem = os.path.basename(rel)[:-3].replace("-", "_")
            if stem in stems:
                twins += 1
                continue
        elif canon(name) in defs:
            twins += 1
            continue
        alias = HELPER_ALIASES.get(key)
        if alias is None:
            failures.append(f"{key} (imported by rust-{'/'.join(sorted(crates))}): no rust twin and no HELPER_ALIASES entry")
            continue
        target, note = alias
        problem = alias_guard.check(None, key, target, note)
        if problem:
            failures.append(problem)
        else:
            aliased += 1
    stale = [k for k in HELPER_ALIASES if k not in {f"{r}::{n}" for r, n in rows}]
    for k in stale:
        failures.append(f"stale HELPER_ALIASES entry (no ported file imports it any more): {k}")
    if "--list" in sys.argv or failures:
        for f in failures:
            print("  " + f)
    print(f"M10 helper-import ledger: {len(rows)} helper imports — {twins} rust twins by name, "
          f"{aliased} aliased, {len(failures)} unresolved (baseline {BASELINE})")
    if len(failures) > BASELINE:
        print("FAIL: add the rust twin (1:1 name) or a verifiable HELPER_ALIASES entry")
        return 1
    return 0

if __name__ == "__main__":
    sys.exit(main())
