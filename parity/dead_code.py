#!/usr/bin/env python3
"""M12 — dead-code census: rust items with no reachable caller.

M11 (`prod_reachability.py`) asks "can `fn main` reach this?" and reports only
items CLAIMING TS parity. This asks the blunter, broader question over all
three crates: **does anything call this at all?** — and sorts the answers by
how dead they are, so a reviewer can act on the top tier without triaging the
rest.

Origin (2026-09-04): `Engine::set_hydration_time_ms` looked alive — it is `pub`,
it has a doc-comment, and `scripts/test-napi-full.mjs` calls
`engine.setHydrationTimeMs(...)`. But the NAPI addon was deleted in a5e502ad9
("remove NAPI rust-IVM hybrid"): no crate depends on `napi`, no `#[napi]`
attribute survives, and `rust-ivm.node` does not exist, so that script cannot
run. Its only caller is a harness for a boundary that no longer exists. Worse,
the function was itself born from porting a COMMENT — TS pipeline-driver.ts:769
says the view-syncer "resets this ... with setHydrationTime()", but no such
method exists anywhere in the TS repo. Dead code that reads as live is how a
false parity hypothesis survives a code review.

Tiers, most-dead first:
  UNCALLED    the name appears nowhere in the workspace outside its own
              declaration. Highest confidence; usually just deletable.
  NAPI-ONLY   the only mentions are in the orphaned `scripts/` + `agentic/`
              .mjs harnesses for the removed NAPI addon. Dead in the same way,
              but deleting it means deleting the harness too.
  TEST-ONLY   no production caller; only tests / benches / examples / fuzz.
              Either the feature was never wired up (a real gap — this is the
              tier `ClientGroupStorage` lands in) or the item is a test helper
              that belongs behind `#[cfg(test)]`.
  UNREACHABLE has production callers, but the whole cluster is unreachable from
              `fn main`. Delegated to M11's fixpoint.

Resolution is BY NAME and deliberately over-approximates "alive": two `fn new`
in different types share a bucket, so a call to either marks both live. A false
"alive" costs nothing; a false "dead" would make the census untrustworthy.

The one place the name rule bites deliberately: a call from one item named `f`
to another item named `f` is NOT a caller, so a family that only calls itself
reports as dead. That is the intended answer — `closest_join_or_source` has 6
definitions and 7 call sites, every one of them inside the family, and nothing
else in the workspace mentions it. A mutually recursive island is still an
island. The cost is that a genuinely live recursive item whose only external
caller happens to share its name would be missed; external-trait methods (the
realistic case, e.g. a delegating `poll`) are exempted above.

Never reported: `fn main`, `#[no_mangle]`/`#[global_allocator]`-style roots, and
methods implementing an external trait (`Drop`, `Serialize`, `Iterator`, …),
which are invoked by machinery with no in-source call site.

Not covered: struct fields, enum variants, and macro-generated call sites.

Usage: python3 parity/dead_code.py [--tier TIER] [--crate CRATE] [--limit N]
                                   [--csv] [--fail-on TIER]
"""
import os
import re
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "parity"))
from parity_ledger import CRATES, canon  # noqa: E402
from prod_reachability import (  # noqa: E402
    EXTERNAL_TRAIT_HINT, IDENT, ROOT_ATTRS, build, mask, parse_file,
    reachable_set,
)

# Consumer trees outside `src/`. `orphan` dirs are harnesses for the NAPI addon
# deleted in a5e502ad9 — they cannot run (no napi dep, no #[napi], no .node),
# so a mention there does NOT keep an item alive.
TEST_DIRS = ("tests", "benches", "examples", "fuzz")
ORPHAN_DIRS = ("scripts", "agentic")
SRC_EXT = (".rs", ".mjs", ".js", ".ts")


def consumer_text():
    """(test_text, orphan_text) — every non-`src/` consumer, comments masked."""
    test_parts, orphan_parts = [], []
    for crate in CRATES:
        base = os.path.join(ROOT, "packages", f"rust-{crate}")
        for sub, sink in ([(d, test_parts) for d in TEST_DIRS]
                          + [(d, orphan_parts) for d in ORPHAN_DIRS]):
            top = os.path.join(base, sub)
            if not os.path.isdir(top):
                continue
            for dp, dirs, fns in os.walk(top):
                dirs[:] = [d for d in dirs if d not in ("target", "node_modules")]
                for fn in fns:
                    if fn.endswith(SRC_EXT):
                        src = open(os.path.join(dp, fn), errors="replace").read()
                        sink.append(mask(src) if fn.endswith(".rs") else src)
        for fn in ("build.rs",):
            p = os.path.join(base, fn)
            if os.path.exists(p):
                test_parts.append(mask(open(p, errors="replace").read()))
    return "\n".join(test_parts), "\n".join(orphan_parts)


def counts(text):
    c = defaultdict(int)
    for h in IDENT.finditer(text):
        c[h.group(0)] += 1
    return c


def is_root(it, masked):
    if it.name == "main":
        return True
    head = masked[max(0, it.start - 400):it.start].rsplit("\n\n", 1)[-1]
    return any(f"#[{a}" in head for a in ROOT_ATTRS)


def implicit_trait_method(it, crate_traits):
    return it.via_trait is not None and (
        it.via_trait not in crate_traits or EXTERNAL_TRAIT_HINT.match(it.via_trait))


def classify():
    items, by_name, mentions, masks = build()
    reachable = reachable_set(items, by_name, mentions, masks)
    crate_traits = {it.name for it in items if it.kind == "trait"}

    # Production text = `src/` with test modules already blanked by parse_file;
    # `full` keeps them, so their difference is exactly the in-src test text.
    prod, in_src_test = defaultdict(int), defaultdict(int)
    self_span = defaultdict(int)
    for crate, spec in CRATES.items():
        root = os.path.join(ROOT, spec["rust_dir"])
        for dp, _d, fns in os.walk(root):
            for fn in sorted(fns):
                if not fn.endswith(".rs"):
                    continue
                p = os.path.join(dp, fn)
                relpath = os.path.relpath(p, os.path.dirname(root))
                src = open(p, errors="replace").read()
                fitems, m_prod = parse_file(f"rust-{crate}", relpath, src)
                m_all = mask(src)
                for k, v in counts(m_prod).items():
                    prod[k] += v
                # keep only the regions parse_file blanked (the test modules)
                test_only = "".join(
                    b if a == " " and b != " " else " "
                    for a, b in zip(m_prod, m_all))
                for k, v in counts(test_only).items():
                    in_src_test[k] += v
                # Occurrences of an item's OWN name inside its OWN span: the
                # declaration token plus any self-recursion. Subtracting these
                # from the global count leaves genuine call sites. It must be
                # per-name — summing every identifier in every body cancels each
                # call site against the caller it sits in, which reported 981
                # plainly-live items as UNCALLED.
                for it in fitems:
                    body = m_prod[it.start:it.end]
                    self_span[it.name] += sum(
                        1 for h in IDENT.finditer(body) if h.group(0) == it.name)

    ext_test, orphan = consumer_text()
    test = counts(ext_test)
    for k, v in in_src_test.items():
        test[k] += v
    # The NAPI harnesses are JavaScript: they call `setHydrationTimeMs`, not
    # `set_hydration_time_ms`. Key this bucket by canon() (lowercased,
    # underscores stripped) so the boundary's camelCase names still bind.
    orph = defaultdict(int)
    for k, v in counts(orphan).items():
        orph[canon(k)] += v

    rows = []
    for it in items:
        if is_root(it, masks[(it.crate, it.relpath)]):
            continue
        if implicit_trait_method(it, crate_traits):
            continue
        n = it.name
        callers = prod[n] - self_span[n]
        if callers > 0:
            tier = None if it.key in reachable else "UNREACHABLE"
        elif test.get(n, 0) > 0:
            tier = "TEST-ONLY"
        elif orph.get(canon(n), 0) > 0:
            tier = "NAPI-ONLY"
        else:
            tier = "UNCALLED"
        if tier:
            rows.append((tier, it, callers, test.get(n, 0), orph.get(canon(n), 0)))
    return items, rows


ORDER = ("UNCALLED", "NAPI-ONLY", "TEST-ONLY", "UNREACHABLE")


def main():
    argv = sys.argv[1:]

    def opt(flag, default=None):
        return argv[argv.index(flag) + 1] if flag in argv else default

    want, crate = opt("--tier"), opt("--crate")
    limit = int(opt("--limit", "40"))
    fail_on = opt("--fail-on")

    items, census = classify()
    rows = [r for r in census
            if (want is None or r[0] == want.upper())
            and (crate is None or r[1].crate.endswith(crate))]
    rows.sort(key=lambda r: (ORDER.index(r[0]), r[1].crate, r[1].relpath, r[1].line))

    if "--csv" in argv:
        print("tier,crate,file,line,name,kind,prod_callers,test_mentions,orphan_mentions")
        for t, it, c, tm, om in rows:
            print(f"{t},{it.crate},{it.relpath},{it.line},{it.name},{it.kind},{c},{tm},{om}")
        return 0

    per = defaultdict(list)
    for r in rows:
        per[r[0]].append(r)
    for tier in ORDER:
        got = per.get(tier, [])
        if not got:
            continue
        print(f"\n{tier} ({len(got)})")
        for t, it, c, tm, om in got[:limit]:
            extra = []
            if tm:
                extra.append(f"{tm} test")
            if om:
                extra.append(f"{om} napi-harness")
            note = f"  [{', '.join(extra)}]" if extra else ""
            print(f"  {it.crate}/{it.relpath}:{it.line} {it.name} ({it.kind}){note}")
        if len(got) > limit:
            print(f"  … {len(got) - limit} more (--limit N)")

    # Always tally the FULL census, never the --tier/--crate view.
    whole = defaultdict(list)
    for r in census:
        whole[r[0]].append(r)
    tally = ", ".join(f"{t}={len(whole.get(t, []))}" for t in ORDER)
    print(f"\nM12 dead-code census: {len(items)} rust items; {tally}")
    if fail_on:
        bad = sum(len(whole.get(t, [])) for t in ORDER[:ORDER.index(fail_on.upper()) + 1])
        if bad:
            print(f"FAIL: {bad} item(s) at or above {fail_on.upper()}")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
