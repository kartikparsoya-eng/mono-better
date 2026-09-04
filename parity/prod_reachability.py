#!/usr/bin/env python3
"""M11 — prod-path reachability: a ported rust symbol that the production
binary cannot reach does not count as parity.

L1 (parity_ledger) binds a TS symbol to a rust symbol by NAME. It never asks
whether anything CALLS the rust side. A faithful, fully-tested port of a TS
file that nothing wires up therefore scores as **1:1** while production runs
completely different code.

Origin (2026-09-04): `zqlite/src/database-storage.ts` — TS gives every Take/Cap
operator SQLite-backed, spillable storage (syncer.ts:124 `DatabaseStorage.create`
→ syncer.ts:227 `createClientGroupStorage(id)` → pipeline-driver.ts:1197
`#createStorage()` → builder.ts:366/376). rust ports the whole file into
`sqlite/database_storage.rs`, and MAP-ivm.md scores it **1:1** — but
`ClientGroupStorage` is constructed only in `tests/db_test.rs`, `create_storage()`
is defined 4× and called 0× in `src/`, `MemoryStorage` is referenced only by
those 4 dead definitions, and rust `Take`/`Cap` carry their own private
`TakeStorage`/`CapStorage` HashMaps instead. Reading the doc-comments said
"ported"; reading the call sites said "dead".

Method: build an item-level mention graph over the three crates' `src/` (with
comments AND string literals masked — take.rs:67 mentions `DatabaseStorage` in
a comment, which is exactly the false signal that hid this), then run a fixpoint
from the production entry point (`fn main` in rust-syncer/src/main.rs) plus
implicit roots (`#[global_allocator]` &c).

Resolution rules, chosen to over-approximate (a false "reachable" is harmless;
a false "dead" is a bad guard):
  * FREE items (types, free fns, consts) are reached by a bare name mention.
  * OWNED items (anything inside an `impl`/`trait` block) are reached only when
    their target TYPE is reachable AND their name is mentioned by reachable
    code. Owner alone is not enough: `impl BuilderDelegate for EngineDelegate`
    is live, but its `create_storage` method is not. Requiring a mention is
    safe because an unqualified `x.method()` call still writes the name.
  * Methods implementing an EXTERNAL trait (Drop, Display, From, Iterator,
    serde, …) are exempt from the mention rule — they are invoked implicitly,
    so the owner type being reachable is the whole signal.
Without the mention rule, ubiquitous method names bridge every dead cluster
into the live graph: one reachable `Foo::new(…)` marked EVERY `fn new` in the
workspace reachable, which then lit up `ClientGroupStorage` and hid the very
divergence this guard exists to find.

Reported failures are unreachable rust items whose canon() name matches a TS
symbol in some crate's ledgered `ts_files` — i.e. exactly the items that are
CLAIMING parity. Unreachable rust-only helpers are not this guard's business.

Usage: python3 parity/prod_reachability.py [--list] [--all]
       (exit 1 when failures > BASELINE)
"""
import os
import re
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "parity"))
from parity_ledger import CRATES, canon, expand_ts_files, extract_ts  # noqa: E402

# Ratchet: may only go DOWN. Seeded 2026-09-04 at the census the guard found on
# the day it was written — NOT at 0. These 72 are a real backlog, not a clean
# bill of health: each is a ported symbol whose TS twin TS's OWN SERVER reaches
# while rust prod does not, so each must end up wired into the rust prod path,
# deleted, or registered in parity/INVENTIONS.md + REACHABILITY_ALIASES. The
# value of the ratchet today is that it cannot GROW.
#
# It is 72 and not 147 because of the TS comparison in `main()`: the other 75
# unreachable ported items have TS twins that TS's server does not reach either
# (`zql/src/query/named.ts` and `query-registry.ts` are re-exported only through
# `zero-client/src/mod.ts`; zero-server names `mutator-registry.ts` in an
# erased `import type`). rust-syncer is a server — not reaching a client-only
# API is parity, not a gap, and counting those 75 as failures buries the real
# ones. `--client-only` lists them.
BASELINE = 72

# Items reached without an in-source call site.
ROOT_ATTRS = ("global_allocator", "no_mangle", "ctor", "panic_handler",
              "export_name", "tokio::main")
ENTRY = ("rust-syncer", "src/main.rs", "main")

# Ported-but-unreachable is EXPECTED for these: the item's TS twin is itself
# only reachable from a TS entry point rust does not have (client fluent API,
# test-only helpers). Key: "<crate>/<relpath>::<name>", value: citation.
REACHABILITY_ALIASES = {}


# ---------------------------------------------------------------------------
# Lexing: mask comments + string/char literals, preserving byte offsets so line
# numbers and spans stay exact.
# ---------------------------------------------------------------------------
def mask(src: str) -> str:
    out = list(src)
    i, n = 0, len(src)

    def blank(a, b):
        for k in range(a, min(b, n)):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        c = src[i]
        if c == "/" and i + 1 < n and src[i + 1] == "/":
            j = src.find("\n", i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
        elif c == "/" and i + 1 < n and src[i + 1] == "*":
            depth, j = 1, i + 2
            while j < n and depth:
                if src.startswith("/*", j):
                    depth += 1
                    j += 2
                elif src.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            blank(i, j)
            i = j
        elif c == "r" and i + 1 < n and src[i + 1] in '"#':
            m = re.match(r'r(#*)"', src[i:])
            if not m:
                i += 1
                continue
            close = '"' + m.group(1)
            j = src.find(close, i + len(m.group(0)))
            j = n if j < 0 else j + len(close)
            blank(i, j)
            i = j
        elif c == '"':
            j = i + 1
            while j < n:
                if src[j] == "\\":
                    j += 2
                elif src[j] == '"':
                    j += 1
                    break
                else:
                    j += 1
            blank(i, j)
            i = j
        elif c == "'":
            # char literal vs lifetime: 'a' / '\n' / '\u{1}' are literals,
            # 'a in `&'a str` is a lifetime (no closing quote).
            m = re.match(r"'(?:\\(?:u\{[0-9a-fA-F]+\}|.)|[^\\'])'", src[i:])
            if m:
                blank(i, i + m.end())
                i += m.end()
            else:
                i += 1
        else:
            i += 1
    return "".join(out)


IDENT = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\b")
VIS = r"(?:pub(?:\s*\([^)]*\))?\s+)?"
MODS = r"(?:default\s+)?(?:const\s+)?(?:async\s+)?(?:unsafe\s+)?(?:extern\s+\S+\s+)?"
RE_FN = re.compile(rf"(?m)^[ \t]*{VIS}{MODS}fn\s+(\w+)")
RE_TYPE = re.compile(rf"(?m)^[ \t]*{VIS}(struct|enum|trait|union)\s+(\w+)")
RE_CONST = re.compile(rf"(?m)^[ \t]*{VIS}(?:const|static)\s+(?:mut\s+)?(\w+)")
RE_ALIAS = re.compile(rf"(?m)^[ \t]*{VIS}type\s+(\w+)")
RE_IMPL = re.compile(r"(?m)^[ \t]*(?:unsafe\s+)?impl(?:\s*<.*?>)?\s+([^{;]*?)\{", re.S)
RE_MOD = re.compile(r"(?m)^[ \t]*(?:pub\s+)?mod\s+(\w+)\s*\{")
RE_TESTMOD_NAME = re.compile(r"^(tests?|test_\w+|\w*_tests?)$")
RE_ATTR = re.compile(r"^[ \t]*#!?\[")


def has_cfg_test(m: str, src: str, at: int) -> bool:
    """True when the item at `at` carries a #[cfg(test)] attribute. Walks the
    contiguous attribute/doc block above it (doc comments read as blank in the
    masked text, so they are skipped rather than terminating the block)."""
    head = m.rfind("\n", 0, at)
    while head > 0:
        ls = m.rfind("\n", 0, head) + 1
        line = m[ls:head]
        if not line.strip():
            head = ls - 1
            continue
        if not RE_ATTR.match(line):
            return False
        if "cfg(test)" in src[ls:head]:
            return True
        head = ls - 1
    return False


def span_from(masked: str, start: int):
    """(body_start, end) for the item declared at `start`: to the matching brace
    of its first top-level `{`, or to the `;` that ends a bodyless decl."""
    i, n = start, len(masked)
    angle = paren = 0
    while i < n:
        c = masked[i]
        if c == "<":
            angle += 1
        elif c == ">":
            angle = max(0, angle - 1)
        elif c == "(":
            paren += 1
        elif c == ")":
            paren = max(0, paren - 1)
        elif c == ";" and not angle and not paren:
            return start, i + 1
        elif c == "{" and not paren:
            # `where T: Trait<{N}>` is vanishingly rare; a `{` outside parens
            # after the signature is the body.
            depth, j = 1, i + 1
            while j < n and depth:
                if masked[j] == "{":
                    depth += 1
                elif masked[j] == "}":
                    depth -= 1
                j += 1
            return start, j
        i += 1
    return start, n


def impl_target(rest: str):
    """Target type of an `impl [Trait for] Type` header."""
    part = rest.split(" for ")[-1] if " for " in rest else rest
    part = re.sub(r"<.*?>", " ", part)
    for m in IDENT.finditer(part):
        w = m.group(0)
        if w not in ("dyn", "impl", "where", "mut", "const"):
            return w
    return None


class Item:
    __slots__ = ("key", "name", "kind", "crate", "relpath", "line", "start",
                 "end", "owner", "via_trait")

    def __init__(self, key, name, kind, crate, relpath, line, start, end):
        self.key, self.name, self.kind = key, name, kind
        self.crate, self.relpath, self.line = crate, relpath, line
        self.start, self.end = start, end
        self.owner = self.via_trait = None


def parse_file(crate, relpath, src):
    """(items, masked). Test modules and #[cfg(test)] items are masked out."""
    m = mask(src)
    # Blank test-module bodies: a whole balanced block, so brace depth
    # stays consistent for every span computed afterwards. `#[cfg(test)]` on a
    # non-mod item is NOT blanked — brace-matching from an attribute swallows
    # the enclosing block's `{` without its `}` and desyncs every later span
    # (it hid ~2500 lines of view_syncer.rs, and with them the real call sites
    # of init_and_reset_common / check_client_schema). Such items are dropped
    # individually below instead.
    for hit in list(RE_MOD.finditer(m)):
        if not (RE_TESTMOD_NAME.match(hit.group(1))
                or has_cfg_test(m, src, hit.start())):
            continue
        _, end = span_from(m, hit.start())
        m = m[:hit.start()] + re.sub(r"[^\n]", " ", m[hit.start():end]) + m[end:]

    impls = []
    for hit in RE_IMPL.finditer(m):
        head = hit.group(1)
        tgt = impl_target(head)
        if tgt:
            tr = None
            if " for " in head:
                tr = impl_target(head.split(" for ")[0] + " ")
            impls.append((hit.start(), span_from(m, hit.start())[1], tgt, tr))
    # a `trait T { fn f(); }` body owns its default/required methods
    for hit in RE_TYPE.finditer(m):
        if hit.group(1) == "trait":
            impls.append((hit.start(), span_from(m, hit.start())[1],
                          hit.group(2), None))

    items = []
    seen = set()

    def add(name, kind, at):
        if (name, at) in seen or has_cfg_test(m, src, at):
            return
        start, end = span_from(m, at)
        seen.add((name, at))
        line = m.count("\n", 0, at) + 1
        it = Item(f"{crate}/{relpath}::{name}@{line}", name, kind, crate,
                  relpath, line, start, end)
        # innermost enclosing impl / trait block => owner type
        best = None
        for s_, e_, tgt, tr in impls:
            if s_ < at < e_ and (best is None or s_ > best[0]):
                best = (s_, tgt, tr)
        if best:
            it.owner, it.via_trait = best[1], best[2]
        items.append(it)

    for hit in RE_FN.finditer(m):
        add(hit.group(1), "fn", hit.start())
    for hit in RE_TYPE.finditer(m):
        add(hit.group(2), hit.group(1), hit.start())
    for hit in RE_CONST.finditer(m):
        add(hit.group(1), "const", hit.start())
    for hit in RE_ALIAS.finditer(m):
        add(hit.group(1), "type", hit.start())
    return items, m


def crate_files():
    for crate, spec in CRATES.items():
        root = os.path.join(ROOT, spec["rust_dir"])
        for dp, _d, fns in os.walk(root):
            for fn in sorted(fns):
                if fn.endswith(".rs"):
                    p = os.path.join(dp, fn)
                    yield (f"rust-{crate}", os.path.relpath(p, os.path.dirname(root)),
                           open(p, errors="replace").read())


def build():
    items, masks = [], {}
    for crate, relpath, src in crate_files():
        fitems, m = parse_file(crate, relpath, src)
        masks[(crate, relpath)] = m
        items.extend(fitems)

    by_name = defaultdict(list)
    for it in items:
        by_name[it.name].append(it)

    # Names mentioned inside each item's body (declaration sites excluded via
    # the masked spans of nested items being their own entries).
    mentions = {}
    for it in items:
        body = masks[(it.crate, it.relpath)][it.start:it.end]
        mentions[it.key] = {h.group(0) for h in IDENT.finditer(body)}
    return items, by_name, mentions, masks


TYPE_KINDS = {"struct", "enum", "trait", "union", "type"}
# Traits whose methods run without an in-source call site.
EXTERNAL_TRAIT_HINT = re.compile(
    r"^(Drop|Display|Debug|Default|Clone|Copy|From|Into|TryFrom|TryInto|Iterator|"
    r"IntoIterator|Deref|DerefMut|Index|IndexMut|Add|Sub|Mul|Div|Neg|Not|"
    r"PartialEq|Eq|PartialOrd|Ord|Hash|Serialize|Deserialize|Future|Error|"
    r"AsRef|AsMut|Borrow|BorrowMut|Send|Sync|Fn|FnMut|FnOnce|Write|Read|"
    r"Termination|FromStr|ToString|Visitor)")


def reachable_set(items, by_name, mentions, masks):
    """Fixpoint: free items by mention, owned items by owner + mention."""
    crate_traits = {it.name for it in items if it.kind == "trait"}
    free = defaultdict(list)
    owned = []
    types = defaultdict(list)
    for it in items:
        if it.owner is None:
            free[it.name].append(it)
            if it.kind in TYPE_KINDS:
                types[it.name].append(it)
        else:
            owned.append(it)

    reach = set(roots(items, masks))
    mentioned = set()
    by_key = {it.key: it for it in items}
    changed = True
    while changed:
        changed = False
        for k in list(reach):
            for n in mentions.get(k, ()):
                if n not in mentioned:
                    mentioned.add(n)
                    changed = True
        for n in list(mentioned):
            for it in free.get(n, ()):
                if it.key not in reach:
                    reach.add(it.key)
                    changed = True
        for it in owned:
            if it.key in reach:
                continue
            if not any(t.key in reach for t in types.get(it.owner, ())):
                continue
            implicit = it.via_trait is not None and (
                it.via_trait not in crate_traits
                or EXTERNAL_TRAIT_HINT.match(it.via_trait))
            if implicit or it.name in mentioned:
                reach.add(it.key)
                changed = True
    return reach


def roots(items, masks):
    out = set()
    for it in items:
        if (it.crate, it.relpath, it.name) == ENTRY:
            out.add(it.key)
            continue
        head = masks[(it.crate, it.relpath)][max(0, it.start - 400):it.start]
        tail = head.rsplit("\n\n", 1)[-1]
        if any(f"#[{a}" in tail or f"#[{a}]" in tail for a in ROOT_ATTRS):
            out.add(it.key)
    return out


def ts_symbols():
    """canon(name) -> {("<crate>:<basename>", "<repo-relative ts path>")}."""
    out = defaultdict(set)
    for crate, spec in CRATES.items():
        for f in expand_ts_files(spec):
            p = os.path.join(ROOT, f)
            if not os.path.exists(p):
                continue
            for c, name, _kind, _line, *_ in extract_ts(p):
                out[c].add((f"{crate}:{os.path.basename(f)}", f))
    return out


# The zero-cache SERVER entry points. `main.ts` is the dispatcher; the others
# are worker entries it spawns as separate processes, so they are roots too and
# not reachable from main.ts by any static import.
TS_ENTRIES = tuple(
    f"packages/zero-cache/src/server/{f}.ts"
    for f in ("main", "syncer", "replicator", "change-streamer", "write-worker",
              "mutator", "worker-dispatcher"))

# `import type {…} from '…'` is fully erased by the compiler: zero-server names
# `mutator-registry.ts` that way and never pulls its `validateInput` call into
# the running server. A plain `import {…}` — even with inline `type` specifiers
# — does execute the module, so only the `import type` form is skipped.
RE_TS_IMPORT = re.compile(
    r"""(?m)^\s*(?:import|export)\s+(?!type\s)(?:[^'"]*?\sfrom\s+)?['"]([^'"]+)['"]""")


def ts_server_reachable():
    """Repo-relative TS files the zero-cache server actually executes.

    Answers the question M11 cannot answer from the rust side alone: is the TS
    twin of an unreachable rust item on TS's OWN server path? If it is not —
    `zql/src/query/named.ts` is re-exported only through `zero-client/src/mod.ts`
    — then rust-syncer, which is a server, is not missing anything by not
    reaching it. If it IS, the rust side has a real wiring gap.
    """
    seen, queue = set(), []
    for e in TS_ENTRIES:
        if os.path.exists(os.path.join(ROOT, e)):
            seen.add(e)
            queue.append(e)
    while queue:
        rel = queue.pop()
        try:
            src = open(os.path.join(ROOT, rel), errors="replace").read()
        except OSError:
            continue
        base = os.path.dirname(rel)
        for hit in RE_TS_IMPORT.finditer(src):
            spec = hit.group(1)
            if not spec.startswith("."):
                continue  # bare package specifier: outside the repo tree
            tgt = os.path.normpath(os.path.join(base, spec))
            if not tgt.endswith(".ts"):
                tgt += ".ts"
            if tgt not in seen and os.path.exists(os.path.join(ROOT, tgt)):
                seen.add(tgt)
                queue.append(tgt)
    return seen


def main():
    items, by_name, mentions, masks = build()
    reachable = reachable_set(items, by_name, mentions, masks)

    ts = ts_symbols()
    server_ts = ts_server_reachable()
    infra = {}
    for crate, spec in CRATES.items():
        for f in spec.get("infra_rust", ()):
            infra[(f"rust-{crate}", f)] = True

    failures, dead_ported, dead_other, client_only = [], [], [], []
    for it in items:
        if it.key in reachable:
            continue
        rp = it.relpath.split("/", 1)[-1]
        origins = ts.get(canon(it.name))
        if not origins or (it.crate, rp) in infra:
            dead_other.append(it)
            continue
        # Prefer the TS file this rust file MIRRORS (1:1 file rule) over the
        # global name index: `push` is defined in 20 ledgered TS files, so the
        # unrestricted index makes every common method name look server-
        # reachable. Fall back to the whole index only when the rust file has no
        # mirrored twin.
        mine = {o for o in origins
                if canon(os.path.basename(o[1]).rsplit(".", 1)[0]) == canon(
                    os.path.basename(rp).rsplit(".", 1)[0])}
        origins = mine or origins
        labels = sorted(lbl for lbl, _path in origins)
        dead_ported.append((it, labels))
        # THE TS COMPARISON. Unreachable in rust only matters if TS's own SERVER
        # reaches the twin. When every TS file defining the symbol sits off the
        # server path (client fluent API, zql test helpers), rust-syncer — a
        # server — is not missing anything, and saying otherwise buries the real
        # gaps under a client-API backlog.
        if not any(path in server_ts for _lbl, path in origins):
            client_only.append((it, labels))
            continue
        alias = REACHABILITY_ALIASES.get(f"{it.crate}/{rp}::{it.name}")
        if alias is None:
            failures.append(
                f"{it.crate}/{rp}:{it.line} `{it.name}` ({it.kind}) claims parity with "
                f"{', '.join(labels)}, which TS's SERVER reaches, but nothing in "
                f"rust prod reaches it")

    # Only spill the backlog when the ratchet is actually breached (or asked):
    # a passing gate printing 72 lines every CI run trains people to skip it.
    if "--list" in sys.argv or len(failures) > BASELINE:
        for f in failures:
            print("  " + f)
    if "--all" in sys.argv:
        print(f"\n  (rust-only unreachable items, not a parity claim: {len(dead_other)})")
        for it in sorted(dead_other, key=lambda x: (x.crate, x.relpath, x.line))[:80]:
            print(f"    {it.crate}/{it.relpath}:{it.line} {it.name} ({it.kind})")
    if "--client-only" in sys.argv:
        print(f"\n  (TS twin is off TS's own server path — not a rust gap: {len(client_only)})")
        for it, labels in sorted(client_only, key=lambda x: (x[0].crate, x[0].relpath, x[0].line)):
            print(f"    {it.crate}/{it.relpath}:{it.line} {it.name} <- {', '.join(labels)}")
    print(f"M11 prod-path reachability: {len(items)} rust items, {len(reachable)} reachable "
          f"from {ENTRY[0]}/{ENTRY[1]}; {len(dead_ported)} ported-but-unreachable "
          f"({len(client_only)} whose TS twin is off TS's server path too), "
          f"{len(failures)} unresolved (baseline {BASELINE})")
    if len(failures) > BASELINE:
        print("FAIL: wire it into the prod path, delete it, or register the "
              "substitution in parity/INVENTIONS.md + REACHABILITY_ALIASES")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
