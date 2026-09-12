#!/usr/bin/env python3
"""M16 — citation freshness: every `<file>.ts:<line>` a rust doc-comment cites
must still point at the symbol it names.

AGENTS.md rule 6 makes the 1:1 mapping auditable through `Port of TS <symbol>
(<file>.ts:<line>)` citations. A citation goes stale two ways:
  * DEAD      — the cited TS file no longer exists at any path ending with the
                cited suffix (renamed, deleted, or a typo);
  * STALE     — the file exists but the symbol the comment names is nowhere
                within ±WINDOW lines of the cited line (TS moved on, or the
                port cites the wrong place). This is the TS-drift signal: a
                rule-6 re-read starts from the citation, so a citation that
                points at the wrong code sends the next editor to the wrong
                spec.
Two more classes are reported but do not fail the guard:
  * AMBIGUOUS — a bare basename matching several TS files, none of which has
                the symbol in the window (a resolvable one passes);
  * UNANCHORED — the comment names no symbol (`(cvr.ts:882)` after prose), so
                only file existence is checkable.

Symbol extraction: the last backtick-quoted token before the citation on the
same comment line (`#privateMethod`, `Class.member`, `fn()` are normalised to
their identifiers), else the previous comment line's last backtick token, else
the last camelCase / PascalCase / `#name` word before the citation.

Usage: python3 parity/citation_freshness.py [--list] [--window N] [--selftest]
       exit 1 when DEAD + STALE > BASELINE (ratchet: may only go down)
"""
import os, re, sys
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Ratchet: may only go DOWN. 2026-09-12 seed = 0 after the first sweep re-cited
# every drifted line (see the commit that added this guard).
BASELINE = 0

# ±lines around the cited line(s) the named symbol must appear in. A cited
# method body can run past this, but the citation names its *declaration*
# (or the exact line), so a symbol further away than this has drifted.
WINDOW = 40

CRATES = ("rust-cvr", "rust-ivm", "rust-syncer")
RS_SKIP_DIRS = {"target", "node_modules"}
TS_SKIP_DIRS = {"node_modules", "dist", "out", "build", ".turbo", "coverage"}
TS_EXT = (".ts", ".mts", ".tsx")

CITE = re.compile(
    r"(?P<file>[A-Za-z0-9_$][A-Za-z0-9_./$-]*\.(?:ts|mts|tsx)):(?P<line>\d+)(?P<rest>(?:[-–,/]\d+)*)"
)
BACKTICK = re.compile(r"`([^`]+)`")
IDENT = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")
# A bare (unquoted) symbol: camelCase, PascalCase-with-a-second-cap, or #name.
BARE = re.compile(r"#[A-Za-z_][A-Za-z0-9_]*|\b[a-z][a-z0-9]*[A-Z][A-Za-z0-9]*\b|\b[A-Z][a-z0-9]+[A-Z][A-Za-z0-9]*\b|\b[A-Z]{2,}[a-z][A-Za-z0-9]*\b|\b[A-Z][A-Z0-9]+(?:_[A-Z0-9]+)+\b")
DOTTED = re.compile(r"[A-Za-z_#$][\w#$]*(?:\.[A-Za-z_#$][\w$]*)+(?:\(\))?")
COMMENT_LINE = re.compile(r"^\s*(//|/\*|\*|#\[doc)")
# Words the bare-symbol fallback must not mistake for a symbol.
NOT_SYMBOLS = {"TypeScript", "JavaScript", "WebSocket", "JSON", "SQLite", "PostgreSQL"}


def ts_index():
    """rel path (posix) -> absolute path for every TS source under packages/."""
    idx = {}
    base = os.path.join(ROOT, "packages")
    for dp, dns, fns in os.walk(base):
        dns[:] = [d for d in dns if d not in TS_SKIP_DIRS and not d.startswith("rust-")]
        for fn in fns:
            if fn.endswith(TS_EXT) and not fn.endswith(".d.ts"):
                q = os.path.join(dp, fn)
                idx[os.path.relpath(q, ROOT).replace(os.sep, "/")] = q
    return idx


def rust_files():
    for crate in CRATES:
        base = os.path.join(ROOT, "packages", crate)
        for dp, dns, fns in os.walk(base):
            dns[:] = [d for d in dns if d not in RS_SKIP_DIRS]
            for fn in fns:
                if fn.endswith(".rs"):
                    yield os.path.join(dp, fn)


def resolve(cited, idx):
    """TS index entries whose path ends with the cited suffix. A citation may
    name the package without its `src/` (`zqlite/table-source.ts`), so that
    form is tried second."""
    c = cited.lstrip("./")
    if c.startswith("packages/"):
        c = c[len("packages/"):]
    hits = [p for p in idx if p == "packages/" + c or p.endswith("/" + c)]
    if not hits and "/" in c:
        pkg, rest = c.split("/", 1)
        hits = [p for p in idx if p == f"packages/{pkg}/src/{rest}"]
    return hits


def camel(ident):
    """`should_yield` → `shouldYield` (a rust twin named in the comment)."""
    if "_" not in ident.strip("_"):
        return ident
    head, *tail = ident.split("_")
    return head + "".join(t[:1].upper() + t[1:] for t in tail)


def symbols_from(span):
    """Identifiers a backtick span names, member-last: `CVRStore.#rowCache`
    → ['rowCache', 'CVRStore'] (checked in that order); a snake_case rust
    name also yields its camelCase TS spelling."""
    ids = [i.lstrip("$") for i in IDENT.findall(span.replace("#", " "))]
    ids = [i for i in ids if i and not i.isdigit()]
    out = []
    for i in reversed(ids):
        for c in (i, camel(i)):
            if c not in out:
                out.append(c)
    return out


SNIPPET_SPLIT = re.compile(r"…|\.\.\.")
DQUOTE = re.compile(r'\\?"((?:[^"\\]|\\.){8,}?)\\?"')


def strip_comment(line):
    return re.sub(r"^\s*(//[/!]?|/\*\*?|\*)\s?", "", line)


def snippet(t):
    piece = SNIPPET_SPLIT.split(t)[0].replace("\\", "").strip(" `'\"")
    return "lit:" + piece if len(piece) >= 8 else None


STOP = {"with", "than", "then", "here", "runs", "plain", "after", "before", "where", "which", "while",
        "there", "their", "these", "those", "every", "other", "only", "must", "into", "from", "that",
        "this", "when", "each", "same", "both", "also", "rust", "Rust", "Port", "port", "twin"}


def strong(word):
    """An identifier-shaped anchor: #name, camelCase, PascalCase, CONSTANT_CASE,
    or a snake_case rust name — not a plain English word."""
    return bool(BARE.fullmatch(word) or "_" in word.strip("_")) and word.lstrip("#") not in NOT_SYMBOLS


def extract_symbol(lines, i, col):
    """Anchors for the citation at (line i, column col) as (strong, weak).

    Strong anchors decide STALE: identifier spans (`#name`, `Class.member`,
    `fn()`), identifier-shaped words inside a rust assertion string, and
    quoted literals (a TS message or code snippet, matched literally). Weak
    anchors are the plain words of a prose snippet; they can only turn a
    verdict into OK. A span that is itself a citation, a `.rs` path, or a
    wrapped fragment (starts with punctuation, contains an em-dash) is skipped.

    Inside a rust string the string's own words are the anchors, plus a
    double-quoted literal on the same or previous line (the expected TS
    message). Otherwise every backtick span on the comment line and the
    comment line before it (a span may wrap across the two, and the symbol
    often follows the citation: `(file.ts:12 passes `costModel`)`) plus
    double-quoted literals; an identifier span also yields the camelCase
    spelling of a snake_case rust name and the `xSchema` name of a valita
    twin. Falls back to the last bare identifier-shaped word before the
    citation."""
    line = lines[i]
    prev = lines[i - 1] if i > 0 else ""
    hard, soft = [], []
    def add(lst, x):
        if x and x not in lst:
            lst.append(x)
    def add_ident(x):
        add(hard, x)
        if not x.endswith("Schema") and not x.startswith("lit:"):
            add(hard, x + "Schema")
    quoted_before = line[:col].count('"') - line[:col].count('\\"')
    if quoted_before % 2 == 1:
        a = line.rfind('"', 0, col)
        b = line.find('"', col)
        text = CITE.sub(" ", line[a + 1 : b if b > 0 else None])
        for w in BARE.findall(text) + DOTTED.findall(text):
            if w.lstrip("#") not in NOT_SYMBOLS and w != "TS":
                for x in symbols_from(w):
                    add(hard, x)
        for q in DQUOTE.findall(prev + " " + line[:a] + " " + (line[b + 1 :] if b > 0 else "")):
            if not q.strip().endswith(".rs"):
                add(hard, snippet(q))
        return hard, soft
    if not COMMENT_LINE.match(prev):
        prev = ""
    prev2 = lines[i - 2] if i > 1 and prev and COMMENT_LINE.match(lines[i - 2]) else ""
    nxt = lines[i + 1] if i + 1 < len(lines) and COMMENT_LINE.match(lines[i + 1]) else ""
    prev, prev2, cur, nxt = strip_comment(prev), strip_comment(prev2), strip_comment(line), strip_comment(nxt)
    if cur.count("`") % 2 == 1:
        cur += " " + nxt  # a span opened on the citation line closes on the next
    text = prev2 + " " + prev + " " + cur
    # A span may have opened further up the comment block, so the pairing of
    # backticks in `text` is unknown: read it under both alignments and keep
    # the spans that are TIGHT (no whitespace against the backticks — the way
    # every real `symbol` is written); a misaligned span is a fragment of
    # prose that starts or ends with a space and drops out.
    spans = [t for t in BACKTICK.findall(text) + BACKTICK.findall("`" + text) if t and t == t.strip()]
    for t in spans:
        ts = t.strip()
        if CITE.search(t) or ts.endswith(".rs") or "/" in ts and ".rs" in ts:
            continue
        if not ts or ts[0] in ".,;:)—]}" or "—" in ts:
            continue  # a fragment of a span wrapped across lines
        if re.search(r"\s", ts) or ("(" in ts and ")" in ts and not re.fullmatch(r"[\w#.$]+\(\)", ts)):
            add(hard, snippet(t))
            for x in symbols_from(t):
                if strong(x):
                    add_ident(x)
                elif len(x) > 3 and x not in STOP:
                    add(soft, x)
            continue
        for x in symbols_from(t):
            if len(x) <= 1:
                continue
            if strong(x) or "." in ts or "#" in ts or "(" in ts or len(x) > 5:
                add_ident(x)
            elif x not in STOP:
                add(soft, x)
    for q in DQUOTE.findall(text):
        if not q.strip().endswith(".rs"):
            add(hard, snippet(q))
    if hard or soft:
        return hard, soft
    bare = [b for b in BARE.findall(line[:col]) if b.lstrip("#") not in NOT_SYMBOLS]
    if bare:
        return symbols_from(bare[-1]), []
    return [], []


_TS_CACHE = {}
def ts_lines(path):
    if path not in _TS_CACHE:
        with open(path, errors="replace") as f:
            _TS_CACHE[path] = f.read().split("\n")
    return _TS_CACHE[path]


def norm(text):
    """Whitespace/comment-marker/backtick-insensitive form for literal snippets
    (a quoted TS comment wraps across lines; TS quotes names in backticks;
    a call's arguments may sit on their own lines)."""
    t = re.sub(r"^\s*(//|\*)", " ", text, flags=re.M).replace("`", "")
    t = re.sub(r"\s+", " ", t)
    return re.sub(r"\s+\)", ")", re.sub(r"\(\s+", "(", t)).strip()


def has_symbol(text, symbols):
    hay = None
    for s in symbols:
        if s.startswith("lit:"):
            hay = norm(text) if hay is None else hay
            lit = norm(s[4:])
            words = lit.split(" ")
            # A rust assertion may append its own tail (`: boom`) to the TS
            # message, so a 4-word prefix of a longer literal also anchors.
            if lit in hay or (len(words) > 4 and " ".join(words[:4]) in hay):
                return True
        elif re.search(r"(?<![A-Za-z0-9_$])" + re.escape(s) + r"(?![A-Za-z0-9_$])", text, re.I):
            return True  # case-insensitive: `assert_ordering_includes_pk` names TS `assertOrderingIncludesPK`
    return False


def indent(line):
    return len(line) - len(line.lstrip(" "))


CLOSER_ONLY = re.compile(r"^[}\])]+[;,]?$")
TS_COMMENT = re.compile(r"^\s*(//|/\*|\*(\s|/|$))")  # `*#gen(` is a generator, not a comment


def enclosed_by(src, lo, symbols):
    """(`lo` is the 0-based index of the cited line.)
    True when the cited line sits inside a block whose header names the
    symbol — a line deep in `#runInLockForClient`'s body is correctly cited
    as `#runInLockForClient (view-syncer.ts:1243)`. Ancestor headers are the
    lines above with a smaller indent than everything between; a line that
    starts with `}` closes a sibling block (its opener is not an ancestor),
    one that starts with `)`/`]` continues a header opened at its own indent."""
    if lo >= len(src):
        return False  # cited past the end of the file: drifted for sure
    cur = None
    cont = None  # a `): T {` line closing a multi-line header above it
    for j in range(lo, -1, -1):
        line = src[j]
        if not line.strip() or TS_COMMENT.match(line):
            continue
        ind = indent(line)
        if cur is None:
            cur = ind
            continue
        head = line.lstrip()[:1]
        if head in ")]":
            if ind < cur:
                cur = ind + 1
                cont = j
            continue
        if ind >= cur:
            continue
        if head == "}":
            if CLOSER_ONLY.match(line.strip()):
                cur = ind  # a sibling block closed: its opener is not an ancestor
            else:
                cur = ind + 1  # `} else {` / `} finally {`: the opener above is
            continue
        # An ancestor header (with its parameter lines when it spans several).
        header = "\n".join(src[j : cont + 1]) if cont is not None and cont > j else line
        cont = None
        if has_symbol(header, symbols):
            return True
        cur = ind
        if cur == 0:
            return False
    return False


def in_window(path, lo, hi, symbols, window):
    src = ts_lines(path)
    a, b = max(0, lo - 1 - window), min(len(src), hi + window)
    if has_symbol("\n".join(src[a:b]), symbols):
        return True
    return enclosed_by(src, lo - 1, symbols)


def scan(window=WINDOW):
    idx = ts_index()
    rows = []  # (status, rs_rel, lineno, cited, symbols, detail)
    for rs in rust_files():
        with open(rs, errors="replace") as f:
            lines = f.read().split("\n")
        rel = os.path.relpath(rs, ROOT)
        for i, line in enumerate(lines):
            for m in CITE.finditer(line):
                cited = m.group("file")
                nums = [int(m.group("line"))] + [int(x) for x in re.findall(r"\d+", m.group("rest"))]
                lo, hi = min(nums), max(nums)
                hits = resolve(cited, idx)
                if not hits:
                    rows.append(("DEAD", rel, i + 1, f"{cited}:{m.group('line')}", [], "no such TS file"))
                    continue
                hard, soft = extract_symbol(lines, i, m.start())
                syms = hard + soft
                if not syms:
                    rows.append(("UNANCHORED", rel, i + 1, f"{cited}:{m.group('line')}", [], hits[0]))
                    continue
                ok = [h for h in hits if in_window(idx[h], lo, hi, syms, window)]
                if ok:
                    rows.append(("OK", rel, i + 1, f"{cited}:{m.group('line')}", syms, ok[0]))
                elif not hard:
                    rows.append(("UNANCHORED", rel, i + 1, f"{cited}:{m.group('line')}", syms, hits[0]))
                elif len(hits) > 1:
                    rows.append(("AMBIGUOUS", rel, i + 1, f"{cited}:{m.group('line')}", syms, ", ".join(hits)))
                else:
                    rows.append(("STALE", rel, i + 1, f"{cited}:{m.group('line')}", syms, hits[0]))
    return rows


def selftest():
    """Pins the extraction + matching rules on synthetic inputs (run with
    --selftest); each case is one rule the sweep needed."""
    fake = "/selftest/a.ts"
    ts = [""] * 120
    ts[1] = "export class Svc {"
    ts[2] = "  *#gen("
    ts[3] = "    a: number,"
    ts[4] = "  ): Stream<Node> {"
    ts[5] = "    const x = fooBar(a);"
    for k in range(6, 48):
        ts[k] = "    step();"
    ts[48] = "    if (a) {"
    ts[49] = "      deep();"
    ts[50] = "    } else {"
    ts[51] = "      other();"
    ts[52] = "    }"
    ts[53] = "  }"
    ts[54] = "  sibling() {"
    ts[55] = "    return 1;"
    ts[56] = "  }"
    ts[57] = "}"
    ts[60] = "export function assertOrderingIncludesPK() {}"
    ts[61] = "  `view-syncer closing connection with error: ${String(e)}`,"
    ts[62] = "  this.#pipelinesSynced = true;"
    ts[63] = "  // why do we not sort `desiredQueryIDs` here?"
    _TS_CACHE[fake] = ts
    def anchors(rs_lines, i):
        col = rs_lines[i].index("a.ts:")
        h, w = extract_symbol(rs_lines, i, col)
        return h, w, h + w
    # 1. identifier in the window / drifted away
    h, w, a = anchors(["/// Port of TS `fooBar` (a.ts:6)."], 0)
    assert "fooBar" in h and in_window(fake, 6, 6, a, WINDOW)
    assert not in_window(fake, 100, 100, a, WINDOW), "drifted citation must be STALE"
    # 2. a line deep in a generator method's body, past the window, is enclosed
    h, w, a = anchors(["/// TS `#gen` (a.ts:30)."], 0)
    assert in_window(fake, 30, 30, a, 10), "enclosing `*#gen(` header must count"
    h, w, a = anchors(["/// TS `sibling` (a.ts:30)."], 0)
    assert not in_window(fake, 30, 30, a, 10), "a sibling method is not an ancestor"
    h, w, a = anchors(["/// TS `#gen` (a.ts:56)."], 0)
    assert not in_window(fake, 56, 56, a, 1), "a line after the method's `}` is outside it"
    # 3. a span wrapped across two comment lines
    h, w, a = anchors(["/// after `this.#pipelinesSynced =", "/// true` (a.ts:63) runs"], 1)
    assert "pipelinesSynced" in a and in_window(fake, 63, 63, a, 0)
    # 4. an assertion string: its #name / camelCase words anchor
    h, w, a = anchors(['    "TS sets #pipelinesSynced = true after the pass (a.ts:63)"'], 0)
    assert "pipelinesSynced" in h and in_window(fake, 63, 63, a, 0)
    # 5. a quoted literal with a rust-only tail matches on its 4-word prefix
    h, w, a = anchors(['        logged.contains("view-syncer closing connection with error: boom"),', '        "TS logs it verbatim (a.ts:62); got: {logged}"'], 1)
    assert in_window(fake, 62, 62, a, 0)
    # 6. a snake_case rust name finds its camelCase TS twin, case-insensitively
    h, w, a = anchors(["//! `assert_ordering_includes_pk` (a.ts:61)"], 0)
    assert in_window(fake, 61, 61, a, 0)
    # 7. a quoted TS comment is matched backtick- and whitespace-insensitively
    h, w, a = anchors(['// (a.ts:64 "why do we not sort desiredQueryIDs here?")'], 0)
    assert in_window(fake, 64, 64, a, 0)
    # 8. a rust .rs path in quotes is not an anchor; a citation inside a string is not a token
    h, w, a = anchors(['    "TS builds it once (a.ts:5); rust-cvr/src/x.rs must too"'], 0)
    assert not any(x.endswith(".rs") or x == "ts" for x in a), a
    print("M16 selftest OK (8 rules)")


def main(argv):
    if "--selftest" in argv:
        selftest()
        return 0
    window = WINDOW
    if "--window" in argv:
        window = int(argv[argv.index("--window") + 1])
    rows = scan(window)
    counts = {}
    for r in rows:
        counts[r[0]] = counts.get(r[0], 0) + 1
    failing = [r for r in rows if r[0] in ("DEAD", "STALE")]
    if "--list" in argv:
        for status, rel, ln, cited, syms, detail in sorted(rows):
            if status == "OK":
                continue
            print(f"{status:<10} {rel}:{ln}  {cited}  {'/'.join(syms) or '-'}  → {detail}")
    print(
        f"M16 citation freshness: {len(rows)} citations, window ±{window}: "
        + ", ".join(f"{k}={counts.get(k, 0)}" for k in ("OK", "STALE", "DEAD", "AMBIGUOUS", "UNANCHORED"))
    )
    n = len(failing)
    if n > BASELINE:
        print(f"FAIL: {n} stale/dead citations > BASELINE {BASELINE} (run with --list; re-cite the TS line or fix the symbol)")
        return 1
    if n < BASELINE:
        print(f"NOTE: {n} < BASELINE {BASELINE} — lower BASELINE in parity/citation_freshness.py")
    print("OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
