#!/usr/bin/env python3
r"""
M4 — profile differential (parity/ layer; the "did it run, and how often?" axis).

L1/L2 compare CODE. M1 compares call COUNTS at the API boundary. Neither sees a
port that runs the right function far too MANY times — which is exactly the
2026-09-02 divergence: `hydrate_unchanged_queries` was a faithful port of
`#hydrateUnchangedQueries`, called from the wrong place, and the only signal was
that it dominated the CPU profile while its TS twin barely appeared.

So: profile both implementations under the SAME workload and compare where the
time goes. A ported function whose share of self-time is wildly larger in rust
than in TS is a call-site/looping divergence until proven otherwise.

Inputs (both optional-but-one-required, see --help):
  --rust  a folded-stack file (`stackcollapse`-format: `frame;frame;frame <count>`)
          or the SVG produced by `/debug/pprof/flamegraph?seconds=N`
  --ts    a folded-stack file from `node --prof` + `--prof-process`, `0x`, or
          clinic flame (all can emit folded stacks)

Output: the top self-time symbols on each side, joined on a normalized name, with
the rust/TS share ratio. `--threshold` sets the ratio that fails the check.

Normalization maps rust snake_case to TS camelCase (`hydrate_unchanged_queries`
<-> `#hydrateUnchangedQueries`) so ported twins line up; unmatched frames are
reported separately rather than silently dropped.

STATUS (2026-09-02): the rust side is wired and validated against a real capture
from the ART box. The TS baseline has NOT been captured yet — running this with
only `--rust` prints the rust profile and exits 0 (informational). It becomes a
GATE once a TS folded-stack baseline is checked in. Do not read a passing
run without `--ts` as evidence of parity.
"""
from __future__ import annotations
import argparse
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict


def load_folded(path: str) -> dict[str, float]:
    """`a;b;c <count>` -> self-time by leaf frame."""
    self_time: dict[str, float] = defaultdict(float)
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            m = re.match(r"^(.*)\s+(\d+(?:\.\d+)?)$", line)
            if not m:
                continue
            stack, count = m.group(1), float(m.group(2))
            leaf = stack.split(";")[-1]
            self_time[leaf] += count
    return dict(self_time)


def load_pprof_svg(path: str) -> dict[str, float]:
    """Self-time by frame from a pprof/flamegraph SVG (leaf = widest-depth frame).

    A flamegraph SVG encodes each frame as a `<g>` with a `<title>` naming the
    symbol and its sample count, plus an `x`/`width` rect. Self time is a frame's
    width minus the sum of its children's widths; children are the frames one row
    below whose x-range falls inside it.
    """
    ns = {"svg": "http://www.w3.org/2000/svg"}
    fg = "{http://github.com/jonhoo/inferno}"
    tree = ET.parse(path)
    frames: list[tuple[float, float, float, str]] = []  # (y, x, width, name)

    def num(v: str | None) -> float | None:
        if v is None:
            return None
        try:
            # inferno writes x/width as percentages ("12.3456%").
            return float(v.rstrip("%"))
        except ValueError:
            return None

    for g in tree.iter("{http://www.w3.org/2000/svg}g"):
        title = g.find("svg:title", ns)
        rect = g.find("svg:rect", ns)
        if title is None or rect is None or not title.text:
            continue
        name = title.text.split(" (")[0].strip()
        y = num(rect.get("y"))
        # inferno carries exact SAMPLE COUNTS in `fg:x` / `fg:w`; prefer them over
        # the rendered percentages (no rounding, and they compose exactly).
        x = num(rect.get(fg + "x"))
        w = num(rect.get(fg + "w"))
        if x is None or w is None:
            x = num(rect.get("x"))
            w = num(rect.get("width"))
        if x is None or w is None or y is None:
            continue
        # `all` is the synthetic root inferno adds; its "self time" is just the
        # unattributed remainder and would top every listing.
        if name == "all":
            continue
        frames.append((y, x, w, name))
    if not frames:
        return {}
    rows = sorted({f[0] for f in frames})
    row_index = {y: i for i, y in enumerate(rows)}
    by_row: dict[int, list[tuple[float, float, str]]] = defaultdict(list)
    for y, x, w, name in frames:
        by_row[row_index[y]].append((x, w, name))
    self_time: dict[str, float] = defaultdict(float)
    for r, items in by_row.items():
        children = by_row.get(r + 1, [])
        for x, w, name in items:
            covered = sum(cw for cx, cw, _ in children if cx >= x and cx + cw <= x + w)
            self_time[name] += max(0.0, w - covered)
    return dict(self_time)


def load(path: str) -> dict[str, float]:
    return load_pprof_svg(path) if path.endswith(".svg") else load_folded(path)


def normalize(sym: str) -> str:
    """Reduce a rust or TS frame to a comparable key.

    `rust_syncer::services::view_syncer::view_syncer::ViewSyncerService::hydrate_unchanged_queries`
    and `ViewSyncerService.#hydrateUnchangedQueries` both -> `hydrateunchangedqueries`.
    """
    sym = sym.split("(")[0].strip()
    sym = re.sub(r"::h[0-9a-f]{8,}$", "", sym)          # rust symbol hash
    leaf = re.split(r"::|\.", sym)[-1]
    leaf = leaf.lstrip("#_")
    return re.sub(r"[^a-z0-9]", "", leaf.lower())


def top(profile: dict[str, float], n: int) -> list[tuple[str, float, float]]:
    total = sum(profile.values()) or 1.0
    rows = sorted(profile.items(), key=lambda kv: -kv[1])[:n]
    return [(name, val, 100.0 * val / total) for name, val in rows]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rust", required=True, help="rust folded stacks or pprof SVG")
    ap.add_argument("--ts", help="TS folded stacks (node --prof / 0x / clinic)")
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--threshold", type=float, default=4.0,
                    help="fail when a ported twin's rust self-time share exceeds "
                         "TS's by this factor (default 4x)")
    ap.add_argument("--min-share", type=float, default=1.0,
                    help="ignore frames below this %% of self time on both sides")
    args = ap.parse_args()

    rust = load(args.rust)
    if not rust:
        print(f"M4: no frames parsed from {args.rust}", file=sys.stderr)
        return 2

    print(f"== M4 profile differential ==\n\nrust top {args.top} self-time frames "
          f"({args.rust}):")
    for name, val, pct in top(rust, args.top):
        print(f"  {pct:6.2f}%  {val:12.1f}  {name}")

    if not args.ts:
        print("\nNo --ts baseline given: printed the rust profile only. This is "
              "INFORMATIONAL, not a parity verdict — capture a TS folded-stack "
              "baseline under the same workload to turn M4 into a gate.")
        return 0

    ts = load(args.ts)
    if not ts:
        print(f"M4: no frames parsed from {args.ts}", file=sys.stderr)
        return 2

    rust_total = sum(rust.values()) or 1.0
    ts_total = sum(ts.values()) or 1.0
    rust_by_key: dict[str, float] = defaultdict(float)
    ts_by_key: dict[str, float] = defaultdict(float)
    label: dict[str, str] = {}
    for name, val in rust.items():
        k = normalize(name)
        rust_by_key[k] += 100.0 * val / rust_total
        label.setdefault(k, name)
    for name, val in ts.items():
        ts_by_key[normalize(name)] += 100.0 * val / ts_total

    print("\nported twins by self-time share (rust vs TS):")
    rc = 0
    matched = 0
    for k, r_pct in sorted(rust_by_key.items(), key=lambda kv: -kv[1]):
        if k not in ts_by_key:
            continue
        matched += 1
        t_pct = ts_by_key[k]
        if r_pct < args.min_share and t_pct < args.min_share:
            continue
        ratio = r_pct / t_pct if t_pct > 0 else float("inf")
        bad = ratio >= args.threshold
        if bad:
            rc = 1
        print(f"  [{'FAIL' if bad else 'ok  '}] {r_pct:6.2f}% rust vs {t_pct:6.2f}% TS "
              f"({ratio:5.1f}x)  {label[k]}")
        if bad:
            print("           A ported twin burning a far larger share of CPU than its TS "
                  "original is a call-site / loop-count divergence until proven otherwise.")
    if matched == 0:
        print("  (no frames matched across the two profiles — check symbolization; "
              "an unsymbolized rust profile cannot be compared)")
        rc = 1

    unmatched = [(label[k], p) for k, p in rust_by_key.items()
                 if k not in ts_by_key and p >= args.min_share]
    if unmatched:
        print("\nrust frames with no TS twin (>= min-share) — inventions or "
              "unsymbolized, review:")
        for name, pct in sorted(unmatched, key=lambda kv: -kv[1])[:15]:
            print(f"  {pct:6.2f}%  {name}")

    print("\nM4 profile differential:", "PASS" if rc == 0 else "FAIL")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
