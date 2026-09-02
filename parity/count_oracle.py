#!/usr/bin/env python3
"""
M1 — differential call-count oracle (parity/ layer).

Coverage tells you what code RAN, not whether behavior matched. A perfectly-ported
FUNCTION can still be CALLED the wrong number of times if its guard was dropped when
the surrounding control structure was re-architected — exactly the 2026-09-02
`hydrate_unchanged_queries` bug (ran per-sync instead of once/init; a big CG
re-materialized 15x). That divergence is invisible to symbol/body/emission checks
and to client-facing latency until it becomes a tail.

This oracle compares per-op INVOCATION COUNTS between the rust candidate and the TS
reference for the SAME workload (TS is the oracle — it must have driven the corpus
first). Two signals:

  1. Differential (external, from the ART run reports): per-query `queries_driven`
     and top-level counters (pokes, dedup_puts). Ratios far from 1.0 flag a
     structural divergence in WHAT ran. Reported (not hard-failed) because invalid
     synthesized args make some queries error asymmetrically — a known confounder.

  2. Amplification invariant (internal, from rust prometheus): rust does
     `zero_sync_hydrations_total` hydrations for `sum(queries_driven)` distinct
     query drives. Healthy ≈ 1.0 (each driven query hydrates ~once). >> 1.0 means an
     internal op is re-running per sync — the re-hydrate class. This is the HARD gate
     and is exactly what would have caught the whale bug (amp was far >1; post-fix
     1.19x).

Usage (run in the ART sandbox after a dual replay):
  python3 count_oracle.py run-<t>-rust.json run-<t>-ts.json \
      --rust-metrics <(curl -s localhost:13200/metrics) \
      --amp-threshold 2.0 --tolerance 3.0 --min-count 10
Exit non-zero iff the amplification invariant is breached.
"""
from __future__ import annotations
import argparse
import json
import re
import sys


def load(path):
    with open(path) as fh:
        return json.load(fh)


def read_hydrations(metrics_path: str | None) -> float | None:
    if not metrics_path:
        return None
    try:
        with open(metrics_path) as fh:
            for line in fh:
                m = re.match(r"^zero_sync_hydrations_total\s+([\d.eE+-]+)", line)
                if m:
                    return float(m.group(1))
    except FileNotFoundError:
        return None
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("rust_run")
    ap.add_argument("ts_run")
    ap.add_argument("--rust-metrics", help="prometheus text with zero_sync_hydrations_total")
    ap.add_argument("--tolerance", type=float, default=3.0, help="per-query rust/ts ratio band")
    ap.add_argument("--min-count", type=int, default=10, help="min drives in both to compare")
    ap.add_argument("--amp-threshold", type=float, default=2.0, help="hard fail if hydrations/drives exceeds")
    a = ap.parse_args()

    r, t = load(a.rust_run), load(a.ts_run)
    rq = r.get("queries_driven", {}) or {}
    tq = t.get("queries_driven", {}) or {}

    # --- signal 1: per-query differential (reported) ---------------------------
    print("== M1 differential: per-query drive counts (rust vs TS; TS = oracle) ==")
    outliers = []
    for q in sorted(set(rq) | set(tq)):
        rc, tc = rq.get(q, 0), tq.get(q, 0)
        if max(rc, tc) < a.min_count:
            continue
        ratio = (rc / tc) if tc else float("inf")
        if ratio > a.tolerance or ratio < 1.0 / a.tolerance:
            outliers.append((ratio, q, rc, tc))
    if outliers:
        outliers.sort(key=lambda x: abs(x[0] - 1.0), reverse=True)
        print(f"  {len(outliers)} query(ies) outside [{1/a.tolerance:.2f}, {a.tolerance:.2f}]x (confounder: asymmetric arg errors):")
        for ratio, q, rc, tc in outliers[:15]:
            print(f"    {ratio:6.2f}x  {q[:40]:40} rust={rc} ts={tc}")
    else:
        print(f"  all shared queries within [{1/a.tolerance:.2f}, {a.tolerance:.2f}]x — no structural drive-count divergence")

    ctr_r, ctr_t = r.get("counters", {}), t.get("counters", {})
    for k in ("pokes", "dedup_puts", "puts_sent"):
        rc, tc = ctr_r.get(k, 0), ctr_t.get(k, 0)
        ratio = (rc / tc) if tc else float("inf")
        print(f"  counter {k}: rust={rc} ts={tc} ({ratio:.2f}x)")

    # --- signal 2: amplification invariant (HARD gate) -------------------------
    print("\n== M1 amplification invariant: rust internal ops per driven query ==")
    hyd = read_hydrations(a.rust_metrics)
    drives = sum(rq.values())
    rc = 0
    if hyd is None:
        print("  rust hydrations metric not provided (--rust-metrics) — invariant SKIPPED")
    elif drives == 0:
        print("  no query drives — invariant SKIPPED")
    else:
        amp = hyd / drives
        status = "OK" if amp <= a.amp_threshold else "FAIL"
        print(f"  hydrations_total={hyd:.0f}  drives={drives}  amplification={amp:.2f}x  "
              f"(threshold {a.amp_threshold:.1f}x) -> {status}")
        if amp > a.amp_threshold:
            print("  FAIL: an internal op re-runs per sync (re-hydrate class). "
                  "Check hydrate_unchanged_queries / transform gating vs TS #pipelinesSynced.")
            rc = 1
    print("\nM1 count oracle:", "PASS" if rc == 0 else "FAIL")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
