"""
Compare a Zelox ClickBench result against LakeSail's published numbers.

Both files are ClickBench-format JSON ([[r1,r2,r3], ...] under "result", or a
bare list). Reports per-query hot (best-of-3) times, the Zelox/LakeSail ratio,
totals, and a verdict — since Zelox shares sail's DataFusion core, per-query
times within ~±25% (and total within ~±15%) means "matching / correctly
implemented"; large systematic divergence flags a fork regression to investigate.

Usage:
    python benchmarks/clickbench/compare.py results/zelox_c6a.4xlarge.json \
                                            results/lakesail_c6a.4xlarge.json
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def load(path: str) -> list[list[float]]:
    obj = json.loads(Path(path).read_text())
    return obj["result"] if isinstance(obj, dict) else obj


def hot(triple: list[float]) -> float | None:
    vals = [t for t in triple if isinstance(t, (int, float))]
    return min(vals) if vals else None


def main() -> int:
    zelox = load(sys.argv[1])
    lake = load(sys.argv[2])
    n = min(len(zelox), len(lake))

    print(f"{'Q':>3}  {'Zelox':>9}  {'LakeSail':>9}  {'V/L':>6}  flag")
    print("-" * 42)
    vt = lt = 0.0
    ratios: list[float] = []
    for i in range(n):
        v, l = hot(zelox[i]), hot(lake[i])
        if v is None or l is None:
            print(f"{i + 1:>3}  {'-' if v is None else v:>9}  "
                  f"{'-' if l is None else l:>9}  {'?':>6}  missing")
            continue
        r = v / l if l else float("inf")
        ratios.append(r)
        vt += v
        lt += l
        flag = "" if 0.75 <= r <= 1.25 else ("SLOW" if r > 1.25 else "fast")
        print(f"{i + 1:>3}  {v:>9.3f}  {l:>9.3f}  {r:>6.2f}  {flag}")

    ratios.sort()
    med = ratios[len(ratios) // 2] if ratios else float("nan")
    print("-" * 42)
    print(f"TOTAL hot (best-of-3):  Zelox {vt:.2f}s   LakeSail {lt:.2f}s   "
          f"ratio {vt / lt:.2f}x")
    print(f"Median per-query Zelox/LakeSail ratio: {med:.2f}x")
    verdict = ("MATCHING (shared core confirmed)"
               if 0.85 <= vt / lt <= 1.15 else
               "DIVERGENT — investigate fork regression")
    print(f"Verdict: {verdict}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
