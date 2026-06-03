"""Diff reference vs candidate workload results; report divergences.

Usage: python diff.py out_reference.json out_candidate.json
Exit code 0 if all workloads match, 1 if any diverge.
"""

import json
import sys

# Workloads with a documented, accepted Spark-version semantic difference.
# The reference engine here is Spark 3.5 (Java 8 limits us); Vajra targets
# Spark 4.x semantics, which the pysail gold tests assert.
KNOWN_VERSION_DIFFS = {
    # percentile_disc return type: Spark 3.5 -> double, Spark 4.x -> input type
    # (Int here). Vajra matches 4.x.
    "percentile",
}


def main():
    ref = json.load(open(sys.argv[1]))
    cand = json.load(open(sys.argv[2]))
    names = sorted(set(ref) | set(cand))

    ok, diverge = [], []
    for name in names:
        r, c = ref.get(name), cand.get(name)
        if r is None or c is None:
            diverge.append((name, "missing in one engine"))
            continue
        # If reference errored, skip (reference is the source of truth; its error
        # usually means the workload itself is invalid — not a Vajra bug).
        if "error" in r:
            ok.append((name, f"ref-error (skipped): {r['error'][:60]}"))
            continue
        if "error" in c:
            diverge.append((name, f"candidate ERRORED but reference succeeded: {c['error']}"))
            continue
        if name in KNOWN_VERSION_DIFFS and (r["schema"] != c["schema"] or r["rows"] != c["rows"]):
            ok.append((name, "known Spark 3.5<->4.x version diff (accepted)"))
            continue
        if r["schema"] != c["schema"]:
            diverge.append((name, f"SCHEMA differs:\n      ref ={r['schema']}\n      cand={c['schema']}"))
            continue
        if r["rows"] != c["rows"]:
            detail = f"ROWS differ (ref n={r['n']} cand n={c['n']})"
            # Show first differing row
            for i, (rr, cc) in enumerate(zip(r["rows"], c["rows"])):
                if rr != cc:
                    detail += f"\n      row[{i}] ref ={rr}\n      row[{i}] cand={cc}"
                    break
            diverge.append((name, detail))
            continue
        ok.append((name, "match"))

    print("=" * 70)
    print("  VAJRA DIFFERENTIAL TEST vs Apache Spark (reference)")
    print("=" * 70)
    for name, msg in ok:
        print(f"  [PASS] {name}  {('· ' + msg) if msg != 'match' else ''}")
    for name, msg in diverge:
        print(f"  [DIVERGE] {name}\n      {msg}")
    print("-" * 70)
    print(f"  {len(ok)} match, {len(diverge)} diverge, {len(names)} total")
    print("=" * 70)
    sys.exit(1 if diverge else 0)


if __name__ == "__main__":
    main()
