# Streaming correctness gate — the standing adversarial harness

Status: SCOPE (2026-06-25). Purpose: catch multi-partition / large-state / crash correctness bugs
**proactively** (a standing gate), instead of reactively (this session found the continuous-EO dup,
and F5.3 compaction-multipart + EKS-scale remain — all the SAME class: passes trivially, breaks at
scale/skew). Flink's edge is decades of this hammering; this gate is how Vajra leapfrogs with the
no-JVM/Arrow architecture + harder-on-ourselves-than-reality discipline.

## Invariant contract (every cell asserts these vs a batch ground-truth on the same input)
1. **Completeness** — output distinct groups == input distinct groups (no silent loss; caught the 64k cap).
2. **No dup / no partial split** — each `(window,key)` appears once with the correct FINAL count
   (caught the per-partition-watermark `3+7=10` splits). Check: per-epoch parquet inspector — no
   `(window,key)` in >1 epoch or >1 row.
3. **Exactly-once across crash** — hard `kill -9` mid-run + restart → (1)&(2) still hold.
4. **Bounded memory** — operator resident state (`F5_PEAK` peak_pending) ≈ budget, not O(N).

## Adversarial dimensions (the conditions that actually broke us)
| dim | values | why |
|---|---|---|
| cardinality `N` | 100k, 1M, 10M | large keyed state; found 64k cap |
| source partitions | 1, 4, 8 | multi-partition keyed routing/watermark |
| ordering | per-partition-ordered, **SCRAMBLED across partitions** | the skew that triggers premature close (EKS missed it) |
| trigger | availableNow (micro-batch), continuous | micro-batch is the proven path; continuous is the gap |
| crash | none, hard kill-9 mid-run | EO recovery |
| state budget | large (in-RAM), tiny (force spill) | spill + incremental-ckpt path |

Not full cross-product — a curated high-signal cell set (below).

## Cells (curated) + expected status
Green = must pass (blocking). XFAIL = known gap, tracked to a gap-register entry (gate stays green on
what should pass; XFAIL flips to a FAIL if it *starts* passing = the fix landed → promote to green).
| # | cell | status |
|---|---|---|
| C1 | availableNow, 1M keys, large budget — completeness+nodup | GREEN |
| C2 | availableNow, 10M keys, tiny budget (spill) — completeness+nodup+bounded peak | GREEN (F5.4 proof) |
| C3 | availableNow, 8 partitions, SCRAMBLED — completeness+nodup | GREEN expected (micro-batch reads all before close) — **verify** |
| C4 | availableNow + crash, 1M keys — EO | GREEN (f3c-class) |
| C5 | continuous, 1 partition — completeness+nodup | GREEN (inc_ckpt_gate PARTS=1 passed) |
| C6 | continuous, 4 partitions, SCRAMBLED — nodup | **XFAIL** → per-partition-watermark gap (STREAMING_ARCHITECTURE gap register) |
| C7 | continuous + crash, 4 partitions — EO | **XFAIL** → same gap |

## Reuse (don't rebuild)
- `inc_ckpt_gate.sh` (continuous, INC/RUN/NOCRASH/PARTS toggles) + its per-epoch `(window,key)` inspector → C5/C6/C7.
- `state_scale_stress.py` (availableNow, large N, batch-vs-stream) → C1/C3.
- `f5_validate.sh` (`F5_PEAK` peak_pending, spill) → C2.
- `f3c_stateful_crash.sh` (continuous stateful EO across kill-9) → C4/C7.
- A SCRAMBLED producer (round-robin all windows at once across partitions) — already how `inc_ckpt_gate.py` produces; factor out as the skew generator.

## Deliverable
`scripts/correctness_gate.sh`: runs the cells, asserts the contract per cell, prints a PASS/XFAIL/FAIL
matrix, exits 0 iff all GREEN cells pass AND no XFAIL unexpectedly passes. Needs docker `vajra_kafka` +
`target/debug/vajra` + `.venvs/smoke`. CI: GREEN micro-batch cells (C1-C4) blocking + fast; continuous
cells (C5-C7) tracked (XFAIL allowed) until the per-partition fix lands.

## Why this is the highest-leverage prod-grade step
- Turns "passes my demo" into "survives adversarial scale/skew/crash" = the actual prod bar.
- Makes the EKS-vs-Flink head-to-head TRUSTWORTHY (same adversarial workload, not the lucky ordered case).
- Reduces reactive spelunking (bugs caught by the gate, not mid-feature) → directly addresses token waste.
- XFAIL register = honest, living view of exactly which Flink-class guarantees hold vs are open.
