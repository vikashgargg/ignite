# Continuous multi-partition stateful exactly-once — the Flink-class fix

Branch: `streaming/continuous-stateful-eo` · Status: DESIGN (diagnosed 2026-07-02) · Owner: streaming
Gate cells this closes: **C6** (continuous 4-partition, no-dup) + **C7** (continuous 4-partition + crash, EO)
in `scripts/correctness_gate.sh` (both XFAIL today → target GREEN).

## 1. Problem (measured, not assumed)

The standing correctness gate is GREEN, but two cells are XFAIL: multi-partition **continuous**
(`Trigger.Continuous` / realtime) **stateful** windowed exactly-once. Direct diagnosis
(`INC=0 NOCRASH=1 PARTS=4 N=300 inc_ckpt_gate.sh`, continuous windowed COUNT):

```
CHECK rows=1330 expected=1800 all_counts_10=False no_dup=False
DUP (win=03:43:30, k=95)  counts=[4, 6]  sum=10
DUP (win=03:43:50, k=298) counts=[2, 8]  sum=10
```

**Signature = PARTIAL-COUNT SPLITS** (each duplicate group sums to the correct 10), not full
duplicates and not epoch-commit dups. This is a **watermark race**: a window closed early, emitted a
partial count, then the remaining events for that `(window, key)` arrived and produced a second row.
Only ~2 groups remain (down from ~1194 pre-per-partition-WM), i.e. this is the **residual** race.

## 2. Root cause (grounded in code + KB)

- The realtime/continuous Kafka source is pinned to **`parallelism = 1`** (`kafka/reader.rs`) because
  its exactly-once commit is single-coordinator (one `realtime/committed` record, per-epoch offset
  staging). One instance reads **all N partitions interleaved**.
- `WatermarkExec` per-partition MIN + `withIdleness` (REFERENCES §2; `streaming/watermark.rs`) largely
  fixes the race, BUT with a single interleaved reader a partition can appear **idle because the reader
  is busy with other partitions** (not because it has no data). The idleness timeout then **excludes it
  from the MIN → the watermark advances → the window closes → that partition's events arrive late → a
  partial-count split**. This false-idle is *inherent to single-instance interleaved reading* — grace
  tuning reduces but cannot eliminate it (a smaller timeout → more false-idle; a larger one → the 3h
  hang risk documented in `streaming-per-partition-watermark.md`).

## 3. The fix — parallelism=N realtime source (Flink-class, structural)

Per **REFERENCES §2 "Phase B synthesis"** and Flink's own model (per-partition parallel ingest, FLIP-27):
run the realtime source at **`parallelism = N` (one reader per Kafka partition)**, reusing the proven
**bounded** read path. Then:

- Each reader consumes **exactly one partition in offset order ⇒ its event-time is monotone** ⇒ a clean
  per-reader watermark with **no idleness heuristic needed** (a reader is only idle if its partition is
  truly empty). This **removes the per-partition-WM workaround and the false-idle race** at the source.
- `StreamExchangeExec` already does keyed shuffle + **watermark MIN-merge** across instances; the
  operator watermark = MIN over readers = exact (a window closes only when *every* partition passed it).
- `StreamBarrierAlignExec` already does the N→1 Chandy-Lamport barrier alignment for checkpoints.

The hard part (why parallelism was pinned to 1) = **exactly-once commit with N instances**:
- **Per-instance epoch staging**: each reader stages its consumed offsets per epoch (generalize the
  single `write_staged_epoch_offsets`).
- **Atomic union commit**: on the aligned epoch barrier, commit the union of all instances' staged
  offsets + the sink outputs as ONE atomic step (generalize the single `realtime/committed`), so restore
  replays from exactly the committed union. Chandy-Lamport aligned snapshot = exactly-once (REFERENCES §
  checkpoints).

This is **Flink-class or better**: like Flink's per-partition source + aligned checkpoint, but on
immutable Arrow state chunks (our F5/inc-ckpt substrate) — O(delta) checkpoints, no RocksDB.

## 4. Plan (SDLC, incremental, each step gated)

1. **T-EO-1** — realtime source `parallelism = N`: build N single-partition readers (reuse bounded
   builders); wire through the planner/rewriter; keep single-instance as fallback (env/flag) to isolate
   regressions. Gate: C5 (1-part) stays green; watermark is monotone per reader (no idleness needed).
2. **T-EO-2** — per-instance epoch offset staging (each reader stages its own offsets/epoch).
3. **T-EO-3** — atomic union commit on the aligned barrier (all instances' offsets + sink outputs commit
   as one); restore replays the union. Gate: **C7** continuous 4-part + crash → EO PASS.
4. **T-EO-4** — remove/neutralize the single-instance per-partition-WM workaround on the realtime path
   (kept for non-parallel sources). Gate: **C6** continuous 4-part → 0 dups (exact).
5. **T-EO-5** — codec round-trip for any new physical-plan fields (`sail-execution/src/codec.rs`).
6. **T-EO-6** — scale/skew validation: C6/C7 at PARTS=8 + scrambled order; then the honest **EKS
   multi-node** run (the KB notes full exact-zero historically needed EKS) before claiming Flink-class.

## 5. Done-criteria (robust the claim only when these pass)

- `correctness_gate.sh`: **C6 + C7 flip XFAIL → GREEN** (0 dups, EO across crash), and they are promoted
  from XFAIL to GREEN in the gate (so a future regression FAILs).
- No regression: C1/C2/C4/C5 stay green; batch + micro-batch EO unchanged.
- Then, and only then, update README/STATUS to claim **multi-partition continuous stateful exactly-once
  (Flink-class realtime)** — with the measured gate + EKS evidence.

## 5b. Progress log (measured)

- **T-EO-1 DONE + validated (2026-07-02).** Realtime `resolve()` now applies the FLIP-27 per-instance
  filter (`g % parallelism == inst`). Before: `VAJRA_RT_MULTI` PARTS=4 gave `counts=[12,28] sum=40`
  (4× over-read, 562 dup groups). After: `counts=[10,10] sum=20` — **over-read eliminated** (per-instance
  counts correct = 10). Committed.
- **Exposed next layer:** multi-instance NOCRASH now shows **FULL duplicates** (`[10,10]`, ~300 groups) =
  the sink/epoch **commit coordination across N instances** is missing. This is T-EO-2/3 (per-instance
  epoch staging + atomic union commit): each of the N instances/sink-tasks emits its window output, and
  without a coordinated per-epoch union commit the same `(window,key)` is written by more than one epoch
  flush / sink task. Confirms the design — multi-instance realtime is correct ONLY with the union commit.
- **Single-instance default path** (no `VAJRA_RT_MULTI`) remains the closest today (~2 partial-count
  split dups = residual watermark race), but has the inherent interleaved-read false-idle limitation.
- **Decision:** the robust Flink-class path is T-EO-2/3 (multi-instance union commit), a real F2/F3
  effort. Continue incrementally, each step gated; do not claim exact-zero until C6+C7 are GREEN.

## 6. Honesty / risk

The residual has historically resisted a single-instance patch and "full exact-zero" was expected to
need EKS (MEMORY). Parallelism=N + multi-instance EO commit is a **real architectural change** (F2/F3
territory), done incrementally behind a flag with per-step gates. If a step doesn't reach exact-zero
locally, we say so and take it to EKS rather than claim prematurely.
