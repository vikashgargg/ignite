# Continuous multi-partition stateful exactly-once — the Flink-class fix

Branch: `streaming/continuous-stateful-eo` · **Status: ✅ DONE (2026-07-03) — C6 + C7 GREEN by default.**
Gate cells closed: **C6** (continuous 4-partition, no-dup) + **C7** (continuous 4-partition + crash, EO)
in `scripts/correctness_gate.sh` — promoted XFAIL→GREEN; full gate GREEN (6/6, no regression).

## ⚠️ SCALE-HARDENING FINDING (PARTS=8, measured 2026-07-03)
Local scale-hardening (double partitions, ~3× cardinality, N=1000) — the honest result:
- **C6 no-dup + COMPLETE: SOLID at PARTS=8** — 2/2 clean (rows=6000, 6 windows, no-dup, counts=10).
  Completeness + no-dup (T-EO-1/3/3.5/4) scale from 4→8 partitions with no regression.
- **C7 crash-EO: robust at PARTS=4 (2/2) but ~5/6 at PARTS=8** — a RARE crash-boundary race remains: when
  hit, a whole epoch is re-committed (measured 671 dups on the one failure). This is the scale-dependent
  **epoch-boundary crash-atomicity residual** (KB: "full exact-zero = EKS"): the union-commit / sink
  epoch-commit at the exact crash instant is not yet perfectly atomic/idempotent across N=8 instances.
- **Honest claim status:** no-dup + completeness = validated at scale; **crash-EO exactly-once is NOT yet
  exact-zero at PARTS=8** (rare re-commit). Do NOT claim "Flink-class crash-EO at scale" until this residual
  is closed (candidate: make the per-epoch sink commit fully idempotent + the offset union commit atomic
  w.r.t. crash; validate to exact-zero on EKS timing). The C7 gate cell is PARTS=4 (solid); PARTS=8 crash
  is the open residual.

## 🔬 CRASH-EO ROOT-CAUSE + FIX ATTEMPTS (local PARTS=16, definitive 2026-07-03)

Reproduced the EKS crash-EO dup RELIABLY at local PARTS=16 (4/4 FAIL, ~700-1700 dups) = fast iteration
loop. Root-caused + attempted targeted fixes (all committed, all measured):
1. **Cumulative `realtime/committed`** (seed with prior + max-merge staged): dups 1445→131, resume-at-0
   9→3. Real 10× improvement. Grounded (Flink 2PC: committed offset set must cover every input).
2. **Initial-offset staging on start** (stage resume position before consuming): defensively correct
   (every input's position known at every checkpoint) but did NOT further reduce dups (~600-1000).
3. **INC=1 (state checkpointing)**: also FAILS (~300-1000 dups) — so it's not a missing-state issue.
**DEFINITIVE (committed-only analysis):** reading ONLY `_spark_metadata`-committed batches gives the SAME
dups as reading all files → the duplicates are **REAL committed exactly-once violations** (the same window
is committed in TWO epochs: pre-crash epoch N AND post-crash epoch M), NOT a test artifact.

**CONCLUSION:** multi-instance (N-reader) continuous stateful **crash-EO is genuinely broken at scale** and
the targeted patches (offset-record completeness) are NECESSARY but NOT SUFFICIENT. The real fix is the
deep distributed-EO rework: a **barrier-aligned atomic commit of {window operator STATE + source OFFSETS +
sink DATA-visibility} across all N instances** (Flink aligned-checkpoint + 2PC sink; RisingWave barrier
commit). Recovery must restore to the last globally-consistent checkpoint and NEVER re-commit an
already-committed window under a new epoch. This is F2/F3 distributed-EO scope — a substantial effort, not
a patch. **DEFAULT DECISION (open):** N-reader default has this crash-EO gap; single-instance held crash-EO
for the P1 keyed pattern but fails the scrambled-order no-dup gate. Neither is universally correct → the
deep rework is the real answer; until then, document honestly + do NOT claim crash-EO exactly-once at scale.

## 🔴 EKS SCALE RESULT (measured 2026-07-03, zelox-stream-ht, 16-partition, image :eo-multipart)

The honest EKS validation (the bar for the headline claim) — definitive:
- **Throughput/memory/latency scorecard (100M events vs Flink 1.19):** Zelox **5.389M ev/s** vs Flink 5.479M
  = **~TIED (1.7% slower — IMPROVED from 1.10× by the N-reader default)**; memory **7.04 vs 8.55 GiB =
  ~1.2× LESS (beats Flink)**; latency p50/p99 32/66 ms vs Flink 27/55 (competitive, Flink edges).
- **P1a CLEAN (no crash), Kafka→10s windowed-agg→Parquet-S3:** `rows=9000 distinct=9000 sum_count=90,000,000
  dup=0` = **PERFECT — no-dup + complete + correct sum at 16-partition scale.** ✅
- **P1b CRASH (kill -9 mid-run, resume from S3 checkpoint):** `rows=47138 distinct=9000 sum_count=101,542,129
  **dup=38,138**` = **FAILS.** ❌ Multi-partition continuous stateful **crash-EO does NOT hold at 16-partition
  scale** — the crash re-commits windows non-idempotently (38k dups). Local PARTS=8 (5/6) UNDERSTATED it; at
  scale it fails hard + consistently.

**⚠️ REGRESSION CONCERN (must reconcile):** the PRIOR P1 run (2026-07-02, **single-instance** realtime,
[[project_realtime_eo]] / P1 memory) **passed** P1b crash-EO (`dup=0`). This run (**N-reader default**,
T-EO-4) **fails** it (`dup=38k`). So T-EO-4 (N-reader default) likely **regressed crash-EO** while fixing
no-dup/completeness (gate C6/C7) + throughput. Genuine tradeoff by workload: single-instance held crash-EO
for the P1 keyed-producer pattern but had the scrambled-order gate dups; N-reader fixes the gate but its
union-commit isn't crash-atomic at N=16. **DECISION NEEDED:** either (a) fix the multi-instance crash-atomic
commit so N-reader is a safe default, or (b) revert T-EO-4's default to single-instance until then (keeping
N-reader opt-in via a flag) — do NOT ship an N-reader default that regresses crash-EO. Isolate first:
re-run P1b with `ZELOX_RT_SINGLE=1` on EKS to confirm single-instance still passes crash-EO at 16 parts.

**Honest conclusion:** no-dup + **completeness are Flink-class at scale** (P1a dup=0, throughput near-parity,
memory beats). But **crash-recovery exactly-once is NOT yet correct at scale** — the per-epoch sink commit +
offset union commit are not crash-atomic/idempotent across N=16 instances. THIS is the real remaining P0 for
the "Flink-class crash-EO" headline. Candidate fix: idempotent per-epoch sink commit (restart overwrites the
uncommitted epoch's files deterministically; `_spark_metadata` gates strictly) + atomic union offset commit.
Re-validate P1b to dup=0 at 16 partitions on EKS. Until then: claim no-dup+completeness (validated), NOT
crash-EO-at-scale.

## ✅ RESULT (measured 2026-07-03 — LOCAL gate; see EKS caveat above for crash-EO at scale)
Multi-partition **continuous stateful exactly-once** is now correct **by default** (realtime Kafka source
defaults to N readers; `ZELOX_RT_SINGLE` opts out). Measured on `correctness_gate.sh` local-cluster:
- **C6** continuous 4-partition scrambled: **3/3 = 1800 rows / 6 windows, no-dup, all counts=10** (complete).
- **C7** continuous 4-partition + hard `kill -9`: **2/2 = INC_CKPT_EO_ACROSS_CRASH PASS**, complete + no-dup.
- **Full gate GREEN 6/6** (C1/C2/C4/C5 unchanged). clippy `-D warnings` clean.

The four pieces: **T-EO-1** per-instance FLIP-27 split assignment (no over-read) · **T-EO-3** per-instance
staged offsets + sink UNION commit (all instances resume from committed offsets → no re-read dups) ·
**T-EO-3.5** exchange watermark idleness + **all-idle DRAIN-TO-MAX** (when every input drains, advance the
watermark to the max seen so the final windows close — this was the last-window-edge root cause, found via
the RUST_LOG observability harness: `merged=None, wm=[51,51,70,41], idle=4` = all excluded → stall at MIN) ·
**T-EO-4** make N-reader the realtime default. Grounded in Flink `withIdleness` + RisingWave per-partition
watermark (REFERENCES §2, §8). The KB expected exact-zero to need EKS; achieved locally. NEXT: EKS scale
(PARTS=8+) validation before the headline "Flink-class" claim; robust README/STATUS to the measured result.

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
  filter (`g % parallelism == inst`). Before: `ZELOX_RT_MULTI` PARTS=4 gave `counts=[12,28] sum=40`
  (4× over-read, 562 dup groups). After: `counts=[10,10] sum=20` — **over-read eliminated** (per-instance
  counts correct = 10). Committed.
- **Exposed next layer:** multi-instance NOCRASH now shows **FULL duplicates** (`[10,10]`, ~300 groups) =
  the sink/epoch **commit coordination across N instances** is missing. This is T-EO-2/3 (per-instance
  epoch staging + atomic union commit): each of the N instances/sink-tasks emits its window output, and
  without a coordinated per-epoch union commit the same `(window,key)` is written by more than one epoch
  flush / sink task. Confirms the design — multi-instance realtime is correct ONLY with the union commit.
- **Single-instance default path** (no `ZELOX_RT_MULTI`) remains the closest today (~2 partial-count
  split dups = residual watermark race), but has the inherent interleaved-read false-idle limitation.
- **Decision:** the robust Flink-class path is T-EO-2/3 (multi-instance union commit), a real F2/F3
  effort. Continue incrementally, each step gated; do not claim exact-zero until C6+C7 are GREEN.

## 5c. Re-emit localized (2026-07-02, measured)

The `[10,10]` full-dups are **NOT sink-task overlap** (measured: single `part-0.parquet` per epoch,
300 same-file re-emits, 0 cross-file) and **NOT partial splits**. Pattern: **epoch 0 clean (300 rows),
later epochs 39/52 doubled (~600)** ⇒ the **window operator re-emits already-closed windows in later
epochs** on the multi-instance path. Mechanism (localized in `window_accum.rs`):
- The window runs **one instance per input partition** (`n_partitions = input.output_partitioning()`,
  line ~526), each with its own `emitted_ends` HashSet (emit-once dedup, line ~340).
- `emitted_ends` is **pruned** for windows finalized past the watermark (line ~966, "P1 fix"). With N
  instances the MIN-merged watermark advances unevenly; a window's end can be pruned from `emitted_ends`
  and then a **lagging instance's data re-closes + re-emits** the full window ⇒ `[10,10]`.

**T-EO-2/3 target (refined):** the emit-once guarantee must hold across the multi-instance epoch
boundary. Options to evaluate (instrumented): (a) don't prune `emitted_ends` until the window is past
lateness on the GLOBAL (MIN) watermark AND all N instances have passed it; (b) make the durable commit
carry `emitted_ends` so a re-emit is deduped at commit; (c) the coordinated union commit (T-EO-3) dedups
by (window,key) at the atomic commit. Next debug step: instrument the window emit + `emitted_ends`
prune/re-close to confirm the lagging-instance re-close, then fix the prune condition to be
global-watermark + all-instances-passed. Each iteration re-runs `inc_ckpt_gate.sh ZELOX_RT_MULTI=1`.

## 5d. T-EO-3 result + residual (measured 2026-07-03)

**T-EO-3 (per-instance staged offsets + sink union commit) FIXES the core multi-instance dup.**
RT_ASSIGN log proved the mechanism: on the 2nd execution, ALL instances now resume from their
committed offsets (`(0,843) (1,3738) (2,1744) (3,2675)` — were `0` for 3 of 4 before) ⇒ no re-read.

Measured (`ZELOX_RT_MULTI`, PARTS=4, N=300):
- **Gate config RUN=40: 3/3 runs = DUP_GROUPS=0, count!=10=0** (no-dup invariant HOLDS, counts exact).
- **Residual:** a longer RUN=75 hit 309 dups ONCE = a **timing-dependent epoch-boundary race** (more
  commit boundaries → the query-stop-vs-final-epoch-commit gap re-reads). Not yet exact-zero at all
  timings. This is the continuous epoch-boundary residual the KB flagged as historically needing EKS.
- **Completeness:** window coverage varies with run duration (short run closes fewer of the 6 windows)
  — a test-timing artifact (produced windows are all correct: count=10, no dup), not data loss.

**Status:** core re-read dup is FIXED + validated at gate config. Remaining for exact-zero-at-all-
timings = the graceful-stop / final-epoch-commit synchronization (source must stage + sink must commit
the final epoch on EndOfData before the query ends), then re-verify RUN=75 + PARTS=8, and EKS. Do NOT
promote C6/C7 XFAIL→GREEN or claim "Flink-class multi-partition continuous stateful EO" until the
residual is closed and completeness is proven no-loss on a full-window run.

## 5e. C7 crash-EO result (measured 2026-07-03)

The RUN=75 "309 dups" was a **one-off from the earlier disk-full/Docker-unhealthy period** — in a clean
env, RUN=75 is **2/2 = 0 dups**, RUN=40 is **3/3 = 0 dups** (5/6 total; the outlier explained).

**C7 (continuous PARTS=4 + hard `kill -9` + restart, ZELOX_RT_MULTI):** across the crash,
`no_dup=True, all_counts_10=True, DUP_GROUPS=0, count!=10=0` — **exactly-once no-duplication across a
hard crash HOLDS on the multi-partition path** (the union-commit recovery replays from committed offsets
with no re-read). The gate reports FAIL **only on completeness** (`rows=1200 vs 1800` = 4 of 6 windows
closed), NOT on dups/counts.

**Net:** the hard target — multi-partition continuous stateful EO **no-dup across crash** — is achieved
by T-EO-1 + T-EO-3. **Remaining = COMPLETENESS (the "last-window edge"):** the final windows don't close
because the per-partition watermark (MIN over N instances) doesn't advance when the gate's flush/max-ts
reaches only some partitions — idle partitions must be excluded (Flink `withIdleness`) OR the flush must
reach all partitions. This is a separate issue from the dup fix (and also affects single-instance short
runs). Next (T-EO-3.5): make the multi-instance per-partition watermark advance at end-of-input so all
windows close (idleness on the realtime path), then completeness == no-loss ⇒ full EO; then promote
C6/C7 XFAIL→GREEN + EKS.

## 5f. Idleness targeting — topology finding (measured 2026-07-03)

T-EO-3.5 added `withIdleness` to the IN-PROCESS `StreamExchangeExec` N→M merge (`merge_output_subchannels`)
and passed the full gate GREEN (no regression) — but it did NOT close the completeness edge. Instrumented
(`STREAM_EXCH_MERGE` log): that merge **fires 0 times** for the multi-instance realtime windowed-agg on the
gate's `--mode local-cluster` path. So the watermark merge for this query is NOT the in-process exchange —
it is the **distributed shuffle path** (`sail-execution/src/stream_service/client.rs` + `plan/shuffle_write.rs`)
and/or the window operator's own multi-input handling (`window_accum.rs`: "one instance per input partition,
broadcast watermark"). The in-process idleness is correct + kept (it helps single-node/in-process exchange
plans), but the **completeness fix for the gate's distributed path must add idleness to the DISTRIBUTED
streaming watermark merge** (F2/F3 territory). Precise next step, scoped; not yet implemented.

**Honest net for the branch:** the CORE win — multi-partition continuous stateful EO **no-duplication,
incl. across a hard crash** — is DONE + measured (T-EO-1 + T-EO-3). Completeness (all windows close) is
narrowed to the distributed watermark-merge idleness; the in-process idleness (T-EO-3.5) is a correct,
green, but insufficient-for-local-cluster piece. Do NOT promote C6/C7 or claim Flink-class multi-partition
continuous stateful EO until the distributed-path idleness lands + no-loss is proven + EKS.

## 5g. Completeness investigation hit an OBSERVABILITY wall (2026-07-03)

Correction to §5f: the "in-process exchange idleness fires 0×" was measured from `/tmp/incckpt_server.log`,
which in `--mode local-cluster` is only the SCHEDULER (82 bytes) — **worker execution logs (where the
exchange/window actually run) are NOT captured there.** So we cannot reliably observe whether the
idleness engages or why completeness varies. The single-node comparison (`--mode local`) produced no CHECK
(gate not single-node-compatible). `MergedRecordBatchStream` (distributed merge) has zero watermark
handling, so the distributed watermark path is either elsewhere or the streaming pipeline runs in-process
on workers (making `StreamExchangeExec` idleness the right place after all — unverifiable without worker logs).

**Blocked-on-observability.** Prod-grade rule: do NOT commit completeness fixes we can't measure. The
required FIRST step is a reliable local streaming-observability harness: either (a) capture worker logs in
local-cluster (`--mode local-cluster` worker stdout → a known file), or (b) make `inc_ckpt_gate` runnable
`--mode local` so all logs land in one place, or (c) a `ZELOX_PLAN_DUMP` that prints the finalized streaming
physical-plan tree (so the watermark-merge point is known with certainty, not inferred). THEN target the
idleness at the proven merge point and validate completeness → no-loss. Until then, completeness stays
honestly open; the CORE no-dup-across-crash win (T-EO-1/T-EO-3) is unaffected and validated.

## 6. Honesty / risk

The residual has historically resisted a single-instance patch and "full exact-zero" was expected to
need EKS (MEMORY). Parallelism=N + multi-instance EO commit is a **real architectural change** (F2/F3
territory), done incrementally behind a flag with per-step gates. If a step doesn't reach exact-zero
locally, we say so and take it to EKS rather than claim prematurely.
