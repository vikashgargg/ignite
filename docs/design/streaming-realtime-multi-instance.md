# Realtime multi-instance source (FLIP-27) — throughput Phase B + watermark correctness

**Status:** design. **Advances:** throughput capstone Phase B ([eks-throughput-capstone.md](eks-throughput-capstone.md))
+ per-partition-watermark/last-window correctness (#2b, [streaming-per-partition-watermark.md](streaming-per-partition-watermark.md)).
**Grounded in:** Flink FLIP-27 (one split per reader), Chandy-Lamport barriers, Spark 4.1 RT-mode, +
Vajra's already-built bounded path / `StreamBarrierAlignExec` / EpochCoordinator (REFERENCES §1/§2).

## Problem (Phase A, measured)
The realtime/continuous Kafka source is pinned `parallelism=1` (`kafka/reader.rs:286`): ONE instance
reads ALL N partitions, runs `from_json` + `WatermarkExec` single-threaded, then the exchange fans out.
`VAJRA_WM_PROF` shows the window **STARVED** (input_wait ≈100%, finalize 0%) ⇒ the source path is the
~2.4×-vs-Flink bottleneck. It also forces the per-partition-watermark workaround (single-instance MIN +
discovery-grace) whose last-window edge is still open. Both stem from **single-instance read**.

## Why it's single-instance today
Realtime EO commit is single-coordinator: one `realtime/committed` object, per-epoch offset staging
(`write_staged_epoch_offsets`), and the sink commits once per aligned epoch. N independent readers would
clobber that coordination. (The BOUNDED path is already N-instance — `reader.rs:270` one-task-per-
partition — because it has no per-epoch realtime commit, just final offsets.)

## Design — N readers + coordinated epoch commit (compose existing parts)
1. **N source instances, one per Kafka partition** (FLIP-27 split assignment — reuse the bounded path's
   `parallelism = count_kafka_partitions` + `offset_key(inst, parallelism)` per-instance offset keys).
   Each instance reads its partition IN EVENT-TIME ORDER → its `WatermarkExec` is monotone (no per-
   partition workaround needed) → the keyed exchange MIN-merges across instances (already does, Flink
   keyBy receiver rule). **This alone fixes throughput (parallel read+from_json) AND the watermark edge.**
2. **Per-instance epoch barriers, aligned.** Each instance emits `Checkpoint{epoch}` at the trigger
   cadence; `StreamBarrierAlignExec` (N→1 Chandy-Lamport, already built) aligns them so the sink sees one
   aligned epoch barrier — the consistent global snapshot. EpochCoordinator drives the epoch number.
3. **Coordinated commit (the one real new piece).** Generalize the single `realtime/committed` to an
   epoch that's committed only when ALL N instances have staged their epoch offsets:
   - each instance writes `sources/0/inst-<i>/staged-epoch-<e>` (per-instance, like bounded offsets);
   - on the aligned barrier, the sink (or a commit coordinator) writes ONE atomic `realtime/committed`
     = `{epoch, offsets: union of all N instances}` — same atomic-single-object principle as F1b/F4.
   - restart: each instance seeks its partition's committed offset from that union. EO preserved.
4. **Sink:** `RealtimeFileSinkExec` already commits per aligned epoch (`_spark_metadata` +
   `realtime/committed`); it now commits the N-instance union. The N→1 align (step 2) means the sink
   still sees a single ordered epoch stream.

## What's REUSED vs NEW
- REUSED: bounded per-partition read + per-instance offset keys; `StreamExchangeExec` MIN-merge;
  `StreamBarrierAlignExec`; EpochCoordinator; `RealtimeFileSinkExec` epoch-atomic commit; per-partition
  `WatermarkExec` becomes unnecessary (each instance is single-partition-ordered → global max is correct).
- NEW: N-instance realtime epoch-offset staging + union commit (generalize the single-coordinator commit).

## Build steps (incremental, each locally gated — final throughput on EKS)
1. **Flip realtime `parallelism`** from 1 to `count_kafka_partitions` (`reader.rs:286`) behind a gate
   (`VAJRA_RT_MULTI`), wire per-instance offset keys (already exist for bounded). Gate OFF = today.
2. **Per-instance epoch staging + union commit**; restart-seek from the union. Validate EO with
   `inc_ckpt_gate.sh PARTS=4` (must stay no-dup/no-loss across crash) at N instances.
3. **Drop the per-partition `WatermarkExec` workaround** on this path (each instance single-partition →
   monotone watermark; exchange MIN-merges) — re-run the continuous gate: expect bit-exact, last-window
   edge GONE (closes #2b).
4. **Re-profile** with `VAJRA_WM_PROF` (window should no longer be starved) + **EKS throughput** A/B vs
   the single-instance baseline and vs Flink. Target ≤1.2× Flink, keep the 6.6× memory win.

## Risks / honest unknowns
- The union-commit across N instances under crash+replay is the delicate part (the same multi-partition-
  commit race the gap register flags) — must be gated + crash-gated before default-on.
- Per-instance ordering assumes one partition per instance; if `parallelism < N_partitions` an instance
  owns multiple partitions and the per-partition-watermark workaround is still needed for THAT instance
  (keep it as the fallback, don't delete the code — just bypass when 1:1).
- Final throughput is only credible on controlled EKS (local totals noisy); local gates prove
  correctness + the re-profile (no-longer-starved) only.
