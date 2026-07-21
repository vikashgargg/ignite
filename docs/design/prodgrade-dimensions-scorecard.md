# Zelox prod-grade dimensions scorecard — measure EVERYTHING, then claim, then fix

**Discipline (2026-07-01):** before claiming "Flink/Spark replacement" or doing prod fixes, measure each
dimension that defines a prod-grade streaming+batch engine, honestly, vs a named baseline. Claim ONLY
measured (see [[feedback_competitive_claims_bar]]). Then commit/push to claim the design, then prod fixes
grounded in Flink 2.x / Spark 4.1 RT-mode / RisingWave / StreamNative (REFERENCES §2/§3/§3c/§3d).

## The dimension matrix
| # | Dimension | Metric | Baseline (KB) | How (harness) | Local? | Status |
|---|---|---|---|---|---|---|
| D1 | Throughput | events/s @100M | Flink 1.19 5.74M ev/s | eks_stream_headtohead.sh | EKS | ✅ 5.37M (1.068×) |
| D2 | **Latency** | p50/p99/p999 e2e (event→sink) | Spark4.1 RT ms–300ms; Flink sub-s | NEW: lat_probe (inject ts, measure at sink) | local | ❌ UNMEASURED |
| D3 | **Memory bounded** | RSS plateau under sustained load (not O(N) growth) | Flink off-heap state + credit backpressure | NEW: mem_soak (long stream, sample RSS) | local | ❌ peak only (9.61 vs 8.57) |
| D4 | **Cold start** | launch→first-output ms | no-JVM should win big vs Flink JVM | NEW: time to first window | local | ❌ UNMEASURED |
| D5 | **Recovery time** | kill→resume→caught-up s | Flink 2.0 49× faster (ForSt) | extend rescale_gate/inc_ckpt_gate w/ timing | local | ⚠️ correctness yes, TIME no |
| D6 | Checkpoint dur/size | ms + bytes per ckpt | Flink 2.0 −94% dur (inc) | inc_ckpt_gate + timers | local | ⚠️ O(delta) proven, not timed |
| D7 | State at scale | M distinct keys, no cap, bounded mem | Flink RocksDB/ForSt millions | state_scale_stress.py (push to 1M+) | local | ⚠️ 64k-cap fixed; re-verify @1M |
| D8 | Correctness/EO | dup/loss=0, EO across crash | Flink EO | correctness_gate.sh (C1–C7) | local | ✅ green (C6/C7 xfail) |
| D9 | Rescale | exact across parallelism change | Flink FLIP-8 | rescale_gate.sh | local | ✅ mechanism; bit-exact gated by EO residual |
| D10 | Backpressure | bounded in-flight under slow sink | Flink credit-flow (FLIP-2) | NEW: slow-sink, watch RSS+progress | local | ❌ UNMEASURED |

## Build order (cheap local first; D1 already have on EKS)
1. **D2 latency** — the headline real-time axis, never measured; likely where no-GC actually wins.
2. **D3 memory-bounded + D10 backpressure** — your stated doubt; root-cause (bounded buffers/spill/allocator).
3. **D4 cold start** — quick, no-JVM win to quantify.
4. **D5/D6 recovery+ckpt timing** — extend existing gates with timers.
5. **D7 state@1M** — re-run stress at 1M keys (post 64k-cap fix).
6. D8/D9 already green — just fold into the scorecard run.

## Output
One `scripts/dimensions_scorecard.sh` that runs D2–D10 locally + emits a table (Zelox vs baseline per
dim), plus the EKS D1 number. Then: commit/push to CLAIM the measured design. Then prod fixes per dim,
grounded — memory→Flink off-heap+credit-flow; latency→Spark RT concurrent-stages; recovery→ForSt;
backpressure→FLIP-2; skew→Flink 2.3 adaptive-partition; state→RisingWave/ForSt tiered.

## Honesty rule
Each cell: ✅ measured-better · ⚠️ measured-competitive · ❌ measured-worse · ❓ unmeasured. No claim
without a number. This scorecard IS the "stands against Spark+Flink" evidence.
