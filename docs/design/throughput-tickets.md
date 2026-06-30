# EPIC VAJ-THRU — beat Flink on windowed-agg throughput

**Baseline (EKS 100M, c7g.4xlarge, 2026-06-30):** Vajra 4.92M vs Flink 5.67M ev/s = **1.15× slower**,
1.2× more memory. Per-stage CPU: `source_read 106s ≫ exchange 80s > from_json 38s > finalize 23s`.
**Goal:** ≤1.0× Flink (beat) on this path, keep correctness/EO. Work the stages in CPU-rank order.

**Working rule:** orient from CODEMAP/MEMORY/REFERENCES; open a file only to edit it (minimal slice).
Each ticket: status · acceptance criteria (AC) · the CODEMAP module it touches · grounding.

| Ticket | Stage | Status | Acceptance criteria |
|---|---|---|---|
| **VAJ-T1** | source_read | ✅ DONE | Projection-aware `KafkaArrowBuilders` — build+append ONLY projected cols; clippy green; committed. Kills 100M constant-topic-string copies + 3 wasted column builds. |
| **VAJ-T2** | source_read | �doing | Partition-keyed offset state — no per-msg `String` alloc/hash in the hot loop; **durable (topic,part)→offset commit format preserved**; unit test; clippy green. |
| **VAJ-T3** | validate | ✅ DONE | EKS 100M re-run (2026-06-30): Vajra **5.18M ev/s** (was 4.92) vs Flink 5.72 = **1.10× slower** (was 1.15×); RSS 10.19 GiB (was 10.38). WM_PROF: source_read 106→**99.7s**, exchange 80→**68s**, from_json 38s, finalize 23→19.6s. Real +5.4% gain, did NOT beat Flink. Torn down $0. |
| **VAJ-T4** | exchange | 🔨 NEXT | Cut `concat_batches` copy in `StreamExchangeExec::distribute` + arrow `Utf8View` (version-upgrade) — now the clearest lever (68s) + helps memory. |
| **VAJ-T5** | source_read | 📋 backlog | source_read floor ~100s = rdkafka poll + necessary value-payload copy (10GB @100M). Investigate: borrow-from-rdkafka-buffer into Arrow (zero-copy value), bigger fetch.max.bytes. Diminishing — after T4. |
| **VAJ-T6** | from_json | 📋 backlog | from_json 38s — simd-json now relatively bigger; revisit only after T4+T5. |

## ROOT CAUSE of the residual gap (WM_PROF 2026-06-30): data-movement-bound, NOT compute-bound
Window is STARVED: `input_wait≈1192%` (idle waiting), `finalize` only 19.6s. We are not compute-bound.
The cost is **stage-boundary copies of the raw JSON `value`**: source_read **materializes raw value
bytes into an Arrow Binary column (~10GB @100M)** → from_json **re-reads + parses it** → exchange
**`concat_batches` copies** it to window instances. **Flink avoids ALL of this**: `KafkaDeserialization
Schema` parses JSON *directly from the fetch buffer* (never materializes raw bytes) + operator chaining
+ pipelined shuffle stream records concurrently. Flink's per-record Jackson+GC is slower per-op but it
never pays the raw-value round-trip. ⇒ our Arrow/no-GC edge is being SPENT on copies.

## REPRIORITIZED — fix the round-trip (turns our advantage into a win)
| Ticket | Status | What |
|---|---|---|
| **VAJ-T7** | 🔨 NEXT (the big one) | **Fuse JSON parse into the source** — parse value→struct cols inside the read; raw value never materialized as a full column; exchange carries narrow parsed cols. Attacks source_read + from_json + exchange at once. Grounded: Flink deserialize-in-source + DataFusion projection/expr pushdown into scan. |
| **VAJ-T4** | ✅ DONE+EKS | One `take` per owner via `vajra_key_groups` (was 128-split+`concat_batches`). EKS: 5.18→**5.33M ev/s**, gap 1.10→**1.085×**, RSS 10.19→**9.90 GiB**. BUT exchange CPU 68→66.5s only (shuffle cost = hash+route+send, NOT the concat copy) ⇒ exchange has a LOW ceiling. Did NOT reach parity. |
| **VAJ-T7** | 🔨 measurement-justified | T4 didn't win; upstream still ~206s, window starved. Biggest remaining = **source_read 102s (value-byte copy) + from_json 37s = ~140s**. Rewrite from_json parse (simd-json + direct-builder, semantic-parity + simd-json-padding care) and/or fuse parse into source. On branch streaming/t7-parse-fusion; final confident EKS after. |
| VAJ-T5/T6 | backlog | source_read poll floor / simd-json — only if T7+T4 don't reach ≤1.0×. |

| **VAJ-T7a** | ✅ DONE+EKS | Flink-class columnar `ColBuilder` from_json (primitives→typed builders, complex→exact fallback; 20 parity tests). EKS: 5.33→**5.37M ev/s**, gap 1.085→**1.068×**, RSS 9.90→**9.61 GiB**, from_json CPU 37.4→**31.7s**. Real but modest — the `serde_json` PARSE (~27s) remains (that's T7b). |

## Measured trajectory (all EKS 100M, c7g.4xlarge, vs Flink 1.19)
- Baseline **1.15×** (4.92M) → T1+T2 **1.10×** (5.18M) → +T4 **1.085×** (5.33M) → +T7a **1.068×** (5.37M).
  Memory 10.38→**9.61 GiB**. Steady, measured — **competitive, ~7% behind, NOT yet beating.**

## What's left to BEAT Flink (honest, ranked by remaining CPU)
1. **source_read 100.6s (value-byte copy) — #1, unchanged.** Only **source-fusion** (parse JSON in the
   Kafka source, never materialize the value Binary col) cuts it. Big/risky: touches the wrapped-source
   rewriter path. ~50s potential → would clearly beat Flink.
2. **from_json 31.7s — T7b simd-json** faster parse (the serde_json tree is the residual). ~32→~18s.
3. exchange 67s = floor (shuffle hash+route+send, not copy).
**To clearly beat Flink: source-fusion (#1) is required; T7b helps but isn't sufficient alone.**

## Module map (no exploration needed — from CODEMAP)
- T1/T2 → `sail-data-source/src/formats/kafka/reader.rs` (`KafkaArrowBuilders`, bounded read loop, offset maps `ends`/`next`, `write_staged_offsets`).
- T4 → `sail-physical-plan/src/streaming/exchange.rs` (`StreamExchangeExec::distribute`, `concat_batches`).
- WM_PROF counters → `sail-common-datafusion/src/streaming/event/encoding.rs`.

## Grounding (REFERENCES)
- §170 — Flink's weakness = per-record deserialize; Vajra's edge = Arrow bulk-columnar (T1).
- Flink per-split (TopicPartition) offset state — not re-hashed per record (T2).
- §68 — Spark RT-mode `KafkaSourceRDD` one task / TopicPartition.
