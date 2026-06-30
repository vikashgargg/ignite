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
| **VAJ-T4** | after T7 | Zero-copy exchange — slice instead of `concat_batches` + arrow `Utf8View` (no payload copy on shuffle). |
| VAJ-T5/T6 | backlog | source_read poll floor / simd-json — only if T7+T4 don't reach ≤1.0×. |

## Measured trajectory
- Baseline 1.15× slower (4.92M). T1+T2 → **1.10× (5.18M)**. Next: T7 (parse-fusion) → T4 (zero-copy exchange) → target ≤1.0×.

## Module map (no exploration needed — from CODEMAP)
- T1/T2 → `sail-data-source/src/formats/kafka/reader.rs` (`KafkaArrowBuilders`, bounded read loop, offset maps `ends`/`next`, `write_staged_offsets`).
- T4 → `sail-physical-plan/src/streaming/exchange.rs` (`StreamExchangeExec::distribute`, `concat_batches`).
- WM_PROF counters → `sail-common-datafusion/src/streaming/event/encoding.rs`.

## Grounding (REFERENCES)
- §170 — Flink's weakness = per-record deserialize; Vajra's edge = Arrow bulk-columnar (T1).
- Flink per-split (TopicPartition) offset state — not re-hashed per record (T2).
- §68 — Spark RT-mode `KafkaSourceRDD` one task / TopicPartition.
