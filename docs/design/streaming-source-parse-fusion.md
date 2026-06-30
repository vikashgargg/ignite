# EPIC VAJ-T7-FUSION — source-side JSON parse fusion (the Flink-beater)

**Status:** design (do NOT implement until this is reviewed — it touches the wrapped-source rewriter
path that already cost us in the per-partition-watermark work + hours of cluster thrash). Design-first.

## Why (measured)
EKS 100M, after T1+T2+T4+T7a we are **1.068× slower than Flink** (5.37M vs 5.74M ev/s), 9.61 vs 8.57
GiB. WM_PROF: `source_read 100.6s ≫ exchange 67s > from_json 31.7s`. The #1 remaining cost is
**`source_read`'s materialization of the raw JSON `value` as an Arrow Binary column (~10GB copy @100M)**,
which `from_json` then re-reads. Flink never does this: `KafkaDeserializationSchema` parses JSON
**directly from the fetch buffer** into the row, no raw-bytes column. **Eliminating that copy is the only
lever left that can clearly beat Flink** (~50s of the 100.6s).

## Goal
When the plan is `KafkaSource → from_json(value, schema) → [select fields] → window`, parse the Kafka
`value` bytes **inside the source read** into the target struct columns (reusing T7a's `ColBuilder`), so:
- the raw `value` Binary column is **never materialized** (kills the ~10GB copy in source_read), and
- the separate `from_json` operator is **removed** (its 31.7s folds into the source, done once).
Target: ≤1.0× Flink throughput while keeping memory ≤ Flink.

## Grounding
- **Flink** `JsonRowDataDeserializationSchema` + `KafkaSource` `RecordEmitter`: deserialize in the source,
  per-field converter into the row — no intermediate generic value. (REFERENCES §170: Flink's per-record
  JVM deserialize is its weakness; ours is Arrow-vectorized + no-GC — but only if we don't re-copy.)
- **Spark/DataFusion** projection + expression pushdown into scans — pushing a parse into the source is
  the aggressive form of the same principle.
- Reuse **T7a `ColBuilder`** (already parity-tested) as the parse sink ⇒ semantics identical by
  construction (the hard de-risking).

## The hard constraint (learned)
The streaming Kafka source is **NOT a plain `TableScan`** in the rewriter — it is wrapped
(`StreamSourceWrapperNode`/`StreamSourceAdapterNode`), and `WatermarkNode` is pre-created at
`resolver/query/misc.rs`. A naive `TableScan`-hooked rule will NOT fire (this exact trap bit the
watermark work — see CODEMAP "Watermark/source wiring"). The fusion rule must hook the wrapped node.

## Mechanism (options)
- **A. Streaming-rewriter rule** (`sail-plan/src/streaming/rewriter.rs`): detect `Projection[from_json(
  col(value), schema) → d.*]` directly above the wrapped Kafka source; rewrite to a fused
  `KafkaSourceExec{ parse: Some(ParseSpec{ value_col, schema, options }) }`; drop the from_json
  projection. ← recommended (keeps from_json semantics in one place).
- B. Physical optimizer rule post-planning (same detection at the exec level). More fragile (exprs lowered).

## KafkaSourceExec change
- Add `parse: Option<ParseSpec>` (value column index + target `Fields` + `SparkFromJsonOptions` + tz).
- When `Some`: the read loop, instead of a `value` BinaryBuilder, drives T7a `ColBuilder`s over the
  parsed value bytes per message; emits the parsed struct columns (+ partition/timestamp for watermark).
- **Codec round-trip** (`sail-execution/src/codec.rs` + `physical.proto`): `ParseSpec` must serialize, or
  log a single-node-only gap. (Distributed-aware rule.)
- **Gated `VAJRA_FUSE_PARSE`** default-off until EKS-validated (zero regression risk while iterating).

## Risk + mitigation
- **Semantic parity:** reuse T7a `ColBuilder` (same coercion) → identical by construction. Gate with the
  from_json differential harness + the streaming correctness gate.
- **Watermark interaction:** the per-partition watermark needs the `partition` + event-time columns
  preserved — the fused source must still emit them (it already projects them in T1).
- **Rewriter wrapping:** hook the wrapped node, not `TableScan` (the known trap). Prototype detection
  behind a debug log first; confirm it fires before wiring the rewrite.

## Validation (gates, in order)
1. from_json differential harness — parity (unchanged outputs).
2. streaming correctness gate (`scripts/correctness_gate.sh`) — no dup/loss/EO regression.
3. clippy lane `--all-targets -D warnings`.
4. ONE EKS 100M run: source_read must drop (~100.6→~50s), from_json→0 (folded), beat Flink? + teardown $0.

## Done-criteria
`VAJRA_FUSE_PARSE` on: from_json operator removed for the Kafka path, source_read down ~50s, throughput
≤1.0× Flink with memory ≤ Flink, all parity/correctness gates green, codec round-trips (or gap logged).
