# DataFusion 53.1.0 → 54.0.0 upgrade plan (optimization opportunities)

> Vajra embeds DataFusion as its execution core. DataFusion **54.0.0** is released
> (https://datafusion.apache.org/download.html, release tracking apache/datafusion#21080).
> This plans the upgrade and where 54.0.0 buys Vajra concrete wins. Current pin: **53.1.0**
> (workspace `Cargo.toml`, ~30 `datafusion*` crates incl. `datafusion-proto` used by our codec).

## What 54.0.0 brings that directly helps Vajra

| 54.0.0 change | Vajra benefit |
|---|---|
| **RepartitionExec: coalesce batches before sending to distributor channels** | Directly speeds our **shuffle** path (`StreamExchangeExec` / Arrow-Flight shuffle build on the same repartition model) — fewer, larger batches across channels = higher throughput, lower per-batch overhead. Helps both batch (ClickBench/TPC-H distributed) and the streaming exchange. |
| **Parquet: skip predicates provably false; reorder files+row-groups by stats; sort pushdown; TopK file reorder** | Faster batch scans (TPC-H/ClickBench, Iceberg/Delta reads) — less I/O + CPU on selective + TopK queries. |
| **ORDER BY: prune functionally-redundant sort keys** | Removes needless sorts in batch plans → lower latency + memory. |
| **Datetime predicate simplification (preimages, date_trunc/date_part)** | Common in TPC-H/analytics; cheaper filters. |
| **Scalar subquery physical execution (`ScalarSubqueryExec`, gated)** | Faster correlated/scalar subqueries vs interpretation. |
| **Lateral joins; lambda expressions + `array_transform`; Extension Type Registry** | Spark-compat surface area (lateral, higher-order fns) + a clean hook for **Vajra-native types** (vector/embedding for the AI-native north star) via the type registry. |
| **arrow-avro migration** | Drops DataFusion's internal Avro conversion → less code, faster Avro reads (we use Avro in the Kafka/source path). |
| Continued **StringView/BinaryView** maturation | Lets us replace the byte-bounded-batch workaround in the Kafka source with `BinaryView` (i64-class offsets) for genuinely huge values — the Arrow-native fix for the i32 offset class. |

## Breaking changes to handle (from the 54.0.0 notes)
- **`ExecutionPlan::apply_expressions()` reverted** → no action (it was never in 53.1.0's stable surface we use).
- **`ScalarSubqueryExec` gated behind a session property** → opt-in; set the session flag if we want the perf path.
- **Higher-order UDF wrapping refactored to a concrete struct** → check our UDF registrations (`sail-function`) still compile.
- **Arrow major bump** (54 tracks a newer arrow-rs than 53) → the largest ripple: Vajra touches Arrow pervasively (arrays, compute, IPC for Flight shuffle, parquet). `datafusion-proto` (our `codec.rs`) and the flow-event encoding must compile against the new arrow. **Verify the exact arrow version from `datafusion 54.0.0`'s `Cargo.toml` before starting.**

## Upgrade approach (low-risk, staged)
1. **Branch + bump** all `datafusion*` pins 53.1.0 → 54.0.0 in workspace `Cargo.toml`; `cargo update`; resolve the arrow version bump.
2. **Compile-fix** the workspace crate-by-crate (expect changes in: `sail-execution/codec.rs` via datafusion-proto; `sail-function` UDF wrappers; any `ExecutionPlan`/`PlanProperties` API drift; arrow array/compute signature changes).
3. **Re-run the full gates**: 105/105 multi-mode scorecard; differential-trust 124/124 vs Spark; clippy `-D warnings`; the streaming suite (6/6 + EO + the new Kafka-sink latency/EO tests).
4. **Re-benchmark** TPC-H SF-1/SF-100 + ClickBench to quantify the 54.0.0 scan/repartition gains (expect modest but real batch speedups; the RepartitionExec coalesce should help distributed throughput).
5. **Then exploit**: enable `ScalarSubqueryExec`; adopt `BinaryView` in the Kafka source; wire the Extension Type Registry for Vajra-native types.

## Sequencing vs the large-state backend
Do the **DF-54 upgrade first** if its Arrow bump is moderate (it de-risks everything downstream and the RepartitionExec/Parquet wins are free), **or** land the large-state backend on 53.1.0 first if the arrow bump is disruptive — decide after step 1 reveals the arrow delta. Both are tracked in `docs/PROD_GRADE_ROADMAP.md`.
