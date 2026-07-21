# Streaming update / retraction output mode (zero-loss late data)

Status: DESIGN (STEP 2 of the Flink-parity streaming work). STEP 1 (throughput +
correctness on ordered streams) validated on EKS 2026-06-21: every group exactly
10000, 0 loss, 6.6× less memory than Flink.

## Why

Zelox's window operator today (`crates/sail-physical-plan/src/streaming/window_accum.rs`)
is **emit-on-window-close, append-only** — the exact model of Spark Structured Streaming
and RisingWave's default. A window emits once when `watermark ≥ window_end`, then its
state is dropped. Any record that arrives for an already-closed window is **silently
dropped**. This is correct for an ordered stream, but on a real out-of-order stream a
tight watermark drops late data — and Flink SQL and Spark drop it too.

| Engine | Late-but-in-bound | Late beyond bound |
|---|---|---|
| Spark append / Flink SQL | dropped | dropped |
| Flink DataStream | re-fire window (allowedLateness) | side output (kept) |
| Materialize / differential dataflow | retract + update | retract + update (never lost) |
| **Zelox today** | dropped | dropped |
| **Zelox (this design)** | retract + update (changelog) | side output (kept) |

Zelox already carries the primitive nobody else exposes under the Spark API:
`FlowEvent::Data { batch, retracted: BooleanArray }`. The window operator currently hard-codes
`retracted = all-false`. Using it, Zelox can deliver Flink-DataStream correctness +
Materialize convergence through the Spark `outputMode("update")` API — beating both
Spark and Flink-SQL on the correctness axis with zero data loss.

## Semantics (grounded in Flink / RisingWave docs)

- **Watermark delay** (`withWatermark("col", d)`) = bounded out-of-orderness, as today.
- **allowedLateness `L`** (new): keep a closed window's aggregate state until
  `watermark > window_end + L`. Within `[window_end, window_end + L]`, a late record that
  changes an already-emitted window triggers a **changelog update**: emit a retraction of
  the previously-emitted row (`retracted = true`) followed by the new aggregate row
  (`retracted = false`). (Flink "emit on update"; differential-dataflow retract+insert.)
- **Late beyond `L`**: route the record to a **late side output** (Flink `sideOutputLateData`)
  instead of silently dropping. Default sink: a `_late/` sub-path; opt-out drops as today.
- **outputMode**:
  - `append` (default, unchanged) — emit once on close, drop late. Spark-identical.
  - `update` (new) — changelog: each finalize emits changed windows as retract+insert;
    late-in-bound updates converge the result; zero loss within `L`.
  - `complete` — already supported by the non-windowed agg path.

## Status (2026-06-21)

**Operator core: DONE + unit-tested** in `crates/sail-physical-plan/src/streaming/window_accum.rs`:
- `WindowOutputMode {Append, Update}` + `WindowAccumExec::with_output_mode(mode, allowed_lateness)`
  (defaults to `Append` — today's behavior, so `planner.rs`/`codec.rs`/distributed path unchanged).
- `emit_changelog`: per-group-key diff (arrow `RowConverter`), retract stale + insert new, coalesced
  retract-then-insert batches via `FlowEvent.retracted`; state retained until `end + L ≤ wm`.
- Tests (`update_mode_tests`): late-in-bound data (count 5→7) converges via retract(5)+insert(7)
  — **zero loss** where append drops; independent keys tracked separately; idempotent on no-change;
  finalized windows evicted from changelog state. Append path: full suite green, no regression.

**Remaining (honest gaps, scoped):**
1. **User API plumbing** — wire `outputMode("update")` + an `allowedLateness` option from the
   `WriteStream` spec through `write_stream.rs` → `rewrite_streaming_plan` → `WindowAccumNode` →
   `with_output_mode`. Until then update mode is reachable only via the builder (tests).
2. **Distributed codec** — serialize `output_mode`/`allowed_lateness` in `sail-execution/codec.rs`
   so update mode survives local-cluster/distributed planning (today always `Append` over the wire).
3. **Late side output** (beyond `L`) — currently such rows are simply not tracked (dropped like
   append); add the `_late/` side sink.
4. **Checkpoint of `last_emitted`** — update-mode changelog state isn't snapshotted on `EndOfData`
   yet (append's partial-state EO recovery is unaffected).
5. **Sink semantics** — changelog output needs an upsert/changelog-capable sink (Kafka/Delta upsert);
   append-only file sinks can't represent retractions.

## Implementation plan (incremental, low-risk)

1. **Plumb `allowed_lateness_micros` + `output_mode` into `WindowAccumExec`** (planner +
   logical `WindowAccumNode`). `append` keeps today's exact path.
2. **State retention**: change `retain_open_window_rows` to keep partials until
   `end + allowed_lateness ≤ watermark` (not `end ≤ watermark`). Bounds state by `L`.
3. **Changelog emit** (`finalize_and_emit`, update mode): track the last-emitted aggregate
   value per (window,key) (a small `HashMap<row-key, agg-row>`); on re-finalize, for a
   window already emitted whose value changed, push `FlowEvent::Data{retracted=true}` for the
   old value then `retracted=false` for the new. Append mode unchanged (single emit, all-false).
4. **Late side output**: in `window_emit_mask`/pre-agg, partition rows whose
   `event_time < watermark - L` (too late) to a side stream; wire an optional `_late` sink.
5. **Sink retraction handling**: the parquet/file sink already receives `retracted`; ensure
   it applies deletes/updates for changelog sinks (or upsert for Kafka/Delta). For
   append-only file sinks, update mode requires a changelog-capable sink (documented gap).

## Validation (must beat both)

- Out-of-order stream (shuffled event-time, watermark `d`, `L` > out-of-orderness):
  Zelox `update` mode → **0 loss**, final per-group counts exact; Spark append + Flink SQL
  → measurable drop on the same stream. This is the head-to-head that demonstrates "better
  than both" on correctness.
- Append mode regression: identical to today's validated EKS result (every group 10000).
