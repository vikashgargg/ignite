# Design: marker-based watermarks + watermark-driven windowed aggregation

Status: **design (ready to build)**. Chosen for long-term scalability (matches Flink:
watermarks as in-band stream events; multi-input operators take the min). Replaces the
fragile "read raw event-time column in `WindowAccumExec`" path, which breaks because the
optimizer folds `window(timestamp)` into a projection and drops the raw column (see
[STREAMING_LATENCY.md](../benchmarks/STREAMING_LATENCY.md)).

## The coupled pieces (must land together)
1. **`WatermarkExec` (new physical operator).** Today `WatermarkNode` is a logical
   passthrough (planner.rs:325). Make the planner build a real `WatermarkExec` that:
   - decodes its flow-event input (`DecodedFlowEventStream`); it sits **below** the
     window-folding projection, so the raw event-time column is still present;
   - tracks `max(event_time)`; computes `watermark = max − delay_micros`;
   - passes data through unchanged, and **emits `FlowMarker::Watermark{timestamp}` only
     when the watermark advances** (monotonic, low overhead);
   - re-encodes (`FlowEventStreamAdapter` + `EncodedFlowEventStream`).
   Model on `StreamSourceAdapterExec`. Read event-time via `index_of(event_time_col)` +
   `TimestampMicrosecondArray` + `compute::max` (as `AccumState::push` does today).
   If the column is absent, pass through without emitting (graceful).
2. **`WindowAccumExec` — consume watermarks, evict on advance (the hard part).**
   - Drop the `col_idx` dependency (the bug source).
   - On `FlowMarker::Watermark{ts}`: set `watermark = ts`, then **evict**: re-aggregate
     pending rows, emit windows with `end ≤ watermark` **exactly once**, and **drop the
     rows belonging to emitted (closed) windows** so pending state stays bounded and
     windows don't re-emit. (Today it re-aggregates on `Checkpoint` — which is never
     emitted in continuous mode — and never evicts → unbounded + re-emits.)
   - Retention: keep only rows whose window `end > watermark` (still open). Late rows
     for already-closed windows are dropped (Spark default; honor `delay_micros` via the
     watermark already including delay).
3. **Sinks/operators skip marker batches.** Any continuous sink that writes data
   (`MemorySinkExec`, console, file) must **not** write `Watermark`/marker batches as
   data rows (marker col non-null, data null). Add the marker-skip in the sink loops
   (the same check prototyped earlier for `LatencyTracker`). Otherwise emitting watermark
   markers corrupts sink output with null rows.
4. **Planner + rewriter wiring.** planner.rs: `WatermarkNode → WatermarkExec` (not
   passthrough). Rewriter: `WindowAccumNode` no longer needs `event_time_col` for
   reading (watermark arrives via markers); keep delay handling in `WatermarkExec`.

## Multi-input min-merge (future, for stream-stream joins)
When an operator has >1 streaming input, its watermark = **min** of per-input
watermarks (Flink semantics). Not needed for single-input windowed aggregation; leave a
documented hook in the watermark-consuming logic so joins can plug in.

## Correctness tests (write FIRST — this is why it's not a rushed patch)
- Tumbling-window count over a rate stream: each window emits **once**, with the
  **correct count** (rows whose event-time ∈ window).
- Window emits only **after** the watermark passes its end (not before).
- **Late data** for a closed window is dropped (or counted if within `delay`).
- Pending-row state stays **bounded** over a long run (no leak).
- Regression: stateless continuous, bounded `availableNow`, and the latency path
  (LatencyTracker) all still work; bounded aggregation unaffected.

## Files
- new `crates/sail-physical-plan/src/streaming/watermark.rs` (`WatermarkExec`)
- `crates/sail-session/src/planner.rs` (`WatermarkNode` → `WatermarkExec`; ~line 324)
- `crates/sail-physical-plan/src/streaming/window_accum.rs` (`AccumState` + eviction)
- `crates/sail-logical-plan/src/streaming/watermark.rs` (`WatermarkNode` — expose fields)
- `crates/sail-session/src/memory_sink_exec.rs` (skip marker batches) + other sinks
- `crates/sail-plan/src/streaming/rewriter.rs` (WindowAccumNode event-time handling)

`FlowMarker::Watermark` already exists (encode/decode) — like `LatencyTracker`, it's
defined but unwired; this completes the engine's intended marker-based design.
