# Zelox watermark-propagation contract (the grounded design)

Status: **implemented + enforced** (supersedes the original June "ready to build" note). This is the
single authoritative contract for how event-time watermarks are generated, propagated, and consumed
across `source → WatermarkExec → StreamExchange → WindowAccum`. Every piece below is an *enforcement
point of one invariant*, grounded in the canonical streaming protocols — not an independent patch.

## Sources (canonical, cited)
- **Flink** — watermarks are in-band stream records; a multi-input operator's watermark = **MIN** over
  its input channels; `WatermarkStatus.IDLE` excludes a genuinely-caught-up channel from that MIN; a
  **status transition flushes the generator** (a source going idle emits its current watermark; end-of-input
  emits `MAX_WATERMARK` to flush all event-time timers). Periodic emission (`pipeline.auto-watermark-interval`,
  default 200ms) is a *steady-state throughput optimization only*. [REFERENCES §2, §2d]
- **RisingWave 3.0 / Arroyo 0.15** — barrier/watermark flows per-channel FIFO; a watermark on a channel
  implies all lower-timestamp data on that channel already passed → the receiver can safely close windows
  ≤ the merged watermark. [REFERENCES §9]

## THE INVARIANT (one line)
> A watermark value `W` observed at an operator input means **no record with event-time ≤ W will ever
> arrive later on any *active* channel.** Therefore any window with `end ≤ W` is complete and may be
> emitted exactly once. Idle channels (caught up to their partition high-watermark) are excluded from the
> MIN; a channel's *last* watermark before it goes idle must be its **true max** (this is what makes the
> all-idle drain sound — see E2/E3).

## Enforcement points (each upholds the invariant; none is standalone)

**E1 — Generation (`WatermarkExec`).** Tracks `max(event_time)` (globally, or per source `partition`
when a partition column is preserved) and emits `FlowMarker::Watermark = max − delay`, monotonically.
Emission is gated to `watermark_interval` (200ms, Flink auto-watermark-interval) in steady state.

**E2 — Flush-before-transition (`WatermarkExec`, THE fix that closed the fast-backlog gap).** The
interval gate must **never** defer a watermark *across a status transition*. Before forwarding an `Idle`
or `EndOfData` marker, `WatermarkExec` flushes its current max watermark (bypassing the gate; monotonic,
so a no-op if already emitted). Without this, a fast backlog whose final records (e.g. a closer advancing
past the last window) are consumed and immediately followed by `Idle` — all inside one 200ms interval —
strands that final watermark: the channel goes idle carrying a *stale* watermark, and E3's all-idle drain
uses the stale value → the last window never closes. This is Flink's "flush generator on status change /
`MAX_WATERMARK` on end-of-input". Regression test: `flushes_pending_watermark_before_idle`
(watermark.rs). Measured: kind (paced, gaps > 200ms) never hit it; EKS 100M backlog did (9/10 windows).

**E3 — Multi-input MIN-merge + idle (`StreamExchange::merge_output_subchannels`).** The window task's
watermark = MIN over channels that are neither ENDED nor IDLE. If any active channel has not yet reported
a watermark, HOLD (never skip a slow-but-active channel — that closes a window early = partial-count
split). A channel is IDLE **only** on a source-signaled `Idle` marker (E4), not a wall-clock gap. When
ALL non-ended channels are idle, advance to the MAX of their last watermarks — sound **because** E2
guarantees each idle channel's last watermark is its true max (caught up to high-watermark).

**E4 — Idle definition (Kafka reader).** A reader emits `Idle` **only** when `next_offset ≥ partition
high-watermark` (fetched live), i.e. genuinely drained — never on a transient empty poll. This is Flink
`WatermarkStatus.IDLE` on the correct (offset-based, not wall-clock) condition. It is what lets E3 exclude
a caught-up channel without closing windows early.

**E5 — Window close-once (`WindowAccum`).** On `Watermark{ts}`: emit every window with `end ≤ ts` exactly
once (tracked in `emitted_ends`), then drop its rows (bounded state). `restore_wm_floor` suppresses
re-firing windows already committed before a crash-restore (emitted_ends may have been pruned). On
`EndOfData` (bounded / `availableNow`): flush ALL open windows (= Flink end-of-input `MAX_WATERMARK`).

## Known consolidation debt (tracked, not silent)
The realtime path currently runs **two** idleness mechanisms: E4's source-signaled `Idle` (grounded) and
a **wall-clock per-partition idle** inside `WatermarkExec` (`active_partition_watermark`, wired via
`preserve_partition`). E4 is the canonical one; the wall-clock path predates it and is the original
"returns None on empty → watermark stalls" hazard. Target end-state = **E4 only** (single idle
definition), removing the wall-clock path — to be done with the crash-EO + completeness suite green, not
as a blind rip-out. Until then this doc is the contract; the wall-clock path must not *contradict* E1–E5.

## Correctness tests (the invariant, not incidental)
- `flushes_pending_watermark_before_idle` — E2: final watermark flushed before `Idle`.
- Tumbling count over a paced stream: each window emits once, correct count, only after `watermark ≥ end`.
- Fast backlog + tail closer: all windows close (E2+E3+E4 together) — EKS 100M = 10/10.
- Late data for a closed window dropped; pending state bounded over a long run.
- Crash-restore: no window re-fires below `restore_wm_floor` (E5).

## Files
- `crates/zelox-physical-plan/src/streaming/watermark.rs` — E1, E2 (`WatermarkExec`).
- `crates/zelox-physical-plan/src/streaming/exchange.rs` — E3 (`merge_output_subchannels`).
- `crates/zelox-data-source/src/formats/kafka/reader.rs` — E4 (source `Idle`).
- `crates/zelox-physical-plan/src/streaming/window_accum.rs` — E5 (close-once + EndOfData flush).
- `crates/zelox-plan/src/streaming/rewriter.rs` — wiring (`preserve_partition` → per-partition E1).
