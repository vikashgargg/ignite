# Design: Kafka sink + record-paced low-latency emission (P0 — close the Flink latency gap)

> Status: **✅ IMPLEMENTED + MEASURED-WON (latency pillar).** `KafkaSinkExec`
> (`crates/sail-data-source/src/formats/kafka/sink.rs`) = record-paced per-row produce +
> transactional EO with per-task `transactional.id` (Flink KafkaSink FLIP-143 parity), wrapped
> N-way (no funnel) in `ParallelStreamSinkExec`.
>
> **T2/kind FAIR head-to-head (2026-07-08, parallelism=2 both, Kafka→passthrough→Kafka):**
> | | p50 | p99 | max |
> |---|---|---|---|
> | **Zelox** | **30 ms** | **125 ms** | **128 ms** |
> | Flink 1.19 | 42 ms | 580 ms | 767 ms |
>
> **Zelox WINS every percentile, and the TAIL by 4.6–6×.** MECHANISM (grounded): Flink's p99/max
> spikes (580/767 ms) are **JVM stop-the-world GC pauses** — the canonical JVM streaming tail-latency
> problem (REFERENCES: no-GC / off-heap-state). Zelox is Rust, **no GC** → the tail stays FLAT
> (125→128 ms, p99≈max). This is the no-JVM advantage made measurable — the axis where "single binary,
> no JVM" shows up exactly as the charter predicts. The old "p50 ~30 s" below = the pre-sink per-epoch
> **file** probe (superseded); the old EKS "p50 257 ms" = the pre-parallel-sink 1/16-partition bug
> (fixed, commit 1d096c8f). REMAINING prod-grade confirm: sustained **100k ev/s** p99<100 ms + EO-chaos
> (acceptance criteria below) on kind/EKS — the win is proven at 5k ev/s; validate it holds under load.

## Grounding (how the proven systems do it)
- **Spark Structured Streaming Kafka sink** (`KafkaStreamWriter`/`KafkaStreamDataWriter`):
  each row maps to a Kafka record via reserved columns — `topic` (optional; else the `topic`
  option), `key` (optional, binary/string), `value` (required), `partition`, `headers`. Default
  delivery = **at-least-once** (producer flush per epoch); EO needs an idempotent/transactional
  producer. https://www.databricks.com/blog/introducing-real-time-mode-apache-sparktm-structured-streaming
  (Real-Time Mode = long-running stages → records emitted on arrival, sub-second p99.)
- **Flink `KafkaSink`** (FLIP-143): `DeliveryGuarantee` = `NONE` | `AT_LEAST_ONCE` (flush on
  checkpoint) | `EXACTLY_ONCE` (Kafka **transactions** begun per checkpoint, committed on
  checkpoint-complete via 2PC; `transactional.id` per subtask, `transaction.timeout.ms` ≥
  checkpoint interval). https://flink.apache.org
- **Arrow/rdkafka:** `rdkafka::producer::FutureProducer`/`ThreadedProducer`; per-record
  `produce()` is fire-and-forget into librdkafka's queue (low latency); `flush()`/poll drains
  delivery reports. Transactions: `init_transactions` / `begin_transaction` /
  `commit_transaction` / `abort_transaction`.

## Zelox integration points (mapped against the current code)
- **Source of truth for the flow-event sink pattern:** `RealtimeFileSinkExec`
  (`crates/sail-data-source/src/streaming_decode.rs`) — consumes the **flow-event** input
  (not decoded), reads `Checkpoint{epoch}` barriers in-band to delimit epochs, accumulates an
  epoch's data, and commits per epoch. The Kafka sink is the same shape with Kafka produce +
  flush/commit replacing the Parquet+metadata commit.
- **Dispatch:** mirror the realtime file sink path in `listing/source.rs::create_writer`
  (which already threads `STREAM_REALTIME_INTERVAL_OPTION`, checkpoint location, and N-way
  parallel sinks via `ParallelStreamSinkExec`). Add a `"kafka"` sink branch → `KafkaSinkExec`.
  `writeStream.format("kafka")` resolves through `write_stream.rs` like other formats.
- **Codec:** add `KafkaSinkExecNode` to `proto/sail/plan/physical.proto` + encode/decode arms
  in `crates/sail-execution/src/codec.rs` (mirror `RealtimeFileSinkExec`'s arms) so it survives
  the driver→worker boundary in distributed mode.

## `KafkaSinkExec` — operator design
Fields: `input` (flow-event plan), `bootstrap_servers`, `topic` (option default), optional
`key_col`/`value_col` names (default: `value` required, `key` if present; else cast the single
output column to value), `checkpoint_location`, `delivery: AtLeastOnce | ExactlyOnce`,
`partition_index`/`num_partitions` (no-funnel parallel, like the file sink),
`tx_id_prefix` (EO).

`execute()` (async_stream over the decoded flow-event input):
1. Build one `FutureProducer` per task. EO: set `transactional.id = "{prefix}-{partition_index}"`,
   `enable.idempotence=true`, `transaction.timeout.ms ≥ commit_interval`; `init_transactions()`.
2. **Per data row → `produce()`** immediately (record-paced; low latency — this is the whole
   point). Map columns: value (required), key (optional). For EO, produce inside the open txn.
3. On **`Checkpoint{epoch}`** marker (realtime) or **EndOfData** (micro-batch): `flush()` the
   producer (AT_LEAST_ONCE) or `commit_transaction()` then `begin_transaction()` (EXACTLY_ONCE).
   Tie the txn commit to the source's per-epoch staged offsets (the same `realtime/committed`
   object the file sink writes) so offset-commit and Kafka-commit are one atomic step → EO.
4. Watermark/other markers pass through; emit an empty/marker output stream (sink has no data
   output), matching `RealtimeFileSinkExec`.

Delivery default = **AT_LEAST_ONCE** (Spark/Flink default); **EXACTLY_ONCE** opt-in via
transactions. Back-pressure: bound the in-flight produce queue (`queue.buffering.max.*`);
on `BufferError` poll/await (already the pattern in the producer harnesses).

## Record-paced realtime emission (the latency win)
Today `StreamDriver::Realtime` commits per epoch and the only sink is per-epoch file. With the
Kafka sink producing **on arrival** (step 2), end-to-end latency becomes
`produce → librdkafka send` (sub-ms to single-digit ms), decoupled from the epoch *commit*
cadence (which only governs durability/EO, off the record path — already the realtime design
intent). Also: honor `Trigger.ProcessingTime` intervals for predictable micro-batch latency.

## Acceptance criteria (how we prove it)
- **Functional:** Kafka→Zelox→Kafka passthrough; consumer reads the output topic; values match
  input 1:1 (no loss/dup at AT_LEAST_ONCE allows dup → assert ⊇; at EXACTLY_ONCE assert ==).
- **Latency:** sustained 100k ev/s, embed produce-wall-ms in each record, measure
  `consume_ms − produce_ms` on the output topic → **p99 < 100 ms** (vs the current ~30 s
  file-sink probe). Compare to Flink `KafkaSink` AT_LEAST_ONCE at the same rate.
- **EO chaos:** EXACTLY_ONCE mode, kill mid-stream, restart → output topic has each input
  exactly once (transactional commit tied to offset commit).
- **Distributed:** N-way parallel Kafka sink (no funnel), codec round-trips, runs local-cluster.

## Build order (each step compiles + is testable)
1. `KafkaSinkExec` module (produce + at-least-once flush-per-epoch/EndOfData) + unit test.
2. `create_writer` `"kafka"` branch + `write_stream.rs` plumbing of bootstrap/topic/key/value.
3. Codec (`KafkaSinkExecNode`) for distributed.
4. EXACTLY_ONCE transactions tied to the per-epoch offset commit.
5. Validate on EKS: Kafka→Kafka latency (p99<100 ms) + EO chaos.
