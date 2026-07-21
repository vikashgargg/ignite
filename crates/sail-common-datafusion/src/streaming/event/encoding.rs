use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    new_null_array, Array, ArrayData, ArrayRef, BinaryArray, BooleanArray, RecordBatch,
};
use datafusion::arrow::buffer::Buffer;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_common::{exec_datafusion_err, exec_err, Result};
use futures::{Stream, TryStreamExt};

use crate::array::placeholder::{placeholder_array, placeholder_boolean_array};
use crate::streaming::event::marker::FlowMarker;
use crate::streaming::event::schema::{to_flow_event_schema, try_from_flow_event_schema};
use crate::streaming::event::stream::{FlowEventStream, SendableFlowEventStream};
use crate::streaming::event::FlowEvent;

/// A stream for encoded flow events.
/// The encoded [`RecordBatch`] has a special schema.
/// The first field contains the encoded marker if not null.
/// The other fields are valid only if the marker is null.
/// The second field is the retraction flag for each data row.
/// For a data event, the marker array only contains the null buffer,
/// which adds 1-bit overhead for each row in a data event.
/// The retracted field adds another 1-bit overhead for each row in a data event.
/// Throughput attribution (env `VAJRA_WM_PROF`): cumulative ns spent in flow-event `encode()` across
/// ALL operator hops (the per-data-batch null-marker-column build). Read by the window operator's prof
/// dump to see the encode's share of the wall. Zero cost when the env var is unset.
pub static ENCODE_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Cumulative ns in `from_json` UDF invoke (the serde_json parse) — attribute the parse share of the
/// streaming throughput gap. Written by sail-function's from_json, read by the window prof dump.
pub static FROM_JSON_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Cumulative ns in the Kafka source read+batch-build loop (across source instances). Written by
/// kafka/reader.rs, read by the window prof dump — the COMPLETE per-stage breakdown for EKS pinpointing.
pub static SOURCE_READ_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// SOURCE_READ split (VAJRA_WM_PROF): time in the rdkafka message drain (`msg_stream.next`) vs the Arrow
/// batch build (`builders.append`) — pinpoints whether source_read is CONSUME-bound or BUILD-bound.
pub static SOURCE_POLL_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SOURCE_BUILD_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Cumulative ns in the keyed shuffle distribute/route+send (across instances). Written by
/// streaming/exchange.rs, read by the window prof dump.
pub static EXCHANGE_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Exchange time SPENT BLOCKED on the bounded send channel (backpressure-wait, NOT route CPU).
pub static EXCHANGE_WAIT_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// DISTRIBUTED shuffle SEND side: cumulative ns the Flight server (stream_service) spends producing +
/// IPC-encoding FlightData batches in `do_get` (serialize + per-batch stream poll). The cross-pod
/// exchange cost that single-node profiling never sees — prime suspect for the distributed throughput gap.
pub static SHUFFLE_SEND_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// DISTRIBUTED shuffle RECV side: cumulative ns the Flight client spends pulling + IPC-decoding each
/// RecordBatch off the wire (network + deserialize) in `fetch_task_stream`.
pub static SHUFFLE_RECV_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// DISTRIBUTED shuffle byte + batch volume across the stage boundary (throughput/serialize denominator).
pub static SHUFFLE_SEND_BATCHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SHUFFLE_RECV_BATCHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// RFC-observability (MEMORY truth): live in-flight Arrow bytes buffered in the exchange sub-channels
/// (sent − received). The 2026-07-01 A/B proved the streaming RSS gap is LIVE BUFFERING not the allocator,
/// so this PEAK gauge directly attributes the realtime memory to the shuffle edge. `EXCHANGE_PEAK_BYTES`
/// is the high-water mark, dumped with the stage report.
pub static EXCHANGE_INFLIGHT_BYTES: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
pub static EXCHANGE_PEAK_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// RFC-observability (MEMORY truth, part 2): live Arrow bytes buffered in the batch-queue READER
/// channels (reader-thread → async generator, depth 8 × N readers). Prime suspect for the 12 GiB:
/// MAX_BATCH_BYTES=128 MiB × depth-8 × 16 readers = up to 16 GiB. `READER_PEAK_BYTES` = high-water.
pub static READER_INFLIGHT_BYTES: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
pub static READER_PEAK_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// Account `signed` bytes into the reader-channel in-flight gauge, tracking the peak (see `inflight_account`).
pub fn reader_inflight_account(signed: i64) {
    use std::sync::atomic::Ordering::Relaxed;
    let cur = READER_INFLIGHT_BYTES.fetch_add(signed, Relaxed) + signed;
    if signed > 0 && cur > 0 {
        let cur_u = cur as u64;
        let mut peak = READER_PEAK_BYTES.load(Relaxed);
        while cur_u > peak {
            match READER_PEAK_BYTES.compare_exchange_weak(peak, cur_u, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }
}
/// KEY-TRACE (env `VAJRA_KEY_TRACE=1`): per-stage cumulative census of the group-key column ("k") to
/// LOCALIZE + CHARACTERIZE the realtime windowed-agg key-corruption bug (keys mis-labelled/dropped while
/// counts stay correct — invisible to aggregate checks). At each pipeline stage we record distinct k,
/// rows, min, max, nulls. The FIRST stage whose distinct_k drops below the source's cardinality is the
/// corrupting operator. `min<0 || max` beyond the key domain ⇒ uninit/OOB garbage; in-domain with
/// distinct<expected ⇒ a take/hash/gather MISLABEL. Zero cost when unset (single OnceLock bool check).
pub fn key_trace_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("VAJRA_KEY_TRACE").as_deref() == Ok("1"))
}
#[derive(Default)]
struct KeyStat {
    distinct: std::collections::HashSet<i64>,
    rows: u64,
    min: i64,
    max: i64,
    nulls: u64,
    seen: bool,
}
fn key_stats() -> &'static std::sync::Mutex<std::collections::BTreeMap<String, KeyStat>> {
    static M: std::sync::OnceLock<std::sync::Mutex<std::collections::BTreeMap<String, KeyStat>>> =
        std::sync::OnceLock::new();
    M.get_or_init(|| std::sync::Mutex::new(std::collections::BTreeMap::new()))
}
/// Periodic KEY_TRACE dumper (every 5s) — realtime mode never emits `EndOfData`, so the census must be
/// flushed on a timer, not only at end-of-stream. Started lazily on the first `key_trace` call.
fn ensure_key_trace_dumper() {
    static STARTED: std::sync::Once = std::sync::Once::new();
    STARTED.call_once(|| {
        let _ = std::thread::Builder::new()
            .name("key-trace-dumper".to_string())
            .spawn(|| loop {
                std::thread::sleep(std::time::Duration::from_secs(5));
                key_trace_dump();
            });
    });
}
/// Census the "k" group-key column of `batch` into the cumulative stats for `tag`. No-op if unset or the
/// batch carries no "k" column (e.g. marker batches). Accepts Int32/Int64.
pub fn key_trace(tag: &str, batch: &datafusion::arrow::record_batch::RecordBatch) {
    if !key_trace_enabled() {
        return;
    }
    ensure_key_trace_dumper();
    use datafusion::arrow::array::{Array, Int32Array, Int64Array};
    let mut g = key_stats().lock().unwrap_or_else(|e| e.into_inner());
    // Census EVERY Int32/Int64 column (robust to whatever the group-key column is named at this stage).
    // Tag = "stage.colname"; the key column is the one with ~1000 distinct that DROPS at the bad stage.
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i);
        let key = format!("{tag}.{}", field.name());
        let mut push = |v: Option<i64>| {
            let s = g.entry(key.clone()).or_default();
            s.rows += 1;
            match v {
                None => s.nulls += 1,
                Some(k) => {
                    if !s.seen {
                        s.min = k;
                        s.max = k;
                        s.seen = true;
                    }
                    s.min = s.min.min(k);
                    s.max = s.max.max(k);
                    // cap the set so a genuinely high-cardinality column can't blow memory
                    if s.distinct.len() < 5000 {
                        s.distinct.insert(k);
                    }
                }
            }
        };
        if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
            for r in 0..a.len() {
                push((!a.is_null(r)).then(|| a.value(r) as i64));
            }
        } else if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            for r in 0..a.len() {
                push((!a.is_null(r)).then(|| a.value(r)));
            }
        }
    }
}
/// Dump the per-stage census (call at EndOfData). Compare `distinct_k` across stages to pinpoint the
/// corrupting operator; `min`/`max`/`nulls` classify the mechanism (garbage vs in-range mislabel).
pub fn key_trace_dump() {
    if !key_trace_enabled() {
        return;
    }
    let g = key_stats().lock().unwrap_or_else(|e| e.into_inner());
    for (tag, s) in g.iter() {
        eprintln!(
            "KEY_TRACE[{tag}] distinct_k={} rows={} min={} max={} nulls={}",
            s.distinct.len(),
            s.rows,
            s.min,
            s.max,
            s.nulls
        );
    }
}

/// Account `bytes` entering (+) / leaving (−) the exchange in-flight buffer, tracking the peak. Cheap
/// (relaxed atomics); gated by the caller to the prof path. `signed` = +bytes on send, −bytes on recv.
pub fn inflight_account(signed: i64) {
    use std::sync::atomic::Ordering::Relaxed;
    let cur = EXCHANGE_INFLIGHT_BYTES.fetch_add(signed, Relaxed) + signed;
    if signed > 0 && cur > 0 {
        let cur_u = cur as u64;
        // monotonic peak update (compare-and-set loop, only grows)
        let mut peak = EXCHANGE_PEAK_BYTES.load(Relaxed);
        while cur_u > peak {
            match EXCHANGE_PEAK_BYTES.compare_exchange_weak(peak, cur_u, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }
}
/// Convenience: add `nanos` to a profiling counter only when profiling is enabled (cheap guard).
pub fn prof_add(counter: &std::sync::atomic::AtomicU64, nanos: u64) {
    counter.fetch_add(nanos, std::sync::atomic::Ordering::Relaxed);
    ensure_process_dumper();
}
/// Shared throughput-profiling gate (env `VAJRA_WM_PROF`), cached. Used across crates.
pub fn wm_prof_enabled() -> bool {
    static E: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *E.get_or_init(|| std::env::var("VAJRA_WM_PROF").is_ok())
}

/// DISTRIBUTED per-process WM_PROF dump. In distributed mode the source / from_json / exchange / window
/// stages run on DIFFERENT worker pods, each with its OWN per-process counters — but only the WindowAccum
/// pod ever dumped (window_accum.rs), so a source or exchange pod's stage time was INVISIBLE (why the last
/// EKS distributed A/B was blind). This spawns ONE background thread per process (first time any counter is
/// touched, gated by VAJRA_WM_PROF) that logs every non-zero counter every 10s + at process end — so
/// `kubectl logs` across ALL pods gives the complete Flink-class per-operator/per-node breakdown.
fn ensure_process_dumper() {
    use std::sync::atomic::Ordering::Relaxed;
    static STARTED: std::sync::Once = std::sync::Once::new();
    if !wm_prof_enabled() {
        return;
    }
    STARTED.call_once(|| {
        std::thread::Builder::new()
            .name("wm-prof-dumper".to_string())
            .spawn(|| {
                let pid = std::process::id();
                let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "?".to_string());
                loop {
                    std::thread::sleep(std::time::Duration::from_secs(10));
                    let vals = [
                        ("source_read", SOURCE_READ_NS.load(Relaxed)),
                        ("source_poll", SOURCE_POLL_NS.load(Relaxed)),
                        ("source_build", SOURCE_BUILD_NS.load(Relaxed)),
                        ("from_json", FROM_JSON_NS.load(Relaxed)),
                        ("exchange_cpu", EXCHANGE_NS.load(Relaxed)),
                        ("exchange_wait", EXCHANGE_WAIT_NS.load(Relaxed)),
                        ("encode", ENCODE_NS.load(Relaxed)),
                        ("shuffle_send", SHUFFLE_SEND_NS.load(Relaxed)),
                        ("shuffle_recv", SHUFFLE_RECV_NS.load(Relaxed)),
                    ];
                    if vals.iter().all(|(_, v)| *v == 0) {
                        continue; // nothing happened on this process yet
                    }
                    let stages = vals
                        .iter()
                        .map(|(k, v)| format!("{k}={}", v / 1_000_000))
                        .collect::<Vec<_>>()
                        .join(" ");
                    let sb = SHUFFLE_SEND_BATCHES.load(Relaxed);
                    let rb = SHUFFLE_RECV_BATCHES.load(Relaxed);
                    let peak_mib = EXCHANGE_PEAK_BYTES.load(Relaxed) / 1048576;
                    // stderr (not log::) so it is captured by `kubectl logs` on EVERY pod regardless of
                    // that pod's RUST_LOG — this diagnostic is gated solely by VAJRA_WM_PROF.
                    eprintln!(
                        "WM_PROF_PROC[pid={pid} host={host}] STAGES(cpu-ms): {stages} \
                         | shuffle_send_batches={sb} shuffle_recv_batches={rb} \
                         | exchange_peak_inflight_MiB={peak_mib}"
                    );
                }
            })
            .ok();
    });
}
fn encode_prof_enabled() -> bool {
    wm_prof_enabled()
}

pub struct EncodedFlowEventStream {
    inner: SendableFlowEventStream,
    schema: SchemaRef,
    /// Cached all-null Binary marker array, grown on demand and sliced per data batch (EPIC-T/T3
    /// structural throughput). `new_null_array(Binary, n)` allocates an (n+1)-element offsets buffer
    /// EVERY data batch at EVERY operator boundary (~6/batch); a cached array sliced to `n` shares
    /// buffers (O(1)) — the alloc happens only when the batch grows past the cache.
    null_marker: Option<ArrayRef>,
}

impl EncodedFlowEventStream {
    pub fn new(stream: SendableFlowEventStream) -> Self {
        let schema = to_flow_event_schema(&stream.schema());
        Self {
            inner: stream,
            schema: Arc::new(schema),
            null_marker: None,
        }
    }

    pub fn encode(&mut self, event: FlowEvent) -> Result<RecordBatch> {
        let _t = encode_prof_enabled().then(std::time::Instant::now);
        let out = self.encode_inner(event);
        if let Some(t) = _t {
            ENCODE_NS.fetch_add(
                t.elapsed().as_nanos() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        out
    }

    fn encode_inner(&mut self, event: FlowEvent) -> Result<RecordBatch> {
        let columns = match event {
            FlowEvent::Data { batch, retracted } => {
                let n = batch.num_rows();
                // Reuse a cached all-null marker array, sliced to `n` (O(1), shares buffers) instead of
                // allocating a fresh (n+1) offsets buffer per batch. Grow (rounded to ≥8Ki) on demand.
                if self.null_marker.as_ref().is_none_or(|a| a.len() < n) {
                    self.null_marker = Some(new_null_array(&DataType::Binary, n.max(8192)));
                }
                let marker = match &self.null_marker {
                    Some(a) => a.slice(0, n),
                    None => new_null_array(&DataType::Binary, n),
                };
                let mut columns: Vec<ArrayRef> = vec![marker, Arc::new(retracted)];
                columns.extend(batch.columns().iter().cloned());
                columns
            }
            FlowEvent::Marker(marker) => {
                let marker = {
                    let values = marker.encode()?;
                    let offsets = vec![0, values.len() as i32];
                    let array_data = ArrayData::builder(DataType::Binary)
                        .len(1)
                        .add_buffer(Buffer::from(offsets))
                        .add_buffer(Buffer::from(values))
                        .build()?;
                    Arc::new(BinaryArray::from(array_data))
                };
                let retracted = placeholder_boolean_array(1);
                let mut columns: Vec<ArrayRef> = vec![marker, retracted];
                for field in self.inner.schema().fields() {
                    if field.is_nullable() {
                        columns.push(new_null_array(field.data_type(), 1));
                    } else {
                        columns.push(placeholder_array(field.data_type(), 1)?);
                    }
                }
                columns
            }
        };
        Ok(RecordBatch::try_new(self.schema.clone(), columns)?)
    }
}

impl RecordBatchStream for EncodedFlowEventStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for EncodedFlowEventStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll_next(cx)
            .map(|x| x.map(|x| x.and_then(|x| this.encode(x))))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// An internal helper stream to decode flow events from encoded [`RecordBatch`].
/// Since a single [`RecordBatch`] may contain multiple events, a user-facing
/// stream should be created by flattening this stream.
struct DecodedMultiFlowEventStream {
    inner: SendableRecordBatchStream,
    /// The schema of the data batches for the decoded events.
    schema: SchemaRef,
}

impl DecodedMultiFlowEventStream {
    fn try_new(stream: SendableRecordBatchStream) -> Result<Self> {
        let schema = try_from_flow_event_schema(&stream.schema())?;
        Ok(Self {
            inner: stream,
            schema: Arc::new(schema),
        })
    }

    fn get_special_columns_and_data<'a>(
        &self,
        batch: &'a RecordBatch,
    ) -> Result<(&'a BinaryArray, &'a BooleanArray, RecordBatch)> {
        let mut columns = batch.columns().iter();
        let Some(marker) = columns.next() else {
            return exec_err!("missing marker array");
        };
        let marker = marker
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| exec_datafusion_err!("invalid marker array type"))?;
        let Some(retracted) = columns.next() else {
            return exec_err!("missing retracted array");
        };
        let retracted = retracted
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| exec_datafusion_err!("invalid retracted array type"))?;
        let data = RecordBatch::try_new(self.schema.clone(), columns.cloned().collect())?;
        Ok((marker, retracted, data))
    }

    fn decode(&self, batch: RecordBatch) -> Result<Vec<FlowEvent>> {
        // We slice the batch rows into either a single marker row,
        // or continuous non-marker rows (data rows).
        let mut events = vec![];
        let (marker, retracted, data) = self.get_special_columns_and_data(&batch)?;
        // FAST PATH (EPIC-T/T3, Flink-chaining analog): an all-data batch (no marker rows — the
        // overwhelmingly common case, markers are rare) is exactly ONE Data event. Skip the
        // O(num_rows) per-row marker-validity scan below — pure structural overhead paid at every
        // one of ~6 operator boundaries per batch.
        if batch.num_rows() > 0 && marker.null_count() == batch.num_rows() {
            return Ok(vec![FlowEvent::Data {
                batch: data,
                retracted: retracted.clone(),
            }]);
        }
        let mut start_data_index = None;
        for i in 0..batch.num_rows() {
            if marker.is_valid(i) {
                // flush the data rows before the marker
                if let Some(start) = start_data_index {
                    let length = i - start;
                    events.push(FlowEvent::Data {
                        batch: data.slice(start, length),
                        retracted: retracted.slice(start, length),
                    });
                    start_data_index = None;
                }
                let marker = FlowMarker::decode(marker.value(i))?;
                events.push(FlowEvent::Marker(marker));
            } else if start_data_index.is_none() {
                start_data_index = Some(i);
            }
        }
        // flush the remaining data rows
        if let Some(start) = start_data_index {
            let length = batch.num_rows() - start;
            events.push(FlowEvent::Data {
                batch: data.slice(start, length),
                retracted: retracted.slice(start, length),
            });
        }
        Ok(events)
    }
}

impl Stream for DecodedMultiFlowEventStream {
    type Item = Result<Vec<FlowEvent>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.inner)
            .poll_next(cx)
            .map(|x| x.map(|x| x.and_then(|x| this.decode(x))))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// A record batch stream for decoded flow events.
pub struct DecodedFlowEventStream {
    inner: Pin<Box<dyn Stream<Item = Result<FlowEvent>> + Send>>,
    schema: SchemaRef,
}

impl DecodedFlowEventStream {
    pub fn try_new(stream: SendableRecordBatchStream) -> Result<Self> {
        let inner = DecodedMultiFlowEventStream::try_new(stream)?;
        let schema = inner.schema.clone();
        let inner = inner
            .map_ok(|events| futures::stream::iter(events.into_iter().map(Ok)))
            .try_flatten();
        Ok(Self {
            inner: Box::pin(inner),
            schema,
        })
    }
}

impl Stream for DecodedFlowEventStream {
    type Item = Result<FlowEvent>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.inner.as_mut().poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl FlowEventStream for DecodedFlowEventStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
