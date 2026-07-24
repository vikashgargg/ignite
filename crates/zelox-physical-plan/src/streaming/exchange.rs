//! `StreamExchangeExec` — the keyed, marker-aware streaming exchange.
//!
//! Repartitions a single-partition flow-event stream into N output partitions for
//! **intra-node streaming parallelism** (docs/design/streaming-parallelism.md, Phase 2):
//! - **Data** rows are routed by `hash(key) % N` so each downstream stateful operator
//!   instance owns a disjoint key subset (Spark/Flink keyed-stream semantics).
//! - **Markers** (watermark / checkpoint / latency / end-of-data) are **broadcast** to
//!   *every* output partition — they are control-plane events every instance must see.
//!
//! Channels are bounded (backpressure → preserves the memory bound). A single distributor
//! task consumes the input once and fans out, mirroring DataFusion `RepartitionExec` but
//! with marker broadcast (which `RepartitionExec` cannot express).

use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, BinaryArray, RecordBatch, UInt32Array};
use datafusion::arrow::compute::{take, BatchCoalescer};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExprRef};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_datafusion_err, internal_err, Result};
use futures::StreamExt;
use zelox_common_datafusion::streaming::event::marker::FlowMarker;
use zelox_common_datafusion::streaming::event::schema::MARKER_FIELD_NAME;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

/// Bounded channel depth per output partition = the streaming BACKPRESSURE point. Lower depth = tighter
/// backpressure = less in-flight memory (DataFusion does NOT account intermediate stream buffers, so this
/// is a primary streaming-RSS lever; the measured EKS 1.20x-vs-Flink memory gap is live in-flight buffering
/// across up to N×M sub-channels). Tunable via `ZELOX_EXCHANGE_CHANNEL_CAP` (default 16). The mpsc send
/// awaits when full, so a smaller cap bounds in-flight without dropping data (Flink FLIP-2 credit-flow
/// analog, coarser). REFERENCES §8 / docs/design/streaming-memory-boundedness.md.
fn channel_capacity() -> usize {
    static CAP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("ZELOX_EXCHANGE_CHANNEL_CAP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(16)
    })
}

/// Flink `WatermarkStrategy.withIdleness` timeout for the REALTIME (unbounded) N→M watermark merge: a
/// sub-channel that produces nothing for this long is EXCLUDED from the watermark MIN so a partition
/// that goes idle (its Kafka partition drained, no `EndOfData` in continuous mode) never holds the MIN
/// back and the final windows still close. Only applied on the unbounded/multi-channel path (bounded
/// channels END when drained, so they need no idleness — the exact MIN is preserved there). A
/// slow-but-ACTIVE channel is not excluded (its last activity is recent), so idleness never closes a
/// window early. Tunable via `ZELOX_RT_IDLE_MS` (default 500). REFERENCES §2; docs/design/
/// streaming-per-partition-watermark.md (idle exclusion = liveness vs completeness).
fn realtime_idle_timeout() -> std::time::Duration {
    static MS: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let ms = *MS.get_or_init(|| {
        std::env::var("ZELOX_RT_IDLE_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(500)
    });
    std::time::Duration::from_millis(ms)
}

type BatchResult = Result<RecordBatch>;
/// Per-output-partition sub-receivers, taken by `execute`; lazily initialized on first call.
/// Outer index = output partition; inner = one sub-channel per INPUT partition (length 1 for the
/// legacy 1→N path, N for the N→M keyed shuffle). `Option` so each is taken exactly once.
type SharedReceivers = Arc<Mutex<Option<Vec<Vec<Option<Receiver<BatchResult>>>>>>>;

#[derive(Debug)]
pub struct StreamExchangeExec {
    input: Arc<dyn ExecutionPlan>,
    /// Hash-key expressions, evaluated against the (flow-event) input schema.
    hash_keys: Vec<PhysicalExprRef>,
    partition_count: usize,
    /// Lazily-initialized receivers, one per output partition (taken by `execute`).
    state: SharedReceivers,
    properties: Arc<PlanProperties>,
}

impl StreamExchangeExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        hash_keys: Vec<PhysicalExprRef>,
        partition_count: usize,
    ) -> Result<Self> {
        if partition_count == 0 {
            return internal_err!("StreamExchangeExec requires partition_count >= 1");
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::Hash(hash_keys.clone(), partition_count),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Ok(Self {
            input,
            hash_keys,
            partition_count,
            state: Arc::new(Mutex::new(None)),
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn hash_keys(&self) -> &[PhysicalExprRef] {
        &self.hash_keys
    }

    pub fn partition_count(&self) -> usize {
        self.partition_count
    }
}

impl DisplayAs for StreamExchangeExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "StreamExchangeExec: partitions={}, keys={}",
            self.partition_count,
            self.hash_keys.len()
        )
    }
}

impl ExecutionPlan for StreamExchangeExec {
    fn name(&self) -> &str {
        "StreamExchangeExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("StreamExchangeExec requires exactly one child");
        }
        Ok(Arc::new(StreamExchangeExec::try_new(
            children.remove(0),
            self.hash_keys.clone(),
            self.partition_count,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.partition_count {
            return internal_err!(
                "StreamExchangeExec: invalid partition {partition} (have {})",
                self.partition_count
            );
        }
        let schema = self.input.schema();
        let m = self.partition_count;
        let n_in = self
            .input
            .properties()
            .output_partitioning()
            .partition_count();
        // Lazily start the distributor(s) on the first `execute` call and stash, per output
        // partition, its sub-receiver(s); subsequent calls just take theirs.
        let mut guard = self
            .state
            .lock()
            .map_err(|e| exec_datafusion_err!("StreamExchangeExec state lock poisoned: {e}"))?;
        if guard.is_none() {
            // Per output m, a sub-channel from each input. 1→N: one distributor reads input 0 and
            // each output has ONE sub-channel. N→M: one sender PER input hashes its rows in
            // parallel into per-output sub-channels (Flink keyBy: each upstream subtask hash-routes
            // its own output), and each output merges its N sub-channels (watermark MIN at the
            // receiver — Flink's "min across input channels").
            let mut out_subs: Vec<Vec<Option<Receiver<BatchResult>>>> =
                (0..m).map(|_| Vec::with_capacity(n_in.max(1))).collect();
            if n_in <= 1 {
                let mut senders: Vec<Sender<BatchResult>> = Vec::with_capacity(m);
                for slot in out_subs.iter_mut() {
                    let (tx, rx) = channel::<BatchResult>(channel_capacity());
                    senders.push(tx);
                    slot.push(Some(rx));
                }
                let input_stream = self.input.execute(0, context.clone())?;
                tokio::spawn(distribute(input_stream, senders, self.hash_keys.clone(), m));
            } else {
                for i in 0..n_in {
                    let mut senders: Vec<Sender<BatchResult>> = Vec::with_capacity(m);
                    for slot in out_subs.iter_mut() {
                        let (tx, rx) = channel::<BatchResult>(channel_capacity());
                        senders.push(tx);
                        slot.push(Some(rx));
                    }
                    let input_stream = self.input.execute(i, context.clone())?;
                    tokio::spawn(distribute(input_stream, senders, self.hash_keys.clone(), m));
                }
            }
            *guard = Some(out_subs);
        }
        let subs: Vec<Receiver<BatchResult>> = guard
            .as_mut()
            .and_then(|rs| rs.get_mut(partition))
            .map(|slots| slots.iter_mut().filter_map(|s| s.take()).collect())
            .filter(|v: &Vec<_>| !v.is_empty())
            .ok_or_else(|| {
                exec_datafusion_err!("StreamExchangeExec: partition {partition} already taken")
            })?;
        drop(guard);
        if subs.len() == 1 {
            // 1→N: single sub-channel already carries broadcast markers — pass through unchanged.
            let rx = subs
                .into_iter()
                .next()
                .ok_or_else(|| exec_datafusion_err!("StreamExchangeExec: missing sub-channel"))?;
            let stream = ReceiverStream::new(rx);
            return Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));
        }
        // N→M: merge this output's N sub-channels — yield data, MIN-merge watermarks across the
        // N input channels (Flink receiver rule), forward one EndOfData once all N inputs end.
        // Realtime (unbounded) path gets Flink `withIdleness` so a drained (idle, never-EndOfData)
        // partition doesn't hold the watermark MIN back — bounded path keeps the exact MIN (channels
        // END when drained). This closes the multi-partition continuous "last-window edge".
        let idle = matches!(
            self.properties().boundedness,
            datafusion::physical_plan::execution_plan::Boundedness::Unbounded { .. }
        )
        .then(realtime_idle_timeout);
        let stream = merge_output_subchannels(subs, idle);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Fan-in (N→1) merge of flow-event partitions for the streaming sink, the symmetric
/// partner of `StreamExchangeExec`. It drains **all** input partitions concurrently (one
/// tokio task each) into a shared bounded channel, so it cannot deadlock against the
/// exchange's bounded per-partition channels (the cause of the earlier 0-output: a generic
/// coalesce that didn't pull every partition left the exchange's broadcast blocked). The
/// merged stream ends once every input partition is exhausted (all-N `EndOfData`). Markers
/// flow through as ordinary flow-event batches; the sink skips them.
#[derive(Debug)]
pub struct StreamCoalesceExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl StreamCoalesceExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(1),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Self { input, properties }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for StreamCoalesceExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "StreamCoalesceExec")
    }
}

impl ExecutionPlan for StreamCoalesceExec {
    fn name(&self) -> &str {
        "StreamCoalesceExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("StreamCoalesceExec requires exactly one child");
        }
        Ok(Arc::new(StreamCoalesceExec::new(children.remove(0))))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("StreamCoalesceExec: invalid partition {partition}");
        }
        let n = self
            .input
            .properties()
            .output_partitioning()
            .partition_count();
        let schema = self.input.schema();
        let (tx, rx) = channel::<BatchResult>(channel_capacity().max(n));
        for i in 0..n {
            let mut stream = self.input.execute(i, context.clone())?;
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Some(item) = stream.next().await {
                    if tx.send(item).await.is_err() {
                        break; // consumer dropped
                    }
                }
            });
        }
        drop(tx); // the receiver ends once all N producer tasks finish
        let stream = ReceiverStream::new(rx);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Is this a marker batch (the `_marker` column has any non-null entry)? Marker batches are
/// broadcast; data batches are hash-routed.
fn is_marker_batch(batch: &RecordBatch) -> bool {
    if let Ok(idx) = batch.schema().index_of(MARKER_FIELD_NAME) {
        if let Some(m) = batch.column(idx).as_any().downcast_ref::<BinaryArray>() {
            return m.null_count() < m.len();
        }
    }
    false
}

/// Consume the input once, routing data by hash (via DataFusion's `BatchPartitioner`, the
/// same hashing as `RepartitionExec`) and broadcasting markers to all outputs.
async fn distribute(
    mut input: SendableRecordBatchStream,
    senders: Vec<Sender<BatchResult>>,
    hash_keys: Vec<PhysicalExprRef>,
    n: usize,
) {
    // Route by KEY-GROUP (rescale-stable): hash keys into a fixed `g` key-groups, then map each
    // key-group to its owning output instance via `key_group_owner` — the SAME math as the rescale
    // state ownership (`instance_key_group_range`), so a key always lands on the instance that owns its
    // state at any parallelism. (Plain `hash % n` is not rescale-stable.) `g >= n` recommended.
    // Routing uses `zelox_key_groups` (PROVEN to match `BatchPartitioner` hashing) so we can `take`
    // ONCE per owning instance, instead of a 128-way `BatchPartitioner` split + `concat_batches`
    // re-merge into n instances — i.e. one copy pass over the rows, not two (VAJ-T4).
    let g = crate::streaming::state_io::DEFAULT_KEY_GROUPS;
    while let Some(item) = input.next().await {
        match item {
            Ok(batch) if is_marker_batch(&batch) => {
                for tx in &senders {
                    if tx.send(Ok(batch.clone())).await.is_err() {
                        return; // a consumer dropped → stop
                    }
                }
            }
            Ok(batch) => {
                // Throughput attribution (ZELOX_WM_PROF): time the shuffle route+coalesce+send.
                let _ex = zelox_common_datafusion::streaming::event::encoding::wm_prof_enabled()
                    .then(std::time::Instant::now);
                let mut wait_ns: u64 = 0; // time BLOCKED on send (backpressure) — separated from route CPU
                let sch = batch.schema();
                let nrows = batch.num_rows();
                // Per-row key-group (matches BatchPartitioner) -> owning instance, then ONE take per
                // owner = a single copy pass (was 128-way take + concat re-merge = two passes).
                let arrays = match hash_keys
                    .iter()
                    .map(|e| e.evaluate(&batch).and_then(|v| v.into_array(nrows)))
                    .collect::<Result<Vec<_>>>()
                {
                    Ok(a) => a,
                    Err(e) => {
                        let _ = senders[0].send(Err(e)).await;
                        return;
                    }
                };
                let kgs = match crate::streaming::state_io::zelox_key_groups(&arrays, g, nrows) {
                    Ok(k) => k,
                    Err(e) => {
                        let _ = senders[0].send(Err(e)).await;
                        return;
                    }
                };
                let mut idx_by_owner: Vec<Vec<u32>> = vec![Vec::new(); n];
                for (row, &kg) in kgs.iter().enumerate() {
                    idx_by_owner[crate::streaming::state_io::key_group_owner(kg, n, g)]
                        .push(row as u32);
                }
                for (owner, idx) in idx_by_owner.into_iter().enumerate() {
                    if idx.is_empty() {
                        continue;
                    }
                    let indices = UInt32Array::from(idx);
                    let cols = match batch
                        .columns()
                        .iter()
                        .map(|c| take(c, &indices, None))
                        .collect::<std::result::Result<Vec<_>, _>>()
                    {
                        Ok(c) => c,
                        Err(e) => {
                            let _ = senders[0].send(Err(e.into())).await;
                            return;
                        }
                    };
                    let b = match RecordBatch::try_new(Arc::clone(&sch), cols) {
                        Ok(b) => b,
                        Err(e) => {
                            let _ = senders[0].send(Err(e.into())).await;
                            return;
                        }
                    };
                    let _w = _ex.map(|_| std::time::Instant::now());
                    // RFC-observability (memory truth): account bytes ENTERING the exchange in-flight.
                    if zelox_common_datafusion::streaming::event::encoding::wm_prof_enabled() {
                        zelox_common_datafusion::streaming::event::encoding::inflight_account(
                            b.get_array_memory_size() as i64,
                        );
                    }
                    if senders[owner].send(Ok(b)).await.is_err() {
                        return;
                    }
                    if let Some(w) = _w {
                        wait_ns = wait_ns.saturating_add(w.elapsed().as_nanos() as u64);
                    }
                }
                if let Some(t) = _ex {
                    let total = t.elapsed().as_nanos() as u64;
                    zelox_common_datafusion::streaming::event::encoding::prof_add(
                        &zelox_common_datafusion::streaming::event::encoding::EXCHANGE_NS,
                        total.saturating_sub(wait_ns),
                    );
                    zelox_common_datafusion::streaming::event::encoding::prof_add(
                        &zelox_common_datafusion::streaming::event::encoding::EXCHANGE_WAIT_NS,
                        wait_ns,
                    );
                }
            }
            Err(e) => {
                // Prod-grade: surface shuffle errors in the (EKS) pod log, not just up the channel.
                log::error!("stream exchange distribute error: {e}");
                let _ = senders[0].send(Err(e)).await;
                return;
            }
        }
    }
}

/// Marker classification for the N→M receiver merge.
enum Mk {
    Data,
    Watermark(i64),
    Checkpoint(u64),
    Idle,
    EndOfData,
    Other,
}

fn classify_mk(batch: &RecordBatch) -> Mk {
    let Ok(idx) = batch.schema().index_of(MARKER_FIELD_NAME) else {
        return Mk::Data;
    };
    let Some(m) = batch.column(idx).as_any().downcast_ref::<BinaryArray>() else {
        return Mk::Data;
    };
    for i in 0..m.len() {
        if m.is_valid(i) {
            return match FlowMarker::decode(m.value(i)) {
                Ok(FlowMarker::Watermark { timestamp, .. }) => {
                    Mk::Watermark(timestamp.timestamp_micros())
                }
                Ok(FlowMarker::Checkpoint { id }) => Mk::Checkpoint(id),
                Ok(FlowMarker::Idle { .. }) => Mk::Idle,
                Ok(FlowMarker::EndOfData) => Mk::EndOfData,
                _ => Mk::Other,
            };
        }
    }
    Mk::Data
}

/// Rebuild a `Checkpoint{id}` marker batch from a template (for emitting ONE aligned barrier
/// downstream after every input reached epoch `id`).
fn rewrite_checkpoint(template: &RecordBatch, id: u64) -> Result<RecordBatch> {
    let idx = template.schema().index_of(MARKER_FIELD_NAME)?;
    let bytes = FlowMarker::Checkpoint { id }.encode()?;
    let mut cols = template.columns().to_vec();
    cols[idx] = Arc::new(BinaryArray::from(vec![Some(bytes.as_slice())]));
    Ok(RecordBatch::try_new(template.schema(), cols)?)
}

/// Rebuild a watermark marker batch from a template (reuses its non-null-column placeholders),
/// overwriting the marker column with a `Watermark` at `micros`.
fn rewrite_watermark(template: &RecordBatch, micros: i64) -> Result<RecordBatch> {
    let idx = template.schema().index_of(MARKER_FIELD_NAME)?;
    let ts = chrono::DateTime::from_timestamp_micros(micros)
        .ok_or_else(|| exec_datafusion_err!("invalid watermark micros {micros}"))?;
    let bytes = FlowMarker::Watermark {
        source: "merged".to_string(),
        timestamp: ts,
    }
    .encode()?;
    let mut cols = template.columns().to_vec();
    cols[idx] = Arc::new(BinaryArray::from(vec![Some(bytes.as_slice())]));
    Ok(RecordBatch::try_new(template.schema(), cols)?)
}

/// Merge one output partition's N input sub-channels (Flink keyBy receiver). Data batches pass
/// through; watermarks are MIN-merged across the N input channels and emitted only when the min
/// strictly advances (so a fast input never closes a window before a slow input's data on its own
/// channel arrives); a single `EndOfData` is forwarded once all N inputs have ended.
/// VAJ-BF2 T-BF2.3b: the exchange's validated N→M receiver merge ([`merge_output_subchannels`] —
/// MIN-merge of DISTINCT source watermarks (Flink keyBy), aligned Chandy-Lamport barriers, source
/// idleness, one `EndOfData`), exposed as a stream combinator so the distributed streaming
/// `ShuffleReadExec` can align its N producer sub-streams that arrive over Arrow Flight. The generic
/// batch shuffle merge (`select_all`) naively interleaves and would mis-align barriers / skip the
/// watermark MIN — this is the streaming-correct counterpart. `realtime` gates Flink `withIdleness`
/// (a drained continuous partition excluded from the MIN); bounded shuffles keep the exact MIN
/// (`idle_timeout=None`) since their sub-channels END when drained.
///
/// Each input stream gets a bounded forwarder channel (natural backpressure); for the degenerate 1→N
/// case (one sub-stream per consumer) it is a correct pass-through with marker handling.
pub fn merge_flow_event_streams(
    streams: Vec<SendableRecordBatchStream>,
    schema: SchemaRef,
    realtime: bool,
) -> SendableRecordBatchStream {
    let mut receivers: Vec<Receiver<BatchResult>> = Vec::with_capacity(streams.len());
    for mut stream in streams {
        let (tx, rx) = channel::<BatchResult>(channel_capacity());
        receivers.push(rx);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });
    }
    let idle_timeout = realtime.then(realtime_idle_timeout);
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        merge_output_subchannels(receivers, idle_timeout),
    ))
}

/// Coalesce a distributed-shuffle stream's small DATA batches into `target`-row batches BEFORE the Flight
/// IPC boundary, so the cross-pod transport carries big batches (amortize per-batch serialize/framing/async
/// overhead — measured: 24k ~4k-row Flight messages at 100M/16-way = the distributed throughput gap; the
/// keyed `take`-route splits each input batch into `n` sub-batches so post-route batches are tiny).
///
/// This is the DataFusion `CoalesceBatchesExec` pattern (re-merge after repartition) + the streaming
/// discipline: MARKER batches (watermark / checkpoint barrier / EndOfData) flush the buffer FIRST then pass
/// through unchanged, so a barrier stays a consistent Chandy-Lamport cut (data is never reordered behind a
/// marker → EO + watermark correctness). Unlike a sender-side push loop, this is a PULL combinator: it only
/// advances when the consumer polls and flushes its buffer on natural stream-end — the Flight client drains
/// `do_get` fully, so buffered rows are never abandoned (the bug a sender-side coalescer hit on consumer
/// drop). Applied ONLY in the distributed Flight path (`stream_service`), leaving the in-process exchange
/// untouched. Grounded: REFERENCES §6 (Arroyo Shuffle-Edge / Ballista), arrow `BatchCoalescer`.
/// Target row count for the distributed Flight-shuffle coalescer (env `ZELOX_SHUFFLE_BATCH_ROWS`,
/// default 16384 = DataFusion CoalesceBatchesExec-style re-merge after the keyed route-split; ~4× the
/// measured ~4k post-route batch). VALIDATED (local-cluster distributed + MinIO, WM_PROF): counts EXACT
/// OFF==ON and shuffle_send_batches 4890→2281 (2.14× fewer Flight messages) with periodic watermarks
/// (D1). The earlier "non-deterministic loss" was machine-load flakiness (nm_dist_gate flakes on main
/// too), not this code — clean runs (EKS + local monotonic) are counts-exact. `0` disables. Cached.
/// See docs/design/distributed-shuffle-throughput.md.
pub fn shuffle_batch_rows() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("ZELOX_SHUFFLE_BATCH_ROWS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(16384)
    })
}

pub fn coalesce_flow_events<S, E>(
    mut input: S,
    target: usize,
) -> impl futures::Stream<Item = std::result::Result<RecordBatch, E>> + Send
where
    S: futures::Stream<Item = std::result::Result<RecordBatch, E>> + Send + Unpin + 'static,
    E: From<datafusion::arrow::error::ArrowError> + Send + 'static,
{
    // Flink `execution.buffer-timeout` (default 100ms): flush a partial buffer on a timer so a low-rate
    // channel never stalls and shuffle latency stays bounded, even when `target` isn't reached and no
    // marker arrives. In a pull combinator this races the input poll (tokio::select).
    let buffer_timeout = std::time::Duration::from_millis(
        std::env::var("ZELOX_SHUFFLE_BUFFER_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(100),
    );
    async_stream::stream! {
        let mut coalescer: Option<BatchCoalescer> = None;
        let mut tick = tokio::time::interval(buffer_timeout);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // consume the immediate first tick
        // Flush buffered rows out of the coalescer (finish partial → drain completed).
        macro_rules! flush {
            () => {
                if let Some(c) = coalescer.as_mut() {
                    if !c.is_empty() {
                        if let Err(e) = c.finish_buffered_batch() { yield Err(e.into()); return; }
                    }
                    while let Some(cb) = c.next_completed_batch() { yield Ok(cb); }
                }
            };
        }
        loop {
            let item = tokio::select! {
                biased;                       // prefer draining input over the flush timer
                item = input.next() => match item {
                    Some(it) => it,
                    None => break,
                },
                _ = tick.tick() => { flush!(); continue; } // buffer-timeout: bound latency, no stall
            };
            match item {
                Ok(batch) if is_marker_batch(&batch) => {
                    flush!();               // data before the marker (consistent cut)
                    yield Ok(batch);        // pass the marker through unchanged
                }
                Ok(batch) => {
                    let c = coalescer
                        .get_or_insert_with(|| BatchCoalescer::new(batch.schema(), target));
                    if let Err(e) = c.push_batch(batch) { yield Err(e.into()); return; }
                    while let Some(cb) = c.next_completed_batch() { yield Ok(cb); }
                }
                Err(e) => { yield Err(e); return; }
            }
        }
        flush!();                            // stream end: emit any partial buffer (no rows lost)
    }
}

fn merge_output_subchannels(
    subs: Vec<Receiver<BatchResult>>,
    idle_timeout: Option<std::time::Duration>,
) -> impl futures::Stream<Item = BatchResult> {
    let n = subs.len();
    async_stream::stream! {
        let mut receivers = subs;
        let mut wm: Vec<Option<i64>> = vec![None; n];
        let mut ended: Vec<bool> = vec![false; n];
        // Flink ABS aligned checkpoint: per-channel highest `Checkpoint{e}` epoch seen. A barrier is
        // BUFFERED (not forwarded) until EVERY non-ended input has reached it; then ONE aligned barrier
        // is emitted downstream. This makes a window's state snapshot at epoch `e` reflect a consistent
        // global cut (data ≤ every reader's `e` offset), so the recovery cut (offset + watermark +
        // emitted_ends) is consistent and exactly-once holds. Forwarding one input's barrier (the old
        // `Mk::Other` path) snapshotted an inconsistent cut → crash re-emitted committed windows.
        let mut ckpt: Vec<Option<u64>> = vec![None; n];
        let mut last_emitted_ckpt: Option<u64> = None;
        let mut ckpt_template: Option<RecordBatch> = None;
        // Source-signaled idleness (Flink WatermarkStatus.IDLE): a channel is idle iff its source last
        // emitted an `Idle` marker (genuinely caught up), NOT a wall-clock gap — a slow-but-active
        // (unscheduled/backpressured) source at scale must keep HOLDING the watermark MIN, or windows
        // close early with partial data → over-emit. Set on `Idle`, cleared on any data/watermark/
        // checkpoint from the channel. Only consulted when `idle_timeout` is Some (realtime).
        let mut idle_marked: Vec<bool> = vec![false; n];
        let mut last_emitted: Option<i64> = None;
        let mut end_batch: Option<RecordBatch> = None;
        // A watermark batch kept as a template so an idle-tick MIN advance can emit even when no
        // channel delivered a batch this iteration (the timeout path has no `batch` in hand).
        let mut wm_template: Option<RecordBatch> = None;
        loop {
            let pollable: Vec<usize> = (0..n).filter(|&j| !ended[j]).collect();
            if pollable.is_empty() {
                if let Some(b) = end_batch.take() {
                    yield Ok(b);
                }
                return;
            }
            // Poll the sub-channels; on the realtime path also wake after `idle_timeout` even if no
            // channel is active, so a newly-idle channel gets excluded from the MIN (idleness tick).
            let polled = {
                let poll_fut = futures::future::poll_fn(|cx| {
                    for &j in &pollable {
                        if let std::task::Poll::Ready(v) = receivers[j].poll_recv(cx) {
                            return std::task::Poll::Ready((j, v));
                        }
                    }
                    std::task::Poll::Pending
                });
                match idle_timeout {
                    Some(to) => tokio::time::timeout(to, poll_fut).await.ok(),
                    None => Some(poll_fut.await),
                }
            };
            // Whether to re-evaluate the watermark MIN this iteration (on a watermark, a channel end,
            // or an idle tick — NOT on a plain data batch, which is just forwarded).
            let mut recompute = false;
            match polled {
                None => recompute = true, // idle tick: no channel produced within `idle_timeout`
                Some((j, item)) => match item {
                    None => { ended[j] = true; recompute = true; } // channel closed = exhausted
                    Some(Err(e)) => { yield Err(e); return; }
                    Some(Ok(batch)) => {
                        match classify_mk(&batch) {
                            Mk::Data => {
                                idle_marked[j] = false;
                                // RFC-observability: account bytes LEAVING the exchange in-flight.
                                if zelox_common_datafusion::streaming::event::encoding::wm_prof_enabled() {
                                    zelox_common_datafusion::streaming::event::encoding::inflight_account(
                                        -(batch.get_array_memory_size() as i64),
                                    );
                                }
                                yield Ok(batch);
                            }
                            Mk::Watermark(ts) => {
                                idle_marked[j] = false; // active again
                                wm[j] = Some(wm[j].map_or(ts, |c| c.max(ts)));
                                wm_template = Some(batch);
                                recompute = true;
                            }
                            Mk::Checkpoint(e) => {
                                idle_marked[j] = false;
                                // Buffer this input's barrier; emit downstream only once aligned.
                                ckpt[j] = Some(ckpt[j].map_or(e, |c| c.max(e)));
                                ckpt_template = Some(batch);
                                recompute = true;
                            }
                            Mk::Idle => {
                                // Source is genuinely caught up — exclude from the watermark MIN
                                // (consumed here, not forwarded downstream).
                                idle_marked[j] = true;
                                recompute = true;
                            }
                            Mk::EndOfData => {
                                ended[j] = true;
                                if end_batch.is_none() {
                                    end_batch = Some(batch);
                                }
                                recompute = true;
                            }
                            // Other broadcast markers (latency): forward only sub-channel 0's copy.
                            Mk::Other => { if j == 0 { yield Ok(batch); } }
                        }
                    }
                },
            }
            if recompute {
                // Watermark MIN over channels that are neither ENDED nor IDLE (Flink withIdleness): a
                // channel idle beyond the timeout is excluded so it can't hold the MIN back (closes the
                // continuous last-window edge). Among the remaining ACTIVE channels, if any has not yet
                // reported a watermark, HOLD (None) — a slow-but-active channel must never be skipped
                // (that would close a window early = the partial-count-split dup).
                let merged = {
                    let mut min_active: Option<i64> = None;
                    let mut any_active_pending = false;
                    let mut has_active = false;
                    let mut max_idle: Option<i64> = None; // max wm among idle (drained) channels
                    for k in 0..n {
                        if ended[k] {
                            continue;
                        }
                        // Source-signaled idle (Flink WatermarkStatus.IDLE), gated to realtime.
                        let idle = idle_timeout.is_some() && idle_marked[k];
                        if idle {
                            // idle → excluded from the active MIN, but remember its wm for the
                            // all-idle drain case below.
                            if let Some(v) = wm[k] {
                                max_idle = Some(max_idle.map_or(v, |c| c.max(v)));
                            }
                            continue;
                        }
                        has_active = true;
                        match wm[k] {
                            Some(v) => min_active = Some(min_active.map_or(v, |c| c.min(v))),
                            None => any_active_pending = true,
                        }
                    }
                    if has_active {
                        // Some channel is actively producing → hold the MIN over active channels (a
                        // slow-but-active channel must bound the watermark; excluded idle ones don't).
                        if any_active_pending { None } else { min_active }
                    } else {
                        // ALL non-ended channels are IDLE. Idle now means CAUGHT UP TO THE PARTITION
                        // HIGH-WATERMARK (source-signaled `Idle` = consumed==high, all data drained) — NOT
                        // a wall-clock gap — so advancing to the MAX seen safely closes the final windows
                        // with COMPLETE data (no re-fire: nothing more will arrive). This is the Flink
                        // withIdleness "all sources idle" drain done on the CORRECT idle definition.
                        max_idle
                    }
                };
                if let Some(mw) = merged {
                    if last_emitted.is_none_or(|l| mw > l) {
                        last_emitted = Some(mw);
                        if let Some(tmpl) = &wm_template {
                            match rewrite_watermark(tmpl, mw) {
                                Ok(b) => yield Ok(b),
                                Err(e) => { yield Err(e); return; }
                            }
                        }
                    }
                }
                // Aligned checkpoint (Flink ABS): the barrier safe to emit = MIN `Checkpoint` epoch over
                // every non-ended input (a non-ended input that has not yet delivered any barrier HOLDS
                // the alignment — it must never be skipped, or its data would land in the wrong epoch's
                // snapshot). Emit one barrier per newly-aligned epoch, in order. Ended inputs are excluded
                // (their stream is exhausted); an all-ended merge emits nothing (EndOfData path handles it).
                let aligned = {
                    let mut min_ck: Option<u64> = None;
                    let mut hold = false;
                    let mut any = false;
                    for k in 0..n {
                        if ended[k] {
                            continue;
                        }
                        any = true;
                        match ckpt[k] {
                            Some(v) => min_ck = Some(min_ck.map_or(v, |c| c.min(v))),
                            None => hold = true,
                        }
                    }
                    if any && !hold { min_ck } else { None }
                };
                if let Some(a) = aligned {
                    // First alignment: emit only `a` (epochs before the first-seen barrier — e.g. those
                    // before a post-restart resume at committed+1 — were never triggered on this stream).
                    let start = last_emitted_ckpt.map_or(a, |l| l + 1);
                    for e in start..=a {
                        if let Some(tmpl) = &ckpt_template {
                            match rewrite_checkpoint(tmpl, e) {
                                Ok(b) => yield Ok(b),
                                Err(err) => { yield Err(err); return; }
                            }
                        }
                    }
                    last_emitted_ckpt = Some(a);
                }
            }
        }
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{BinaryArray, BooleanArray, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::expressions::Column;
    use futures::TryStreamExt;

    use super::*;

    fn flow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(MARKER_FIELD_NAME, DataType::Binary, true),
            Field::new("_retracted", DataType::Boolean, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn data_batch(schema: &Arc<Schema>, values: &[i64]) -> RecordBatch {
        let n = values.len();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BinaryArray::from(vec![None::<&[u8]>; n])),
                Arc::new(BooleanArray::from(vec![false; n])),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn marker_batch(schema: &Arc<Schema>) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BinaryArray::from(vec![Some(b"wm".as_ref())])),
                Arc::new(BooleanArray::from(vec![false])),
                Arc::new(Int64Array::from(vec![None::<i64>])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn coalesce_preserves_rows_and_marker_order() {
        // Small data batches (10 rows each) + a marker: coalesce to target=25 must (a) preserve EVERY
        // row in order, (b) pass the marker through exactly once, (c) actually merge (emit a >10-row
        // batch), and (d) flush all pre-marker data BEFORE the marker (consistent Chandy-Lamport cut).
        let schema = flow_schema();
        let batches: Vec<Result<RecordBatch>> = vec![
            Ok(data_batch(&schema, &(0..10).collect::<Vec<_>>())),
            Ok(data_batch(&schema, &(10..20).collect::<Vec<_>>())),
            Ok(data_batch(&schema, &(20..30).collect::<Vec<_>>())),
            Ok(marker_batch(&schema)),
            Ok(data_batch(&schema, &(30..40).collect::<Vec<_>>())),
        ];
        let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(batches),
        ));
        let out: Vec<RecordBatch> = Box::pin(coalesce_flow_events(input, 25))
            .try_collect()
            .await
            .unwrap();

        let mut data_vals = Vec::new();
        let mut markers = 0;
        let mut max_data_batch = 0;
        for b in &out {
            if is_marker_batch(b) {
                markers += 1;
                continue;
            }
            max_data_batch = max_data_batch.max(b.num_rows());
            let col = b
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                data_vals.push(col.value(i));
            }
        }
        assert_eq!(
            data_vals,
            (0..40).collect::<Vec<i64>>(),
            "every row preserved, in order (no loss/dup)"
        );
        assert_eq!(markers, 1, "marker passed through exactly once");
        assert!(max_data_batch > 10, "coalesced beyond input batch size (was 10)");
        let marker_pos = out.iter().position(is_marker_batch).unwrap();
        let rows_before: usize = out[..marker_pos].iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows_before, 30,
            "all 30 pre-marker rows flushed BEFORE the marker (consistent cut)"
        );
    }

    #[tokio::test]
    async fn routes_data_disjoint_and_broadcasts_markers() {
        let schema = flow_schema();
        let values: Vec<i64> = (0..100).collect();
        let input = MemorySourceConfig::try_new_exec(
            &[vec![data_batch(&schema, &values), marker_batch(&schema)]],
            schema.clone(),
            None,
        )
        .unwrap();
        let key: PhysicalExprRef = Arc::new(Column::new("value", 2));
        let exchange = Arc::new(StreamExchangeExec::try_new(input, vec![key], 4).unwrap());
        let ctx = Arc::new(TaskContext::default());

        // Drain all 4 output partitions concurrently (bounded channels would deadlock if
        // one partition is fully consumed before the others are read).
        let mut futs = vec![];
        for p in 0..4 {
            let s = exchange.execute(p, ctx.clone()).unwrap();
            futs.push(s.try_collect::<Vec<RecordBatch>>());
        }
        let per_partition = futures::future::try_join_all(futs).await.unwrap();

        let mut data_rows = 0usize;
        let mut seen = std::collections::HashSet::new();
        let mut marker_count = 0usize;
        for batches in &per_partition {
            let mut partition_markers = 0;
            for b in batches {
                if is_marker_batch(b) {
                    partition_markers += 1;
                } else {
                    let col = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
                    for i in 0..col.len() {
                        data_rows += 1;
                        assert!(
                            seen.insert(col.value(i)),
                            "duplicate value across partitions"
                        );
                    }
                }
            }
            // Each partition must see the broadcast marker exactly once.
            assert_eq!(
                partition_markers, 1,
                "marker not broadcast to every partition"
            );
            marker_count += partition_markers;
        }
        assert_eq!(data_rows, 100, "no data rows lost");
        assert_eq!(seen.len(), 100, "all values present, no dup");
        assert_eq!(marker_count, 4, "marker broadcast to all 4 partitions");
    }
}
