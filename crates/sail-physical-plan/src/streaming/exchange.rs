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

use std::any::Any;
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::{Array, BinaryArray, RecordBatch, UInt32Array};
use datafusion::arrow::compute::take;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExprRef};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_datafusion_err, internal_err, Result};
use futures::StreamExt;
use sail_common_datafusion::streaming::event::marker::FlowMarker;
use sail_common_datafusion::streaming::event::schema::MARKER_FIELD_NAME;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

/// Bounded channel depth per output partition = the streaming BACKPRESSURE point. Lower depth = tighter
/// backpressure = less in-flight memory (DataFusion does NOT account intermediate stream buffers, so this
/// is a primary streaming-RSS lever; the measured EKS 1.20x-vs-Flink memory gap is live in-flight buffering
/// across up to N×M sub-channels). Tunable via `VAJRA_EXCHANGE_CHANNEL_CAP` (default 16). The mpsc send
/// awaits when full, so a smaller cap bounds in-flight without dropping data (Flink FLIP-2 credit-flow
/// analog, coarser). REFERENCES §8 / docs/design/streaming-memory-boundedness.md.
fn channel_capacity() -> usize {
    static CAP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("VAJRA_EXCHANGE_CHANNEL_CAP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .filter(|&n| n >= 1)
            .unwrap_or(16)
    })
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
    fn as_any(&self) -> &dyn Any {
        self
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
        let stream = merge_output_subchannels(subs);
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
    fn as_any(&self) -> &dyn Any {
        self
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
    // Routing uses `vajra_key_groups` (PROVEN to match `BatchPartitioner` hashing) so we can `take`
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
                // Throughput attribution (VAJRA_WM_PROF): time the shuffle route+coalesce+send.
                let _ex = sail_common_datafusion::streaming::event::encoding::wm_prof_enabled()
                    .then(std::time::Instant::now);
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
                let kgs = match crate::streaming::state_io::vajra_key_groups(&arrays, g, nrows) {
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
                    if senders[owner].send(Ok(b)).await.is_err() {
                        return;
                    }
                }
                if let Some(t) = _ex {
                    sail_common_datafusion::streaming::event::encoding::prof_add(
                        &sail_common_datafusion::streaming::event::encoding::EXCHANGE_NS,
                        t.elapsed().as_nanos() as u64,
                    );
                }
            }
            Err(e) => {
                // Prod-grade: surface shuffle errors in the (EKS) pod log, not just up the channel.
                eprintln!("STREAM_EXCHANGE error (distribute): {e}");
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
                Ok(FlowMarker::EndOfData) => Mk::EndOfData,
                _ => Mk::Other,
            };
        }
    }
    Mk::Data
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
fn merge_output_subchannels(
    subs: Vec<Receiver<BatchResult>>,
) -> impl futures::Stream<Item = BatchResult> {
    let n = subs.len();
    async_stream::stream! {
        let mut receivers = subs;
        let mut wm: Vec<Option<i64>> = vec![None; n];
        let mut ended: Vec<bool> = vec![false; n];
        let mut last_emitted: Option<i64> = None;
        let mut end_batch: Option<RecordBatch> = None;
        loop {
            let pollable: Vec<usize> = (0..n).filter(|&j| !ended[j]).collect();
            if pollable.is_empty() {
                if let Some(b) = end_batch.take() {
                    yield Ok(b);
                }
                return;
            }
            let (j, item) = futures::future::poll_fn(|cx| {
                for &j in &pollable {
                    if let std::task::Poll::Ready(v) = receivers[j].poll_recv(cx) {
                        return std::task::Poll::Ready((j, v));
                    }
                }
                std::task::Poll::Pending
            })
            .await;
            match item {
                None => ended[j] = true, // channel closed = input exhausted
                Some(Err(e)) => { yield Err(e); return; }
                Some(Ok(batch)) => match classify_mk(&batch) {
                    Mk::Data => yield Ok(batch),
                    Mk::Watermark(ts) => {
                        wm[j] = Some(wm[j].map_or(ts, |c| c.max(ts)));
                        let merged = {
                            let mut mn: Option<i64> = None;
                            let mut all = true;
                            for k in 0..n {
                                if ended[k] { continue; }
                                match wm[k] {
                                    Some(v) => mn = Some(mn.map_or(v, |c| c.min(v))),
                                    None => { all = false; break; }
                                }
                            }
                            if all { mn } else { None }
                        };
                        if let Some(mw) = merged {
                            if last_emitted.is_none_or(|l| mw > l) {
                                last_emitted = Some(mw);
                                match rewrite_watermark(&batch, mw) {
                                    Ok(b) => yield Ok(b),
                                    Err(e) => { yield Err(e); return; }
                                }
                            }
                        }
                    }
                    Mk::EndOfData => {
                        ended[j] = true;
                        if end_batch.is_none() {
                            end_batch = Some(batch);
                        }
                    }
                    // Other broadcast markers (latency): forward only sub-channel 0's copy.
                    Mk::Other => { if j == 0 { yield Ok(batch); } }
                },
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
