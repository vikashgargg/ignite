//! `StreamBarrierAlignExec` — Chandy-Lamport barrier **alignment** for an N→1 streaming merge.
//!
//! This is the F3 primitive that makes distributed/parallel streaming exactly-once. When a single
//! logical epoch barrier (`FlowMarker::Checkpoint{epoch}`) is **broadcast** across N partitions by
//! [`super::exchange::StreamExchangeExec`], every partition eventually delivers its own copy. A
//! naive N→1 merge would forward all N copies and, worse, interleave a fast partition's
//! *post-barrier* data ahead of a slow partition's barrier — so a downstream committer would seal
//! epoch `e` while data belonging to `e` is still in flight (data loss on recovery).
//!
//! Alignment fixes this exactly as Flink does (stateful-stream-processing docs): *"barriers never
//! overtake records"* and *"once the last stream has received barrier n, the operator emits all
//! pending outgoing records and then emits barrier n itself."* Concretely, on `Checkpoint{e}` from
//! input `i` we **block** input `i` (stop consuming it) until **all** inputs have reached `e`; then
//! we forward a **single** `Checkpoint{e}` and unblock everyone. Data before the barrier on every
//! input is therefore forwarded before the barrier; data after it waits for the next epoch.
//!
//! Output is single-partition so a single-partition committer (e.g. the realtime durable sink) can
//! seal each globally-consistent epoch. Non-checkpoint broadcast markers (watermark/latency) are
//! de-duplicated by forwarding only partition 0's copy; `EndOfData` is forwarded once all inputs end.

use std::sync::Arc;

use datafusion::arrow::array::{Array, BinaryArray, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{internal_err, Result};
use futures::StreamExt;
use zelox_common_datafusion::streaming::event::marker::FlowMarker;
use zelox_common_datafusion::streaming::event::schema::MARKER_FIELD_NAME;
use tokio::sync::mpsc::{channel, Receiver};

const CHANNEL_CAPACITY: usize = 16;

type BatchResult = Result<RecordBatch>;

#[derive(Debug)]
pub struct StreamBarrierAlignExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl StreamBarrierAlignExec {
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

impl DisplayAs for StreamBarrierAlignExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "StreamBarrierAlignExec")
    }
}

/// Classify a flow-event batch for alignment: the first marker's kind, or `None` for a data batch.
enum BatchKind {
    Data,
    Checkpoint(u64),
    OtherMarker,
    EndOfData,
}

fn classify(batch: &RecordBatch) -> Result<BatchKind> {
    let Ok(idx) = batch.schema().index_of(MARKER_FIELD_NAME) else {
        return Ok(BatchKind::Data);
    };
    let Some(m) = batch.column(idx).as_any().downcast_ref::<BinaryArray>() else {
        return Ok(BatchKind::Data);
    };
    // Find the first non-null marker row (marker batches are single-marker, but be defensive).
    for i in 0..m.len() {
        if m.is_valid(i) {
            let marker = FlowMarker::decode(m.value(i))?;
            return Ok(match marker {
                FlowMarker::Checkpoint { id } => BatchKind::Checkpoint(id),
                FlowMarker::EndOfData => BatchKind::EndOfData,
                _ => BatchKind::OtherMarker,
            });
        }
    }
    Ok(BatchKind::Data)
}

impl ExecutionPlan for StreamBarrierAlignExec {
    fn name(&self) -> &str {
        "StreamBarrierAlignExec"
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
            return internal_err!("StreamBarrierAlignExec requires exactly one child");
        }
        Ok(Arc::new(StreamBarrierAlignExec::new(children.remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("StreamBarrierAlignExec: invalid partition {partition}");
        }
        let n = self
            .input
            .properties()
            .output_partitioning()
            .partition_count();
        let schema = self.input.schema();
        let streams = (0..n)
            .map(|i| self.input.execute(i, context.clone()))
            .collect::<Result<Vec<_>>>()?;
        Ok(align_flow_event_streams(streams, schema))
    }
}

/// VAJ-BF2 T-BF2.3a: the Chandy-Lamport barrier-**alignment** merge over N flow-event streams,
/// factored out of [`StreamBarrierAlignExec`] so it can be reused by the cross-network streaming
/// shuffle read (a keyed N→M exchange's consumer partition receives its N producer sub-streams and
/// must align them — the same algorithm, but the sub-streams arrive over Arrow Flight instead of
/// in-process partitions). Behaviour is identical to the previous inline implementation:
/// - **Data** batches pass through immediately.
/// - **`Checkpoint{e}`** blocks its input until *every* non-ended input has reached `e`, then forwards
///   ONE stashed marker batch (barriers never overtake records; consistent cut).
/// - Other broadcast markers (watermark/latency) are de-duplicated by forwarding only input 0's copy.
///   *(N→M shuffle consumers instead need a MIN-merge across distinct source watermarks — added as a
///   mode in T-BF2.3b; this factoring keeps the exact current N→1 broadcast behaviour.)*
/// - **`EndOfData`** ends its input; one real `EndOfData` is forwarded once all inputs end.
///
/// Each input gets a bounded forwarder channel: "blocking an input" during alignment = not receiving
/// from its channel, so the bounded channel applies natural upstream backpressure.
pub fn align_flow_event_streams(
    streams: Vec<SendableRecordBatchStream>,
    schema: SchemaRef,
) -> SendableRecordBatchStream {
    let n = streams.len();
    let schema_out = schema;

    let mut receivers: Vec<Receiver<BatchResult>> = Vec::with_capacity(n);
    for mut stream in streams {
        let (tx, rx) = channel::<BatchResult>(CHANNEL_CAPACITY);
        receivers.push(rx);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                if tx.send(item).await.is_err() {
                    break;
                }
            }
        });
    }

    let out = async_stream::stream! {
            // Per-input state: `reached` = the epoch this input is currently blocked at (Some),
            // `ended` = the input is exhausted. An input contributes to the next barrier once it is
            // blocked; an ended input no longer needs to reach barriers. We stash the actual
            // `Checkpoint`/`EndOfData` marker batch each input delivered and forward ONE of them on
            // alignment — reusing the source's exact encoding (incl. non-nullable-column
            // placeholders) instead of reconstructing it (which would mishandle nullability).
            let mut reached: Vec<Option<u64>> = vec![None; n];
            let mut ended: Vec<bool> = vec![false; n];
            let mut checkpoint_batch: Vec<Option<RecordBatch>> = vec![None; n];
            let mut end_batch: Option<RecordBatch> = None;

            loop {
                // Can we seal an epoch? Every non-ended input must be blocked at the same epoch,
                // and at least one input must be blocked (so we don't loop on an all-ended set).
                let active: Vec<usize> = (0..n).filter(|&i| !ended[i]).collect();
                if !active.is_empty() && active.iter().all(|&i| reached[i].is_some()) {
                    // All active inputs reached the barrier — forward ONE of their stashed marker
                    // batches (they carry the same broadcast epoch), then unblock.
                    if let Some(b) = checkpoint_batch[active[0]].take() {
                        yield Ok(b);
                    }
                    for &i in &active {
                        reached[i] = None;
                        checkpoint_batch[i] = None;
                    }
                    continue;
                }
                if active.is_empty() {
                    // Every input ended: forward a single EndOfData (a real one if we saw it).
                    if let Some(b) = end_batch.take() {
                        yield Ok(b);
                    }
                    return;
                }

                // Receive from any input that is neither blocked nor ended. `poll_fn` polls each
                // pollable receiver in turn (disjoint `&mut` borrows, one at a time) and returns the
                // first ready item — registering wakers on the rest so we resume when data arrives.
                let pollable: Vec<usize> = (0..n)
                    .filter(|&i| !ended[i] && reached[i].is_none())
                    .collect();
                let (i, item) = futures::future::poll_fn(|cx| {
                    for &i in &pollable {
                        if let std::task::Poll::Ready(v) = receivers[i].poll_recv(cx) {
                            return std::task::Poll::Ready((i, v));
                        }
                    }
                    std::task::Poll::Pending
                })
                .await;
                match item {
                    None => ended[i] = true, // channel closed = input exhausted
                    Some(Ok(batch)) => {
                        match classify(&batch) {
                            Ok(BatchKind::Data) => yield Ok(batch),
                            Ok(BatchKind::Checkpoint(e)) => {
                                // Block this input until all align; stash its real marker batch.
                                reached[i] = Some(e);
                                checkpoint_batch[i] = Some(batch);
                            }
                            // De-dup broadcast watermark/latency: forward only input 0's copy.
                            Ok(BatchKind::OtherMarker) => { if i == 0 { yield Ok(batch); } }
                            // EndOfData: this input is done; keep one real EndOfData batch to forward.
                            Ok(BatchKind::EndOfData) => {
                                ended[i] = true;
                                if end_batch.is_none() {
                                    end_batch = Some(batch);
                                }
                            }
                            Err(e) => { yield Err(e); return; }
                        }
                    }
                    Some(Err(e)) => { yield Err(e); return; }
                }
            }
        };
    Box::pin(RecordBatchStreamAdapter::new(schema_out, out))
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{
        new_null_array, ArrayRef, BinaryArray, BooleanArray, Int64Array, RecordBatch,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::TryStreamExt;
    use zelox_common_datafusion::streaming::event::marker::FlowMarker;
    use zelox_common_datafusion::streaming::event::schema::{
        MARKER_FIELD_NAME, RETRACTED_FIELD_NAME,
    };

    use super::{classify, BatchKind, StreamBarrierAlignExec};

    fn flow_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(MARKER_FIELD_NAME, DataType::Binary, true),
            Field::new(RETRACTED_FIELD_NAME, DataType::Boolean, false),
            Field::new("v", DataType::Int64, true),
        ]))
    }

    fn data(vals: &[i64]) -> RecordBatch {
        let schema = flow_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BinaryArray::from(vec![None::<&[u8]>; vals.len()])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false; vals.len()])) as ArrayRef,
                Arc::new(Int64Array::from(vals.to_vec())) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn marker(m: FlowMarker) -> RecordBatch {
        let schema = flow_schema();
        let bytes = m.encode().unwrap();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BinaryArray::from(vec![Some(bytes.as_slice())])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                new_null_array(&DataType::Int64, 1),
            ],
        )
        .unwrap()
    }

    fn count_checkpoints(batches: &[RecordBatch]) -> Vec<u64> {
        batches
            .iter()
            .filter_map(|b| match classify(b).unwrap() {
                BatchKind::Checkpoint(e) => Some(e),
                _ => None,
            })
            .collect()
    }

    fn sum_data(batches: &[RecordBatch]) -> i64 {
        batches
            .iter()
            .filter(|b| matches!(classify(b).unwrap(), BatchKind::Data))
            .map(|b| {
                b.column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .flatten()
                    .sum::<i64>()
            })
            .sum()
    }

    #[tokio::test]
    async fn aligns_broadcast_checkpoints_to_one_and_preserves_all_data() {
        // Two partitions, each: data, Checkpoint{0}, data, Checkpoint{1}, EndOfData. The aligned
        // output must contain exactly ONE Checkpoint{0} and ONE Checkpoint{1}, all data preserved,
        // and every Checkpoint{0} must precede all Checkpoint{1} (barrier ordering).
        let schema = flow_schema();
        let p0 = vec![
            data(&[1, 2]),
            marker(FlowMarker::Checkpoint { id: 0 }),
            data(&[5]),
            marker(FlowMarker::Checkpoint { id: 1 }),
            marker(FlowMarker::EndOfData),
        ];
        let p1 = vec![
            data(&[3, 4]),
            marker(FlowMarker::Checkpoint { id: 0 }),
            data(&[6]),
            marker(FlowMarker::Checkpoint { id: 1 }),
            marker(FlowMarker::EndOfData),
        ];
        let src = MemorySourceConfig::try_new_exec(&[p0, p1], schema.clone(), None).unwrap();
        let align = Arc::new(StreamBarrierAlignExec::new(src));
        let out: Vec<RecordBatch> = align
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        assert_eq!(
            count_checkpoints(&out),
            vec![0, 1],
            "one aligned barrier per epoch, in order"
        );
        assert_eq!(
            sum_data(&out),
            1 + 2 + 3 + 4 + 5 + 6,
            "no data lost or duplicated"
        );
    }

    // VAJ-BF2 T-BF2.3a: the align algorithm works as a standalone combinator over N arbitrary
    // flow-event streams (not just an ExecutionPlan's partitions) — this is what the cross-network
    // streaming shuffle read will call with its N producer sub-streams.
    #[tokio::test]
    async fn combinator_aligns_arbitrary_streams() {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

        use super::align_flow_event_streams;

        let schema = flow_schema();
        let mk = |batches: Vec<RecordBatch>| -> datafusion::execution::SendableRecordBatchStream {
            Box::pin(RecordBatchStreamAdapter::new(
                flow_schema(),
                futures::stream::iter(batches.into_iter().map(Ok)),
            ))
        };
        let s0 = mk(vec![
            data(&[10, 20]),
            marker(FlowMarker::Checkpoint { id: 7 }),
            data(&[30]),
            marker(FlowMarker::EndOfData),
        ]);
        let s1 = mk(vec![
            data(&[40]),
            marker(FlowMarker::Checkpoint { id: 7 }),
            data(&[50, 60]),
            marker(FlowMarker::EndOfData),
        ]);

        let out: Vec<RecordBatch> = align_flow_event_streams(vec![s0, s1], schema)
            .try_collect()
            .await
            .unwrap();

        // Exactly ONE aligned Checkpoint{7}; all data across both streams preserved once.
        assert_eq!(count_checkpoints(&out), vec![7], "one aligned barrier");
        assert_eq!(sum_data(&out), 10 + 20 + 30 + 40 + 50 + 60, "no loss/dup");
    }
}
