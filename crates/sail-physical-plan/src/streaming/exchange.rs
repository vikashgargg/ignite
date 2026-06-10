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

use datafusion::arrow::array::{Array, BinaryArray, RecordBatch};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExprRef};
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::repartition::BatchPartitioner;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion_common::{exec_datafusion_err, internal_err, Result};
use futures::StreamExt;
use sail_common_datafusion::streaming::event::schema::MARKER_FIELD_NAME;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;

/// Bounded channel depth per output partition (backpressure point).
const CHANNEL_CAPACITY: usize = 16;

type BatchResult = Result<RecordBatch>;
/// Per-output-partition receivers, taken by `execute`; lazily initialized on first call.
type SharedReceivers = Arc<Mutex<Option<Vec<Option<Receiver<BatchResult>>>>>>;

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
        // Lazily start the single distributor task on the first `execute` call and stash a
        // receiver for each output partition; subsequent calls just take their receiver.
        let mut guard = self
            .state
            .lock()
            .map_err(|e| exec_datafusion_err!("StreamExchangeExec state lock poisoned: {e}"))?;
        if guard.is_none() {
            let n = self.partition_count;
            let mut senders: Vec<Sender<BatchResult>> = Vec::with_capacity(n);
            let mut receivers: Vec<Option<Receiver<BatchResult>>> = Vec::with_capacity(n);
            for _ in 0..n {
                let (tx, rx) = channel::<BatchResult>(CHANNEL_CAPACITY);
                senders.push(tx);
                receivers.push(Some(rx));
            }
            let input_stream = self.input.execute(0, context.clone())?;
            let hash_keys = self.hash_keys.clone();
            tokio::spawn(distribute(input_stream, senders, hash_keys, n));
            *guard = Some(receivers);
        }
        let rx = guard
            .as_mut()
            .and_then(|rs| rs.get_mut(partition).and_then(|slot| slot.take()))
            .ok_or_else(|| {
                exec_datafusion_err!("StreamExchangeExec: partition {partition} already taken")
            })?;
        drop(guard);
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
    // `input_partition`/`num_input_partitions` only matter for round-robin; unused for Hash.
    let mut partitioner =
        match BatchPartitioner::try_new(Partitioning::Hash(hash_keys, n), Time::default(), 0, 1) {
            Ok(p) => p,
            Err(e) => {
                let _ = senders[0].send(Err(e)).await;
                return;
            }
        };
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
                // Hash-route the data batch into per-partition sub-batches (sync), then send.
                let mut parts: Vec<(usize, RecordBatch)> = Vec::new();
                let res = partitioner.partition(batch, |idx, sub| {
                    parts.push((idx, sub));
                    Ok(())
                });
                if let Err(e) = res {
                    let _ = senders[0].send(Err(e)).await;
                    return;
                }
                for (idx, sub) in parts {
                    if sub.num_rows() == 0 {
                        continue;
                    }
                    if senders[idx].send(Ok(sub)).await.is_err() {
                        return;
                    }
                }
            }
            Err(e) => {
                let _ = senders[0].send(Err(e)).await;
                return;
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
                    let col = b
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    for i in 0..col.len() {
                        data_rows += 1;
                        assert!(seen.insert(col.value(i)), "duplicate value across partitions");
                    }
                }
            }
            // Each partition must see the broadcast marker exactly once.
            assert_eq!(partition_markers, 1, "marker not broadcast to every partition");
            marker_count += partition_markers;
        }
        assert_eq!(data_rows, 100, "no data rows lost");
        assert_eq!(seen.len(), 100, "all values present, no dup");
        assert_eq!(marker_count, 4, "marker broadcast to all 4 partitions");
    }
}
