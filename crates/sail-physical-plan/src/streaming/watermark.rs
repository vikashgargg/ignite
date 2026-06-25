use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use chrono::DateTime;
use datafusion::arrow::array::{
    Array, Int32Array, Int64Array, RecordBatch, TimestampMicrosecondArray,
};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion_common::{plan_err, Result};
use futures::{stream, StreamExt};
use sail_common_datafusion::streaming::event::encoding::{
    DecodedFlowEventStream, EncodedFlowEventStream,
};
use sail_common_datafusion::streaming::event::marker::FlowMarker;
use sail_common_datafusion::streaming::event::schema::to_flow_event_schema;
use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
use sail_common_datafusion::streaming::event::FlowEvent;

/// Emits event-time watermarks as in-band `FlowMarker::Watermark` events.
///
/// Sits below the window-folding projection (so the raw event-time column is still
/// present), tracks `max(event_time)`, and emits a watermark marker
/// (`max − delay`) whenever the watermark advances. Data events pass through
/// unchanged. Downstream stateful operators (e.g. `WindowAccumExec`) consume the
/// markers to drive eviction — decoupling watermarking from a raw column surviving
/// query optimization. This is the scalable, Flink-style model (watermarks as stream
/// events; multi-input operators take the min — see docs/design/streaming-watermark.md).
#[derive(Debug)]
pub struct WatermarkExec {
    input: Arc<dyn ExecutionPlan>,
    event_time_col: String,
    delay_micros: i64,
    /// Decoded data schema (without flow-event fields).
    data_schema: SchemaRef,
    /// Flink per-partition watermark: when set, track `max(event_time)` PER value of this column
    /// (the source `partition`) and emit watermark = MIN across partitions − delay, emitting only
    /// once all `num_partitions` partitions are seen. `None` ⇒ single global-max (today's behavior).
    /// Fixes premature window close when one instance reads N out-of-order partitions (see
    /// docs/design/streaming-per-partition-watermark.md).
    partition_col: Option<String>,
    num_partitions: usize,
    properties: Arc<PlanProperties>,
}

impl WatermarkExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        event_time_col: String,
        delay_micros: i64,
        data_schema: SchemaRef,
    ) -> Result<Self> {
        // Passthrough schema: same flow-event schema as the input.
        let flow_schema = Arc::new(to_flow_event_schema(&data_schema));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(flow_schema),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            event_time_col,
            delay_micros,
            data_schema,
            partition_col: None,
            num_partitions: 1,
            properties,
        })
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
    pub fn event_time_col(&self) -> &str {
        &self.event_time_col
    }
    pub fn delay_micros(&self) -> i64 {
        self.delay_micros
    }
    pub fn data_schema(&self) -> &SchemaRef {
        &self.data_schema
    }
    /// Enable Flink per-partition watermarking on `partition_col` across `num_partitions` partitions.
    pub fn with_partition_watermark(mut self, partition_col: String, num_partitions: usize) -> Self {
        self.partition_col = Some(partition_col);
        self.num_partitions = num_partitions.max(1);
        self
    }
    pub fn partition_col(&self) -> Option<&str> {
        self.partition_col.as_deref()
    }
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }
}

impl DisplayAs for WatermarkExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "WatermarkExec: eventTime={}, delay={}µs",
            self.event_time_col, self.delay_micros
        )
    }
}

impl ExecutionPlan for WatermarkExec {
    fn name(&self) -> &str {
        "WatermarkExec"
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
        let (Some(child), true) = (children.pop(), children.is_empty()) else {
            return plan_err!("{} expects exactly one child", self.name());
        };
        let mut exec = WatermarkExec::try_new(
            child,
            self.event_time_col.clone(),
            self.delay_micros,
            self.data_schema.clone(),
        )?;
        if let Some(col) = &self.partition_col {
            exec = exec.with_partition_watermark(col.clone(), self.num_partitions);
        }
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let data_schema = self.data_schema.clone();
        // Event-time column index in the decoded data schema (None ⇒ pass through
        // without emitting watermarks, e.g. if optimization renamed it away).
        let event_time_idx = data_schema.index_of(&self.event_time_col).ok();
        let delay = self.delay_micros;
        // Flink per-partition watermark: (partition-col index, total partition count). When present,
        // emit watermark = MIN(max_et per partition) − delay, only once all N partitions are seen.
        let part_idx = self
            .partition_col
            .as_ref()
            .and_then(|c| data_schema.index_of(c).ok());
        let num_partitions = self.num_partitions;
        let in_stream = self.input.execute(partition, context)?;
        let input_stream = DecodedFlowEventStream::try_new(in_stream).map_err(|e| {
            let names: Vec<_> = self
                .input
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            datafusion_common::exec_datafusion_err!("WatermarkExec decode (input {names:?}): {e}")
        })?;

        type State = (
            DecodedFlowEventStream,
            Option<i64>,                       // global max_ts (single-partition path)
            std::collections::HashMap<i64, i64>, // per-partition max_et (per-partition path)
            Option<i64>,                       // last emitted watermark (monotonic)
            VecDeque<FlowEvent>,
        );
        let init: State = (
            input_stream,
            None,
            std::collections::HashMap::new(),
            None,
            VecDeque::new(),
        );

        let event_stream = stream::unfold(
            init,
            move |(mut input, mut max_ts, mut per_part, mut last_wm, mut pending)| async move {
                loop {
                    if let Some(ev) = pending.pop_front() {
                        return Some((Ok(ev), (input, max_ts, per_part, last_wm, pending)));
                    }
                    match input.next().await {
                        None => return None,
                        Some(Err(e)) => {
                            return Some((Err(e), (input, max_ts, per_part, last_wm, pending)))
                        }
                        Some(Ok(FlowEvent::Data { batch, retracted })) => {
                            // Candidate watermark from this batch (before delay), or None if we
                            // can't/shouldn't advance yet.
                            let candidate: Option<i64> = match (event_time_idx, part_idx) {
                                // Flink per-partition: update each partition's max_et, then the
                                // watermark = MIN across partitions, but only once all N are seen.
                                (Some(et_i), Some(p_i)) => {
                                    update_per_partition(&batch, et_i, p_i, &mut per_part);
                                    if per_part.len() >= num_partitions {
                                        per_part.values().copied().min()
                                    } else {
                                        None // withhold until every partition has reported
                                    }
                                }
                                // Single global max (default).
                                (Some(et_i), None) => {
                                    let col = batch.column(et_i);
                                    if matches!(
                                        col.data_type(),
                                        DataType::Timestamp(TimeUnit::Microsecond, _)
                                    ) {
                                        if let Some(m) = col
                                            .as_any()
                                            .downcast_ref::<TimestampMicrosecondArray>()
                                            .and_then(compute::max)
                                        {
                                            max_ts = Some(max_ts.map_or(m, |c| c.max(m)));
                                        }
                                    }
                                    max_ts
                                }
                                _ => None,
                            };
                            pending.push_back(FlowEvent::Data { batch, retracted });
                            // Emit a watermark only when it advances (monotonic).
                            if let Some(m) = candidate {
                                let wm = m - delay;
                                if last_wm.is_none_or(|l| wm > l) {
                                    last_wm = Some(wm);
                                    if let Some(ts) = DateTime::from_timestamp_micros(wm) {
                                        pending.push_back(FlowEvent::Marker(
                                            FlowMarker::Watermark {
                                                source: "watermark".to_string(),
                                                timestamp: ts,
                                            },
                                        ));
                                    }
                                }
                            }
                        }
                        Some(Ok(other)) => {
                            return Some((Ok(other), (input, max_ts, per_part, last_wm, pending)));
                        }
                    }
                }
            },
        );

        let flow_stream = Box::pin(FlowEventStreamAdapter::new(data_schema, event_stream));
        Ok(Box::pin(EncodedFlowEventStream::new(flow_stream)))
    }
}

/// Update `per_part[partition] = max(existing, max event_time of that partition's rows in `batch`)`.
/// Reads the µs event-time column and the (Int32) partition column row-wise. Rows with a null
/// partition or non-µs event-time are skipped.
fn update_per_partition(
    batch: &RecordBatch,
    et_idx: usize,
    part_idx: usize,
    per_part: &mut std::collections::HashMap<i64, i64>,
) {
    let Some(et) = batch
        .column(et_idx)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
    else {
        return;
    };
    let parts = batch.column(part_idx);
    let parts_i32 = parts.as_any().downcast_ref::<Int32Array>();
    let parts_i64 = parts.as_any().downcast_ref::<Int64Array>();
    for i in 0..batch.num_rows() {
        if et.is_null(i) {
            continue;
        }
        let p = match (parts_i32, parts_i64) {
            (Some(a), _) if !a.is_null(i) => a.value(i) as i64,
            (_, Some(a)) if !a.is_null(i) => a.value(i),
            _ => continue,
        };
        let e = et.value(i);
        per_part.entry(p).and_modify(|m| *m = (*m).max(e)).or_insert(e);
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::execution::TaskContext;
    use datafusion::physical_expr::Partitioning;
    use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion::physical_plan::PlanProperties;
    use futures::stream;
    use sail_common_datafusion::streaming::event::stream::FlowEventStreamAdapter;
    use sail_common_datafusion::streaming::event::FlowEvent;

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("et", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("partition", DataType::Int32, false),
        ]))
    }

    // One row: event-time `et_us` on Kafka `part`.
    fn row(s: &SchemaRef, et_us: i64, part: i32) -> FlowEvent {
        let b = RecordBatch::try_new(
            s.clone(),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![et_us])),
                Arc::new(Int32Array::from(vec![part])),
            ],
        )
        .unwrap();
        FlowEvent::append_only_data(b)
    }

    #[derive(Debug)]
    struct Src {
        events: Vec<FlowEvent>,
        schema: SchemaRef,
        props: Arc<PlanProperties>,
    }
    impl Src {
        fn new(events: Vec<FlowEvent>, schema: SchemaRef) -> Self {
            let props = Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::new(to_flow_event_schema(&schema))),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Both,
                Boundedness::Bounded,
            ));
            Self { events, schema, props }
        }
    }
    impl DisplayAs for Src {
        fn fmt_as(&self, _: datafusion::physical_plan::DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "Src")
        }
    }
    impl ExecutionPlan for Src {
        fn name(&self) -> &str { "Src" }
        fn as_any(&self) -> &dyn Any { self }
        fn properties(&self) -> &Arc<PlanProperties> { &self.props }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
        fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> { Ok(self) }
        fn execute(&self, _p: usize, _c: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
            let s = stream::iter(self.events.clone().into_iter().map(Ok));
            let flow = Box::pin(FlowEventStreamAdapter::new(self.schema.clone(), s));
            Ok(Box::pin(EncodedFlowEventStream::new(flow)))
        }
    }

    // Per-partition watermark: partition 0 races ahead (100s) but the watermark must NOT advance
    // until partition 1 is also seen, and then tracks the LAGGING min (50s) — not the global max.
    #[tokio::test]
    async fn per_partition_watermark_tracks_lagging_min() {
        let s = schema();
        let sec = 1_000_000i64;
        let events = vec![
            row(&s, 100 * sec, 0), // only p0 seen -> withhold (1 < 2 partitions)
            row(&s, 50 * sec, 1),  // both seen -> wm = min(100,50) = 50s
            row(&s, 120 * sec, 1), // p1 advances -> wm = min(100,120) = 100s
        ];
        let exec = Arc::new(
            WatermarkExec::try_new(Arc::new(Src::new(events, s.clone())), "et".to_string(), 0, s)
                .unwrap()
                .with_partition_watermark("partition".to_string(), 2),
        );
        let mut dec = DecodedFlowEventStream::try_new(
            exec.execute(0, Arc::new(TaskContext::default())).unwrap(),
        )
        .unwrap();
        let mut wms = vec![];
        while let Some(ev) = dec.next().await {
            if let FlowEvent::Marker(FlowMarker::Watermark { timestamp, .. }) = ev.unwrap() {
                wms.push(timestamp.timestamp_micros() / sec);
            }
        }
        // No watermark before both partitions seen; then the lagging MIN (50s), then 100s. Never 120.
        assert_eq!(wms, vec![50, 100], "per-partition watermark = min over partitions, withheld until all seen");
    }
}
