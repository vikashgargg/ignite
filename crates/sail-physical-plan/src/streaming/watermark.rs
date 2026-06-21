use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use chrono::DateTime;
use datafusion::arrow::array::{Array, TimestampMicrosecondArray};
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
        Ok(Arc::new(WatermarkExec::try_new(
            child,
            self.event_time_col.clone(),
            self.delay_micros,
            self.data_schema.clone(),
        )?))
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
            Option<i64>,
            Option<i64>,
            VecDeque<FlowEvent>,
        );
        let init: State = (input_stream, None, None, VecDeque::new());

        let event_stream = stream::unfold(
            init,
            move |(mut input, mut max_ts, mut last_wm, mut pending)| async move {
                loop {
                    if let Some(ev) = pending.pop_front() {
                        return Some((Ok(ev), (input, max_ts, last_wm, pending)));
                    }
                    match input.next().await {
                        None => return None,
                        Some(Err(e)) => return Some((Err(e), (input, max_ts, last_wm, pending))),
                        Some(Ok(FlowEvent::Data { batch, retracted })) => {
                            if let Some(idx) = event_time_idx {
                                let col = batch.column(idx);
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
                            }
                            pending.push_back(FlowEvent::Data { batch, retracted });
                            // Emit a watermark only when it advances (monotonic).
                            if let Some(m) = max_ts {
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
                            return Some((Ok(other), (input, max_ts, last_wm, pending)));
                        }
                    }
                }
            },
        );

        let flow_stream = Box::pin(FlowEventStreamAdapter::new(data_schema, event_stream));
        Ok(Box::pin(EncodedFlowEventStream::new(flow_stream)))
    }
}
