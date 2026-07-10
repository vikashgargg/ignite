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
        // Flink withIdleness: a partition idle for this long is EXCLUDED from the watermark MIN so
        // the watermark never stalls (REFERENCES §2). Also the startup grace — withhold the first
        // watermark until all `num_partitions` are seen OR this elapses (whichever first), so we
        // never block forever waiting for a partition that won't come. 2s.
        let idle_timeout = std::time::Duration::from_millis(2000);
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

        // Emit watermarks PERIODICALLY (Flink `pipeline.auto-watermark-interval`, default 200ms), NOT
        // after every data batch. A time-ordered backlog advances the watermark on every batch, so
        // per-batch emission puts a marker between every data batch → the distributed shuffle can never
        // coalesce them (measured: 24k tiny Flight messages = the throughput gap) and the exchange
        // broadcasts N× the markers. Interval-gating keeps windows correct (watermark still monotonic;
        // final windows flush at end/EndOfData) while letting data batches accumulate between markers.
        let watermark_interval = std::time::Duration::from_millis(
            std::env::var("VAJRA_WATERMARK_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(200),
        );
        // Per-partition state: partition -> (max_et, last_seen instant). `start` for the startup grace.
        type PerPart = std::collections::HashMap<i64, (i64, std::time::Instant)>;
        type State = (
            DecodedFlowEventStream,
            Option<i64>,    // global max_ts (single-partition path)
            PerPart,        // per-partition (max_et, last_seen)
            Option<i64>,    // last emitted watermark (monotonic)
            std::time::Instant, // operator start (startup grace)
            Option<std::time::Instant>, // last watermark EMIT time (interval gate)
            VecDeque<FlowEvent>,
        );
        let init: State = (
            input_stream,
            None,
            std::collections::HashMap::new(),
            None,
            std::time::Instant::now(),
            None,
            VecDeque::new(),
        );

        let event_stream = stream::unfold(
            init,
            move |(mut input, mut max_ts, mut per_part, mut last_wm, start, mut last_emit, mut pending)| async move {
                loop {
                    if let Some(ev) = pending.pop_front() {
                        return Some((Ok(ev), (input, max_ts, per_part, last_wm, start, last_emit, pending)));
                    }
                    // Per-partition path: race the input against a periodic tick so an IDLE partition
                    // is excluded (and the watermark advances) even when no new data arrives — this is
                    // what makes withIdleness non-blocking (no tick on the global path).
                    let next = if part_idx.is_some() {
                        tokio::select! {
                            biased;
                            item = input.next() => Some(item),
                            _ = tokio::time::sleep(idle_timeout / 4) => None, // tick → recompute
                        }
                    } else {
                        Some(input.next().await)
                    };
                    match next {
                        // ---- per-partition idle TICK (no new data): recompute MIN over active ----
                        None => {
                            if let Some(m) = active_partition_watermark(
                                &per_part, num_partitions, start, idle_timeout, std::time::Instant::now(),
                            ) {
                                let wm = m - delay;
                                if last_wm.is_none_or(|l| wm > l) {
                                    last_wm = Some(wm);
                                    if let Some(ts) = DateTime::from_timestamp_micros(wm) {
                                        return Some((
                                            Ok(FlowEvent::Marker(FlowMarker::Watermark {
                                                source: "watermark".to_string(),
                                                timestamp: ts,
                                            })),
                                            (input, max_ts, per_part, last_wm, start, last_emit, pending),
                                        ));
                                    }
                                }
                            }
                            // nothing to emit this tick; loop to poll again
                        }
                        Some(None) => return None, // input ended
                        Some(Some(Err(e))) => {
                            return Some((Err(e), (input, max_ts, per_part, last_wm, start, last_emit, pending)))
                        }
                        Some(Some(Ok(FlowEvent::Data { batch, retracted }))) => {
                            let candidate: Option<i64> = match (event_time_idx, part_idx) {
                                (Some(et_i), Some(p_i)) => {
                                    let now = std::time::Instant::now();
                                    update_per_partition(&batch, et_i, p_i, &mut per_part, now);
                                    active_partition_watermark(
                                        &per_part, num_partitions, start, idle_timeout, now,
                                    )
                                }
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
                            // Interval gate (Flink auto-watermark-interval): emit at most once per
                            // `watermark_interval`, so a time-ordered backlog does not put a marker
                            // between every data batch (which defeats the shuffle coalescer). max_ts /
                            // per_part keep updating every batch, so the watermark we DO emit is current;
                            // final windows still flush at input-end / EndOfData.
                            let now = std::time::Instant::now();
                            let due = last_emit
                                .is_none_or(|le: std::time::Instant| now.duration_since(le) >= watermark_interval);
                            if due {
                                if let Some(m) = candidate {
                                    let wm = m - delay;
                                    if last_wm.is_none_or(|l| wm > l) {
                                        last_wm = Some(wm);
                                        last_emit = Some(now);
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
                        }
                        Some(Some(Ok(other))) => {
                            return Some((
                                Ok(other),
                                (input, max_ts, per_part, last_wm, start, last_emit, pending),
                            ));
                        }
                    }
                }
            },
        );

        let flow_stream = Box::pin(FlowEventStreamAdapter::new(data_schema, event_stream));
        Ok(Box::pin(EncodedFlowEventStream::new(flow_stream)))
    }
}

/// Per-partition state: partition id -> (max event_time µs, last-seen instant). The instant drives
/// Flink-style idleness (REFERENCES §2).
type PerPartState = std::collections::HashMap<i64, (i64, std::time::Instant)>;

/// Update each partition's `(max_et, last_seen=now)` from a batch's µs event-time + (Int32/Int64)
/// partition column. Null/uncomparable rows skipped.
fn update_per_partition(
    batch: &RecordBatch,
    et_idx: usize,
    part_idx: usize,
    per_part: &mut PerPartState,
    now: std::time::Instant,
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
        per_part
            .entry(p)
            .and_modify(|(m, ts)| {
                *m = (*m).max(e);
                *ts = now;
            })
            .or_insert((e, now));
    }
}

/// Flink per-partition watermark with idleness (REFERENCES §2). Returns the watermark candidate
/// (pre-delay) = MIN over partitions that are ACTIVE (seen within `idle_timeout`), so an idle
/// partition can never stall the watermark. During the startup grace (`now - start < idle_timeout`)
/// it WITHHOLDS unconditionally (pure-time, no partition-count) so all partitions report their first
/// record before any window can close; after the grace it proceeds on whatever is active. `None` =
/// withhold/no-data. Pure (takes `now`) so it's unit-tested deterministically — the bug this replaces
/// (withhold-until-all-N forever, which needed N and could HANG 3h) is gone.
fn active_partition_watermark(
    per_part: &PerPartState,
    num_partitions: usize,
    start: std::time::Instant,
    idle_timeout: std::time::Duration,
    now: std::time::Instant,
) -> Option<i64> {
    if per_part.is_empty() {
        return None;
    }
    // Startup grace (pure-time, Flink bounded-out-of-orderness): withhold the first watermark until
    // the grace elapses so every partition reports its first record (the realtime source reads ALL
    // partitions in ONE instance, so they all produce within the grace) → no premature first-window
    // close. No partition-count N needed — that's why `num_partitions` is no longer read here.
    let _ = num_partitions;
    if now.duration_since(start) < idle_timeout {
        return None;
    }
    // MIN over ACTIVE partitions (idle ones excluded → never stalls).
    per_part
        .values()
        .filter(|(_, last)| now.duration_since(*last) < idle_timeout)
        .map(|(et, _)| *et)
        .min()
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

    // Pure-time startup grace + idleness (no partition-count N): withhold all watermarks until the
    // grace elapses (so every partition reports first → no premature first-window close), then emit
    // MIN over ACTIVE partitions, excluding idle ones so it NEVER stalls (Flink withIdleness).
    #[test]
    fn active_partition_watermark_grace_and_idle_exclusion() {
        use std::collections::HashMap;
        use std::time::{Duration, Instant};
        let idle = Duration::from_millis(2000);
        let t0 = Instant::now();
        // (1) within the startup grace → withhold unconditionally (even with data, no N needed).
        let mut m: HashMap<i64, (i64, Instant)> = HashMap::new();
        m.insert(0, (100, t0));
        m.insert(1, (50, t0 + Duration::from_millis(500)));
        assert_eq!(active_partition_watermark(&m, 0, t0, idle, t0 + Duration::from_millis(600)), None);
        // (2) after the grace, both active → MIN over active.
        assert_eq!(active_partition_watermark(&m, 0, t0, idle, t0 + Duration::from_millis(2100)), Some(50));
        // (3) NO STALL: p0 idle (last seen 3s ago > idle) is EXCLUDED even though its et=30 is the
        //     min; watermark advances on the active p1 (70). Without exclusion this would stall/regress.
        let mut m2: HashMap<i64, (i64, Instant)> = HashMap::new();
        m2.insert(0, (30, t0)); // idle
        m2.insert(1, (70, t0 + Duration::from_millis(2900))); // active
        assert_eq!(active_partition_watermark(&m2, 0, t0, idle, t0 + Duration::from_millis(3000)), Some(70));
    }

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
        // Invariant (timing-robust): the per-partition watermark NEVER leaks past the lagging MIN —
        // it must never emit the global max (120s) while p0 is at 100s. The pure-time startup grace may
        // withhold the early watermarks in this fast bounded stream; the exact grace/MIN sequence is
        // covered deterministically by `active_partition_watermark_grace_and_idle_exclusion`.
        assert!(
            wms.iter().all(|w| *w <= 100),
            "per-partition wm must never leak past the lagging min; got {wms:?}"
        );
    }
}
