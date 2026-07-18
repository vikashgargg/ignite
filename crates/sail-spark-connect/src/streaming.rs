use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sail_common_datafusion::streaming::checkpoint::CheckpointStore;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::{PlanType, StringifiedPlan};
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use log::warn;
use sail_common_datafusion::error::CommonErrorCause;
use sail_python_udf::error::PyErrExtractor;
use tokio::sync::{oneshot, watch};

use crate::error::{SparkError, SparkResult, SparkThrowable};
use crate::spark::connect;
use crate::web_ui;

/// Keep the last N micro-batch progress reports (Spark default is 100).
const MAX_RECENT_PROGRESS: usize = 100;

pub struct StreamingQuery {
    name: String,
    info: Vec<StringifiedPlan>,
    error: watch::Receiver<Option<SparkThrowable>>,
    stopped: watch::Receiver<bool>,
    signal: Option<oneshot::Sender<()>>,
    awaitable: bool,
    /// Ring buffer of recent `StreamingQueryProgress` JSON reports (newest last), written by
    /// the run loop per micro-batch and read by `lastProgress`/`recentProgress`.
    progress: Arc<Mutex<VecDeque<String>>>,
}

/// An executed micro-batch: its physical plan (for reading leaf-scan row metrics) + output stream.
pub type PlannedStream = (Arc<dyn ExecutionPlan>, SendableRecordBatchStream);

/// Produces a fresh (bounded) micro-batch (plan + stream) by re-planning + executing the query —
/// used for continuous (`ProcessingTime`) triggers, where each trigger is a fresh availableNow-style
/// micro-batch that reuses the proven offset/state commit + recovery (Spark `MicroBatchExecution`
/// re-plan model). See docs/design/streaming-file-source.md.
pub type MakeStream = Box<
    dyn Fn() -> std::pin::Pin<
            Box<dyn std::future::Future<Output = SparkResult<PlannedStream>> + Send>,
        > + Send
        + Sync,
>;

/// How `StreamingQuery::run` drives execution.
enum StreamDriver {
    /// `availableNow`/`once` (bounded): consume a single pre-built (plan, stream), then stop.
    Once(Arc<dyn ExecutionPlan>, SendableRecordBatchStream),
    /// `ProcessingTime` (continuous): re-plan + execute a bounded micro-batch each `interval`,
    /// committing after each, until stopped.
    Continuous {
        make_stream: MakeStream,
        interval: Duration,
    },
    /// `Trigger.Continuous` (Vajra realtime mode): one long-lived unbounded pipeline that flows
    /// records continuously for low latency; offsets are committed per epoch every
    /// `commit_interval`, asynchronously off the data path (Spark Continuous Processing model).
    /// See docs/design/streaming-realtime-mode.md.
    Realtime {
        plan: Arc<dyn ExecutionPlan>,
        stream: SendableRecordBatchStream,
        commit_interval: Duration,
    },
}

/// Sum the output rows of the plan's leaf nodes (sources/scans) — `numInputRows` for one
/// micro-batch. Leaf scans (e.g. the parquet scan under the streaming file source) record
/// `output_rows` via DataFusion metrics; sources without metrics contribute 0.
///
/// Leaves are deduplicated by `Arc` identity: the parallel streaming sink fans the *same*
/// source `Arc` into N writer children, so a naive walk would count it N times.
fn count_input_rows(plan: &Arc<dyn ExecutionPlan>) -> u64 {
    fn walk(plan: &Arc<dyn ExecutionPlan>, seen: &mut std::collections::HashSet<*const ()>) -> u64 {
        let id = Arc::as_ptr(plan) as *const ();
        if !seen.insert(id) {
            return 0; // already counted this shared node
        }
        let children = plan.children();
        if children.is_empty() {
            return plan.metrics().and_then(|m| m.output_rows()).unwrap_or(0) as u64;
        }
        children.iter().map(|c| walk(c, seen)).sum()
    }
    walk(plan, &mut std::collections::HashSet::new())
}

/// Build a Spark `StreamingQueryProgress`-shaped JSON for one micro-batch (the fields the
/// PySpark `StreamingQueryProgress`/`SourceProgress`/`SinkProgress` parsers require).
fn progress_json(
    id: &str,
    run_id: &str,
    name: &str,
    batch_id: u64,
    num_input_rows: u64,
    duration_ms: u128,
    source_desc: &str,
    sink_desc: &str,
) -> String {
    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let rate = if duration_ms > 0 {
        (num_input_rows as f64) * 1000.0 / (duration_ms as f64)
    } else {
        0.0
    };
    let name_json = if name.is_empty() {
        "null".to_string()
    } else {
        format!("{name:?}")
    };
    format!(
        "{{\"id\":{id:?},\"runId\":{run_id:?},\"name\":{name_json},\"timestamp\":{timestamp:?},\
         \"batchId\":{batch_id},\"batchDuration\":{duration_ms},\
         \"durationMs\":{{\"triggerExecution\":{duration_ms}}},\"eventTime\":{{}},\
         \"stateOperators\":[],\
         \"sources\":[{{\"description\":{source_desc:?},\"startOffset\":null,\"endOffset\":null,\
         \"latestOffset\":null,\"numInputRows\":{num_input_rows},\
         \"inputRowsPerSecond\":{rate:.1},\"processedRowsPerSecond\":{rate:.1}}}],\
         \"sink\":{{\"description\":{sink_desc:?},\"numOutputRows\":{num_input_rows}}},\
         \"numInputRows\":{num_input_rows},\"inputRowsPerSecond\":{rate:.1},\
         \"processedRowsPerSecond\":{rate:.1}}}"
    )
}

/// Consume one (bounded) micro-batch stream to completion. Writes the per-batch offset marker
/// (informational: UI/progress; numbering for non-file sources) and returns whether it ran
/// without error (the durability signal for committing offsets).
///
/// Crash-recovery: for the file→file sink path the batch number is derived from the file source's
/// commit record (`sail_data_source::formats::file_stream::current_batch_id`), which advances
/// atomically with the source offset (one rename of `staged`→`committed` carrying both the id and
/// the processed-files set). So a crash anywhere around commit replays the batch at the SAME id,
/// idempotently overwriting `<out>/<N>/` + `_spark_metadata/<N>` — no duplicate and no silent-loss
/// window (verified by the W3 simulation + SIGKILL gates).
async fn consume_stream(
    mut stream: SendableRecordBatchStream,
    ck: &Option<CheckpointStore>,
    batch_id: &mut u64,
    ui_id: &str,
    error: &watch::Sender<Option<SparkThrowable>>,
) -> bool {
    let mut clean = true;
    // Drain the bounded micro-batch to completion. The sink does the durable work (files +
    // `_spark_metadata` + source-offset staging); the driver only needs to detect a clean end.
    // We deliberately do NOT key the offset marker on output items: the streaming sink emits an
    // empty-schema (0-row, 0-column) completion batch, which the single-node path receives but the
    // distributed Arrow-Flight shuffle drops in transit — so per-item marker writes silently skip in
    // cluster mode, leaving no `offsets/<batch_id>` WAL and breaking batch-id resume across restart.
    while let Some(x) = stream.next().await {
        if let Err(e) = x {
            clean = false;
            let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
            let _ = error.send(Some(cause.into()));
        }
    }
    // One bounded micro-batch = one offset commit. On a clean end, write the per-batch offset marker
    // (the WAL that lets a restart resume at `batch_id + 1`) exactly once — robust whether or not the
    // sink's completion batch reached the driver. Identical behavior single-node and distributed.
    if clean {
        if let Some(ck) = ck {
            let payload = format!(
                "v1\n{{\"batchId\":{batch_id},\"timestamp\":{}}}\n",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0)
            );
            if let Err(e) = ck
                .put(&format!("offsets/{batch_id}"), bytes::Bytes::from(payload))
                .await
            {
                warn!("Failed to write checkpoint offset {batch_id}: {e}");
            }
        }
        web_ui::increment_batch(ui_id).await;
        *batch_id += 1;
    }
    clean
}

/// Latest committed batch id from the `offsets` markers in the checkpoint store.
async fn latest_batch_id(ck: &CheckpointStore) -> Option<u64> {
    ck.list("offsets")
        .await
        .ok()?
        .iter()
        .filter_map(|s| s.parse::<u64>().ok())
        .max()
}

impl StreamingQuery {
    pub fn new(
        query_id: String,
        run_id: String,
        name: String,
        info: Vec<StringifiedPlan>,
        plan: Arc<dyn ExecutionPlan>,
        stream: SendableRecordBatchStream,
        checkpoint_location: Option<String>,
    ) -> Self {
        Self::spawn(
            query_id,
            run_id,
            name,
            info,
            StreamDriver::Once(plan, stream),
            checkpoint_location,
        )
    }

    /// Continuous (`ProcessingTime`) query: re-plan + execute a bounded micro-batch every
    /// `interval`, committing after each (reusing the availableNow exactly-once + state
    /// recovery), until stopped.
    pub fn new_continuous(
        query_id: String,
        run_id: String,
        name: String,
        info: Vec<StringifiedPlan>,
        make_stream: MakeStream,
        interval: Duration,
        checkpoint_location: Option<String>,
    ) -> Self {
        Self::spawn(
            query_id,
            run_id,
            name,
            info,
            StreamDriver::Continuous {
                make_stream,
                interval,
            },
            checkpoint_location,
        )
    }

    /// Realtime (`Trigger.Continuous`) query: run one long-lived unbounded pipeline continuously
    /// (low latency), committing offsets per epoch every `commit_interval`. See
    /// docs/design/streaming-realtime-mode.md.
    pub fn new_realtime(
        query_id: String,
        run_id: String,
        name: String,
        info: Vec<StringifiedPlan>,
        plan: Arc<dyn ExecutionPlan>,
        stream: SendableRecordBatchStream,
        commit_interval: Duration,
        checkpoint_location: Option<String>,
    ) -> Self {
        Self::spawn(
            query_id,
            run_id,
            name,
            info,
            StreamDriver::Realtime {
                plan,
                stream,
                commit_interval,
            },
            checkpoint_location,
        )
    }

    fn spawn(
        query_id: String,
        run_id: String,
        name: String,
        info: Vec<StringifiedPlan>,
        driver: StreamDriver,
        checkpoint_location: Option<String>,
    ) -> Self {
        // `initial_batch_id` is computed inside `run` (it needs async checkpoint-store I/O).
        let ui_id = uuid::Uuid::new_v4().to_string();
        {
            let id = ui_id.clone();
            let n = name.clone();
            tokio::spawn(async move {
                web_ui::register_query(id, n).await;
            });
        }

        let (signal_tx, signal_rx) = oneshot::channel();
        let (error_tx, error_rx) = watch::channel(None);
        let (stopped_tx, stopped_rx) = watch::channel(false);
        let ui_id_run = ui_id.clone();
        let progress = Arc::new(Mutex::new(VecDeque::new()));
        let progress_run = Arc::clone(&progress);
        let name_run = name.clone();
        tokio::spawn(async move {
            Self::run(
                signal_rx,
                error_tx,
                stopped_tx,
                driver,
                checkpoint_location,
                ui_id_run,
                query_id,
                run_id,
                name_run,
                progress_run,
            )
            .await;
        });
        Self {
            name,
            info,
            error: error_rx,
            stopped: stopped_rx,
            signal: Some(signal_tx),
            awaitable: true,
            progress,
        }
    }

    /// Recent micro-batch progress reports as JSON (newest last) — `lastProgress`/`recentProgress`.
    pub fn recent_progress(&self) -> Vec<String> {
        self.progress
            .lock()
            .map(|p| p.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn status(&self) -> StreamingQueryStatus {
        let stopped = *self.stopped.borrow();
        let default_message = if stopped {
            "The query is not active"
        } else {
            "The query is active"
        };
        StreamingQueryStatus {
            name: self.name.clone(),
            message: self
                .error
                .borrow()
                .as_ref()
                .map(|x| x.message().to_string())
                .unwrap_or_else(|| default_message.to_string()),
            is_active: !stopped,
        }
    }

    #[expect(clippy::too_many_arguments)]
    async fn run(
        signal: oneshot::Receiver<()>,
        error: watch::Sender<Option<SparkThrowable>>,
        stopped: watch::Sender<bool>,
        driver: StreamDriver,
        checkpoint_location: Option<String>,
        ui_id: String,
        query_id: String,
        run_id: String,
        name: String,
        progress: Arc<Mutex<VecDeque<String>>>,
    ) {
        // Checkpoint store (local FS, S3, or GCS); checkpoint I/O goes through it so streaming
        // recovery survives a pod restart on object storage.
        let ck: Option<CheckpointStore> = checkpoint_location
            .as_deref()
            .and_then(|loc| match CheckpointStore::from_location(loc) {
                Ok(c) => Some(c),
                Err(e) => {
                    warn!("Failed to open checkpoint store {loc}: {e}");
                    None
                }
            });
        // Resume from the last committed batch id (the `offsets` markers exist iff committed).
        let initial_batch_id = match &ck {
            Some(ck) => latest_batch_id(ck).await.map(|n| n + 1).unwrap_or(0),
            None => 0,
        };
        if initial_batch_id > 0 {
            log::info!("Streaming checkpoint recovery: resuming from batch {initial_batch_id}");
        }
        let mut batch_id: u64 = initial_batch_id;
        let mut mb_id: u64 = 0; // micro-batch (trigger) index, for progress reporting

        let record = |plan: &Arc<dyn ExecutionPlan>, mb_id: u64, duration_ms: u128| {
            if let Ok(mut p) = progress.lock() {
                p.push_back(progress_json(
                    &query_id,
                    &run_id,
                    &name,
                    mb_id,
                    count_input_rows(plan),
                    duration_ms,
                    "streaming source",
                    "streaming sink",
                ));
                while p.len() > MAX_RECENT_PROGRESS {
                    p.pop_front();
                }
            }
        };

        match driver {
            StreamDriver::Once(plan, stream) => {
                let t0 = std::time::Instant::now();
                tokio::select! {
                    _ = signal => {}
                    clean = consume_stream(stream, &ck, &mut batch_id, &ui_id, &error) => {
                        // The micro-batch completed (availableNow/once). If clean, the output is
                        // durable, so commit the sources' staged offsets (write-ahead →
                        // committed) — exactly-once recovery on the next run.
                        if clean {
                            if let Some(ck) = &ck {
                                commit_source_offsets(ck).await;
                            }
                            record(&plan, mb_id, t0.elapsed().as_millis());
                        }
                    }
                }
            }
            StreamDriver::Continuous {
                make_stream,
                interval,
            } => {
                // Spark micro-batch model: each trigger is a fresh bounded micro-batch that
                // re-plans (picking up new files) and reuses the availableNow commit + state
                // recovery. Each micro-batch commits only after its output is durable (clean
                // end), so a crash replays only the uncommitted micro-batch — exactly-once.
                let mut signal = signal;
                loop {
                    let t0 = std::time::Instant::now();
                    let made = tokio::select! {
                        _ = &mut signal => break,
                        m = make_stream() => m,
                    };
                    let (plan, stream) = match made {
                        Ok(ps) => ps,
                        Err(e) => {
                            let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                            let _ = error.send(Some(cause.into()));
                            break;
                        }
                    };
                    let clean = tokio::select! {
                        _ = &mut signal => break,
                        c = consume_stream(stream, &ck, &mut batch_id, &ui_id, &error) => c,
                    };
                    if clean {
                        if let Some(ck) = &ck {
                            commit_source_offsets(ck).await;
                        }
                        record(&plan, mb_id, t0.elapsed().as_millis());
                        mb_id += 1;
                    } else {
                        break; // error already reported
                    }
                    // Wait for the next trigger (interruptible by stop).
                    tokio::select! {
                        _ = &mut signal => break,
                        _ = tokio::time::sleep(interval) => {}
                    }
                }
            }
            StreamDriver::Realtime {
                plan,
                stream,
                commit_interval,
            } => {
                // Vajra realtime mode: one long-lived unbounded pipeline. Records flow continuously
                // (low latency); offsets are committed per epoch on a timer — asynchronously, off
                // the record path (Spark Continuous Processing model), so commits never stall flow.
                // Slice 1: at-least-once (commits whatever sources have staged). Exactly-once for
                // stateless via Checkpoint{epoch} markers + per-source epoch staging is slice 2.
                let mut signal = signal;
                let mut stream = stream;
                let t0 = std::time::Instant::now();
                let mut commit_timer = tokio::time::interval(commit_interval);
                commit_timer.tick().await; // discard the immediate first tick
                // EPIC-T/T2 (Arroyo async-checkpoint): the epoch commit runs OFF the record path in a
                // spawned task so `stream.next()` keeps draining during the S3 puts (previously the
                // commit ran inline in the select! → the pipeline stalled ~200-600ms EVERY epoch). One
                // commit in flight at a time (await the prior at each tick) preserves ordering + bounds
                // staged growth; committed state = staged-as-of-this-tick, so crash-EO is unchanged.
                let mut commit_handle: Option<tokio::task::JoinHandle<()>> = None;
                loop {
                    tokio::select! {
                        _ = &mut signal => break,
                        item = stream.next() => {
                            match item {
                                Some(Ok(_)) => web_ui::increment_batch(&ui_id).await,
                                Some(Err(e)) => {
                                    let cause = CommonErrorCause::new::<PyErrExtractor>(&e);
                                    let _ = error.send(Some(cause.into()));
                                    break;
                                }
                                None => break, // source finished
                            }
                        }
                        _ = commit_timer.tick() => {
                            // Epoch boundary: commit progress OFF the record path (spawned). The loop
                            // resumes polling `stream.next()` immediately — the S3 puts no longer stall
                            // the pipeline. Await the prior commit first (one in flight, ordered).
                            if let Some(ck) = &ck {
                                if let Some(h) = commit_handle.take() {
                                    let _ = h.await;
                                }
                                let ck2 = ck.clone();
                                let bid = batch_id;
                                commit_handle = Some(tokio::spawn(async move {
                                    let payload = format!(
                                        "v1\n{{\"batchId\":{bid},\"timestamp\":{}}}\n",
                                        std::time::SystemTime::now()
                                            .duration_since(std::time::UNIX_EPOCH)
                                            .map(|d| d.as_millis())
                                            .unwrap_or(0)
                                    );
                                    if let Err(e) = ck2
                                        .put(&format!("offsets/{bid}"), bytes::Bytes::from(payload))
                                        .await
                                    {
                                        warn!("realtime epoch {bid} marker write failed: {e}");
                                    }
                                    commit_source_offsets(&ck2).await;
                                }));
                            }
                            record(&plan, mb_id, t0.elapsed().as_millis());
                            batch_id += 1;
                            mb_id += 1;
                        }
                    }
                }
                // Final commit on stop: drain the in-flight off-path commit, then a final sync commit
                // so the last epoch's staged offsets/state land committed at a consistent boundary.
                if let Some(h) = commit_handle.take() {
                    let _ = h.await;
                }
                if let Some(ck) = &ck {
                    commit_source_offsets(ck).await;
                }
            }
        }

        web_ui::mark_stopped(&ui_id).await;
        let _ = stopped.send(true);
    }
}

/// Promote every source's and operator's staged (write-ahead) artifact to committed, once the
/// batch output is durable. This is the commit step of the offset/state WAL → commit-log protocol
/// (Spark `MicroBatchExecution`). Each artifact is a single object, so a "commit" is one atomic
/// `put` of `committed` (object stores have no rename) — works on `file://` and `s3://` alike.
/// See docs/design/streaming-exactly-once.md.
async fn commit_source_offsets(ck: &CheckpointStore) {
    // Find every `sources/<id>/staged` and `state/<op>/staged`, promote each to its `committed`.
    for root in ["sources", "state"] {
        let staged = match ck.list_rel(root).await {
            Ok(items) => items,
            Err(e) => {
                warn!("Failed to list {root} for commit: {e}");
                continue;
            }
        };
        for rel in staged.into_iter().filter(|r| r.ends_with("/staged")) {
            let committed = rel.trim_end_matches("staged").to_string() + "committed";
            if let Err(e) = ck.promote(&rel, &committed).await {
                warn!("Failed to commit {rel}: {e}");
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamingQueryId {
    pub query_id: String,
    pub run_id: String,
}

impl From<connect::StreamingQueryInstanceId> for StreamingQueryId {
    fn from(value: connect::StreamingQueryInstanceId) -> Self {
        Self {
            query_id: value.id,
            run_id: value.run_id,
        }
    }
}

impl From<StreamingQueryId> for connect::StreamingQueryInstanceId {
    fn from(value: StreamingQueryId) -> Self {
        Self {
            id: value.query_id,
            run_id: value.run_id,
        }
    }
}

pub struct StreamingQueryManager {
    queries: HashMap<StreamingQueryId, StreamingQuery>,
}

impl StreamingQueryManager {
    pub fn new() -> Self {
        Self {
            queries: HashMap::new(),
        }
    }

    pub fn add_query(&mut self, id: StreamingQueryId, query: StreamingQuery) {
        self.queries.insert(id, query);
    }

    pub fn stop_query(&mut self, id: &StreamingQueryId) -> SparkResult<()> {
        let Some(query) = self.queries.get_mut(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        if let Some(signal) = query.signal.take() {
            let _ = signal.send(());
        };
        Ok(())
    }

    pub fn explain_query(&self, id: &StreamingQueryId, extended: bool) -> SparkResult<String> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        let mut result = String::new();
        let mut write = |kind: &'static str, t: &PlanType| {
            for item in query.info.iter() {
                if &item.plan_type == t {
                    result.push_str("== ");
                    result.push_str(kind);
                    result.push_str(" ==\n");
                    result.push_str(item.plan.trim_end_matches('\n'));
                    result.push_str("\n\n");
                }
            }
        };
        if extended {
            write("Initial Logical Plan", &PlanType::InitialLogicalPlan);
            write("Final Logical Plan", &PlanType::FinalLogicalPlan);
        }
        write("Final Physical Plan", &PlanType::FinalPhysicalPlan);
        Ok(result)
    }

    pub fn get_query_status(&self, id: &StreamingQueryId) -> SparkResult<StreamingQueryStatus> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        Ok(query.status())
    }

    pub fn recent_progress(&self, id: &StreamingQueryId) -> SparkResult<Vec<String>> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        Ok(query.recent_progress())
    }

    pub fn get_query_error(&self, id: &StreamingQueryId) -> SparkResult<Option<SparkThrowable>> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        Ok(query.error.borrow().clone())
    }

    pub fn find_query_by_query_id(
        &self,
        query_id: &str,
    ) -> SparkResult<(StreamingQueryId, StreamingQueryStatus)> {
        for (id, query) in self.queries.iter() {
            if id.query_id == query_id {
                return Ok((id.clone(), query.status()));
            }
        }
        Err(SparkError::invalid(format!(
            "streaming query not found by query id: {query_id}"
        )))
    }

    pub fn list_active_queries(&self) -> Vec<(StreamingQueryId, StreamingQueryStatus)> {
        self.queries
            .iter()
            .filter_map(|(id, query)| {
                if !*query.stopped.borrow() {
                    Some((id.clone(), query.status()))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn await_query(
        &self,
        id: &StreamingQueryId,
    ) -> SparkResult<Option<StreamingQueryAwaitHandle>> {
        let Some(query) = self.queries.get(id) else {
            return Err(SparkError::invalid(format!(
                "streaming query not found: {id:?}"
            )));
        };
        if !query.awaitable {
            Ok(None)
        } else {
            let stopped = query.stopped.clone();
            Ok(Some(StreamingQueryAwaitHandle { stopped }))
        }
    }

    pub fn await_queries(&self) -> SparkResult<StreamingQueryAwaitHandleSet> {
        let mut handles = Vec::new();
        for query in self.queries.values() {
            if query.awaitable {
                handles.push(StreamingQueryAwaitHandle {
                    stopped: query.stopped.clone(),
                });
            }
        }
        Ok(StreamingQueryAwaitHandleSet::new(handles))
    }

    pub fn reset_stopped_queries(&mut self) {
        for query in self.queries.values_mut() {
            if *query.stopped.borrow() {
                query.awaitable = false;
            }
        }
    }
}

pub struct StreamingQueryStatus {
    pub name: String,
    pub message: String,
    pub is_active: bool,
}

pub fn timeout_millis(value: i64) -> SparkResult<Duration> {
    if value < 0 {
        return Err(SparkError::invalid(format!(
            "invalid timeout value: {value}"
        )));
    }
    Ok(Duration::from_millis(value as u64))
}

pub struct StreamingQueryAwaitHandle {
    stopped: watch::Receiver<bool>,
}

impl StreamingQueryAwaitHandle {
    async fn wait(mut self) -> () {
        // We ignore the receiver error since the streaming query must have been
        // terminated in that case.
        let _ = self.stopped.wait_for(|x| *x).await;
    }

    pub async fn terminated(self, timeout: Option<Duration>) -> SparkResult<bool> {
        if let Some(timeout) = timeout {
            match tokio::time::timeout(timeout, self.wait()).await {
                Ok(()) => Ok(true),
                Err(_) => Ok(false),
            }
        } else {
            self.wait().await;
            Ok(true)
        }
    }
}

pub struct StreamingQueryAwaitHandleSet {
    handles: Vec<StreamingQueryAwaitHandle>,
}

impl StreamingQueryAwaitHandleSet {
    pub fn new(handles: Vec<StreamingQueryAwaitHandle>) -> Self {
        Self { handles }
    }

    pub async fn any_terminated(self, timeout: Option<Duration>) -> SparkResult<bool> {
        let mut join_set = tokio::task::JoinSet::new();
        for handle in self.handles {
            join_set.spawn(handle.wait());
        }
        let next = async move {
            match join_set.join_next().await {
                Some(Ok(())) => Ok(true),
                Some(Err(e)) => Err(SparkError::internal(format!(
                    "failed to await any termination for streaming queries: {e}"
                ))),
                None => Ok(false),
            }
        };
        if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, next)
                .await
                .unwrap_or_else(|_| Ok(false))
        } else {
            next.await
        }
    }
}
