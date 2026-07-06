use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::internal_err;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, ParquetSource};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use log::debug;
use prost::Message;
use sail_common_datafusion::error::CommonErrorCause;
use sail_data_source::formats::kafka::{KafkaSourceExec, ValueParseSpec};
use sail_delta_lake::physical_plan::DeltaPhysicalExprAdapterFactory;
use sail_function::scalar::json::from_json::SparkFromJsonOptions;
use sail_python_udf::error::PyErrExtractor;
use sail_server::actor::{Actor, ActorContext};
use sail_telemetry::telemetry::global_metrics;
use sail_telemetry::{trace_execution_plan, TracingExecOptions};
use tokio::sync::oneshot;

use crate::codec::RemoteExecutionCodec;
use crate::driver::TaskStatus;
use crate::error::{ExecutionError, ExecutionResult};
use crate::id::{TaskKey, TaskKeyDisplay};
use crate::plan::{ShuffleReadExec, ShuffleWriteExec, StageInputExec};
use crate::stream_accessor::{StreamAccessor, StreamAccessorMessage};
use crate::task::definition::{TaskDefinition, TaskInput, TaskOutput};
use crate::task_runner::monitor::TaskMonitor;
use crate::task_runner::{TaskRunner, TaskRunnerMessage};

impl TaskRunner {
    pub fn new() -> Self {
        Self {
            signals: HashMap::new(),
            codec: Box::new(RemoteExecutionCodec),
        }
    }

    pub fn run_task<T: Actor>(
        &mut self,
        ctx: &mut ActorContext<T>,
        key: TaskKey,
        definition: TaskDefinition,
        context: Arc<TaskContext>,
    ) where
        T::Message: TaskRunnerMessage + StreamAccessorMessage,
    {
        let stream = match self.execute_plan(ctx, &key, definition, context) {
            Ok(x) => x,
            Err(e) => {
                let event = T::Message::report_task_status(
                    key,
                    TaskStatus::Failed,
                    Some(format!("failed to execute plan: {e}")),
                    Some(CommonErrorCause::new::<PyErrExtractor>(&e)),
                );
                ctx.send(event);
                return;
            }
        };
        let handle = ctx.handle().clone();
        let (tx, rx) = oneshot::channel();
        self.signals.insert(key.clone(), tx);
        let monitor = TaskMonitor::new(handle, key, stream, rx);
        ctx.spawn(monitor.run());
    }

    pub fn stop_task(&mut self, key: &TaskKey) {
        if let Some(signal) = self.signals.remove(key) {
            let _ = signal.send(());
        }
    }

    /// Deserializes and prepares a physical plan for execution on this node.
    fn execute_plan<T: Actor>(
        &mut self,
        ctx: &mut ActorContext<T>,
        key: &TaskKey,
        definition: TaskDefinition,
        context: Arc<TaskContext>,
    ) -> ExecutionResult<SendableRecordBatchStream>
    where
        T::Message: TaskRunnerMessage + StreamAccessorMessage,
    {
        let plan = PhysicalPlanNode::decode(definition.plan.as_ref())?;
        let plan = plan.try_into_physical_plan(&context, self.codec.as_ref())?;
        let plan = self.rewrite_parquet_adapters(plan)?;
        let plan = Self::rewrite_source_fusion(plan)?;
        let plan = self.rewrite_shuffle(
            ctx,
            key,
            &definition.inputs,
            &definition.output,
            plan,
            &context,
        )?;
        debug!(
            "{} execution plan\n{}",
            TaskKeyDisplay(key),
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
        let options = TracingExecOptions {
            metrics: global_metrics(),
            job_id: Some(key.job_id.into()),
            stage: Some(key.stage),
            attempt: Some(key.attempt),
            operator_id: None,
        };
        let plan = trace_execution_plan(plan, options)?;
        let stream = plan.execute(key.partition, context)?;
        Ok(stream)
    }

    fn rewrite_parquet_adapters(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        let result = plan.transform(|node| {
            let Some(ds) = node.downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(config) = ds.data_source().downcast_ref::<FileScanConfig>() else {
                return Ok(Transformed::no(node));
            };
            // Distributed execution runs each output partition as an ISOLATED task. DataFusion 54's
            // morsel-driven file scan pools every file into a `SharedWorkSource` across sibling
            // partition streams so in-process siblings steal work and read each file exactly once.
            // A distributed task has no in-process siblings, so it would drain the whole pool and
            // re-read every file once per partition (N file groups -> N× duplication). Marking the
            // scan `partitioned_by_file_group` disables the shared work source (see
            // `FileScanConfig::create_sibling_state`), so each partition reads ONLY its own file
            // group via `WorkSource::Local` — which is exactly the fixed one-group-per-task model
            // the scheduler already assigns.
            let mut builder =
                FileScanConfigBuilder::from(config.clone()).with_partitioned_by_file_group(true);
            // Parquet additionally gets the Delta schema-evolution expr adapter.
            if ds.downcast_to_file_source::<ParquetSource>().is_some() {
                builder = builder.with_expr_adapter(Some(Arc::new(DeltaPhysicalExprAdapterFactory {})));
            }
            let new_exec = DataSourceExec::from_data_source(builder.build());
            Ok(Transformed::yes(new_exec as Arc<dyn ExecutionPlan>))
        });
        Ok(result.data()?)
    }

    /// VAJ-T7 source-fusion (opt-in `VAJRA_T7_FUSE`): when a `ProjectionExec` whose single output
    /// is `from_json(value)` sits directly over a `KafkaSourceExec`, push the parse INTO the source
    /// (`with_parse_value_as`) and drop the projection. The source then parses `value` -> the struct
    /// column in-batch, so the ~10 GB raw `value:Binary` column is never materialized past batch
    /// build (REFERENCES §6: columnar end-to-end is the beat, not the parser). The fused source's
    /// output schema equals the dropped projection's, so the parent (exchange/window) is unchanged.
    ///
    /// Runs per-worker on the decoded stage plan (source + from_json live in the same pre-shuffle
    /// stage), alongside `rewrite_parquet_adapters`. Conservative: bails (plan unchanged) on any
    /// shape it does not recognize, so correctness can never regress — at worst the fast path is
    /// missed and logged. Opt-in until T2 kind + T3 EKS confirm; default is byte-identical.
    fn rewrite_source_fusion(
        plan: Arc<dyn ExecutionPlan>,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
        if std::env::var_os("VAJRA_T7_FUSE").is_none() {
            return Ok(plan);
        }
        let result = plan.transform(|node| {
            let Some(proj) = node.downcast_ref::<ProjectionExec>() else {
                return Ok(Transformed::no(node));
            };
            // The child is the (un-fused) Kafka source, possibly behind a single-child transparent
            // wrapper the optimizer inserts (e.g. `CooperativeExec` from EnforceCooperative). Unwrap
            // one level; re-wrap the fused source in it below so cooperative scheduling is preserved.
            let input = proj.input();
            let (src, wrapper): (&KafkaSourceExec, bool) =
                if let Some(s) = input.downcast_ref::<KafkaSourceExec>() {
                    (s, false)
                } else if input.children().len() == 1 {
                    match input.children()[0].downcast_ref::<KafkaSourceExec>() {
                        Some(s) => (s, true),
                        None => return Ok(Transformed::no(node)),
                    }
                } else {
                    return Ok(Transformed::no(node));
                };
            if src.parse_value_as().is_some() {
                return Ok(Transformed::no(node));
            }
            // Fusion is provably equivalent iff the projection is an IDENTITY map over the source's
            // (flow-event encoded) output EXCEPT the `value` column, which becomes
            // `from_json(CAST(value)) AS <alias>`. i.e. every other output column is an unrenamed
            // passthrough `Column@i`, and the value column carries the sole from_json. Then dropping
            // the projection and parsing `value` in-source yields the identical batch. The real plan
            // shape (T1 dump): `[_marker@0, _retracted@1, from_json(value@2) as #7, partition@3]`.
            let input_schema = proj.input().schema();
            let Ok(value_idx) = input_schema.index_of("value") else {
                return Ok(Transformed::no(node));
            };
            let exprs = proj.expr();
            let out_schema = proj.schema();
            if exprs.len() != input_schema.fields().len() {
                return Ok(Transformed::no(node));
            }
            let mut from_json: Option<(String, datafusion::arrow::datatypes::Fields)> = None;
            for (i, pe) in exprs.iter().enumerate() {
                if i == value_idx {
                    let Some(sfe) = pe.expr.downcast_ref::<ScalarFunctionExpr>() else {
                        return Ok(Transformed::no(node));
                    };
                    if sfe.fun().name() != "from_json" {
                        return Ok(Transformed::no(node));
                    }
                    let DataType::Struct(fields) = out_schema.field(i).data_type().clone() else {
                        return Ok(Transformed::no(node));
                    };
                    from_json = Some((out_schema.field(i).name().clone(), fields));
                } else {
                    // Unrenamed identity passthrough of input column i.
                    let Some(col) = pe.expr.downcast_ref::<Column>() else {
                        return Ok(Transformed::no(node));
                    };
                    if col.index() != i
                        || out_schema.field(i).name() != input_schema.field(i).name()
                    {
                        return Ok(Transformed::no(node));
                    }
                }
            }
            let Some((output_field, fields)) = from_json else {
                return Ok(Transformed::no(node));
            };
            // NOTE: options default to Spark defaults — correct for the measured benchmark (no
            // timestampFormat/dateFormat). Extend by reading the from_json options arg when targeted.
            let spec = ValueParseSpec {
                output_field: output_field.clone(),
                fields,
                options: SparkFromJsonOptions::default(),
            };
            let fused = KafkaSourceExec::try_new(
                src.options().clone(),
                src.original_schema().clone(),
                src.projection().to_vec(),
                src.bounded(),
                src.checkpoint_location().map(str::to_string),
                src.realtime_interval_ms(),
                src.parallelism(),
            )
            .and_then(|s| s.with_parse_value_as(spec))?;
            let fused_arc: Arc<dyn ExecutionPlan> = Arc::new(fused);
            // Drop the projection; keep the wrapper (if any) around the fused source.
            let new_node = if wrapper {
                input.clone().with_new_children(vec![fused_arc])?
            } else {
                fused_arc
            };
            debug!(
                "VAJ-T7 source-fusion: fused from_json -> '{output_field}' into KafkaSourceExec (value parsed in-source)"
            );
            Ok(Transformed::yes(new_node))
        });
        Ok(result.data()?)
    }

    fn rewrite_shuffle<T: Actor>(
        &mut self,
        ctx: &mut ActorContext<T>,
        key: &TaskKey,
        inputs: &[TaskInput],
        output: &TaskOutput,
        plan: Arc<dyn ExecutionPlan>,
        context: &TaskContext,
    ) -> ExecutionResult<Arc<dyn ExecutionPlan>>
    where
        T::Message: TaskRunnerMessage + StreamAccessorMessage,
    {
        let handle = ctx.handle();
        let result = plan.transform(move |node| {
            if let Some(placeholder) = node.downcast_ref::<StageInputExec<usize>>() {
                let Some(input) = inputs.get(*placeholder.input()) else {
                    return internal_err!(
                        "stage input index {} out of bounds for {}",
                        placeholder.input(),
                        TaskKeyDisplay(key)
                    );
                };
                let locations = input.locations(key.job_id);
                let accessor = StreamAccessor::new(handle.clone());
                let shuffle = ShuffleReadExec::new(
                    locations,
                    Arc::new(accessor),
                    placeholder.properties().clone(),
                );
                Ok(Transformed::yes(Arc::new(shuffle)))
            } else {
                Ok(Transformed::no(node))
            }
        });
        let plan = result.data()?;
        let schema = plan.schema();
        let accessor = StreamAccessor::new(handle.clone());
        let mut locations = vec![vec![]; plan.output_partitioning().partition_count()];
        match locations.get_mut(key.partition) {
            Some(x) => x.extend(output.locations(key)),
            None => {
                return Err(ExecutionError::InternalError(format!(
                    "invalid partition: {}",
                    TaskKeyDisplay(key)
                )));
            }
        };
        let partitioning = output.partitioning(context, &schema, self.codec.as_ref())?;
        let shuffle = ShuffleWriteExec::new(plan, locations, Arc::new(accessor), partitioning);
        Ok(Arc::new(shuffle))
    }
}
