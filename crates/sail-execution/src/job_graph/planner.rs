use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{plan_datafusion_err, JoinType, Result};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, PartitionMode, PiecewiseMergeJoinExec,
};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::recursive_query::RecursiveQueryExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{
    with_new_children_if_necessary, ExecutionPlan, ExecutionPlanProperties, PhysicalExpr,
    PlanProperties,
};
use sail_catalog_system::physical_plan::SystemTableExec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_physical_plan::catalog_command::CatalogCommandExec;
use sail_physical_plan::streaming::barrier_align::StreamBarrierAlignExec;
use sail_physical_plan::streaming::exchange::StreamExchangeExec;

use crate::error::{ExecutionError, ExecutionResult};
use crate::job_graph::{
    InputMode, JobGraph, OutputDistribution, OutputMode, Stage, StageInput, TaskPlacement,
};
use crate::plan::{ShuffleConsumption, StageInputExec};

impl JobGraph {
    pub fn try_new(plan: Arc<dyn ExecutionPlan>) -> ExecutionResult<Self> {
        // VAJ-BF2 T-BF2.2: resolve the distributed-streaming gate ONCE (not per-node).
        let distributed_stream = std::env::var("VAJRA_DISTRIBUTED_STREAM").as_deref() == Ok("1");
        Self::try_new_with_distributed_stream(plan, distributed_stream)
    }

    /// Explicit-flag entry point (deterministic; used by `try_new` after resolving the env gate,
    /// and by unit tests that must not depend on process-global env).
    pub fn try_new_with_distributed_stream(
        plan: Arc<dyn ExecutionPlan>,
        distributed_stream: bool,
    ) -> ExecutionResult<Self> {
        let plan = ensure_single_input_partition_for_global_limit(plan)?;
        let plan = ensure_partitioned_hash_join_if_build_side_emits_unmatched_rows(plan)?;
        let mut graph = Self {
            stages: vec![],
            schema: plan.schema(),
        };
        let last = build_job_graph(plan, PartitionUsage::Once, &mut graph, distributed_stream)?;
        let (last, inputs) = rewrite_inputs(last)?;
        graph.stages.push(Stage {
            inputs,
            plan: last,
            group: String::new(),
            mode: OutputMode::Pipelined,
            distribution: OutputDistribution::RoundRobin { channels: 1 },
            placement: TaskPlacement::Worker,
        });
        Ok(graph)
    }
}

fn ensure_single_input_partition_for_global_limit(
    plan: Arc<dyn ExecutionPlan>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    // Rewrite *all* `GlobalLimitExec` nodes in the tree to ensure their input is single-partition.
    let result = plan.transform(|node| {
        if let Some(gl) = node.downcast_ref::<GlobalLimitExec>() {
            let skip = gl.skip();
            let fetch = gl.fetch();
            let input = gl.input();
            if fetch.is_none() && skip == 0 {
                // If there is neither LIMIT nor OFFSET, return the node as is.
                Ok(Transformed::no(node))
            } else if input.output_partitioning().partition_count() > 1 {
                // Keep `LocalLimitExec` (if any) to preserve the per-partition top-k optimization,
                // but make sure the input to `GlobalLimitExec` is single-partition.
                let input = Arc::new(CoalescePartitionsExec::new(input.clone()));
                Ok(Transformed::yes(Arc::new(GlobalLimitExec::new(
                    input, skip, fetch,
                ))))
            } else {
                Ok(Transformed::no(node))
            }
        } else {
            Ok(Transformed::no(node))
        }
    });
    Ok(result.data()?)
}

fn ensure_partitioned_hash_join_if_build_side_emits_unmatched_rows(
    plan: Arc<dyn ExecutionPlan>,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    fn repartition(
        plan: Arc<dyn ExecutionPlan>,
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // We have to remove unnecessary repartitioning explicitly here
        // since no physical optimizer will run afterward.
        let plan = if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
            Arc::clone(coalesce.input())
        } else if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
            Arc::clone(repartition.input())
        } else {
            plan
        };
        Ok(Arc::new(RepartitionExec::try_new(
            plan,
            Partitioning::Hash(exprs, count),
        )?))
    }

    let result = plan.transform_up(|plan| {
        let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(plan));
        };

        if join.mode != PartitionMode::CollectLeft {
            return Ok(Transformed::no(plan));
        }

        if !matches!(
            join.join_type,
            JoinType::Left
                | JoinType::LeftAnti
                | JoinType::LeftSemi
                | JoinType::LeftMark
                | JoinType::Full
        ) {
            // `LEFT` or `FULL` joins need to emit unmatched rows from the build side.
            // This is not yet possible in distributed execution since the bitmap for
            // row matching is not shared across partitions.
            // So we need to turn the join into a partitioned hash join for now.
            return Ok(Transformed::no(plan));
        }

        // Convert the join to a partitioned hash join with explicit repartitioning on both sides,
        // so each output partition can be executed independently in the distributed engine.
        let partition_count = join.right.output_partitioning().partition_count();

        let (left_exprs, right_exprs): (Vec<_>, Vec<_>) = join
            .on
            .iter()
            .map(|(l, r)| (Arc::clone(l), Arc::clone(r)))
            .unzip();

        Ok(Transformed::yes(Arc::new(HashJoinExec::try_new(
            repartition(Arc::clone(&join.left), left_exprs, partition_count)?,
            repartition(Arc::clone(&join.right), right_exprs, partition_count)?,
            join.on.clone(),
            join.filter.clone(),
            &join.join_type,
            join.projection.as_deref().map(|p| p.to_vec()),
            PartitionMode::Partitioned,
            join.null_equality,
            false,
        )?)))
    })?;

    Ok(result.data)
}

/// A flag to indicate how the partitions from physical plan execution are used.
#[derive(Clone, Copy)]
enum PartitionUsage {
    /// Each partition of the plan is only used once.
    Once,
    /// The same partition may be used multiple times when producing partitions
    /// for the parent physical plan.
    ///
    /// This is typically needed for an optimized join operation where
    /// the build-side data (small) of only one partition is gathered via `plan.execute(0)`
    /// for each partition of the probe-side data.
    /// For single-host execution, DataFusion uses `OnceAsync` to ensure the
    /// build-side is only evaluated once. In the distributed setting, we use this
    /// usage information to create materialized shuffle data that can be
    /// consumed multiple times.
    Shared,
}

/// VAJ-BF2 T-BF2.2: whether to cut a **cross-node stage boundary** at a `StreamExchangeExec`,
/// so a keyed streaming windowed-aggregation distributes its N window instances across worker
/// pods (instead of collapsing the whole source→exchange→window pipeline onto one worker — the
/// root cause measured in Exp 2, docs/design/vaj-bf2-distributed-streaming.md §4c).
///
/// Grounded in the F2/F3 distributed-streaming design (docs/design/distributed-streaming-f2f3.md):
/// the existing marker-aware Hash shuffle (`ShuffleWriteExec` broadcasts watermark/checkpoint/
/// end-of-data markers, hash-routes data; `StageInputExec`/`ShuffleReadExec` receive) IS the
/// cross-node `StreamExchangeExec`.
/// - **1→N** (single-partition source): each window instance has exactly one upstream, so the
///   broadcast marker arrives once per instance — no receiver alignment needed (Flink single-input).
/// - **N→M** (multi-partition source): each consumer receives its N producer sub-streams, whose
///   markers must be **MIN-merged (watermarks) + Chandy-Lamport aligned (barriers)** at the receiver.
///   As of T-BF2.3b the streaming `ShuffleReadExec` does exactly this (`merge_flow_event_streams` =
///   the exchange's validated receiver logic), so N→M is now safe to cut too. Paired with T-BF2.5
///   even-spread placement, the M window instances distribute across workers.
///
/// Opt-in via `VAJRA_DISTRIBUTED_STREAM=1` — the default keeps the in-process `StreamExchangeExec`
/// (the F2/F3-validated single-stage path), so this is additive and reversible. The flag is resolved
/// ONCE at `JobGraph::try_new` and threaded in (not read per-node), so it is deterministic and unit
/// testable via `try_new_with_distributed_stream`.
fn distributed_stream_boundary(plan: &Arc<dyn ExecutionPlan>, distributed_stream: bool) -> bool {
    if !distributed_stream {
        return false;
    }
    // Two streaming stage boundaries:
    // - `StreamExchangeExec` (keyed N→M exchange): properties = `Hash(keys, N)` → an N-partition Hash
    //   shuffle; the marker-aware read aligns N producer sub-streams (T-BF2.2/2.3).
    // - `StreamBarrierAlignExec` (N→1 funnel before the sink): properties = `UnknownPartitioning(1)` →
    //   a RoundRobin{1} funnel shuffle. This is the streaming analog of `CoalescePartitionsExec` (also
    //   a boundary): cutting here makes the CHILD (`WindowAccumExec`, N partitions) run as N DISTRIBUTED
    //   tasks instead of collapsing onto the single funnel task (T2-measured: 8 window instances on one
    //   pod). The N→1 barrier-align + watermark MIN that `StreamBarrierAlign` did in-process is then
    //   performed by the aligning shuffle read (`merge_flow_event_streams`, a proven superset) — so the
    //   funnel node is subsumed by the shuffle. (T-BF2.6.)
    plan.is::<StreamExchangeExec>() || plan.is::<StreamBarrierAlignExec>()
}

fn build_job_graph(
    plan: Arc<dyn ExecutionPlan>,
    usage: PartitionUsage,
    graph: &mut JobGraph,
    distributed_stream: bool,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    // Recursively build the job graph for the children first
    // and propagate partition usage information.
    let children = if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        let (left, right) = join.children().two()?;
        match join.mode {
            PartitionMode::Partitioned => {
                vec![
                    build_job_graph(left.clone(), usage, graph, distributed_stream)?,
                    build_job_graph(right.clone(), usage, graph, distributed_stream)?,
                ]
            }
            PartitionMode::CollectLeft => {
                vec![
                    build_job_graph(left.clone(), PartitionUsage::Shared, graph, distributed_stream)?,
                    build_job_graph(right.clone(), usage, graph, distributed_stream)?,
                ]
            }
            PartitionMode::Auto => {
                return Err(ExecutionError::DataFusionError(plan_datafusion_err!(
                    "unresolved auto partition mode in hash join"
                )));
            }
        }
    } else if plan.is::<NestedLoopJoinExec>()
        || plan.is::<CrossJoinExec>()
        || plan.is::<PiecewiseMergeJoinExec>()
    {
        let (left, right) = plan.children().two()?;
        vec![
            build_job_graph(left.clone(), PartitionUsage::Shared, graph, distributed_stream)?,
            build_job_graph(right.clone(), usage, graph, distributed_stream)?,
        ]
    } else if plan.is::<RepartitionExec>()
        || plan.is::<CoalescePartitionsExec>()
        || plan.is::<SortPreservingMergeExec>()
        || distributed_stream_boundary(&plan, distributed_stream)
    {
        let child = plan.children().one()?;
        // At the stage boundary, we only expect to use the child partition once
        // since the shuffle writer can materialize the data for multiple consumption.
        vec![build_job_graph(child.clone(), PartitionUsage::Once, graph, distributed_stream)?]
    } else if plan.is::<RecursiveQueryExec>() {
        // Recursive queries maintain a shared work table mutated across iterations
        // by RecursiveQueryExec and read by a WorkTableExec in the recursive term.
        // Both must run in the same task — splitting them across stages causes
        // "Unexpected empty work table". Keep the whole subtree intact (no shuffle
        // boundary inside the recursion).
        plan.children().into_iter().cloned().collect()
    } else {
        plan.children()
            .into_iter()
            .map(|x| build_job_graph(x.clone(), usage, graph, distributed_stream))
            .collect::<ExecutionResult<Vec<_>>>()?
    };
    let plan = with_new_children_if_necessary(plan, children)?;

    let consumption = match usage {
        PartitionUsage::Once => ShuffleConsumption::Single,
        PartitionUsage::Shared => ShuffleConsumption::Multiple,
    };
    let plan = if let Some(repartition) = plan.downcast_ref::<RepartitionExec>() {
        if repartition.preserve_order() {
            // We haven't found a case when order-preserving repartition can be constructed,
            // so it's fine to return an error for now.
            // TODO: support order-preserving repartition
            return Err(ExecutionError::InternalError(
                "repartition is order-preserving and would result in incorrect results in distributed execution".to_string()
            ));
        }
        let properties = repartition.properties().clone();
        let child = plan.children().one()?;
        match &properties.partitioning {
            Partitioning::UnknownPartitioning(n) => {
                let n = *n;
                let properties = Arc::new(
                    properties
                        .as_ref()
                        .clone()
                        .with_partitioning(Partitioning::RoundRobinBatch(n)),
                );
                create_shuffle(child, graph, properties, consumption)?
            }
            Partitioning::RoundRobinBatch(_) | Partitioning::Hash(_, _) => {
                create_shuffle(child, graph, properties, consumption)?
            }
        }
    } else if let Some(coalesce) = plan.downcast_ref::<CoalescePartitionsExec>() {
        let properties = coalesce.properties().clone();
        let child = plan.children().one()?;
        let fetch = coalesce.fetch();
        let shuffled = create_shuffle(child, graph, properties, consumption)?;
        if let Some(f) = fetch {
            Arc::new(GlobalLimitExec::new(shuffled, 0, Some(f))) as Arc<dyn ExecutionPlan>
        } else {
            shuffled
        }
    } else if plan.is::<SortPreservingMergeExec>() {
        let child = plan.children().one()?;
        plan.clone()
            .with_new_children(vec![create_merge_input(child, graph)?])?
    } else if distributed_stream_boundary(&plan, distributed_stream) {
        // VAJ-BF2 T-BF2.2/2.6: cut a streaming stage boundary. The node's own `properties` carry the
        // output partitioning `create_shuffle` needs — `Hash(keys, N)` for `StreamExchangeExec` (N→M
        // keyed shuffle; distributes the source), `UnknownPartitioning(1)` → `RoundRobin{1}` for
        // `StreamBarrierAlignExec` (N→1 funnel; makes the child `WindowAccum` run as N distributed
        // tasks — see `distributed_stream_boundary`). The marker-aware `ShuffleWriteExec` broadcasts
        // barriers/watermarks while routing data, and the aligning shuffle read (`merge_flow_event_
        // streams`) MIN-merges watermarks + aligns barriers on the consumer side. The boundary node is
        // replaced by the `StageInputExec`; the shuffle performs the fan-out / funnel.
        let properties = plan.properties().clone();
        let child = plan.children().one()?;
        create_shuffle(child, graph, properties, consumption)?
    } else if plan.is::<SystemTableExec>() || plan.is::<CatalogCommandExec>() {
        plan.children().zero()?;
        create_driver_stage(&plan, graph)?
    } else {
        plan
    };
    Ok(plan)
}

fn create_merge_input(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let properties = plan.properties().clone();
    let (plan, inputs) = rewrite_inputs(plan.clone())?;
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode: OutputMode::Pipelined,
        distribution: OutputDistribution::RoundRobin { channels: 1 },
        placement: TaskPlacement::Worker,
    };
    let s = graph.stages.len();
    graph.stages.push(stage);
    Ok(Arc::new(StageInputExec::new(
        StageInput {
            stage: s,
            mode: InputMode::Merge,
        },
        properties,
    )))
}

fn create_shuffle(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
    // These are the properties after repartition/coalesce,
    // which are different from the properties of the input plan.
    properties: Arc<PlanProperties>,
    consumption: ShuffleConsumption,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let distribution = match properties.partitioning.clone() {
        Partitioning::RoundRobinBatch(channels) | Partitioning::UnknownPartitioning(channels) => {
            OutputDistribution::RoundRobin { channels }
        }
        Partitioning::Hash(keys, channels) => OutputDistribution::Hash { keys, channels },
    };
    let (plan, inputs) = rewrite_inputs(plan.clone())?;
    let stage = Stage {
        inputs,
        plan,
        group: String::new(),
        mode: OutputMode::Pipelined,
        distribution,
        placement: TaskPlacement::Worker,
    };
    let s = graph.stages.len();
    graph.stages.push(stage);
    let mode = match consumption {
        ShuffleConsumption::Single => InputMode::Shuffle,
        ShuffleConsumption::Multiple => InputMode::Broadcast,
    };
    Ok(Arc::new(StageInputExec::new(
        StageInput { stage: s, mode },
        properties,
    )))
}

fn rewrite_inputs(
    plan: Arc<dyn ExecutionPlan>,
) -> ExecutionResult<(Arc<dyn ExecutionPlan>, Vec<StageInput>)> {
    let mut inputs = vec![];
    let result = plan.transform(|node| {
        if let Some(placeholder) = node.downcast_ref::<StageInputExec<StageInput>>() {
            let index = inputs.len();
            inputs.push(placeholder.input().clone());
            let placeholder = StageInputExec::new(index, placeholder.properties().clone());
            Ok(Transformed::yes(Arc::new(placeholder)))
        } else {
            Ok(Transformed::no(node))
        }
    });
    Ok((result.data()?, inputs))
}

// TODO: support driver stage with inputs
fn create_driver_stage(
    plan: &Arc<dyn ExecutionPlan>,
    graph: &mut JobGraph,
) -> ExecutionResult<Arc<dyn ExecutionPlan>> {
    let stage = Stage {
        inputs: vec![],
        plan: plan.clone(),
        group: String::new(),
        mode: OutputMode::Pipelined,
        distribution: OutputDistribution::RoundRobin { channels: 1 },
        placement: TaskPlacement::Driver,
    };
    let s = graph.stages.len();
    graph.stages.push(stage);
    Ok(Arc::new(StageInputExec::new(
        StageInput {
            stage: s,
            mode: InputMode::Forward,
        },
        plan.properties().clone(),
    )))
}

#[cfg(test)]
mod tests {
    #[expect(clippy::unwrap_used)]
    mod stage_boundary {
        use std::sync::Arc;

        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::physical_expr::expressions::Column;
        use datafusion::physical_plan::empty::EmptyExec;
        use datafusion::physical_plan::ExecutionPlan;
        use sail_common_datafusion::streaming::event::schema::{
            MARKER_FIELD_NAME, RETRACTED_FIELD_NAME,
        };
        use sail_physical_plan::streaming::barrier_align::StreamBarrierAlignExec;
        use sail_physical_plan::streaming::exchange::StreamExchangeExec;

        use crate::job_graph::JobGraph;

        fn flow_schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new(MARKER_FIELD_NAME, DataType::Binary, true),
                Field::new(RETRACTED_FIELD_NAME, DataType::Boolean, false),
                Field::new("k", DataType::Int64, true),
            ]))
        }

        /// A `StreamExchangeExec` over a single-partition (flow-event) source — the 1→N keyed
        /// exchange the realtime Kafka windowed-agg builds (WindowAccum sits above it; here we
        /// test the exchange boundary in isolation).
        fn single_partition_stream_exchange() -> Arc<dyn ExecutionPlan> {
            // EmptyExec is single-partition => the align-free 1→N case.
            let child: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(flow_schema()));
            Arc::new(
                StreamExchangeExec::try_new(child, vec![Arc::new(Column::new("k", 2))], 3).unwrap(),
            )
        }

        /// A `StreamBarrierAlignExec` (the N→1 funnel that sits above `WindowAccum` before the sink).
        /// Cutting here makes the funnel's CHILD (the window) run as its own distributed stage.
        fn stream_barrier_align_funnel() -> Arc<dyn ExecutionPlan> {
            let child: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(flow_schema()));
            Arc::new(StreamBarrierAlignExec::new(child))
        }

        // VAJ-BF2 T-BF2.2: with the gate OFF the streaming exchange stays INLINE (one stage — the
        // validated F2/F3 default); with the gate ON it is cut into a cross-node shuffle (the source
        // becomes its own producer stage), so the N window instances can distribute across workers.
        #[test]
        fn gate_off_keeps_stream_exchange_inline_single_stage() {
            let graph =
                JobGraph::try_new_with_distributed_stream(single_partition_stream_exchange(), false)
                    .unwrap();
            assert_eq!(graph.stages.len(), 1, "gate off must not cut a stage boundary");
        }

        #[test]
        fn gate_on_cuts_stream_exchange_stage_boundary() {
            let graph =
                JobGraph::try_new_with_distributed_stream(single_partition_stream_exchange(), true)
                    .unwrap();
            assert_eq!(
                graph.stages.len(),
                2,
                "gate on must cut the streaming exchange into a producer + consumer stage"
            );
        }

        // VAJ-BF2 T-BF2.6: the N→1 `StreamBarrierAlignExec` funnel is a stage boundary when gated, so
        // its child (`WindowAccum`) distributes as N tasks instead of collapsing onto the funnel task.
        #[test]
        fn gate_off_keeps_stream_barrier_align_inline() {
            let graph =
                JobGraph::try_new_with_distributed_stream(stream_barrier_align_funnel(), false)
                    .unwrap();
            assert_eq!(graph.stages.len(), 1, "gate off must not cut the funnel");
        }

        #[test]
        fn gate_on_cuts_stream_barrier_align_funnel_boundary() {
            let graph =
                JobGraph::try_new_with_distributed_stream(stream_barrier_align_funnel(), true)
                    .unwrap();
            assert_eq!(
                graph.stages.len(),
                2,
                "gate on must cut the funnel so the window child runs as its own distributed stage"
            );
        }
    }
}
