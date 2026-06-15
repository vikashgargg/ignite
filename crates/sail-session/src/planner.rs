use std::sync::Arc;

use crate::foreach_batch_exec::ForeachBatchSinkExec;
use crate::memory_sink_exec::MemorySinkExec;
use async_trait::async_trait;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::SessionState;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{internal_datafusion_err, internal_err, DFSchema, ToDFSchema};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_expr::{create_physical_sort_exprs, Partitioning};
use sail_catalog::manager::CatalogManager;
use sail_catalog_system::planner::SystemTablePhysicalPlanner;
use sail_common_datafusion::catalog::TableKind;
use sail_common_datafusion::datasource::{SourceInfo, TableFormatRegistry};
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_common_datafusion::logical_rewriter::LogicalRewriter;
use sail_common_datafusion::rename::physical_plan::rename_projected_physical_plan;
use sail_common_datafusion::streaming::event::schema::{
    to_flow_event_field_names, to_flow_event_projection,
};
use sail_logical_plan::barrier::BarrierNode;
use sail_logical_plan::file_delete::FileDeleteNode;
use sail_logical_plan::file_write::FileWriteNode;
use sail_logical_plan::map_partitions::MapPartitionsNode;
use sail_logical_plan::merge::MergeIntoNode;
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::range::RangeNode;
use sail_logical_plan::repartition::ExplicitRepartitionNode;
use sail_logical_plan::schema_pivot::SchemaPivotNode;
use sail_logical_plan::show_string::ShowStringNode;
use sail_logical_plan::sort::SortWithinPartitionsNode;
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;
use sail_logical_plan::streaming::collector::StreamCollectorNode;
use sail_logical_plan::streaming::dedup::StreamDeduplicateNode;
use sail_logical_plan::streaming::filter::StreamFilterNode;
use sail_logical_plan::streaming::foreach_batch::ForeachBatchSinkNode;
use sail_logical_plan::streaming::limit::StreamLimitNode;
use sail_logical_plan::streaming::memory_sink::MemorySinkNode;
use sail_logical_plan::streaming::source_adapter::StreamSourceAdapterNode;
use sail_logical_plan::streaming::source_wrapper::StreamSourceWrapperNode;
use sail_logical_plan::streaming::stream_join::StreamJoinNode;
use sail_logical_plan::streaming::watermark::WatermarkNode;
use sail_logical_plan::streaming::window_accum::WindowAccumNode;
use sail_physical_plan::barrier::BarrierExec;
use sail_physical_plan::catalog_command::CatalogCommandExec;
use sail_physical_plan::file_delete::create_file_delete_physical_plan;
use sail_physical_plan::file_write::create_file_write_physical_plan;
use sail_physical_plan::map_partitions::MapPartitionsExec;
use sail_physical_plan::monotonic_id::MonotonicIdExec;
use sail_physical_plan::range::RangeExec;
use sail_physical_plan::repartition::ExplicitRepartitionExec;
use sail_physical_plan::schema_pivot::SchemaPivotExec;
use sail_physical_plan::show_string::ShowStringExec;
use sail_physical_plan::spark_partition_id::SparkPartitionIdExec;
use sail_physical_plan::streaming::collector::StreamCollectorExec;
use sail_physical_plan::streaming::dedup::StreamDeduplicateExec;
use sail_physical_plan::streaming::filter::StreamFilterExec;
use sail_physical_plan::streaming::limit::StreamLimitExec;
use sail_physical_plan::streaming::source_adapter::StreamSourceAdapterExec;
use sail_physical_plan::streaming::barrier_align::StreamBarrierAlignExec;
use sail_physical_plan::streaming::exchange::StreamExchangeExec;
use sail_physical_plan::streaming::stream_join::StreamJoinExec;
use sail_physical_plan::streaming::watermark::WatermarkExec;
use sail_physical_plan::streaming::window_accum::WindowAccumExec;
use sail_plan::catalog::CatalogCommandNode;
use sail_plan_lakehouse::new_lakehouse_extension_planners;

#[derive(Debug)]
pub struct ExtensionQueryPlanner {}

#[async_trait]
impl QueryPlanner for ExtensionQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // TODO: show rewriters and the final logical plan in `EXPLAIN`
        // Note: the rewriter list is currently empty but may be useful for future logical rewrites.
        let rewriters: Vec<Box<dyn LogicalRewriter>> = vec![];
        let mut logical_plan = logical_plan.clone();
        for rewriter in rewriters {
            logical_plan = rewriter.rewrite(logical_plan)?.data
        }
        let mut extension_planners = new_lakehouse_extension_planners();
        extension_planners.push(Arc::new(SystemTablePhysicalPlanner));
        extension_planners.push(Arc::new(ExtensionPhysicalPlanner));
        let planner = DefaultPhysicalPlanner::with_extension_planners(extension_planners);
        planner
            .create_physical_plan(&logical_plan, session_state)
            .await
    }
}

pub struct ExtensionPhysicalPlanner;

#[async_trait]
impl ExtensionPlanner for ExtensionPhysicalPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion_common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let plan: Arc<dyn ExecutionPlan> = if let Some(node) =
            node.as_any().downcast_ref::<RangeNode>()
        {
            let schema = UserDefinedLogicalNode::schema(node).inner().clone();
            let projection = (0..schema.fields().len()).collect();
            Arc::new(RangeExec::try_new(
                node.range().clone(),
                node.num_partitions(),
                schema,
                projection,
            )?)
        } else if let Some(node) = node.as_any().downcast_ref::<ShowStringNode>() {
            let [input] = physical_inputs else {
                return internal_err!("ShowStringExec requires exactly one physical input");
            };
            Arc::new(ShowStringExec::new(
                input.clone(),
                node.names().to_vec(),
                node.limit(),
                node.format().clone(),
                UserDefinedLogicalNode::schema(node).inner().clone(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<MapPartitionsNode>() {
            let [input] = physical_inputs else {
                return internal_err!("MapPartitionsExec requires exactly one physical input");
            };
            Arc::new(MapPartitionsExec::new(
                input.clone(),
                node.udf().clone(),
                UserDefinedLogicalNode::schema(node).inner().clone(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<MonotonicIdNode>() {
            let [input] = physical_inputs else {
                return internal_err!("MonotonicIdExec requires exactly one physical input");
            };
            Arc::new(MonotonicIdExec::try_new(
                input.clone(),
                node.column_name().to_string(),
                UserDefinedLogicalNode::schema(node).inner().clone(),
            )?)
        } else if let Some(node) = node.as_any().downcast_ref::<SparkPartitionIdNode>() {
            let [input] = physical_inputs else {
                return internal_err!("SparkPartitionIdExec requires exactly one physical input");
            };
            Arc::new(SparkPartitionIdExec::try_new(
                input.clone(),
                node.column_name().to_string(),
                UserDefinedLogicalNode::schema(node).inner().clone(),
            )?)
        } else if let Some(node) = node.as_any().downcast_ref::<SortWithinPartitionsNode>() {
            let [input] = physical_inputs else {
                return internal_err!("SortExec requires exactly one physical input");
            };
            let expr = create_physical_sort_exprs(
                node.sort_expr(),
                UserDefinedLogicalNode::schema(node),
                session_state.execution_props(),
            )?;
            let Some(ordering) = LexOrdering::new(expr) else {
                return internal_err!("SortExec requires at least one sort expression");
            };
            let sort = SortExec::new(ordering, input.clone())
                .with_fetch(node.fetch())
                .with_preserve_partitioning(true);
            Arc::new(sort)
        } else if let Some(node) = node.as_any().downcast_ref::<SchemaPivotNode>() {
            let [input] = physical_inputs else {
                return internal_err!("SchemaPivotExec requires exactly one physical input");
            };
            Arc::new(SchemaPivotExec::new(
                input.clone(),
                node.names().to_vec(),
                node.schema().inner().clone(),
            ))
        } else if let Some(node) = node.as_any().downcast_ref::<FileWriteNode>() {
            let [logical_input] = logical_inputs else {
                return internal_err!("FileWriteNode requires exactly one logical input");
            };
            let [physical_input] = physical_inputs else {
                return internal_err!("FileWriteNode requires exactly one physical input");
            };
            create_file_write_physical_plan(
                session_state,
                planner,
                logical_input,
                physical_input.clone(),
                node.options().clone(),
                node.declared_schema().cloned(),
            )
            .await?
        } else if let Some(node) = node.as_any().downcast_ref::<FileDeleteNode>() {
            if !logical_inputs.is_empty() || !physical_inputs.is_empty() {
                return internal_err!("FileDeleteNode should have no inputs");
            }
            // Create a dummy logical plan for schema context
            let catalog_manager = session_state
                .config()
                .get_extension::<CatalogManager>()
                .ok_or_else(|| internal_datafusion_err!("CatalogManager extension not found"))?;
            let table_status = catalog_manager
                .get_table_or_view(&node.options().table_name)
                .await
                .map_err(|e| internal_datafusion_err!("Failed to get table: {e}"))?;

            let schema = match &table_status.kind {
                TableKind::Table {
                    columns,
                    format,
                    location,
                    ..
                } if columns.is_empty() && format.eq_ignore_ascii_case("DELTA") => {
                    let Some(location) = location.as_ref() else {
                        return internal_err!("Table for delete has no location");
                    };
                    let source_info = SourceInfo {
                        paths: vec![location.clone()],
                        schema: None,
                        constraints: Default::default(),
                        partition_by: vec![],
                        bucket_by: None,
                        sort_order: vec![],
                        options: vec![],
                        is_streaming: false,
                    };
                    let registry = session_state.extension::<TableFormatRegistry>()?;
                    let source = registry
                        .get(format)?
                        .create_source(session_state, source_info)
                        .await?;
                    Ok(source.schema().to_dfschema_ref()?)
                }
                TableKind::Table { columns, .. } => {
                    let schema = datafusion::arrow::datatypes::Schema::new(
                        columns.iter().map(|c| c.field()).collect::<Vec<_>>(),
                    );
                    Ok(schema.to_dfschema_ref()?)
                }
                _ => internal_err!("Expected a table for DELETE"),
            }?;
            create_file_delete_physical_plan(session_state, planner, schema, node.options().clone())
                .await?
        } else if let Some(node) = node.as_any().downcast_ref::<MergeIntoNode>() {
            let _ = (
                planner,
                logical_inputs,
                physical_inputs,
                session_state,
                node,
            );
            return internal_err!(
                "MERGE planning expects a pre-expanded logical plan (RowLevelWriteNode). \
Ensure expand_row_level_op is enabled; MERGE is currently only supported for lakehouse tables."
            );
        } else if let Some(node) = node.as_any().downcast_ref::<ExplicitRepartitionNode>() {
            let [input] = physical_inputs else {
                return internal_err!(
                    "ExplicitRepartitionExec requires exactly one physical input"
                );
            };
            let partitioning = plan_explicit_partitioning(
                planner,
                UserDefinedLogicalNode::schema(node),
                input.as_ref(),
                node.num_partitions(),
                node.partitioning_expressions(),
                session_state,
            )?;
            Arc::new(ExplicitRepartitionExec::new(input.clone(), partitioning))
        } else if node.as_any().is::<StreamSourceAdapterNode>() {
            let [input] = physical_inputs else {
                return internal_err!("StreamSourceExec requires exactly one physical input");
            };
            Arc::new(StreamSourceAdapterExec::new(input.clone()))
        } else if let Some(node) = node.as_any().downcast_ref::<StreamSourceWrapperNode>() {
            let plan = node
                .source()
                .scan(
                    session_state,
                    node.projection(),
                    node.filters(),
                    node.fetch(),
                    node.bounded(),
                    node.checkpoint_location(),
                    node.realtime_interval_ms(),
                )
                .await?;
            match node.names() {
                Some(names) => {
                    let names = to_flow_event_field_names(names);
                    let projection = node.projection().map(|x| to_flow_event_projection(x));
                    rename_projected_physical_plan(plan, &names, projection.as_ref())?
                }
                None => plan,
            }
        } else if let Some(node) = node.as_any().downcast_ref::<StreamLimitNode>() {
            let [input] = physical_inputs else {
                return internal_err!("StreamLimitExec requires exactly one physical input");
            };
            Arc::new(StreamLimitExec::try_new(
                input.clone(),
                node.skip(),
                node.fetch(),
            )?)
        } else if let Some(node) = node.as_any().downcast_ref::<StreamFilterNode>() {
            let [logical_input] = logical_inputs else {
                return internal_err!("StreamFilterExec requires exactly one logical input");
            };
            let [input] = physical_inputs else {
                return internal_err!("StreamFilterExec requires exactly one physical input");
            };
            let predicate = planner.create_physical_expr(
                node.predicate(),
                logical_input.schema(),
                session_state,
            )?;
            Arc::new(StreamFilterExec::try_new(input.clone(), predicate)?)
        } else if node.as_any().is::<StreamCollectorNode>() {
            let [input] = physical_inputs else {
                return internal_err!("StreamCollectorExec requires exactly one physical input");
            };
            Arc::new(StreamCollectorExec::try_new(input.clone())?)
        } else if let Some(node) = node.as_any().downcast_ref::<WatermarkNode>() {
            // WatermarkExec emits in-band watermark markers from the raw event-time
            // column (still present here, below the window-folding projection).
            let [input] = physical_inputs else {
                return internal_err!("WatermarkNode requires exactly one physical input");
            };
            let data_schema = Arc::new(
                sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema(
                    &input.schema(),
                )
                .map_err(|e| {
                    let names: Vec<_> = input
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name().clone())
                        .collect();
                    internal_datafusion_err!("WatermarkExec input schema {names:?}: {e}")
                })?,
            );
            Arc::new(WatermarkExec::try_new(
                input.clone(),
                node.event_time_col.clone(),
                node.delay_micros,
                data_schema,
            )?)
        } else if let Some(node) = node.as_any().downcast_ref::<StreamDeduplicateNode>() {
            let [input] = physical_inputs else {
                return internal_err!("StreamDeduplicateExec requires exactly one physical input");
            };
            let [logical_input] = logical_inputs else {
                return internal_err!("StreamDeduplicateExec requires exactly one logical input");
            };
            let data_schema = Arc::new(
                sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema(
                    logical_input.schema().inner(),
                )?,
            );
            Arc::new(StreamDeduplicateExec::try_new(
                input.clone(),
                node.key_cols.clone(),
                node.event_time_col.clone(),
                data_schema,
            )?)
        } else if let Some(node) = node.as_any().downcast_ref::<WindowAccumNode>() {
            let [input] = physical_inputs else {
                return internal_err!("WindowAccumExec requires exactly one physical input");
            };
            let [logical_input] = logical_inputs else {
                return internal_err!("WindowAccumExec requires exactly one logical input");
            };
            // The input is a flow-event stream (so Watermark markers reach the
            // operator), but the aggregate runs on the *decoded* data batches — so
            // build the group/aggregate expressions against the data schema, not the
            // flow-event input schema.
            let data_schema = Arc::new(
                sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema(
                    logical_input.schema().inner(),
                )?,
            );
            let data_dfschema =
                Arc::new(datafusion_common::DFSchema::try_from(data_schema.as_ref().clone())?);
            // Build physical group expressions.
            let group_exprs: Vec<(Arc<dyn datafusion_physical_expr::PhysicalExpr>, String)> = node
                .group_exprs
                .iter()
                .map(|e| {
                    let phys = planner.create_physical_expr(e, &data_dfschema, session_state)?;
                    let name = e.name_for_alias().unwrap_or_else(|_| "__group".to_string());
                    Ok((phys, name))
                })
                .collect::<datafusion_common::Result<_>>()?;
            // Intra-node parallelism (docs/design/streaming-parallelism.md, Phase 2).
            // Cost-based gate: only parallelize KEYED windowed aggregation (a group key
            // beyond the window); window-only has one group per window and gains nothing.
            let window_parallelism = if node.group_exprs.len() > 1 {
                session_state.config().target_partitions().max(1)
            } else {
                1
            };
            // Hash keys = the GROUP-BY keys (group_exprs after the window at index 0).
            // Hashing by keys (not the window struct) is correct — same key -> same
            // partition, and each window for that key is handled within one partition via the
            // broadcast watermark. Shift Column indices by the flow-event prefix
            // (`_marker`,`_retracted` = 2) + rename to the flow-event field, since the
            // exchange runs on the flow-event input while group exprs target the data schema.
            let window_hash_keys: Vec<Arc<dyn datafusion_physical_expr::PhysicalExpr>> =
                if window_parallelism > 1 {
                    group_exprs
                        .iter()
                        .skip(1)
                        .map(|(e, _)| shift_physical_columns(Arc::clone(e), 2, &input.schema()))
                        .collect::<datafusion_common::Result<_>>()?
                } else {
                    vec![]
                };
            let physical_group_by =
                datafusion::physical_plan::aggregates::PhysicalGroupBy::new_single(group_exprs);
            // Build physical aggregate function expressions.
            let aggr_exprs: Vec<Arc<datafusion::physical_expr::aggregate::AggregateFunctionExpr>> =
                node.aggr_exprs
                    .iter()
                    .map(|e| {
                        let (agg_expr, _filter, _order_bys) =
                            datafusion::physical_planner::create_aggregate_expr_and_maybe_filter(
                                e,
                                &data_dfschema,
                                &data_schema,
                                session_state.execution_props(),
                            )?;
                        Ok(agg_expr)
                    })
                    .collect::<datafusion_common::Result<_>>()?;
            // Parallel keyed path: hash the (watermarked) input by group key into N
            // partitions, run N independent window instances, then merge for the sink.
            let window_input: Arc<dyn ExecutionPlan> = if window_parallelism > 1 {
                Arc::new(StreamExchangeExec::try_new(
                    input.clone(),
                    window_hash_keys,
                    window_parallelism,
                )?)
            } else {
                input.clone()
            };
            let window: Arc<dyn ExecutionPlan> = Arc::new(WindowAccumExec::try_new(
                window_input,
                physical_group_by,
                aggr_exprs,
                data_schema,
                node.event_time_col.clone(),
                node.delay_micros,
                node.checkpoint_location.clone(),
            )?);
            if window_parallelism > 1 {
                // Barrier-aligning N->1 merge: a strict superset of a plain coalesce — it fans the N
                // keyed window instances back into one partition AND aligns broadcast
                // `Checkpoint{epoch}` barriers (collect from all N before forwarding one), so the
                // downstream commit sees a globally-consistent epoch. For micro-batch (no epoch
                // barriers) it behaves exactly like the prior coalesce. This is the F3 alignment
                // primitive wired into a real parallel streaming plan (Flink "merger"/RisingWave).
                Arc::new(StreamBarrierAlignExec::new(window))
            } else {
                window
            }
        } else if let Some(node) = node.as_any().downcast_ref::<StreamJoinNode>() {
            let [left, right] = physical_inputs else {
                return internal_err!("StreamJoinExec requires exactly two physical inputs");
            };
            let [left_logical, right_logical] = logical_inputs else {
                return internal_err!("StreamJoinExec requires exactly two logical inputs");
            };
            // Inputs are flow-event streams; the join runs on the decoded data, so build
            // the equi-key expressions against each side's data schema.
            let left_data = Arc::new(
                sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema(
                    left_logical.schema().inner(),
                )?,
            );
            let right_data = Arc::new(
                sail_common_datafusion::streaming::event::schema::try_from_flow_event_schema(
                    right_logical.schema().inner(),
                )?,
            );
            let left_df = Arc::new(datafusion_common::DFSchema::try_from(left_data.as_ref().clone())?);
            let right_df =
                Arc::new(datafusion_common::DFSchema::try_from(right_data.as_ref().clone())?);
            // The join `on`/`filter` come from an explicit equality condition, so their
            // column refs are relation-qualified (e.g. `a."#2"`), but each side's data
            // schema is unqualified. Internal `#N` ids are globally unique, so strip the
            // relation qualifier before resolving against the (unqualified) schemas.
            let strip_quals = |e: &datafusion_expr::Expr| -> datafusion_expr::Expr {
                e.clone()
                    .transform(|node| {
                        Ok(match node {
                            datafusion_expr::Expr::Column(c) => datafusion_common::tree_node::Transformed::yes(
                                datafusion_expr::Expr::Column(datafusion_common::Column::new(
                                    None::<datafusion_common::TableReference>,
                                    c.name,
                                )),
                            ),
                            other => datafusion_common::tree_node::Transformed::no(other),
                        })
                    })
                    .map(|t| t.data)
                    .unwrap_or_else(|_| e.clone())
            };
            let on = node
                .on
                .iter()
                .map(|(l, r)| {
                    let lp = planner.create_physical_expr(&strip_quals(l), &left_df, session_state)?;
                    let rp = planner.create_physical_expr(&strip_quals(r), &right_df, session_state)?;
                    Ok((lp, rp))
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            // Residual (interval) filter is built against the inner-join output schema
            // (left data columns ++ right data columns) and applied to matched pairs.
            let mut out_fields = left_data.fields().iter().cloned().collect::<Vec<_>>();
            out_fields.extend(right_data.fields().iter().cloned());
            let out_schema = datafusion::arrow::datatypes::Schema::new(out_fields);
            let out_df = Arc::new(datafusion_common::DFSchema::try_from(out_schema)?);
            let filter = node
                .filter
                .as_ref()
                .map(|f| planner.create_physical_expr(&strip_quals(f), &out_df, session_state))
                .transpose()?;
            // Resolve event-time column indices (for interval-join state eviction).
            let left_ts_idx = node
                .left_event_time
                .as_ref()
                .and_then(|c| left_data.index_of(c).ok());
            let right_ts_idx = node
                .right_event_time
                .as_ref()
                .and_then(|c| right_data.index_of(c).ok());
            Arc::new(StreamJoinExec::try_new(
                left.clone(),
                right.clone(),
                on,
                node.join_type,
                left_data,
                right_data,
                filter,
                node.interval_bounds,
                left_ts_idx,
                right_ts_idx,
                node.checkpoint_location.clone(),
            )?)
        } else if let Some(node) = node.as_any().downcast_ref::<CatalogCommandNode>() {
            let schema = node.schema().inner().clone();
            Arc::new(CatalogCommandExec::new(node.command().clone(), schema))
        } else if let Some(node) = node.as_any().downcast_ref::<ForeachBatchSinkNode>() {
            let [input] = physical_inputs else {
                return internal_err!("ForeachBatchSinkExec requires exactly one physical input");
            };
            Arc::new(ForeachBatchSinkExec::new(
                input.clone(),
                node.command.clone(),
                node.eval_type,
            )?)
        } else if let Some(node) = node.as_any().downcast_ref::<MemorySinkNode>() {
            let [input] = physical_inputs else {
                return internal_err!("MemorySinkExec requires exactly one physical input");
            };
            // The shared buffer handle is carried directly on the node (and also
            // backs the queryable temporary view), so no catalog lookup is needed.
            Arc::new(MemorySinkExec::new(input.clone(), node.buffer().clone()))
        } else if let Some(_node) = node.as_any().downcast_ref::<BarrierNode>() {
            let (plan, preconditions) = physical_inputs.split_last().ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(format!(
                    "{} requires at least one physical input",
                    BarrierExec::static_name()
                ))
            })?;
            if preconditions.is_empty() {
                plan.clone()
            } else {
                Arc::new(BarrierExec::new(preconditions.to_vec(), plan.clone()))
            }
        } else {
            return internal_err!("unsupported logical extension node: {:?}", node);
        };
        Ok(Some(plan))
    }
}

/// Retarget a physical expression's `Column`s onto a different schema by shifting their
/// index by `by` and renaming to the target schema's field at the new index. Moves
/// data-schema group-key exprs onto the flow-event input schema (offset 2 for
/// `_marker`/`_retracted`) for the streaming keyed exchange.
fn shift_physical_columns(
    expr: Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    by: usize,
    target: &datafusion::arrow::datatypes::Schema,
) -> datafusion_common::Result<Arc<dyn datafusion_physical_expr::PhysicalExpr>> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::physical_expr::expressions::Column;
    expr.transform_down(|e| {
        if let Some(c) = e.as_any().downcast_ref::<Column>() {
            let idx = c.index() + by;
            let name = target.field(idx).name();
            Ok(Transformed::yes(Arc::new(Column::new(name, idx))
                as Arc<dyn datafusion_physical_expr::PhysicalExpr>))
        } else {
            Ok(Transformed::no(e))
        }
    })
    .map(|t| t.data)
}

fn plan_explicit_partitioning(
    planner: &dyn PhysicalPlanner,
    schema: &DFSchema,
    input: &dyn ExecutionPlan,
    num_partitions: Option<usize>,
    expressions: &[Expr],
    session_state: &SessionState,
) -> datafusion_common::Result<Partitioning> {
    match (num_partitions, expressions) {
        (Some(0), _) => internal_err!("number of explicit partitions cannot be zero"),
        (Some(1), _) => Ok(Partitioning::UnknownPartitioning(1)),
        (Some(_) | None, expressions) => {
            if expressions.is_empty() {
                return internal_err!(
                    "explicit repartitioning requires at least one partitioning expression"
                );
            }
            let num_partitions = num_partitions
                .unwrap_or_else(|| input.properties().output_partitioning().partition_count());
            let expressions = expressions
                .iter()
                .map(|e| planner.create_physical_expr(e, schema, session_state))
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            Ok(Partitioning::Hash(expressions, num_partitions))
        }
    }
}

pub fn new_query_planner() -> Arc<dyn QueryPlanner + Send + Sync> {
    Arc::new(ExtensionQueryPlanner {})
}
