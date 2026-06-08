use std::sync::Arc;

use datafusion::datasource::{provider_as_source, TableProvider};
use datafusion_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
use sail_catalog::manager::CatalogManager;
use sail_catalog::provider::{CatalogPartitionField, CreateTemporaryViewOptions};
use sail_common::spec;
use sail_common_datafusion::extension::SessionExtensionAccessor;
use sail_logical_plan::streaming::foreach_batch::ForeachBatchSinkNode;
use sail_logical_plan::streaming::memory_sink::MemorySinkNode;

use crate::error::{PlanError, PlanResult};
use crate::memory_buffer::MemoryStreamBuffer;
use crate::resolver::command::write::{WriteColumnMatch, WriteMode, WritePlanBuilder, WriteTarget};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    /// Resolves the write operation for the Spark streaming query API.
    pub(super) async fn resolve_command_write_stream(
        &self,
        write_stream: spec::WriteStream,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::WriteStreamSinkDestination;

        let spec::WriteStream {
            input,
            format,
            options,
            partitioning_column_names,
            query_name,
            foreach_writer,
            foreach_batch,
            clustering_column_names,
            sink_destination,
            trigger,
        } = write_stream;
        // Trigger support: bounded sources (file/table via the source adapter) already
        // drain and emit EndOfData, so `availableNow`/`once` terminate correctly for
        // them today. Making an unbounded source (rate/Kafka) stop under
        // `availableNow`/`once` requires per-source bounded reads and is tracked in
        // docs/STREAMING.md (P0). `processingTime`/`continuous` pacing is not yet honored
        // (processing is source-driven). Log so the behavior is never silent.
        match &trigger {
            Some(spec::StreamTrigger::AvailableNow) | Some(spec::StreamTrigger::Once) => {
                log::info!(
                    "streaming trigger {trigger:?}: bounded sources will run-once and stop; \
                     unbounded sources (rate/Kafka) still run continuously (see docs/STREAMING.md)"
                );
            }
            Some(spec::StreamTrigger::ProcessingTime { .. })
            | Some(spec::StreamTrigger::Continuous { .. }) => {
                log::info!(
                    "streaming trigger {trigger:?}: interval/pacing is not yet honored; \
                     processing is source-driven (see docs/STREAMING.md)"
                );
            }
            None => {}
        }
        if foreach_writer.is_some() {
            return Err(PlanError::invalid(
                "writeStream.foreach() row-level writer is not supported; use writeStream.foreachBatch() instead",
            ));
        }

        // foreachBatch: wrap the streaming input in a ForeachBatchSinkNode and return early.
        if let Some(foreach_batch) = foreach_batch {
            let (command, eval_type, python_version) = match foreach_batch {
                spec::FunctionDefinition::PythonUdf {
                    command,
                    eval_type,
                    python_version,
                    ..
                } => (command, i32::from(eval_type), python_version),
                spec::FunctionDefinition::ScalarScalaUdf { .. } => {
                    return Err(PlanError::todo("Scala foreachBatch is not supported"));
                }
                spec::FunctionDefinition::JavaUdf { .. } => {
                    return Err(PlanError::todo("Java foreachBatch is not supported"));
                }
            };
            let resolved_input = self.resolve_write_input(*input, state).await?;
            return Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(ForeachBatchSinkNode::new(
                    Arc::new(resolved_input),
                    command,
                    eval_type,
                    python_version,
                )),
            }));
        }

        // memory format: register a shared buffer as a queryable table and return a sink node.
        if format.eq_ignore_ascii_case("memory") {
            if query_name.is_empty() {
                return Err(PlanError::invalid(
                    "writeStream.format(\"memory\") requires a non-empty queryName",
                ));
            }
            let resolved_input = self.resolve_write_input(*input, state).await?;
            let data_schema = Arc::new(resolved_input.schema().as_arrow().clone());
            let buffer = Arc::new(MemoryStreamBuffer::new(data_schema));
            // Make `writeStream.format("memory").queryName(q)` queryable as `q`, matching
            // Spark. Register an ephemeral temporary view that scans the live buffer
            // through Vajra's CatalogManager (NOT `SessionContext::register_table`, which
            // targets DataFusion's default `datafusion` catalog that Vajra does not use —
            // that path fails with "failed to resolve catalog: datafusion").
            let scan = LogicalPlanBuilder::scan(
                query_name.as_str(),
                provider_as_source(Arc::clone(&buffer) as Arc<dyn TableProvider>),
                None,
            )
            .and_then(|b| b.build())
            .map_err(|e| PlanError::internal(e.to_string()))?;
            let manager = self.ctx.extension::<CatalogManager>()?;
            manager
                .create_temporary_view(
                    query_name.as_str(),
                    CreateTemporaryViewOptions {
                        input: Arc::new(scan),
                        columns: vec![],
                        if_not_exists: false,
                        replace: true,
                        comment: None,
                        properties: vec![],
                    },
                )
                .await?;
            return Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(MemorySinkNode::new(
                    Arc::new(resolved_input),
                    query_name,
                    buffer.buffer_handle(),
                )),
            }));
        }

        let input = self.resolve_write_input(*input, state).await?;
        let clustering_columns = self.resolve_write_cluster_by_columns(clustering_column_names)?;
        let partition_by = partitioning_column_names
            .into_iter()
            .map(|c| CatalogPartitionField {
                column: c.into(),
                transform: None,
            })
            .collect();
        let mut builder = WritePlanBuilder::new()
            .with_partition_by(partition_by)
            .with_cluster_by(clustering_columns)
            .with_format(format)
            .with_options(options)
            .with_mode(WriteMode::Append {
                error_if_absent: false,
            });
        match sink_destination {
            None => {
                builder = builder.with_target(WriteTarget::DataSource);
            }
            Some(WriteStreamSinkDestination::Path { path }) => {
                builder = builder
                    .with_target(WriteTarget::DataSource)
                    .with_options(vec![("path".to_string(), path)]);
            }
            Some(WriteStreamSinkDestination::Table { table }) => {
                builder = builder.with_target(WriteTarget::Table {
                    table,
                    column_match: WriteColumnMatch::ByName,
                })
            }
        }
        self.resolve_write_with_builder(input, builder, state).await
    }
}
