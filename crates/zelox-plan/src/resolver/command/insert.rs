use std::sync::Arc;

use datafusion_common::DFSchemaRef;
use datafusion_expr::{Expr, LogicalPlan, Projection};
use zelox_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::command::write::{WriteColumnMatch, WriteMode, WritePlanBuilder, WriteTarget};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(super) async fn resolve_command_insert_overwrite_directory(
        &self,
        input: spec::QueryPlan,
        // TODO: `local` is ignored for now since the object store can be inferred from
        //   the URL scheme in `location`. But we may want to validate it in the future
        //   and return an error if `local` does not match the type of `location`.
        _local: bool,
        location: Option<String>,
        file_format: Option<spec::TableFileFormat>,
        row_format: Option<spec::TableRowFormat>,
        options: Vec<(String, String)>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let Some(location) = location else {
            return Err(PlanError::invalid(
                "missing location for INSERT OVERWRITE DIRECTORY",
            ));
        };
        if row_format.is_some() {
            log::warn!(
                "ROW FORMAT in INSERT OVERWRITE DIRECTORY is not supported and will be ignored"
            );
        }
        let format = match file_format {
            Some(spec::TableFileFormat::General { format }) => format,
            Some(spec::TableFileFormat::Table {
                input_format,
                output_format: _,
            }) => {
                let fmt = input_format.to_ascii_lowercase();
                if fmt.contains("parquet") {
                    "parquet".to_string()
                } else if fmt.contains("orc") {
                    "orc".to_string()
                } else if fmt.contains("json") {
                    "json".to_string()
                } else if fmt.contains("csv") || fmt.contains("text") {
                    "csv".to_string()
                } else {
                    log::warn!("Unknown INPUTFORMAT '{}' for INSERT OVERWRITE DIRECTORY; defaulting to parquet", input_format);
                    "parquet".to_string()
                }
            }
            None => {
                return Err(PlanError::invalid(
                    "missing file format for INSERT OVERWRITE DIRECTORY",
                ));
            }
        };
        let builder = WritePlanBuilder::new()
            .with_mode(WriteMode::Replace {
                error_if_absent: false,
            })
            .with_target(WriteTarget::DataSource)
            .with_format(format)
            .with_options(options)
            .with_options(vec![("path".to_string(), location)]);
        let input = self.resolve_write_input(input, state).await?;
        self.resolve_write_with_builder(input, builder, state).await
    }

    pub(super) async fn resolve_command_insert_into(
        &self,
        input: spec::QueryPlan,
        table: spec::ObjectName,
        mode: spec::InsertMode,
        partition: Vec<(spec::Identifier, Option<spec::Expr>)>,
        if_not_exists: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::InsertMode;

        if if_not_exists {
            log::warn!("INSERT INTO ... IF NOT EXISTS: ignoring IF NOT EXISTS clause");
        }

        let mut input = self.resolve_write_input(input, state).await?;

        // Apply static partition values: inject them as literal columns into the input plan.
        // Dynamic partition columns (no value) are already in the SELECT output — no injection needed.
        if !partition.is_empty() {
            let empty_schema: DFSchemaRef = Arc::new(datafusion_common::DFSchema::empty());
            let mut static_cols: Vec<Expr> = vec![];
            for (col_name, opt_val) in partition {
                if let Some(val_expr) = opt_val {
                    let lit_expr = self
                        .resolve_expression(val_expr, &empty_schema, state)
                        .await?;
                    static_cols.push(lit_expr.alias(state.register_field_name(col_name)));
                }
            }
            if !static_cols.is_empty() {
                // Prefix the static partition literals; existing columns follow.
                let existing: Vec<Expr> = input
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect();
                let all_cols: Vec<Expr> = existing.into_iter().chain(static_cols).collect();
                input = LogicalPlan::Projection(Projection::try_new(all_cols, Arc::new(input))?);
            }
        }

        let mut builder = WritePlanBuilder::new();
        match mode {
            InsertMode::InsertByPosition { overwrite } => {
                let write_mode = if overwrite {
                    WriteMode::TruncatePartitions
                } else {
                    WriteMode::Append {
                        error_if_absent: true,
                    }
                };
                builder = builder
                    .with_mode(write_mode)
                    .with_target(WriteTarget::Table {
                        table,
                        column_match: WriteColumnMatch::ByPosition,
                    });
            }
            InsertMode::InsertByName { overwrite } => {
                let write_mode = if overwrite {
                    WriteMode::TruncatePartitions
                } else {
                    WriteMode::Append {
                        error_if_absent: true,
                    }
                };
                builder = builder
                    .with_mode(write_mode)
                    .with_target(WriteTarget::Table {
                        table,
                        column_match: WriteColumnMatch::ByName,
                    });
            }
            InsertMode::InsertByColumns { columns, overwrite } => {
                let write_mode = if overwrite {
                    WriteMode::TruncatePartitions
                } else {
                    WriteMode::Append {
                        error_if_absent: true,
                    }
                };
                builder = builder
                    .with_mode(write_mode)
                    .with_target(WriteTarget::Table {
                        table,
                        column_match: WriteColumnMatch::ByColumns { columns },
                    });
            }
            InsertMode::Replace { condition } => {
                builder = builder
                    .with_mode(WriteMode::TruncateIf { condition })
                    .with_target(WriteTarget::Table {
                        table,
                        column_match: WriteColumnMatch::ByPosition,
                    });
            }
        };

        self.resolve_write_with_builder(input, builder, state).await
    }
}
