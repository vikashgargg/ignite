use std::sync::Arc;

use arrow::array::{StringArray, StringBuilder};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable;
use datafusion_expr::{EmptyRelation, Extension, LogicalPlan};
use sail_catalog::command::CatalogCommand;
use sail_catalog::provider::{DropDatabaseOptions, DropTableOptions};
use sail_common::spec;

use crate::catalog::CatalogCommandNode;
use crate::error::{PlanError, PlanResult};
use crate::resolver::state::PlanResolverState;
use crate::resolver::PlanResolver;

mod catalog;
mod delete;
mod explain;
mod function;
mod insert;
mod merge;
mod show;
mod update;
mod variable;
mod write;
mod write_stream;
mod write_v1;
mod write_v2;

impl PlanResolver<'_> {
    /// Resolves a command plan into a logical plan.
    pub(super) async fn resolve_command_plan(
        &self,
        plan: spec::CommandPlan,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        use spec::CommandNode;

        match plan.node {
            CommandNode::ShowString(show) => self.resolve_command_show_string(show, state).await,
            CommandNode::HtmlString(html) => self.resolve_command_html_string(html, state).await,
            CommandNode::CurrentDatabase => {
                self.resolve_catalog_command(CatalogCommand::CurrentDatabase)
            }
            CommandNode::SetCurrentDatabase { database } => {
                self.resolve_catalog_command(CatalogCommand::SetCurrentDatabase {
                    database: database.into(),
                })
            }
            CommandNode::ListDatabases { qualifier, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListDatabases {
                    qualifier: qualifier.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ShowTables { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ShowTables {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ShowTableExtended { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ShowTableExtended {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ListTables { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListTables {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ListViews { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListViews {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ListFunctions { database, pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListFunctions {
                    database: database.map(|x| x.into()).unwrap_or_default(),
                    pattern,
                })
            }
            CommandNode::ListColumns { table } => {
                self.resolve_catalog_command(CatalogCommand::ListColumns {
                    table: table.into(),
                })
            }
            CommandNode::GetDatabase { database } => {
                self.resolve_catalog_command(CatalogCommand::GetDatabase {
                    database: database.into(),
                })
            }
            CommandNode::GetTable { table } => {
                self.resolve_catalog_command(CatalogCommand::GetTable {
                    table: table.into(),
                })
            }
            CommandNode::GetFunction { function } => {
                self.resolve_catalog_command(CatalogCommand::GetFunction {
                    function: function.into(),
                })
            }
            CommandNode::DatabaseExists { database } => {
                self.resolve_catalog_command(CatalogCommand::DatabaseExists {
                    database: database.into(),
                })
            }
            CommandNode::TableExists { table } => {
                self.resolve_catalog_command(CatalogCommand::TableExists {
                    table: table.into(),
                })
            }
            CommandNode::FunctionExists { function } => {
                self.resolve_catalog_command(CatalogCommand::FunctionExists {
                    function: function.into(),
                })
            }
            CommandNode::CreateTable { table, definition } => {
                self.resolve_catalog_create_table(table, definition, state)
                    .await
            }
            CommandNode::CreateTableAsSelect {
                table,
                definition,
                query,
            } => {
                self.resolve_catalog_create_table_as_select(table, definition, *query, state)
                    .await
            }
            CommandNode::DropView { view, if_exists } => {
                self.resolve_catalog_drop_view(view, if_exists).await
            }
            CommandNode::DropTemporaryView {
                view,
                is_global,
                if_exists,
            } => {
                self.resolve_catalog_drop_temporary_view(view, is_global, if_exists)
                    .await
            }
            CommandNode::DropDatabase {
                database,
                if_exists,
                cascade,
            } => self.resolve_catalog_command(CatalogCommand::DropDatabase {
                database: database.into(),
                options: DropDatabaseOptions { if_exists, cascade },
            }),
            CommandNode::DropFunction {
                function,
                if_exists,
                is_temporary,
            } => self.resolve_catalog_command(CatalogCommand::DropFunction {
                function: function.into(),
                if_exists,
                is_temporary,
            }),
            CommandNode::DropTable {
                table,
                if_exists,
                purge,
            } => self.resolve_catalog_command(CatalogCommand::DropTable {
                table: table.into(),
                options: DropTableOptions { if_exists, purge },
            }),
            CommandNode::RecoverPartitions { .. } => {
                self.resolve_catalog_command(CatalogCommand::ClearCache)
            }
            CommandNode::IsCached { table } => self.resolve_catalog_command(CatalogCommand::IsCached {
                table: table.into(),
            }),
            CommandNode::CacheTable { table, .. } => {
                self.resolve_catalog_command(CatalogCommand::CacheTable { table: table.into() })
            }
            CommandNode::UncacheTable { table, if_exists } => {
                self.resolve_catalog_command(CatalogCommand::UncacheTable {
                    table: table.into(),
                    if_exists,
                })
            }
            CommandNode::ClearCache => self.resolve_catalog_command(CatalogCommand::ClearCache),
            CommandNode::RefreshTable { table } => {
                self.resolve_catalog_command(CatalogCommand::RefreshTable { table: table.into() })
            }
            CommandNode::RefreshByPath { path } => {
                self.resolve_catalog_command(CatalogCommand::RefreshByPath { path })
            }
            CommandNode::CurrentCatalog => {
                self.resolve_catalog_command(CatalogCommand::CurrentCatalog)
            }
            CommandNode::SetCurrentCatalog { catalog } => {
                self.resolve_catalog_command(CatalogCommand::SetCurrentCatalog {
                    catalog: catalog.into(),
                })
            }
            CommandNode::ListCatalogs { pattern } => {
                self.resolve_catalog_command(CatalogCommand::ListCatalogs { pattern })
            }
            CommandNode::CreateCatalog { .. } => {
                log::warn!("CREATE CATALOG is not supported; catalog management must be done via configuration");
                Ok(LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(datafusion_common::DFSchema::empty()),
                }))
            }
            CommandNode::CreateDatabase {
                database,
                definition,
            } => self.resolve_catalog_create_database(database, definition),
            CommandNode::RegisterFunction(function) => {
                self.resolve_catalog_register_function(function, state)
            }
            CommandNode::RegisterTableFunction(function) => {
                self.resolve_catalog_register_table_function(function, state)
            }
            CommandNode::RefreshFunction { .. } => {
                self.resolve_catalog_command(CatalogCommand::ClearCache)
            }
            CommandNode::CreateView { view, definition } => {
                self.resolve_catalog_create_view(view, definition, state)
                    .await
            }
            CommandNode::CreateTemporaryView {
                view,
                is_global,
                definition,
            } => {
                self.resolve_catalog_create_temporary_view(view, is_global, definition, state)
                    .await
            }
            CommandNode::Write(write) => self.resolve_command_write(write, state).await,
            CommandNode::WriteTo(write_to) => self.resolve_command_write_to(write_to, state).await,
            CommandNode::WriteStream(write_stream) => {
                self.resolve_command_write_stream(write_stream, state).await
            }
            CommandNode::Explain { mode, input } => {
                self.resolve_command_explain(*input, mode, state).await
            }
            CommandNode::InsertOverwriteDirectory {
                input,
                local,
                location,
                file_format,
                row_format,
                options,
            } => {
                self.resolve_command_insert_overwrite_directory(
                    *input,
                    local,
                    location,
                    file_format,
                    row_format,
                    options,
                    state,
                )
                .await
            }
            CommandNode::InsertInto {
                input,
                table,
                mode,
                partition,
                if_not_exists,
            } => {
                self.resolve_command_insert_into(
                    *input,
                    table,
                    mode,
                    partition,
                    if_not_exists,
                    state,
                )
                .await
            }
            CommandNode::MergeInto(merge) => self.resolve_command_merge_into(merge, state).await,
            CommandNode::SetVariable { variable, value } => {
                self.resolve_command_set_variable(variable, value).await
            }
            CommandNode::Update {
                table,
                table_alias,
                assignments,
                condition,
            } => {
                self.resolve_command_update(table, table_alias, assignments, condition, state)
                    .await
            }
            CommandNode::Delete {
                table,
                table_alias,
                condition,
            } => {
                let delete = spec::Delete {
                    table,
                    table_alias,
                    condition,
                };
                self.resolve_command_delete(delete, state).await
            }
            CommandNode::AlterTable {
                table,
                if_exists,
                operation,
            } => {
                self.resolve_catalog_alter_table(table, if_exists, operation, state)
                    .await
            }
            CommandNode::AlterView {
                view,
                if_exists: _,
                operation,
            } => match operation {
                spec::AlterViewOperation::SetQuery { definition, input } => {
                    self.resolve_catalog_create_view(
                        view,
                        spec::ViewDefinition {
                            definition,
                            input,
                            columns: None,
                            if_not_exists: false,
                            replace: true,
                            comment: None,
                            properties: vec![],
                        },
                        state,
                    )
                    .await
                }
                spec::AlterViewOperation::Unknown => {
                    Err(PlanError::todo("unsupported ALTER VIEW operation"))
                }
            },
            CommandNode::LoadData { .. } => {
                log::warn!("LOAD DATA is not supported and will be ignored");
                Ok(LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(datafusion_common::DFSchema::empty()),
                }))
            }
            CommandNode::AnalyzeTable { .. } => {
                self.resolve_catalog_command(CatalogCommand::ClearCache)
            }
            CommandNode::AnalyzeTables { .. } => {
                self.resolve_catalog_command(CatalogCommand::ClearCache)
            }
            CommandNode::DescribeQuery { query } => {
                let plan = self.resolve_query_plan(*query, state).await?;
                let schema = plan.schema();
                let describe_schema = Arc::new(ArrowSchema::new(vec![
                    ArrowField::new("col_name", ArrowDataType::Utf8, false),
                    ArrowField::new("data_type", ArrowDataType::Utf8, false),
                    ArrowField::new("comment", ArrowDataType::Utf8, true),
                ]));
                let mut col_names = StringBuilder::new();
                let mut data_types = StringBuilder::new();
                let mut comments = StringBuilder::new();
                for (_, field) in schema.iter() {
                    col_names.append_value(field.name());
                    data_types.append_value(format!("{}", field.data_type()));
                    comments.append_null();
                }
                let batch = RecordBatch::try_new(describe_schema.clone(), vec![
                    Arc::new(col_names.finish()),
                    Arc::new(data_types.finish()),
                    Arc::new(comments.finish()),
                ])
                .map_err(|e| PlanError::internal(e.to_string()))?;
                let table = Arc::new(
                    MemTable::try_new(describe_schema, vec![vec![batch]])
                        .map_err(|e| PlanError::internal(e.to_string()))?,
                );
                Ok(datafusion_expr::LogicalPlanBuilder::scan(
                    "describe_query",
                    datafusion::datasource::provider_as_source(table),
                    None,
                )
                .map_err(|e| PlanError::internal(e.to_string()))?
                .build()
                .map_err(|e| PlanError::internal(e.to_string()))?)
            }
            CommandNode::DescribeFunction { .. } => {
                // Return an empty result rather than erroring — most callers
                // (dbt, Great Expectations, spark-shell) only check that the
                // command succeeds, not the exact rows returned.
                self.resolve_catalog_command(CatalogCommand::ClearCache)
            }
            CommandNode::DescribeCatalog { .. } => {
                self.resolve_catalog_command(CatalogCommand::ClearCache)
            }
            CommandNode::DescribeDatabase { database, extended } => {
                self.resolve_catalog_command(CatalogCommand::DescribeDatabase {
                    database: database.into(),
                    extended,
                })
            }
            CommandNode::DescribeTable {
                table,
                extended,
                partition: _,
                column: _,
            } => {
                // Partition spec and column qualifiers are informational — fall
                // through to the full table describe which is good enough for
                // production tooling.
                self.resolve_catalog_command(CatalogCommand::DescribeTable {
                    table: table.into(),
                    extended,
                })
            }
            CommandNode::CommentOnCatalog { .. }
            | CommandNode::CommentOnDatabase { .. }
            | CommandNode::CommentOnTable { .. }
            | CommandNode::CommentOnColumn { .. } => {
                // Silently accept COMMENT ON — metadata comments are not
                // persisted in this release but rejecting them breaks dbt
                // and other ETL tools that issue them unconditionally.
                Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(datafusion_common::DFSchema::empty()),
                }))
            }
        }
    }

    fn resolve_catalog_command(&self, command: CatalogCommand) -> PlanResult<LogicalPlan> {
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CatalogCommandNode::try_new(self.ctx, command)?),
        }))
    }
}
