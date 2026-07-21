use datafusion_expr::LogicalPlan;
use zelox_catalog::command::CatalogCommand;
use zelox_catalog::provider::CreateDatabaseOptions;
use zelox_common::spec;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;

impl PlanResolver<'_> {
    pub(in super::super) fn resolve_catalog_create_database(
        &self,
        database: spec::ObjectName,
        definition: spec::DatabaseDefinition,
    ) -> PlanResult<LogicalPlan> {
        let spec::DatabaseDefinition {
            if_not_exists,
            comment,
            location,
            properties,
        } = definition;
        let command = CatalogCommand::CreateDatabase {
            database: database.into(),
            options: CreateDatabaseOptions {
                if_not_exists,
                comment,
                location,
                properties,
            },
        };
        self.resolve_catalog_command(command)
    }
}
