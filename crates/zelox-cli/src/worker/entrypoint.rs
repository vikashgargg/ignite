use std::sync::Arc;

use zelox_common::config::AppConfig;
use zelox_common::runtime::RuntimeManager;
use zelox_session::session_factory::{SessionFactory, WorkerSessionFactory};
use zelox_telemetry::telemetry::{init_telemetry, shutdown_telemetry, ResourceOptions};

pub fn run_worker() -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);
    let runtime = RuntimeManager::try_new(&config.runtime)?;

    runtime.handle().primary().block_on(async {
        let resource = ResourceOptions { kind: "worker" };
        init_telemetry(&config.telemetry, resource)
    })?;

    let session = WorkerSessionFactory::new(config.clone(), runtime.handle()).create(())?;
    runtime
        .handle()
        .primary()
        .block_on(zelox_execution::run_worker(
            &config,
            runtime.handle(),
            session,
        ))?;

    shutdown_telemetry();

    Ok(())
}
