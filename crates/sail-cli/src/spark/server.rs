use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use log::{error, info};
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeManager;
use sail_execution::worker_manager::leader_election::{KubernetesLeaderElector, LeaderElectionConfig};
use sail_spark_connect::entrypoint::serve;
use tokio::net::TcpListener;

/// Handles graceful shutdown by waiting for a `SIGINT` or `SIGTERM` signal.
///
/// `SIGTERM` is sent by Docker (`docker stop`), Kubernetes pod eviction, and
/// `container stop`.  `SIGINT` is the interactive Ctrl-C signal.
///
/// The `SIGINT` signal is captured by Python if the `_signal` module is
/// imported (https://github.com/PyO3/pyo3/issues/2576). Handling it here
/// prevents a double-signal scenario where Python and the server both respond.
async fn shutdown() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c  => info!("Received SIGINT, shutting down gracefully..."),
        _ = sigterm => info!("Received SIGTERM, shutting down gracefully..."),
    }
}

pub(super) mod telemetry {
    use sail_common::config::AppConfig;
    use sail_telemetry::telemetry::{init_telemetry, shutdown_telemetry, ResourceOptions};

    pub struct TelemetryGuard {
        /// A marker to prevent struct creation without calling [`TelemetryGuard::try_new()`].
        _marker: (),
    }

    impl TelemetryGuard {
        pub fn try_new(config: &AppConfig) -> Result<Self, Box<dyn std::error::Error>> {
            let resource = ResourceOptions { kind: "server" };
            init_telemetry(&config.telemetry, resource)?;
            Ok(Self { _marker: () })
        }
    }

    impl Drop for TelemetryGuard {
        fn drop(&mut self) {
            shutdown_telemetry();
        }
    }
}

/// A user-facing error for the Spark Connect server.
/// This does not wrap the underlying error but only tracks the error message,
/// so that it can be `Send` from the server task.
#[derive(Debug)]
pub struct ServerError(String);

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "server error: {}", self.0)
    }
}

impl std::error::Error for ServerError {}

/// Starts a Spark Connect server and runs the given workload with the server address.
/// This function should be called only once in the entire process since it initializes
/// the telemetry and shuts down the telemetry when the server stops.
pub(super) fn with_spark_connect_server<S, W, F>(
    config: Arc<AppConfig>,
    address: (IpAddr, u16),
    signal: S,
    workload: W,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: Future<Output = ()> + Send + 'static,
    W: FnOnce(SocketAddr) -> F,
    F: Future<Output = Result<(), Box<dyn std::error::Error>>>,
{
    let runtime = RuntimeManager::try_new(&config.runtime)?;

    let _telemetry = runtime
        .handle()
        .primary()
        .block_on(async { telemetry::TelemetryGuard::try_new(&config) })?;

    let handle = runtime.handle();
    let (server_address, server_task) = runtime.handle().primary().block_on(async {
        // A secure connection can be handled by a gateway in production.
        let listener = TcpListener::bind(address).await?;
        let server_address = listener.local_addr()?;
        let mode_tag = match config.mode {
            ExecutionMode::Local => "local".to_string(),
            ExecutionMode::LocalCluster => format!(
                "local-cluster, workers: {}",
                config.cluster.worker_initial_count
            ),
            ExecutionMode::KubernetesCluster => "kubernetes-cluster".to_string(),
        };
        let server_task = async move {
            info!("Vajra ready on {server_address} (Spark Connect gRPC) [mode: {mode_tag}]");
            match serve(listener, signal, config, handle).await {
                Ok(()) => {
                    info!("The Spark Connect server has stopped.");
                    Ok(())
                }
                Err(e) => {
                    error!("{e}");
                    Err(ServerError(e.to_string()))
                }
            }
        };
        <Result<_, Box<dyn std::error::Error>>>::Ok((server_address, server_task))
    })?;

    let server_task = runtime.handle().primary().spawn(server_task);

    runtime.handle().primary().block_on(async move {
        let result = workload(server_address).await;
        let server_result = server_task.await;
        match (result, server_result) {
            (Err(e), _) => Err(e),
            (Ok(()), Ok(Ok(()))) => Ok(()),
            (Ok(()), Ok(Err(e))) => Err(Box::new(e) as Box<dyn std::error::Error>),
            (Ok(()), Err(e)) => Err(Box::new(e) as Box<dyn std::error::Error>),
        }
    })
}

pub fn run_spark_connect_server(ip: IpAddr, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);
    with_spark_connect_server(config, (ip, port), shutdown(), |_| async { Ok(()) })
}

/// Starts a Spark Connect server wrapped in Kubernetes Lease-based leader election.
///
/// Multiple replicas can run this; only one will hold the lease and serve traffic at
/// a time. If the current leader loses the lease, the server stops and the caller
/// can restart it (e.g., via `kubectl` restart policy).
pub fn run_spark_connect_server_kubernetes_ha(
    ip: IpAddr,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = Arc::new(AppConfig::load()?);
    let election_config = LeaderElectionConfig {
        namespace: config.kubernetes.namespace.clone(),
        ..LeaderElectionConfig::default()
    };

    let runtime = RuntimeManager::try_new(&config.runtime)?;
    let _telemetry = runtime
        .handle()
        .primary()
        .block_on(async { telemetry::TelemetryGuard::try_new(&config) })?;

    let handle = runtime.handle().clone();

    runtime.handle().primary().block_on(async move {
        let elector = KubernetesLeaderElector::try_new(election_config)
            .await
            .map_err(|e| Box::new(ServerError(e.to_string())) as Box<dyn std::error::Error>)?;

        elector
            .run_as_leader(|| {
                let config = config.clone();
                let handle = handle.clone();
                async move {
                    let listener = match TcpListener::bind((ip, port)).await {
                        Ok(l) => l,
                        Err(e) => {
                            error!("Leader: failed to bind {ip}:{port}: {e}");
                            return;
                        }
                    };
                    let server_address = listener
                        .local_addr()
                        .expect("local_addr after successful bind");
                    info!("Vajra ready on {server_address} (Spark Connect gRPC) [mode: kubernetes-cluster-ha]");
                    if let Err(e) = serve(listener, shutdown(), config, handle).await {
                        error!("{e}");
                    }
                }
            })
            .await;

        Ok(())
    })
}

/// Start the Spark Connect server in `local-cluster` mode, overriding whatever
/// `SAIL_MODE` says.  `workers == 0` keeps the config-file default.
pub fn run_spark_connect_server_local_cluster(
    ip: IpAddr,
    port: u16,
    workers: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut config = AppConfig::load()?;
    config.mode = ExecutionMode::LocalCluster;
    if workers > 0 {
        config.cluster.worker_initial_count = workers;
    }
    with_spark_connect_server(Arc::new(config), (ip, port), shutdown(), |_| async { Ok(()) })
}
