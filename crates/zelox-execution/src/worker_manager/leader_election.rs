use std::time::Duration;

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::{Patch, PatchParams, PostParams};
use kube::{Api, Client};
use log::{debug, info, warn};
use tokio::time::Instant;

const MICRO_TIME_FMT: &str = "%Y-%m-%dT%H:%M:%S%.6fZ";

fn format_timestamp(ts: &k8s_openapi::jiff::Timestamp) -> kube::Result<String> {
    k8s_openapi::jiff::fmt::strtime::format(MICRO_TIME_FMT, *ts)
        .map_err(|e| kube::Error::Service(Box::new(e)))
}

/// Configuration for Kubernetes Lease-based leader election.
#[derive(Debug, Clone)]
pub struct LeaderElectionConfig {
    /// Name of the `coordination.k8s.io/v1/Lease` object.
    pub lease_name: String,
    /// Kubernetes namespace where the Lease is managed.
    pub namespace: String,
    /// Unique identity of this candidate (e.g. pod name).
    pub identity: String,
    /// How long a lease is valid after its last renewal.
    pub lease_duration: Duration,
    /// How long to keep retrying renewal before giving up leadership.
    pub renew_deadline: Duration,
    /// Interval between retry attempts.
    pub retry_period: Duration,
}

impl Default for LeaderElectionConfig {
    fn default() -> Self {
        Self {
            lease_name: "zelox-scheduler-leader".to_string(),
            namespace: "zelox".to_string(),
            identity: std::env::var("HOSTNAME").unwrap_or_else(|_| "zelox-server".to_string()),
            lease_duration: Duration::from_secs(15),
            renew_deadline: Duration::from_secs(10),
            retry_period: Duration::from_secs(2),
        }
    }
}

/// Kubernetes Lease-based leader elector.
///
/// Call [`KubernetesLeaderElector::run_as_leader`] to participate in leader
/// election. The supplied future is executed while this instance holds the
/// lease and is cancelled if the lease is lost.
pub struct KubernetesLeaderElector {
    config: LeaderElectionConfig,
    api: Api<Lease>,
}

impl KubernetesLeaderElector {
    /// Creates a new leader elector by connecting to the in-cluster Kubernetes
    /// API server.
    pub async fn try_new(config: LeaderElectionConfig) -> kube::Result<Self> {
        let client = Client::try_default().await?;
        let api = Api::namespaced(client, &config.namespace);
        Ok(Self { config, api })
    }

    /// Participates in leader election. Blocks until this instance acquires
    /// the lease, runs `on_leading` while holding it, then returns.
    ///
    /// If the lease is lost mid-run (renewal failures exhaust
    /// `renew_deadline`), `on_leading` is dropped and the function returns so
    /// the caller can decide whether to retry or shut down.
    pub async fn run_as_leader<F, Fut>(&self, on_leading: F)
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        // 1. Wait until we acquire the lease.
        loop {
            match self.try_acquire_or_renew().await {
                Ok(true) => {
                    info!(
                        "Leader election: {} acquired lease '{}'",
                        self.config.identity, self.config.lease_name
                    );
                    break;
                }
                Ok(false) => {
                    debug!(
                        "Leader election: lease '{}' held by another instance, retrying in {:?}",
                        self.config.lease_name, self.config.retry_period
                    );
                    tokio::time::sleep(self.config.retry_period).await;
                }
                Err(e) => {
                    warn!("Leader election error during acquire: {e}; retrying...");
                    tokio::time::sleep(self.config.retry_period).await;
                }
            }
        }

        // 2. Run the leader workload while keeping the lease renewed.
        let renew_interval = self.config.retry_period;
        let identity = self.config.identity.clone();
        let lease_name = self.config.lease_name.clone();

        let work_fut = on_leading();
        tokio::pin!(work_fut);

        let mut renewal_interval = tokio::time::interval(renew_interval);
        renewal_interval.tick().await; // consume the immediate first tick

        let renew_deadline_start = Instant::now();

        loop {
            tokio::select! {
                _ = &mut work_fut => {
                    info!("Leader election: leader workload completed, releasing lease '{lease_name}'");
                    return;
                }
                _ = renewal_interval.tick() => {
                    match self.try_acquire_or_renew().await {
                        Ok(true) => {
                            // Successfully renewed.
                        }
                        Ok(false) => {
                            warn!(
                                "Leader election: lease '{lease_name}' taken by another holder, stopping leadership"
                            );
                            return;
                        }
                        Err(e) => {
                            if renew_deadline_start.elapsed() > self.config.renew_deadline {
                                warn!(
                                    "Leader election: failed to renew lease '{lease_name}' within renew_deadline ({e}), stopping leadership"
                                );
                                return;
                            }
                            warn!("Leader election: transient renewal error for '{identity}': {e}");
                        }
                    }
                }
            }
        }
    }

    /// Tries to acquire (first time) or renew (already holding) the lease.
    ///
    /// Returns `Ok(true)` if this instance is now the holder, `Ok(false)` if
    /// another instance holds a valid lease.
    async fn try_acquire_or_renew(&self) -> kube::Result<bool> {
        let now = k8s_openapi::jiff::Timestamp::now();
        let now_str = format_timestamp(&now)?;
        let duration_secs = self.config.lease_duration.as_secs() as i32;
        let micro_time = MicroTime(now);

        match self.api.get_opt(&self.config.lease_name).await? {
            None => {
                // No lease exists yet — create it and claim it.
                let lease = Lease {
                    metadata: ObjectMeta {
                        name: Some(self.config.lease_name.clone()),
                        namespace: Some(self.config.namespace.clone()),
                        ..Default::default()
                    },
                    spec: Some(LeaseSpec {
                        holder_identity: Some(self.config.identity.clone()),
                        lease_duration_seconds: Some(duration_secs),
                        acquire_time: Some(micro_time.clone()),
                        renew_time: Some(micro_time),
                        lease_transitions: Some(0),
                        ..Default::default()
                    }),
                };
                match self.api.create(&PostParams::default(), &lease).await {
                    Ok(_) => Ok(true),
                    Err(kube::Error::Api(e)) if e.code == 409 => Ok(false), // conflict
                    Err(e) => Err(e),
                }
            }
            Some(existing) => {
                let spec = existing.spec.as_ref();
                let holder = spec.and_then(|s| s.holder_identity.as_deref());
                let renew_time = spec.and_then(|s| s.renew_time.as_ref());
                let lease_duration = spec
                    .and_then(|s| s.lease_duration_seconds)
                    .unwrap_or(duration_secs);

                let is_expired = renew_time
                    .map(|t| {
                        // Compare Unix seconds directly — avoids duration type gymnastics.
                        let elapsed_secs = now.as_second() - t.0.as_second();
                        elapsed_secs >= lease_duration as i64
                    })
                    .unwrap_or(true);

                let we_hold_it = holder == Some(self.config.identity.as_str());

                if !we_hold_it && !is_expired {
                    return Ok(false);
                }

                // Acquire or renew: patch the lease.
                let transitions = spec.and_then(|s| s.lease_transitions).unwrap_or(0)
                    + if we_hold_it { 0 } else { 1 };

                let acquire_time_str = if we_hold_it {
                    spec.and_then(|s| s.acquire_time.as_ref())
                        .map(|t| format_timestamp(&t.0))
                        .transpose()?
                        .unwrap_or_else(|| now_str.clone())
                } else {
                    now_str.clone()
                };

                let patch_body = serde_json::json!({
                    "spec": {
                        "holderIdentity": self.config.identity,
                        "leaseDurationSeconds": duration_secs,
                        "acquireTime": acquire_time_str,
                        "renewTime": now_str,
                        "leaseTransitions": transitions,
                    }
                });

                match self
                    .api
                    .patch(
                        &self.config.lease_name,
                        &PatchParams::apply("zelox-leader-elector"),
                        &Patch::Merge(patch_body),
                    )
                    .await
                {
                    Ok(_) => Ok(true),
                    Err(kube::Error::Api(e)) if e.code == 409 => Ok(false),
                    Err(e) => Err(e),
                }
            }
        }
    }
}
