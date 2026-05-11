use tokio::sync::RwLock;
use tokio::time;
use tracing::info;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HealthStatus {
    Healthy,
    Dead,
    // we can add draining and unknown later
}

#[derive(Clone)]
pub struct HealthRegistry {
    statuses: Arc<RwLock<HashMap<SocketAddr, HealthStatus>>>,
}

impl HealthRegistry {
    pub fn new(backends: &[SocketAddr]) -> Self {
        let mut map = HashMap::new();

        // assume all backends are healthy at startup
        for &addr in backends {
            map.insert(addr, HealthStatus::Healthy);
        }

        Self {
            statuses: Arc::new(RwLock::new(map)),
        }
    }

    // update the health status of specific backend
    pub async fn set_status(&self, addr: SocketAddr, status: HealthStatus) {
        let mut map = self.statuses.write().await;
        if let Some(current) = map.get_mut(&addr) {
            *current = status;
        }
    }

    // retrieve status of a specific backend
    pub async fn get_status(&self, addr: &SocketAddr) -> Option<HealthStatus> {
        let map = self.statuses.read().await;
        map.get(addr).copied()
    }

    // returns a snapshot of only the healthy backends
    pub async fn get_healthy_backends(&self) -> Vec<SocketAddr> {
        let map = self.statuses.read().await;
        map.iter()
            .filter_map(|(&addr, &status)| {
                if status == HealthStatus::Healthy {
                    Some(addr)
                } else {
                    None
                }
            })
            .collect()
    }

    // helper function to get all the backends regardless of their health
    pub async fn get_all_backends(&self) -> Vec<SocketAddr> {
        let map = self.statuses.read().await;
        map.keys().copied().collect()
    }
}

// spawn a background task for continuous health check
pub fn start_health_check_loop(
    registry: HealthRegistry,
    interval: Duration,
    http_targets: Vec<SocketAddr>,
) {
    // wrap http targets list in an Arc so we can cheaply share it across the ping tasks
    let shared_http_targets = Arc::new(http_targets);

    // we spawn this on tokio runtime so it runs forever
    tokio::spawn(async move {
        // build http client with strictly 2 seconds timeout
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("Failed to build HTTP client");

        let mut ticker = time::interval(interval);

        loop {
            // wait for the ticker
            ticker.tick().await;

            let backends = registry.get_all_backends().await;

            for addr in backends {
                let registry_clone = registry.clone();
                let client_clone = client.clone();
                let targets_clone = shared_http_targets.clone();

                // spawn concurrent task for each ping so that a slow ping on A doesnt delay ping on B
                tokio::spawn(async move {
                    // protocol aware ping
                    let is_healthy = if targets_clone.contains(&addr) {
                        // l7 HTTP health check
                        let url = format!("http://{}/", addr);
                        match client_clone.get(&url).send().await {
                            Ok(response) => response.status().is_success(),
                            Err(_) => false,
                        }
                    } else {
                        // l4 TCP health check
                        tokio::net::TcpStream::connect(&addr).await.is_ok()
                    };
                    // state eval & structured logging
                    let currently_healthy =
                        registry_clone.get_status(&addr).await == Some(HealthStatus::Healthy);

                    if is_healthy {
                        if !currently_healthy {
                            info!(
                                backend = %addr,
                                "Backend recovered. Marking as healthy"
                            );
                        }
                        registry_clone.set_status(addr, HealthStatus::Healthy).await;
                    } else {
                        if currently_healthy {
                            info!(
                                backend = %addr,
                                "Backend health check failed. Marking as dead"
                            );
                        }
                        registry_clone.set_status(addr, HealthStatus::Dead).await;
                    }
                });
            }
        }
    });
}
