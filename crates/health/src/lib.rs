use reqwest;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;
use tracing::info;

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
pub fn start_health_check_loop(registry: HealthRegistry, interval: Duration) {
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

                // spawn concurrent task for each ping so that a slow ping on A doesnt delay ping on B
                tokio::spawn(async move {
                    let url = format!("http://{}/", addr);

                    match client_clone.get(&url).send().await {
                        Ok(response) if response.status().is_success() => {
                            // if it was previously dead log its recovery
                            if registry_clone.get_status(&addr).await == Some(HealthStatus::Dead) {
                                info!("Backend {} recovered. Marking as healthy", addr);
                            }
                            registry_clone.set_status(addr, HealthStatus::Healthy).await;
                        }
                        Ok(response) => {
                            // backend responded but with a failing status code
                            if registry_clone.get_status(&addr).await == Some(HealthStatus::Healthy)
                            {
                                info!(
                                    "Backend {} returned status {}. Marking as dead",
                                    addr,
                                    response.status()
                                );
                            }
                            registry_clone.set_status(addr, HealthStatus::Dead).await;
                        }
                        Err(e) => {
                            if registry_clone.get_status(&addr).await == Some(HealthStatus::Healthy)
                            {
                                info!(
                                    "Backend {} health check failed {}. Marking as dead",
                                    addr, e
                                );
                            }
                            registry_clone.set_status(addr, HealthStatus::Dead).await;
                        }
                    }
                });
            }
        }
    });
}
