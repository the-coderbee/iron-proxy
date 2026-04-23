use std::fs;
use std::sync::{Arc, RwLock};
use std::collections::HashSet;
use tracing::info;
use tokio::time::{Duration, sleep};
use crate::balancer::LeastConnections;


#[derive(Clone)]
pub struct ProviderRegistry {
    static_targets: Arc<RwLock<HashSet<String>>>,
    docker_targets: Arc<RwLock<HashSet<String>>>,
    balancer: LeastConnections,
}

impl ProviderRegistry {
    pub fn new(balancer: LeastConnections) -> Self {
        Self {
            static_targets: Arc::new(RwLock::new(HashSet::new())),
            docker_targets: Arc::new(RwLock::new(HashSet::new())),
            balancer,
        }
    }

    pub fn update_static(&self, targets: Vec<String>) {
        let mut static_lock = self.static_targets.write().unwrap();
        *static_lock = targets.into_iter().collect();
        self.sync();
    }

    pub fn update_docker(&self, targets: Vec<String>) {
        let mut docker_lock = self.docker_targets.write().unwrap();
        *docker_lock = targets.into_iter().collect();
        self.sync();
    }

    // merge both lists and update balancer
    fn sync(&self) {
        let static_lock = self.static_targets.read().unwrap();
        let docker_lock = self.docker_targets.read().unwrap();

        let mut combined: Vec<String> = static_lock.iter().cloned().collect();
        combined.extend(docker_lock.iter().cloned());

        combined.sort();
        combined.dedup();

        self.balancer.update_backends(combined);
    }
}

pub async fn watch_static_routes(registry: ProviderRegistry) {
    let mut last_content = String::new();
    loop {
        if let Ok(content) = fs::read_to_string("routes.txt") {
            if content != last_content {
                let targets: Vec<String> = content
                    .lines()
                    .map(|l| l.trim().to_string())
                    .filter(|l| !l.is_empty() && !l.starts_with("#"))
                    .collect();

                info!("Static Provider: Loaded {} bare-metal targets.", targets.len());
                registry.update_static(targets);
                last_content = content;
            }
        } 
        sleep(Duration::from_secs(3)).await;
    }
}