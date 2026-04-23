use bollard::Docker;
use bollard::system::EventsOptions;
use bollard::container::InspectContainerOptions;
use futures_util::stream::StreamExt;
use std::collections::HashMap;
use tracing::{info, warn, error};

use crate::registry::ProviderRegistry;

pub async fn watch_docker_events(registry: ProviderRegistry) {
    let docker = match Docker::connect_with_local_defaults() {
        Ok(d) => d,
        Err(e) => {
            error!("Failed to connect to Docker daemon: {}", e);
            return;
        }
    };

    info!("Connected to Docker API! Waiting for containers with label 'iron-proxy=true'...");

    let mut filters = HashMap::new();
    filters.insert("type".to_string(), vec!["container".to_string()]);
    filters.insert("event".to_string(), vec!["start".to_string(), "die".to_string()]);

    let options = EventsOptions::<String>{
        since: None,
        until: None,
        filters
    };

    let mut stream = docker.events(Some(options));

    let mut active_containers: HashMap<String, String> = HashMap::new();

    while let Some(Ok(event)) = stream.next().await {
        let action = event.action.unwrap_or_default();
        let actor = event.actor.unwrap_or_default();
        let attributes = actor.attributes.unwrap_or_default();
        let container_id = actor.id.unwrap_or_default();
        let name = attributes.get("name").cloned().unwrap_or_default();

        if attributes.get("iron-proxy") == Some(&"true".to_string()) {
            if action == "start" {
                if let Ok(info) = docker.inspect_container(&container_id, None::<InspectContainerOptions>).await {
                    let port = attributes.get("iron-proxy.port").cloned().unwrap_or_else(|| "8000".to_string());


                    if let Some(ip) = info.network_settings.and_then(|n| n.networks).and_then(|mut nets| nets.remove("bridge")).and_then(|b| b.ip_address) {
                        if !ip.is_empty() {
                            let target = format!("{}:{}", ip, port);
                            info!("Auto discovered: {} at {}", name, target);

                            active_containers.insert(container_id, target);
                            registry.update_docker(active_containers.values().cloned().collect());
                        }
                    }
                }
            }
            else if action == "die" {
                if let Some(target) = active_containers.remove(&container_id) {
                    warn!("Backend Died: {}. Removed {} from rotation.", name, target);
                    registry.update_docker(active_containers.values().cloned().collect());
                }
            }
        }
    }
}