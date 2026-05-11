use config::AdminConfig;
use health::{HealthRegistry, HealthStatus};

use axum::{Json, Router, extract::State, routing::get};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;
use tracing::{error, info};

use std::collections::HashMap;

#[derive(Clone)]
struct AppState {
    registry: HealthRegistry,
    metrics_handle: PrometheusHandle,
}

#[derive(serde::Serialize)]
struct SystemHealthResponse {
    status: String,
    backends: HashMap<String, String>,
}

pub async fn start_admin_server(config: AdminConfig, registry: HealthRegistry) {
    let metrics_handle = PrometheusBuilder::new()
        .install_recorder()
        .expect("Failed to install Prometheus recorder");

    let state = AppState {
        registry,
        metrics_handle,
    };

    // define API routes
    let app = Router::new()
        .route("/api/v1/health", get(get_health_status))
        .route("/metrics", get(get_metrics))
        .with_state(state);

    let bind_addr = format!("{}:{}", config.bind_addr, config.port);

    match TcpListener::bind(&bind_addr).await {
        Ok(listener) => {
            info!("Admin API listening on http://{}", bind_addr);
            if let Err(e) = axum::serve(listener, app).await {
                error!("Admin server failed: {}", e);
            }
        }
        Err(e) => error!("Failed to bind Admin API to {}: {}", bind_addr, e),
    }
}

async fn get_health_status(State(state): State<AppState>) -> Json<SystemHealthResponse> {
    // get all known backends
    let all_backends = state.registry.get_all_backends().await;
    let mut backend_map = HashMap::new();

    let mut all_healthy = true;

    for addr in all_backends {
        let status = state
            .registry
            .get_status(&addr)
            .await
            .unwrap_or(HealthStatus::Dead);
        let status_str = match status {
            HealthStatus::Healthy => "Healthy",
            HealthStatus::Dead => {
                all_healthy = false;
                "Dead"
            }
        };
        backend_map.insert(addr.to_string(), status_str.to_string());
    }

    let overall_status = if backend_map.is_empty() {
        "No backends Configured"
    } else if all_healthy {
        "OK"
    } else {
        "Degraded"
    };

    Json(SystemHealthResponse {
        status: overall_status.to_string(),
        backends: backend_map,
    })
}

async fn get_metrics(State(state): State<AppState>) -> String {
    state.metrics_handle.render()
}
