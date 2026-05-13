//! # Administrative Control Plane
//!
//! This crate provides the internal HTTP API for Iron-Proxy. It exposes endpoints
//! for real-time observability, including cluster health status and Prometheus metrics.
//! The server runs asynchronously on its own Tokio task using `axum` framework.

use config::AdminConfig;
use health::{HealthRegistry, HealthStatus};

use axum::{Json, Router, extract::State, routing::get};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tokio::net::TcpListener;
use tracing::{error, info};

use std::collections::HashMap;

/// Shared application state injected into Axum route handlers.
///
/// This struct is cheaply cloneable and provides thread-safe access to the global
/// health registry and the Prometheus metrics recorder.
#[derive(Clone)]
struct AppState {
    /// The global registry tracking backend health states.
    registry: HealthRegistry,
    /// The handle used to render currently collected metrics.
    metrics_handle: PrometheusHandle,
}

/// The JSON response schema for the `/api/v1/health` endpoint.
#[derive(serde::Serialize)]
struct SystemHealthResponse {
    /// The overall health of the cluster (e.g., "OK", "Degraded", "No backends Configured").
    status: String,
    /// A map of the backend addresses to their current operational state (e.g., "Healthy", "Dead").
    backends: HashMap<String, String>,
}

/// Starts the administrative API server in the background.
///
/// This function installs the global Prometheus recorder and binds an `axum`
/// web server to the address specified in the `AdminConfig`. It exposes two primary routes:
/// * `/api/v1/health` - Cluster health summary.
/// * `/metrics` - Raw Prometheus metrics telemetry.
///
/// # Arguments
///
/// * `config` - The configuration specifying the bind address and port.
/// * `registry` - The cloned reference to the global health registry.
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

/// Route handler for `GET /api/v1/health`.
///
/// Aggregates the real-time status of all active backends tracked by the proxy.
/// If any single backend is marked as `Dead`, the overall cluster status is
/// reported as `Degraded`.
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

/// Route handler for `GET /metrics`.
///
/// Renders the current state of all recorded counters, gauges, and histograms
/// into a Prometheus-compatible plain-text format.
async fn get_metrics(State(state): State<AppState>) -> String {
    state.metrics_handle.render()
}
