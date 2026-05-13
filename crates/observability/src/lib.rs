//! # Observability & Telemetry
//!
//! This crate provides centralized logging and telemetry setup using the `tracing`
//! ecosystem. It configures structured JSON logging that is suitable for ingestion
//! by systems like Datadog, ELK, or Grafana Loki.

use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

/// Initializes the global tracing subscriber.
///
/// This function reads the `RUST_LOG` environment variable to determine the
/// log level (defaulting to `info`). It formats log events as flattened JSON
/// objects to ensure compatibility with modern structured logging pipelines.
pub fn init_telemetry() {
    // default to info if the RUST_LOG env variable is not set
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Configure json formatter
    let formatting_layer = fmt::layer()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false);

    tracing_subscriber::registry()
        .with(filter)
        .with(formatting_layer)
        .init();
}
