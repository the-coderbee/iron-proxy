//! # Layer 4 Proxy Engine
//!
//! This crate implements a high-performance, raw TCP byte-streaming proxy.
//! It seamlessly routes incoming TCP connections to upstream backends using a
//! Least Connections strategy, providing zero-downtime graceful shutdowns and
//! bidirectional byte shoveling.

use config::TcpServerConfig;
use health::HealthRegistry;
use router::ConnectionTracker;

use metrics::{counter, gauge};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tracing::{error, info};

use core::panic;
use std::net::SocketAddr;

/// Asynchronously waits for a system shutdown signal (Ctrl+C or SIGTERM).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// The core struct representing a Layer 4 TCP proxy instance.
pub struct L4Proxy {
    config: TcpServerConfig,
    registry: HealthRegistry,
    tracker: ConnectionTracker,
}

impl L4Proxy {
    /// Initialize a new instance of l$ Proxy.
    ///
    /// # Arguments
    ///
    /// * `config` - The TCP server configuration.
    /// * `registry` - The health registry for backend monitoring.
    /// * `tracker` - The connection tracker for managing active connections.
    pub fn new(
        config: TcpServerConfig,
        registry: HealthRegistry,
        tracker: ConnectionTracker,
    ) -> Self {
        Self {
            config,
            registry,
            tracker,
        }
    }

    /// Queries the registry and tracker to find the best available upstream backend.
    async fn get_next_backend(&self) -> Option<SocketAddr> {
        let healthy = self.registry.get_healthy_backends().await;
        let mut available = Vec::new();
        for addr in healthy {
            if self.config.targets.contains(&addr.to_string()) {
                available.push(addr);
            }
        }
        if available.is_empty() {
            return None;
        }

        self.tracker.get_best_l4(&available)
    }

    /// Starts the main event loop for accepting and routing TCP connections.
    ///
    /// This method runs indefinitely until a shutdown signal is received. Upon receiving
    /// a signal, it stops accepting new connections and gracefully waits for all active
    /// byte streams to drain.
    ///
    /// # Errors
    ///
    /// Returns an `std::io::Error` if proxy fails to bind to the configured local
    /// port or address.
    pub async fn run(&self) -> std::io::Result<()> {
        let bind_addr = format!("{}:{}", self.config.bind_addr, self.config.port);
        let listener = TcpListener::bind(&bind_addr).await?;

        info!("L4 Proxy Listening on {}", bind_addr);

        // join set for holding spawned connection tasks
        let mut active_connections = JoinSet::new();

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((mut inbound, client_addr)) => {
                            gauge!("iron_proxy_l4_active_connection").increment(1.0);
                            counter!("iron_proxy_l4_connections_total").increment(1);

                            let backend_addr_opt = self.get_next_backend().await;


                            if let Some(backend_addr) = backend_addr_opt {
                                info!("Accepted L4 connection from {}, routing to {}", client_addr, backend_addr);
                                // spawn dedicated task for this connection
                                // so loop can accept next connection immediately.

                                self.tracker.inc(backend_addr);

                                let task_tracker = self.tracker.clone();

                                active_connections.spawn(async move {
                                    match TcpStream::connect(backend_addr).await {
                                        Ok(mut outbound) => {
                                            // copy biderectional handles shoveling bytes back and forth until either sides disconnect.
                                            match tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await{
                                                Ok((to_client, to_backend)) => {
                                                    info!(
                                                        "L4 Connection Closed! Wrote {} bytes to client, {} bytes to backend",
                                                        to_client, to_backend
                                                    );
                                                }
                                                Err(e) => error!("Proxy stream error for {}: {}", client_addr, e),
                                            }
                                        }
                                        Err(e) => error!("Failed to connect to backend {}: {}", backend_addr, e),
                                    }
                                    task_tracker.dec(backend_addr);
                                    gauge!("iron_proxy_l4_active_connections").decrement(1.0);
                                });
                            } else {
                                error!("Dropping L4 connection from {}: No healthy backends available", client_addr);
                                gauge!("iron_proxy_l4_active_connections").decrement(1.0);
                            }
                        }
                        Err(e) => error!("Failed to accept connection: {}", e),
                    }
                }
                _ = shutdown_signal() => {
                    info!("L4 Shutdown signal received. Stopping accepting connection...");
                    break;
                }
            }
        }

        if !active_connections.is_empty() {
            info!(
                "Waiting for {} active connection(s) to drain...",
                active_connections.len()
            );
            while let Some(res) = active_connections.join_next().await {
                if let Err(e) = res {
                    error!("A proxy task panicked during shutdown: {}", e);
                }
            }
        }

        info!("All connections closed. Proxy shut down gracefully.");
        Ok(())
    }
}
