use config::ProxyConfig;
use core::panic;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tracing::{error, info};

pub struct L4Proxy {
    config: Arc<ProxyConfig>,
    backends: Vec<SocketAddr>,
    // we use atomic usize so that multiple async tasks
    // can update the counter without needing mutex lock
    current_backend: AtomicUsize,
}

// listen for standard os termination signals (Ctrl+C or SIGTERM)
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

impl L4Proxy {
    pub fn new(config: ProxyConfig) -> Self {
        // for mvp, we simply grab the first cluster's targets.
        // later we'll map multiple listeners to multiple clusters.
        let targets = config
            .clusters
            .first()
            .map(|c| c.targets.clone())
            .unwrap_or_default();

        let mut backends = Vec::new();
        for target in targets {
            match target.parse::<SocketAddr>() {
                Ok(addr) => backends.push(addr),
                Err(e) => error!("Failed to parse backend address '{}': {}", target, e),
            }
        }

        if backends.is_empty() {
            error!("No valid backend targets found in configuration!");
            // in prod we'll panic or fail gracefully
        } else {
            info!("L4 Proxy loaded {} backend targets", backends.len());
        }

        Self {
            config: Arc::new(config),
            backends,
            current_backend: AtomicUsize::new(0),
        }
    }

    // safely fetch next backend for round robin
    fn get_next_backend(&self) -> SocketAddr {
        if self.backends.is_empty() {
            panic!("Attempted to route traffic with an empty backend pool");
        }
        let idx = self.current_backend.fetch_add(1, Ordering::Relaxed);
        self.backends[idx % self.backends.len()]
    }

    pub async fn run(&self) -> std::io::Result<()> {
        let bind_addr = format!(
            "{}:{}",
            self.config.server.bind_addr, self.config.server.port
        );
        let listener = TcpListener::bind(&bind_addr).await?;

        info!("L4 Proxy Listening on {}", bind_addr);

        // join set for holding spawned connection tasks
        let mut active_connections = JoinSet::new();

        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((mut inbound, client_addr)) => {
                            if self.backends.is_empty() {
                                error!("Dropping connection from {}: no backends available", client_addr);
                                continue;
                            }

                            let backend_addr = self.get_next_backend();
                            info!(
                                "Accepted connection from {}, routing to {}",
                                client_addr, backend_addr
                            );

                            // spawn dedicated task for this connection
                            // so loop can accept next connection immediately.
                            active_connections.spawn(async move {
                                match TcpStream::connect(backend_addr).await {
                                    Ok(mut outbound) => {
                                        // copy biderectional handles shoveling bytes back and forth until either sides disconnect.
                                        match tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await{
                                            Ok((to_client, to_backend)) => {
                                                info!(
                                                    "Connection Closed! Wrote {} bytes to client, {} bytes to backend",
                                                    to_client, to_backend
                                                );
                                            }
                                            Err(e) => error!("Proxy stream error for {}: {}", client_addr, e),
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to connect to backend {}: {}", backend_addr, e);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                            // decision making on how to proceed
                            // for now we will continue
                        }
                    }
                }
                _ = shutdown_signal() => {
                    info!("Shutdown signal received. Stopping accepting connection...");
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
