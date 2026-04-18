mod balancer;
mod config;
mod limiter;

use balancer::RoundRobin;
use config::load_config;
use limiter::RateLimiter;
use tokio::io::{self, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::task::JoinSet;

use tracing::{error, info, warn};
use tracing_subscriber;

#[tokio::main]
async fn main() {
    // initialize logging subscriber
    tracing_subscriber::fmt().with_target(false).init();

    // load the dynamic configuration
    let config = load_config();

    // bind to the address specified in the toml file
    let listener = TcpListener::bind(&config.server.bind_address)
        .await
        .expect("Failed to bind to configured address");

    info!(
        "Async Gateway starting on {}...",
        config.server.bind_address
    );

    let balancer = RoundRobin::new(config.backends.targets);
    balancer.start_health_checker();
    let limiter = RateLimiter::new(config.rate_limiting.max_requests_per_minute);

    let mut active_connections = JoinSet::new();

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((mut client_stream, addr)) => {
                        let client_ip = addr.ip();
                        let balancer = balancer.clone();
                        let limiter = limiter.clone();

                        active_connections.spawn(async move {
                            if limiter.is_blocked(client_ip) {
                                warn!("Rate Limit Exceeded for IP: {}", client_ip);
                                let response = "HTTP/1.1 429 Too Many Requests\r\n\r\nRate Limit Exceeded. Slow Down!!";

                                let _ = client_stream.write_all(response.as_bytes()).await;
                                return;
                            }

                            // safely aquire the lock to figure out where to route this request
                            let target_backend = match balancer.next_target() {
                                Some(addr) => addr,
                                None => {
                                    error!("🔥 All backends are offline! Dropping request from {}", addr);
                                    let response = "HTTP/1.1 503 Service Unavailable\r\n\r\nAll backend servers are down.";
                                    let _ = client_stream.write_all(response.as_bytes()).await;
                                    return;
                                }
                            };

                            info!("Routing request from {} to backend: {}", addr, target_backend);

                            // proxying
                            match TcpStream::connect(&target_backend).await {
                                Ok(mut backend_stream) => {
                                    if let Err(e) = io::copy_bidirectional(&mut client_stream, &mut backend_stream).await {
                                        error!("Error during stream proxy: {}", e);
                                    }
                                }
                                Err(e) => error!("Gateway Error: Could not connect to {}: {}", target_backend, e),
                            }
                        });
                    }
                    Err(e) => error!("Failed to accept connections: {}", e),
                }
            }
            // os sends ctrl+c shutdown signal
            _ = signal::ctrl_c() => {
                info!("Shutdown signal received! Stopping new connections...");
                break;
            }
        }
    }

    info!("Waiting for {} active connections to finish...", active_connections.len());

    while let Some(_) = active_connections.join_next().await {
        // this unblocks one by one
    }

    info!("All connections safely closed. Shutting down...");
}
