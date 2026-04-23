mod balancer;
mod config;
mod limiter;
pub mod discovery;
pub mod registry;
pub mod tls;

use balancer::LeastConnections;
use config::load_config;
use limiter::RateLimiter;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
// use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;

use metrics::counter;


use tracing::{error, info, warn};
use tracing_subscriber;

use crate::discovery::watch_docker_events;
use crate::registry::{ProviderRegistry, watch_static_routes};
use crate::tls::load_tls_config;

#[tokio::main]
async fn main() {
    // initialize logging subscriber
    tracing_subscriber::fmt().with_target(false).init();

    // boot Prometheus metrcis endpoint on port 9090
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], 9090))
        .install()
        .expect("Failed to install Prometheus exporter");

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

    let balancer = LeastConnections::new(vec![]);
    balancer.start_health_checker();


    let redis_url = "redis://127.0.0.1:6379/";
    let limiter = RateLimiter::new(redis_url, config.rate_limiting.max_requests_per_minute);

    let registry = ProviderRegistry::new(balancer.clone());

    let docker_registry = registry.clone();

    tokio::spawn(async move {
        watch_docker_events(docker_registry).await;
    });

    let static_registry = registry.clone();
    tokio::spawn(async move {
        watch_static_routes(static_registry).await;
    });

    let mut active_connections = JoinSet::new();

    let tls_config = load_tls_config();
    let tls_acceptor = TlsAcceptor::from(tls_config);
    info!("TLS Encryption enabled. Listening for HTTPS traffic .");

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((client_stream, addr)) => {
                        // log total requests
                        counter!("gateway_requests_total").increment(1);
                        
                        let client_ip = addr.ip();
                        let balancer = balancer.clone();
                        let limiter = limiter.clone();
                        let tls_acceptor = tls_acceptor.clone();

                        active_connections.spawn(async move {
                            // the handshake: upgrade raw tcp connection to encrypted TLS connection.
                            // we make secure stream mut because we write data to it later.
                            let mut secure_stream = match tls_acceptor.accept(client_stream).await {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("TLS handshake failed from {}: {}", addr, e);
                                    return;
                                }
                            };

                            // rate limiting
                            if limiter.is_blocked(client_ip).await {
                                warn!("Rate Limit Exceeded for IP: {}", client_ip);

                                // 🚨 THE FIX: Drain the OS buffer so it doesn't fire a TCP Reset!
                                let mut drain_buf = [0; 1024];
                                let _ = secure_stream.read(&mut drain_buf).await;

                                let response = "HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\nContent-Length: 32\r\n\r\nRate Limit Exceeded. Slow Down!!";

                                let _ = secure_stream.write_all(response.as_bytes()).await;
                                let _ = secure_stream.shutdown().await;
                                return;
                            }

                            // safely aquire the lock to figure out where to route this request
                            let (target_backend, _tracker) = match balancer.next_target() {
                                Some((addr, tracker)) => (addr, tracker),
                                None => {
                                    error!("🔥 All backends are offline! Dropping request from {}", addr);

                                    // 🚨 THE FIX: Drain the OS buffer!
                                    let mut drain_buf = [0; 1024];
                                    let _ = secure_stream.read(&mut drain_buf).await;

                                    let response = "HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 29\r\n\r\nAll backend servers are down.";
                                    let _ = secure_stream.write_all(response.as_bytes()).await;
                                    let _ = secure_stream.shutdown().await;
                                    return;
                                }
                            };

                            info!("Routing request from {} to backend: {}", addr, target_backend);

                            // proxying
                            match tokio::time::timeout(std::time::Duration::from_secs(3), TcpStream::connect(&target_backend)).await {
                                // the connection succeded within 3 seconds
                                Ok(Ok(mut backend_stream)) => {
                                    // critical fix: pass secure stream to bidirectional_copy
                                    match tokio::time::timeout(
                                        std::time::Duration::from_secs(30),
                                        tokio::io::copy_bidirectional(&mut secure_stream, &mut backend_stream)
                                    ).await {
                                        Ok(Err(e)) => error!("Error during stream proxy: {}", e),
                                        Err(_) => {
                                            warn!("Backend {} hung mid-request! Dropping connection.", target_backend);
                                            let response = "HTTP/1.1 504 Gateway Timeout\r\nConnection: close\r\nContent-Length: 36\r\n\r\nThe server took too long to respond.";
                                            let _ = secure_stream.write_all(response.as_bytes()).await;
                                            let _ = secure_stream.shutdown().await;
                                        }
                                        _ => {} // success!
                                    }
                                }
                                // the connection was instantly rejected
                                Ok(Err(e)) => error!("Gateway Error: Could not connect to {}: {}", target_backend, e),

                                // connection timer ran out
                                Err(_) => {
                                    error!("Connection to {} timed out!", target_backend);
                                    let response = "HTTP/1.1 504 Gateway Timeout\r\n\r\nConnection timed out.";
                                    let _ = secure_stream.write_all(response.as_bytes()).await;
                                    let _ = secure_stream.shutdown().await;
                                }
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

    info!(
        "Waiting for {} active connections to finish...",
        active_connections.len()
    );

    while let Some(_) = active_connections.join_next().await {
        // this unblocks one by one
    }

    info!("All connections safely closed. Shutting down...");
    std::process::exit(0);
}
