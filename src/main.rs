mod balancer;
mod config;
mod limiter;

use balancer::LeastConnections;
use config::load_config;
use limiter::RateLimiter;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use tracing::{error, info, warn};
use tracing_subscriber;

use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::Path;

use std::time::{Duration, Instant};

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

    let balancer = LeastConnections::new(config.backends.targets);
    balancer.start_health_checker();
    let redis_url = "redis://127.0.0.1:6379/";
    let limiter = RateLimiter::new(redis_url, config.rate_limiting.max_requests_per_minute);

    let balancer_for_watcher = balancer.clone();
    tokio::spawn(async move {
        // tokio async channel
        let (tx, mut rx) = mpsc::channel(100);

        let mut watcher = RecommendedWatcher::new(
            move |res| {
                let _ = tx.blocking_send(res);
            },
            Config::default(),
        )
        .expect("Failed to create file watcher");

        watcher
            .watch(
                Path::new("gateway_config.toml"),
                RecursiveMode::NonRecursive,
            )
            .expect("Failed to watch config file");

        let mut last_reload = Instant::now();

        while let Some(res) = rx.recv().await {
            match res {
                Ok(event) => {
                    if event.kind.is_modify() {
                        // debounce: reload if its more than 500ms
                        if last_reload.elapsed() > Duration::from_millis(500) {
                            info!("Config file changed on disk! Reloading...");
                            // reload the config using existing logic
                            let new_config = load_config();
                            // inject the new IPs directly into the active memory
                            balancer_for_watcher.update_backends(new_config.backends.targets);

                            // reset clock
                            last_reload = Instant::now();
                        }
                    }
                }
                Err(e) => error!("Watch error: {:?}", e),
            }
        }
    });

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
                            if limiter.is_blocked(client_ip).await {
                                warn!("Rate Limit Exceeded for IP: {}", client_ip);
                                let response = "HTTP/1.1 429 Too Many Requests\r\n\r\nRate Limit Exceeded. Slow Down!!";

                                let _ = client_stream.write_all(response.as_bytes()).await;
                                return;
                            }

                            // safely aquire the lock to figure out where to route this request
                            let (target_backend, _tracker) = match balancer.next_target() {
                                Some((addr, tracker)) => (addr, tracker),
                                None => {
                                    error!("🔥 All backends are offline! Dropping request from {}", addr);
                                    let response = "HTTP/1.1 503 Service Unavailable\r\n\r\nAll backend servers are down.";
                                    let _ = client_stream.write_all(response.as_bytes()).await;
                                    return;
                                }
                            };

                            info!("Routing request from {} to backend: {}", addr, target_backend);

                            // proxying
                            match tokio::time::timeout(std::time::Duration::from_secs(3), TcpStream::connect(&target_backend)).await {
                                // the connection succeded within 3 seconds
                                Ok(Ok(mut backend_stream)) => {
                                    // wrap the actual data streaming in a 30 second timeout
                                    match tokio::time::timeout(
                                        std::time::Duration::from_secs(30),
                                        tokio::io::copy_bidirectional(&mut client_stream, &mut backend_stream)
                                    ).await {
                                        Ok(Err(e)) => error!("Error during stream proxy: {}", e),
                                        Err(_) => {
                                            warn!("Backend {} hung mid-request! Dropping connection.", target_backend);
                                            let response = "HTTP/1.1 504 Gateway Timeout\r\nConnection: close\r\nContent-Length: 36\r\n\r\nThe server took too long to respond.";
                                            let _ = client_stream.write_all(response.as_bytes()).await;
                                            let _ = client_stream.shutdown().await;
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
                                    let _ = client_stream.write_all(response.as_bytes()).await;
                                    let _ = client_stream.shutdown().await;
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
