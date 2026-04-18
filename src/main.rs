mod config;
mod limiter;
mod balancer;

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{self, AsyncWriteExt};
use config::load_config;
use limiter::RateLimiter;
use balancer::RoundRobin;

use tracing::{info, warn, error};
use tracing_subscriber;

#[tokio::main]
async fn main() {
    // initialize logging subscriber
    tracing_subscriber::fmt()
        .with_target(false)
        .init();

    // load the dynamic configuration
    let config = load_config();
    
    // bind to the address specified in the toml file
    let listener = TcpListener::bind(&config.server.bind_address)
        .await
        .expect("Failed to bind to configured address");

    info!("Async Gateway starting on {}...", config.server.bind_address);

    let balancer = RoundRobin::new(config.backends.targets);
    balancer.start_health_checker();
    let limiter = RateLimiter::new(config.rate_limiting.max_requests_per_minute);
    
    loop {
        let (mut client_stream, addr) = listener.accept().await.expect("Failed to accept");
        let client_ip = addr.ip();

        let balancer = balancer.clone();
        let limiter = limiter.clone();

        // tokio::spwan takes this connection and throws it onto a background process
        tokio::spawn(async move {
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
}
