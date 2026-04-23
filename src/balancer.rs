use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tracing::{info, warn};

use metrics::gauge;

// RAII guard
// when created tracks connection
pub struct ConnectionTracker {
    counter: Arc<AtomicUsize>,
    address: String,
}

impl Drop for ConnectionTracker {
    fn drop(&mut self) {
        // automatically subtract 1 when user disconnects or task finishes.
        self.counter.fetch_sub(1, Ordering::SeqCst);

        gauge!("gateway_active_connections").decrement(1.0);
        gauge!("backend_active_connections", "server" => self.address.clone()).decrement(1.0);
    }
}

#[derive(Clone)]
pub struct Backend {
    pub address: String,
    pub is_healthy: bool,
    active_connections: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub struct LeastConnections {
    backends: Arc<RwLock<Vec<Backend>>>,
}

impl LeastConnections {
    pub fn new(addresses: Vec<String>) -> Self {
        // assume every ip is healthy
        let backends = addresses
            .into_iter()
            .map(|addr| Backend {
                address: addr,
                is_healthy: true,
                active_connections: Arc::new(AtomicUsize::new(0)),
            })
            .collect();
        Self {
            backends: Arc::new(RwLock::new(backends)),
        }
    }

    // new hot reloading method
    pub fn update_backends(&self, new_addresses: Vec<String>) {
        let mut backends = self.backends.write().unwrap();

        let mut updated_backends: Vec<Backend> = Vec::new();

        for addr in new_addresses {
            if let Some(existing) = backends.iter().find(|b| b.address == addr) {
                updated_backends.push(existing.clone());
            } else {
                updated_backends.push(Backend {
                    address: addr,
                    is_healthy: true,
                    active_connections: Arc::new(AtomicUsize::new(0)),
                });
            }
        }

        *backends = updated_backends;
        info!("Routing table hot-reloaded! (Least Connections Mode)");
    }

    pub fn next_target(&self) -> Option<(String, ConnectionTracker)> {
        let backends = self.backends.read().unwrap();

        let best_backend = backends.iter().filter(|b| b.is_healthy).min_by_key(|b| b.active_connections.load(Ordering::Relaxed));
        
        if let Some(backend) = best_backend {
            backend.active_connections.fetch_add(1, Ordering::SeqCst);

            // 2. THE FIX: Make sure BOTH of these lines exist!
            gauge!("gateway_active_connections").increment(1.0);
            gauge!("backend_active_connections", "server" => backend.address.clone()).increment(1.0);
            
            let tracker = ConnectionTracker { 
                counter: backend.active_connections.clone(),
                address: backend.address.clone(),
            };
            return Some((backend.address.clone(), tracker));
        }
        None
    }

    // the background daemon (manager)
    pub fn start_health_checker(&self) {
        let backends_arc = Arc::clone(&self.backends);

        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(5)).await;

                // snapshot using read
                let addresses: Vec<String> = {
                    let backends = backends_arc.read().unwrap();
                    backends.iter().map(|b| b.address.clone()).collect()
                };

                for address in addresses {
                    let is_alive = match tokio::time::timeout(
                        Duration::from_secs(1),
                        TcpStream::connect(&address),
                    )
                    .await
                    {
                        Ok(Ok(_)) => true,
                        _ => false,
                    };

                    // surgical update using write lock
                    let mut backends = backends_arc.write().unwrap();
                    if let Some(backend) = backends.iter_mut().find(|b| b.address == address) {
                        if backend.is_healthy != is_alive {
                            if is_alive {
                                info!("Backend {} is back online!", address);
                            } else {
                                warn!("Backend {} is dead! Removing from rotation.", address);
                            }
                            backend.is_healthy = is_alive;
                        }
                    }
                }
            }
        });
    }
}
