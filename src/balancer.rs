use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::sleep;
use tracing::{info, warn};


#[derive(Clone)]
pub struct Backend {
    pub address: String,
    pub is_healthy: bool,
}

#[derive(Clone)]
pub struct RoundRobin {
    backends: Arc<Mutex<Vec<Backend>>>,
    current_index: Arc<Mutex<usize>>,
}

impl RoundRobin {
    pub fn new(addresses: Vec<String>) -> Self {
        // assume every ip is healthy
        let backends = addresses
            .into_iter()
            .map(|addr| Backend {
                address: addr,
                is_healthy: true,
            })
            .collect();
        Self {
            backends: Arc::new(Mutex::new(backends)),
            current_index: Arc::new(Mutex::new(0)),
        }
    }

    pub fn next_target(&self) -> Option<String> {
        let mut index = self.current_index.lock().unwrap();
        let backends = self.backends.lock().unwrap();
        let len = backends.len();

        for _ in 0..len {
            let backend = &backends[*index];
            *index = (*index + 1) % len;

            if backend.is_healthy {
                return Some(backend.address.clone());
            }
        }
        None
    }

    // the background daemon (manager)
    pub fn start_health_checker(&self) {
        let backends_arc = Arc::clone(&self.backends);

        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(5)).await;

                let addresses: Vec<String> = {
                    let backends = backends_arc.lock().unwrap();
                    backends.iter().map(|b| b.address.clone()).collect()
                };

                for address in addresses {
                    let is_alive = match tokio::time::timeout(
                        Duration::from_secs(1), 
                        TcpStream::connect(&address),
                    ).await {
                        Ok(Ok(_)) => true,
                        _ => false,
                    };

                    let mut backends = backends_arc.lock().unwrap();
                    if let Some(backend) = backends.iter_mut().find(|b| b.address == address) {
                        if backend.is_healthy != is_alive {
                            if is_alive {
                                info!("Backend {} is back online!", address);
                            }else {
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
