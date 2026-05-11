use dashmap::DashMap;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// half-life for exponential decay in ms
const EWMA_HL_MS: f64 = 10000.0;

#[derive(Debug)]
pub struct BackendStats {
    pub active_requests: AtomicUsize,
    pub ewma_latency_bits: AtomicU64,
    pub last_update_ms: AtomicU64,
}

impl BackendStats {
    pub fn new() -> Self {
        Self {
            active_requests: AtomicUsize::new(0),
            ewma_latency_bits: AtomicU64::new(f64::to_bits(0.0)),
            last_update_ms: AtomicU64::new(current_time_ms()),
        }
    }

    pub fn get_ewma(&self) -> f64 {
        f64::from_bits(self.ewma_latency_bits.load(Ordering::Relaxed))
    }
}

impl Default for BackendStats {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Default)]
pub struct ConnectionTracker {
    stats: Arc<DashMap<SocketAddr, BackendStats>>,
}

impl ConnectionTracker {
    pub fn new() -> Self {
        Self {
            stats: Arc::new(DashMap::new()),
        }
    }

    // shared lifecycle

    // call this the instant a connection is established
    pub fn inc(&self, addr: SocketAddr) {
        let entry = self.stats.entry(addr).or_default();
        entry.active_requests.fetch_add(1, Ordering::Relaxed);
    }

    // l4 only: call this when a tcp stream disconnects (no latency tracking)
    pub fn dec(&self, addr: SocketAddr) {
        if let Some(entry) = self.stats.get(&addr) {
            let current_reqs = entry.active_requests.load(Ordering::Relaxed);
            if current_reqs > 0 {
                entry.active_requests.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    // l7 only: call this when an HTTP response finishes to update the peak EWMA
    pub fn dec_and_update_ewma(&self, addr: SocketAddr, rtt_ms: f64) {
        if let Some(entry) = self.stats.get(&addr) {
            let current_reqs = entry.active_requests.load(Ordering::Relaxed);
            if current_reqs > 0 {
                entry.active_requests.fetch_sub(1, Ordering::Relaxed);
            }

            // calculate peak EWMA
            let now = current_time_ms();
            let last_update = entry.last_update_ms.swap(now, Ordering::Relaxed);
            let delta_t = (now.saturating_sub(last_update)) as f64;

            let current_ewma = f64::from_bits(entry.ewma_latency_bits.load(Ordering::Relaxed));

            let new_ewma = if current_ewma == 0.0 || rtt_ms > current_ewma {
                rtt_ms
            } else {
                let alpha = (-delta_t / EWMA_HL_MS).exp();
                (rtt_ms * (1.0 - alpha)) + (current_ewma * alpha)
            };

            entry
                .ewma_latency_bits
                .store(f64::to_bits(new_ewma), Ordering::Relaxed);
        }
    }

    // routing algorithms

    pub fn get_sticky_backend(
        &self,
        client_ip: IpAddr,
        healthy_backends: &[SocketAddr],
    ) -> Option<SocketAddr> {
        if healthy_backends.is_empty() {
            return None;
        }

        // we must sort the backend alphabetically/numerically first!
        // if we dont the registry might hand us list in random order
        // which would break the deterministic hashing
        let mut sorted_backends = healthy_backends.to_vec();
        sorted_backends.sort();

        let mut hasher = DefaultHasher::new();
        client_ip.hash(&mut hasher);
        let hash_value = hasher.finish();

        let idx = (hash_value as usize) % sorted_backends.len();
        Some(sorted_backends[idx])
    }

    pub fn get_best_l4(&self, healthy_backends: &[SocketAddr]) -> Option<SocketAddr> {
        healthy_backends
            .iter()
            .min_by_key(|addr| {
                self.stats
                    .get(addr)
                    .map(|entry| entry.active_requests.load(Ordering::Relaxed))
                    .unwrap_or(0)
            })
            .copied()
    }

    // l7 routing: peak EWMA cost function (active connections * EWMA latency)
    pub fn get_best_l7(&self, healthy_backends: &[SocketAddr]) -> Option<SocketAddr> {
        healthy_backends
            .iter()
            .min_by(|&&a, &&b| {
                let cost_a = self.calculate_l7_cost(a);
                let cost_b = self.calculate_l7_cost(b);
                cost_a
                    .partial_cmp(&cost_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .copied()
    }

    fn calculate_l7_cost(&self, addr: SocketAddr) -> f64 {
        self.stats
            .get(&addr)
            .map(|entry| {
                let active = entry.active_requests.load(Ordering::Relaxed) as f64;
                let ewma = entry.get_ewma();

                // if EWMA is 0 (brand new backend), we give it tiny baseline cost
                // to avoid multiplying by 0.
                let baseline_ewma = if ewma == 0.0 { 1.0 } else { ewma };

                // cost = connections * latency
                // we add 1 to active connections so a completely idle server
                // still has a cost strictly based on its historical latency.
                (active + 1.0) * baseline_ewma // brand new untracked server have lower costs (0) to force exploration
            })
            .unwrap_or(0.0)
    }
}

// utility function to get current time in milliseconds quickly
pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|e| panic!("System time error: {}", e))
        .as_millis() as u64
}
