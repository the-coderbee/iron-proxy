//! # Routing Engine
//!
//! This crate provides the mathematical foundation for Iron-Proxy's load balancing.
//! It features highly concurrent state tracking and advanced routing algorithms
//! including Least Connections (Layer 4), Peak EWMA (Layer 7), and deterministic
//! Ip hashing for sticky sessions.

use dashmap::DashMap;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// half-life for exponential decay in ms
const EWMA_HL_MS: f64 = 10000.0;

/// Represents the real-time routing metrics of a single backend server.
///
/// We utilize atomic operations to update connection counts and latency
/// in sub-microseconds without thread-blocking mutexes.
#[derive(Debug)]
pub struct BackendStats {
    /// The number of currently active requests to this backend.
    pub active_requests: AtomicUsize,
    /// The ewma latency in bits.
    pub ewma_latency_bits: AtomicU64,
    /// The timestamp of the last update in milliseconds.
    pub last_update_ms: AtomicU64,
}

/// Implementation for backend statistics.
impl BackendStats {
    /// Initializes a new instance of `BackendStats`.
    pub fn new() -> Self {
        Self {
            active_requests: AtomicUsize::new(0),
            ewma_latency_bits: AtomicU64::new(f64::to_bits(0.0)),
            last_update_ms: AtomicU64::new(current_time_ms()),
        }
    }

    /// Safely reconstructs the floating-point EWMA value from the raw atomic bits.
    ///
    /// # Returns
    ///
    /// Returns the EWMA latency in milliseconds as an `6f4`.
    pub fn get_ewma(&self) -> f64 {
        f64::from_bits(self.ewma_latency_bits.load(Ordering::Relaxed))
    }
}

/// Implementation for default backend statistics.
impl Default for BackendStats {
    fn default() -> Self {
        Self::new()
    }
}

/// A lock-free state manager for routing decisions.
///
/// Shares a `DashMap` across tasks to monitor active connections and track latency costs.
#[derive(Clone, Default)]
pub struct ConnectionTracker {
    stats: Arc<DashMap<SocketAddr, BackendStats>>,
}

/// Implementation for connection tracker.
impl ConnectionTracker {
    /// Creates a new, empty connection tracker.
    pub fn new() -> Self {
        Self {
            stats: Arc::new(DashMap::new()),
        }
    }

    // [shared lifecycle] call this the instant a connection is established
    /// Increments the active request counter for the specified backend.
    ///
    /// # Arguments
    ///
    /// * `addr` - The socket address of the backend server.
    pub fn inc(&self, addr: SocketAddr) {
        let entry = self.stats.entry(addr).or_default();
        entry.active_requests.fetch_add(1, Ordering::Relaxed);
    }

    // l4 only: call this when a tcp stream disconnects (no latency tracking)
    /// Decrements the active request counter without updating latency.
    ///
    /// # Arguments
    ///
    /// * `addr` - The socket address of the backend server.
    pub fn dec(&self, addr: SocketAddr) {
        if let Some(entry) = self.stats.get(&addr) {
            let current_reqs = entry.active_requests.load(Ordering::Relaxed);
            if current_reqs > 0 {
                entry.active_requests.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    // l7 only: call this when an HTTP response finishes to update the peak EWMA
    /// Decrements the active request counter and calculates a new latency penalty.
    ///
    /// # Arguments
    ///
    /// * `addr` - The socket address of the target backend server.
    /// * `rtt_ms` - The round-trip time of the completed request in milliseconds.
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

    /// Returns a sticky backend based on the client IP and a list of healthy backends.
    ///
    /// # Arguments
    ///
    /// * `client_ip` - The IP address of the client.
    /// * `healthy_backends` - A list of healthy backend addresses.
    ///
    /// # Returns
    ///
    /// Returns `Some<SocketAddr>` representing the selected backend, or `None` if
    /// no healthy backends are available.
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

    /// Returns the backend with the fewest active requests (Layer 4 strategy).
    ///
    /// # Arguments
    ///
    /// * `healthy_backends` - A list of healthy backend addresses.
    ///
    /// # Returns
    ///
    /// Returns `Some<SocketAddr>` representing the selected backend, or `None` if
    /// no healthy backends are available.
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

    /// Returns the backend with the lowest L7 cost (Peak EWMA strategy).
    ///
    /// # Arguments
    ///
    /// * `healthy_backends` - A list of healthy backend addresses.
    ///
    /// # Returns
    ///
    /// Returns `Some<SocketAddr>` representing the selected backend, or `None` if
    /// no healthy backends are available.
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

    /// Calculates the L7 cost for a given backend address.
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

/// Returns the current time in milliseconds since the Unix epoch.
///
/// # Returns
///
/// Returns a `u64` representing the current time in milliseconds since the Unix epoch.
///
/// # Panics
///
/// Panics if the system clock is set before the Unix epoch (1970-01-01 00:00:00 UTC).
pub fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|e| panic!("System time error: {}", e))
        .as_millis() as u64
}
