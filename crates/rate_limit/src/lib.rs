//! # Rate Limiting
//!
//! This crate implements a highly concurrent, IP-based token-bucket rate limiter.
//! It protects the downstream proxy targets from volumetric attacks or abusive clients.

use tokio::sync::RwLock;

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

/// Represents a single IP's token state.
///
/// The token bucket algorithm allows for bursts of traffic up to `capacity`,
/// while enforcing a steady long-term `refill_rate`.
struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_rate: f64,
    last_refill: Instant,
}

/// Represents a rate limiter for managing token buckets per IP.
impl TokenBucket {
    /// Creates a new token bucket, initially full to its capacity.
    pub fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            capacity,
            tokens: capacity,
            refill_rate,
            last_refill: Instant::now(),
        }
    }

    /// Attempts to consume a specified number of tokens from the bucket.
    ///
    /// Returns `true` if the tokens were successfully consumed, or `false` if
    /// the bucket does not have enough tokens.
    fn consume(&mut self, tokens: f64) -> bool {
        self.refill();
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    /// Calculates the time elapsed since the last request and adds new tokens.
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        let new_tokens = elapsed * self.refill_rate;

        // add new tokens but dont exceed max capacity
        self.tokens = (self.tokens + new_tokens).min(self.capacity);
        self.last_refill = now;
    }
}

/// A globally shared, thread-safe rate limiter.
///
/// Maintains an internal map of IP addresses to thei individual `TokenBucket`s.
#[derive(Clone)]
pub struct RateLimiter {
    /// A map of IP addresses to their respective token buckets.
    buckets: Arc<RwLock<HashMap<IpAddr, TokenBucket>>>,
    /// The maximum number of tokens the bucket can hold.
    capacity: f64,
    /// The rate at which tokens are added to the bucket per second.
    refill_rate: f64,
}

/// Creates a new rate limiter with the specified capacity and refill rate.
impl RateLimiter {
    pub fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            buckets: Arc::new(RwLock::new(HashMap::new())),
            capacity,
            refill_rate,
        }
    }

    /// Checks if a request from the given IP address is permitted.
    ///
    /// Consumes 1.0 token per request. Returns `true` if allowed, `false` if rate-limited.
    pub async fn check(&self, ip: IpAddr) -> bool {
        let mut buckets = self.buckets.write().await;

        // find ip bucket or create one if new
        let bucket = buckets
            .entry(ip)
            .or_insert_with(|| TokenBucket::new(self.capacity, self.refill_rate));
        bucket.consume(1.0)
    }
}
