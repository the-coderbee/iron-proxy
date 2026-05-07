use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_rate: f64,
    last_refill: Instant,
}

impl TokenBucket {
    pub fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            capacity,
            tokens: capacity,
            refill_rate,
            last_refill: Instant::now(),
        }
    }

    fn consume(&mut self, tokens: f64) -> bool {
        self.refill();
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        let new_tokens = elapsed * self.refill_rate;

        // add new tokens but dont exceed max capacity
        self.tokens = (self.tokens + new_tokens).min(self.capacity);
        self.last_refill = now;
    }
}

#[derive(Clone)]
pub struct RateLimiter {
    // map an IP to its own token bucket
    buckets: Arc<RwLock<HashMap<IpAddr, TokenBucket>>>,
    capacity: f64,
    refill_rate: f64,
}

impl RateLimiter {
    pub fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            buckets: Arc::new(RwLock::new(HashMap::new())),
            capacity,
            refill_rate,
        }
    }

    pub async fn check(&self, ip: IpAddr) -> bool {
        let mut buckets = self.buckets.write().await;

        // find ip bucket or create one if new
        let bucket = buckets
            .entry(ip)
            .or_insert_with(|| TokenBucket::new(self.capacity, self.refill_rate));
        bucket.consume(1.0)
    }
}
