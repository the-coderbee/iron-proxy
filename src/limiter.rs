use deadpool_redis::{Config, Pool, Runtime};
use deadpool_redis::redis::AsyncCommands;
use std::net::IpAddr;
use tracing::{error, info};

#[derive(Clone)]
pub struct RateLimiter {
    pool: Pool,
    max_requests: u64,
}

impl RateLimiter {
    pub fn new(redis_url: &str, max_requests: u64) -> Self {
        let cfg = Config::from_url(redis_url);
        let pool = cfg.create_pool(Some(Runtime::Tokio1))
            .expect("Failed to create Redis pool");
        info!("Distributed Rate Limiter connected to Redis at {}", redis_url);
        Self { pool, max_requests }
    }

    pub async fn is_blocked(&self, ip:IpAddr) -> bool {
        let mut conn = match self.pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Redis pool error: {}. Failing to open.", e);
                return false;
            }
        };

        let key = format!("rate_limit:{}", ip);

        let count: u64 = match conn.incr(&key, 1).await {
            Ok(c) => c,
            Err(e) => {
                error!("Redis INCR error: {}. Failing to open.", e);
                return false;
            }
        };

        if count == 1 {
            let _: Result<(), _> = conn.expire(&key, 60).await;
        }

        count > self.max_requests
    }
}