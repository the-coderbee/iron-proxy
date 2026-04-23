use serde::Deserialize;
use std::env;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub rate_limiting: RateLimitConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ServerConfig {
    pub bind_address: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RateLimitConfig {
    pub max_requests_per_minute: u64,
}

pub fn load_config() -> Config {
    // Read from Environment Variables, or fallback to sensible defaults
    let bind_address = env::var("GATEWAY_BIND_ADDRESS")
        .unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        
    let rate_limit_str = env::var("GATEWAY_RATE_LIMIT")
        .unwrap_or_else(|_| "100".to_string());
        
    let max_requests_per_minute = rate_limit_str.parse().unwrap_or(100);

    Config {
        server: ServerConfig { bind_address },
        rate_limiting: RateLimitConfig { max_requests_per_minute },
    }
}
