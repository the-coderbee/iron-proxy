use serde::Deserialize;
use std::fs;
use std::process;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub rate_limiting: RateLimitConfig,
    pub backends: BackendsConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ServerConfig {
    pub bind_address: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RateLimitConfig {
    pub max_requests_per_minute: usize,
}

#[derive(Deserialize, Debug, Clone)]
pub struct BackendsConfig {
    pub targets: Vec<String>,
}

pub fn load_config() -> Config {
    let config_content = match fs::read_to_string("gateway_config.toml") {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Failed to read gateway_config.toml: {}", e);
            process::exit(1);
        }
    };

    match toml::from_str(&config_content) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Failed to parse config file: {}", e);
            process::exit(1);
        }
    }
}