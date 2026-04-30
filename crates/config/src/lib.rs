use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct ProxyConfig {
    pub server: ServerConfig,
    // TODO: we work on this when we build l4/l7 pools
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub port: u16,
}

pub fn load_config<P: AsRef<Path>>(path: P) -> Result<ProxyConfig, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;
    let config: ProxyConfig = toml::from_str(&content)?;
    Ok(config)
}
