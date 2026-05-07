use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
pub struct ProxyConfig {
    pub server: ServerConfig,
    pub admin: Option<AdminConfig>,
    pub rate_limit: Option<RateLimitConfig>,
    // we expect a list of backend clusters
    #[serde(default)]
    pub clusters: Vec<ClusterConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    pub capacity: f64,
    pub refill_rate: f64,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub port: u16,
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AdminConfig {
    pub bind_addr: String,
    pub port: u16,
}

#[derive(Debug, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub mode: ProxyMode,
    #[serde(default = "default_routing")]
    pub routing_strategy: String,
    pub targets: Vec<String>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ProxyMode {
    Tcp,
    Http,
}

fn default_routing() -> String {
    "round_robin".to_string()
}

pub fn load_config<P: AsRef<Path>>(path: P) -> Result<ProxyConfig, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;
    let config: ProxyConfig = toml::from_str(&content)?;
    Ok(config)
}
