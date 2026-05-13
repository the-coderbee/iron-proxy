//! # Configuration Management
//!
//! This crate handles parsing and validating the `iron-proxy.toml` configuration file.
//! It defines the strongly-typed schema used across the proxy to configure Layer 4 (TCP)
//! and Layer 7 (HTTP) routing, token-bucket rate limiting, and the administrative control plane.

use serde::Deserialize;
use std::path::Path;

/// The root configuration object representing the entire state of the proxy.
///
/// This struct is safely hot-reloadable. When the configuration file on disk changed,
/// a new instance is parsed and atomically swapped in memory without dropping active connections.
#[derive(Debug, Deserialize)]
pub struct ProxyConfig {
    /// Global settings for the primary HTTP/HTTPS listener.
    pub server: ServerConfig,
    /// Optional configuration for the telemetry and administrative API.
    pub admin: Option<AdminConfig>,
    /// Optional configuration for token-bucket rate limiting.
    pub rate_limit: Option<RateLimitConfig>,
    /// A collection of Layer 7 (HTTP) backend clusters.
    #[serde(default)]
    pub clusters: Vec<ClusterConfig>,
    /// A collection of Layer 4 (TCP) backend clusters.
    #[serde(default)]
    pub tcp_servers: Vec<TcpServerConfig>,
}

/// Defines a raw Layer 4 TCP proxy listener and its upstream targets.
#[derive(Debug, Deserialize, Clone)]
pub struct TcpServerConfig {
    /// The local IP address to bind the TCP server to (e.g., "127.0.0.1").
    pub bind_addr: String,
    /// The port to bind the TCP server to.
    pub port: u16,
    /// A list of upstream backend addresses for the TCP server.
    pub targets: Vec<String>,
}

/// Configuration for token-bucket rate limiting.
#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    /// The maximum number of tokens the bucket can hold (burst capacity).
    pub capacity: f64,
    /// The number of tokens added to the bucket per second.
    pub refill_rate: f64,
}

/// Primary server settings, including optional TLS termination.
#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    /// The local IP address to bind the HTTP server to.
    pub bind_addr: String,
    /// The local port for the HTTP server.
    pub port: u16,
    /// Optional TLS configuration. If provided, the server accepts HTTPS traffic.
    pub tls: Option<TlsConfig>,
}

/// TLS certificate and private key paths for HTTPS termination.
#[derive(Debug, Deserialize, Clone)]
pub struct TlsConfig {
    /// Path to the X.509 certificate chain (PEM format).
    pub cert_path: String,
    /// Path to the private key (PEM format).
    pub key_path: String,
}

/// Configuration for the internal Control Plane API.
///
/// Exposes real-tome health metrics, Prometheus endpoints, and operational status.
#[derive(Debug, Deserialize, Clone)]
pub struct AdminConfig {
    /// The local IP address to bind the Admin API to.
    pub bind_addr: String,
    /// The local port for the Admin API.
    pub port: u16,
}

/// Defines a Layer 7 routing cluster and its resilience policies.
#[derive(Debug, Deserialize)]
pub struct ClusterConfig {
    /// A human-readable identifier for this backend cluster.
    pub name: String,
    /// The protocol mode for this cluster.
    pub mode: ProxyMode,
    /// A list of upstream HTTP backend addresses.
    pub targets: Vec<String>,
    /// The number of times to automatically retry failed (5xx) requests.
    #[serde(default)]
    pub max_retries: usize,
    /// If true, bypass Peak EWMA routing and uses deterministic IP hashing.
    #[serde(default)]
    pub sticky_sessions: bool,
}

/// Specifies the operational mode of a proxy cluster.
#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ProxyMode {
    /// Raw Layer 4 TCP byte streaming.
    Tcp,
    /// Layer 7 HTTP request/response handling.
    Http,
}

/// Parses a TOML configuration file from the filesystem into a strongly-typed `ProxyConfig`.
///
/// # Arguments
///
/// * `path` - A path-like reference pointing to the `iron-proxy.toml` configuration file.
///
/// * Returns
///
/// Returns the parsed configuration, or an error if the file is missing, unreadable,
/// or fails strict schema validation.
pub fn load_config<P: AsRef<Path>>(path: P) -> Result<ProxyConfig, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;
    let config: ProxyConfig = toml::from_str(&content)?;
    Ok(config)
}
