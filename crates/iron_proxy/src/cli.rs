use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(author, version, about = "Iron-Proxy: High-performance load balancer")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Init,

    // Run the proxy server
    Run {
        // Path to the configuration file
        #[arg(short, long, default_value = "iron-proxy.toml")]
        config: String,
    },
    Start {
        #[arg(short, long, default_value = "iron-proxy.toml")]
        config: String,
    },
    Stop,

    // Get real time health status of the cluster
    Status {
        #[arg(long, default_value = "http://127.0.0.1:9090")]
        admin_url: String,
    },
    // Validate the configuration file
    Check {
        #[arg(short, long, default_value = "iron-proxy.toml")]
        config: String,
    },
}

pub const DEFAULT_CONFIG: &str = r#"# Iron-Proxy Enterprise Configuration
# -----------------------------------
# This file is hot-reloadable. Changes made here will be applied automatically without dropping connections.

[admin]
bind_addr = "127.0.0.1:9090"

# -----------------------------------
# Layer 7: HTTP Reverse Proxy (Peak EWMA & Sticky Sessions)
# -----------------------------------
[[clusters]]
name = "web_backend"
mode = "http"
# Enable deterministic IP Hashing for stateful legacy apps
sticky_sessions = false
# Automatically retry requests on 5xx errors before returning to the client
max_retries = 3 
targets = [
    "127.0.0.1:8081",
    "127.0.0.1:8082",
    "127.0.0.1:8083"
]

# -----------------------------------
# Layer 4: Raw TCP Proxy (Least Connections)
# -----------------------------------
[[tcp_servers]]
name = "database_cluster"
bind_addr = "127.0.0.1:6000"
targets = [
    "127.0.0.1:9000",
    "127.0.0.1:9001"
]
"#;
