//! # Command Line Interface
//!
//! This module defines the command-line argument parsing for Iron-Proxy.
//! It uses the `clap` crate to provide subcommands for managing the proxy's
//! lifecycle, including initialization, validation, and execution.

use clap::{Parser, Subcommand};

/// The main command-line interface struct for Iron-Proxy.
#[derive(Parser)]
#[command(author, version, about = "Iron-Proxy: High-performance load balancer")]
pub struct Cli {
    /// The specific subcommand to execute.
    #[command(subcommand)]
    pub command: Commands,
}

/// Available subcommands for managing the proxy lifecycle.
#[derive(Subcommand)]
pub enum Commands {
    /// Generates a standard `iron-proxy.toml` template configuration file in the current directory.
    Init,

    /// Runs the proxy in the foreground (best for Docker or systemd).
    Run {
        /// Path to the configuration file.
        #[arg(short, long, default_value = "iron-proxy.toml")]
        config: String,
    },
    Start {
        /// Path to the configuration file.
        #[arg(short, long, default_value = "iron-proxy.toml")]
        config: String,
    },

    /// Gracefully stops the background daemon process via SIGTERM.
    Stop,

    /// Queries the Admin API for real-time backend health and cluster status.
    Status {
        /// The base URL of the Admin API.
        #[arg(long, default_value = "http://127.0.0.1:9090")]
        admin_url: String,
    },

    /// Validates the TOML syntax and schema without opening any ports.
    Check {
        /// Path to the configuration file.
        #[arg(short, long, default_value = "iron-proxy.toml")]
        config: String,
    },
}

/// The default TOML configuration string written to disk by the `init` command.
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
