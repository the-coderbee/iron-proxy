use clap::{Parser, Subcommand};
use std::process;
use tracing::{error, info};

#[derive(Parser)]
#[command(author, version, about = "Iron-Proxy: High-performance load balancer")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    // Run the proxy server
    Run {
        // Path to the configuration file
        #[arg(short, long, default_value = "iron-proxy.toml")]
        config: String,
    },
    // Validate the configuration file
    Check {
        #[arg(short, long, default_value = "iron-proxy.toml")]
        config: String,
    },
}

#[tokio::main]
async fn main() {
    observability::init_tracing();

    let cli = Cli::parse();

    match &cli.command {
        Commands::Run { config } => {
            info!("Starting Iron-Proxy...");
            match config::load_config(config) {
                Ok(cfg) => {
                    info!(
                        "Loaded config: Binding to {}:{}",
                        cfg.server.bind_addr, cfg.server.port
                    );
                    // TODO: Initialize TCP listener
                }
                Err(e) => {
                    error!("Failed to load config: {}", e);
                    process::exit(1);
                }
            }
        }

        Commands::Check { config } => {
            info!("Checking configuration at {}", config);
            match config::load_config(config) {
                Ok(_) => {
                    info!("Configuration is valid");
                    process::exit(0);
                }
                Err(e) => {
                    error!("Configuration error: {}", e);
                    process::exit(1);
                }
            }
        }
    }
}
