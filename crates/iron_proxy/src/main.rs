use clap::{Parser, Subcommand};
use std::{process, time::Duration};
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
    // boot json logging engine
    observability::init_telemetry();

    // parse CLI args
    let cli = Cli::parse();

    match &cli.command {
        Commands::Run { config } => {
            info!("Starting Iron-Proxy...");
            match config::load_config(config) {
                Ok(cfg) => {
                    // extract backend targets from config
                    let targets = cfg
                        .clusters
                        .first()
                        .map(|c| c.targets.clone())
                        .unwrap_or_default();

                    let mut backends = Vec::new();
                    for target in targets {
                        if let Ok(addr) = target.parse::<std::net::SocketAddr>() {
                            backends.push(addr);
                        }
                    }

                    // initialize health registry
                    let registry = health::HealthRegistry::new(&backends);

                    health::start_health_check_loop(registry.clone(), Duration::from_secs(5));

                    info!("Initializing L7 HTTP Engine with active health checks...");

                    // pass the registry to the new L7 Proxy
                    let proxy = std::sync::Arc::new(proxy_l7::L7Proxy::new(cfg, registry));

                    if let Err(e) = proxy.run().await {
                        error!("Proxy failed: {}", e);
                        std::process::exit(1);
                    }
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
