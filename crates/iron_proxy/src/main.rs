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

                    // start background health checker
                    health::start_health_check_loop(registry.clone(), Duration::from_secs(5));

                    // start Admin API Control Plane
                    if let Some(admin_cfg) = cfg.admin.clone() {
                        let admin_registry = registry.clone();
                        tokio::spawn(async move {
                            admin::start_admin_server(admin_cfg, admin_registry).await;
                        });
                    } else {
                        info!("No [admin] block found in config. Control Plane disabled.");
                    }

                    info!("Initializing L7 HTTP Engine with active health checks...");

                    // Start the Data Plane
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

        Commands::Status { admin_url } => {
            let url = format!("{}/api/v1/health", admin_url);
            println!("Fetching cluster status from {}", url);

            match reqwest::get(&url).await {
                Ok(response) => {
                    if response.status().is_success() {
                        let text = response.text().await.unwrap_or_default();

                        // parse the json into a generic value for easy reading
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                            let status = json["status"].as_str().unwrap_or("Unknown");

                            println!("CLUSTER STATUS: {}", status);
                            println!("-----------------------------------------------");

                            if let Some(backends) = json["backends"].as_object() {
                                for (addr, state) in backends {
                                    let state_str = state.as_str().unwrap_or("Unknown");
                                    let icon = if state_str == "Healthy" { "✅" } else { "❌" };
                                    println!("{} {} ({})", icon, addr, state_str);
                                }
                            }
                            println!("-----------------------------------------------");
                        } else {
                            error!("Failed to parse response from Admin API.");
                        }
                    } else {
                        error!("Admin API returned an error: {}", response.status());
                    }
                }
                Err(_) => {
                    error!("Failed to connect to Admin API");
                    error!(
                        "Is Iron-Proxy running, and is the [admin] block configured in your TOML?"
                    );
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
