use clap::{Parser, Subcommand};
use health::HealthRegistry;
use proxy_l4::L4Proxy;
use proxy_l7::L7Proxy;
use router::ConnectionTracker;
use std::net::SocketAddr;
use std::path::Path;
use std::{process, sync::Arc, time::Duration};
use tracing::{error, info};

#[derive(Parser)]
#[command(author, version, about = "Iron-Proxy: High-performance load balancer")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
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

const DEFAULT_CONFIG: &str = r#"# Iron-Proxy Enterprise Configuration
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

fn main() {
    // parse CLI args
    let cli = Cli::parse();

    match &cli.command {
        Commands::Init => {
            let path = Path::new("iron-proxy.toml");
            if path.exists() {
                error!("A file named 'iron-proxy.toml' already exists in this directory.");
                error!("Refusing to overwrite. Please rename or delete it first.");
                process::exit(1);
            }

            match std::fs::write(path, DEFAULT_CONFIG) {
                Ok(_) => {
                    info!("Successfully generated default configuration at ./iron-proxy.toml");
                    info!("Run 'cargo run -- run' to start the proxy!");
                }
                Err(e) => {
                    error!("Failed to write configuration file: {}", e);
                    process::exit(1);
                }
            }
            return;
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
        Commands::Stop => {
            let pid_file = "iron-proxy.pid";
            if let Ok(pid_str) = std::fs::read_to_string(pid_file) {
                let pid = pid_str.trim();
                println!("Sending graceful shutdown signal to PID {}...", pid);

                let status = std::process::Command::new("kill")
                    .args(["-15", pid]) // -15 is SIGTERM
                    .status();

                if status.is_ok() && status.unwrap().success() {
                    println!("Iron-Proxy background process stopped.");
                    let _ = std::fs::remove_file(pid_file);
                } else {
                    eprintln!("Failed to stop process. Is it still running?");
                }
            } else {
                eprintln!("No iron-proxy.pid file found. Is the daemon running?");
            }
            return;
        }
        Commands::Status { admin_url } => {
            let url = format!("{}/api/v1/health", admin_url);
            println!("Fetching cluster status from {}", url);

            // create a temporary single threaded runtime just to run this one HTTP request
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
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
                            eprintln!("Failed to parse response from Admin API.");
                        }
                    } else {
                        eprintln!("Admin API returned an error: {}", response.status());
                    }
                }
                Err(_) => {
                    eprintln!("Failed to connect to Admin API");
                    eprintln!(
                        "Is Iron-Proxy running, and is the [admin] block configured in your TOML?"
                    );
                }
            }
            });
            return;
        }
        _ => {}
    }

    // handle daemonization

    let config_path = match &cli.command {
        Commands::Run { config } => config.clone(),
        Commands::Start { config } => {
            use daemonize::Daemonize;
            use std::fs::File;

            let stdout = File::create("iron-proxy.out").unwrap();
            let stderr = File::create("iron-proxy.err").unwrap();

            let daemonize = Daemonize::new()
                .pid_file("iron-proxy.pid")
                .working_directory(".")
                .stdout(stdout)
                .stderr(stderr);

            match daemonize.start() {
                Ok(_) => config.clone(),
                Err(e) => {
                    eprintln!("Error starting daemon: {}", e);
                    process::exit(1);
                }
            }
        }
        _ => unreachable!(),
    };

    observability::init_telemetry();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        info!("Starting Iron-Proxy...");
        match config::load_config(&config_path) {
            Ok(cfg) => {
                let mut backends = Vec::new();
                let mut http_addrs = Vec::new();

                let http_targets = cfg
                    .clusters
                    .first()
                    .map(|c| c.targets.clone())
                    .unwrap_or_default();
                for target in http_targets {
                    if let Ok(addr) = target.parse::<SocketAddr>() {
                        backends.push(addr);
                        http_addrs.push(addr);
                    }
                }

                for tcp_server in &cfg.tcp_servers {
                    for target in &tcp_server.targets {
                        if let Ok(addr) = target.parse::<SocketAddr>() {
                            backends.push(addr);
                        }
                    }
                }

                info!("Initializing shared state...");
                let registry = HealthRegistry::new(&backends);
                let tracker = ConnectionTracker::new();

                health::start_health_check_loop(
                    registry.clone(),
                    Duration::from_secs(5),
                    http_addrs,
                );

                if let Some(admin_cfg) = cfg.admin.clone() {
                    let admin_registy = registry.clone();
                    tokio::spawn(async move {
                        admin::start_admin_server(admin_cfg, admin_registy).await;
                    });
                } else {
                    info!("No [admin] block found in config. Control plane disabled.");
                }

                for tcp_server in &cfg.tcp_servers {
                    let config_clone = tcp_server.clone();
                    let log_addr = config_clone.bind_addr.clone();
                    let l4_proxy = Arc::new(L4Proxy::new(
                        config_clone,
                        registry.clone(),
                        tracker.clone(),
                    ));

                    tokio::spawn(async move {
                        if let Err(e) = l4_proxy.run().await {
                            error!("L4Proxy failed for {}: {}", log_addr, e);
                        }
                    });
                }

                info!("Initializing L7 HTTP Engine...");
                let l7_proxy = Arc::new(L7Proxy::new(cfg, registry, tracker.clone()));
                l7_proxy.clone().watch_config(config_path.clone());

                if let Err(e) = l7_proxy.run().await {
                    error!("L7Proxy failed: {}", e);
                    process::exit(1);
                }
            }
            Err(e) => {
                error!("Failed to load config: {}", e);
                process::exit(1);
            }
        }
    });
}
