mod cli;

#[cfg(unix)]
mod daemon;

use cli::{Cli, Commands, DEFAULT_CONFIG};
use proxy_l4::L4Proxy;
use proxy_l7::L7Proxy;

use clap::Parser;
use tracing::{error, info};

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::{path::Path, process};

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Init => {
            let path = Path::new("iron-proxy.toml");
            if path.exists() {
                error!("'iron-proxy.toml' already exists. Refusing to overwrite.");
                process::exit(1);
            }
            if let Err(e) = std::fs::write(path, DEFAULT_CONFIG) {
                error!("Failed to write configuration file: {}", e);
                process::exit(1);
            }
            info!("Successfully generated default configuration at ./iron-proxy.toml");
            return;
        }
        Commands::Check { config } => {
            if config::load_config(config).is_ok() {
                info!("Configuration is valid");
                process::exit(0);
            } else {
                error!("Configuration is invalid");
                process::exit(1);
            }
        }
        Commands::Stop => {
            #[cfg(unix)]
            {
                daemon::stop_background_process();
            }
            #[cfg(not(unix))]
            {
                eprintln!("Daemon mode is not supported natively on Windows.");
            }
            return;
        }
        Commands::Status { admin_url } => {
            let url = format!("{}/api/v1/health", admin_url);
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                match reqwest::get(&url).await {
                    Ok(response) if response.status().is_success() => {
                        let text = response.text().await.unwrap_or_default();
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                            println!(
                                "CLUSTER STATUS: {}",
                                json["status"].as_str().unwrap_or("Unknown")
                            );
                            println!("-----------------------------------------------");
                            if let Some(backends) = json["backends"].as_object() {
                                for (addr, state) in backends {
                                    let state_str = state.as_str().unwrap_or("Unknown");
                                    let icon = if state_str == "Healthy" { "✅" } else { "❌" };
                                    println!("{} {} ({})", icon, addr, state_str);
                                }
                            }
                            println!("-----------------------------------------------");
                        }
                    }
                    _ => eprintln!("Failed to connect to Admin API. Is Iron-Proxy running?"),
                }
            });
            return;
        }
        _ => {}
    }

    let config_path = match &cli.command {
        Commands::Run { config } => config.clone(),
        Commands::Start { config } => {
            #[cfg(unix)]
            {
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
            #[cfg(not(unix))]
            {
                eprintln!(
                    "Daemon mode ('start' and 'stop') is restricted to Unix systems (Linux/macOS)."
                );
                eprintln!(
                    "On Windows, please run Iron-Proxy in the foreground using: iron-proxy run"
                );
                process::exit(1);
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
        info!("Starting Iron-Proxy Engine...");
        let cfg = config::load_config(&config_path).expect("Failed to load config");

        let mut backends = Vec::new();
        let mut http_addrs = Vec::new();

        if let Some(cluster) = cfg.clusters.first() {
            for target in &cluster.targets {
                if let Ok(addr) = target.parse::<SocketAddr>() {
                    backends.push(addr);
                    http_addrs.push(addr);
                }
            }
        }

        for tcp_server in &cfg.tcp_servers {
            for target in &tcp_server.targets {
                if let Ok(addr) = target.parse::<SocketAddr>() {
                    backends.push(addr);
                }
            }
        }

        let registry = health::HealthRegistry::new(&backends);
        let tracker = router::ConnectionTracker::new();

        health::start_health_check_loop(registry.clone(), Duration::from_secs(5), http_addrs);

        if let Some(admin_cfg) = cfg.admin.clone() {
            let admin_registry = registry.clone();
            tokio::spawn(async move {
                admin::start_admin_server(admin_cfg, admin_registry).await;
            });
        }

        for tcp_server in &cfg.tcp_servers {
            let l4_proxy = Arc::new(L4Proxy::new(
                tcp_server.clone(),
                registry.clone(),
                tracker.clone(),
            ));
            tokio::spawn(async move {
                let _ = l4_proxy.run().await;
            });
        }

        let l7_proxy = Arc::new(L7Proxy::new(cfg, registry, tracker.clone()));
        l7_proxy.clone().watch_config(config_path);

        if let Err(e) = l7_proxy.run().await {
            error!("L7 Proxy encountered a fatal error: {}", e);
            process::exit(1);
        }
    });
}
