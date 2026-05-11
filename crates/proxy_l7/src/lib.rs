mod handler;

use config::ProxyConfig;
use health::HealthRegistry;
use router::ConnectionTracker;

use arc_swap::ArcSwap;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use metrics::gauge;
use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};
use rate_limit::RateLimiter;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tracing::{error, info};

use std::fs::File;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;

pub type ProxyBody = BoxBody<Bytes, hyper::Error>;

// a type alias for our http client to keep code clean
type HttpClient = Client<HttpConnector, ProxyBody>;

// helper to create a boxed empty body
// we will use it later
#[allow(dead_code)]
fn empty_body() -> BoxBody<Bytes, hyper::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

// helper to create a boxed body with text
fn text_body(text: &'static str) -> BoxBody<Bytes, hyper::Error> {
    Full::new(Bytes::from(text))
        .map_err(|never| match never {})
        .boxed()
}

// TLS helper function
fn load_certs(path: &str) -> std::io::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader).collect::<std::io::Result<Vec<_>>>()
}

fn load_private_key(path: &str) -> std::io::Result<PrivateKeyDer<'static>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)?.ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "No private key found in file",
        )
    })
}

// Listen for standard OS termination signals (Ctrl+C or SIGTERM)
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

pub struct L7Proxy {
    pub(crate) config: Arc<ArcSwap<ProxyConfig>>,
    pub(crate) registry: HealthRegistry,
    pub(crate) tracker: ConnectionTracker,
    pub(crate) client: HttpClient,
    pub(crate) rate_limiter: Option<RateLimiter>,
}

impl L7Proxy {
    pub fn new(config: ProxyConfig, registry: HealthRegistry, tracker: ConnectionTracker) -> Self {
        // initialize the connection-pooling HTTP client
        let client = Client::builder(TokioExecutor::new()).build_http();

        // check if rate limiting is enabled in cofig
        let rate_limiter = config.rate_limit.as_ref().map(|rl| {
            info!(
                "Rate Limiting enabled: {} bursts, {}/sec refill",
                rl.capacity, rl.refill_rate
            );
            RateLimiter::new(rl.capacity, rl.refill_rate)
        });
        Self {
            config: Arc::new(ArcSwap::from_pointee(config)),
            registry,
            tracker,
            client,
            rate_limiter,
        }
    }

    async fn get_next_backend(&self, client_ip: IpAddr) -> Option<SocketAddr> {
        let healthy_backends = self.registry.get_healthy_backends().await;

        // load config to see which targets actually belong to l7
        let cfg = self.config.load();
        let cluster = cfg.clusters.first()?;
        let http_targets = &cluster.targets;
        let is_sticky = cluster.sticky_sessions;

        let mut available = Vec::new();
        for addr in healthy_backends {
            if http_targets.contains(&addr.to_string()) {
                available.push(addr);
            }
        }

        if available.is_empty() {
            return None;
        }

        if is_sticky {
            self.tracker.get_sticky_backend(client_ip, &available)
        } else {
            self.tracker.get_best_l7(&available)
        }
    }

    pub async fn run(self: Arc<Self>) -> std::io::Result<()> {
        let cfg = self.config.load();
        let bind_addr = format!("{}:{}", cfg.server.bind_addr, cfg.server.port);
        let listener = TcpListener::bind(&bind_addr).await?;

        // TLS config setup
        let tls_acceptor = if let Some(tls_config) = &cfg.server.tls {
            info!("Loading TLS certificates from {}", tls_config.cert_path);
            // we explicitly install the ring crypto provider for the process
            let _ = rustls::crypto::ring::default_provider().install_default();

            let certs = load_certs(&tls_config.cert_path)?;
            let key = load_private_key(&tls_config.key_path)?;

            let mut server_config = rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

            // enable ALPN to negotiate HTTP/1.1
            server_config.alpn_protocols = vec![b"http/1.1".to_vec()];

            info!("TLS configured successfully!");
            Some(tokio_rustls::TlsAcceptor::from(Arc::new(server_config)))
        } else {
            None
        };

        info!(
            "L7 (HTTP) Proxy listening on {} (TLS: {})",
            bind_addr,
            tls_acceptor.is_some()
        );

        let mut active_connections = JoinSet::new();

        loop {
            tokio::select! {
                // event 1: a new client connects
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, client_addr)) => {
                            gauge!("iron_proxy_active_connections").increment(1.0);

                            let proxy = self.clone();
                            let tls_acceptor_clone = tls_acceptor.clone();

                            active_connections.spawn(async move {
                                // fetch healthy backends asynchronously inside tha task
                                let proxy_for_req = proxy.clone();

                                let service = service_fn(move |req| {
                                    handler::handle_request(
                                        req,
                                        client_addr,
                                        proxy_for_req.clone(),
                                    )
                                });

                                // branch execution based on whether TLS in enabled
                                if let Some(acceptor) = tls_acceptor_clone {
                                    if let Ok(tls_stream) = acceptor.accept(stream).await {
                                        let io = TokioIo::new(tls_stream);
                                        let _ = http1::Builder::new().serve_connection(io, service).await;
                                    }
                                } else {
                                    let io = TokioIo::new(stream);
                                    let _ = http1::Builder::new().serve_connection(io, service).await;
                                }

                                gauge!("iron_proxy_active_connections").decrement(1.0);
                            });
                        }
                        Err(e) => error!("Failed to accept connection: {}", e),
                    }
                }

                // event 2: SIGTERM received
                _ = shutdown_signal() => {
                    info!("Shutdown signal received! Stopping L7 traffic intake.");
                    break;
                }
            }
        }
        info!(
            "Waiting for {} active connections to drain...",
            active_connections.len()
        );
        while active_connections.join_next().await.is_some() {}
        info!("All connections cleanly drained. Iron-Proxy shutting down.");

        Ok(())
    }

    pub fn watch_config(self: Arc<Self>, config_path: String) {
        tokio::spawn(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

            let mut watcher = RecommendedWatcher::new(
                move |res| {
                    let _ = tx.send(res);
                },
                Config::default(),
            )
            .unwrap();

            watcher
                .watch(Path::new(&config_path), RecursiveMode::NonRecursive)
                .unwrap();
            info!("Watching {} for hot-reloads...", config_path);

            while let Some(res) = rx.recv().await {
                match res {
                    Ok(event) => {
                        if event.kind.is_modify() {
                            info!("Config modification detected! Reloading...");
                            match ::config::load_config(&config_path) {
                                Ok(new_cfg) => {
                                    self.config.store(Arc::new(new_cfg));
                                    info!("Hot-reload successful! New routing rules applied.");
                                }
                                Err(e) => {
                                    error!("Failed to hot-reload config: {}. Keeping old rules.", e)
                                }
                            }
                        }
                    }
                    Err(e) => error!("Watch error: {:?}", e),
                }
            }
        });
    }
}
