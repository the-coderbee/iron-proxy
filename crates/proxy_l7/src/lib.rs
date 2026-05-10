use arc_swap::ArcSwap;
use config::ProxyConfig;
use health::{HealthRegistry, HealthStatus};
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::{Bytes, Incoming};
use hyper::header::HeaderValue;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use metrics::{counter, gauge, histogram};
use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};
use rate_limit::RateLimiter;
use router::ConnectionTracker;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::fs::File;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

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
    config: Arc<ArcSwap<ProxyConfig>>,
    registry: HealthRegistry,
    tracker: ConnectionTracker,
    client: HttpClient,
    rate_limiter: Option<RateLimiter>,
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

    // this is our core http handler. right now it just returms a 502.
    // later, this will use a hyper::Client to forward the request.
    async fn handle_request(
        req: Request<Incoming>,
        client_addr: SocketAddr,
        proxy: Arc<Self>,
    ) -> Result<Response<ProxyBody>, hyper::Error> {
        let start_time = Instant::now();
        let request_id = uuid::Uuid::new_v4().to_string();
        let req_method = req.method().clone();
        let req_path = req.uri().path().to_string();
        let client_ip = client_addr.ip().to_string();

        counter!("iron_proxy_requests_total", "method" => req_method.to_string(), "path" => req_path.clone()).increment(1);
        info!(
            request_id = %request_id,
            method = %req_method,
            path = %req_path,
            client_ip = %client_ip,
            "Incoming request"
        );

        // rate limiting shield
        if let Some(rl) = &proxy.rate_limiter
            && !rl.check(client_addr.ip()).await
        {
            warn!(
                request_id = %request_id,
                client_ip = %client_addr.ip(),
                "Rate Limit Exceeded! Dropping request."
            );
            counter!("iron_proxy_response_total", "status" => "429").increment(1);

            let mut error_response = Response::new(text_body("Iron-Proxy: 429 Too Many Requests"));
            *error_response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
            return Ok(error_response);
        }

        // body buffering
        let cfg = proxy.config.load();
        let max_retries = cfg.clusters.first().map(|c| c.max_retries).unwrap_or(0);

        let (mut parts, body) = req.into_parts();
        let body_bytes = body
            .collect()
            .await
            .map(|b| b.to_bytes())
            .unwrap_or_default();

        // inject proxy headers into `parts` once
        if let Some(existing) = parts.headers.get("x-forwarded-for") {
            if let Ok(existing_str) = existing.to_str() {
                let new_xff = format!("{}, {}", existing_str, client_ip);
                parts
                    .headers
                    .insert("x-forwarded-for", HeaderValue::from_str(&new_xff).unwrap());
            }
        } else {
            parts.headers.insert(
                "x-forwarded-for",
                HeaderValue::from_str(&client_ip).unwrap(),
            );
        }
        parts
            .headers
            .insert("x-forwarded-proto", HeaderValue::from_static("https"));
        parts
            .headers
            .insert("x-request-id", HeaderValue::from_str(&request_id).unwrap());

        // remove standard hop-by-hop headers
        let hop_by_hop = [
            hyper::header::CONNECTION,
            hyper::header::TE,
            hyper::header::TRAILER,
            hyper::header::TRANSFER_ENCODING,
            hyper::header::UPGRADE,
            hyper::header::HeaderName::from_static("keep-alive"),
            hyper::header::HeaderName::from_static("proxy-authenticate"),
            hyper::header::HeaderName::from_static("proxy-authorization"),
        ];

        if let Some(conn_header) = parts.headers.get(hyper::header::CONNECTION).cloned()
            && let Ok(conn_str) = conn_header.to_str()
        {
            for h in conn_str.split(',') {
                parts.headers.remove(h.trim());
            }
        }

        for header in &hop_by_hop {
            parts.headers.remove(header);
        }

        let mut attempts = 0;

        loop {
            let backend_addr_opt = proxy.get_next_backend(client_addr.ip()).await;

            let backend_addr = match backend_addr_opt {
                Some(addr) => addr,
                None => {
                    error!(
                        request_id = %request_id,
                        client_ip = %client_ip,
                        "No healthy backends available"
                    );
                    counter!("iron_proxy_response_total", "status" => "503").increment(1);
                    let mut error_response =
                        Response::new(text_body("Iron-Proxy: 503 Service Unavailable"));
                    *error_response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                    return Ok(error_response);
                }
            };

            proxy.tracker.inc(backend_addr);

            info!(
                request_id = %request_id,
                method = %req_method,
                path = %req_path,
                target = %backend_addr,
                attempt = attempts + 1,
                "Routing request"
            );

            let path_and_query = parts
                .uri
                .path_and_query()
                .map(|pq| pq.as_str())
                .unwrap_or("/");

            let new_uri = format!("http://{}{}", backend_addr, path_and_query);

            let mut attempt_req = Request::builder()
                .method(parts.method.clone())
                .uri(new_uri)
                .version(parts.version);

            for (k, v) in parts.headers.iter() {
                attempt_req = attempt_req.header(k, v);
            }

            attempt_req = attempt_req.header(hyper::header::HOST, backend_addr.to_string());

            let attempt_body = http_body_util::Full::new(body_bytes.clone())
                .map_err(|never| match never {})
                .boxed();

            let final_req = attempt_req.body(attempt_body).unwrap();

            // forward to backend
            match proxy.client.request(final_req).await {
                Ok(mut response) => {
                    if response.status().is_server_error() && attempts < max_retries {
                        warn!(
                            request_id = %request_id,
                            backend = %backend_addr,
                            status = response.status().as_u16(),
                            attempt = attempts + 1,
                            max_retries = max_retries,
                            "Backend returned error, retrying..."
                        );
                        attempts += 1;
                        continue;
                    }

                    let latency = start_time.elapsed();
                    let latency_ms = latency.as_secs_f64() * 1000.0;

                    proxy.tracker.dec_and_update_ewma(backend_addr, latency_ms);

                    histogram!("iron_proxy_request_duration_seconds").record(latency.as_secs_f64());
                    counter!("iron_proxy_responses_total", "status" => response.status().as_u16().to_string()).increment(1);
                    if response.status().is_server_error() {
                        info!(
                            request_id = %request_id,
                            method = %req_method,
                            path = %req_path,
                            status = response.status().as_u16(),
                            latency =latency.as_millis(),
                            backend = %backend_addr,
                            client_ip = %client_addr.ip(),
                            "Exhausted retries, returning error"
                        );
                    } else {
                        info!(
                            request_id = %request_id,
                            method = %req_method,
                            path = %req_path,
                            status = response.status().as_u16(),
                            latency =latency.as_millis(),
                            backend = %backend_addr,
                            client_ip = %client_addr.ip(),
                            "Request successful."
                        )
                    }

                    // strip hop-by-hop headers
                    // we also clean response before sending it to client
                    let res_headers = response.headers_mut();

                    res_headers.insert(
                        "x-request-id",
                        hyper::header::HeaderValue::from_str(&request_id).unwrap(),
                    );
                    if let Some(conn_header) = res_headers.get(hyper::header::CONNECTION).cloned()
                        && let Ok(conn_str) = conn_header.to_str()
                    {
                        for h in conn_str.split(',') {
                            res_headers.remove(h.trim());
                        }
                    }
                    for header in &hop_by_hop {
                        res_headers.remove(header);
                    }

                    return Ok(response.map(|body| body.boxed()));
                }
                Err(e) => {
                    if attempts < max_retries {
                        warn!(
                            request_id = %request_id,
                            backend = %backend_addr,
                            error = %e,
                            attempt = attempts + 1,
                            max_retries = max_retries,
                            "Backend network failed, retrying..."
                        );
                        attempts += 1;
                        continue;
                    }
                    proxy.tracker.dec(backend_addr);
                    warn!(
                        request_id = %request_id,
                        backend = %backend_addr,
                        "Passive check tripped! Marking backend as DEAD"
                    );
                    proxy
                        .registry
                        .set_status(backend_addr, HealthStatus::Dead)
                        .await;

                    error!(
                        request_id = %request_id,
                        backend = %backend_addr,
                        "Request failed after {} atempts", attempts + 1
                    );

                    counter!("iron_proxy_responses_total", "status" => "502").increment(1);
                    let mut err_response = Response::new(text_body("Iron-Proxy: 502 Bad Gateway"));
                    *err_response.status_mut() = StatusCode::BAD_GATEWAY;
                    return Ok(err_response);
                }
            }
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
                                    Self::handle_request(
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
