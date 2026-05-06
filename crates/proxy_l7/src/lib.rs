use config::ProxyConfig;
use health::HealthRegistry;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::convert::Infallible;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

// a type alias for our http client to keep code clean
type HttpClient = Client<HttpConnector, Incoming>;

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

pub struct L7Proxy {
    config: Arc<ProxyConfig>,
    registry: HealthRegistry,
    current_backend: AtomicUsize,
    client: HttpClient,
}

impl L7Proxy {
    pub fn new(config: ProxyConfig, registry: HealthRegistry) -> Self {
        // initialize the connection-pooling HTTP client
        let client = Client::builder(TokioExecutor::new()).build_http();

        Self {
            config: Arc::new(config),
            registry,
            current_backend: AtomicUsize::new(0),
            client,
        }
    }

    async fn get_next_backend(&self) -> Option<SocketAddr> {
        let healthy_backends = self.registry.get_healthy_backends().await;
        if healthy_backends.is_empty() {
            return None;
        }
        let idx = self.current_backend.fetch_add(1, Ordering::Relaxed);
        Some(healthy_backends[idx % healthy_backends.len()])
    }

    // this is our core http handler. right now it just returms a 502.
    // later, this will use a hyper::Client to forward the request.
    async fn handle_request(
        mut req: Request<hyper::body::Incoming>,
        client_addr: SocketAddr,
        backend_addr_opt: Option<SocketAddr>,
        client: HttpClient,
        registry: HealthRegistry,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        // first check if any backends are available
        let backend_addr = match backend_addr_opt {
            Some(addr) => addr,
            None => {
                error!("No healthy backends available");
                let mut error_response =
                    Response::new(text_body("Iron-Proxy: 503 Service Unavailable"));
                *error_response.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
                return Ok(error_response);
            }
        };

        info!("Routing {} {} to {}", req.method(), req.uri(), backend_addr);

        let headers = req.headers_mut();

        // remove headers explicitly listed in the 'Connection' header
        if let Some(conn_header) = headers.get(hyper::header::CONNECTION).cloned()
            && let Ok(conn_str) = conn_header.to_str()
        {
            for h in conn_str.split(',') {
                headers.remove(h.trim());
            }
        }

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

        for header in &hop_by_hop {
            headers.remove(header);
        }

        // inject proxy headers
        let client_ip = client_addr.ip().to_string();

        // if there's already a proxy in front of us we append
        if let Some(existing) = headers.get("x-forwarded-for") {
            if let Ok(existing_str) = existing.to_str() {
                let new_xff = format!("{}, {}", existing_str, client_ip);
                headers.insert(
                    "x-forwarded-for",
                    hyper::header::HeaderValue::from_str(&new_xff).unwrap(),
                );
            }
        } else {
            headers.insert(
                "x-forwarded-for",
                hyper::header::HeaderValue::from_str(&client_ip).unwrap(),
            );
        }

        headers.insert(
            "x-forwarded-proto",
            hyper::header::HeaderValue::from_static("https"),
        );

        // rewrite uri and host
        let path_and_query = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");

        let new_uri = format!("http://{}{}", backend_addr, path_and_query);

        *req.uri_mut() = new_uri.parse::<Uri>().unwrap();

        // adjust the host header to match the backend
        req.headers_mut().insert(
            hyper::header::HOST,
            hyper::header::HeaderValue::from_str(&backend_addr.to_string()).unwrap(),
        );

        // forward to backend
        match client.request(req).await {
            Ok(mut response) => {
                info!(
                    "Backend {} responded with {}",
                    backend_addr,
                    response.status()
                );

                // strip hop-by-hop headers
                // we also clean response before sending it to client
                let res_headers = response.headers_mut();
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

                Ok(response.map(|body| body.boxed()))
            }
            Err(e) => {
                error!("Failed to forward request to {}: {}", backend_addr, e);

                // passive health check. instantly mark the server as dead
                warn!("Passive check tripped! Marking {} as DEAD", backend_addr);
                registry
                    .set_status(backend_addr, health::HealthStatus::Dead)
                    .await;

                let mut err_response = Response::new(text_body("Iron-Proxy: 502 Bad Gateway"));
                *err_response.status_mut() = StatusCode::BAD_GATEWAY;
                Ok(err_response)
            }
        }
    }

    pub async fn run(self: Arc<Self>) -> std::io::Result<()> {
        let bind_addr = format!(
            "{}:{}",
            self.config.server.bind_addr, self.config.server.port
        );
        let listener = TcpListener::bind(&bind_addr).await?;

        // TLS config setup
        let tls_acceptor = if let Some(tls_config) = &self.config.server.tls {
            info!("Loading TLS certificates from {}", tls_config.cert_path);
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
            match listener.accept().await {
                Ok((stream, client_addr)) => {
                    let client = self.client.clone();

                    let proxy = self.clone();
                    let registry_clone = self.registry.clone();
                    let tls_acceptor_clone = tls_acceptor.clone();

                    active_connections.spawn(async move {
                        // fetch healthy backends asynchronously inside tha task
                        let backend_addr_opt = proxy.get_next_backend().await;

                        let service = service_fn(move |req| {
                            Self::handle_request(
                                req,
                                client_addr,
                                backend_addr_opt,
                                client.clone(),
                                registry_clone.clone(),
                            )
                        });

                        // branch execution based on whether TLS in enabled
                        if let Some(acceptor) = tls_acceptor_clone {
                            // perform the TLS handshake
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    let io = TokioIo::new(tls_stream);
                                    if let Err(e) =
                                        http1::Builder::new().serve_connection(io, service).await
                                    {
                                        error!("Error serving TLS connection: {:?}", e);
                                    }
                                }
                                Err(e) => error!("TLS handshake failed for {}: {}", client_addr, e),
                            }
                        } else {
                            // standard plain http
                            // hyper 1.0 requires wrapping the tokio stream in TokioIO
                            let io = TokioIo::new(stream);
                            if let Err(e) =
                                http1::Builder::new().serve_connection(io, service).await
                            {
                                error!("Error serving TCP connection: {:?}", e);
                            }
                        }
                    });
                }
                Err(e) => error!("Failed to accept connection: {}", e),
            }
        }
    }
}
