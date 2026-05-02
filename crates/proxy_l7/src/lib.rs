use config::ProxyConfig;
use http_body_util::{BodyExt, Empty, Full, combinators::BoxBody};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tracing::{error, info};

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

pub struct L7Proxy {
    config: Arc<ProxyConfig>,
    backends: Vec<SocketAddr>,
    current_backend: AtomicUsize,
    client: HttpClient,
}

impl L7Proxy {
    pub fn new(config: ProxyConfig) -> Self {
        let targets = config
            .clusters
            .first()
            .map(|c| c.targets.clone())
            .unwrap_or_default();

        let mut backends = Vec::new();
        for target in targets {
            match target.parse::<SocketAddr>() {
                Ok(addr) => backends.push(addr),
                Err(e) => error!("Failed to parse backend address '{}':{}", target, e),
            }
        }

        // initialize the connection-pooling HTTP client
        let client = Client::builder(TokioExecutor::new()).build_http();

        Self {
            config: Arc::new(config),
            backends,
            current_backend: AtomicUsize::new(0),
            client,
        }
    }

    fn get_next_backend(&self) -> SocketAddr {
        if self.backends.is_empty() {
            panic!("Attempted to route traffic with an empty backend pool");
        }

        let idx = self.current_backend.fetch_add(1, Ordering::Relaxed);
        self.backends[idx % self.backends.len()]
    }

    // this is our core http handler. right now it just returms a 502.
    // later, this will use a hyper::Client to forward the request.
    async fn handle_request(
        mut req: Request<hyper::body::Incoming>,
        client_addr: SocketAddr,
        backend_addr: SocketAddr,
        client: HttpClient,
    ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, Infallible> {
        info!("Routing {} {} to {}", req.method(), req.uri(), backend_addr);

        let headers = req.headers_mut();

        // remove headers explicitly listed in the 'Connection' header
        if let Some(conn_header) = headers.get(hyper::header::CONNECTION).cloned() {
            if let Ok(conn_str) = conn_header.to_str() {
                for h in conn_str.split(',') {
                    headers.remove(h.trim());
                }
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
                if let Some(conn_header) = res_headers.get(hyper::header::CONNECTION).cloned() {
                    if let Ok(conn_str) = conn_header.to_str() {
                        for h in conn_str.split(',') {
                            res_headers.remove(h.trim());
                        }
                    }
                }
                for header in &hop_by_hop {
                    res_headers.remove(header);
                }

                Ok(response.map(|body| body.boxed()))
            }
            Err(e) => {
                error!("Failed to forward request to {}: {}", backend_addr, e);

                let mut err_response = Response::new(text_body("Iron-Proxy: 502 Bad Gateway"));
                *err_response.status_mut() = StatusCode::BAD_GATEWAY;
                Ok(err_response)
            }
        }
    }

    pub async fn run(&self) -> std::io::Result<()> {
        let bind_addr = format!(
            "{}:{}",
            self.config.server.bind_addr, self.config.server.port
        );
        let listener = TcpListener::bind(&bind_addr).await?;

        info!("L7 (HTTP) Proxy listening on {}", bind_addr);

        let mut active_connections = JoinSet::new();

        loop {
            match listener.accept().await {
                Ok((stream, client_addr)) => {
                    if self.backends.is_empty() {
                        error!(
                            "Dropping connection from {}: no backends available",
                            client_addr
                        );
                        continue;
                    }

                    let backend_addr = self.get_next_backend();
                    let client = self.client.clone();

                    // hyper 1.0 requires wrapping the tokio stream in TokioIO
                    let io = TokioIo::new(stream);

                    active_connections.spawn(async move {
                        let service = service_fn(move |req| {
                            Self::handle_request(req, client_addr, backend_addr, client.clone())
                        });

                        if let Err(err) = http1::Builder::new().serve_connection(io, service).await
                        {
                            error!("Error serving connection: {:?}", err);
                        }
                    });
                }
                Err(e) => error!("Failed to accept connection: {}", e),
            }
        }
    }
}
