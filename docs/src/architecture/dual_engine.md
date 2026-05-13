# The Dual-Engine Design

Iron-Proxy operates on a dual-engine architecture, completely isolating Layer 4 (Transport) and Layer 7 (Application) logic. Both engines share a highly concurrent, lock-free global state (via `DashMap`) for routing decisions, but they handle network I/O fundamentally differently.

## Layer 4: Raw TCP Streaming
The L4 engine acts as a blind byte-shoveler. 
* It intercepts the incoming TCP connection.
* Selects the upstream backend with the lowest active connection count.
* Establishes an outbound TCP socket.
* Uses `tokio::io::copy_bidirectional` to achieve maximum throughput with near-zero memory overhead.

## Layer 7: HTTP/HTTPS Proxy
The L7 engine is protocol-aware and computationally heavier.
* Manages TLS termination via `rustls`.
* Parses HTTP headers and strips hop-by-hop metadata.
* Implements token-bucket rate limiting per IP address.
* Buffers request bodies in memory to facilitate zero-downtime automated retries on backend failure.
