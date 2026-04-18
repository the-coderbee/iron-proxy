# Iron Proxy 🛡️

A high-performance, asynchronous API Gateway and Load Balancer written purely in Rust. 

Iron Proxy sits in front of your backend services, absorbing traffic spikes, routing requests, and actively monitoring the health of your infrastructure.

## 🚀 Architecture & Features

* **Massive Concurrency:** Built on the `tokio` asynchronous runtime to handle thousands of concurrent connections with a minimal memory footprint.
* **Zero-Copy Streaming:** Utilizes bidirectional stream copying (`io::copy_bidirectional`) to pipe TCP traffic directly from client to backend without buffering entire requests in memory.
* **Active Health Checking:** A background daemon continuously monitors backend nodes. Dead nodes are automatically removed from the Round-Robin rotation and re-added once they recover, ensuring zero downtime for users.
* **Thread-Safe Shared State:** Employs `Arc` and `Mutex` with optimized lock-dropping (the "snapshot" pattern) to prevent data races and deadlocks across concurrent tasks.
* **Layer-4 Rate Limiting:** Built-in IP-based rate limiter using concurrent HashMaps to drop abusive traffic before it ever reaches the backend pool.
* **Dynamic Configuration:** Reads targets and thresholds from a `gateway_config.toml` file at runtime.

## 🛠️ Usage

**1. Clone and Build**
Compile the optimized release binary:
```bash
git clone [https://github.com/yourusername/iron-proxy.git](https://github.com/yourusername/iron-proxy.git)
cd iron-proxy
cargo build --release