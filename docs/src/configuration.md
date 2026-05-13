# Configuration Reference

Iron-Proxy uses a strictly typed TOML configuration schema (`iron-proxy.toml`). The configuration is **hot-reloadable**; saving changes to this file on disk will seamlessly update the routing rules without dropping active TCP or HTTP connections.

## Global Settings

```toml
[admin]
bind_addr = "127.0.0.1"
port = 9090

[rate_limit]
capacity = 1000.0      # Maximum burst allowance per IP
refill_rate = 50.0     # Tokens regenerated per second
```

## Layer & (HTTP) Clusters

The `[[clusters]]` array defines HTTP/HTTPS reverse proxy targets.

```toml
[[clusters]]
name = "api_gateway"
mode = "http"
sticky_sessions = false
max_retries = 3 
targets = [
    "10.0.0.1:8080",
    "10.0.0.2:8080"
]
```

- `max-retries`: Enables in-memory body buffering to automatically retry requests against healthy nodes if a target returns a 5xx error.

- `sticky-session`: Override Peak EWMA routing to deterministically pin client IPs to specific backend nodes.

## Layer 4 (TCP) Servers

The `[[tcp_servers]]` array defines raw byte-streaming proxies.

```toml
[[tcp_servers]]
name = "redis_cluster"
bind_addr = "127.0.0.1"
port = 6379
targets = [
    "10.0.1.1:6379",
    "10.0.1.2:6379"
]
```
