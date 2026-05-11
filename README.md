# 🛡️ Iron-Proxy

![Build Status](https://img.shields.io/github/actions/workflow/status/thecoderbee/iron-proxy/release.yml?branch=main)
![Version](https://img.shields.io/github/v/tag/thecoderbee/iron-proxy?label=version)
![Language](https://img.shields.io/badge/Language-Rust-f5a34e)
![License](https://img.shields.io/badge/License-MIT-blue)

A high-performance, concurrent, dual-engine (L4/L7) reverse proxy and load balancer engineered in Rust. 

Iron-Proxy is designed for modern enterprise infrastructure, featuring mathematical latency-aware routing, automatic request resilience, and zero-downtime operations. Built on top of `tokio` and `hyper`, it safely multiplexes raw TCP streams alongside HTTP traffic without blocking the main event loop.

## ✨ Enterprise Features

* **Dual-Engine Multiplexing:** Run raw Layer 4 (TCP) and Layer 7 (HTTP) proxies side-by-side from a single binary.
* **Advanced Load Balancing:** Lock-free, highly concurrent routing using **Peak EWMA** (Exponentially Weighted Moving Average) for HTTP, **Least Connections** for TCP, and **IP Hashing** for stateful Sticky Sessions.
* **Zero-Downtime Hot Reloading:** Modify your `iron-proxy.toml` on the fly. The proxy watches for file system events and safely swaps configuration states without dropping active client connections.
* **Self-Healing Resilience:** Features asynchronous background health checks and automatic L7 request retries with in-memory body buffering for 5xx backend errors.
* **First-Class Observability:** Native Prometheus `/metrics` endpoint and strict, structured JSON telemetry for seamless Datadog/Grafana integration.
* **Unix Daemonization:** Production-ready CLI with native background process management.

---

## ⚙️ Architecture & Internals

Iron-Proxy is architected for **maximum throughput** and **minimal tail latency**.

---

### 🔒 Lock-Free State Management

The internal `ConnectionTracker` uses:

- `DashMap` for concurrent shared state
- Raw 64-bit atomic floating-point math via `AtomicU64` bit-casting

This enables latency metrics to be updated in **sub-microsecond time** without traditional `Mutex` thread locking.

```text
Atomic updates → zero lock contention → lower tail latency
```

## 💻 Command Line Interface

| Command | Description |
|---------|-------------|
| `init` | Generates a standard `iron-proxy.toml` template |
| `start` | Forks the process and runs the proxy in daemon mode |
| `stop` | Gracefully stops the daemon via `SIGTERM` |
| `status` | Queries the Admin API for real-time backend health |
| `check` | Validates TOML syntax without opening ports |
| `run` | Runs the proxy in the foreground (Docker/systemd friendly) |

---


## 🚀 Quick Start

### Installation
Pre-compiled binaries for Linux, macOS (Apple Silicon/Intel), and Windows are available.

1. Download the latest binary from the [Releases](../../releases) tab.
2. Extract the executable and add it to your system `$PATH`.

*(Alternatively, build from source: `cargo install --path .`)*

### Running the Proxy

Generate the default configuration file in your current directory:
```bash
iron-proxy init
```

Start the proxy in the background (Daemon mode):
```bash
iron-proxy start
```
Check the real-time cluster status:
```bash
iron-proxy status
```
