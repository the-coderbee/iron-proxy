# Quick Start

Getting Iron-Proxy running in your environment takes less than a minute. The proxy is distributed as a single, statically compiled binary with no external dependencies.

## Installation

Download the latest release for your operating system from the [GitHub Releases page](https://github.com/thecoderbee/iron-proxy/releases).

```bash
# Example for Linux x86_64
wget [https://github.com/thecoderbee/iron-proxy/releases/download/v4.0.3/iron-proxy-linux-amd64](https://github.com/thecoderbee/iron-proxy/releases/download/v4.0.3/iron-proxy-linux-amd64)
chmod +x iron-proxy-linux-amd64
sudo mv iron-proxy-linux-amd64 /usr/local/bin/iron-proxy
```

## Initialization

Generate the default configuration file in your current directory:
```bash
iron-proxy init
```

This will create an `iron-proxy.toml` file containing a standard Layer 4 and Layer 7 cluster setup.

## Running the Proxy

Validate your configuration syntax before starting:

```bash
iron-proxy check -c iron-proxy.toml
```

Start the proxy in the foreground (ideal for Docker/systemd):

```bash
iron-proxy run -c iron-proxy.toml
```

(*Unix Only*) Start the proxy as a detached background daemon:
```bash
iron-proxy start
```
