# Command Line Interface

The Iron-Proxy binary provides a robust CLI for operational lifecycle management.

| Command | Description |
|---------|-------------|
| `iron-proxy init` | Generates a template `iron-proxy.toml` in the current directory. |
| `iron-proxy check -c <file>` | Validates the TOML syntax without binding to any ports. |
| `iron-proxy run -c <file>` | Starts the proxy synchronously in the foreground. |
| `iron-proxy start -c <file>` | *(Unix)* Forks the proxy to a background daemon process. |
| `iron-proxy stop` | *(Unix)* Sends a SIGTERM to the daemon for graceful shutdown. |
| `iron-proxy status` | Queries the local Admin API to print real-time backend health. |

### Daemonization Logs
When using the `start` command on Unix systems, standard output and errors are detached from the TTY and piped directly to `iron-proxy.out` and `iron-proxy.err` in the working directory.
