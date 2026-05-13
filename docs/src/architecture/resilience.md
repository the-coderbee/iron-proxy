# Resilience &amp; Health Checks

Iron-Proxy is designed to assume that upstream networks are hostile and backend servers will inevitably fail.

## Active Background Probing
A dedicated Tokio task runs outside the proxy engines, firing HTTP GET requests or TCP pings at the configured interval. 
* If a backend fails to respond within the strict 2-second timeout, the `DashMap` registry instantly marks the node as `Dead`.
* The routing engine will immediately cease sending new connections to that node.
* The probe continues pinging; once the node returns a successful response, it is automatically reintroduced to the cluster.

## Passive L7 Circuit Breaking
If the active probe hasn't detected a failure yet, but the L7 proxy engine encounters an unexpected socket drop or `502 Bad Gateway` from a backend, the L7 proxy acts defensively. It automatically retries the buffered request on another node, and proactively flags the failing node as `Dead` in the shared registry.