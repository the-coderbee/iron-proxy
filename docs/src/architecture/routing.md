# Mathematical Routing (Peak EWMA)

To prevent the "thundering herd" problem and avoid routing traffic to slow but technically healthy nodes, Iron-Proxy utilizes advanced mathematical heuristics.

## Peak EWMA (Exponentially Weighted Moving Average)

For Layer 7 HTTP traffic, round-robin or simple least-connections is insufficient. Iron-Proxy tracks the latency of every request using atomic bitwise operations to update an EWMA score without thread-blocking mutexes.

The routing engine calculates a real-time penalty cost for each backend:
`Cost = (Active Connections + 1) * Peak Latency EWMA`

This ensures that a server experiencing a sudden latency spike will be temporarily bypassed until its historical latency average cools down, preventing localized cascading failures.

## Deterministic IP Hashing

If `sticky_sessions` are enabled for legacy stateful applications, Peak EWMA is bypassed. The client's IP address is cryptographically hashed, and the modulo of the hash against the alphabetically sorted list of healthy backends ensures the client always hits the exact same server.