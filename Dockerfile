# Stage 1: Build
FROM rust:1.94-slim AS builder
WORKDIR /app

# Install pkg-config and OpenSSL dependencies
RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release -p iron_proxy

# Stage 2: Minimal runtime image
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/iron-proxy /usr/local/bin/iron-proxy
ENTRYPOINT ["iron-proxy"]