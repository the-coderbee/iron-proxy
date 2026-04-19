# ==========================================
# STAGE 1: The Builder
# ==========================================
# We start with an official Debian Linux image that has Rust pre-installed
FROM rust:1.94-slim AS builder

# Create a folder inside the container to do our work
WORKDIR /usr/src/iron-proxy

# Copy everything from your laptop's folder into the container
COPY . .

# Tell Rust to compile the highly-optimized production binary
RUN cargo build --release

# ==========================================
# STAGE 2: The Runner (The Final Product)
# ==========================================
# We start fresh with a tiny, bare-bones Debian Linux image
FROM debian:bookworm-slim

# Create the folder where our app will live
WORKDIR /app

# COPY the finished executable from STAGE 1 into this new container
COPY --from=builder /usr/src/iron-proxy/target/release/gateway ./gateway

# COPY your configuration file so the gateway knows how to route traffic
COPY --from=builder /usr/src/iron-proxy/gateway_config.toml ./

# Tell the container what to do when it wakes up
CMD ["./gateway"]