# syntax=docker/dockerfile:1.7

FROM rust:1.82-bookworm AS builder

WORKDIR /app

# Build dependencies for crates that compile native libraries (rdkafka/openssl/protobuf).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        cmake \
        perl \
        clang \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/event-processor /app/event-processor

EXPOSE 8080

# HEALTH_BIND_ADDR format: host:port (e.g. 0.0.0.0:8080).
# Extract port at runtime so this stays in sync with the env var.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS "http://localhost:${HEALTH_BIND_ADDR##*:}/health" || exit 1

ENTRYPOINT ["/app/event-processor"]