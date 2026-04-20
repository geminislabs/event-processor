# syntax=docker/dockerfile:1.7

FROM rust:1.92-bookworm AS base

WORKDIR /app

# Build dependencies for crates that compile native libraries (rdkafka/openssl/protobuf).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        cmake \
        perl \
        clang \
        libcurl4-openssl-dev \
        libsasl2-dev \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-chef --locked

FROM base AS planner
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY assets ./assets
COPY src ./src
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY assets ./assets
COPY src ./src

RUN cargo build --release --locked

FROM debian:bookworm-slim AS runtime

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libsasl2-2 \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/event-processor /app/event-processor

EXPOSE 8080

# HEALTH_BIND_ADDR format: host:port (e.g. 0.0.0.0:8080).
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS "http://${HEALTH_BIND_ADDR}/health" || exit 1

ENTRYPOINT ["/app/event-processor"]