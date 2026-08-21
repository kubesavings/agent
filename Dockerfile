# Stage 1: Build static binary using cargo-chef for proper layer caching
FROM rust:1.97-slim AS chef

RUN apt-get update && apt-get install -y --no-install-recommends \
    musl-tools \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add x86_64-unknown-linux-musl \
    && cargo install cargo-chef --locked

WORKDIR /build

# Compute the recipe (dependency manifest) — this layer is cheap and rarely changes
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Build dependencies only — this layer is cached until Cargo.toml/Cargo.lock change
FROM chef AS builder
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --target x86_64-unknown-linux-musl --recipe-path recipe.json

# Build the actual binary — only re-runs when src/ changes.
# --locked keeps the released image reproducible against the committed Cargo.lock.
# [profile.release] already sets strip = true, so no separate strip step.
COPY . .
RUN cargo build --release --locked --target x86_64-unknown-linux-musl

# Stage 2: Minimal image — binary only
FROM scratch

COPY --from=builder /build/target/x86_64-unknown-linux-musl/release/kubesavings-agent /agent
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENTRYPOINT ["/agent"]
