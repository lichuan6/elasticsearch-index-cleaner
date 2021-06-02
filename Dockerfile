# 1: Build the exe
FROM rust:1.52 as builder
ENV PKG_CONFIG_ALLOW_CROSS=1
WORKDIR /usr/src

# 1a: Prepare for static linking
RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install -y pkg-config libssl-dev
    #  musl-tools && \
    # rustup target add x86_64-unknown-linux-musl

# 1b: Download and compile Rust dependencies (and store as a separate Docker layer)
RUN USER=root cargo new elasticsearch-index-cleaner
WORKDIR /usr/src/elasticsearch-index-cleaner
COPY Cargo.toml Cargo.lock ./
# RUN cargo install --target x86_64-unknown-linux-musl --path .
# RUN RUSTFLAGS=-Clinker=musl-gcc cargo install -—release —target=x86_64-unknown-linux-musl --path .
RUN cargo install --path .

# 1c: Build the exe using the actual source code
COPY src ./src
# RUN cargo install --target x86_64-unknown-linux-musl --path .
# RUN RUSTFLAGS=-Clinker=musl-gcc cargo install -—release —target=x86_64-unknown-linux-musl --path .
RUN cargo install --path .

# 2: Copy the exe and extra files ("static") to an empty Docker image
#FROM scratch
# FROM alpine:latest
FROM gcr.io/distroless/cc-debian10
COPY --from=builder /usr/local/cargo/bin/elasticsearch-index-cleaner /pulsar-elasticsearch-sync-rs
#COPY static .
# USER 1000
ENTRYPOINT ["/elasticsearch-index-cleaner"]
