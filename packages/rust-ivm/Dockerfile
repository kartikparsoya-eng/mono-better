FROM rust:1.85-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src/ src/

RUN cargo build --release --bin rust-ivm-server

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/rust-ivm-server /usr/local/bin/rust-ivm-server

ENV PORT=8080
EXPOSE 8080

CMD ["rust-ivm-server"]
