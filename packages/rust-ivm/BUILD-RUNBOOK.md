# Rust IVM Zero-Cache Image — Build & Run Runbook

## Build
```bash
cd /Users/kartik.parsoya/Documents/Go-RS
docker build -t zero-cache-rust .
```

## Run
```bash
docker run -p 4848:4848 -p 4849:4849 \
  -e ZERO_UPSTREAM_DB=<pg-url> \
  -e ZERO_APP_ID=<app-id> \
  zero-cache-rust
```

## Verify
```bash
docker exec -it <container> sh -c 'echo $USE_RUST_IVM'  # true
```

## Dev path (no Docker)
```bash
cd rust-ivm && cargo run --release --bin rust-ivm-server  # HTTP on :8080
cd rust-ivm/napi && cargo build --release  # build .node addon
```

## Point ART at it
1. docker build -t zero-cache-rust .
2. docker run -p 4848:4848 -e ZERO_UPSTREAM_DB=... -e ZERO_APP_ID=... zero-cache-rust
3. Point ART at ws://localhost:4848
4. Run G8 differential oracle

## Troubleshooting
- "addon not loaded": check rust-ivm/napi/rust-ivm.node exists
- "BEGIN CONCURRENT error": WAL2 SQLite needed (Dockerfile builds it from wal2-sqlite/)
- "AST parse error": driver must pass astJson: JSON.stringify(query)
