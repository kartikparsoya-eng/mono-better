//! rust-syncer binary entry point.
//!
//! When `ZERO_SYNCER=rust`, the process manager (`main.ts`) launches this
//! binary instead of the TS syncer worker. The binary:
//! 1. Parses config from env vars / CLI args.
//! 2. Starts the WebSocket server.
//! 3. Accepts connections, parses connect params, sends `connected`.
//! 4. (Phase 2+) Routes connections to CG threads with ViewSyncer dispatch loops.

use rust_syncer::{run_ws_server, WsServerConfig};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let port: u16 = env::var("ZERO_SYNCER_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);

    let max_payload: usize = env::var("ZERO_WEBSOCKET_MAX_PAYLOAD_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(64 * 1024 * 1024); // 64MB default

    let config = WsServerConfig {
        port,
        max_payload_bytes: max_payload,
        compression: env::var("ZERO_WEBSOCKET_COMPRESSION").is_ok(),
    };

    tracing::info!("Starting rust-syncer on port {port}");

    // Phase 1: Just accept connections, parse params, send connected.
    // Phase 2+ will route to CG threads.
    run_ws_server(config, |ctx| {
        tracing::info!(
            "Connection accepted: clientGroupID={}, clientID={}, wsID={}, protocolVersion={}",
            ctx.params.client_group_id,
            ctx.params.client_id,
            ctx.params.ws_id,
            ctx.params.protocol_version
        );
        // Phase 1: The connection is accepted and `connected` has been sent.
        // The WS reader/writer tasks are running. No CG thread yet.
        //
        // In Phase 2, this handler will:
        // 1. Look up or create a CG thread for the clientGroupID.
        // 2. Send the ConnectionContext to the CG thread via a channel.
        // 3. The CG thread runs the ViewSyncer dispatch loop.
    })
    .await?;

    Ok(())
}
