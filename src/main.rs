use clap::Parser;
use crabkv::config::{Cli, Config};
use crabkv::server::Server;
use tracing_subscriber::EnvFilter;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let mut config = match &cli.config {
        Some(path) => Config::from_file(path)?,
        None => Config::default(),
    };
    config.apply_cli(&cli);

    let filter = if config.server.debug {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"))
    } else {
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    tracing::info!(
        "CrabKV starting on {}:{} (debug={})",
        config.server.bind,
        config.server.port,
        config.server.debug
    );

    let server = Server::new(config).await;
    server.run().await?;

    Ok(())
}
