use clap::Parser;
use crabkv::config::{Cli, Config};
use crabkv::server::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli = Cli::parse();

    let mut config = match &cli.config {
        Some(path) => Config::from_file(path)?,
        None => Config::default(),
    };
    config.apply_cli(&cli);

    log::info!(
        "CrabKV starting on {}:{}",
        config.server.bind,
        config.server.port
    );

    let server = Server::new(config).await;
    server.run().await?;

    Ok(())
}
