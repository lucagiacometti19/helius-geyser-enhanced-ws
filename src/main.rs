mod config;
mod handler;
mod model;
mod ws;

use crate::config::Config;
use anyhow::Result;
use dotenvy::dotenv;
use rustls::crypto::CryptoProvider;
use rustls::crypto::ring::default_provider as ring_default;
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    // wtf is this?
    let _ = CryptoProvider::install_default(ring_default());

    // fast, structured logging
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    fmt().with_env_filter(filter).compact().init();

    let config = Config::from_env()?;
    ws::run(config).await?;
    Ok(())
}
