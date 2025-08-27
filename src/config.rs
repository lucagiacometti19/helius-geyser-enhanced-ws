use anyhow::{Context, Result, bail};
use std::env;

#[derive(Clone, Debug)]
pub struct Config {
    pub ws_url: String,
    pub api_key: String,
    pub accounts: Vec<String>, // subscribe filter (OR semantics)
    pub commitment: Commitment,
    pub include_failed: bool,
    pub include_votes: bool,
    pub ping_secs: u64,
}

#[derive(Clone, Debug)]
pub enum Commitment {
    Processed,
    Confirmed,
    Finalized,
}
impl Commitment {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Processed => "processed",
            Self::Confirmed => "confirmed",
            Self::Finalized => "finalized",
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let api_key = env::var("HELIUS_API_KEY")
            .context("Set HELIUS_API_KEY in your environment")?;

        // Default Atlas mainnet WS; can override with HELIUS_WS_URL (e.g., devnet)
        let ws_url = env::var("HELIUS_WS_URL")
            .unwrap_or("wss://atlas-mainnet.helius-rpc.com/".to_string());

        // To avoid firehose by mistake, require at least one account by default.
        let accounts = env::var("ACCOUNTS")
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>();

        if accounts.is_empty() {
            bail!(
                "ACCOUNTS is empty. Provide a comma-separated list of accounts to filter (to avoid subscribing to the whole chain)."
            );
        }

        let commitment = match env::var("COMMITMENT")
            .unwrap_or("processed".to_string())
            .as_str()
        {
            "processed" => Commitment::Processed,
            "confirmed" => Commitment::Confirmed,
            "finalized" => Commitment::Finalized,
            other => bail!("Invalid COMMITMENT '{}'", other),
        };

        let include_failed = matches!(
            env::var("INCLUDE_FAILED").as_deref(),
            Ok("1") | Ok("true")
        );
        let include_votes = matches!(
            env::var("INCLUDE_VOTES").as_deref(),
            Ok("1") | Ok("true")
        );

        let ping_secs = env::var("PING_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30);

        Ok(Self {
            ws_url,
            api_key,
            accounts,
            commitment,
            include_failed,
            include_votes,
            ping_secs,
        })
    }
}
