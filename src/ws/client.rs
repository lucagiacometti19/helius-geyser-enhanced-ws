use crate::config::Config;
use crate::handler::handle_parsed_tx;
use crate::model::{ParsedTx, TxNotification};
use crate::ws::parser::build_parsed_tx;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, mpsc};
use tokio::time::{Duration, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};
use url::Url;

use crate::ws::Metrics;

#[inline]
fn parse_json<T: serde::de::DeserializeOwned>(buf: String) -> Result<T> {
    Ok(serde_json::from_str(&buf)?)
}

pub async fn run(config: Config) -> Result<()> {
    // Channel from parser → app handler
    let (parsed_tx, mut parsed_rx) = mpsc::channel::<ParsedTx>(256);

    let metrics = Arc::new(Metrics::default());
    let _ = metrics.clone().start_metrics_collection();

    // App handler task
    tokio::spawn(async move {
        while let Some(tx) = parsed_rx.recv().await {
            metrics.observe(
                tx.serde_time.unwrap(),
                tx.parsedtx_time.unwrap(),
                tx.queue_start.unwrap().elapsed().as_nanos() as u64,
            );
            handle_parsed_tx(tx).await;
        }
    });

    // Connection loop with backoff
    let mut backoff_ms = 500u64;
    loop {
        match connect_once(&config, &parsed_tx).await {
            Ok(_) => {
                // Normal close — reset backoff
                backoff_ms = 500;
            }
            Err(err) => {
                error!(error = %err, "WS connection failed");
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(30_000);
            }
        }
    }
}

async fn connect_once(config: &Config, parsed_tx: &mpsc::Sender<ParsedTx>) -> Result<()> {
    let mut url = Url::parse(&config.ws_url)?;
    // ensure ?api-key=... present
    let mut qp: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| (k.into(), v.into()))
        .collect();
    if !qp.iter().any(|(k, _)| k == "api-key") {
        qp.push(("api-key".to_string(), config.api_key.clone()));
    }
    url.query_pairs_mut().clear().extend_pairs(qp);

    info!(%url, "connecting to Helius Enhanced WebSocket");
    let (ws_stream, _resp) = connect_async(url.as_str())
        .await
        .context("connect_async failed")?;
    let (write, mut read) = ws_stream.split();

    // Share the write half for pings and any other sends
    let write = Arc::new(Mutex::new(write));

    // Send subscription
    {
        let subscribe = build_transaction_subscribe(config);
        let mut w = write.lock().await;
        w.send(Message::Text(subscribe)).await?;
    }

    // Ping task
    let ping_secs = config.ping_secs;
    let write_for_ping = write.clone();
    let ping_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(ping_secs));
        loop {
            interval.tick().await;
            debug!("ping");
            let mut w = write_for_ping.lock().await;
            if let Err(e) = w.send(Message::Ping(Vec::new())).await {
                warn!(error = %e, "ping failed; ending ping task");
                break;
            }
        }
    });

    // Reader loop
    while let Some(msg) = read.next().await {
        let serde_start = Instant::now();
        match msg {
            Ok(Message::Text(txt)) => match parse_json::<TxNotification>(txt) {
                Ok(note) => {
                    let serde_time = serde_start.elapsed().as_nanos() as u64;
                    if let Some(params) = note.params {
                        let parsed = build_parsed_tx(params.result).set_serde_time(serde_time);
                        if parsed_tx
                            .try_send(parsed.clone().start_queue_timer())
                            .is_err()
                        {
                            let _ = parsed_tx.send(parsed.start_queue_timer()).await;
                            debug!("handler channel full; used async send");
                        }
                    } else {
                        warn!("missing params in notification");
                    }
                }
                Err(e) => {
                    warn!(error = %e, "failed to parse JSON message");
                }
            },
            Ok(Message::Binary(_)) => {}
            Ok(Message::Ping(_)) => { /* auto-pong handled by tungstenite */ }
            Ok(Message::Pong(_)) => {
                debug!("pong");
            }
            Ok(Message::Close(frame)) => {
                info!(?frame, "server closed connection");
                break;
            }
            Ok(Message::Frame(_)) => {
                debug!("frame");
            }
            Err(e) => {
                warn!(error = %e, "WS read error");
                break;
            }
        }
    }

    // Stop ping and try to close nicely
    ping_handle.abort();
    if let Ok(mut w) = write.try_lock() {
        let _ = w.send(Message::Close(None)).await;
    }

    Ok(())
}

fn build_transaction_subscribe(cfg: &Config) -> String {
    let filter = serde_json::json!({
        "vote": cfg.include_votes,
        "failed": cfg.include_failed,
        "accountInclude": cfg.accounts,
    });
    let options = serde_json::json!({
        "commitment": cfg.commitment.as_str(),
        "encoding": "jsonParsed",
        "transactionDetails": "full",
        "showRewards": false,
        "maxSupportedTransactionVersion": 0
    });
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "transactionSubscribe",
        "params": [ filter, options ]
    });
    req.to_string()
}
