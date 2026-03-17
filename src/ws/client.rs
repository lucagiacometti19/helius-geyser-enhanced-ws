use crate::config::Config;
use crate::handlers::handle_tx_notification;
use crate::model::TxNotification;
use crate::redis::RedisManager;
use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::sync::mpsc::error::TrySendError;
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
    // Channel from websocket → app handler
    // We send raw Strings to offload parsing to the parallel workers
    let (parsed_tx, mut parsed_rx) = mpsc::channel::<(String, Instant)>(4096);

    let metrics = Arc::new(Metrics::default());
    metrics.clone().start_metrics_collection();

    let redis_manager = Arc::new(
        RedisManager::new(&config.redis_url)
            .await
            .context("Failed to init Redis")?,
    );

    let sem = Arc::new(Semaphore::new(100));
    // App handler task
    let metrics_for_dispatcher = metrics.clone();
    let redis_for_dispatcher = redis_manager.clone();
    tokio::spawn(async move {
        while let Some((txt, recv_ts)) = parsed_rx.recv().await {
            let wait_time = recv_ts.elapsed().as_nanos() as u64;
            let _permit = if let Ok(permit) = sem.clone().try_acquire_owned() {
                permit
            } else {
                warn!("Handler busy: waiting for semaphore!");
                sem.clone().acquire_owned().await.expect("Semaphore closed")
            };
            let metrics = metrics_for_dispatcher.clone();
            let redis = redis_for_dispatcher.clone();

            tokio::spawn(async move {
                let _permit = _permit;

                let parse_s = Instant::now();
                match parse_json::<TxNotification>(txt) {
                    Ok(tx) => {
                        let parse_time = parse_s.elapsed().as_nanos() as u64;
                        let logic_s = Instant::now();

                        if let Err(e) = handle_tx_notification(tx, redis).await {
                            warn!(error = %e, "Txn handler failed");
                        }

                        let logic_time = logic_s.elapsed().as_nanos() as u64;
                        metrics.observe(wait_time, parse_time, logic_time);
                    }
                    Err(e) => {
                        error!(error = %e.root_cause(), "Failed to parse JSON message");
                    }
                }
                // permit drops here
            });
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
    // Ok(())
}

async fn connect_once(config: &Config, parsed_tx: &mpsc::Sender<(String, Instant)>) -> Result<()> {
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

    info!(%url, "Connecting to Helius Enhanced WebSocket");
    let (ws_stream, _resp) = connect_async(url.as_str())
        .await
        .context("Failed to connect to Helius Enhanced WebSocket")?;
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
    let ping_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(ping_secs));
        loop {
            interval.tick().await;
            debug!("ping");
            let mut w = write_for_ping.lock().await;
            if let Err(e) = w.send(Message::Ping(Vec::new())).await {
                error!(error = %e, "Ping failed; ending ping task");
                break;
            }
        }
    });

    // Reader loop
    while let Some(msg) = read.next().await {
        match msg {
            Ok(Message::Text(txt)) => {
                let recv_ts = Instant::now();
                match parsed_tx.try_reserve() {
                    Ok(permit) => {
                        // send immediately
                        permit.send((txt, recv_ts));
                    }
                    Err(TrySendError::Closed(_)) => {
                        error!("Handler channel closed");
                        break;
                    }
                    Err(TrySendError::Full(_)) => {
                        // no space now: wait for capacity, then send
                        match parsed_tx.reserve().await {
                            Ok(permit) => permit.send((txt, recv_ts)),
                            Err(_) => {
                                error!("Handler channel closed while waiting");
                                break;
                            }
                        }
                    }
                }
            }
            Ok(Message::Binary(_) | Message::Ping(_) | Message::Frame(_)) => {}
            Ok(Message::Pong(_)) => {
                debug!("Pong");
            }
            Ok(Message::Close(frame)) => {
                info!(?frame, "Server closed connection");
                break;
            }
            Err(e) => {
                warn!(error = %e, "WS read error");
                break;
            }
        }
    }

    // stop ping and try to close nicely
    ping_task.abort();
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
        "accountExclude": cfg.excluded_accounts,
    });
    let options = serde_json::json!({
        "commitment": cfg.commitment.as_str(),
        "encoding": "base64",
        "transactionDetails": "full",
        "showRewards": false,
        "maxSupportedTransactionVersion": 1,
    });
    let req = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "transactionSubscribe",
        "params": [ filter, options ]
    });
    req.to_string()
}
