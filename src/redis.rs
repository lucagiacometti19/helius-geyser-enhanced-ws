use crate::handlers::common::{PlatformActivity, TxLabels};
use anyhow::{Context, Result};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use tracing::warn;

#[derive(Clone)]
pub struct RedisManager {
    manager: ConnectionManager,
}

impl RedisManager {
    pub async fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let manager = ConnectionManager::new(client)
            .await
            .context("Failed to create Redis connection manager")?;

        Ok(Self { manager })
    }

    pub async fn update_stats(&self, labels: &TxLabels) -> Result<()> {
        let wallet = &labels.fee_payer;
        let priority_fee = labels.priority_fee;
        let compute_units = labels.compute_units;
        let current_ts = labels.current_ts;
        let jito_tip = labels.jito_tip;
        let block = labels.block;

        // cheap to clone and handles reconnections automatically
        let mut conn = self.manager.clone();
        let key = format!("wallet:{}", wallet);

        // 1. Get last stats for delta calculations
        let (last_ts_val, last_block_val): (Option<u64>, Option<u64>) =
            conn.hget(&key, &["last_tx_ts", "last_tx_block"]).await?;

        let mut pipeline = redis::pipe();
        pipeline.atomic();

        pipeline.hincr(&key, "tx_count", 1);
        pipeline.hincr(&key, "total_priority_fee", priority_fee as i64);
        pipeline.hincr(&key, "total_cu", compute_units as i64);

        if let Some(pf_sq) = priority_fee.checked_mul(priority_fee)
            && pf_sq <= i64::MAX as u64
        {
            pipeline.hincr(&key, "total_sq_priority_fee", pf_sq as i64);
        } else if priority_fee > 0 {
            warn!(
                "Priority fee squared overflow for wallet {} (priority_fee: {})",
                wallet, priority_fee
            );
        }

        if let Some(cu_sq) = compute_units.checked_mul(compute_units)
            && cu_sq <= i64::MAX as u64
        {
            pipeline.hincr(&key, "total_sq_cu", cu_sq as i64);
        } else {
            warn!(
                "Compute units squared overflow for wallet {} (compute_units: {})",
                wallet, compute_units
            );
        }

        if jito_tip > 0 {
            pipeline.hincr(&key, "total_jito_tip", jito_tip as i64);
            pipeline.hincr(&key, "tipped_tx_count", 1);
        }

        if let Some(last_block) = last_block_val
            && block == last_block
        {
            pipeline.hincr(&key, "same_block_tx_count", 1);
        }

        if let Some(diff_ms) = last_ts_val.and_then(|last| current_ts.checked_sub(last)) {
            // Unify to seconds (float) for both sum and squared sum
            let diff_s = diff_ms as f64 / 1000.0;
            let sq_diff_s = diff_s * diff_s;

            pipeline
                .cmd("HINCRBYFLOAT")
                .arg(&key)
                .arg("sum_time_diff_s")
                .arg(diff_s);
            pipeline
                .cmd("HINCRBYFLOAT")
                .arg(&key)
                .arg("sum_sq_time_diff_s")
                .arg(sq_diff_s);
        }

        pipeline.hset(&key, "last_tx_ts", current_ts);
        pipeline.hset(&key, "last_tx_block", block);

        // 2. Process Platform Activities
        let mints_key = format!("wallet:{}:mints", wallet);
        for act in &labels.activities {
            match act {
                PlatformActivity::Buy {
                    amount,
                    mint,
                    ix_name,
                    slippage_percent,
                }
                | PlatformActivity::Sell {
                    amount,
                    mint,
                    ix_name,
                    slippage_percent,
                } => {
                    let prefix = format!("ix:{}:", ix_name);
                    pipeline.hincr(&key, format!("{}count", prefix), 1);

                    // Convert lamports to SOL (float) for precision
                    // Amount comes from TradeEvent (actual), not instruction params (limits)
                    let sol = *amount as f64 / 1_000_000_000.0;
                    pipeline
                        .cmd("HINCRBYFLOAT")
                        .arg(&key)
                        .arg(format!("{}sum", prefix))
                        .arg(sol);
                    pipeline
                        .cmd("HINCRBYFLOAT")
                        .arg(&key)
                        .arg(format!("{}sum_sq", prefix))
                        .arg(sol * sol);

                    if let Some(slippage) = slippage_percent {
                        pipeline
                            .cmd("HINCRBYFLOAT")
                            .arg(&key)
                            .arg(format!("{}slippage_pct_sum", prefix))
                            .arg(*slippage);

                        pipeline
                            .cmd("HINCRBYFLOAT")
                            .arg(&key)
                            .arg(format!("{}slippage_pct_sq_sum", prefix))
                            .arg(*slippage * *slippage);
                    }

                    pipeline.pfadd(&mints_key, mint);
                }
                PlatformActivity::Other { ix_name, .. } => {
                    pipeline.hincr(&key, format!("ix:{}:count", ix_name), 1);
                }
            }
        }

        pipeline.query_async::<()>(&mut conn).await?;

        Ok(())
    }
}
