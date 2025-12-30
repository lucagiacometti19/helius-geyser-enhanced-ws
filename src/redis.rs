use crate::handlers::common::TxLabels;
use anyhow::{Context, Result};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use tracing::error;

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

        pipeline.hincr(&key, "total_priority_fee", priority_fee);
        pipeline.hincr(&key, "total_cu", compute_units);
        pipeline.hincr(&key, "total_sq_cu", compute_units * compute_units);

        if jito_tip > 0 {
            pipeline.hincr(&key, "total_jito_tip", jito_tip);
            pipeline.hincr(&key, "tipped_tx_count", 1);
        }

        if let Some(last_block) = last_block_val
            && block == last_block
        {
            pipeline.hincr(&key, "same_block_tx_count", 1);
        }

        if let Some(diff) = last_ts_val.and_then(|last| current_ts.checked_sub(last)) {
            pipeline.hincr(&key, "sum_time_diff", diff);
            // squared diff in milliseconds can overflow i64 (Redis HINCRBY limit)
            // if diff > ~35 days we skip the squared update
            if let Some(sq_diff) = diff.checked_mul(diff) {
                pipeline.hincr(&key, "sum_sq_time_diff", sq_diff);
            } else {
                error!(
                    "Timestamp diff squared overflows for wallet {} (diff: {}ms)",
                    wallet, diff
                );
            }
        }

        pipeline.hset(&key, "last_tx_ts", current_ts);
        pipeline.hset(&key, "last_tx_block", block);
        pipeline.query_async::<()>(&mut conn).await?;

        Ok(())
    }
}
