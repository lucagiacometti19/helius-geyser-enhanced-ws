use crate::handlers::common::{PlatformActivity, TxLabels};
use anyhow::{Context, Result};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;

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

    pub async fn save_transaction(&self, labels: &TxLabels) -> Result<()> {
        let mut conn = self.manager.clone();
        let json = serde_json::to_string(labels).context("Failed to serialize labels")?;
        conn.hset::<_, _, _, ()>("transactions", &labels.signature, json)
            .await?;
        Ok(())
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

        // Normalize units for consistency and overflow prevention
        // - Priority Fee: Lamports/CU (from micro-lamports/CU)
        // - Compute Units: raw CU
        // - Jito Tip: SOL (from lamports)
        let pf_lam = priority_fee as f64 / 1_000_000.0;
        let cu = compute_units as f64;
        let tip_sol = jito_tip as f64 / 1_000_000_000.0;

        // 1. Priority Fees (Lamports/CU)
        pipeline
            .cmd("HINCRBYFLOAT")
            .arg(&key)
            .arg("priority_fee_lam_sum")
            .arg(pf_lam);
        pipeline
            .cmd("HINCRBYFLOAT")
            .arg(&key)
            .arg("priority_fee_lam_sum_sq")
            .arg(pf_lam * pf_lam);

        // 2. Compute Units
        pipeline.cmd("HINCRBYFLOAT").arg(&key).arg("cu_sum").arg(cu);
        pipeline
            .cmd("HINCRBYFLOAT")
            .arg(&key)
            .arg("cu_sum_sq")
            .arg(cu * cu);

        // 3. Jito Tips (SOL)
        if jito_tip > 0 {
            pipeline.hincr(&key, "tipped_tx_count", 1);
            pipeline
                .cmd("HINCRBYFLOAT")
                .arg(&key)
                .arg("jito_tip_sol_sum")
                .arg(tip_sol);
            pipeline
                .cmd("HINCRBYFLOAT")
                .arg(&key)
                .arg("jito_tip_sol_sum_sq")
                .arg(tip_sol * tip_sol);
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
                .arg("time_diff_s_sum")
                .arg(diff_s);
            pipeline
                .cmd("HINCRBYFLOAT")
                .arg(&key)
                .arg("time_diff_s_sum_sq")
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
                    ..
                }
                | PlatformActivity::Sell {
                    amount,
                    mint,
                    ix_name,
                    slippage_percent,
                    ..
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
