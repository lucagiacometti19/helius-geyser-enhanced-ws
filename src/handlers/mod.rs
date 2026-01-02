pub mod common;
pub mod pump_amm;
pub mod pump_fun;

use anyhow::Result;
use solana_transaction_status::option_serializer::OptionSerializer;
use std::sync::Arc;
use tracing::{debug, warn};
use yellowstone_grpc_proto::convert_to;
use yellowstone_grpc_proto::geyser::{SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo};
use yellowstone_vixen_core::instruction::InstructionUpdate;

use crate::handlers::common::{PlatformActivity, extract_labels};
use crate::handlers::pump_amm::parse_pump_amm_ix;
use crate::handlers::pump_fun::{parse_pump_fun_event, parse_pump_fun_ix};
use crate::model::{IntoTransactionStatusMeta, TxNotification};
use crate::redis::RedisManager;
use yellowstone_grpc_proto::prost::Message;

pub async fn handle_tx_notification(tx: TxNotification, redis: Arc<RedisManager>) -> Result<()> {
    // 1. Extract behavioral features and sanitized tx
    let (mut labels, sanitized) = match extract_labels(&tx) {
        Ok(res) => res,
        Err(_) => {
            // Handle parsing errors gracefully
            // In production, you might want to log this or just skip
            return Ok(());
        }
    };

    // 2. Conversion layer
    let tx_params = tx.params.as_ref().unwrap();
    let tx_envelope = &tx_params.result.transaction;

    let proto_meta = convert_to::create_transaction_meta(
        &tx_envelope.meta.clone().into_transaction_status_meta(),
    );
    let proto_tx = convert_to::create_transaction(&sanitized);

    let info = SubscribeUpdateTransactionInfo {
        transaction: Some(proto_tx),
        index: 0,
        is_vote: false,
        signature: tx_params.result.signature.encode_to_vec(),
        meta: Some(proto_meta),
    };
    let d = SubscribeUpdateTransaction {
        slot: tx_params.result.slot,
        transaction: Some(info),
    };

    // Nested list of instructions and inner instructions
    let ixs = InstructionUpdate::parse_from_txn(&d)?;

    // 3. Single-pass Stack Parsing
    // We hold unresolved activities until we might find their corresponding event
    let mut relevant_activities: Vec<PlatformActivity> = Vec::new();
    for ix in ixs.iter().flat_map(|ix| ix.visit_all()) {
        // A) Check for PumpFun Instruction (Buy/Sell/etc)
        if let Some(act) = parse_pump_fun_ix(ix).await {
            relevant_activities.push(act);
            continue;
        }
        // B) Check for PumpFun Event (CPI/Log data)
        // If we find an event, we try to attach it to the LAST activity if matches
        // Here strong assumption that events and ixs are in the same order, which i assume
        // to be true given the atomicity of instructions implicit of the solana blockchain
        // and the preservation of ordering by helius websocket subscription
        if let Some(event) = parse_pump_fun_event(ix).await
            && let Some(last_act) = relevant_activities.last_mut()
        {
            // Check if last activity matches this event (mint & side)
            match last_act {
                PlatformActivity::Buy {
                    mint,
                    amount,
                    slippage_percent,
                    ..
                } => {
                    if event.mint.to_string() == *mint && event.is_buy {
                        let limit_amount = *amount;
                        let actual_amount = event.sol_amount;
                        // Update real amount
                        *amount = actual_amount;
                        // Calc slippage
                        *slippage_percent = if limit_amount > 0 {
                            Some(
                                ((limit_amount as f64 - actual_amount as f64)
                                    / limit_amount as f64)
                                    * 100.0,
                            )
                        } else {
                            Some(0.0)
                        };
                    } else {
                        warn!(
                            "Shouldn't have happened, I was expecting a trade event matching the last Buy. txn {}",
                            tx_params.result.signature
                        );
                        warn!(
                            "Current event mint: {}, Last buy mint: {}",
                            event.mint, mint
                        )
                    }
                }
                PlatformActivity::Sell {
                    mint,
                    amount,
                    slippage_percent,
                    ..
                } => {
                    if event.mint.to_string() == *mint && !event.is_buy {
                        let min_output = *amount; // Saved as min_sol_output
                        let actual_amount = event.sol_amount;
                        // Update real amount
                        *amount = actual_amount;
                        // Calc slippage
                        *slippage_percent = if actual_amount > 0 {
                            Some(
                                ((actual_amount as f64 - min_output as f64) / actual_amount as f64)
                                    * 100.0,
                            )
                        } else {
                            Some(0.0)
                        };
                    } else {
                        warn!(
                            "Shouldn't have happened, I was expecting a trade event matching the last Sell. txn {}",
                            tx_params.result.signature
                        );
                        warn!(
                            "Current event mint: {}, Last sell mint: {}",
                            event.mint, mint
                        )
                    }
                }
                _ => {
                    debug!("Last activity not Buy/Sell, cannot match event!"); // Shouldn't happen
                }
            }
            continue;
        }

        // C) @todo: Check for other platforms (PumpAMM)
        //if let Some(act) = parse_pump_amm_ix(ix).await {
        //    relevant_activities.push(act);
        //}
    }

    // 4. Update Labels & Redis
    let mut relevant = false;
    for act in relevant_activities {
        match &act {
            PlatformActivity::Buy {
                amount,
                slippage_percent,
                ..
            } => {
                debug!(
                    "PumpFun Buy in txn {}: {} SOL, Slip: {:?}",
                    tx_params.result.signature,
                    *amount as f64 / 1_000_000_000.0,
                    slippage_percent
                );
                relevant = true;
            }
            PlatformActivity::Sell {
                amount,
                slippage_percent,
                ..
            } => {
                debug!(
                    "PumpFun Sell in txn {}: {} SOL, Slip: {:?}",
                    tx_params.result.signature,
                    *amount as f64 / 1_000_000_000.0,
                    slippage_percent
                );
                relevant = true;
            }
            PlatformActivity::Other {
                ix_name: _,
                mint: _,
            } => {
                // Count other activities as relevant too
                relevant = true;
            }
        }
        labels.activities.push(act);
    }

    if relevant && let Err(e) = redis.update_stats(&labels).await {
        warn!(
            "Failed to update redis stats for {}: {:?}",
            labels.fee_payer, e
        );
    }

    Ok(())
}
