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
use crate::handlers::pump_fun::reconcile_pump_activities;
use crate::handlers::pump_fun::{parse_pump_fun_event, parse_pump_fun_ix};
use crate::model::{IntoTransactionStatusMeta, TxNotification};
use crate::redis::RedisManager;
use yellowstone_grpc_proto::prost::Message;

pub async fn handle_tx_notification(tx: TxNotification, redis: Arc<RedisManager>) -> Result<()> {
    // 1. Extract behavioral features and sanitized tx
    let (mut labels, sanitized) = match extract_labels(&tx) {
        Ok(res) => res,
        Err(_) => {
            // @todo
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
    let mut relevant_activities = Vec::new();
    let mut trade_events = Vec::new();

    for ix in ixs.iter().flat_map(|ix| ix.visit_all()) {
        // A) Check for PumpFun Instructions
        if let Some(act) = parse_pump_fun_ix(ix).await {
            relevant_activities.push(act);
            continue;
        }
        // B) Check for PumpFun CPI Events
        if let Some(event) = parse_pump_fun_event(ix).await {
            trade_events.push(event);
            continue;
        }
        // @todo: other programs to parse
    }

    // 4. Reconcile unordered activities and events
    reconcile_pump_activities(&mut relevant_activities, trade_events);

    // 5. Update Labels & Redis
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
                if slippage_percent.is_some() {
                    relevant = true;
                }
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
                if slippage_percent.is_some() {
                    relevant = true;
                }
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

    if relevant {
        //if let Err(e) = redis.update_stats(&labels).await {
        //    warn!(
        //        "Failed to update redis stats for {}: {:?}",
        //        labels.fee_payer, e
        //    );
        //}
        if let Err(e) = redis.save_transaction(&labels).await {
            warn!(
                "Failed to save transaction for {}: {:?}",
                labels.fee_payer, e
            );
        }
    }

    Ok(())
}
