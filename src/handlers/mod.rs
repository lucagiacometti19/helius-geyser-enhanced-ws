pub mod common;
pub mod pump_amm;
pub mod pump_fun;

use anyhow::Result;
use std::sync::Arc;
use tracing::{debug, info, warn};
use yellowstone_grpc_proto::convert_to;
use yellowstone_grpc_proto::geyser::{SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo};
use yellowstone_vixen_core::instruction::InstructionUpdate;

use crate::handlers::common::{PlatformActivity, extract_labels};
use crate::handlers::pump_amm::parse_pump_amm_ix;
use crate::handlers::pump_fun::parse_pump_fun_ix;
use crate::model::{IntoTransactionStatusMeta, TxNotification};
use crate::redis::RedisManager;
use yellowstone_grpc_proto::prost::Message;

pub async fn handle_tx_notification(tx: TxNotification, redis: Arc<RedisManager>) -> Result<()> {
    // 1. Extract behavioral features and sanitized tx
    let (labels, sanitized) = match extract_labels(&tx) {
        Ok(res) => res,
        Err(_) => {
            // Handle parsing errors gracefully
            todo!()
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

    let ixs = InstructionUpdate::parse_from_txn(&d)?;

    // 3. Dispatch to platform parsers
    let mut relevant = false;
    for ix in &ixs {
        if let Some(act) = parse_pump_fun_ix(ix).await {
            relevant = true;
            match act {
                PlatformActivity::Buy { amount } => debug!("PumpFun Buy detected: {}", amount),
                PlatformActivity::Sell { amount } => debug!("PumpFun Sell detected: {}", amount),
                _ => {}
            }
        }
        if let Some(act) = parse_pump_amm_ix(ix).await {
            relevant = true;
            match act {
                PlatformActivity::Buy { amount } => debug!("PumpAMM Buy detected: {}", amount),
                PlatformActivity::Sell { amount } => debug!("PumpAMM Sell detected: {}", amount),
                _ => {}
            }
        }
    }

    // 4. If any relevant activity was found, update behavioral stats in Redis
    if relevant && let Err(e) = redis.update_stats(&labels).await {
        warn!(
            "Failed to update redis stats for {}: {:?}",
            labels.fee_payer, e
        );
    }

    Ok(())
}
