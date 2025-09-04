use crate::model::{IntoTransactionStatusMeta, TxNotification};
use anyhow::{Context, Result};
use solana_message::SimpleAddressLoader;
use solana_message::v0::LoadedAddresses;
use solana_pubkey::Pubkey;
use solana_transaction::sanitized::{MessageHash, SanitizedTransaction};
use solana_transaction_status::EncodedTransaction;
use std::collections::HashSet;
use yellowstone_grpc_proto::geyser::{SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo};
use yellowstone_grpc_proto::prost::Message;
use yellowstone_grpc_proto::{convert_to, solana::storage::confirmed_block};
use yellowstone_vixen_core::Parser;
use yellowstone_vixen_core::instruction::InstructionUpdate;
use yellowstone_vixen_pumpfun_parser::{
    instructions_parser::InstructionParser as pumpfun_ix_parser, programs::PUMP_ID,
};

/// Replace this with your own business logic.
/// Runs on a dedicated Tokio task, fed by a bounded channel.
pub async fn handle_tx_notification(tx: TxNotification) -> Result<()> {
    // … do your low‑latency work here …
    let tx_params = if let Some(params) = tx.params {
        params
    } else {
        return Err(anyhow::anyhow!("Missing transaction parameters"));
    };
    let tx_envelope = tx_params.result.transaction;
    let tx_slot = tx_params.result.slot;
    let tx_signature = tx_params.result.signature;

    let vtx = tx_envelope
        .transaction
        .decode()
        .with_context(|| format!("failed to decode transaction: sig={}", tx_signature))?;

    let proto_meta =
        convert_to::create_transaction_meta(&tx_envelope.meta.into_transaction_status_meta());

    let load_addresses = LoadedAddresses {
        writable: proto_meta
            .loaded_writable_addresses
            .iter()
            .map(|addr| {
                let bytes: [u8; 32] = addr.as_slice().try_into().with_context(|| {
                    format!(
                        "invalid pubkey length: addr={:?}, sig={}",
                        addr, tx_signature
                    )
                })?;
                Ok(Pubkey::new_from_array(bytes))
            })
            .collect::<Result<Vec<_>>>()?,
        readonly: proto_meta
            .loaded_readonly_addresses
            .iter()
            .map(|addr| {
                let bytes: [u8; 32] = addr.as_slice().try_into().with_context(|| {
                    format!(
                        "invalid pubkey length: addr={:?}, sig={}",
                        addr, tx_signature
                    )
                })?;
                Ok(Pubkey::new_from_array(bytes))
            })
            .collect::<Result<Vec<_>>>()?,
    };

    let sanitized = SanitizedTransaction::try_create(
        vtx.clone(),
        MessageHash::Compute,
        // is_simple_vote_tx, force it to false to save time from useless check
        Some(false),
        SimpleAddressLoader::Enabled(load_addresses),
        &HashSet::new(), // reserved account keys
    )?;
    let proto_tx: confirmed_block::Transaction = convert_to::create_transaction(&sanitized);

    let info = SubscribeUpdateTransactionInfo {
        transaction: Some(proto_tx),
        index: 0,
        is_vote: false,
        signature: tx_signature.encode_to_vec(),
        meta: Some(proto_meta),
    };
    let d = SubscribeUpdateTransaction {
        slot: tx_slot,
        transaction: Some(info),
    };

    let ixs = InstructionUpdate::parse_from_txn(&d).with_context(|| {
        format!(
            "failed to parse instruction updates from transaction: {:#?}",
            d
        )
    })?;
    for ix in &ixs {
        if PUMP_ID.to_bytes() == ix.program.into_bytes() {
            if let Ok(f) = pumpfun_ix_parser.parse(ix).await {
                //info!("{:?}", f);
            } else {
                //warn!("failed to parse pumpfun ix: {:?}", ix);
            }
        } else {
            // decide what to do with other instructions
        }
    }
    Ok(())
}
