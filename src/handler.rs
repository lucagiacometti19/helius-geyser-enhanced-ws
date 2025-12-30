use crate::model::{IntoTransactionStatusMeta, TxNotification};
use anyhow::{Context, Result};
use solana_message::SimpleAddressLoader;
use solana_message::v0::LoadedAddresses;
use solana_pubkey::Pubkey;
use solana_transaction::sanitized::{MessageHash, SanitizedTransaction};
//use solana_transaction_status::EncodedTransaction;
use pump_parser_gen::ID as PUMP_ID;
use pump_parser_gen::instructions_parser::{InstructionParser as pumpfun_ix_parser, PumpProgramIx};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{info, warn};
use yellowstone_grpc_proto::geyser::{SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo};
use yellowstone_grpc_proto::prost::Message;
use yellowstone_grpc_proto::{convert_to, solana::storage::confirmed_block};
use yellowstone_vixen_core::Parser;
use yellowstone_vixen_core::instruction::InstructionUpdate;

use crate::redis_stats::RedisManager;

pub async fn handle_tx_notification(tx: TxNotification, redis: Arc<RedisManager>) -> Result<()> {
    // … do your low‑latency work here …
    let tx_params = if let Some(params) = tx.params {
        params
    } else {
        return Err(anyhow::anyhow!("Missing transaction parameters"));
    };
    let tx_envelope = tx_params.result.transaction;
    let tx_slot = tx_params.result.slot;
    let tx_signature = tx_params.result.signature;

    // We assume system time as current time
    let current_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    let vtx = tx_envelope
        .transaction
        .decode()
        .with_context(|| format!("failed to decode transaction: sig={}", tx_signature))?;

    // Extract basic features
    let success = tx_envelope.meta.err.is_none();
    let compute_units = tx_envelope.meta.compute_units_consumed.clone().unwrap_or(0);

    let proto_meta =
        convert_to::create_transaction_meta(&tx_envelope.meta.into_transaction_status_meta());

    // Extract Priority Fee
    // ComputeBudget instructions allow setting price/limit.
    // Program ID: ComputeBudget111111111111111111111111111111
    // We need to parse instructions from `vtx`.
    let mut priority_fee_micro_lamports = 0u64;
    let compute_budget_program_id =
        Pubkey::from_str("ComputeBudget111111111111111111111111111111").unwrap();

    // We can iterate over instructions in `vtx.message.instructions()`
    // `vtx` is `VersionedTransaction`.
    // But iterating `vtx` directly is raw compiled instructions. We need account keys.
    // We already do a full sanitization below for `proto_tx`.
    // Maybe we can check the sanitized tx or just iterate basic structure.
    // Let's use the SanitizedTransaction we create below, it has easy access methods.

    // ... Copying existing code ...
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

    // Extract Signer (Fee Payer)
    let fee_payer = sanitized.message().fee_payer().to_string();

    // Scan instructions for ComputeBudget
    // SanitizedTransaction has `instructions()`.
    for (program_id, _ix) in sanitized.message().program_instructions_iter() {
        if program_id == &compute_budget_program_id {
            // We need to parse instruction data.
            // ComputeBudget instruction format:
            // 0: RequestUnits(units, additional_fee) [Deprecated]
            // 1: RequestHeapFrame(bytes)
            // 2: SetComputeUnitLimit(units)
            // 3: SetComputeUnitPrice(micro_lamports)

            if _ix.data.len() >= 9 && _ix.data[0] == 3 {
                // SetComputeUnitPrice
                let val_bytes: [u8; 8] = _ix.data[1..9].try_into().unwrap_or([0; 8]);
                priority_fee_micro_lamports = u64::from_le_bytes(val_bytes);
            }
        }
    }

    // Update Redis Stats
    // "buy_exact is considered a normal buy" -> user said "save pumpfun buys (buy_exact is considered a normal buy) and sells".
    // I should only record stats IF it involves pumpfun?
    // The user said: "I'll use websockets to detect and save pumpfun buys... and sells on pumpfun... I need help in THIS: 1 - save the aggregated data in redis."
    // It implies I should filter for Pumpfun transactions.
    // The existing code has `if PUMP_ID.to_bytes() == ix.program.into_bytes()` check.

    let mut is_pump_activity = false;

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
            match pumpfun_ix_parser.parse(ix).await {
                Ok(f) => match f {
                    PumpProgramIx::Buy(_, _) | PumpProgramIx::Sell(_, _) => {
                        is_pump_activity = true;
                    }
                    _ => {}
                },
                Err(_) => {}
            }
        }
    }

    if is_pump_activity {
        // Fire and forget (or log error)
        if let Err(e) = redis
            .update_stats(
                &fee_payer,
                success,
                priority_fee_micro_lamports,
                compute_units,
                current_ts,
            )
            .await
        {
            warn!("Failed to update redis stats for {}: {:?}", fee_payer, e);
        }
    }

    // Existing logging logic...
    for ix in &ixs {
        if PUMP_ID.to_bytes() == ix.program.into_bytes() {
            match pumpfun_ix_parser.parse(ix).await {
                Ok(f) => match f {
                    PumpProgramIx::Buy(_, data) => {
                        info!("buy - amount: {:?}", data.amount);
                    }
                    PumpProgramIx::Sell(_, data) => {
                        info!("sell - amount: {:?}", data.amount);
                    }
                    PumpProgramIx::CollectCreatorFee(_) => {
                        info!("collect creator fee");
                    }
                    PumpProgramIx::Create(_, data) => {
                        info!("create - name: {:?}", data.name);
                    }
                    PumpProgramIx::ExtendAccount(_) => {
                        info!("extend account");
                    }
                    PumpProgramIx::Initialize(_) => {
                        info!("initialize");
                    }
                    PumpProgramIx::Migrate(_) => {
                        info!("migrate");
                    }
                    PumpProgramIx::SetCreator(_, _) => {
                        info!("set creator");
                    }
                    PumpProgramIx::SetMetaplexCreator(_) => {
                        info!("set metaplex creator");
                    }
                    PumpProgramIx::SetParams(_, _) => {
                        info!("set params");
                    }
                    PumpProgramIx::UpdateGlobalAuthority(_) => {
                        info!("update global authority");
                    }
                    _ => {
                        info!("other pump instruction: {:?}", f);
                    }
                },
                Err(e) => {
                    warn!("Signature: {:?}", tx_signature);
                    warn!("failed to parse pumpfun ix: {:?}", e);
                }
            }
        } else {
            // decide what to do with other instructions
        }
    }
    Ok(())
}
// Add FromStr for Pubkey if not in scope
use std::str::FromStr;
