use crate::model::{IntoTransactionStatusMeta, TxNotification};
use anyhow::{Context, Result};
use serde::Serialize;
use solana_message::SimpleAddressLoader;
use solana_message::v0::LoadedAddresses;
use solana_pubkey::Pubkey;
use solana_transaction::sanitized::{MessageHash, SanitizedTransaction};
use std::collections::HashSet;
use std::str::FromStr;
use yellowstone_grpc_proto::convert_to;

#[derive(Debug, Clone, Serialize)]
pub enum PlatformActivity {
    Buy {
        amount: u64, // Actual SOL executed
        mint: String,
        ix_name: String,
        slippage_percent: Option<f64>,
        limit_amount: u64, // max_sol (Standard Buy) or min_tokens (BuyExactSolIn)
        requested_amount: u64, // requested_tokens (Standard Buy) or spendable_sol (BuyExactSolIn)
    },
    Sell {
        amount: u64, // Actual SOL executed
        mint: String,
        ix_name: String,
        slippage_percent: Option<f64>,
        requested_token_amount: u64,
        limit_sol_amount: u64,
    },
    Other {
        ix_name: String,
        mint: Option<String>,
    },
}

#[derive(Serialize)]
pub struct TxLabels {
    pub signature: String,
    pub fee_payer: String,
    /// for some reason, setting failed = true in the config
    /// seems to be interpreted by Helius geyser websocket as
    /// "send only unsuccessfull transactions"
    /// which is not what their docs say
    pub success: bool,
    pub priority_fee: u64,
    pub compute_units: u64,
    pub current_ts: u64,
    pub jito_tip: u64,
    pub block: u64,
    pub activities: Vec<PlatformActivity>,
}

const JITO_TIP_ACCOUNTS: [&str; 8] = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
];

pub fn extract_labels(tx: &TxNotification) -> Result<(TxLabels, SanitizedTransaction)> {
    let tx_params = tx
        .params
        .as_ref()
        .context("Missing transaction parameters")?;
    let tx_envelope = &tx_params.result.transaction;
    let tx_signature = &tx_params.result.signature;

    let current_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let vtx = tx_envelope
        .transaction
        .decode()
        .with_context(|| format!("failed to decode transaction: sig={}", tx_signature))?;

    let proto_meta = convert_to::create_transaction_meta(
        &tx_envelope.meta.clone().into_transaction_status_meta(),
    );

    let success = tx_envelope.meta.err.is_none();
    let compute_units = tx_envelope.meta.compute_units_consumed.clone().unwrap_or(0);

    let load_addresses = LoadedAddresses {
        writable: proto_meta
            .loaded_writable_addresses
            .iter()
            .map(|addr| {
                let bytes: [u8; 32] = addr
                    .as_slice()
                    .try_into()
                    .context("invalid pubkey length")?;
                Ok(Pubkey::new_from_array(bytes))
            })
            .collect::<Result<Vec<_>>>()?,
        readonly: proto_meta
            .loaded_readonly_addresses
            .iter()
            .map(|addr| {
                let bytes: [u8; 32] = addr
                    .as_slice()
                    .try_into()
                    .context("invalid pubkey length")?;
                Ok(Pubkey::new_from_array(bytes))
            })
            .collect::<Result<Vec<_>>>()?,
    };

    let sanitized = SanitizedTransaction::try_create(
        vtx,
        MessageHash::Compute,
        Some(false),
        SimpleAddressLoader::Enabled(load_addresses),
        &HashSet::new(),
    )?;

    let fee_payer = sanitized.message().fee_payer().to_string();

    let mut priority_fee = 0u64;
    let mut jito_tip = 0u64;
    let compute_budget_pkey =
        Pubkey::from_str("ComputeBudget111111111111111111111111111111").unwrap();
    let system_program_pkey = Pubkey::from_str("11111111111111111111111111111111").unwrap();

    let account_keys = sanitized.message().account_keys();

    for (program_id, ix) in sanitized.message().program_instructions_iter() {
        // set compute unit price ix
        if program_id == &compute_budget_pkey && ix.data.len() >= 9 && ix.data[0] == 3 {
            priority_fee = ix.data[1..9]
                .try_into()
                .map(u64::from_le_bytes)
                .unwrap_or(0);
        }

        if program_id == &system_program_pkey && ix.data.len() >= 12 && ix.data[0] == 2 {
            // System Program Transfer: [2, 0, 0, 0, lamports(8)]
            if let Some(dest_pubkey) = ix
                .accounts
                .get(1)
                .and_then(|idx| account_keys.get(*idx as usize))
            {
                let dest_str = dest_pubkey.to_string();
                if JITO_TIP_ACCOUNTS.contains(&dest_str.as_str()) {
                    let amount = ix.data[4..12]
                        .try_into()
                        .map(u64::from_le_bytes)
                        .unwrap_or(0);
                    jito_tip += amount;
                }
            }
        }
    }

    Ok((
        TxLabels {
            signature: tx_signature.to_string(),
            fee_payer,
            success,
            priority_fee,
            compute_units,
            current_ts,
            jito_tip,
            block: tx_params.result.slot,
            activities: vec![],
        },
        sanitized,
    ))
}
