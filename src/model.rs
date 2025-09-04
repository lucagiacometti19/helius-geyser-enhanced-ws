use std::str::FromStr;

use serde::Deserialize;
use solana_message::compiled_instruction::CompiledInstruction;
use solana_message::v0::LoadedAddresses;
use solana_pubkey::Pubkey;
use solana_transaction_context::TransactionReturnData;
use solana_transaction_error::TransactionError;
use solana_transaction_status::{
    EncodedTransaction, InnerInstruction, InnerInstructions, TransactionStatusMeta,
    TransactionTokenBalance, UiInnerInstructions, UiInstruction, UiLoadedAddresses,
    UiTransactionStatusMeta,
};
/// Outer WS envelope. We keep it minimal for speed and forward-compat.
#[derive(Debug, Deserialize)]
pub(crate) struct TxNotification {
    pub(crate) method: Option<String>,
    pub(crate) params: Option<Params>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Params {
    pub(crate) subscription: Option<u64>,
    pub(crate) result: TxNotificationResult,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TxNotificationResult {
    pub(crate) signature: String,
    pub(crate) slot: u64,
    pub(crate) transaction: TxEnvelope,
}

#[derive(Debug, Deserialize)]
pub struct TxEnvelope {
    pub transaction: EncodedTransaction,
    pub meta: UiTransactionStatusMeta,
}

pub trait IntoTransactionStatusMeta {
    fn into_transaction_status_meta(self) -> TransactionStatusMeta;
}

impl IntoTransactionStatusMeta for UiTransactionStatusMeta {
    fn into_transaction_status_meta(self) -> TransactionStatusMeta {
        // Convert error to status
        let status = if let Some(err) = self.err {
            Err(TransactionError::from(err))
        } else {
            Ok(())
        };
        // Convert loaded addresses
        let UiLoadedAddresses { writable, readonly } = self.loaded_addresses.unwrap();
        let loaded_addresses = LoadedAddresses {
            writable: writable
                .iter()
                .filter_map(|addr| Pubkey::from_str(addr).ok())
                .collect(),
            readonly: readonly
                .iter()
                .filter_map(|addr| Pubkey::from_str(addr).ok())
                .collect(),
        };

        TransactionStatusMeta {
            status,
            fee: self.fee,
            pre_balances: self.pre_balances,
            post_balances: self.post_balances,
            inner_instructions: self.inner_instructions.map(|instructions| {
                // Convert inner instructions - simplified
                instructions
                    .into_iter()
                    .map(|ui_inner| ui_inner.into_inner_instructions())
                    .collect()
            }),
            log_messages: self.log_messages.into(),
            pre_token_balances: self.pre_token_balances.map(|balances| {
                balances
                    .into_iter()
                    .map(|ui_balance| TransactionTokenBalance {
                        account_index: ui_balance.account_index,
                        mint: ui_balance.mint,
                        ui_token_amount: ui_balance.ui_token_amount,
                        owner: ui_balance.owner.unwrap(),
                        program_id: ui_balance.program_id.unwrap(),
                    })
                    .collect()
            }),
            post_token_balances: self.post_token_balances.map(|balances| {
                balances
                    .into_iter()
                    .map(|ui_balance| TransactionTokenBalance {
                        account_index: ui_balance.account_index,
                        mint: ui_balance.mint,
                        ui_token_amount: ui_balance.ui_token_amount,
                        owner: ui_balance.owner.unwrap(),
                        program_id: ui_balance.program_id.unwrap(),
                    })
                    .collect()
            }),
            rewards: self.rewards.into(),
            loaded_addresses,
            return_data: self.return_data.map(|rd| TransactionReturnData {
                program_id: Pubkey::from_str(&rd.program_id).unwrap(),
                data: base64::decode(&rd.data.0).unwrap(),
            }),
            compute_units_consumed: self.compute_units_consumed.into(),
        }
    }
}

pub trait IntoInnerInstructions {
    fn into_inner_instructions(self) -> InnerInstructions;
}

impl IntoInnerInstructions for UiInnerInstructions {
    fn into_inner_instructions(self) -> InnerInstructions {
        InnerInstructions {
            index: self.index,
            instructions: self
                .instructions
                .into_iter()
                .map(|ui_inst| ui_inst.into_inner_instruction())
                .collect(),
        }
    }
}

pub trait IntoInnerInstruction {
    fn into_inner_instruction(self) -> InnerInstruction;
}

impl IntoInnerInstruction for UiInstruction {
    fn into_inner_instruction(self) -> InnerInstruction {
        match self {
            Self::Compiled(ui_ci) => InnerInstruction {
                instruction: CompiledInstruction {
                    program_id_index: ui_ci.program_id_index,
                    accounts: ui_ci.accounts,
                    data: ui_ci.data.into(),
                },
                stack_height: ui_ci.stack_height,
            },
            Self::Parsed(_) => {
                panic!("Parsed instructions are not supported");
            }
        }
    }
}
