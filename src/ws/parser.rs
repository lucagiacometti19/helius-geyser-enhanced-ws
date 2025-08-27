use std::time::Instant;

use serde_json::Value;

use crate::model::{CompactIx, ParsedTx, TxResult};

/// Build a `ParsedTx` from a raw `TxResult`.
pub(crate) fn build_parsed_tx(result: TxResult) -> ParsedTx {
    let t_start = Instant::now();
    let tx_root = &result.transaction.transaction;
    // meta can live on either level depending on payload variant
    let meta = result
        .transaction
        .transaction
        .get("meta")
        .or(result.transaction.meta.as_ref());

    let log_messages = extract_log_messages(meta);
    let (outer_ix, inner_ix) = extract_all_instructions(tx_root, meta);

    ParsedTx {
        signature: result.signature,
        slot: result.slot,
        log_messages,
        instructions: outer_ix,
        inner_instructions: inner_ix,
        parsedtx_time: Some(t_start.elapsed().as_nanos() as u64),
        serde_time: None,
        queue_start: None,
    }
}

fn extract_log_messages(meta: Option<&Value>) -> Vec<String> {
    meta.and_then(|m| m.get("logMessages"))
        .and_then(|v| v.as_array())
        .map_or_else(Vec::new, |arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
}

fn extract_all_instructions(
    tx_root: &Value,
    meta: Option<&Value>,
) -> (Vec<CompactIx>, Vec<CompactIx>) {
    // Try transaction.message.instructions
    let outer_ix = tx_root
        .get("transaction")
        .and_then(|t| t.get("message"))
        .and_then(|m| m.get("instructions"))
        .or_else(|| tx_root.get("message").and_then(|m| m.get("instructions")))
        .and_then(|v| v.as_array())
        .map_or_else(Vec::new, |arr| {
            arr.iter()
                .filter_map(|ix| to_compact_ix(ix, false))
                .collect()
        });

    // Inner instructions in meta.innerInstructions[*].instructions
    let inner_ix = meta
        .and_then(|m| m.get("innerInstructions"))
        .and_then(|v| v.as_array())
        .map(|outer| {
            outer
                .iter()
                .flat_map(|entry| {
                    entry
                        .get("instructions")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|ix| to_compact_ix(ix, true))
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    (outer_ix, inner_ix)
}

fn to_compact_ix(ix: &Value, is_inner: bool) -> Option<CompactIx> {
    let program_id = ix
        .get("programId")
        .and_then(|v| v.as_str())
        .map_or_else(|| "unknown".to_string(), str::to_owned);

    let accounts = ix
        .get("accounts")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|a| {
                    if let Some(s) = a.as_str() {
                        Some(s.to_string())
                    } else if let Some(obj) = a.as_object() {
                        obj.get("pubkey").and_then(|v| v.as_str()).map(String::from)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let parsed = ix.get("parsed").cloned();
    let data_b64 = ix.get("data").and_then(|v| v.as_str()).map(String::from);

    Some(CompactIx {
        program_id,
        accounts,
        parsed,
        data_b64,
        is_inner,
    })
}
