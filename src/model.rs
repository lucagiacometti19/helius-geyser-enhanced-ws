use std::time::Instant;

use serde::Deserialize;
use serde_json::Value;

/// Outer WS envelope. We keep it minimal for speed and forward-compat.
#[derive(Debug, Deserialize)]
pub(crate) struct TxNotification {
    pub(crate) method: Option<String>,
    pub(crate) params: Option<Params>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Params {
    pub(crate) subscription: Option<u64>,
    pub(crate) result: TxResult,
}

#[derive(Debug, Deserialize)]
pub(crate) struct TxResult {
    pub(crate) signature: String,
    pub(crate) slot: u64,
    pub(crate) transaction: TransactionEnvelope,
}

/// The Helius payload (with `transactionDetails: "full", encoding: "jsonParsed"`).
/// We leave internal fields as `serde_json::Value` to avoid tight coupling and
/// keep parsing cost low.
#[derive(Debug, Deserialize)]
pub(crate) struct TransactionEnvelope {
    pub(crate) transaction: Value,
    pub(crate) meta: Option<Value>,
}

/// Compact instruction form.
/// Works for both `jsonParsed` and `base64` styles.
#[derive(Debug, Clone)]
pub(crate) struct CompactIx {
    pub(crate) program_id: String,
    pub(crate) accounts: Vec<String>, // account keys (resolved to base58 if present)
    /// If encoding=jsonParsed, Helius puts a `parsed` object; otherwise `data` (base64).
    pub(crate) parsed: Option<Value>, // keep as Value for flexibility/speed
    pub(crate) data_b64: Option<String>, // only when not parsed (kept for completeness)
    pub(crate) is_inner: bool,        // mark inner vs top-level
}

/// A light view extracted from the raw envelope.
#[derive(Debug, Clone)]
pub(crate) struct ParsedTx {
    pub(crate) signature: String,
    pub(crate) slot: u64,
    pub(crate) log_messages: Vec<String>,
    pub(crate) instructions: Vec<CompactIx>,
    pub(crate) inner_instructions: Vec<CompactIx>,
    pub(crate) serde_time: Option<u64>,
    pub(crate) parsedtx_time: Option<u64>,
    pub(crate) queue_start: Option<Instant>,
}

impl ParsedTx {
    // Parsing/construction moved to `ws::parser::build_parsed_tx`.

    pub(crate) fn set_serde_time(mut self, json_parse_duration: u64) -> Self {
        self.serde_time = Some(json_parse_duration);
        self
    }

    pub(crate) fn start_queue_timer(mut self) -> Self {
        self.queue_start = Some(Instant::now());
        self
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
    // The structure we expect under jsonParsed from Helius:
    // transaction: { message: { instructions: [...] }, ... }
    // meta: { innerInstructions: [ { index, instructions: [...] }, ... ] }
    // But we defensively probe a few shapes.

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

/// Convert a jsonParsed instruction (or raw) to CompactIx.
/// Handles both forms seen in Solana JSON:
///  - parsed-form: { "programId": "...", "program": "...", "parsed": {...}, "accounts": [...] }
///  - raw-form:    { "programId": "...", "accounts": [...], "data": "base64-..." }
///
/// Current implementation never returns None, but keep the Option for future-proofing.
fn to_compact_ix(ix: &Value, is_inner: bool) -> Option<CompactIx> {
    let program_id = ix
        .get("programId")
        .and_then(|v| v.as_str())
        .map_or_else(|| "unknown".to_string(), str::to_owned);

    // accounts can be array of account keys or indices; in jsonParsed, usually keys.
    let accounts = ix
        .get("accounts")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|a| {
                    if let Some(s) = a.as_str() {
                        Some(s.to_string())
                    } else if let Some(obj) = a.as_object() {
                        // Some providers return objects with `pubkey`
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
