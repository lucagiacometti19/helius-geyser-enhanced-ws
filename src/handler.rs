use crate::model::ParsedTx;
use tracing::info;

/// Replace this with your own business logic.
/// Runs on a dedicated Tokio task, fed by a bounded channel.
pub async fn handle_parsed_tx(tx: ParsedTx) {
    //info!(sig = tx.signature.as_str(), slot = tx.slot);
    // … do your low‑latency work here …
}
