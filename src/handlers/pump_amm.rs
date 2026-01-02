use crate::handlers::common::PlatformActivity;
use yellowstone_vixen_core::instruction::InstructionUpdate;

pub async fn parse_pump_amm_ix(ix: &InstructionUpdate) -> Option<PlatformActivity> {
    todo!();
    // Placeholder for PumpAMM Program ID and parsing logic
    let pump_amm_id = [0u8; 32]; // @todo: replace with real ID
    if ix.program.into_bytes() == pump_amm_id {
        Some(PlatformActivity::Other {
            ix_name: "pump_amm_other".to_string(),
            mint: None,
        })
    } else {
        None
    }
}
