use pump_parser_gen::ID as PUMP_ID;
use pump_parser_gen::instructions_parser::{InstructionParser as pumpfun_ix_parser, PumpProgramIx};
use yellowstone_vixen_core::Parser;
use yellowstone_vixen_core::instruction::InstructionUpdate;

use crate::handlers::common::PlatformActivity;

pub async fn parse_pump_fun_ix(ix: &InstructionUpdate) -> Option<PlatformActivity> {
    if PUMP_ID.to_bytes() == ix.program.into_bytes() {
        match pumpfun_ix_parser.parse(ix).await {
            Ok(f) => match f {
                PumpProgramIx::Buy(_, data) => Some(PlatformActivity::Buy {
                    amount: data.amount,
                }),
                PumpProgramIx::BuyExactSolIn(_, data) => Some(PlatformActivity::Buy {
                    amount: data.spendable_sol_in,
                }),
                PumpProgramIx::Sell(_, data) => Some(PlatformActivity::Sell {
                    amount: data.amount,
                }),
                _ => Some(PlatformActivity::Other),
            },
            Err(_) => None,
        }
    } else {
        None
    }
}
