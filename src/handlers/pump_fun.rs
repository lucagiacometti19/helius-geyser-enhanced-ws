use tracing::error;
use yellowstone_vixen_core::Parser;
use yellowstone_vixen_core::instruction::InstructionUpdate;
use yellowstone_vixen_pumpfun_parser::ID as PUMP_ID;
use yellowstone_vixen_pumpfun_parser::events_parser::{
    EventParser as pumpfun_event_parser, PumpProgramEvent,
};
use yellowstone_vixen_pumpfun_parser::instructions_parser::{
    InstructionParser as pumpfun_ix_parser, PumpProgramIx,
};
use yellowstone_vixen_pumpfun_parser::types::TradeEvent;

use crate::handlers::common::PlatformActivity;

pub async fn parse_pump_fun_event(ix: &InstructionUpdate) -> Option<TradeEvent> {
    if ix.program.into_bytes() != PUMP_ID.to_bytes() {
        return None;
    }

    match pumpfun_event_parser.parse(ix).await {
        Ok(event_parsed) => match event_parsed {
            PumpProgramEvent::TradeEvent(event) => Some(event),
            _ => None,
        },
        Err(yellowstone_vixen_core::ParseError::Other(e)) => {
            error!(error = %e, "Failed to parse PumpFun event");
            None
        }
        Err(yellowstone_vixen_core::ParseError::Filtered) => None,
    }
}

pub async fn parse_pump_fun_ix(ix: &InstructionUpdate) -> Option<PlatformActivity> {
    // Check program ID again to be safe, though parser usually checks it too
    if ix.program.into_bytes() != PUMP_ID.to_bytes() {
        return None;
    }

    match pumpfun_ix_parser.parse(ix).await {
        Ok(ix_parsed) => match ix_parsed {
            PumpProgramIx::Buy(accounts, data) => {
                Some(PlatformActivity::Buy {
                    amount: data.max_sol_cost, // Limit amount, will be refined by event
                    mint: accounts.mint.to_string(),
                    ix_name: "buy".to_string(),
                    slippage_percent: None,
                })
            }
            PumpProgramIx::Sell(accounts, data) => {
                Some(PlatformActivity::Sell {
                    amount: data.min_sol_output, // Limit amount, will be refined by event
                    mint: accounts.mint.to_string(),
                    ix_name: "sell".to_string(),
                    slippage_percent: None,
                })
            }
            PumpProgramIx::BuyExactSolIn(accounts, data) => {
                Some(PlatformActivity::Buy {
                    amount: data.spendable_sol_in, // Exact SOL input
                    mint: accounts.mint.to_string(),
                    ix_name: "buy_exact_sol_in".to_string(),
                    slippage_percent: None,
                })
            }
            PumpProgramIx::Create(accounts, _) => Some(PlatformActivity::Other {
                ix_name: "create".to_string(),
                mint: Some(accounts.mint.to_string()),
            }),
            _ => Some(PlatformActivity::Other {
                ix_name: "other_pump_ix".to_string(),
                mint: None,
            }),
        },
        Err(_) => None,
    }
}
