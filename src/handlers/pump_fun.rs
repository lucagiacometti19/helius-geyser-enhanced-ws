use tracing::{error, warn};
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

pub fn reconcile_pump_activities(activities: &mut [PlatformActivity], events: Vec<TradeEvent>) {
    let mut unmatched_events = events;

    for act in activities.iter_mut() {
        match act {
            PlatformActivity::Buy {
                mint,
                amount,
                slippage_percent,
                limit_amount,
                requested_amount,
                ix_name,
                ..
            } => {
                if let Some(pos) = unmatched_events
                    .iter()
                    .position(|e| e.is_buy && e.mint.to_string() == *mint)
                {
                    let event = unmatched_events.remove(pos);
                    *amount = event.sol_amount;

                    // Hardcode 0% slippage for Mayhem AI Agent since can't find info on the bonding curve
                    if event.mayhem_mode {
                        if event.user.to_string() == "BwWK17cbHxwWBKZkUYvzxLcNQ1YVyaFezduWbtm2de6s"
                        {
                            *slippage_percent = Some(0.0);
                        }
                        continue;
                    }

                    // 1. Calculate Spot Price BEFORE
                    // Buy: The curve receives SOL and gives tokens.
                    // sol_amount in the event is the amount swapped on the curve (NET).
                    // We simply subtract the sol_amount and add back the token_amount.
                    let sol_before = event.virtual_sol_reserves.saturating_sub(event.sol_amount);
                    let token_before = event
                        .virtual_token_reserves
                        .saturating_add(event.token_amount);

                    if token_before > 0 {
                        let mut price_before = sol_before as f64 / token_before as f64;

                        // Mayhem Mode Adjustment:
                        // The Mayhem curve has a steeper slope/different initialization that results in
                        // an execution price ~1.25x higher than the raw virtual reserve ratio.
                        // We apply this scalar to align the "Market Price" with reality.
                        if event.mayhem_mode {
                            price_before *= 1.25;
                        }

                        // 2. Calculate Limit Price (Normalized to Curve Price Space)
                        // Market Price (price_before) is the price ON THE CURVE (sol_amount / token_amount).
                        // The user's instruction limit is often set on the TOTAL SOL (with protocol + creator fees).
                        // To compare fairly, we must normalize the limit by the same fee ratio seen in the execution.
                        let limit_price = if *limit_amount > 0 && *requested_amount > 0 {
                            let total_user_sol = event.sol_amount + event.fee + event.creator_fee;
                            let fee_ratio = if total_user_sol > 0 {
                                event.sol_amount as f64 / total_user_sol as f64
                            } else {
                                1.0
                            };

                            if ix_name == "buy" {
                                (*limit_amount as f64 * fee_ratio) / *requested_amount as f64
                            } else {
                                // buy_exact_sol_in -> (spendable_sol * fee_ratio) / min_tokens
                                (*requested_amount as f64 * fee_ratio) / *limit_amount as f64
                            }
                        } else {
                            0.0
                        };

                        // 3. Slippage Percentage (Intent)
                        if price_before > 0.0 {
                            *slippage_percent = Some(
                                (((limit_price - price_before) / price_before) * 100.0).min(100.0),
                            )
                        } else {
                            // @todo: does this ever happen?
                            *slippage_percent = None
                        };
                    }
                }
            }
            PlatformActivity::Sell {
                mint,
                amount,
                slippage_percent,
                requested_token_amount,
                limit_sol_amount,
                ..
            } => {
                if let Some(pos) = unmatched_events
                    .iter()
                    .position(|e| !e.is_buy && e.mint.to_string() == *mint)
                {
                    let event = unmatched_events.remove(pos);
                    *amount = event.sol_amount;

                    // Hardcode 0% slippage for Mayhem AI Agent since can't find info on the bonding curve
                    if event.mayhem_mode
                        && event.user.to_string() == "BwWK17cbHxwWBKZkUYvzxLcNQ1YVyaFezduWbtm2de6s"
                    {
                        *slippage_percent = Some(0.0);
                        continue;
                    }

                    // 1. Spot Price BEFORE
                    // Sell: The curve gives SOL and receives tokens.
                    // To find state BEFORE, we add the sol_amount back and subtract the token_amount.
                    // Note: sol_amount in the event is the amount swapped on the curve (GROSS before user fee).
                    let sol_before = event.virtual_sol_reserves.saturating_add(event.sol_amount);
                    let token_before = event
                        .virtual_token_reserves
                        .saturating_sub(event.token_amount);

                    if token_before > 0 {
                        let mut price_before = sol_before as f64 / token_before as f64;

                        // Mayhem Mode Adjustment
                        if event.mayhem_mode {
                            price_before *= 1.25;
                        }

                        // 2. Calculate Limit Price (Normalized to Curve Price Space)
                        // For Sells, the User Limit (min_sol_output) is what remains AFTER fees.
                        // The Curve Price (price_before) is the price BEFORE fees.
                        // Ratio = (What Curve Produced) / (What User Received)
                        let limit_price = if *requested_token_amount > 0 {
                            let sol_to_user = event
                                .sol_amount
                                .saturating_sub(event.fee)
                                .saturating_sub(event.creator_fee);
                            let fee_ratio = if sol_to_user > 0 {
                                event.sol_amount as f64 / sol_to_user as f64
                            } else {
                                1.0
                            };
                            (*limit_sol_amount as f64 * fee_ratio) / *requested_token_amount as f64
                        } else {
                            0.0
                        };

                        // 3. Slippage Percentage (Intent)
                        if price_before > 0.0 {
                            *slippage_percent =
                                Some(((price_before - limit_price) / price_before) * 100.0);
                        }
                    }
                } else {
                    warn!("No matching event found for sell activity: {:#?}", act);
                }
            }
            PlatformActivity::Other { .. } => {}
        }
    }
}

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
    if ix.program.into_bytes() != PUMP_ID.to_bytes() {
        return None;
    }

    match pumpfun_ix_parser.parse(ix).await {
        Ok(ix_parsed) => match ix_parsed {
            PumpProgramIx::Buy(accounts, data) => Some(PlatformActivity::Buy {
                amount: data.max_sol_cost, // Placeholder
                mint: accounts.mint.to_string(),
                ix_name: "buy".to_string(),
                slippage_percent: None,
                limit_amount: data.max_sol_cost,
                requested_amount: data.amount,
            }),
            PumpProgramIx::Sell(accounts, data) => Some(PlatformActivity::Sell {
                amount: 0, // Placeholder
                mint: accounts.mint.to_string(),
                ix_name: "sell".to_string(),
                slippage_percent: None,
                requested_token_amount: data.amount,
                limit_sol_amount: data.min_sol_output,
            }),
            PumpProgramIx::BuyExactSolIn(accounts, data) => Some(PlatformActivity::Buy {
                amount: data.spendable_sol_in, // Placeholder
                mint: accounts.mint.to_string(),
                ix_name: "buy_exact_sol_in".to_string(),
                slippage_percent: None,
                limit_amount: data.min_tokens_out,
                requested_amount: data.spendable_sol_in,
            }),
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
