use std::str::FromStr as _;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};

use super::{push_gap, PortfolioAccountingGap, PortfolioState};
use crate::polymarket::discovery::TokenLifecycleState;
use crate::polymarket::fixed_math::FIXED_8DP_SCALE;
use crate::polymarket::state::{parse_token_lifecycle_state, PolymarketOddsState};
use crate::trade_session_ledger::{
    CanonicalCohortCashflow, CanonicalCtfEvent, ConfirmedCashflowKind,
    ConfirmedPortfolioReceiptIdentity,
};

pub(super) struct TokenProjection {
    pub(super) symbol: String,
    pub(super) token: String,
    pub(super) condition_id: String,
    pub(super) qty_units_6dp: u128,
    pub(super) remaining_basis_units_6dp: u128,
    pub(super) display_mark_price_units_8dp: Option<u64>,
    pub(super) display_open_pnl_units_6dp: Option<i128>,
    pub(super) display_valued_at_ms: Option<u64>,
}

pub(super) struct CohortProjection {
    pub(super) marked_units_6dp: u128,
    pub(super) open_units_6dp: i128,
    pub(super) tokens: [TokenProjection; 2],
}

pub(super) fn project_cohort(
    symbol: &str,
    cashflow: &CanonicalCohortCashflow,
    odds: Option<&PolymarketOddsState>,
    valued_at_ms: u64,
) -> Result<CohortProjection> {
    let (up_weight, down_weight) = if let Some(authorization) = cashflow.buy_authorization.as_ref()
    {
        authorization.validate()?;
        anyhow::ensure!(
            authorization.symbol.as_str() == symbol
                && authorization.condition_id.to_string() == cashflow.condition_id
                && authorization.up_token.to_string() == cashflow.up_token
                && authorization.down_token.to_string() == cashflow.down_token
                && cashflow.events.iter().all(|event| match &event.event {
                    CanonicalCtfEvent::BuyConfirmed(fact) => fact.authorization == *authorization,
                    CanonicalCtfEvent::SplitConfirmed(_) => false,
                    _ => true,
                }),
            "open BUY cohort authorization differs from canonical entry"
        );
        anyhow::ensure!(
            if authorization.selected_up {
                cashflow.up_created_qty_units_6dp > 0 && cashflow.down_created_qty_units_6dp == 0
            } else {
                cashflow.down_created_qty_units_6dp > 0 && cashflow.up_created_qty_units_6dp == 0
            },
            "open BUY cohort contains unauthorized acquisition side"
        );
        if authorization.selected_up {
            (1, 0)
        } else {
            (0, 1)
        }
    } else {
        let splits = cashflow
            .events
            .iter()
            .filter_map(|event| match &event.event {
                CanonicalCtfEvent::SplitConfirmed(split) => Some(split),
                _ => None,
            })
            .collect::<Vec<_>>();
        anyhow::ensure!(splits.len() == 1, "open cohort has no unique split fact");
        let split = splits[0];
        anyhow::ensure!(
            split.underlying == symbol
                && split.condition_id == cashflow.condition_id
                && split.up_token == cashflow.up_token
                && split.down_token == cashflow.down_token
                && split.up_qty_units_6dp == cashflow.up_created_qty_units_6dp
                && split.down_qty_units_6dp == cashflow.down_created_qty_units_6dp
                && split.collateral_debit_units_6dp == cashflow.split_principal_units_6dp
                && split.fee_units_6dp == cashflow.split_fee_units_6dp
                && split.approved_direct_cost_units_6dp
                    == cashflow.split_approved_direct_cost_units_6dp,
            "open cohort split identity/economics mismatch"
        );
        let weights = split
            .up_cost_basis_weight_units_8dp
            .zip(split.down_cost_basis_weight_units_8dp)
            .context("open cohort split cost-basis weights unavailable")?;
        weights
    };
    let up_remaining = cashflow.up_remaining_qty_units_6dp;
    let down_remaining = cashflow.down_remaining_qty_units_6dp;
    anyhow::ensure!(
        cashflow
            .up_confirmed_disposed_qty_units_6dp
            .checked_add(up_remaining)
            == Some(cashflow.up_created_qty_units_6dp)
            && cashflow
                .down_confirmed_disposed_qty_units_6dp
                .checked_add(down_remaining)
                == Some(cashflow.down_created_qty_units_6dp),
        "open cohort share conservation mismatch"
    );
    let condition_id =
        B256::from_str(&cashflow.condition_id).context("open condition is not bytes32")?;
    let up_token = U256::from_str(&cashflow.up_token).context("UP token is not uint256")?;
    let down_token = U256::from_str(&cashflow.down_token).context("DOWN token is not uint256")?;
    anyhow::ensure!(up_token != down_token, "open binary tokens are identical");
    let winner = match odds.map(|odds| {
        (
            odds.resolved_market_winners.get(&condition_id).copied(),
            odds.resolved_market_contexts.get(&condition_id),
        )
    }) {
        None => None,
        Some((Some(winner), Some(context))) => {
            anyhow::ensure!(
                context.up_token == up_token
                    && context.down_token == down_token
                    && context.observed_at_ms > 0
                    && context.observed_at_ms <= valued_at_ms
                    && (winner == up_token || winner == down_token),
                "resolved binary identity is invalid or noncausal"
            );
            Some(winner)
        }
        Some((None, None)) => {
            let odds = odds.expect("resolved-state branch has canonical odds");
            let unavailable = [up_token, down_token].into_iter().any(|token| {
                odds.token_lifecycle.get(&token).is_some_and(|raw| {
                    matches!(
                        parse_token_lifecycle_state(raw),
                        TokenLifecycleState::InvalidAfter404 | TokenLifecycleState::Resolved
                    )
                })
            });
            anyhow::ensure!(
                !unavailable,
                "open market is resolved or quarantined without exact payout truth"
            );
            None
        }
        Some(_) => anyhow::bail!("open market has partial terminal identity"),
    };
    let (up_marked, down_marked) = if let Some(winner) = winner {
        (
            if winner == up_token { up_remaining } else { 0 },
            if winner == down_token {
                down_remaining
            } else {
                0
            },
        )
    } else {
        let paired = up_remaining.min(down_remaining);
        let up_residual = up_remaining - paired;
        let down_residual = down_remaining - paired;
        let residual_value = match (up_residual, down_residual) {
            (up, 0) if up > 0 => exact_unresolved_residual_value(
                odds.context("unresolved residual has no canonical market state")?,
                up_token,
                up,
                valued_at_ms,
            )?,
            (0, down) if down > 0 => exact_unresolved_residual_value(
                odds.context("unresolved residual has no canonical market state")?,
                down_token,
                down,
                valued_at_ms,
            )?,
            (0, 0) => 0,
            _ => anyhow::bail!("binary residual is not unique"),
        };
        let (mut up_marked, mut down_marked) =
            crate::ctf_accounting::allocate_units(paired, up_weight, down_weight)?;
        if up_residual > 0 {
            up_marked = up_marked
                .checked_add(residual_value)
                .context("UP marked value overflow")?;
        } else {
            down_marked = down_marked
                .checked_add(residual_value)
                .context("DOWN marked value overflow")?;
        }
        (up_marked, down_marked)
    };
    let marked = up_marked
        .checked_add(down_marked)
        .context("cohort marked value overflow")?;

    let (up_basis, down_basis) = remaining_basis(cashflow, up_weight, down_weight)?;
    let realization = cashflow
        .total_realization()
        .context("cohort receipt-realized PnL unavailable")?;
    let unrecovered = cashflow
        .unrecovered_cash_units_6dp()
        .context("cohort unrecovered cash unavailable")?;
    let remaining_net_basis = unrecovered
        .checked_add(realization.realized_pnl_units_6dp)
        .context("remaining net entry basis overflow")?;
    let projected_basis = if cashflow.buy_authorization.is_some() {
        remaining_net_basis.max(0)
    } else {
        remaining_net_basis
    };
    anyhow::ensure!(
        up_basis
            .checked_add(down_basis)
            .and_then(|basis| i128::try_from(basis).ok())
            == Some(projected_basis),
        "remaining entry basis does not reconcile with canonical cashflow"
    );
    let current = i128::try_from(marked)
        .ok()
        .and_then(|marked| marked.checked_sub(unrecovered))
        .context("cohort current PnL overflow")?;
    let open = current
        .checked_sub(realization.realized_pnl_units_6dp)
        .context("cohort open PnL overflow")?;
    let up_realized = cashflow
        .realization_for_token(&cashflow.up_token)
        .context("UP receipt-realized PnL unavailable")?
        .realized_pnl_units_6dp;
    let down_realized = cashflow
        .realization_for_token(&cashflow.down_token)
        .context("DOWN receipt-realized PnL unavailable")?
        .realized_pnl_units_6dp;
    let up_net_basis = if cashflow
        .buy_authorization
        .as_ref()
        .is_some_and(|entry| entry.selected_up)
    {
        Some(remaining_net_basis)
    } else {
        i128::try_from(up_basis).ok()
    };
    let down_net_basis = if cashflow
        .buy_authorization
        .as_ref()
        .is_some_and(|entry| !entry.selected_up)
    {
        Some(remaining_net_basis)
    } else {
        i128::try_from(down_basis).ok()
    };
    let up_open = i128::try_from(up_marked)
        .ok()
        .zip(up_net_basis)
        .and_then(|(mark, basis)| mark.checked_sub(basis))
        .context("UP open PnL overflow")?;
    let down_open = i128::try_from(down_marked)
        .ok()
        .zip(down_net_basis)
        .and_then(|(mark, basis)| mark.checked_sub(basis))
        .context("DOWN open PnL overflow")?;
    let token_current = up_realized.checked_add(up_open).and_then(|up| {
        down_realized
            .checked_add(down_open)
            .and_then(|down| up.checked_add(down))
    });
    anyhow::ensure!(
        up_marked.checked_add(down_marked) == Some(marked)
            && up_marked <= up_remaining
            && down_marked <= down_remaining
            && up_realized.checked_add(down_realized) == Some(realization.realized_pnl_units_6dp)
            && up_open.checked_add(down_open) == Some(open)
            && token_current == Some(current),
        "token accounting rows do not reconcile with cohort"
    );
    let resolution_observed_at_ms = winner.and_then(|_| {
        odds.and_then(|odds| odds.resolved_market_contexts.get(&condition_id))
            .map(|context| context.observed_at_ms)
    });
    let up_display = display_token_mark(
        odds,
        up_token,
        up_remaining,
        up_basis,
        winner,
        resolution_observed_at_ms,
        valued_at_ms,
    );
    let down_display = display_token_mark(
        odds,
        down_token,
        down_remaining,
        down_basis,
        winner,
        resolution_observed_at_ms,
        valued_at_ms,
    );

    Ok(CohortProjection {
        marked_units_6dp: marked,
        open_units_6dp: open,
        tokens: [
            TokenProjection {
                symbol: symbol.to_string(),
                token: cashflow.up_token.clone(),
                condition_id: cashflow.condition_id.clone(),
                qty_units_6dp: up_remaining,
                remaining_basis_units_6dp: up_basis,
                display_mark_price_units_8dp: up_display.map(|mark| mark.price_units_8dp),
                display_open_pnl_units_6dp: up_display.map(|mark| mark.open_pnl_units_6dp),
                display_valued_at_ms: up_display.map(|mark| mark.valued_at_ms),
            },
            TokenProjection {
                symbol: symbol.to_string(),
                token: cashflow.down_token.clone(),
                condition_id: cashflow.condition_id.clone(),
                qty_units_6dp: down_remaining,
                remaining_basis_units_6dp: down_basis,
                display_mark_price_units_8dp: down_display.map(|mark| mark.price_units_8dp),
                display_open_pnl_units_6dp: down_display.map(|mark| mark.open_pnl_units_6dp),
                display_valued_at_ms: down_display.map(|mark| mark.valued_at_ms),
            },
        ],
    })
}

fn remaining_basis(
    cashflow: &CanonicalCohortCashflow,
    up_weight: u128,
    down_weight: u128,
) -> Result<(u128, u128)> {
    if cashflow.buy_authorization.is_some() {
        return Ok((
            cashflow
                .up_remaining_basis_units_6dp
                .context("BUY UP remaining basis is absent")?,
            cashflow
                .down_remaining_basis_units_6dp
                .context("BUY DOWN remaining basis is absent")?,
        ));
    }
    let mut up_basis = 0u128;
    let mut down_basis = 0u128;
    for total in [
        cashflow.split_principal_units_6dp,
        cashflow.split_fee_units_6dp,
        cashflow.split_approved_direct_cost_units_6dp,
    ] {
        let (up_original, down_original) =
            crate::ctf_accounting::allocate_units(total, up_weight, down_weight)?;
        let (_, up_remaining) = crate::ctf_accounting::allocate_units(
            up_original,
            cashflow.up_confirmed_disposed_qty_units_6dp,
            cashflow.up_remaining_qty_units_6dp,
        )?;
        let (_, down_remaining) = crate::ctf_accounting::allocate_units(
            down_original,
            cashflow.down_confirmed_disposed_qty_units_6dp,
            cashflow.down_remaining_qty_units_6dp,
        )?;
        up_basis = up_basis
            .checked_add(up_remaining)
            .context("UP remaining basis overflow")?;
        down_basis = down_basis
            .checked_add(down_remaining)
            .context("DOWN remaining basis overflow")?;
    }
    Ok((up_basis, down_basis))
}

#[derive(Clone, Copy)]
struct DisplayTokenMark {
    price_units_8dp: u64,
    open_pnl_units_6dp: i128,
    valued_at_ms: u64,
}

fn display_token_mark(
    odds: Option<&PolymarketOddsState>,
    token: U256,
    qty_units_6dp: u128,
    remaining_basis_units_6dp: u128,
    winner: Option<U256>,
    resolution_observed_at_ms: Option<u64>,
    valued_at_ms: u64,
) -> Option<DisplayTokenMark> {
    let (price_units_8dp, marked_units_6dp, mark_valued_at_ms) = match winner {
        Some(winner) => (
            if winner == token { FIXED_8DP_SCALE } else { 0 },
            if winner == token { qty_units_6dp } else { 0 },
            resolution_observed_at_ms?,
        ),
        None => {
            let Some(mark) = odds.and_then(|odds| odds.last_canonical_bid(token)) else {
                return None;
            };
            if mark.book_ts_ms == 0
                || mark.book_ts_ms > valued_at_ms
                || mark.observed_at_ms == 0
                || mark.observed_at_ms > valued_at_ms
            {
                return None;
            }
            let bid = mark.last_canonical_bid_units_8dp;
            if bid == 0 || bid > FIXED_8DP_SCALE {
                return None;
            }
            let marked = crate::ctf_accounting::allocate_units(
                qty_units_6dp,
                u128::from(bid),
                u128::from(FIXED_8DP_SCALE.checked_sub(bid)?),
            )
            .ok()?
            .0;
            (bid, marked, mark.book_ts_ms)
        }
    };
    let marked_signed = i128::try_from(marked_units_6dp).ok()?;
    let basis_signed = i128::try_from(remaining_basis_units_6dp).ok()?;
    let open_pnl_units_6dp = marked_signed.checked_sub(basis_signed)?;
    Some(DisplayTokenMark {
        price_units_8dp,
        open_pnl_units_6dp,
        valued_at_ms: mark_valued_at_ms,
    })
}

fn exact_unresolved_residual_value(
    odds: &PolymarketOddsState,
    token: U256,
    qty_units_6dp: u128,
    valued_at_ms: u64,
) -> Result<u128> {
    let mark = odds
        .last_canonical_bid(token)
        .context("unresolved residual has no retained canonical bid")?;
    anyhow::ensure!(
        mark.book_ts_ms > 0
            && mark.book_ts_ms <= valued_at_ms
            && mark.observed_at_ms > 0
            && mark.observed_at_ms <= valued_at_ms,
        "unresolved residual retained canonical bid is noncausal"
    );
    let bid = mark.last_canonical_bid_units_8dp;
    anyhow::ensure!(
        bid > 0 && bid <= FIXED_8DP_SCALE,
        "unresolved residual retained canonical bid is invalid"
    );
    Ok(crate::ctf_accounting::allocate_units(
        qty_units_6dp,
        u128::from(bid),
        u128::from(
            FIXED_8DP_SCALE
                .checked_sub(bid)
                .context("canonical bid exceeds par")?,
        ),
    )?
    .0)
}

pub(super) fn validate_confirmed_portfolio_receipts(
    portfolio: &PortfolioState,
    receipts: &[ConfirmedPortfolioReceiptIdentity],
    gaps: &mut Vec<PortfolioAccountingGap>,
) {
    if !receipts.windows(2).all(|pair| pair[0] < pair[1]) {
        push_gap(
            gaps,
            "*",
            None,
            "duplicate_confirmed_portfolio_receipt_identity",
        );
    }
    let mut split_count = 0usize;
    let mut buy_count = 0usize;
    let mut sell_count = 0usize;
    let mut terminal_count = 0usize;
    let mut nonterminal_mismatch = false;
    for identity in receipts {
        match identity {
            ConfirmedPortfolioReceiptIdentity::Split {
                cohort_id,
                transaction_id,
            } => {
                split_count += 1;
                nonterminal_mismatch |= portfolio
                    .confirmed_ctf_split_outlays
                    .get(cohort_id)
                    .and_then(crate::portfolio::state::ConfirmedCtfEntryOutlay::split)
                    .is_none_or(|split| split.transaction_id.as_str() != transaction_id.as_str());
            }
            ConfirmedPortfolioReceiptIdentity::Buy { cohort_id, fill_id } => {
                buy_count += 1;
                nonterminal_mismatch |= !portfolio
                    .confirmed_direct_buy_receipts()
                    .any(|fact| fact.cohort_id == *cohort_id && fact.fill_id == *fill_id);
            }
            ConfirmedPortfolioReceiptIdentity::ManagedSell { cohort_id, fill_id } => {
                sell_count += 1;
                nonterminal_mismatch |= !portfolio
                    .confirmed_lifecycle_sell_fills
                    .keys()
                    .any(|(cohort, fill)| cohort == cohort_id && fill == fill_id);
            }
            ConfirmedPortfolioReceiptIdentity::Terminal {
                market_id,
                token,
                transaction_id,
                kind,
            } => {
                terminal_count += 1;
                let close_reason = match kind {
                    ConfirmedCashflowKind::Merge => "merge",
                    ConfirmedCashflowKind::Redemption => "redeem",
                    ConfirmedCashflowKind::Split
                    | ConfirmedCashflowKind::Buy
                    | ConfirmedCashflowKind::ManagedSale => {
                        push_gap(
                            gaps,
                            "*",
                            Some(market_id),
                            "terminal_portfolio_receipt_kind_invalid",
                        );
                        continue;
                    }
                };
                if !portfolio.seen_terminal_close_keys.iter().any(|key| {
                    key.market_id.as_str() == market_id.as_str()
                        && key.token == *token
                        && key.close_reason == close_reason
                        && key.exit_tx_hash == *transaction_id
                }) {
                    push_gap(
                        gaps,
                        "*",
                        Some(market_id),
                        "confirmed_terminal_portfolio_receipt_identity_mismatch",
                    );
                }
            }
        }
    }
    if nonterminal_mismatch
        || split_count
            != portfolio
                .confirmed_ctf_split_outlays
                .values()
                .filter(|entry| entry.split().is_some())
                .count()
        || buy_count != portfolio.confirmed_direct_buy_receipts().count()
        || sell_count != portfolio.confirmed_lifecycle_sell_fills.len()
    {
        push_gap(
            gaps,
            "*",
            None,
            "confirmed_portfolio_receipt_identity_mismatch",
        );
    }
    if terminal_count != portfolio.seen_terminal_close_keys.len() {
        push_gap(
            gaps,
            "*",
            None,
            "confirmed_terminal_portfolio_receipt_count_mismatch",
        );
    }
}
