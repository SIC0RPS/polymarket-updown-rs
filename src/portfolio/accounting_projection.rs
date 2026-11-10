use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::polymarket::fixed_math::FIXED_8DP_SCALE;
use crate::polymarket::state::PolymarketOddsState;
use crate::trade_session_ledger::{
    CanonicalFinancialView, CanonicalUnresolvedCohortState, ConfirmedPortfolioReceiptIdentity,
};

use super::PortfolioState;

mod cohort;
use cohort::{project_cohort, validate_confirmed_portfolio_receipts, TokenProjection};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PortfolioAccountingPnlRow {
    pub symbol: String,
    #[serde(with = "crate::trade_session_ledger::exact_serde::optional_u128_string")]
    pub marked_remaining_value_units_6dp: Option<u128>,
    #[serde(with = "crate::trade_session_ledger::exact_serde::optional_i128_string")]
    pub receipt_realized_pnl_units_6dp: Option<i128>,
    #[serde(with = "crate::trade_session_ledger::exact_serde::optional_i128_string")]
    pub open_pnl_units_6dp: Option<i128>,
    #[serde(with = "crate::trade_session_ledger::exact_serde::optional_i128_string")]
    pub total_pnl_units_6dp: Option<i128>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PortfolioAccountingGap {
    pub symbol: String,
    pub condition_id: Option<String>,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PortfolioAccountingProjectionProof {
    pub expected_symbols: Vec<String>,
    pub rows: Vec<PortfolioAccountingPnlRow>,
    pub gaps: Vec<PortfolioAccountingGap>,
    pub portfolio_revision: u64,
    pub portfolio_positions_fingerprint: bbwick_engine::state::Fingerprint,
    pub portfolio_updated_at_ms: u64,
    pub canonical_revision: Option<u64>,
    pub odds_revision: Option<u64>,
    pub odds_updated_at_ms: Option<u64>,
    pub valued_at_ms: u64,
}

impl PortfolioAccountingProjectionProof {
    pub fn row_for_symbol(&self, symbol: &str) -> Option<&PortfolioAccountingPnlRow> {
        self.rows.iter().find(|row| row.symbol == symbol)
    }

    pub fn is_complete(&self) -> bool {
        projection_is_complete(&self.expected_symbols, &self.rows, &self.gaps)
    }

    pub fn is_symbol_complete(&self, symbol: &str) -> bool {
        symbol_projection_is_complete(symbol, &self.rows, &self.gaps)
    }
}

#[derive(Debug, Clone)]
pub struct ExactPortfolioAccountingProjection {
    pub portfolio: PortfolioState,
    pub expected_symbols: Vec<String>,
    pub rows: Vec<PortfolioAccountingPnlRow>,
    pub gaps: Vec<PortfolioAccountingGap>,
    pub portfolio_revision: u64,
    pub portfolio_positions_fingerprint: bbwick_engine::state::Fingerprint,
    pub portfolio_updated_at_ms: u64,
    pub canonical_revision: Option<u64>,
    pub odds_revision: Option<u64>,
    pub odds_updated_at_ms: Option<u64>,
    pub valued_at_ms: u64,
}

impl ExactPortfolioAccountingProjection {
    pub fn row_for_symbol(&self, symbol: &str) -> Option<&PortfolioAccountingPnlRow> {
        self.rows.iter().find(|row| row.symbol == symbol)
    }

    pub fn is_complete(&self) -> bool {
        projection_is_complete(&self.expected_symbols, &self.rows, &self.gaps)
    }

    pub fn is_symbol_complete(&self, symbol: &str) -> bool {
        symbol_projection_is_complete(symbol, &self.rows, &self.gaps)
    }

    pub fn proof(&self) -> PortfolioAccountingProjectionProof {
        PortfolioAccountingProjectionProof {
            expected_symbols: self.expected_symbols.clone(),
            rows: self.rows.clone(),
            gaps: self.gaps.clone(),
            portfolio_revision: self.portfolio_revision,
            portfolio_positions_fingerprint: self.portfolio_positions_fingerprint,
            portfolio_updated_at_ms: self.portfolio_updated_at_ms,
            canonical_revision: self.canonical_revision,
            odds_revision: self.odds_revision,
            odds_updated_at_ms: self.odds_updated_at_ms,
            valued_at_ms: self.valued_at_ms,
        }
    }
}

fn projection_is_complete(
    expected_symbols: &[String],
    rows: &[PortfolioAccountingPnlRow],
    gaps: &[PortfolioAccountingGap],
) -> bool {
    let actual = rows
        .iter()
        .filter(|row| row.symbol != "TOTAL")
        .map(|row| row.symbol.as_str())
        .collect::<BTreeSet<_>>();
    let expected = expected_symbols
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    gaps.is_empty()
        && actual == expected
        && actual.len() == expected_symbols.len()
        && rows.len() == expected_symbols.len() + 1
        && rows.iter().all(|row| {
            row.marked_remaining_value_units_6dp.is_some()
                && row.receipt_realized_pnl_units_6dp.is_some()
                && row.open_pnl_units_6dp.is_some()
                && row.total_pnl_units_6dp.is_some()
        })
}

fn symbol_projection_is_complete(
    symbol: &str,
    rows: &[PortfolioAccountingPnlRow],
    gaps: &[PortfolioAccountingGap],
) -> bool {
    !gaps
        .iter()
        .any(|gap| gap.symbol == "*" || gap.symbol == symbol)
        && rows.iter().any(|row| {
            row.symbol == symbol
                && row.marked_remaining_value_units_6dp.is_some()
                && row.receipt_realized_pnl_units_6dp.is_some()
                && row.open_pnl_units_6dp.is_some()
                && row.total_pnl_units_6dp.is_some()
        })
}

#[derive(Default)]
struct SymbolProjection {
    marked_units_6dp: u128,
    open_units_6dp: i128,
}

impl PortfolioState {
    pub fn canonical_accounting_projection(
        &self,
        financial: Option<CanonicalFinancialView<'_>>,
        confirmed_portfolio_receipts: Option<&[ConfirmedPortfolioReceiptIdentity]>,
        odds: Option<&PolymarketOddsState>,
        valued_at_ms: u64,
    ) -> ExactPortfolioAccountingProjection {
        let mut portfolio = self.clone();
        clear_projection(&mut portfolio);
        let mut projection = ExactPortfolioAccountingProjection {
            portfolio_revision: self.revision,
            portfolio_positions_fingerprint: self.positions_fingerprint.clone(),
            portfolio_updated_at_ms: self.updated_at_ms,
            canonical_revision: financial.map(|financial| financial.revision),
            odds_revision: odds.map(|odds| odds.revision),
            odds_updated_at_ms: odds.map(|odds| odds.updated_at_ms),
            valued_at_ms,
            portfolio,
            expected_symbols: financial.map_or_else(
                || {
                    crate::config::VALID_ASSETS
                        .iter()
                        .map(ToString::to_string)
                        .collect()
                },
                |financial| {
                    financial
                        .ctf_summary
                        .iter()
                        .filter(|row| row.symbol != "TOTAL")
                        .map(|row| row.symbol.clone())
                        .collect()
                },
            ),
            rows: Vec::new(),
            gaps: Vec::new(),
        };
        let Some(financial) = financial else {
            push_gap(
                &mut projection.gaps,
                "*",
                None,
                "canonical_stats_unavailable",
            );
            projection.rows = unavailable_rows(&projection.expected_symbols);
            return projection;
        };
        if let Some(receipts) = confirmed_portfolio_receipts {
            validate_confirmed_portfolio_receipts(self, receipts, &mut projection.gaps);
        }
        let mut symbols = BTreeMap::<String, SymbolProjection>::new();
        let mut token_rows = BTreeMap::<String, TokenProjection>::new();
        for cohort in financial.unresolved_cohorts.iter() {
            let cashflow = match &cohort.state {
                CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete { cashflow } => {
                    cashflow
                }
                CanonicalUnresolvedCohortState::InvalidConfirmedReceiptSet { .. } => {
                    push_gap(
                        &mut projection.gaps,
                        &cohort.symbol,
                        Some(cohort.market_id.as_str()),
                        "invalid_confirmed_receipt_set",
                    );
                    continue;
                }
            };
            match project_cohort(&cohort.symbol, cashflow, odds, valued_at_ms) {
                Ok(projected) => {
                    let symbol = symbols.entry(cohort.symbol.clone()).or_default();
                    match symbol
                        .marked_units_6dp
                        .checked_add(projected.marked_units_6dp)
                    {
                        Some(marked) => symbol.marked_units_6dp = marked,
                        None => push_gap(
                            &mut projection.gaps,
                            &cohort.symbol,
                            Some(&cashflow.condition_id),
                            "symbol_marked_value_overflow",
                        ),
                    }
                    match symbol.open_units_6dp.checked_add(projected.open_units_6dp) {
                        Some(open) => symbol.open_units_6dp = open,
                        None => push_gap(
                            &mut projection.gaps,
                            &cohort.symbol,
                            Some(&cashflow.condition_id),
                            "symbol_open_pnl_overflow",
                        ),
                    }
                    for token in projected.tokens {
                        if token.qty_units_6dp > 0
                            && token_rows.insert(token.token.clone(), token).is_some()
                        {
                            push_gap(
                                &mut projection.gaps,
                                &cohort.symbol,
                                Some(&cashflow.condition_id),
                                "duplicate_open_token_identity",
                            );
                        }
                    }
                }
                Err(error) => push_gap(
                    &mut projection.gaps,
                    &cohort.symbol,
                    Some(&cashflow.condition_id),
                    format!("{error:#}"),
                ),
            }
        }
        let condition_symbols = financial
            .unresolved_cohorts
            .iter()
            .map(|cohort| (cohort.market_id.to_string(), cohort.symbol.clone()))
            .collect::<BTreeMap<_, _>>();
        if condition_symbols.len() != financial.unresolved_cohorts.len() {
            push_gap(
                &mut projection.gaps,
                "*",
                None,
                "duplicate_open_condition_identity",
            );
        }
        validate_portfolio_quantities(self, &token_rows, &condition_symbols, &mut projection.gaps);
        build_rows(financial, &symbols, &mut projection);
        projection.portfolio.receipt_realized_pnl_units_6dp = financial
            .ctf_summary
            .iter()
            .find(|row| row.symbol == "TOTAL")
            .expect("canonical CTF summary always contains TOTAL")
            .realized_pnl
            .units_6dp();

        for (token, display) in &token_rows {
            if !projection.is_symbol_complete(&display.symbol) {
                continue;
            }
            let Some(position) = projection.portfolio.positions.get_mut(token) else {
                continue;
            };
            if display.qty_units_6dp == 0 {
                continue;
            }
            position.mark_price = display
                .display_mark_price_units_8dp
                .map(|price| price as f64 / FIXED_8DP_SCALE as f64);
            position.unrealized_pnl_units_6dp = display.display_open_pnl_units_6dp;
            position.remaining_basis_units_6dp = Some(display.remaining_basis_units_6dp);
            position.mark_updated_at_ms = display.display_valued_at_ms;
        }
        if projection.is_complete() {
            let market_marked_unrealized_pnl_units_6dp =
                token_rows.values().try_fold(0i128, |total, display| {
                    total.checked_add(display.display_open_pnl_units_6dp?)
                });
            let market_marked_total_pnl_units_6dp = projection
                .portfolio
                .receipt_realized_pnl_units_6dp
                .and_then(|realized| {
                    market_marked_unrealized_pnl_units_6dp
                        .and_then(|open| realized.checked_add(open))
                });
            let total = projection
                .row_for_symbol("TOTAL")
                .expect("complete projection contains TOTAL")
                .clone();
            projection.portfolio.marked_remaining_value_units_6dp =
                total.marked_remaining_value_units_6dp;
            projection.portfolio.total_unrealized_pnl_units_6dp = total.open_pnl_units_6dp;
            projection.portfolio.total_pnl_units_6dp = total.total_pnl_units_6dp;
            projection.portfolio.market_marked_unrealized_pnl_units_6dp =
                market_marked_unrealized_pnl_units_6dp;
            projection.portfolio.market_marked_total_pnl_units_6dp =
                market_marked_total_pnl_units_6dp;
            projection.portfolio.valuation_canonical_revision = projection.canonical_revision;
            projection.portfolio.valuation_odds_revision = projection.odds_revision;
            projection.portfolio.valued_at_ms = Some(valued_at_ms);
        }
        projection
    }
}

fn build_rows(
    financial: CanonicalFinancialView<'_>,
    symbols: &BTreeMap<String, SymbolProjection>,
    projection: &mut ExactPortfolioAccountingProjection,
) {
    let summaries = financial
        .ctf_summary
        .iter()
        .filter(|row| row.symbol != "TOTAL")
        .collect::<Vec<_>>();
    let unique = summaries
        .iter()
        .map(|row| row.symbol.as_str())
        .collect::<BTreeSet<_>>();
    if summaries.len() != unique.len()
        || financial
            .ctf_summary
            .iter()
            .filter(|row| row.symbol == "TOTAL")
            .count()
            != 1
    {
        push_gap(
            &mut projection.gaps,
            "*",
            None,
            "canonical_summary_identity_invalid",
        );
    }
    if symbols
        .keys()
        .any(|symbol| !unique.contains(symbol.as_str()))
    {
        push_gap(
            &mut projection.gaps,
            "*",
            None,
            "canonical_summary_open_symbol_mismatch",
        );
    }
    for summary in summaries {
        let symbol_has_gap = projection
            .gaps
            .iter()
            .any(|gap| gap.symbol == "*" || gap.symbol == summary.symbol);
        let values = symbols.get(&summary.symbol);
        let realized = summary.realized_pnl.units_6dp();
        if realized.is_none() {
            push_gap(
                &mut projection.gaps,
                &summary.symbol,
                None,
                "receipt_realized_pnl_unavailable",
            );
        }
        let available = !symbol_has_gap && realized.is_some();
        let marked = available.then_some(values.map_or(0, |value| value.marked_units_6dp));
        let open = available.then_some(values.map_or(0, |value| value.open_units_6dp));
        let total = realized
            .zip(open)
            .and_then(|(realized, open)| realized.checked_add(open));
        if available && total.is_none() {
            push_gap(
                &mut projection.gaps,
                &summary.symbol,
                None,
                "symbol_total_pnl_overflow",
            );
        }
        projection.rows.push(PortfolioAccountingPnlRow {
            symbol: summary.symbol.clone(),
            marked_remaining_value_units_6dp: marked,
            receipt_realized_pnl_units_6dp: realized,
            open_pnl_units_6dp: open,
            total_pnl_units_6dp: total,
        });
    }
    let total_realized = financial
        .ctf_summary
        .iter()
        .find(|row| row.symbol == "TOTAL")
        .expect("canonical CTF summary always contains TOTAL")
        .realized_pnl
        .units_6dp();
    let summed_realized = projection.rows.iter().try_fold(0i128, |sum, row| {
        sum.checked_add(row.receipt_realized_pnl_units_6dp?)
    });
    if summed_realized != total_realized {
        push_gap(
            &mut projection.gaps,
            "*",
            None,
            "receipt_realized_total_mismatch",
        );
    }
    if total_realized.is_none() {
        push_gap(
            &mut projection.gaps,
            "*",
            None,
            "total_receipt_realized_pnl_unavailable",
        );
    }
    let all_symbols_available = projection.gaps.is_empty()
        && projection.rows.len() == projection.expected_symbols.len()
        && projection.rows.iter().all(|row| {
            row.marked_remaining_value_units_6dp.is_some()
                && row.receipt_realized_pnl_units_6dp.is_some()
                && row.open_pnl_units_6dp.is_some()
                && row.total_pnl_units_6dp.is_some()
        });
    let marked_sum = projection.rows.iter().try_fold(0u128, |sum, row| {
        sum.checked_add(row.marked_remaining_value_units_6dp?)
    });
    if all_symbols_available && marked_sum.is_none() {
        push_gap(
            &mut projection.gaps,
            "*",
            None,
            "total_marked_value_overflow",
        );
    }
    let open_sum = projection
        .rows
        .iter()
        .try_fold(0i128, |sum, row| sum.checked_add(row.open_pnl_units_6dp?));
    if all_symbols_available && open_sum.is_none() {
        push_gap(&mut projection.gaps, "*", None, "total_open_pnl_overflow");
    }
    let initially_available = all_symbols_available && projection.gaps.is_empty();
    let open = initially_available.then_some(open_sum).flatten();
    let total = total_realized
        .zip(open)
        .and_then(|(realized, open)| realized.checked_add(open));
    if initially_available && total.is_none() {
        push_gap(&mut projection.gaps, "*", None, "total_pnl_overflow");
    }
    let global_failure = projection.gaps.iter().any(|gap| gap.symbol == "*");
    if global_failure {
        for row in &mut projection.rows {
            row.marked_remaining_value_units_6dp = None;
            row.open_pnl_units_6dp = None;
            row.total_pnl_units_6dp = None;
        }
    }
    let available = initially_available && !global_failure;
    projection.rows.push(PortfolioAccountingPnlRow {
        symbol: "TOTAL".to_string(),
        marked_remaining_value_units_6dp: available.then_some(marked_sum).flatten(),
        receipt_realized_pnl_units_6dp: total_realized,
        open_pnl_units_6dp: available.then_some(open_sum).flatten(),
        total_pnl_units_6dp: available.then_some(total).flatten(),
    });
}

fn validate_portfolio_quantities(
    portfolio: &PortfolioState,
    expected: &BTreeMap<String, TokenProjection>,
    condition_symbols: &BTreeMap<String, String>,
    gaps: &mut Vec<PortfolioAccountingGap>,
) {
    for (token, row) in expected {
        let matches = portfolio.positions.get(token).is_some_and(|position| {
            position.qty_units_6dp == row.qty_units_6dp
                && portfolio
                    .token_market_hint
                    .get(token)
                    .is_some_and(|market| market.as_str() == row.condition_id)
        });
        if !matches {
            push_gap(
                gaps,
                &row.symbol,
                Some(&row.condition_id),
                "canonical_cashflow_portfolio_quantity_mismatch",
            );
        }
    }
    for (token, _) in portfolio
        .positions
        .iter()
        .filter(|(_, position)| position.qty_units_6dp > 0)
    {
        if expected.contains_key(token) {
            continue;
        }
        let Some(condition_id) = portfolio
            .token_market_hint
            .get(token)
            .map(ToString::to_string)
        else {
            push_gap(gaps, "*", None, "portfolio_token_market_identity_missing");
            continue;
        };
        match condition_symbols.get(&condition_id) {
            Some(symbol) => push_gap(
                gaps,
                symbol,
                Some(&condition_id),
                "portfolio_contains_unexpected_open_token",
            ),
            None => push_gap(
                gaps,
                "*",
                Some(&condition_id),
                "portfolio_contains_unknown_open_inventory",
            ),
        }
    }
}

fn unavailable_rows(symbols: &[String]) -> Vec<PortfolioAccountingPnlRow> {
    symbols
        .iter()
        .map(|symbol| PortfolioAccountingPnlRow {
            symbol: symbol.clone(),
            marked_remaining_value_units_6dp: None,
            receipt_realized_pnl_units_6dp: None,
            open_pnl_units_6dp: None,
            total_pnl_units_6dp: None,
        })
        .chain(std::iter::once(PortfolioAccountingPnlRow {
            symbol: "TOTAL".to_string(),
            marked_remaining_value_units_6dp: None,
            receipt_realized_pnl_units_6dp: None,
            open_pnl_units_6dp: None,
            total_pnl_units_6dp: None,
        }))
        .collect()
}

fn clear_projection(portfolio: &mut PortfolioState) {
    portfolio.marked_remaining_value_units_6dp = None;
    portfolio.receipt_realized_pnl_units_6dp = None;
    portfolio.total_unrealized_pnl_units_6dp = None;
    portfolio.total_pnl_units_6dp = None;
    portfolio.market_marked_unrealized_pnl_units_6dp = None;
    portfolio.market_marked_total_pnl_units_6dp = None;
    portfolio.valuation_canonical_revision = None;
    portfolio.valuation_odds_revision = None;
    portfolio.valued_at_ms = None;
    for position in portfolio.positions.values_mut() {
        position.mark_price = None;
        position.unrealized_pnl_units_6dp = None;
        position.remaining_basis_units_6dp = None;
        position.mark_updated_at_ms = None;
    }
}

fn push_gap(
    gaps: &mut Vec<PortfolioAccountingGap>,
    symbol: impl Into<String>,
    condition_id: Option<&str>,
    detail: impl Into<String>,
) {
    gaps.push(PortfolioAccountingGap {
        symbol: symbol.into(),
        condition_id: condition_id.map(str::to_string),
        detail: detail.into(),
    });
}
