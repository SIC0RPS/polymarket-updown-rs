use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::strategy::facts::{CtfLegEconomicOutcome, CtfOwnedLegCloseReason};
use crate::trade_session_ledger::stats::{
    CohortEntryIdentity, FinancialCohortProjection, FinancialProjectionContribution,
};
use crate::trade_session_ledger::{
    CanonicalCtfEvent, CanonicalCtfSummaryRow, CanonicalOwnedLegOutcome,
    ConfirmedOrderSubmissionFact, ExactOpenUnrecoveredCash, ExactRealizedPnl, TradeLedgerRecord,
};

#[derive(Clone)]
pub(super) struct FinancialSummaryBook {
    symbols: Vec<String>,
    rows: BTreeMap<String, CanonicalCtfSummaryRow>,
    markets: BTreeSet<(String, String)>,
    entries: BTreeMap<String, CohortEntryIdentity>,
    intents: BTreeMap<u64, CanonicalOwnedLegOutcome>,
    submissions: BTreeMap<String, ConfirmedOrderSubmissionFact>,
    current_outcomes: BTreeMap<(String, String), CanonicalOwnedLegOutcome>,
    realized: BTreeMap<String, i128>,
    unrecovered: BTreeMap<String, i128>,
    invalid: BTreeMap<String, u64>,
}

impl FinancialSummaryBook {
    pub(super) fn new(expected_symbols: &[String]) -> Self {
        let mut rows = BTreeMap::new();
        for symbol in expected_symbols {
            let mut row = CanonicalCtfSummaryRow::unavailable(symbol);
            row.realized_pnl = ExactRealizedPnl::Available { units_6dp: 0 };
            row.open_unrecovered_cash = ExactOpenUnrecoveredCash::available(0);
            rows.insert(symbol.clone(), row);
        }
        Self {
            symbols: expected_symbols.to_vec(),
            rows,
            markets: BTreeSet::new(),
            entries: BTreeMap::new(),
            intents: BTreeMap::new(),
            submissions: BTreeMap::new(),
            current_outcomes: BTreeMap::new(),
            realized: expected_symbols.iter().map(|s| (s.clone(), 0)).collect(),
            unrecovered: expected_symbols.iter().map(|s| (s.clone(), 0)).collect(),
            invalid: expected_symbols.iter().map(|s| (s.clone(), 0)).collect(),
        }
    }

    pub(super) fn apply_record(
        &mut self,
        record: &TradeLedgerRecord,
        replacement: Option<(
            Option<&FinancialCohortProjection>,
            &FinancialCohortProjection,
        )>,
    ) -> Result<bool> {
        let mut changed = false;
        let mut rows = BTreeMap::new();
        let mut markets = BTreeSet::new();
        let mut entries = BTreeMap::new();
        let mut intents = BTreeMap::new();
        let mut submissions = BTreeMap::new();
        let mut current_outcomes = BTreeMap::new();
        let mut realized = None;
        let mut unrecovered = None;
        let mut invalid = None;
        if let Some(outcome) = record.owned_leg_outcome.as_ref() {
            changed = true;
            if let Some(intent_id) = outcome.execution_intent_id {
                anyhow::ensure!(
                    !self.intents.contains_key(&intent_id) && !intents.contains_key(&intent_id),
                    "financial owner intent identity reused intent_id={intent_id}"
                );
                let row = staged_row(&mut rows, &self.rows, &outcome.symbol)?;
                row.sell_now_intent_count = checked_inc(row.sell_now_intent_count)?;
                match outcome.close_reason {
                    Some(CtfOwnedLegCloseReason::PairedFirstManagedSell) => {
                        row.paired_first_managed_sell_intent_count =
                            checked_inc(row.paired_first_managed_sell_intent_count)?
                    }
                    Some(CtfOwnedLegCloseReason::PairedPostProceedsSell) => {
                        row.paired_post_proceeds_sell_intent_count =
                            checked_inc(row.paired_post_proceeds_sell_intent_count)?
                    }
                    None => {}
                }
                intents.insert(intent_id, outcome.clone());
            }
            stage_current_outcome(self, &mut rows, &mut current_outcomes, outcome.clone())?;
        }
        if let Some(event) = record.ctf_event.as_ref() {
            match event {
                CanonicalCtfEvent::SplitConfirmed(split) => {
                    changed = true;
                    anyhow::ensure!(
                        !self.entries.contains_key(&split.cohort_id)
                            && !entries.contains_key(&split.cohort_id),
                        "financial split cohort identity reused"
                    );
                    let market = (split.underlying.clone(), split.market_id.to_string());
                    let first_market =
                        !self.markets.contains(&market) && !markets.contains(&market);
                    let row = staged_row(&mut rows, &self.rows, &split.underlying)?;
                    if first_market {
                        row.confirmed_entry_market_count =
                            checked_inc(row.confirmed_entry_market_count)?;
                    }
                    row.split_entries = checked_inc(row.split_entries)?;
                    row.split_principal_units_6dp = row
                        .split_principal_units_6dp
                        .checked_add(split.collateral_debit_units_6dp)
                        .context("financial split principal overflow")?;
                    row.created_share_quantity_units_6dp = row
                        .created_share_quantity_units_6dp
                        .checked_add(
                            split
                                .up_qty_units_6dp
                                .checked_add(split.down_qty_units_6dp)
                                .context("financial created quantity overflow")?,
                        )
                        .context("financial created quantity total overflow")?;
                    entries.insert(
                        split.cohort_id.clone(),
                        CohortEntryIdentity::from_split(split),
                    );
                    markets.insert(market);
                }
                CanonicalCtfEvent::BuyConfirmed(buy) => {
                    changed = true;
                    let entry = CohortEntryIdentity::from_event(event)?;
                    if let Some(original) =
                        planned_or_current(&entries, &self.entries, &buy.cohort_id)
                    {
                        anyhow::ensure!(original == &entry, "BUY acquisition identity changed");
                    }
                    let market = (entry.underlying.clone(), entry.market_id.to_string());
                    let first_market =
                        !self.markets.contains(&market) && !markets.contains(&market);
                    let row = staged_row(&mut rows, &self.rows, &entry.underlying)?;
                    if first_market {
                        row.confirmed_entry_market_count =
                            checked_inc(row.confirmed_entry_market_count)?;
                    }
                    row.created_share_quantity_units_6dp = row
                        .created_share_quantity_units_6dp
                        .checked_add(buy.qty_units_6dp)
                        .context("BUY created quantity overflow")?;
                    entries.insert(buy.cohort_id.clone(), entry);
                    markets.insert(market);
                }
                CanonicalCtfEvent::OrderAccepted(order) => {
                    changed = true;
                    anyhow::ensure!(
                        !self.submissions.contains_key(&order.order_id)
                            && !submissions.contains_key(&order.order_id),
                        "financial submission order identity reused"
                    );
                    if let Some(authorization) = order.buy_authorization {
                        let entry = CohortEntryIdentity::from_buy_authorization(authorization)?;
                        if let Some(original) =
                            planned_or_current(&entries, &self.entries, &order.cohort_id)
                        {
                            anyhow::ensure!(
                                original == &entry,
                                "BUY submission acquisition identity changed"
                            );
                        } else {
                            entries.insert(order.cohort_id.clone(), entry);
                        }
                    }
                    let split = planned_or_current(&entries, &self.entries, &order.cohort_id)
                        .context("financial submission precedes acquisition authorization")?;
                    let intent = planned_or_current(&intents, &self.intents, &order.intent_id);
                    let row = staged_row(&mut rows, &self.rows, &split.underlying)?;
                    let mut violations = Vec::new();
                    anyhow::ensure!(
                        super::super::stats::validate_financial_submission(
                            split,
                            order,
                            intent,
                            row,
                            &mut violations
                        ),
                        "{}",
                        violations.join(";")
                    );
                    submissions.insert(order.order_id.clone(), order.clone());
                }
                CanonicalCtfEvent::TradeConfirmed(sell) => {
                    changed = true;
                    let split = planned_or_current(&entries, &self.entries, &sell.cohort_id)
                        .context("financial sale precedes split")?;
                    let submission =
                        planned_or_current(&submissions, &self.submissions, &sell.order_id)
                            .context("financial sale has no accepted submission")?;
                    let outcome = planned_or_current(&intents, &self.intents, &sell.intent_id)
                        .context("financial sale has no owner intent")?
                        .clone();
                    anyhow::ensure!(
                        submission.intent_id == sell.intent_id
                            && submission.asset_id == sell.token
                            && outcome.symbol == split.underlying
                            && outcome.cohort_id == split.cohort_id
                            && outcome.token == sell.token,
                        "financial sale lineage identity mismatch"
                    );
                    stage_current_outcome(self, &mut rows, &mut current_outcomes, outcome.clone())?;
                    let row = staged_row(&mut rows, &self.rows, &split.underlying)?;
                    row.confirmed_managed_sell_fill_count =
                        checked_inc(row.confirmed_managed_sell_fill_count)?;
                    row.confirmed_managed_sell_quantity_units_6dp = row
                        .confirmed_managed_sell_quantity_units_6dp
                        .checked_add(sell.qty_units_6dp)
                        .context("financial sale quantity overflow")?;
                    row.confirmed_managed_sell_proceeds_units_6dp = row
                        .confirmed_managed_sell_proceeds_units_6dp
                        .checked_add(sell.gross_proceeds_units_6dp)
                        .context("financial sale proceeds overflow")?;
                    let (count, qty, proceeds) = match outcome.close_reason {
                        Some(CtfOwnedLegCloseReason::PairedFirstManagedSell) => (
                            &mut row.paired_first_managed_sell_confirmed_fill_count,
                            &mut row.paired_first_managed_sell_confirmed_fill_quantity_units_6dp,
                            &mut row.paired_first_managed_sell_confirmed_proceeds_units_6dp,
                        ),
                        Some(CtfOwnedLegCloseReason::PairedPostProceedsSell) => (
                            &mut row.paired_post_proceeds_sell_confirmed_fill_count,
                            &mut row.paired_post_proceeds_sell_confirmed_fill_quantity_units_6dp,
                            &mut row.paired_post_proceeds_sell_confirmed_proceeds_units_6dp,
                        ),
                        None => anyhow::bail!("financial sale owner intent has no close reason"),
                    };
                    *count = checked_inc(*count)?;
                    *qty = qty
                        .checked_add(sell.qty_units_6dp)
                        .context("financial classified sale quantity overflow")?;
                    *proceeds = proceeds
                        .checked_add(sell.gross_proceeds_units_6dp)
                        .context("financial classified sale proceeds overflow")?;
                }
                CanonicalCtfEvent::MergeConfirmed(terminal)
                | CanonicalCtfEvent::RedeemConfirmed(terminal) => {
                    changed = true;
                    let symbol = planned_or_current(&entries, &self.entries, &terminal.cohort_id)
                        .context("financial terminal precedes acquisition")?
                        .underlying
                        .clone();
                    let row = staged_row(&mut rows, &self.rows, &symbol)?;
                    row.settlement_receipt_count = checked_inc(row.settlement_receipt_count)?;
                    if matches!(event, CanonicalCtfEvent::MergeConfirmed(_)) {
                        row.merge_confirmed_count = checked_inc(row.merge_confirmed_count)?;
                    } else {
                        row.redeem_confirmed_count = checked_inc(row.redeem_confirmed_count)?;
                    }
                }
                CanonicalCtfEvent::TradeMatched(_)
                | CanonicalCtfEvent::TradeFailed(_)
                | CanonicalCtfEvent::FeeApplied(_)
                | CanonicalCtfEvent::CostApplied(_)
                | CanonicalCtfEvent::ResolutionObserved(_) => {}
            }
        }
        if let Some((prior, next)) = replacement {
            let prior = prior
                .map(FinancialProjectionContribution::from_projection)
                .transpose()?;
            let next = FinancialProjectionContribution::from_projection(next)?;
            if let Some(prior) = prior.as_ref() {
                anyhow::ensure!(
                    prior.symbol == next.symbol,
                    "financial cohort symbol changed"
                );
            }
            let symbol = next.symbol.clone();
            let row = staged_row(&mut rows, &self.rows, &symbol)?;
            let mut realized_total = self.realized[&symbol];
            let mut unrecovered_total = self.unrecovered[&symbol];
            let mut invalid_total = self.invalid[&symbol];
            if let Some(prior) = prior.as_ref() {
                apply_contribution(
                    row,
                    &mut realized_total,
                    &mut unrecovered_total,
                    &mut invalid_total,
                    prior,
                    false,
                )?;
            }
            apply_contribution(
                row,
                &mut realized_total,
                &mut unrecovered_total,
                &mut invalid_total,
                &next,
                true,
            )?;
            realized = Some((symbol.clone(), realized_total));
            unrecovered = Some((symbol.clone(), unrecovered_total));
            invalid = Some((symbol, invalid_total));
        }
        self.rows.extend(rows);
        self.markets.extend(markets);
        self.entries.extend(entries);
        self.intents.extend(intents);
        self.submissions.extend(submissions);
        self.current_outcomes.extend(current_outcomes);
        self.realized.extend(realized);
        self.unrecovered.extend(unrecovered);
        self.invalid.extend(invalid);
        Ok(changed)
    }

    pub(super) fn replace_many(
        &mut self,
        replacements: &[(FinancialCohortProjection, FinancialCohortProjection)],
    ) -> Result<()> {
        let mut rows = BTreeMap::new();
        let mut realized = BTreeMap::new();
        let mut unrecovered = BTreeMap::new();
        let mut invalid = BTreeMap::new();
        for (prior, next) in replacements {
            let prior = FinancialProjectionContribution::from_projection(prior)?;
            let next = FinancialProjectionContribution::from_projection(next)?;
            anyhow::ensure!(
                prior.symbol == next.symbol,
                "financial cohort symbol changed"
            );
            let symbol = next.symbol.clone();
            let row = rows
                .entry(symbol.clone())
                .or_insert_with(|| self.rows[&symbol].clone());
            let realized_total = realized
                .entry(symbol.clone())
                .or_insert(self.realized[&symbol]);
            let unrecovered_total = unrecovered
                .entry(symbol.clone())
                .or_insert(self.unrecovered[&symbol]);
            let invalid_total = invalid
                .entry(symbol.clone())
                .or_insert(self.invalid[&symbol]);
            apply_contribution(
                row,
                realized_total,
                unrecovered_total,
                invalid_total,
                &prior,
                false,
            )?;
            apply_contribution(
                row,
                realized_total,
                unrecovered_total,
                invalid_total,
                &next,
                true,
            )?;
        }
        self.rows.extend(rows);
        self.realized.extend(realized);
        self.unrecovered.extend(unrecovered);
        self.invalid.extend(invalid);
        Ok(())
    }

    pub(super) fn rows(&self) -> Arc<[CanonicalCtfSummaryRow]> {
        crate::trade_session_ledger::stats::finish_financial_rows(
            &self.symbols,
            &self.rows,
            &self.realized,
            &self.unrecovered,
            &self.invalid,
        )
    }
}

pub(super) fn prepare_portfolio_receipt_counts(
    by_cohort: &BTreeMap<
        String,
        BTreeSet<crate::trade_session_ledger::ConfirmedPortfolioReceiptIdentity>,
    >,
    current_counts: &BTreeMap<crate::trade_session_ledger::ConfirmedPortfolioReceiptIdentity, u64>,
    updates: &[(
        String,
        BTreeSet<crate::trade_session_ledger::ConfirmedPortfolioReceiptIdentity>,
    )],
) -> Result<BTreeMap<crate::trade_session_ledger::ConfirmedPortfolioReceiptIdentity, u64>> {
    let mut deltas = BTreeMap::new();
    for (cohort_id, next) in updates {
        let prior = by_cohort.get(cohort_id).cloned().unwrap_or_default();
        for identity in prior.difference(next) {
            *deltas.entry(identity.clone()).or_insert(0i128) -= 1;
        }
        for identity in next.difference(&prior) {
            *deltas.entry(identity.clone()).or_insert(0i128) += 1;
        }
    }
    deltas
        .into_iter()
        .map(|(identity, delta)| {
            let current = i128::from(current_counts.get(&identity).copied().unwrap_or_default());
            let next = current
                .checked_add(delta)
                .context("portfolio receipt reference count overflow")?;
            anyhow::ensure!(
                next >= 0 && next <= i128::from(u64::MAX),
                "portfolio receipt reference count out of range"
            );
            Ok((
                identity,
                u64::try_from(next).expect("validated portfolio count is unsigned"),
            ))
        })
        .collect()
}

pub(super) fn prepare_portfolio_receipt(
    by_cohort: &BTreeMap<
        String,
        BTreeSet<crate::trade_session_ledger::ConfirmedPortfolioReceiptIdentity>,
    >,
    record: &TradeLedgerRecord,
) -> Vec<(
    String,
    BTreeSet<crate::trade_session_ledger::ConfirmedPortfolioReceiptIdentity>,
)> {
    use crate::trade_session_ledger::{ConfirmedCashflowKind, ConfirmedPortfolioReceiptIdentity};
    let Some(event) = record.ctf_event.as_ref() else {
        return Vec::new();
    };
    let Some(cohort_id) = event.cohort_id() else {
        return Vec::new();
    };
    if matches!(
        event,
        CanonicalCtfEvent::FeeApplied(_) | CanonicalCtfEvent::CostApplied(_)
    ) {
        return vec![(cohort_id.to_string(), BTreeSet::new())];
    }
    let mut next = by_cohort.get(cohort_id).cloned().unwrap_or_default();
    match event {
        CanonicalCtfEvent::SplitConfirmed(split) => {
            next.insert(ConfirmedPortfolioReceiptIdentity::Split {
                cohort_id: split.cohort_id.clone(),
                transaction_id: split.transaction_id.clone(),
            });
        }
        CanonicalCtfEvent::BuyConfirmed(buy) => {
            next.insert(ConfirmedPortfolioReceiptIdentity::Buy {
                cohort_id: buy.cohort_id.clone(),
                fill_id: buy.fill_id.clone(),
            });
        }
        CanonicalCtfEvent::TradeConfirmed(sell) => {
            next.insert(ConfirmedPortfolioReceiptIdentity::ManagedSell {
                cohort_id: sell.cohort_id.clone(),
                fill_id: sell.fill_id.clone(),
            });
        }
        CanonicalCtfEvent::MergeConfirmed(terminal)
        | CanonicalCtfEvent::RedeemConfirmed(terminal) => {
            let kind = if matches!(event, CanonicalCtfEvent::MergeConfirmed(_)) {
                ConfirmedCashflowKind::Merge
            } else {
                ConfirmedCashflowKind::Redemption
            };
            for (token, quantity) in [
                (&terminal.up_token, terminal.up_terminal_qty_units_6dp),
                (&terminal.down_token, terminal.down_terminal_qty_units_6dp),
            ] {
                if quantity > 0 {
                    next.insert(ConfirmedPortfolioReceiptIdentity::Terminal {
                        market_id: terminal.market_id.to_string(),
                        token: token.clone(),
                        transaction_id: terminal.terminal_operation_identity().to_string(),
                        kind,
                    });
                }
            }
        }
        CanonicalCtfEvent::OrderAccepted(_)
        | CanonicalCtfEvent::TradeMatched(_)
        | CanonicalCtfEvent::TradeFailed(_)
        | CanonicalCtfEvent::FeeApplied(_)
        | CanonicalCtfEvent::CostApplied(_)
        | CanonicalCtfEvent::ResolutionObserved(_) => {}
    }
    vec![(cohort_id.to_string(), next)]
}

fn planned_or_current<'a, K: Ord, V>(
    planned: &'a BTreeMap<K, V>,
    current: &'a BTreeMap<K, V>,
    key: &K,
) -> Option<&'a V> {
    planned.get(key).or_else(|| current.get(key))
}

fn staged_row<'a>(
    staged: &'a mut BTreeMap<String, CanonicalCtfSummaryRow>,
    current: &BTreeMap<String, CanonicalCtfSummaryRow>,
    symbol: &str,
) -> Result<&'a mut CanonicalCtfSummaryRow> {
    if !staged.contains_key(symbol) {
        let row = current
            .get(symbol)
            .with_context(|| format!("financial summary has unknown symbol={symbol}"))?
            .clone();
        staged.insert(symbol.to_string(), row);
    }
    Ok(staged.get_mut(symbol).expect("staged financial row exists"))
}

fn stage_current_outcome(
    book: &FinancialSummaryBook,
    rows: &mut BTreeMap<String, CanonicalCtfSummaryRow>,
    outcomes: &mut BTreeMap<(String, String), CanonicalOwnedLegOutcome>,
    outcome: CanonicalOwnedLegOutcome,
) -> Result<()> {
    let key = (outcome.cohort_id.clone(), outcome.token.clone());
    if let Some(prior) = outcomes
        .get(&key)
        .or_else(|| book.current_outcomes.get(&key))
    {
        stage_disposition(rows, &book.rows, prior, false)?;
    }
    stage_disposition(rows, &book.rows, &outcome, true)?;
    outcomes.insert(key, outcome);
    Ok(())
}

fn stage_disposition(
    rows: &mut BTreeMap<String, CanonicalCtfSummaryRow>,
    current: &BTreeMap<String, CanonicalCtfSummaryRow>,
    outcome: &CanonicalOwnedLegOutcome,
    add: bool,
) -> Result<()> {
    let row = staged_row(rows, current, &outcome.symbol)?;
    let value = match outcome.outcome {
        CtfLegEconomicOutcome::SellNow => &mut row.sell_now_outcome_count,
        CtfLegEconomicOutcome::HoldForSettlement => &mut row.hold_for_settlement_outcome_count,
    };
    *value = if add {
        value.checked_add(1)
    } else {
        value.checked_sub(1)
    }
    .context("financial disposition count overflow")?;
    Ok(())
}

fn apply_contribution(
    row: &mut CanonicalCtfSummaryRow,
    realized: &mut i128,
    unrecovered: &mut i128,
    invalid: &mut u64,
    contribution: &FinancialProjectionContribution,
    add: bool,
) -> Result<()> {
    row.checked_apply_financial_delta(&contribution.delta, add)?;
    *realized = if add {
        realized.checked_add(contribution.delta.realized)
    } else {
        realized.checked_sub(contribution.delta.realized)
    }
    .context("financial realized total overflow")?;
    *unrecovered = if add {
        unrecovered.checked_add(contribution.delta.open_unrecovered)
    } else {
        unrecovered.checked_sub(contribution.delta.open_unrecovered)
    }
    .context("financial unrecovered total overflow")?;
    *invalid = if add {
        invalid.checked_add(contribution.delta.invalid)
    } else {
        invalid.checked_sub(contribution.delta.invalid)
    }
    .context("financial invalid total overflow")?;
    Ok(())
}

fn checked_inc(value: u64) -> Result<u64> {
    value.checked_add(1).context("financial count overflow")
}
