use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalCoverageGapKind {
    MissingSymbol,
    InvalidSymbolScope,
    EligibleMarketIdentityMismatch,
    ConfirmedEntryCoverageMismatch,
    SplitEntryCoverageMismatch,
    CohortCoverageMismatch,
    ShareConservationUnproven,
    PrincipalCoverageMismatch,
    OpenAccountingUnavailable,
    EndpointValuationUnavailable,
    TerminalReceiptAuditMismatch,
    RealizedPnlUnavailable,
    CanonicalViolation,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCoverageGap {
    pub symbol: String,
    pub kind: CanonicalCoverageGapKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCoverageProof {
    pub expected_symbols: Vec<String>,
    pub gaps: Vec<CanonicalCoverageGap>,
}

impl CanonicalCoverageProof {
    pub fn is_complete(&self) -> bool {
        self.gaps.is_empty()
    }
}

impl CanonicalSessionStats {
    pub fn entry_condition_ids_by_symbol(&self) -> BTreeMap<String, BTreeSet<String>> {
        let mut markets = BTreeMap::<String, BTreeSet<String>>::new();
        for row in &self.canonical.ctf_summary {
            if row.symbol != "TOTAL" {
                markets.entry(row.symbol.clone()).or_default();
            }
        }
        for cohort in &self.closed_cohorts {
            markets
                .entry(cohort.symbol.clone())
                .or_default()
                .insert(cohort.cashflow.condition_id.clone());
        }
        for cohort in &self.unresolved_cohorts {
            let condition_id = match &cohort.state {
                CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete { cashflow } => {
                    cashflow.condition_id.clone()
                }
                CanonicalUnresolvedCohortState::InvalidConfirmedReceiptSet { .. } => {
                    cohort.market_id.to_string()
                }
            };
            markets
                .entry(cohort.symbol.clone())
                .or_default()
                .insert(condition_id);
        }
        markets
    }

    pub fn proof_for_eligible_markets(
        &self,
        portfolio_accounting: &crate::portfolio::PortfolioAccountingProjectionProof,
    ) -> CanonicalCoverageProof {
        let eligible_markets = self.entry_condition_ids_by_symbol();
        let expected_symbols = eligible_markets.keys().cloned().collect::<Vec<_>>();
        let mut gaps = Vec::new();
        let expected_symbol_set = expected_symbols.iter().cloned().collect::<BTreeSet<_>>();
        let summary_symbols = self
            .canonical
            .ctf_summary
            .iter()
            .filter(|row| row.symbol != "TOTAL")
            .map(|row| row.symbol.clone())
            .collect::<Vec<_>>();
        let summary_symbol_set = summary_symbols.iter().cloned().collect::<BTreeSet<_>>();
        let total_row_count = self
            .canonical
            .ctf_summary
            .iter()
            .filter(|row| row.symbol == "TOTAL")
            .count();
        let odds_provenance_is_valid = match (
            portfolio_accounting.odds_revision,
            portfolio_accounting.odds_updated_at_ms,
        ) {
            (None, None) => true,
            (Some(_), Some(updated_at_ms)) => updated_at_ms <= portfolio_accounting.valued_at_ms,
            _ => false,
        };
        if expected_symbols.is_empty()
            || expected_symbols
                .iter()
                .any(|symbol| !crate::config::VALID_ASSETS.contains(&symbol.as_str()))
            || summary_symbols.len() != summary_symbol_set.len()
            || summary_symbol_set != expected_symbol_set
            || total_row_count != 1
        {
            push_gap(&mut gaps, "*", CanonicalCoverageGapKind::InvalidSymbolScope);
        }
        if portfolio_accounting.expected_symbols != expected_symbols
            || portfolio_accounting.canonical_revision != Some(self.canonical.revision)
            || portfolio_accounting.valued_at_ms == 0
            || portfolio_accounting.portfolio_updated_at_ms > portfolio_accounting.valued_at_ms
            || !odds_provenance_is_valid
        {
            push_gap(
                &mut gaps,
                "*",
                CanonicalCoverageGapKind::EndpointValuationUnavailable,
            );
        }
        if !self.violations.is_empty() {
            push_gap(&mut gaps, "*", CanonicalCoverageGapKind::CanonicalViolation);
        }
        for symbol in &expected_symbols {
            let Some(summary) = self.canonical.summary_for_symbol(symbol) else {
                push_gap(&mut gaps, symbol, CanonicalCoverageGapKind::MissingSymbol);
                continue;
            };
            let eligible = &eligible_markets[symbol];
            let closed = self
                .closed_cohorts
                .iter()
                .filter(|cohort| cohort.symbol == *symbol)
                .collect::<Vec<_>>();
            let unresolved = self
                .unresolved_cohorts
                .iter()
                .filter(|cohort| cohort.symbol == *symbol)
                .collect::<Vec<_>>();
            let valid_unresolved = unresolved
                .iter()
                .filter_map(|cohort| match &cohort.state {
                    CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete {
                        cashflow,
                    } => Some(cashflow),
                    CanonicalUnresolvedCohortState::InvalidConfirmedReceiptSet { .. } => None,
                })
                .collect::<Vec<_>>();
            if valid_unresolved.len() != unresolved.len() {
                push_gap(
                    &mut gaps,
                    symbol,
                    CanonicalCoverageGapKind::OpenAccountingUnavailable,
                );
            }
            let cohort_conditions = closed
                .iter()
                .map(|cohort| cohort.cashflow.condition_id.clone())
                .chain(
                    valid_unresolved
                        .iter()
                        .map(|cashflow| cashflow.condition_id.clone()),
                )
                .collect::<BTreeSet<_>>();
            let eligible_count = u64::try_from(eligible.len()).ok();
            let closed_count = u64::try_from(closed.len()).ok();
            let open_count = u64::try_from(valid_unresolved.len()).ok();
            let cohort_count = closed_count
                .zip(open_count)
                .and_then(|(closed, open)| closed.checked_add(open));

            let cashflows = || {
                closed
                    .iter()
                    .map(|cohort| &cohort.cashflow)
                    .chain(valid_unresolved.iter().copied())
            };
            let split_count = u64::try_from(
                cashflows()
                    .filter(|cashflow| cashflow.buy_authorization.is_none())
                    .count(),
            )
            .ok();
            let buy_count = u64::try_from(
                cashflows()
                    .filter(|cashflow| cashflow.buy_authorization.is_some())
                    .count(),
            )
            .ok();
            let entry_principal = cashflows().try_fold(0u128, |sum, cashflow| {
                sum.checked_add(cashflow.entry_principal_units_6dp()?)
            });
            let split_principal = cashflows().try_fold(0u128, |sum, cashflow| {
                sum.checked_add(cashflow.split_principal_units_6dp)
            });

            if cohort_conditions != *eligible
                || cohort_conditions.len() != closed.len() + valid_unresolved.len()
            {
                push_gap(
                    &mut gaps,
                    symbol,
                    CanonicalCoverageGapKind::EligibleMarketIdentityMismatch,
                );
            }
            if Some(summary.confirmed_entry_market_count) != eligible_count {
                push_gap(
                    &mut gaps,
                    symbol,
                    CanonicalCoverageGapKind::ConfirmedEntryCoverageMismatch,
                );
            }
            if Some(summary.split_entries) != split_count {
                push_gap(
                    &mut gaps,
                    symbol,
                    CanonicalCoverageGapKind::SplitEntryCoverageMismatch,
                );
            }
            if cohort_count != eligible_count
                || split_count
                    .zip(buy_count)
                    .and_then(|(split, buy)| split.checked_add(buy))
                    != cohort_count
                || Some(summary.closed_cohort_count) != closed_count
                || Some(summary.open_cohort_count) != open_count
            {
                push_gap(
                    &mut gaps,
                    symbol,
                    CanonicalCoverageGapKind::CohortCoverageMismatch,
                );
            }
            if summary
                .confirmed_disposed_share_quantity_units_6dp
                .checked_add(summary.open_share_quantity_units_6dp)
                != Some(summary.created_share_quantity_units_6dp)
            {
                push_gap(
                    &mut gaps,
                    symbol,
                    CanonicalCoverageGapKind::ShareConservationUnproven,
                );
            }
            if !summary.share_conservation_proven {
                push_gap(
                    &mut gaps,
                    symbol,
                    CanonicalCoverageGapKind::ShareConservationUnproven,
                );
            }
            if summary
                .closed_cohort_principal_units_6dp
                .checked_add(summary.open_cohort_principal_units_6dp)
                != entry_principal
                || Some(summary.split_principal_units_6dp) != split_principal
            {
                push_gap(
                    &mut gaps,
                    symbol,
                    CanonicalCoverageGapKind::PrincipalCoverageMismatch,
                );
            }
            if summary.open_unrecovered_cash.units_6dp().is_none() {
                push_gap(
                    &mut gaps,
                    symbol,
                    CanonicalCoverageGapKind::OpenAccountingUnavailable,
                );
            }
            if !terminal_receipt_audit_matches(summary, &closed, &valid_unresolved) {
                push_gap(
                    &mut gaps,
                    symbol,
                    CanonicalCoverageGapKind::TerminalReceiptAuditMismatch,
                );
            }
            if summary.realized_pnl.units_6dp().is_none() {
                push_gap(
                    &mut gaps,
                    symbol,
                    CanonicalCoverageGapKind::RealizedPnlUnavailable,
                );
            }
            let endpoint_row = portfolio_accounting.row_for_symbol(symbol);
            if !portfolio_accounting.is_symbol_complete(symbol)
                || endpoint_row.is_none_or(|row| {
                    row.marked_remaining_value_units_6dp.is_none()
                        || row.receipt_realized_pnl_units_6dp != summary.realized_pnl.units_6dp()
                        || row.open_pnl_units_6dp.is_none()
                        || row.total_pnl_units_6dp.is_none()
                })
            {
                push_gap(
                    &mut gaps,
                    symbol,
                    CanonicalCoverageGapKind::EndpointValuationUnavailable,
                );
            }
        }
        CanonicalCoverageProof {
            expected_symbols,
            gaps,
        }
    }
}

fn terminal_receipt_audit_matches(
    summary: &CanonicalCtfSummaryRow,
    closed: &[&CanonicalClosedCohortStats],
    unresolved: &[&CanonicalCohortCashflow],
) -> bool {
    let terminal_facts = closed
        .iter()
        .map(|cohort| &cohort.cashflow)
        .chain(unresolved.iter().copied())
        .flat_map(|cashflow| &cashflow.events)
        .filter_map(|event| match &event.event {
            CanonicalCtfEvent::MergeConfirmed(_) => Some((event.ledger_seq, "merge")),
            CanonicalCtfEvent::RedeemConfirmed(_) => Some((event.ledger_seq, "redeem")),
            _ => None,
        })
        .collect::<Vec<_>>();
    let terminal_receipts = closed
        .iter()
        .map(|cohort| &cohort.cashflow)
        .chain(unresolved.iter().copied())
        .flat_map(|cashflow| &cashflow.terminal_receipts)
        .map(|receipt| {
            (
                receipt.accounting_ledger_seq,
                match receipt.operation {
                    CtfTerminalOperation::Merge => "merge",
                    CtfTerminalOperation::Redeem => "redeem",
                },
            )
        })
        .collect::<Vec<_>>();
    let merge_count = terminal_receipts
        .iter()
        .filter(|(_, operation)| *operation == "merge")
        .count();
    let redeem_count = terminal_receipts.len().saturating_sub(merge_count);
    terminal_facts == terminal_receipts
        && u64::try_from(merge_count).ok() == Some(summary.merge_confirmed_count)
        && u64::try_from(redeem_count).ok() == Some(summary.redeem_confirmed_count)
        && u64::try_from(terminal_receipts.len()).ok() == Some(summary.settlement_receipt_count)
}

fn push_gap(
    gaps: &mut Vec<CanonicalCoverageGap>,
    symbol: impl Into<String>,
    kind: CanonicalCoverageGapKind,
) {
    gaps.push(CanonicalCoverageGap {
        symbol: symbol.into(),
        kind,
    });
}
