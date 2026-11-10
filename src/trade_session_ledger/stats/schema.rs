use std::sync::Arc;

use anyhow::Context;

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ExactOpenUnrecoveredCash {
    Available {
        #[serde(
            serialize_with = "serialize_i128_string",
            deserialize_with = "deserialize_i128_string"
        )]
        units_6dp: i128,
    },
    UnavailableCanonicalOpenCohortFact,
}

impl ExactOpenUnrecoveredCash {
    pub(in crate::trade_session_ledger) fn available(units_6dp: i128) -> Self {
        Self::Available { units_6dp }
    }

    pub(in crate::trade_session_ledger) fn unavailable() -> Self {
        Self::UnavailableCanonicalOpenCohortFact
    }

    pub fn status(&self) -> &'static str {
        match self {
            Self::Available { .. } => "available",
            Self::UnavailableCanonicalOpenCohortFact => "unavailable_canonical_open_cohort_fact",
        }
    }

    pub fn units_6dp(&self) -> Option<i128> {
        match self {
            Self::Available { units_6dp } => Some(*units_6dp),
            Self::UnavailableCanonicalOpenCohortFact => None,
        }
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        match (self.units_6dp(), other.units_6dp()) {
            (Some(left), Some(right)) => left.checked_add(right).map(Self::available),
            _ => Some(Self::unavailable()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalCtfSummaryRow {
    pub symbol: String,
    pub confirmed_entry_market_count: u64,
    pub split_entries: u64,
    pub sell_now_intent_count: u64,
    pub paired_first_managed_sell_intent_count: u64,
    pub paired_post_proceeds_sell_intent_count: u64,
    pub submitted_orders: u64,
    pub paired_first_managed_sell_submitted_order_count: u64,
    pub paired_post_proceeds_sell_submitted_order_count: u64,
    pub paired_first_managed_sell_confirmed_fill_count: u64,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub paired_first_managed_sell_confirmed_fill_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub paired_first_managed_sell_confirmed_proceeds_units_6dp: i128,
    pub paired_post_proceeds_sell_confirmed_fill_count: u64,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub paired_post_proceeds_sell_confirmed_fill_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub paired_post_proceeds_sell_confirmed_proceeds_units_6dp: i128,
    pub confirmed_managed_sell_fill_count: u64,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub confirmed_managed_sell_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub confirmed_managed_sell_proceeds_units_6dp: i128,
    pub sell_now_outcome_count: u64,
    pub hold_for_settlement_outcome_count: u64,
    pub closed_cohort_count: u64,
    pub open_cohort_count: u64,
    pub merge_confirmed_count: u64,
    pub redeem_confirmed_count: u64,
    pub settlement_receipt_count: u64,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub created_share_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub confirmed_disposed_share_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub open_share_quantity_units_6dp: u128,
    pub share_conservation_proven: bool,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub closed_outcome_share_quantity_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub split_principal_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub closed_cohort_principal_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub open_cohort_principal_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub open_confirmed_sell_proceeds_units_6dp: i128,
    #[serde(
        serialize_with = "serialize_i128_string",
        deserialize_with = "deserialize_i128_string"
    )]
    pub open_confirmed_terminal_receipts_units_6dp: i128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub open_confirmed_fee_units_6dp: u128,
    #[serde(
        serialize_with = "serialize_u128_string",
        deserialize_with = "deserialize_u128_string"
    )]
    pub open_confirmed_approved_direct_cost_units_6dp: u128,
    pub open_unrecovered_cash: ExactOpenUnrecoveredCash,
    pub realized_pnl: ExactRealizedPnl,
}

impl CanonicalCtfSummaryRow {
    pub(in crate::trade_session_ledger) fn unavailable(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            confirmed_entry_market_count: 0,
            split_entries: 0,
            sell_now_intent_count: 0,
            paired_first_managed_sell_intent_count: 0,
            paired_post_proceeds_sell_intent_count: 0,
            submitted_orders: 0,
            paired_first_managed_sell_submitted_order_count: 0,
            paired_post_proceeds_sell_submitted_order_count: 0,
            paired_first_managed_sell_confirmed_fill_count: 0,
            paired_first_managed_sell_confirmed_fill_quantity_units_6dp: 0,
            paired_first_managed_sell_confirmed_proceeds_units_6dp: 0,
            paired_post_proceeds_sell_confirmed_fill_count: 0,
            paired_post_proceeds_sell_confirmed_fill_quantity_units_6dp: 0,
            paired_post_proceeds_sell_confirmed_proceeds_units_6dp: 0,
            confirmed_managed_sell_fill_count: 0,
            confirmed_managed_sell_quantity_units_6dp: 0,
            confirmed_managed_sell_proceeds_units_6dp: 0,
            sell_now_outcome_count: 0,
            hold_for_settlement_outcome_count: 0,
            closed_cohort_count: 0,
            open_cohort_count: 0,
            merge_confirmed_count: 0,
            redeem_confirmed_count: 0,
            settlement_receipt_count: 0,
            created_share_quantity_units_6dp: 0,
            confirmed_disposed_share_quantity_units_6dp: 0,
            open_share_quantity_units_6dp: 0,
            share_conservation_proven: false,
            closed_outcome_share_quantity_units_6dp: 0,
            split_principal_units_6dp: 0,
            closed_cohort_principal_units_6dp: 0,
            open_cohort_principal_units_6dp: 0,
            open_confirmed_sell_proceeds_units_6dp: 0,
            open_confirmed_terminal_receipts_units_6dp: 0,
            open_confirmed_fee_units_6dp: 0,
            open_confirmed_approved_direct_cost_units_6dp: 0,
            open_unrecovered_cash: ExactOpenUnrecoveredCash::unavailable(),
            realized_pnl: ExactRealizedPnl::unavailable(),
        }
    }

    pub(super) fn checked_add_from(&mut self, other: &Self) -> Option<()> {
        macro_rules! checked_add_fields {
            ($($field:ident),+ $(,)?) => {
                $(self.$field = self.$field.checked_add(other.$field)?;)+
            };
        }
        checked_add_fields!(
            confirmed_entry_market_count,
            split_entries,
            sell_now_intent_count,
            paired_first_managed_sell_intent_count,
            paired_post_proceeds_sell_intent_count,
            submitted_orders,
            paired_first_managed_sell_submitted_order_count,
            paired_post_proceeds_sell_submitted_order_count,
            paired_first_managed_sell_confirmed_fill_count,
            paired_first_managed_sell_confirmed_fill_quantity_units_6dp,
            paired_first_managed_sell_confirmed_proceeds_units_6dp,
            paired_post_proceeds_sell_confirmed_fill_count,
            paired_post_proceeds_sell_confirmed_fill_quantity_units_6dp,
            paired_post_proceeds_sell_confirmed_proceeds_units_6dp,
            confirmed_managed_sell_fill_count,
            confirmed_managed_sell_quantity_units_6dp,
            confirmed_managed_sell_proceeds_units_6dp,
            sell_now_outcome_count,
            hold_for_settlement_outcome_count,
            closed_cohort_count,
            open_cohort_count,
            merge_confirmed_count,
            redeem_confirmed_count,
            settlement_receipt_count,
            created_share_quantity_units_6dp,
            confirmed_disposed_share_quantity_units_6dp,
            open_share_quantity_units_6dp,
            closed_outcome_share_quantity_units_6dp,
            split_principal_units_6dp,
            closed_cohort_principal_units_6dp,
            open_cohort_principal_units_6dp,
            open_confirmed_sell_proceeds_units_6dp,
            open_confirmed_terminal_receipts_units_6dp,
            open_confirmed_fee_units_6dp,
            open_confirmed_approved_direct_cost_units_6dp,
        );
        self.open_unrecovered_cash = self
            .open_unrecovered_cash
            .checked_add(other.open_unrecovered_cash)?;
        Some(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalStatsProof {
    pub revision: u64,
    pub ctf_summary: Vec<CanonicalCtfSummaryRow>,
    pub ctf_exit_summary: Vec<CanonicalCtfExitSummaryRow>,
    pub ctf_exit_cohorts: Vec<CanonicalCtfExitCohortRow>,
    pub order_terminal_summary: Vec<CanonicalOrderTerminalSummaryRow>,
    pub order_execution_lineage: Vec<CanonicalOrderExecutionLineageRow>,
    pub owned_leg_transition_history: Vec<CanonicalOwnedLegOutcome>,
}
impl CanonicalStatsProof {
    pub(super) fn unavailable(revision: u64, expected_symbols: &[String]) -> Self {
        let mut ctf_summary = expected_symbols
            .iter()
            .map(|symbol| CanonicalCtfSummaryRow::unavailable(symbol.clone()))
            .collect::<Vec<_>>();
        ctf_summary.push(CanonicalCtfSummaryRow::unavailable("TOTAL"));
        Self {
            revision,
            ctf_summary,
            ctf_exit_summary: Vec::new(),
            ctf_exit_cohorts: Vec::new(),
            order_terminal_summary: Vec::new(),
            order_execution_lineage: Vec::new(),
            owned_leg_transition_history: Vec::new(),
        }
    }
    pub fn summary_for_symbol(&self, symbol: &str) -> Option<&CanonicalCtfSummaryRow> {
        self.ctf_summary.iter().find(|row| row.symbol == symbol)
    }
    pub fn total_summary(&self) -> &CanonicalCtfSummaryRow {
        self.summary_for_symbol("TOTAL")
            .expect("canonical CTF summary always contains TOTAL")
    }
}
#[derive(Clone, Default)]
pub(in crate::trade_session_ledger) struct FinancialRowDelta {
    pub(in crate::trade_session_ledger) open_count: u64,
    pub(in crate::trade_session_ledger) closed_count: u64,
    pub(in crate::trade_session_ledger) disposed: u128,
    pub(in crate::trade_session_ledger) remaining: u128,
    pub(in crate::trade_session_ledger) closed_qty: u128,
    pub(in crate::trade_session_ledger) open_principal: u128,
    pub(in crate::trade_session_ledger) closed_principal: u128,
    pub(in crate::trade_session_ledger) open_sell_proceeds: i128,
    pub(in crate::trade_session_ledger) open_terminal_receipts: i128,
    pub(in crate::trade_session_ledger) open_fees: u128,
    pub(in crate::trade_session_ledger) open_costs: u128,
    pub(in crate::trade_session_ledger) open_unrecovered: i128,
    pub(in crate::trade_session_ledger) realized: i128,
    pub(in crate::trade_session_ledger) invalid: u64,
}

impl CanonicalCtfSummaryRow {
    pub(in crate::trade_session_ledger) fn checked_apply_financial_delta(
        &mut self,
        delta: &FinancialRowDelta,
        add: bool,
    ) -> anyhow::Result<()> {
        self.open_cohort_count = if add {
            self.open_cohort_count.checked_add(delta.open_count)
        } else {
            self.open_cohort_count.checked_sub(delta.open_count)
        }
        .context("financial open cohort count delta overflow")?;
        self.closed_cohort_count = if add {
            self.closed_cohort_count.checked_add(delta.closed_count)
        } else {
            self.closed_cohort_count.checked_sub(delta.closed_count)
        }
        .context("financial closed cohort count delta overflow")?;
        self.confirmed_disposed_share_quantity_units_6dp = if add {
            self.confirmed_disposed_share_quantity_units_6dp
                .checked_add(delta.disposed)
        } else {
            self.confirmed_disposed_share_quantity_units_6dp
                .checked_sub(delta.disposed)
        }
        .context("financial disposed quantity delta overflow")?;
        self.open_share_quantity_units_6dp = if add {
            self.open_share_quantity_units_6dp
                .checked_add(delta.remaining)
        } else {
            self.open_share_quantity_units_6dp
                .checked_sub(delta.remaining)
        }
        .context("financial remaining quantity delta overflow")?;
        self.closed_outcome_share_quantity_units_6dp = if add {
            self.closed_outcome_share_quantity_units_6dp
                .checked_add(delta.closed_qty)
        } else {
            self.closed_outcome_share_quantity_units_6dp
                .checked_sub(delta.closed_qty)
        }
        .context("financial closed quantity delta overflow")?;
        self.open_cohort_principal_units_6dp = if add {
            self.open_cohort_principal_units_6dp
                .checked_add(delta.open_principal)
        } else {
            self.open_cohort_principal_units_6dp
                .checked_sub(delta.open_principal)
        }
        .context("financial open principal delta overflow")?;
        self.closed_cohort_principal_units_6dp = if add {
            self.closed_cohort_principal_units_6dp
                .checked_add(delta.closed_principal)
        } else {
            self.closed_cohort_principal_units_6dp
                .checked_sub(delta.closed_principal)
        }
        .context("financial closed principal delta overflow")?;
        self.open_confirmed_sell_proceeds_units_6dp = if add {
            self.open_confirmed_sell_proceeds_units_6dp
                .checked_add(delta.open_sell_proceeds)
        } else {
            self.open_confirmed_sell_proceeds_units_6dp
                .checked_sub(delta.open_sell_proceeds)
        }
        .context("financial open sell proceeds delta overflow")?;
        self.open_confirmed_terminal_receipts_units_6dp = if add {
            self.open_confirmed_terminal_receipts_units_6dp
                .checked_add(delta.open_terminal_receipts)
        } else {
            self.open_confirmed_terminal_receipts_units_6dp
                .checked_sub(delta.open_terminal_receipts)
        }
        .context("financial open terminal receipts delta overflow")?;
        self.open_confirmed_fee_units_6dp = if add {
            self.open_confirmed_fee_units_6dp
                .checked_add(delta.open_fees)
        } else {
            self.open_confirmed_fee_units_6dp
                .checked_sub(delta.open_fees)
        }
        .context("financial open fee delta overflow")?;
        self.open_confirmed_approved_direct_cost_units_6dp = if add {
            self.open_confirmed_approved_direct_cost_units_6dp
                .checked_add(delta.open_costs)
        } else {
            self.open_confirmed_approved_direct_cost_units_6dp
                .checked_sub(delta.open_costs)
        }
        .context("financial open cost delta overflow")?;
        Ok(())
    }
}

pub(in crate::trade_session_ledger) fn finish_financial_rows(
    symbols: &[String],
    source_rows: &BTreeMap<String, CanonicalCtfSummaryRow>,
    realized: &BTreeMap<String, i128>,
    unrecovered: &BTreeMap<String, i128>,
    invalid: &BTreeMap<String, u64>,
) -> Arc<[CanonicalCtfSummaryRow]> {
    let mut rows = Vec::with_capacity(symbols.len() + 1);
    for symbol in symbols {
        let mut row = source_rows
            .get(symbol)
            .expect("financial summary contains every expected symbol")
            .clone();
        let invalid_count = invalid
            .get(symbol)
            .copied()
            .expect("financial invalid count contains every expected symbol");
        row.share_conservation_proven = invalid_count == 0
            && row
                .confirmed_disposed_share_quantity_units_6dp
                .checked_add(row.open_share_quantity_units_6dp)
                .is_some_and(|quantity| quantity == row.created_share_quantity_units_6dp);
        row.realized_pnl = if invalid_count == 0 {
            ExactRealizedPnl::Available {
                units_6dp: realized[symbol],
            }
        } else {
            ExactRealizedPnl::unavailable()
        };
        row.open_unrecovered_cash = if invalid_count == 0 {
            ExactOpenUnrecoveredCash::available(unrecovered[symbol])
        } else {
            ExactOpenUnrecoveredCash::unavailable()
        };
        rows.push(row);
    }
    let mut total = CanonicalCtfSummaryRow::unavailable("TOTAL");
    total.share_conservation_proven = true;
    total.open_unrecovered_cash = ExactOpenUnrecoveredCash::available(0);
    total.realized_pnl = ExactRealizedPnl::Available { units_6dp: 0 };
    let mut realized_total = Some(0i128);
    for row in &rows {
        if total.checked_add_from(row).is_none() {
            rows.push(CanonicalCtfSummaryRow::unavailable("TOTAL"));
            return rows.into();
        }
        total.share_conservation_proven &= row.share_conservation_proven;
        realized_total = realized_total.and_then(|sum| {
            row.realized_pnl
                .units_6dp()
                .and_then(|value| sum.checked_add(value))
        });
    }
    total.realized_pnl = realized_total
        .map(|units_6dp| ExactRealizedPnl::Available { units_6dp })
        .unwrap_or_else(ExactRealizedPnl::unavailable);
    total.share_conservation_proven &= total
        .confirmed_disposed_share_quantity_units_6dp
        .checked_add(total.open_share_quantity_units_6dp)
        .is_some_and(|quantity| quantity == total.created_share_quantity_units_6dp);
    rows.push(total);
    rows.into()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalFinancialProjection {
    pub revision: u64,
    pub ctf_summary: Arc<[CanonicalCtfSummaryRow]>,
    pub closed_cohorts: Arc<[Arc<CanonicalClosedCohortStats>]>,
    pub unresolved_cohorts: Arc<[Arc<CanonicalUnresolvedCohortStats>]>,
}

#[derive(Clone, Copy, Debug)]
pub enum CanonicalUnresolvedFinancialView<'a> {
    Canonical(&'a [CanonicalUnresolvedCohortStats]),
    Shared(&'a [Arc<CanonicalUnresolvedCohortStats>]),
}

pub enum CanonicalUnresolvedFinancialIter<'a> {
    Canonical(std::slice::Iter<'a, CanonicalUnresolvedCohortStats>),
    Shared(std::slice::Iter<'a, Arc<CanonicalUnresolvedCohortStats>>),
}

#[derive(Clone, Copy, Debug)]
pub struct CanonicalFinancialView<'a> {
    pub revision: u64,
    pub ctf_summary: &'a [CanonicalCtfSummaryRow],
    pub unresolved_cohorts: CanonicalUnresolvedFinancialView<'a>,
}

impl<'a> CanonicalUnresolvedFinancialView<'a> {
    pub fn len(self) -> usize {
        match self {
            Self::Canonical(cohorts) => cohorts.len(),
            Self::Shared(cohorts) => cohorts.len(),
        }
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }

    pub fn iter(self) -> CanonicalUnresolvedFinancialIter<'a> {
        match self {
            Self::Canonical(cohorts) => CanonicalUnresolvedFinancialIter::Canonical(cohorts.iter()),
            Self::Shared(cohorts) => CanonicalUnresolvedFinancialIter::Shared(cohorts.iter()),
        }
    }
}

impl<'a> Iterator for CanonicalUnresolvedFinancialIter<'a> {
    type Item = &'a CanonicalUnresolvedCohortStats;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Canonical(cohorts) => cohorts.next(),
            Self::Shared(cohorts) => cohorts.next().map(Arc::as_ref),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Canonical(cohorts) => cohorts.size_hint(),
            Self::Shared(cohorts) => cohorts.size_hint(),
        }
    }
}

impl ExactSizeIterator for CanonicalUnresolvedFinancialIter<'_> {}

impl CanonicalFinancialProjection {
    pub fn financial_view(&self) -> CanonicalFinancialView<'_> {
        CanonicalFinancialView {
            revision: self.revision,
            ctf_summary: &self.ctf_summary,
            unresolved_cohorts: CanonicalUnresolvedFinancialView::Shared(&self.unresolved_cohorts),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CanonicalSessionStats {
    pub schema_version: u32,
    pub canonical: CanonicalStatsProof,
    pub market_count: usize,
    pub trade_count: usize,
    pub event_count: usize,
    pub resolved_decisions: Option<u64>,
    pub emitted_side_decisions: Option<u64>,
    pub emitted_side_correct: Option<u64>,
    pub abstained_decisions: Option<u64>,
    pub wrong_side_first_sells: Option<u64>,
    pub owned_leg_outcomes: Vec<CanonicalOwnedLegOutcome>,
    pub closed_cohorts: Vec<CanonicalClosedCohortStats>,
    pub unresolved_cohorts: Vec<CanonicalUnresolvedCohortStats>,
    pub violations: Vec<String>,
}

impl CanonicalSessionStats {
    pub fn financial_view(&self) -> CanonicalFinancialView<'_> {
        CanonicalFinancialView {
            revision: self.canonical.revision,
            ctf_summary: &self.canonical.ctf_summary,
            unresolved_cohorts: CanonicalUnresolvedFinancialView::Canonical(
                &self.unresolved_cohorts,
            ),
        }
    }

    pub(super) fn unavailable(expected_symbols: &[String]) -> Self {
        Self {
            schema_version: 30,
            canonical: CanonicalStatsProof::unavailable(0, expected_symbols),
            market_count: 0,
            trade_count: 0,
            event_count: 0,
            resolved_decisions: None,
            emitted_side_decisions: None,
            emitted_side_correct: None,
            abstained_decisions: None,
            wrong_side_first_sells: None,
            owned_leg_outcomes: Vec::new(),
            closed_cohorts: Vec::new(),
            unresolved_cohorts: Vec::new(),
            violations: Vec::new(),
        }
    }
}
