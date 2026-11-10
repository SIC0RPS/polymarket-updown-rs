use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::ctf_accounting::{CtfMarketEntryCashflow, CtfMarketEntryCashflowSummary};
use crate::trade_session_ledger::{
    CanonicalCtfEvent, ConfirmedCashflowIdentity, ConfirmedCashflowKind, ConfirmedCtfTerminalFact,
    ConfirmedReceiptCashflow, CtfTerminalOperation, DirectEoaGasReceipt, DirectEoaOperation,
    FinalizedConditionResolutionReceipt, ReceiptBlockIdentity, SettlementCollateralReceipt,
    TradeLedgerRecord,
};

use super::super::FinancialRowDelta;
use super::{
    canonical_cashflow, validate_sell_identity, validate_terminal_identity,
    validate_terminal_receipts, CanonicalClosedCohortStats, CanonicalUnresolvedCohortState,
    CanonicalUnresolvedCohortStats, CohortEntryIdentity, StampedFact,
};

// BUY receipt timestamps can precede finality admission. Preserve earlier realizations
// by reducing those cohorts in ledger order; split cohorts retain their historical order.
pub(in crate::trade_session_ledger) fn financial_event_order_key(
    buy_origin: bool,
    ts_ms: u64,
    ledger_seq: u64,
) -> (u64, u64) {
    if buy_origin {
        (ledger_seq, 0)
    } else {
        (ts_ms, ledger_seq)
    }
}

#[derive(Clone)]
pub(in crate::trade_session_ledger) struct CohortFinancialState {
    entry: CohortEntryIdentity,
    cashflow: CtfMarketEntryCashflow,
}

#[derive(Clone)]
pub(in crate::trade_session_ledger) struct FinancialStampedEvent {
    pub(in crate::trade_session_ledger) run_id: String,
    pub(in crate::trade_session_ledger) ledger_seq: u64,
    pub(in crate::trade_session_ledger) ts_ms: u64,
    pub(in crate::trade_session_ledger) event: CanonicalCtfEvent,
}

#[derive(Clone)]
pub(in crate::trade_session_ledger) enum FinancialCohortProjection {
    Closed(Arc<CanonicalClosedCohortStats>),
    Unresolved(Arc<CanonicalUnresolvedCohortStats>),
}

impl FinancialStampedEvent {
    pub(in crate::trade_session_ledger) fn new(
        record: &TradeLedgerRecord,
        event: CanonicalCtfEvent,
    ) -> Self {
        Self {
            run_id: record.run_id.clone(),
            ledger_seq: record.ledger_seq,
            ts_ms: record.ts_ms,
            event,
        }
    }

    fn as_stamped_fact(&self) -> StampedFact {
        StampedFact {
            run_id: self.run_id.clone(),
            ledger_seq: self.ledger_seq,
            ts_ms: self.ts_ms,
            fact: self.event.clone(),
        }
    }
}

impl CohortFinancialState {
    pub(in crate::trade_session_ledger) fn new(
        cohort_id: &str,
        event: &CanonicalCtfEvent,
    ) -> Result<Self> {
        let entry = CohortEntryIdentity::from_event(event)?;
        anyhow::ensure!(
            entry.cohort_id == cohort_id,
            "acquisition cohort identity mismatch"
        );
        let cashflow = match event {
            CanonicalCtfEvent::SplitConfirmed(split) => CtfMarketEntryCashflow::new(
                cohort_id,
                split.up_token.clone(),
                split.up_qty_units_6dp,
                split.down_token.clone(),
                split.down_qty_units_6dp,
                split.collateral_debit_units_6dp,
                split.fee_units_6dp,
                split.approved_direct_cost_units_6dp,
                split
                    .up_cost_basis_weight_units_8dp
                    .context("split UP cost-basis weight unavailable")?,
                split
                    .down_cost_basis_weight_units_8dp
                    .context("split DOWN cost-basis weight unavailable")?,
            )?,
            CanonicalCtfEvent::BuyConfirmed(buy) => CtfMarketEntryCashflow::new_buy(buy)?,
            _ => anyhow::bail!("cohort has no confirmed acquisition"),
        };
        Ok(Self { entry, cashflow })
    }

    pub(in crate::trade_session_ledger) fn entry(&self) -> &CohortEntryIdentity {
        &self.entry
    }

    pub(in crate::trade_session_ledger) fn apply_validated_event(
        &mut self,
        event: &CanonicalCtfEvent,
    ) -> Result<()> {
        match event {
            CanonicalCtfEvent::BuyConfirmed(buy) => {
                self.entry.validate_buy(buy)?;
                self.cashflow.confirm_buy(buy)?;
            }
            CanonicalCtfEvent::SplitConfirmed(_) => {
                anyhow::bail!("cohort has more than one acquisition origin");
            }
            CanonicalCtfEvent::TradeConfirmed(sell) => {
                validate_sell_identity(&self.entry, sell)?;
                self.cashflow.confirm_sell(
                    sell.receipt_id.clone(),
                    sell.token.clone(),
                    sell.qty_units_6dp,
                    sell.gross_proceeds_units_6dp,
                    sell.fee_units_6dp,
                    sell.approved_direct_cost_units_6dp,
                )?;
            }
            CanonicalCtfEvent::MergeConfirmed(terminal) => {
                validate_terminal_identity(&self.entry, terminal)?;
                self.confirm_terminal(CtfTerminalOperation::Merge, terminal)?;
            }
            CanonicalCtfEvent::RedeemConfirmed(terminal) => {
                validate_terminal_identity(&self.entry, terminal)?;
                self.confirm_terminal(CtfTerminalOperation::Redeem, terminal)?;
            }
            CanonicalCtfEvent::OrderAccepted(_)
            | CanonicalCtfEvent::TradeMatched(_)
            | CanonicalCtfEvent::TradeFailed(_)
            | CanonicalCtfEvent::FeeApplied(_)
            | CanonicalCtfEvent::CostApplied(_)
            | CanonicalCtfEvent::ResolutionObserved(_) => {}
        }
        Ok(())
    }

    pub(in crate::trade_session_ledger) fn summary(&self) -> Result<CtfMarketEntryCashflowSummary> {
        self.cashflow.summary()
    }

    pub(in crate::trade_session_ledger) fn realized_pnl_units(&self) -> Result<Option<i128>> {
        self.cashflow.realized_pnl_units()
    }

    fn confirm_terminal(
        &mut self,
        operation: CtfTerminalOperation,
        terminal: &ConfirmedCtfTerminalFact,
    ) -> Result<()> {
        let mut quantities = BTreeMap::new();
        if terminal.up_terminal_qty_units_6dp > 0 {
            quantities.insert(
                terminal.up_token.clone(),
                terminal.up_terminal_qty_units_6dp,
            );
        }
        if terminal.down_terminal_qty_units_6dp > 0 {
            quantities.insert(
                terminal.down_token.clone(),
                terminal.down_terminal_qty_units_6dp,
            );
        }
        let token_proceeds = if operation == CtfTerminalOperation::Redeem {
            BTreeMap::from([
                (
                    terminal.up_token.clone(),
                    i128::try_from(
                        terminal
                            .up_collateral_proceeds_units_6dp
                            .context("redeem UP proceeds missing")?,
                    )
                    .context("redeem UP proceeds exceed signed range")?,
                ),
                (
                    terminal.down_token.clone(),
                    i128::try_from(
                        terminal
                            .down_collateral_proceeds_units_6dp
                            .context("redeem DOWN proceeds missing")?,
                    )
                    .context("redeem DOWN proceeds exceed signed range")?,
                ),
            ])
        } else {
            BTreeMap::new()
        };
        self.cashflow.confirm_terminal(
            terminal.settlement_id(),
            operation,
            quantities,
            terminal.collateral_proceeds_units_6dp,
            token_proceeds,
            terminal.fee_units_6dp,
            terminal.approved_direct_cost_units_6dp,
        )
    }
}

impl FinancialCohortProjection {
    pub(in crate::trade_session_ledger) fn invalid(
        entry: &CohortEntryIdentity,
        events: &[FinancialStampedEvent],
        detail: String,
    ) -> Result<Self> {
        let first = events
            .first()
            .context("invalid financial cohort has no split event")?;
        let last_ts_ms = events
            .iter()
            .map(|event| event.ts_ms)
            .max()
            .unwrap_or(first.ts_ms);
        Ok(Self::Unresolved(Arc::new(CanonicalUnresolvedCohortStats {
            symbol: entry.underlying.clone(),
            market_id: entry.market_id.clone(),
            market_name: entry.market_name.clone(),
            paired_cohort_key: entry.cohort_id.clone(),
            started_at_ms: first.ts_ms,
            last_ts_ms,
            event_count: events.len(),
            state: CanonicalUnresolvedCohortState::InvalidConfirmedReceiptSet { detail },
        })))
    }

    pub(in crate::trade_session_ledger) fn unresolved(
        &self,
    ) -> Option<&Arc<CanonicalUnresolvedCohortStats>> {
        match self {
            Self::Closed(_) => None,
            Self::Unresolved(cohort) => Some(cohort),
        }
    }

    pub(in crate::trade_session_ledger) fn closed(
        &self,
    ) -> Option<&Arc<CanonicalClosedCohortStats>> {
        match self {
            Self::Closed(cohort) => Some(cohort),
            Self::Unresolved(_) => None,
        }
    }

    pub(in crate::trade_session_ledger) fn cashflow(
        &self,
    ) -> Option<&super::CanonicalCohortCashflow> {
        match self {
            Self::Closed(cohort) => Some(&cohort.cashflow),
            Self::Unresolved(cohort) => match &cohort.state {
                CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete { cashflow } => {
                    Some(cashflow)
                }
                CanonicalUnresolvedCohortState::InvalidConfirmedReceiptSet { .. } => None,
            },
        }
    }
}

#[derive(Clone)]
pub(in crate::trade_session_ledger) struct FinancialProjectionContribution {
    pub(in crate::trade_session_ledger) symbol: String,
    pub(in crate::trade_session_ledger) delta: FinancialRowDelta,
}

impl FinancialProjectionContribution {
    pub(in crate::trade_session_ledger) fn from_projection(
        projection: &FinancialCohortProjection,
    ) -> Result<Self> {
        let (symbol, open, invalid, cashflow, closed_qty, realized) = match projection {
            FinancialCohortProjection::Closed(cohort) => (
                cohort.symbol.clone(),
                false,
                false,
                Some(&cohort.cashflow),
                cohort.closed_outcome_share_quantity_units_6dp,
                cohort.realized_pnl_units_6dp,
            ),
            FinancialCohortProjection::Unresolved(cohort) => {
                let cashflow = match &cohort.state {
                    CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete {
                        cashflow,
                    } => Some(cashflow),
                    CanonicalUnresolvedCohortState::InvalidConfirmedReceiptSet { .. } => None,
                };
                (
                    cohort.symbol.clone(),
                    true,
                    cashflow.is_none(),
                    cashflow,
                    0,
                    0,
                )
            }
        };
        let mut delta = FinancialRowDelta {
            open_count: u64::from(open),
            closed_count: u64::from(!open),
            closed_qty,
            invalid: u64::from(invalid),
            ..FinancialRowDelta::default()
        };
        if let Some(cashflow) = cashflow {
            delta.disposed = cashflow
                .up_confirmed_disposed_qty_units_6dp
                .checked_add(cashflow.down_confirmed_disposed_qty_units_6dp)
                .context("financial disposed quantity overflow")?;
            delta.remaining = cashflow
                .up_remaining_qty_units_6dp
                .checked_add(cashflow.down_remaining_qty_units_6dp)
                .context("financial remaining quantity overflow")?;
            delta.realized = if open {
                cashflow
                    .total_realization()
                    .context("financial cohort realization overflow")?
                    .realized_pnl_units_6dp
            } else {
                realized
            };
            if open {
                delta.open_principal = cashflow
                    .entry_principal_units_6dp()
                    .context("financial open principal overflow")?;
                delta.open_sell_proceeds = cashflow.confirmed_sell_proceeds_units_6dp;
                delta.open_terminal_receipts = cashflow.confirmed_terminal_receipts_units_6dp;
                delta.open_fees = cashflow.confirmed_fee_units_6dp;
                delta.open_costs = cashflow.confirmed_approved_direct_cost_units_6dp;
                delta.open_unrecovered = cashflow
                    .unrecovered_cash_units_6dp()
                    .context("financial open unrecovered cash overflow")?;
            } else {
                delta.closed_principal = cashflow
                    .entry_principal_units_6dp()
                    .context("financial closed principal overflow")?;
            }
        }
        Ok(Self { symbol, delta })
    }
}

pub(in crate::trade_session_ledger) fn validate_retained_financial_stamped_event(
    entry: &CohortEntryIdentity,
    stamped: &FinancialStampedEvent,
    resolutions: &BTreeMap<String, FinalizedConditionResolutionReceipt>,
    settlements: &BTreeMap<String, SettlementCollateralReceipt>,
) -> Result<()> {
    let event = &stamped.event;
    match event {
        CanonicalCtfEvent::BuyConfirmed(buy) => entry.validate_buy(buy),
        CanonicalCtfEvent::TradeConfirmed(sell) => validate_sell_identity(entry, sell),
        CanonicalCtfEvent::MergeConfirmed(terminal)
        | CanonicalCtfEvent::RedeemConfirmed(terminal) => {
            let operation = match event {
                CanonicalCtfEvent::MergeConfirmed(_) => CtfTerminalOperation::Merge,
                CanonicalCtfEvent::RedeemConfirmed(_) => CtfTerminalOperation::Redeem,
                _ => unreachable!("terminal event match is exhaustive"),
            };
            validate_terminal_identity(entry, terminal)?;
            let stamped = stamped.as_stamped_fact();
            let mut resolution_refs = BTreeMap::new();
            if operation == CtfTerminalOperation::Redeem {
                if let Some(receipt) = resolutions.get(&terminal.condition_id) {
                    resolution_refs.insert(terminal.condition_id.clone(), receipt);
                }
            }
            let mut settlement_refs = BTreeMap::new();
            if let Some((_, transaction_id)) = terminal.confirmed_receipt_identity() {
                if let Some(receipt) = settlements.get(transaction_id) {
                    settlement_refs.insert(transaction_id.to_string(), receipt);
                }
            }
            validate_terminal_receipts(
                &stamped,
                operation,
                terminal,
                &resolution_refs,
                &settlement_refs,
            )
        }
        _ => Ok(()),
    }
}

pub(in crate::trade_session_ledger) fn project_retained_financial_cohort(
    run_id: &str,
    state: &CohortFinancialState,
    events: &[FinancialStampedEvent],
    _resolutions: &BTreeMap<String, FinalizedConditionResolutionReceipt>,
    settlements: &BTreeMap<String, SettlementCollateralReceipt>,
) -> Result<FinancialCohortProjection> {
    anyhow::ensure!(
        events.iter().all(|event| event.run_id == run_id),
        "retained financial cohort crosses run identity"
    );
    let facts = events
        .iter()
        .map(FinancialStampedEvent::as_stamped_fact)
        .collect::<Vec<_>>();
    let mut settlement_refs = BTreeMap::new();
    for event in events {
        let terminal = match &event.event {
            CanonicalCtfEvent::MergeConfirmed(terminal)
            | CanonicalCtfEvent::RedeemConfirmed(terminal) => terminal,
            _ => continue,
        };
        let Some((_, transaction_id)) = terminal.confirmed_receipt_identity() else {
            continue;
        };
        if let Some(receipt) = settlements.get(transaction_id) {
            settlement_refs.insert(transaction_id.to_string(), receipt);
        }
    }
    let summary = state.summary()?;
    let cashflow = canonical_cashflow(
        state.entry(),
        &facts,
        Vec::new(),
        &summary,
        &settlement_refs,
    )?;
    let first = events
        .first()
        .context("retained financial cohort has no split event")?;
    let last_ts_ms = events
        .iter()
        .map(|event| event.ts_ms)
        .max()
        .unwrap_or(first.ts_ms);
    if let Some(realized_pnl_units_6dp) = state.realized_pnl_units()? {
        return Ok(FinancialCohortProjection::Closed(Arc::new(
            CanonicalClosedCohortStats {
                symbol: state.entry().underlying.clone(),
                market_id: Some(state.entry().market_id.clone()),
                market_name: state.entry().market_name.clone(),
                trade_id: state.entry().cohort_id.clone(),
                paired_cohort_key: state.entry().cohort_id.clone(),
                authority_source: Some("trade_session_ledger".to_string()),
                owner: None,
                durability_state: Some("confirmed_immutable_receipts".to_string()),
                close_hint: None,
                reconciliation_status: Some("exact_share_conservation".to_string()),
                started_at_ms: first.ts_ms,
                last_ts_ms,
                event_count: events.len(),
                closed_outcome_share_quantity_units_6dp: cashflow
                    .up_created_qty_units_6dp
                    .checked_add(cashflow.down_created_qty_units_6dp)
                    .context("closed outcome share quantity overflow")?,
                realized_pnl_units_6dp,
                cashflow,
            },
        )));
    }
    Ok(FinancialCohortProjection::Unresolved(Arc::new(
        CanonicalUnresolvedCohortStats {
            symbol: state.entry().underlying.clone(),
            market_id: state.entry().market_id.clone(),
            market_name: state.entry().market_name.clone(),
            paired_cohort_key: state.entry().cohort_id.clone(),
            started_at_ms: first.ts_ms,
            last_ts_ms,
            event_count: events.len(),
            state: CanonicalUnresolvedCohortState::ConfirmedReceiptQuantityIncomplete { cashflow },
        },
    )))
}

pub(in crate::trade_session_ledger) fn confirmed_split_cashflow(
    projection: &FinancialCohortProjection,
    gas: Option<&DirectEoaGasReceipt>,
) -> Result<Option<ConfirmedReceiptCashflow>> {
    let Some(cashflow) = projection.cashflow() else {
        return Ok(None);
    };
    if cashflow.buy_authorization.is_some() {
        return Ok(None);
    }
    let block_number = if let Some(gas) = gas {
        if !gas.status
            || gas.operation != DirectEoaOperation::Split
            || cashflow.split_transaction_id != gas.tx_hash
        {
            return Ok(None);
        }
        let Some(block_number) = gas.block_number else {
            return Ok(None);
        };
        block_number
    } else {
        0
    };
    Ok(Some(ConfirmedReceiptCashflow {
        identity: ConfirmedCashflowIdentity {
            ledger_sequence: cashflow.split_ledger_seq,
            transaction_identity: cashflow.split_transaction_id.clone(),
            kind: ConfirmedCashflowKind::Split,
        },
        block: ReceiptBlockIdentity {
            number: block_number,
            hash: None,
        },
        net_pusd_units_6dp: negative_outlay(
            cashflow.split_principal_units_6dp,
            cashflow.split_fee_units_6dp,
            cashflow.split_approved_direct_cost_units_6dp,
        )?,
    }))
}

pub(in crate::trade_session_ledger) fn confirmed_event_cashflow(
    ledger_sequence: u64,
    projection: &FinancialCohortProjection,
) -> Result<Option<ConfirmedReceiptCashflow>> {
    let Some(cashflow) = projection.cashflow() else {
        return Ok(None);
    };
    if let Some(buy) = cashflow.events.iter().find_map(|event| {
        if event.ledger_seq != ledger_sequence {
            return None;
        }
        match &event.event {
            CanonicalCtfEvent::BuyConfirmed(buy) => Some(buy),
            _ => None,
        }
    }) {
        let (transaction_identity, block) =
            managed_trade_cashflow_identity(&buy.receipt_identity, &buy.fill_id);
        return Ok(Some(ConfirmedReceiptCashflow {
            identity: ConfirmedCashflowIdentity {
                ledger_sequence,
                transaction_identity,
                kind: ConfirmedCashflowKind::Buy,
            },
            block,
            net_pusd_units_6dp: buy.collateral_cash_delta_units_6dp,
        }));
    }
    if let Some(receipt) = cashflow
        .managed_sell_receipts
        .iter()
        .find(|receipt| receipt.ledger_seq == ledger_sequence)
    {
        let (transaction_identity, block) =
            managed_trade_cashflow_identity(&receipt.receipt_identity, &receipt.fill_id);
        return Ok(Some(ConfirmedReceiptCashflow {
            identity: ConfirmedCashflowIdentity {
                ledger_sequence,
                transaction_identity,
                kind: ConfirmedCashflowKind::ManagedSale,
            },
            block,
            net_pusd_units_6dp: net_proceeds(
                receipt.gross_proceeds_units_6dp,
                receipt.fee_units_6dp,
                receipt.approved_direct_cost_units_6dp,
            )?,
        }));
    }
    let Some(receipt) = cashflow
        .terminal_receipts
        .iter()
        .find(|receipt| receipt.accounting_ledger_seq == ledger_sequence)
    else {
        return Ok(None);
    };
    let (transaction_identity, block) = match &receipt.evidence {
        super::CanonicalCohortTerminalReceiptEvidence::ConfirmedCollateralReceipt {
            transaction_id,
            finalized_block_number,
            finalized_block_hash,
            ..
        } => (
            transaction_id.clone(),
            ReceiptBlockIdentity {
                number: *finalized_block_number,
                hash: Some(finalized_block_hash.clone()),
            },
        ),
        super::CanonicalCohortTerminalReceiptEvidence::SimulatedMergeOperation { operation_id }
        | super::CanonicalCohortTerminalReceiptEvidence::SimulatedResolutionRedemption {
            operation_id,
            ..
        } => (
            operation_id.clone(),
            ReceiptBlockIdentity {
                number: 0,
                hash: None,
            },
        ),
    };
    Ok(Some(ConfirmedReceiptCashflow {
        identity: ConfirmedCashflowIdentity {
            ledger_sequence,
            transaction_identity,
            kind: match receipt.operation {
                CtfTerminalOperation::Merge => ConfirmedCashflowKind::Merge,
                CtfTerminalOperation::Redeem => ConfirmedCashflowKind::Redemption,
            },
        },
        block,
        net_pusd_units_6dp: net_proceeds(
            receipt.collateral_receipt_units_6dp,
            receipt.fee_units_6dp,
            receipt.approved_direct_cost_units_6dp,
        )?,
    }))
}

pub(in crate::trade_session_ledger) fn negative_outlay(
    principal: u128,
    fee: u128,
    cost: u128,
) -> Result<i128> {
    let outlay = principal
        .checked_add(fee)
        .and_then(|value| value.checked_add(cost))
        .context("confirmed split cash outlay overflow")?;
    i128::try_from(outlay)
        .context("confirmed split cash outlay exceeds signed range")?
        .checked_neg()
        .context("confirmed split cash delta overflow")
}

pub(in crate::trade_session_ledger) fn net_proceeds(
    proceeds: i128,
    fee: u128,
    cost: u128,
) -> Result<i128> {
    let deductions = fee
        .checked_add(cost)
        .context("confirmed receipt cash deduction overflow")?;
    proceeds
        .checked_sub(i128::try_from(deductions).context("cash deduction exceeds signed range")?)
        .context("confirmed receipt cash delta overflow")
}

fn managed_trade_cashflow_identity(
    identity: &crate::trade_session_ledger::ManagedSellReceiptIdentity,
    fill_id: &str,
) -> (String, ReceiptBlockIdentity) {
    match identity {
        crate::trade_session_ledger::ManagedSellReceiptIdentity::ExchangeV2 {
            transaction_hash,
            block_number,
            block_hash,
            ..
        } => (
            transaction_hash.clone(),
            ReceiptBlockIdentity {
                number: *block_number,
                hash: Some(block_hash.clone()),
            },
        ),
        crate::trade_session_ledger::ManagedSellReceiptIdentity::CapturedSimulation { .. } => (
            fill_id.to_string(),
            ReceiptBlockIdentity {
                number: 0,
                hash: None,
            },
        ),
    }
}
