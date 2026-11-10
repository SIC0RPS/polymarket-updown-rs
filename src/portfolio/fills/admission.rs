use anyhow::Context;

use crate::engine_trade_journal::EngineTradeJournalRecord;
use crate::polymarket::identity::MarketId;
use crate::trade_session_ledger::{
    CanonicalCtfEvent, ConfirmedCtfManagedSellFact, FinancialAdmissionDisposition,
    ObservedCtfTradeFact,
};
use crate::types::OrderCloseReason;

use super::{
    fill_dedupe_key, FillAdmission, FillEvent, NormalizedStrategyFill, QueuedFill,
    MAX_FILL_QUEUE_LEN,
};
use crate::portfolio::state::{ConfirmedCtfEntryOutlay, PortfolioState};
use crate::trade_session_ledger::{ConfirmedCtfBuyFact, ManagedSellReceiptIdentity};

pub(super) enum ManagedFillAdmissionError {
    Definitive(&'static str),
    Unresolved(anyhow::Error),
}

impl From<&'static str> for ManagedFillAdmissionError {
    fn from(reason: &'static str) -> Self {
        Self::Definitive(reason)
    }
}

pub(super) fn append_confirmed_lifecycle_sell_receipt(
    fill: &FillEvent,
    fill_id: &str,
    lifecycle_cohort: &str,
    market_id: &MarketId,
    intent_id: u64,
    qty_units_6dp: u128,
    gross_proceeds_units_6dp: i128,
    fee_units_6dp: u128,
    confirmed_at_ms: u64,
    close_reason: OrderCloseReason,
    matched_trade: Option<ObservedCtfTradeFact>,
    append_record: &mut impl FnMut(
        EngineTradeJournalRecord,
    ) -> anyhow::Result<FinancialAdmissionDisposition>,
) -> anyhow::Result<FinancialAdmissionDisposition> {
    let accounting_fact = CanonicalCtfEvent::TradeConfirmed(ConfirmedCtfManagedSellFact {
        receipt_id: format!("ctf_managed_sell:{fill_id}"),
        fill_id: fill_id.to_string(),
        receipt_identity: match fill.receipt_identity.clone() {
            Some(identity) => identity,
            None => return Ok(FinancialAdmissionDisposition::DefinitivelyRejected),
        },
        order_id: fill.order_id.clone(),
        intent_id,
        market_id: market_id.clone(),
        condition_id: market_id.to_string(),
        cohort_id: lifecycle_cohort.to_string(),
        token: fill.token.clone(),
        qty_units_6dp,
        gross_proceeds_units_6dp,
        fee_units_6dp,
        approved_direct_cost_units_6dp: 0,
        confirmed_at_ms,
        close_reason: close_reason.to_string(),
        matched_trade,
    });
    if let Err(err) = accounting_fact.validate() {
        tracing::warn!(
            target: "portfolio.engine_trade_journal",
            error = %err,
            lifecycle_cohort,
            fill_id,
            "confirmed_lifecycle_sell_accounting_fact_invalid"
        );
        return Ok(FinancialAdmissionDisposition::DefinitivelyRejected);
    }
    let mut record = EngineTradeJournalRecord::new(
        lifecycle_cohort.to_string(),
        accounting_fact.event_kind(),
        confirmed_at_ms,
    );
    record.ctf_event = Some(accounting_fact);
    record.market_id = Some(market_id.clone());
    record.tokens = vec![fill.token.clone()];
    record.paired_cohort_key = Some(lifecycle_cohort.to_string());
    record.exit_ts_ms = Some(confirmed_at_ms);
    record.strategy_exit_reason = Some(close_reason.to_string());
    record.close_hint = Some(close_reason.to_string());
    record.local_order_ids = vec![fill.order_id.clone()];
    record.fill_ids = vec![fill_id.to_string()];
    record.exit_fill_id = Some(fill_id.to_string());
    record.tx_hashes = fill.transaction_hash.clone().into_iter().collect();
    record.reconciliation_status = Some("confirmed_lifecycle_sell_fill".to_string());
    append_record(record)
}

impl PortfolioState {
    pub(super) fn apply_confirmed_direct_buy(
        &mut self,
        retained_fill: NormalizedStrategyFill,
        append_record: &mut impl FnMut(
            EngineTradeJournalRecord,
        ) -> anyhow::Result<FinancialAdmissionDisposition>,
    ) -> FillAdmission {
        let outcome = (|| -> anyhow::Result<Option<ConfirmedCtfBuyFact>> {
            let fill = &retained_fill.fill;
            let fill_id = fill
                .canonical_fill_id()
                .context("confirmed BUY fill identity is absent")?;
            let market_id = fill
                .market_id
                .clone()
                .context("confirmed BUY market is absent")?;
            let fact = ConfirmedCtfBuyFact {
                receipt_id: format!("ctf_buy:{fill_id}"),
                fill_id,
                receipt_identity: fill
                    .receipt_identity
                    .clone()
                    .context("confirmed BUY receipt proof is absent")?,
                order_id: fill.order_id.clone(),
                authorization: fill
                    .direct_buy_authorization
                    .context("confirmed BUY authorization is absent")?,
                condition_id: market_id.to_string(),
                market_id,
                cohort_id: fill
                    .lifecycle_cohort
                    .clone()
                    .context("confirmed BUY cohort is absent")?,
                token: fill.token.clone(),
                qty_units_6dp: fill
                    .receipt_qty_units_6dp
                    .context("confirmed BUY exact quantity is absent")?,
                principal_units_6dp: fill
                    .receipt_principal_units_6dp
                    .context("confirmed BUY principal is absent")?,
                fee_units_6dp: fill
                    .receipt_fee_units_6dp
                    .context("confirmed BUY fee is absent")?,
                approved_direct_cost_units_6dp: fill
                    .receipt_approved_direct_cost_units_6dp
                    .context("confirmed BUY direct cost is absent")?,
                collateral_refund_units_6dp: fill
                    .receipt_collateral_refund_units_6dp
                    .context("confirmed BUY collateral refund is absent")?,
                collateral_cash_delta_units_6dp: fill
                    .receipt_collateral_cash_delta_units_6dp
                    .context("confirmed BUY collateral cash delta is absent")?,
                confirmed_at_ms: fill
                    .receipt_confirmed_at_ms
                    .context("confirmed BUY confirmation time is absent")?,
                wallet: fill.receipt_wallet.clone(),
                matched_trade: retained_fill.matched_trade.clone(),
            };
            fact.validate()?;
            anyhow::ensure!(
                fill.execution_intent_id == Some(fact.authorization.intent_id)
                    && fill.ts_ms == fact.confirmed_at_ms,
                "confirmed BUY fill conflicts with authorization or confirmation time"
            );
            if let ManagedSellReceiptIdentity::ExchangeV2 {
                transaction_hash, ..
            } = &fact.receipt_identity
            {
                anyhow::ensure!(
                    fill.transaction_hash.as_deref() == Some(transaction_hash.as_str()),
                    "confirmed BUY fill transaction conflicts with receipt proof"
                );
            }
            if !self.validate_confirmed_buy_entry(&fact)? {
                return Ok(None);
            }
            let event = CanonicalCtfEvent::BuyConfirmed(fact.clone());
            let mut record = EngineTradeJournalRecord::new(
                fact.cohort_id.clone(),
                event.event_kind(),
                fact.confirmed_at_ms,
            );
            record.market_id = Some(fact.market_id.clone());
            record.tokens = vec![fact.token.clone()];
            record.paired_cohort_key = Some(fact.cohort_id.clone());
            record.local_order_ids = vec![fact.order_id.clone()];
            record.fill_ids = vec![fact.fill_id.clone()];
            record.tx_hashes = fill.transaction_hash.clone().into_iter().collect();
            record.reconciliation_status = Some("confirmed_direct_buy_fill".to_string());
            record.ctf_event = Some(event);
            anyhow::ensure!(
                append_record(record)? == FinancialAdmissionDisposition::Admitted,
                "confirmed BUY canonical admission was rejected"
            );
            self.apply_buy_position_change(
                &fact.token,
                Some(fact.market_id.as_str()),
                fact.confirmed_at_ms,
                fill.px,
                fill.size,
                0.0,
                0.0,
                Some(fact.cohort_id.clone()),
                Some(fact.order_id.clone()),
                Some(fact.fill_id.clone()),
                fill.transaction_hash.clone(),
                Some(&fact),
            )?;
            let entry = self
                .confirmed_ctf_split_outlays
                .entry(fact.cohort_id.clone())
                .or_insert_with(|| ConfirmedCtfEntryOutlay::Buy(Default::default()));
            let ConfirmedCtfEntryOutlay::Buy(receipts) = entry else {
                unreachable!("BUY entry mode was validated before canonical admission");
            };
            receipts.insert(fact.fill_id.clone(), fact.clone());
            self.token_expiry_ms
                .insert(fact.token.clone(), fact.authorization.market_end_ms);
            if let Some(position) = self.positions.get_mut(&fact.token) {
                position.end_time_ms = Some(fact.authorization.market_end_ms);
            }
            self.seen_fill_keys.insert(fill_dedupe_key(fill));
            self.record_fill_quality(fill);
            self.next_fill_seq = self.next_fill_seq.saturating_add(1);
            let mut published_fill = fill.clone();
            published_fill.size = crate::strategy::full_set_units_to_qty(fact.qty_units_6dp);
            published_fill.px = crate::strategy::full_set_units_to_qty(fact.principal_units_6dp)
                / published_fill.size;
            published_fill.fee = Some(crate::strategy::full_set_units_to_qty(fact.fee_units_6dp));
            self.fills.push_back(QueuedFill {
                seq: self.next_fill_seq,
                fill: published_fill,
            });
            while self.fills.len() > MAX_FILL_QUEUE_LEN {
                self.fills.pop_front();
            }
            self.refresh_position_aggregates();
            self.refresh_positions_metadata(self.updated_at_ms.max(fact.confirmed_at_ms));
            Ok(Some(fact))
        })();
        match outcome {
            Ok(Some(fact)) => FillAdmission::DirectBuyAdmitted(fact),
            Ok(None) => FillAdmission::Admitted,
            Err(error) => FillAdmission::Unresolved {
                fill: retained_fill,
                error,
            },
        }
    }
}
