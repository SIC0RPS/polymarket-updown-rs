use crate::capture::session::MaybeCapture;
use crate::engine_trade_journal::{
    append as append_engine_trade_journal, append_with_disposition, EngineTradeJournalRecord,
};
use crate::execution::supervisor::{emit_receipt_admission_timing, OrderExecutionTiming};
use crate::polymarket::fills::UnboundCtfTradeObservation;
use crate::polymarket::identity::MarketId;
use crate::polymarket::order_context::OrderFillContext;
use crate::trade_session_ledger::{
    CanonicalCtfEvent, ConfirmedCtfBuyFact, FinancialAdmissionDisposition,
    ManagedSellReceiptIdentity, ObservedCtfTradeFact,
};
use crate::types::OrderCloseReason;

use super::state::{PortfolioState, EPS};

mod admission;

use admission::{append_confirmed_lifecycle_sell_receipt, ManagedFillAdmissionError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FillSide {
    Buy,
    Sell,
}

impl FillSide {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "buy",
            Self::Sell => "sell",
        }
    }
}

#[derive(Debug, Clone)]
pub struct FillEvent {
    pub ts_ms: u64,
    pub order_id: String,
    pub trade_id: Option<String>,
    pub transaction_hash: Option<String>,
    pub token: String,
    pub market_id: Option<MarketId>,
    pub side: FillSide,
    pub quote_group: Option<String>,
    pub lifecycle_cohort: Option<String>,
    pub execution_intent_id: Option<u64>,
    pub(crate) trade_observation: Option<UnboundCtfTradeObservation>,
    pub receipt_identity: Option<ManagedSellReceiptIdentity>,
    pub receipt_qty_units_6dp: Option<u128>,
    pub receipt_gross_proceeds_units_6dp: Option<i128>,
    pub receipt_fee_units_6dp: Option<u128>,
    pub receipt_confirmed_at_ms: Option<u64>,
    pub direct_buy_authorization: Option<crate::types::DirectBuyAuthorization>,
    pub receipt_principal_units_6dp: Option<u128>,
    pub receipt_approved_direct_cost_units_6dp: Option<u128>,
    pub receipt_collateral_refund_units_6dp: Option<u128>,
    pub receipt_collateral_cash_delta_units_6dp: Option<i128>,
    pub receipt_wallet: Option<String>,
    pub submitted_order_size: Option<f64>,
    pub px: f64,
    pub size: f64,
    pub fee: Option<f64>,
    pub at_top_of_book: Option<bool>,
    pub queue_ahead_at_fill: Option<f64>,
    pub time_to_fill_ms: Option<u64>,
}

impl FillEvent {
    pub fn canonical_id(trade_id: Option<&str>, order_id: &str) -> Option<String> {
        trade_id
            .map(str::trim)
            .filter(|trade_id| !trade_id.is_empty())
            .map(|trade_id| format!("{trade_id}:{order_id}"))
    }

    pub fn canonical_fill_id(&self) -> Option<String> {
        if let Some(identity) = self.receipt_identity.as_ref() {
            return identity.fill_id(&self.order_id).ok();
        }
        Self::canonical_id(self.trade_id.as_deref(), &self.order_id)
    }
}

#[derive(Debug, Clone)]
pub struct QueuedFill {
    pub seq: u64,
    pub fill: FillEvent,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConfirmedLifecycleSellFill {
    pub fill_id: String,
    /// Populated only by the central accounting owner. The symbol-process
    /// lifecycle view carries a fixed-layout identity digest instead.
    pub receipt_identity: Option<ManagedSellReceiptIdentity>,
    pub order_id: String,
    pub lifecycle_cohort: String,
    pub market_id: MarketId,
    pub token: String,
    pub intent_id: u64,
    pub qty_units_6dp: u128,
    pub gross_proceeds_units_6dp: i128,
    pub fee_units_6dp: u128,
    pub confirmed_at_ms: u64,
    pub close_reason: OrderCloseReason,
}

#[derive(Clone)]
pub struct NormalizedStrategyFill {
    fill: FillEvent,
    close_reason: Option<OrderCloseReason>,
    matched_trade: Option<ObservedCtfTradeFact>,
    execution_timing: Option<OrderExecutionTiming>,
    timing_capture: Option<MaybeCapture>,
    order_context_owner: Option<OrderFillContext>,
}

impl std::fmt::Debug for NormalizedStrategyFill {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NormalizedStrategyFill")
            .field("fill", &self.fill)
            .field("close_reason", &self.close_reason)
            .field("matched_trade", &self.matched_trade)
            .field("execution_timing", &self.execution_timing)
            .finish_non_exhaustive()
    }
}

impl NormalizedStrategyFill {
    pub fn buy(fill: FillEvent) -> Self {
        Self {
            fill,
            close_reason: None,
            matched_trade: None,
            execution_timing: None,
            timing_capture: None,
            order_context_owner: None,
        }
    }

    pub fn sell(fill: FillEvent, close_reason: OrderCloseReason) -> Self {
        Self {
            fill,
            close_reason: Some(close_reason),
            matched_trade: None,
            execution_timing: None,
            timing_capture: None,
            order_context_owner: None,
        }
    }

    pub fn fill(&self) -> &FillEvent {
        &self.fill
    }

    pub(crate) fn with_matched_trade(mut self, fact: ObservedCtfTradeFact) -> anyhow::Result<Self> {
        anyhow::ensure!(
            fact.status_str() == "CONFIRMED",
            "cash fill matched lineage is not CONFIRMED"
        );
        anyhow::ensure!(
            self.fill.trade_id.as_deref() == Some(fact.trade_id.as_str())
                && self.fill.order_id == fact.order_id
                && self.fill.token == fact.asset_id
                && self.fill.market_id.as_ref() == Some(&fact.market_id)
                && self.fill.lifecycle_cohort.as_deref() == Some(fact.cohort_id.as_str()),
            "cash fill differs from matched trade lineage"
        );
        self.matched_trade = Some(fact);
        Ok(self)
    }

    pub(crate) fn with_execution_timing(
        mut self,
        execution_timing: Option<OrderExecutionTiming>,
    ) -> Self {
        self.execution_timing = execution_timing;
        self
    }

    pub(crate) fn with_timing_capture(mut self, capture: MaybeCapture) -> Self {
        self.timing_capture = Some(capture);
        self
    }

    pub(crate) fn with_order_context_owner(mut self, context: OrderFillContext) -> Self {
        self.order_context_owner = Some(context);
        self
    }

    fn into_parts(
        self,
    ) -> (
        FillEvent,
        Option<OrderCloseReason>,
        Option<ObservedCtfTradeFact>,
        Option<OrderExecutionTiming>,
        Option<MaybeCapture>,
        Option<OrderFillContext>,
    ) {
        (
            self.fill,
            self.close_reason,
            self.matched_trade,
            self.execution_timing,
            self.timing_capture,
            self.order_context_owner,
        )
    }
}

#[derive(Debug)]
pub(crate) enum FillAdmission {
    Admitted,
    DirectBuyAdmitted(ConfirmedCtfBuyFact),
    DefinitivelyRejected,
    Unresolved {
        fill: NormalizedStrategyFill,
        error: anyhow::Error,
    },
}

#[derive(Debug, Clone)]
pub struct QuarantinedFill {
    pub seq: u64,
    pub reason: String,
    pub fill: FillEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(in crate::portfolio) enum FillDedupeKey {
    FinalizedReceipt {
        fill_id: String,
    },
    VenueFill {
        trade_id: String,
        order_id: String,
    },
    OrderTimestampPriceSize {
        order_id: String,
        ts_ms: u64,
        px_key: i64,
        size_key: i64,
    },
}

pub(in crate::portfolio) const MAX_FILL_QUEUE_LEN: usize = 1_024;
pub(in crate::portfolio) const MAX_QUARANTINED_FILL_QUEUE_LEN: usize = 1_024;
fn net_buy_shares(fill: &FillEvent, fee: f64) -> f64 {
    if fill.side != FillSide::Buy || !(fill.px.is_finite() && fill.px > 0.0) {
        return fill.size.max(0.0);
    }
    let fee_in_shares = fee / fill.px;
    (fill.size - fee_in_shares).max(0.0)
}

pub(crate) fn append_observed_trade_event(event: CanonicalCtfEvent) -> anyhow::Result<()> {
    event.validate()?;
    let fact = match &event {
        CanonicalCtfEvent::TradeMatched(fact) | CanonicalCtfEvent::TradeFailed(fact) => fact,
        _ => anyhow::bail!("noncash trade journal adapter received a cash or non-trade event"),
    };
    let mut record = EngineTradeJournalRecord::new(
        fact.cohort_id.clone(),
        event.event_kind(),
        fact.observed_at_ms,
    );
    record.market_id = Some(fact.market_id.clone());
    record.tokens = vec![fact.asset_id.clone()];
    record.paired_cohort_key = Some(fact.cohort_id.clone());
    record.local_order_ids = vec![fact.order_id.clone()];
    record.reconciliation_status = Some(fact.status_str().to_ascii_lowercase());
    record.ctf_event = Some(event);
    append_engine_trade_journal(record)
}

impl PortfolioState {
    pub(in crate::portfolio) fn apply_fill(
        &mut self,
        fill: NormalizedStrategyFill,
    ) -> FillAdmission {
        let mut append_record = append_with_disposition;
        self.apply_fill_with_accounting_destination(fill, &mut append_record)
    }

    pub(crate) fn apply_runtime_fill_with_accounting_destination(
        &mut self,
        fill: NormalizedStrategyFill,
        mut append_record: impl FnMut(
            EngineTradeJournalRecord,
        ) -> anyhow::Result<FinancialAdmissionDisposition>,
    ) -> FillAdmission {
        self.apply_fill_with_accounting_destination(fill, &mut append_record)
    }

    fn apply_fill_with_accounting_destination(
        &mut self,
        retained_fill: NormalizedStrategyFill,
        append_record: &mut impl FnMut(
            EngineTradeJournalRecord,
        ) -> anyhow::Result<FinancialAdmissionDisposition>,
    ) -> FillAdmission {
        if retained_fill.fill.side == FillSide::Buy
            && retained_fill.fill.direct_buy_authorization.is_some()
        {
            return self.apply_confirmed_direct_buy(retained_fill, append_record);
        }
        let fill = &retained_fill.fill;
        let sell_exit_reason = retained_fill.close_reason;
        let matched_trade = retained_fill.matched_trade.as_ref();
        let execution_timing = retained_fill.execution_timing.as_ref();
        let timing_capture = retained_fill.timing_capture.as_ref();
        let canonical_fill_id = fill.canonical_fill_id();
        let fee = fill.fee.filter(|fee| fee.is_finite() && *fee >= 0.0);
        if fee.is_none() {
            let (fill, _, _, _, _, _) = retained_fill.into_parts();
            self.quarantine_unmanaged_fill(fill, "missing_or_invalid_fill_fee");
            return FillAdmission::DefinitivelyRejected;
        }
        let updated_at_ms = self.updated_at_ms.max(fill.ts_ms);
        let dedupe_key = fill_dedupe_key(&fill);
        if !(fill.px.is_finite() && fill.px > 0.0) {
            return FillAdmission::DefinitivelyRejected;
        }
        if !(fill.size.is_finite() && fill.size > 0.0) {
            return FillAdmission::DefinitivelyRejected;
        }

        let managed_close_reason = sell_exit_reason.filter(|close_reason| {
            matches!(
                close_reason,
                OrderCloseReason::PairedFirstManagedSell | OrderCloseReason::PairedPostProceedsSell
            )
        });
        if managed_close_reason.is_none() && self.seen_fill_keys.contains(&dedupe_key) {
            return FillAdmission::Admitted;
        }
        let managed_receipt_qty_units_6dp = if let Some(close_reason) = managed_close_reason {
            let receipt_result = (|| -> Result<(u128, bool), ManagedFillAdmissionError> {
                let lifecycle_cohort = fill
                    .lifecycle_cohort
                    .as_deref()
                    .filter(|cohort| !cohort.trim().is_empty())
                    .ok_or("missing_lifecycle_sell_cohort_identity")?;
                let fill_id = canonical_fill_id
                    .as_deref()
                    .ok_or("missing_lifecycle_sell_fill_identity")?;
                let exact_receipt = fill
                    .execution_intent_id
                    .zip(fill.receipt_qty_units_6dp)
                    .zip(fill.receipt_gross_proceeds_units_6dp)
                    .zip(fill.receipt_fee_units_6dp)
                    .zip(fill.receipt_confirmed_at_ms)
                    .map(
                        |(
                            (((intent_id, qty_units_6dp), gross_proceeds_units_6dp), fee_units_6dp),
                            confirmed_at_ms,
                        )| {
                            (
                                intent_id,
                                qty_units_6dp,
                                gross_proceeds_units_6dp,
                                fee_units_6dp,
                                confirmed_at_ms,
                            )
                        },
                    )
                    .filter(|(intent_id, qty, gross, fee, confirmed_at_ms)| {
                        let receipt_identity_valid = fill
                            .receipt_identity
                            .as_ref()
                            .is_some_and(|identity| identity.validate(&fill.order_id).is_ok());
                        *intent_id > 0
                            && *qty > 0
                            && *gross >= 0
                            && *fee <= *gross as u128
                            && *confirmed_at_ms > 0
                            && *confirmed_at_ms == fill.ts_ms
                            && receipt_identity_valid
                    });
                let (
                    intent_id,
                    qty_units_6dp,
                    gross_proceeds_units_6dp,
                    fee_units_6dp,
                    confirmed_at_ms,
                ) = exact_receipt.ok_or("missing_or_invalid_exact_managed_sell_receipt")?;
                let market_id = self
                    .market_id_for_fill(fill)
                    .ok_or("missing_lifecycle_sell_market_identity")?;
                let key = (lifecycle_cohort.to_string(), fill_id.to_string());
                if self.seen_fill_keys.contains(&dedupe_key) {
                    let existing = self
                        .confirmed_lifecycle_sell_fills
                        .get(&key)
                        .ok_or("conflicting_lifecycle_sell_receipt_reuse")?;
                    if existing.order_id != fill.order_id
                        || existing.receipt_identity.as_ref()
                            != Some(
                                fill.receipt_identity
                                    .as_ref()
                                    .ok_or("missing_lifecycle_sell_receipt_identity")?,
                            )
                        || existing.market_id != market_id
                        || existing.token != fill.token
                        || existing.intent_id != intent_id
                        || existing.qty_units_6dp != qty_units_6dp
                        || existing.gross_proceeds_units_6dp != gross_proceeds_units_6dp
                        || existing.fee_units_6dp != fee_units_6dp
                        || existing.confirmed_at_ms != confirmed_at_ms
                        || existing.close_reason != close_reason
                    {
                        return Err(ManagedFillAdmissionError::Definitive(
                            "conflicting_lifecycle_sell_receipt_reuse",
                        ));
                    }
                    return Ok((qty_units_6dp, true));
                }
                let position = self
                    .positions
                    .get(&fill.token)
                    .ok_or("missing_lifecycle_sell_inventory")?;
                if qty_units_6dp > position.qty_units_6dp
                    || qty_units_6dp > position.sellable_qty_units_6dp
                {
                    return Err(ManagedFillAdmissionError::Definitive(
                        "lifecycle_sell_receipt_exceeds_owned_inventory",
                    ));
                }
                let disposition = append_confirmed_lifecycle_sell_receipt(
                    fill,
                    fill_id,
                    lifecycle_cohort,
                    &market_id,
                    intent_id,
                    qty_units_6dp,
                    gross_proceeds_units_6dp,
                    fee_units_6dp,
                    confirmed_at_ms,
                    close_reason,
                    matched_trade.cloned(),
                    append_record,
                )
                .map_err(ManagedFillAdmissionError::Unresolved)?;
                if disposition == FinancialAdmissionDisposition::DefinitivelyRejected {
                    return Err(ManagedFillAdmissionError::Definitive(
                        "confirmed_lifecycle_sell_receipt_definitively_rejected",
                    ));
                }
                let receipt_admission_at = std::time::Instant::now();
                if let (Some(timing), Some(capture)) = (execution_timing, timing_capture) {
                    emit_receipt_admission_timing(
                        capture,
                        timing,
                        &fill.order_id,
                        format!("ctf_managed_sell:{fill_id}"),
                        confirmed_at_ms,
                        receipt_admission_at,
                    );
                }
                self.confirmed_lifecycle_sell_fills.insert(
                    key,
                    ConfirmedLifecycleSellFill {
                        fill_id: fill_id.to_string(),
                        receipt_identity: Some(
                            fill.receipt_identity
                                .clone()
                                .ok_or("missing_lifecycle_sell_receipt_identity")?,
                        ),
                        order_id: fill.order_id.clone(),
                        lifecycle_cohort: lifecycle_cohort.to_string(),
                        market_id,
                        token: fill.token.clone(),
                        intent_id,
                        qty_units_6dp,
                        gross_proceeds_units_6dp,
                        fee_units_6dp,
                        confirmed_at_ms,
                        close_reason,
                    },
                );
                Ok((qty_units_6dp, false))
            })();
            match receipt_result {
                Ok((_, true)) => return FillAdmission::Admitted,
                Ok((qty_units_6dp, false)) => Some(qty_units_6dp),
                Err(ManagedFillAdmissionError::Definitive(reason)) => {
                    let (fill, _, _, _, _, _) = retained_fill.into_parts();
                    self.quarantine_unmanaged_fill(fill, reason);
                    return FillAdmission::DefinitivelyRejected;
                }
                Err(ManagedFillAdmissionError::Unresolved(error)) => {
                    return FillAdmission::Unresolved {
                        fill: retained_fill,
                        error,
                    };
                }
            }
        } else {
            None
        };

        let (fill, sell_exit_reason, _, _, _, _order_context_owner) = retained_fill.into_parts();

        self.seen_fill_keys.insert(dedupe_key);
        if let Some(market_id) = fill.market_id.as_ref() {
            let trimmed = market_id.trim();
            if !trimmed.is_empty() && trimmed != "-" {
                self.token_market_hint
                    .insert(fill.token.clone(), MarketId::new(trimmed));
            }
        }
        self.record_fill_quality(&fill);

        self.next_fill_seq = self.next_fill_seq.saturating_add(1);
        self.fills.push_back(QueuedFill {
            seq: self.next_fill_seq,
            fill: fill.clone(),
        });
        while self.fills.len() > MAX_FILL_QUEUE_LEN {
            let _ = self.fills.pop_front();
        }

        match fill.side {
            FillSide::Buy => {
                let fee = fee.expect("buy fill fee validated before portfolio mutation");
                let net_qty = net_buy_shares(&fill, fee);
                if net_qty <= EPS {
                    return FillAdmission::DefinitivelyRejected;
                }
                let total_notional = fill.px * fill.size;
                if !total_notional.is_finite() || total_notional <= 0.0 {
                    return FillAdmission::DefinitivelyRejected;
                }
                self.apply_buy_position_change(
                    &fill.token,
                    fill.market_id.as_deref(),
                    fill.ts_ms,
                    fill.px,
                    net_qty,
                    total_notional,
                    fee,
                    fill.quote_group.clone(),
                    Some(fill.order_id.clone()),
                    canonical_fill_id,
                    fill.transaction_hash.clone(),
                    None,
                )
                .expect("validated ordinary BUY lot quantity converts to exact units");
            }
            FillSide::Sell => {
                let close_reason = sell_exit_reason
                    .expect("normalized strategy sell fill carries order close reason");
                let Some(market_id) = self.market_id_for_fill(&fill) else {
                    self.quarantine_fill(fill, "missing_market_identity");
                    return FillAdmission::DefinitivelyRejected;
                };
                self.apply_sell_position_change(
                    &fill.token,
                    market_id,
                    fill.ts_ms,
                    fill.px,
                    fill.size,
                    managed_receipt_qty_units_6dp,
                    fee,
                    close_reason,
                    Some(fill.order_id.clone()),
                    canonical_fill_id,
                    fill.transaction_hash.clone(),
                    fill.lifecycle_cohort.clone(),
                );
            }
        }

        self.refresh_position_aggregates();
        self.refresh_positions_metadata(updated_at_ms);
        FillAdmission::Admitted
    }

    pub(crate) fn quarantine_unmanaged_fill(&mut self, fill: FillEvent, reason: &str) {
        self.next_fill_seq = self.next_fill_seq.saturating_add(1);
        self.quarantine_fill(fill, reason);
    }

    fn quarantine_fill(&mut self, fill: FillEvent, reason: &str) {
        tracing::warn!(
            target: "portfolio.fills",
            reason,
            order_id = %fill.order_id,
            trade_id = fill.trade_id.as_deref().unwrap_or("-"),
            token = %fill.token,
            market_id = fill.market_id.as_deref().unwrap_or("-"),
            side = fill.side.as_str(),
            quote_group = fill.quote_group.as_deref().unwrap_or("-"),
            px = fill.px,
            size = fill.size,
            "quarantined_fill"
        );
        self.quarantined_fills.push_back(QuarantinedFill {
            seq: self.next_fill_seq,
            reason: reason.to_string(),
            fill,
        });
        while self.quarantined_fills.len() > MAX_QUARANTINED_FILL_QUEUE_LEN {
            let _ = self.quarantined_fills.pop_front();
        }
    }

    fn record_fill_quality(&mut self, fill: &FillEvent) {
        self.fill_quality.fills_total = self.fill_quality.fills_total.saturating_add(1);
        if fill.at_top_of_book.unwrap_or(false) {
            self.fill_quality.top_of_book_fills =
                self.fill_quality.top_of_book_fills.saturating_add(1);
        }
        if let Some(queue_ahead) = fill.queue_ahead_at_fill {
            if queue_ahead.is_finite() && queue_ahead >= 0.0 {
                self.fill_quality.queue_ahead_sum += queue_ahead;
                self.fill_quality.queue_ahead_samples =
                    self.fill_quality.queue_ahead_samples.saturating_add(1);
            }
        }
        if let Some(time_ms) = fill.time_to_fill_ms {
            self.fill_quality.time_to_fill_ms_sum = self
                .fill_quality
                .time_to_fill_ms_sum
                .saturating_add(time_ms as u128);
            self.fill_quality.time_to_fill_samples =
                self.fill_quality.time_to_fill_samples.saturating_add(1);
        }
    }
}

fn fill_dedupe_key(fill: &FillEvent) -> FillDedupeKey {
    if let Some(fill_id) = fill
        .receipt_identity
        .as_ref()
        .and_then(|_| fill.canonical_fill_id())
    {
        return FillDedupeKey::FinalizedReceipt { fill_id };
    }
    if let Some(trade_id) = fill
        .trade_id
        .as_ref()
        .map(|id| id.trim())
        .filter(|id| !id.is_empty())
    {
        return FillDedupeKey::VenueFill {
            trade_id: trade_id.to_string(),
            order_id: fill.order_id.clone(),
        };
    }
    let px_key = (fill.px * 1_000_000.0).round() as i64;
    let size_key = (fill.size * 1_000_000.0).round() as i64;
    FillDedupeKey::OrderTimestampPriceSize {
        order_id: fill.order_id.clone(),
        ts_ms: fill.ts_ms,
        px_key,
        size_key,
    }
}
