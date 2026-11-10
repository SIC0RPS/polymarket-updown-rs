use anyhow::{Context, Result};
use polymarket_client_sdk::types::U256;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

use crate::execution::SimSubmittedOrder;
use crate::orders::OrderLifecycleKey;
use crate::polymarket::state::{book_for_token, PolymarketOddsState};
use crate::portfolio::FillEvent;
use crate::strategy_runtime::backtest_live_latency::{
    sample_observation_index, stable_latency_seed, ReplayClobTimingObservation,
    ReplayExecutionTimingModel, EXECUTION_TIMING_MODEL_VERSION_V5,
};
use crate::strategy_runtime::backtest_matrix_report::BacktestClobTimingDiagnostic;
use crate::types::{Side, TimeInForce};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapturedClobSubmitModelIdentity {
    pub(crate) model_version: String,
    pub(crate) artifact_sha256: [u8; 32],
    pub(crate) captured_rows: u64,
    /// Representative applied delay for reports (avg of active profile).
    pub(crate) applied_submit_latency_ms: u64,
    pub(crate) applied_itode_delay_ms: u64,
    pub(crate) latency_min_ms: u64,
    pub(crate) latency_max_ms: u64,
    pub(crate) source_capture: Option<String>,
    pub(crate) source_config_sha256: Option<String>,
    pub(crate) journal_path: Option<String>,
    pub(crate) journal_sha256: Option<String>,
    pub(crate) endpoint: String,
    pub(crate) unit: String,
    pub(crate) current_rule_source: String,
}

#[derive(Debug, Clone)]
pub(crate) struct CapturedClobSubmitModel {
    identity: CapturedClobSubmitModelIdentity,
    observations: Vec<ReplayClobTimingObservation>,
}

impl CapturedClobSubmitModel {
    pub(crate) fn from_execution_timing(model: &ReplayExecutionTimingModel) -> Result<Self> {
        let artifact = serde_json::to_vec(model).context("serialize CLOB timing model")?;
        let artifact_sha256 = Sha256::digest(&artifact).into();
        let observations = model
            .clob
            .as_ref()
            .map(|source| source.observations.clone())
            .unwrap_or_default();
        let values = observations
            .iter()
            .map(|sample| sample.invocation_elapsed_us.saturating_add(999) / 1_000)
            .collect::<Vec<_>>();
        let profile = (!values.is_empty())
            .then(|| crate::strategy_runtime::backtest_live_latency::distribution(&values))
            .transpose()?;
        Ok(Self {
            identity: CapturedClobSubmitModelIdentity {
                model_version: model.model_version.clone(),
                artifact_sha256,
                captured_rows: observations.len() as u64,
                applied_submit_latency_ms: profile
                    .as_ref()
                    .and_then(|value| value.avg)
                    .unwrap_or(0),
                applied_itode_delay_ms: model.current_marketable_taker_hold_us / 1_000,
                latency_min_ms: profile.as_ref().and_then(|value| value.min).unwrap_or(0),
                latency_max_ms: profile.as_ref().and_then(|value| value.max).unwrap_or(0),
                source_capture: model
                    .clob
                    .as_ref()
                    .map(|source| source.source_capture.clone()),
                source_config_sha256: model
                    .clob
                    .as_ref()
                    .and_then(|source| source.source_config_sha256.clone()),
                journal_path: model
                    .clob
                    .as_ref()
                    .map(|source| source.journal_path.clone()),
                journal_sha256: model
                    .clob
                    .as_ref()
                    .map(|source| source.journal_sha256.clone()),
                endpoint: "post_request_send_to_synchronous_response".to_string(),
                unit: "microseconds".to_string(),
                current_rule_source: model.current_marketable_taker_hold_source.clone(),
            },
            observations,
        })
    }

    pub(crate) fn clock(&self) -> CapturedClobSubmitClock {
        CapturedClobSubmitClock {
            identity: self.identity.clone(),
            pending: BTreeMap::new(),
            pending_by_order_id: BTreeMap::new(),
            seen_order_ids: BTreeSet::new(),
            next_acceptance_sequence: 1,
            observations: self.observations.clone(),
            diagnostics: Vec::new(),
            supported_admission_submissions: 0,
            approximate_admission_submissions: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct DueClobSubmissions {
    pub(crate) due_ts_ms: u64,
    pub(crate) events: Vec<DueClobEvent>,
}

#[derive(Debug)]
pub(crate) enum DueClobEvent {
    Submission {
        order: SimSubmittedOrder,
        receipt_due_ts_ms: Option<u64>,
    },
    FinancialReceipt(FillEvent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct PendingKey {
    due_ts_ms: u64,
    acceptance_sequence: u64,
}

#[derive(Debug)]
pub(crate) struct CapturedClobSubmitClock {
    identity: CapturedClobSubmitModelIdentity,
    pending: BTreeMap<PendingKey, DueClobEvent>,
    pending_by_order_id: BTreeMap<String, PendingKey>,
    seen_order_ids: BTreeSet<String>,
    next_acceptance_sequence: u64,
    observations: Vec<ReplayClobTimingObservation>,
    diagnostics: Vec<BacktestClobTimingDiagnostic>,
    supported_admission_submissions: u64,
    approximate_admission_submissions: u64,
}

impl CapturedClobSubmitClock {
    pub(crate) fn identity(&self) -> &CapturedClobSubmitModelIdentity {
        &self.identity
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.pending_by_order_id.len()
    }

    pub(crate) fn pending_receipts_len(&self) -> usize {
        self.pending.len() - self.pending_by_order_id.len()
    }

    pub(crate) fn admission_coverage(&self) -> (u64, u64) {
        (
            self.supported_admission_submissions,
            self.approximate_admission_submissions,
        )
    }

    pub(crate) fn diagnostics(&self) -> &[BacktestClobTimingDiagnostic] {
        &self.diagnostics
    }

    pub(crate) fn next_due_ts(&self) -> Option<u64> {
        self.pending.first_key_value().map(|(key, _)| key.due_ts_ms)
    }

    pub(crate) fn schedule(
        &mut self,
        submitted: SimSubmittedOrder,
        accepted_ts_ms: u64,
        market_itode: Option<bool>,
        odds: &PolymarketOddsState,
    ) -> Result<u64> {
        anyhow::ensure!(
            accepted_ts_ms > 0,
            "captured CLOB acceptance timestamp is zero"
        );
        let order_id = submitted.order_id.trim().to_string();
        anyhow::ensure!(
            !order_id.is_empty() && order_id == submitted.order_id,
            "captured CLOB order identity is empty or noncanonical"
        );
        anyhow::ensure!(
            self.seen_order_ids.insert(order_id.clone()),
            "duplicate captured CLOB order identity order_id={order_id}"
        );

        let intent = &submitted.intent;
        let marketable = if intent.post_only {
            anyhow::ensure!(
                !intent.time_in_force.is_immediate(),
                "captured CLOB post-only order has immediate time-in-force"
            );
            false
        } else {
            let token = intent
                .instrument
                .parse::<U256>()
                .context("captured CLOB order token identity is invalid")?;
            let book = book_for_token(odds, token)
                .context("captured CLOB order book is unavailable at acceptance")?;
            let limit_px = crate::ctf_accounting::qty_to_units_6dp(intent.limit_px)
                .context("captured CLOB order price is not exact fixed-6dp")?;
            let crossing_px = match match intent.side() {
                Side::Buy => book.ask,
                Side::Sell => book.bid,
            } {
                Some(price) => Some(
                    crate::ctf_accounting::qty_to_units_6dp(price)
                        .context("captured CLOB crossing book price is not exact fixed-6dp")?,
                ),
                None => None,
            };
            crossing_px.is_some_and(|crossing_px| match intent.side() {
                Side::Buy => limit_px >= crossing_px,
                Side::Sell => limit_px <= crossing_px,
            })
        };
        let itode_delay_ms = if market_itode.unwrap_or(false) && marketable {
            self.identity.applied_itode_delay_ms
        } else {
            0
        };
        let (market, token, side, purpose, symbol) = if intent.side() == Side::Buy {
            let authorization = intent
                .buy_authorization()
                .context("captured CLOB BUY has no entry authorization")?;
            authorization.validate()?;
            (
                authorization.condition_id.to_string(),
                authorization.selected_token().to_string(),
                "buy",
                "direct_buy",
                authorization.symbol.as_str().to_string(),
            )
        } else {
            let lifecycle = OrderLifecycleKey::from_intent(intent)
                .context("captured CLOB order has no canonical lifecycle identity")?;
            let side = match lifecycle.side {
                Side::Buy => "buy",
                Side::Sell => "sell",
            };
            (
                lifecycle.market,
                lifecycle.token,
                side,
                lifecycle.close_reason.as_str(),
                lifecycle.symbol,
            )
        };
        let instruction_qty = intent.execution_instruction_qty_units_6dp.to_string();
        let instruction_limit_px = intent.execution_instruction_limit_px_units_8dp.to_string();
        let seed = stable_latency_seed(
            &[
                market.as_str(),
                token.as_str(),
                side,
                purpose,
                symbol.as_str(),
                instruction_qty.as_str(),
                instruction_limit_px.as_str(),
                intent.time_in_force.as_str(),
                if intent.post_only {
                    "post_only"
                } else {
                    "not_post_only"
                },
                if marketable { "mkt" } else { "rest" },
            ],
            accepted_ts_ms,
        );
        let sample_index = sample_observation_index(self.observations.len(), seed)
            .context("CLOB timing is uncalibrated; submission was not simulated")?;
        let sample = &self.observations[sample_index];
        let simulated_request_send_us = accepted_ts_ms
            .checked_mul(1_000)
            .and_then(|value| value.checked_add(sample.decision_to_handoff_us))
            .and_then(|value| value.checked_add(sample.handoff_to_send_invocation_us))
            .context("captured CLOB request-send timestamp overflow")?;
        let qualifying_hold_us = itode_delay_ms.saturating_mul(1_000);
        let modeled_delay_us = sample.invocation_elapsed_us.max(qualifying_hold_us);
        let absolute_due_us = simulated_request_send_us
            .checked_add(modeled_delay_us)
            .context("captured CLOB due timestamp overflow")?;
        let due_ts_ms = absolute_due_us
            .checked_add(999)
            .context("captured CLOB due timestamp rounding overflow")?
            / 1_000;
        let receipt_admission = sample.receipt_admission.as_ref().filter(|_| {
            self.identity.model_version == EXECUTION_TIMING_MODEL_VERSION_V5
                && !intent.post_only
                && intent.time_in_force == TimeInForce::Fak
                && intent.side() == Side::Sell
        });
        let absolute_receipt_due_us = receipt_admission
            .and_then(|admission| {
                simulated_request_send_us
                    .checked_add(sample.invocation_elapsed_us)
                    .and_then(|value| value.checked_add(admission.response_to_canonical_append_us))
            })
            .filter(|deadline| *deadline >= absolute_due_us);
        let receipt_due_ts_ms = absolute_receipt_due_us
            .map(|deadline| {
                deadline
                    .checked_add(999)
                    .context("captured CLOB receipt timestamp rounding overflow")
                    .map(|value| value / 1_000)
            })
            .transpose()?;
        if receipt_due_ts_ms.is_some() {
            self.supported_admission_submissions += 1;
        } else {
            self.approximate_admission_submissions += 1;
        }
        if self.diagnostics.len() < 16 {
            self.diagnostics.push(BacktestClobTimingDiagnostic {
                order_id: order_id.clone(),
                sample_index,
                accepted_ts_ms,
                simulated_request_send_us,
                sampled_response_duration_us: sample.invocation_elapsed_us,
                qualifying_current_venue_hold_us: qualifying_hold_us,
                modeled_delay_us,
                absolute_due_us,
                due_ts_ms,
                admission_timing: (self.identity.model_version
                    == EXECUTION_TIMING_MODEL_VERSION_V5)
                    .then(|| {
                        if receipt_due_ts_ms.is_some() {
                            "measured_response_to_canonical_append"
                        } else {
                            "approximate_response_matching"
                        }
                        .to_string()
                    }),
                response_to_canonical_append_us: receipt_admission
                    .map(|admission| admission.response_to_canonical_append_us),
                absolute_receipt_due_us,
                receipt_due_ts_ms,
            });
        }
        let key = self.next_pending_key(due_ts_ms)?;
        anyhow::ensure!(
            self.pending
                .insert(
                    key,
                    DueClobEvent::Submission {
                        order: submitted,
                        receipt_due_ts_ms,
                    },
                )
                .is_none(),
            "duplicate captured CLOB pending key"
        );
        anyhow::ensure!(
            self.pending_by_order_id
                .insert(order_id.clone(), key)
                .is_none(),
            "duplicate captured CLOB pending order identity order_id={order_id}"
        );
        Ok(due_ts_ms)
    }

    pub(crate) fn cancel_pending(&mut self, order_ids: &[String]) -> Result<Vec<String>> {
        let mut visible = Vec::new();
        for order_id in order_ids {
            let Some(key) = self.pending_by_order_id.remove(order_id) else {
                visible.push(order_id.clone());
                continue;
            };
            let removed = self
                .pending
                .remove(&key)
                .context("captured CLOB pending order disappeared before cancellation")?;
            anyhow::ensure!(
                matches!(removed, DueClobEvent::Submission { order, .. } if order.order_id == *order_id),
                "captured CLOB pending cancellation identity mismatch"
            );
        }
        Ok(visible)
    }

    fn next_pending_key(&mut self, due_ts_ms: u64) -> Result<PendingKey> {
        let acceptance_sequence = self.next_acceptance_sequence;
        self.next_acceptance_sequence = acceptance_sequence
            .checked_add(1)
            .context("captured CLOB acceptance sequence overflow")?;
        Ok(PendingKey {
            due_ts_ms,
            acceptance_sequence,
        })
    }

    pub(crate) fn retain_financial_receipt(
        &mut self,
        fill: FillEvent,
        due_ts_ms: u64,
    ) -> Result<()> {
        anyhow::ensure!(
            due_ts_ms >= fill.ts_ms && fill.receipt_qty_units_6dp.is_some_and(|qty| qty > 0),
            "captured CLOB financial receipt deadline or quantity is invalid"
        );
        let key = self.next_pending_key(due_ts_ms)?;
        anyhow::ensure!(
            self.pending
                .insert(key, DueClobEvent::FinancialReceipt(fill))
                .is_none(),
            "duplicate captured CLOB financial receipt key"
        );
        Ok(())
    }

    pub(crate) fn pop_next_due(&mut self, up_to_ts_ms: u64) -> Result<Option<DueClobSubmissions>> {
        let Some(due_ts_ms) = self.next_due_ts().filter(|due| *due <= up_to_ts_ms) else {
            return Ok(None);
        };
        let mut events = Vec::new();
        while self
            .pending
            .first_key_value()
            .is_some_and(|(key, _)| key.due_ts_ms == due_ts_ms)
        {
            let (pending_key, mut event) = self
                .pending
                .pop_first()
                .context("captured CLOB pending queue changed while draining")?;
            match &mut event {
                DueClobEvent::Submission { order, .. } => {
                    let key = self
                        .pending_by_order_id
                        .remove(&order.order_id)
                        .context("captured CLOB pending identity disappeared while draining")?;
                    anyhow::ensure!(
                        key == pending_key,
                        "captured CLOB pending due identity mismatch"
                    );
                }
                DueClobEvent::FinancialReceipt(fill) => {
                    fill.ts_ms = due_ts_ms;
                    fill.receipt_confirmed_at_ms = Some(due_ts_ms);
                }
            }
            events.push(event);
        }
        anyhow::ensure!(!events.is_empty(), "captured CLOB due batch is empty");
        Ok(Some(DueClobSubmissions { due_ts_ms, events }))
    }

    pub(crate) fn reset(&mut self) {
        self.pending.clear();
        self.pending_by_order_id.clear();
        self.seen_order_ids.clear();
        self.next_acceptance_sequence = 1;
        self.diagnostics.clear();
        self.supported_admission_submissions = 0;
        self.approximate_admission_submissions = 0;
    }
}
