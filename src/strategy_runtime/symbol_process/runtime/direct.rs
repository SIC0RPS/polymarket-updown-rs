use std::sync::Arc;

use anyhow::{Context, Result};

use super::CandidateSymbolRuntime;
use crate::capture::events::{StrategyConsumptionInput, StrategyConsumptionRecord};
use crate::polymarket::state::{MarketProjection, PolymarketOddsState};
use crate::portfolio::PortfolioState;
use crate::strategy_runtime::prediction_metrics::PredictionMetrics;
use crate::strategy_runtime::runtime_driver::RuntimeInput;
use crate::strategy_runtime::symbol_process::{
    ActiveLifecycleDeltaRecord, CandidateReset, Drive, DriveKind, EventRef, Finalize,
    InventoryReceiptRecord, ManagedSellReceiptRecord, MarketBudgetRecord, OpenOrderDeltaRecord,
    PositionDeltaRecord, ProtocolMode, ResultIdentity, RiskBudget, WorkerCommandBatch,
};

impl CandidateSymbolRuntime {
    pub(crate) fn reset_candidate(
        &mut self,
        reset: &CandidateReset,
        initial_sequence: u64,
        initial_oracle_learning: Option<crate::strategy::OracleLearningSnapshot>,
        oracle_direct_market_research: bool,
        collect_prediction_metrics: bool,
    ) -> Result<()> {
        anyhow::ensure!(
            self.candidate.is_none(),
            "candidate symbol runtime reset before prior candidate finalized symbol={}",
            self.symbol.as_str()
        );
        reset.validate()?;
        anyhow::ensure!(
            reset.base_config_sha256 == self.base_sha256,
            "candidate reset base config identity mismatch"
        );
        anyhow::ensure!(
            reset.parameter_schema_sha256 == self.parameter_schema.sha256(),
            "candidate reset parameter schema identity mismatch"
        );
        let candidate = self.build_candidate(
            reset,
            initial_oracle_learning,
            oracle_direct_market_research,
            collect_prediction_metrics,
        )?;
        self.worker_epoch = self
            .worker_epoch
            .checked_add(1)
            .context("candidate symbol runtime epoch overflow")?;
        self.next_intent_id = 1;
        self.next_owner_audit_seq = 1;
        self.last_global_sequence = (initial_sequence > 0).then_some(initial_sequence);
        self.last_central_state_seq = 0;
        self.local_event_count = 0;
        self.candidate = Some(candidate);
        Ok(())
    }

    pub(crate) fn commit_views(
        &mut self,
        source_sequence: u64,
        _timestamp_ms: u64,
        portfolio: &PortfolioState,
        financial: crate::trade_session_ledger::CanonicalFinancialView<'_>,
    ) -> Result<()> {
        self.validate_current_source(source_sequence)?;
        let candidate = self.candidate_mut()?;
        candidate.portfolio.clone_from(portfolio);
        candidate.portfolio.project_canonical_buy_basis(financial);
        super::helpers::apply_views(candidate)
    }

    pub(crate) fn commit_odds(
        &mut self,
        odds: Arc<PolymarketOddsState>,
        projection: MarketProjection,
        projection_catalog_sha256: [u8; 32],
        source_sequence: u64,
        timestamp_ms: u64,
    ) -> Result<()> {
        anyhow::ensure!(
            source_sequence > 0,
            "candidate odds source sequence is zero"
        );
        anyhow::ensure!(
            self.last_global_sequence
                .is_none_or(|current| source_sequence > current),
            "candidate odds source sequence is not strictly increasing symbol={} current={:?} next={source_sequence}",
            self.symbol.as_str(),
            self.last_global_sequence
        );
        anyhow::ensure!(
            odds.updated_at_ms <= timestamp_ms,
            "candidate symbol odds timestamp exceeds central frame symbol={} odds_ms={} frame_ms={timestamp_ms}",
            self.symbol.as_str(),
            odds.updated_at_ms
        );
        anyhow::ensure!(
            odds.markets
                .iter()
                .chain(odds.selection.iter())
                .chain(odds.next_selection.iter())
                .all(|market| market.underlying.eq_ignore_ascii_case(self.symbol.as_str())),
            "captured strategy odds contain a foreign symbol actor={}",
            self.symbol.as_str()
        );
        anyhow::ensure!(
            projection_catalog_sha256 == self.catalog.sha256(),
            "candidate odds projection catalog identity mismatch symbol={}",
            self.symbol.as_str()
        );
        anyhow::ensure!(
            projection.source_revision() == odds.revision
                && projection.source_updated_at_ms() == odds.updated_at_ms,
            "candidate odds projection source mismatch symbol={} revision={}/{} updated_at_ms={}/{}",
            self.symbol.as_str(),
            projection.source_revision(),
            odds.revision,
            projection.source_updated_at_ms(),
            odds.updated_at_ms
        );
        self.candidate_mut()?
            .actor
            .apply_symbol_odds(odds, projection)
    }

    pub(crate) fn apply_position_deltas(
        &mut self,
        source_sequence: u64,
        central_state_seq: u64,
        timestamp_ms: u64,
        records: &[PositionDeltaRecord],
    ) -> Result<()> {
        self.validate_central_source(source_sequence, central_state_seq)?;
        for record in records {
            record.validate()?.validate_catalog(&self.catalog)?;
        }
        self.ingest_position_deltas(records, timestamp_ms)?;
        self.last_central_state_seq = central_state_seq;
        Ok(())
    }

    pub(crate) fn apply_open_order_deltas(
        &mut self,
        source_sequence: u64,
        central_state_seq: u64,
        timestamp_ms: u64,
        records: &[OpenOrderDeltaRecord],
    ) -> Result<()> {
        self.validate_central_source(source_sequence, central_state_seq)?;
        for record in records {
            record.validate()?.validate_catalog(&self.catalog)?;
        }
        self.ingest_open_order_deltas(records, timestamp_ms)?;
        self.last_central_state_seq = central_state_seq;
        Ok(())
    }

    pub(crate) fn apply_active_lifecycle_deltas(
        &mut self,
        source_sequence: u64,
        central_state_seq: u64,
        timestamp_ms: u64,
        records: &[ActiveLifecycleDeltaRecord],
    ) -> Result<()> {
        self.validate_central_source(source_sequence, central_state_seq)?;
        for record in records {
            record.validate()?.validate_catalog(&self.catalog)?;
        }
        self.ingest_active_lifecycle_deltas(records, timestamp_ms)?;
        self.last_central_state_seq = central_state_seq;
        Ok(())
    }

    pub(crate) fn apply_managed_sell_receipts(
        &mut self,
        source_sequence: u64,
        central_state_seq: u64,
        timestamp_ms: u64,
        records: &[ManagedSellReceiptRecord],
    ) -> Result<()> {
        self.validate_central_source(source_sequence, central_state_seq)?;
        for record in records {
            record.validate()?.validate_catalog(&self.catalog)?;
            anyhow::ensure!(
                record.confirmed_at_ms <= timestamp_ms,
                "managed-sell receipt confirmation exceeds central frame symbol={} confirmed_ms={} frame_ms={timestamp_ms}",
                self.symbol.as_str(),
                record.confirmed_at_ms
            );
        }
        self.ingest_managed_sell_receipts(records, timestamp_ms)?;
        self.last_central_state_seq = central_state_seq;
        Ok(())
    }

    pub(crate) fn apply_inventory_receipts(
        &mut self,
        source_sequence: u64,
        central_state_seq: u64,
        timestamp_ms: u64,
        records: &[InventoryReceiptRecord],
    ) -> Result<()> {
        self.validate_central_source(source_sequence, central_state_seq)?;
        for record in records {
            record.validate()?.validate_catalog(&self.catalog)?;
            anyhow::ensure!(
                record.confirmed_at_ms <= timestamp_ms,
                "inventory receipt confirmation exceeds central frame symbol={} confirmed_ms={} frame_ms={timestamp_ms}",
                self.symbol.as_str(),
                record.confirmed_at_ms
            );
        }
        self.ingest_inventory_receipts(records, timestamp_ms)?;
        self.last_central_state_seq = central_state_seq;
        Ok(())
    }

    pub(crate) fn apply_risk_budget(&mut self, budget: RiskBudget) -> Result<()> {
        budget.validate()?;
        self.validate_central_source(budget.source_sequence, budget.central_state_seq)?;
        self.begin_risk_view(budget)?;
        self.last_central_state_seq = budget.central_state_seq;
        Ok(())
    }

    pub(crate) fn apply_market_budgets(
        &mut self,
        source_sequence: u64,
        central_state_seq: u64,
        _timestamp_ms: u64,
        records: &[MarketBudgetRecord],
    ) -> Result<()> {
        self.validate_central_source(source_sequence, central_state_seq)?;
        for record in records {
            record.validate()?.validate_catalog(&self.catalog)?;
        }
        self.complete_risk_view(records)?;
        self.last_central_state_seq = central_state_seq;
        Ok(())
    }

    pub(crate) fn process_event(
        &mut self,
        event: EventRef,
        record: &StrategyConsumptionRecord,
        projection: MarketProjection,
        projection_catalog_sha256: [u8; 32],
    ) -> Result<WorkerCommandBatch> {
        anyhow::ensure!(
            self.mode != ProtocolMode::Live,
            "live symbol runtime cannot consume prepared EventRef"
        );
        anyhow::ensure!(
            self.candidate.is_some(),
            "candidate symbol runtime has no active candidate"
        );
        event.validate_wire(self.symbol as u8)?;
        anyhow::ensure!(
            event.sequence > 0
                && self
                    .last_global_sequence
                    .is_none_or(|previous| event.sequence > previous),
            "candidate symbol event sequence is not strictly monotonic symbol={} previous={:?} actual={}",
            self.symbol.as_str(),
            self.last_global_sequence,
            event.sequence
        );
        anyhow::ensure!(
            event.kind
                == crate::strategy_runtime::replay_mode::prepared_event_stream::EVENT_KIND_STRATEGY_CONSUMPTION,
            "candidate symbol runtime requires a strategy consumption event"
        );
        let (path, file_batch, batch_row) = self.tape.strategy_consumption_location(event)?;
        record.validate_schema()?;
        anyhow::ensure!(
            record.actor_symbol == self.symbol,
            "strategy consumption actor is foreign expected={} actual={}",
            self.symbol.as_str(),
            record.actor_symbol.as_str()
        );
        anyhow::ensure!(
            record.consumption_sequence == event.sequence
                && record.consumed_at_ms == event.timestamp_ms,
            "strategy consumption EventRef differs from captured sequence or timestamp file={} batch={file_batch} row={batch_row} global_row={} expected_sequence={} actual_sequence={} expected_timestamp_ms={} actual_timestamp_ms={}",
            path.display(),
            event.start,
            event.sequence,
            record.consumption_sequence,
            event.timestamp_ms,
            record.consumed_at_ms
        );
        let sequence = record.consumption_sequence;
        let timestamp_ms = record.consumed_at_ms;
        let decision_timestamp_ms = match &record.input {
            StrategyConsumptionInput::Runtime(RuntimeInput::AcceptedTick { tick }) => {
                crate::strategy::support::shared::common::ts_ms_from_ns(tick.ts_ns)
            }
            StrategyConsumptionInput::Runtime(RuntimeInput::Heartbeat { now_ms, .. })
            | StrategyConsumptionInput::Runtime(RuntimeInput::PortfolioChanged {
                now_ms,
                ..
            })
            | StrategyConsumptionInput::Runtime(RuntimeInput::PolymarketDataChanged {
                now_ms,
                ..
            }) => Some(*now_ms),
            StrategyConsumptionInput::Chainlink {
                learning_applied_ts_ms,
                ..
            } => Some(*learning_applied_ts_ms),
        }
        .with_context(|| {
            format!(
                "strategy consumption input has no decision timestamp file={} batch={file_batch} row={batch_row} global_row={}",
                path.display(),
                event.start
            )
        })?;
        anyhow::ensure!(
            decision_timestamp_ms > 0,
            "strategy consumption decision timestamp is zero file={} batch={file_batch} row={batch_row} global_row={}",
            path.display(),
            event.start
        );
        anyhow::ensure!(
            decision_timestamp_ms <= timestamp_ms,
            "strategy consumption decision timestamp exceeds consumption timestamp file={} batch={file_batch} row={batch_row} global_row={} decision={} consumption={}",
            path.display(),
            event.start,
            decision_timestamp_ms,
            timestamp_ms
        );
        let research_scope = tracing::enabled!(
            target: "strategy.oracle_probability",
            tracing::Level::TRACE
        )
        .then(|| {
            let candidate_sha256 = self
                .candidate
                .as_ref()
                .map(|candidate| candidate.candidate_sha256);
            tracing::trace_span!(
                target: "strategy.oracle_probability",
                "oracle_research_worker",
                candidate_slot = self.candidate_slot,
                candidate_sha256 = ?candidate_sha256,
                symbol = self.symbol.as_str(),
                global_sequence = event.sequence,
                decision_eval_ts_ms = decision_timestamp_ms,
                materialization_drive = false,
            )
        });
        let _research_scope_guard = research_scope.as_ref().map(|span| span.enter());
        self.commit_odds(
            Arc::clone(&record.odds),
            projection,
            projection_catalog_sha256,
            sequence,
            timestamp_ms,
        )?;
        if record.schema_version > 1 {
            let candidate = self.candidate_mut()?;
            candidate
                .actor
                .strategy
                .oracle_history
                .apply_consumed_state(
                    &candidate.actor.oracle_symbol,
                    record
                        .candle_state
                        .as_deref()
                        .context("missing resolved consumed candle state")?,
                    record.consumed_mono_ns,
                )?;
        }
        {
            let candidate = self.candidate_mut()?;
            candidate
                .actor
                .strategy
                .oracle_history
                .apply_consumed_trade_flow(
                    &candidate.actor.oracle_symbol,
                    record.trade_flow,
                    record.consumed_mono_ns,
                )?;
        }
        let output = self.process_consumption_output(&record.input)?;
        {
            let candidate = self
                .candidate
                .as_mut()
                .context("candidate state missing after recorded consumption")?;
            if let Some(prediction) = candidate.prediction.as_mut() {
                match &record.input {
                    StrategyConsumptionInput::Runtime(_) => prediction.observe_runtime(
                        &candidate.actor.strategy,
                        sequence,
                        timestamp_ms,
                    )?,
                    StrategyConsumptionInput::Chainlink { .. } => prediction.observe_labels(
                        &output.oracle_labels,
                        &self.catalog,
                        timestamp_ms,
                    )?,
                }
            }
        }
        let batch = self.finish_output(
            output,
            event.sequence,
            self.last_central_state_seq,
            decision_timestamp_ms,
        )?;
        self.last_global_sequence = Some(event.sequence);
        self.local_event_count = self
            .local_event_count
            .checked_add(1)
            .context("candidate symbol local event count overflow")?;
        Ok(batch)
    }

    pub(crate) fn drive(&mut self, drive: Drive, timestamp_ms: u64) -> Result<WorkerCommandBatch> {
        drive.validate()?;
        anyhow::ensure!(
            drive.kind == DriveKind::PortfolioChanged && self.mode != ProtocolMode::Live,
            "candidate symbol runtime only accepts replay PortfolioChanged drive"
        );
        anyhow::ensure!(
            drive.central_state_seq == self.last_central_state_seq
                && drive.source_sequence == self.last_global_sequence.unwrap_or(0),
            "candidate symbol drive cursor mismatch symbol={} source={}/{:?} central={}/{}",
            self.symbol.as_str(),
            drive.source_sequence,
            self.last_global_sequence,
            drive.central_state_seq,
            self.last_central_state_seq
        );
        let research_scope = tracing::enabled!(
            target: "strategy.oracle_probability",
            tracing::Level::TRACE
        )
        .then(|| {
            let candidate_sha256 = self
                .candidate
                .as_ref()
                .map(|candidate| candidate.candidate_sha256);
            tracing::trace_span!(
                target: "strategy.oracle_probability",
                "oracle_research_worker",
                candidate_slot = self.candidate_slot,
                candidate_sha256 = ?candidate_sha256,
                symbol = self.symbol.as_str(),
                global_sequence = drive.source_sequence,
                decision_eval_ts_ms = timestamp_ms,
                materialization_drive = true,
            )
        });
        let _research_scope_guard = research_scope.as_ref().map(|span| span.enter());
        let output = self.process_drive_output(drive, timestamp_ms)?;
        self.finish_output(
            output,
            drive.source_sequence,
            drive.central_state_seq,
            timestamp_ms,
        )
    }

    pub(crate) fn finalize(
        &mut self,
        finalize: Finalize,
        _timestamp_ms: u64,
        capture_identities: Vec<String>,
        knowledge_cutoff_ms: u64,
    ) -> Result<(
        ResultIdentity,
        crate::strategy::OracleLearningSnapshot,
        Option<PredictionMetrics>,
    )> {
        finalize.validate()?;
        anyhow::ensure!(
            self.mode != ProtocolMode::Live && self.candidate.is_some(),
            "candidate symbol runtime cannot finalize without an active replay candidate"
        );
        anyhow::ensure!(
            finalize.last_global_sequence == self.last_global_sequence.unwrap_or(0)
                && finalize.last_central_state_seq == self.last_central_state_seq
                && finalize.local_event_count == self.local_event_count,
            "candidate symbol finalize cursor mismatch symbol={} global={:?}/{} central={}/{} local={}/{}",
            self.symbol.as_str(),
            self.last_global_sequence,
            finalize.last_global_sequence,
            self.last_central_state_seq,
            finalize.last_central_state_seq,
            self.local_event_count,
            finalize.local_event_count
        );
        let result = self.finalize_identity(finalize)?.validate()?;
        anyhow::ensure!(
            result.candidate_sha256 == finalize.candidate_sha256
                && result.last_global_sequence == finalize.last_global_sequence
                && result.last_central_state_seq == finalize.last_central_state_seq
                && result.local_event_count == finalize.local_event_count,
            "candidate symbol final result identity mismatch symbol={}",
            self.symbol.as_str()
        );
        let learning = self
            .candidate
            .as_ref()
            .context("candidate symbol runtime lost state before learning export")?
            .actor
            .strategy
            .oracle_learning(capture_identities, knowledge_cutoff_ms)?;
        let prediction = self
            .candidate_mut()?
            .prediction
            .take()
            .map(|collector| collector.finish())
            .transpose()?;
        self.candidate = None;
        Ok((result, learning, prediction))
    }

    pub(crate) fn abort_candidate(&mut self) {
        self.candidate = None;
    }

    fn validate_current_source(&self, source_sequence: u64) -> Result<()> {
        anyhow::ensure!(
            self.last_global_sequence
                .map_or(source_sequence == 0, |current| source_sequence == current),
            "central view source mismatch symbol={} current={:?} actual={source_sequence}",
            self.symbol.as_str(),
            self.last_global_sequence
        );
        Ok(())
    }

    fn validate_central_source(&self, source_sequence: u64, central_state_seq: u64) -> Result<()> {
        self.validate_current_source(source_sequence)?;
        let expected = self
            .last_central_state_seq
            .checked_add(1)
            .context("candidate symbol central-state sequence overflow")?;
        anyhow::ensure!(
            central_state_seq == expected,
            "central-state sequence mismatch symbol={} expected={expected} actual={central_state_seq}",
            self.symbol.as_str()
        );
        Ok(())
    }
}
