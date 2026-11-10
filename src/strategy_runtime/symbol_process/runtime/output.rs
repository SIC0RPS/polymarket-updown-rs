use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};

use super::{CandidateSymbolRuntime, WorkerRuntimeOutput};
use crate::oracle_resolution::{
    OracleResolvedLabel, ResolutionBoundaryValue, CHAINLINK_RESOLUTION_PROVENANCE,
};
use crate::strategy::facts::{
    CtfFirstInstructionEconomicsEvidence, CtfLegDecisionReason, CtfLegEconomicOutcome,
    CtfOwnedLegCloseReason,
};
use crate::strategy::hold_to_resolution::PairTokenSide;
use crate::strategy_runtime::symbol_process::protocol::{
    EntryContextRecord, IntentRecord, OracleResolvedLabelRecord, OwnerAuditRecord,
    OWNER_AUDIT_POST_SELL_ECONOMICS_FLAG,
};
use crate::strategy_runtime::symbol_process::{
    intent_record_to_strategy_action, DecodedEntryContext, DecodedIntent, WorkerCommandBatch,
};
use crate::trade_session_ledger::UnjoinedOwnedLegOutcome;

impl CandidateSymbolRuntime {
    pub(super) fn finish_output(
        &mut self,
        mut output: WorkerRuntimeOutput,
        source_sequence: u64,
        central_state_seq: u64,
        timestamp_ms: u64,
    ) -> Result<WorkerCommandBatch> {
        let mut next_intent_id = self.next_intent_id;
        for intent in &mut output.intents {
            intent.intent_id = next_intent_id;
            intent.validate()?.validate_catalog(&self.catalog)?;
            next_intent_id = next_intent_id
                .checked_add(1)
                .context("candidate symbol intent sequence overflow")?;
        }
        remap_entry_context_intent_ids(&output.intents, &mut output.entry_contexts)?;
        remap_owner_audit_intent_ids(&output.intents, &mut output.owner_audits)?;

        let mut entry_contexts_by_intent = BTreeMap::new();
        for record in output.entry_contexts {
            record.validate()?;
            anyhow::ensure!(
                record.decision_ts_ms() <= timestamp_ms,
                "candidate symbol entry decision is after output symbol={}",
                self.symbol.as_str()
            );
            anyhow::ensure!(
                entry_contexts_by_intent
                    .insert(record.intent_id(), record)
                    .is_none(),
                "candidate symbol entry context intent identity is duplicated intent_id={}",
                record.intent_id()
            );
        }
        let oracle_symbol = self
            .base
            .oracle_config
            .resolve_symbol_for_underlying(self.symbol.as_str());
        let intents = output
            .intents
            .into_iter()
            .map(|record| -> Result<DecodedIntent> {
                let mut decoded = intent_record_to_strategy_action(
                    record,
                    &self.catalog,
                    self.symbol,
                    self.worker_epoch,
                )?;
                if let Some(context) = entry_contexts_by_intent.remove(&record.intent_id) {
                    context.validate_intent(record, &self.catalog, self.symbol)?;
                    decoded.entry_context = Some(match context {
                        EntryContextRecord::Split(context) => {
                            DecodedEntryContext::Split(super::super::intent::record_to_journal(
                                context,
                                record,
                                &self.catalog,
                                self.symbol,
                                oracle_symbol.as_str(),
                            )?)
                        }
                        EntryContextRecord::DirectBuy(authorization) => {
                            DecodedEntryContext::DirectBuy(authorization)
                        }
                    });
                }
                anyhow::ensure!(
                    ((record.action_kind == 2 || record.action_kind == 1)
                        && record.side_or_operation == 1)
                        == decoded.entry_context.is_some(),
                    "candidate symbol entry intent/context 1:1 binding failed"
                );
                Ok(decoded)
            })
            .collect::<Result<Vec<_>>>()?;
        anyhow::ensure!(
            entry_contexts_by_intent.is_empty(),
            "candidate symbol entry context has no same-batch intent count={}",
            entry_contexts_by_intent.len()
        );

        let mut next_owner_audit_seq = self.next_owner_audit_seq;
        for audit in &mut output.owner_audits {
            audit.audit_seq = next_owner_audit_seq;
            audit.source_sequence = source_sequence;
            audit.central_state_seq = central_state_seq;
            anyhow::ensure!(
                audit.ts_ms == timestamp_ms,
                "candidate symbol owner audit timestamp mismatch symbol={} fact_ms={} frame_ms={timestamp_ms}",
                self.symbol.as_str(),
                audit.ts_ms
            );
            audit.validate()?.validate_catalog(&self.catalog)?;
            next_owner_audit_seq = next_owner_audit_seq
                .checked_add(1)
                .context("candidate symbol owner audit sequence overflow")?;
        }
        let owned_leg_outcomes = output
            .owner_audits
            .into_iter()
            .map(|record| self.decode_owner_audit(record))
            .collect::<Result<Vec<_>>>()?;

        for label in output.oracle_labels.iter().copied() {
            label.validate()?.validate_catalog(&self.catalog)?;
            anyhow::ensure!(
                label.release_ts_ms <= timestamp_ms,
                "candidate symbol oracle label release is after output"
            );
        }
        let oracle_labels = output
            .oracle_labels
            .into_iter()
            .map(|record| self.decode_oracle_label(record))
            .collect::<Result<Vec<_>>>()?;

        self.next_intent_id = next_intent_id;
        self.next_owner_audit_seq = next_owner_audit_seq;
        Ok(WorkerCommandBatch::new(
            self.symbol,
            self.worker_epoch,
            timestamp_ms,
            intents,
            owned_leg_outcomes,
            oracle_labels,
        ))
    }

    fn decode_oracle_label(
        &self,
        record: OracleResolvedLabelRecord,
    ) -> Result<OracleResolvedLabel> {
        let record = record.validate()?.validate_catalog(&self.catalog)?;
        let market = self
            .catalog
            .record(record.market_id)
            .context("oracle label market handle disappeared from frozen catalog")?;
        let condition_id = B256::from(market.condition_id).to_string();
        Ok(OracleResolvedLabel {
            market_id: condition_id.clone().into(),
            condition_id,
            symbol: crate::oracle_resolution::canonical_chainlink_symbol(self.symbol.as_str())?,
            window_start_ms: record.window_start_ms,
            window_end_ms: record.window_end_ms,
            open: ResolutionBoundaryValue {
                price_e18: record.open_price_e18,
                source_ts_ms: record.open_source_ts_ms,
                observed_ts_ms: record.open_observed_ts_ms,
                capture_sequence: record.open_capture_sequence,
            },
            close: ResolutionBoundaryValue {
                price_e18: record.close_price_e18,
                source_ts_ms: record.close_source_ts_ms,
                observed_ts_ms: record.close_observed_ts_ms,
                capture_sequence: record.close_capture_sequence,
            },
            winner: match record.winner {
                1 => PairTokenSide::Up,
                2 => PairTokenSide::Down,
                _ => unreachable!("validated oracle label winner"),
            },
            release_ts_ms: record.release_ts_ms,
            provenance: CHAINLINK_RESOLUTION_PROVENANCE,
        })
    }

    fn decode_owner_audit(&self, record: OwnerAuditRecord) -> Result<UnjoinedOwnedLegOutcome> {
        let record = record.validate()?.validate_catalog(&self.catalog)?;
        let market = self
            .catalog
            .record(record.market_id)
            .context("owner audit market handle disappeared from frozen catalog")?;
        let token = if record.token_id == market.up_token_handle {
            market.up_token
        } else if record.token_id == market.down_token_handle {
            market.down_token
        } else {
            anyhow::bail!("owner audit token handle is foreign to resolved market")
        };
        let outcome = CtfLegEconomicOutcome::try_from(record.outcome)
            .map_err(|_| anyhow::anyhow!("invalid owner audit outcome tag={}", record.outcome))?;
        let reason = CtfLegDecisionReason::try_from(record.reason)
            .map_err(|_| anyhow::anyhow!("invalid owner audit reason tag={}", record.reason))?;
        let close_reason = match record.close_reason {
            0 => None,
            value => Some(
                CtfOwnedLegCloseReason::try_from(value)
                    .map_err(|_| anyhow::anyhow!("invalid owner audit close reason tag={value}"))?,
            ),
        };
        let causal_winner_side = match record.causal_winner_side {
            0 => None,
            1 => Some(PairTokenSide::Up),
            2 => Some(PairTokenSide::Down),
            value => anyhow::bail!("invalid owner audit causal winner side tag={value}"),
        };
        let has_post_sell_economics = record.flags & OWNER_AUDIT_POST_SELL_ECONOMICS_FLAG != 0;
        let has_first_economics =
            record.flags & OwnerAuditRecord::FIRST_INSTRUCTION_ECONOMICS_FLAG != 0;
        let has_first_upper = record.flags & OwnerAuditRecord::FIRST_UPPER_VALUATION_FLAG != 0;
        Ok(UnjoinedOwnedLegOutcome {
            symbol: self.symbol.as_str().to_string(),
            candidate_slot: self.candidate_slot,
            worker_epoch: self.worker_epoch,
            audit_seq: record.audit_seq,
            source_sequence: record.source_sequence,
            central_state_seq: record.central_state_seq,
            ts_ms: record.ts_ms,
            market_id: B256::from(market.condition_id).to_string().into(),
            cohort_id: record.cohort_id.required_str()?.to_string(),
            token: U256::from_be_bytes(token).to_string(),
            owned_qty_units_6dp: record.owned_qty_units_6dp,
            action_qty_units_6dp: record.action_qty_units_6dp,
            execution_intent_id: (record.execution_intent_id > 0)
                .then_some(record.execution_intent_id),
            execution_evidence_revision: (record.execution_evidence_revision > 0)
                .then_some(record.execution_evidence_revision),
            causal_winner_side,
            first_instruction_economics: has_first_economics.then_some(
                CtfFirstInstructionEconomicsEvidence {
                    instruction_limit_px_bits: record.first_instruction_limit_px_bits,
                    depth_gross_proceeds_bits: record.first_depth_gross_proceeds_bits,
                    depth_fee_bits: record.first_depth_fee_bits,
                    depth_net_proceeds_bits: record.first_depth_net_proceeds_bits,
                    authorization_fee_bits: record.first_authorization_fee_bits,
                    authorization_net_proceeds_bits: record.first_authorization_net_proceeds_bits,
                    delay_cushion_bits: record.first_delay_cushion_bits,
                    allocated_basis_units_6dp: record.first_allocated_basis_units_6dp,
                    upper_valuation_bits: has_first_upper
                        .then_some(record.first_upper_valuation_bits),
                    reservation_bits: record.first_reservation_bits,
                    mandatory_urgency: record.first_mandatory_urgency,
                },
            ),
            post_sell_full_entry_pnl_bits: has_post_sell_economics
                .then_some(record.sell_full_entry_pnl_bits),
            post_hold_full_entry_pnl_bits: has_post_sell_economics
                .then_some(record.hold_full_entry_pnl_bits),
            post_fillable_qty_bits: has_post_sell_economics.then_some(record.fillable_qty_bits),
            outcome,
            reason,
            close_reason,
        })
    }
}

fn remap_entry_context_intent_ids(
    intents: &[IntentRecord],
    entry_contexts: &mut [EntryContextRecord],
) -> Result<()> {
    let entry_count = intents
        .iter()
        .filter(|intent| matches!(intent.action_kind, 1 | 2) && intent.side_or_operation == 1)
        .count();
    anyhow::ensure!(
        entry_contexts.len() == entry_count,
        "candidate symbol entry intent/context count mismatch intents={entry_count} contexts={}",
        entry_contexts.len()
    );
    let mut claimed = BTreeSet::new();
    for context in entry_contexts {
        let ordinal = context.intent_id();
        anyhow::ensure!(
            claimed.insert(ordinal),
            "candidate symbol entry context intent ordinal reused ordinal={ordinal}"
        );
        let index = usize::try_from(
            ordinal
                .checked_sub(1)
                .context("candidate symbol entry context intent ordinal is zero")?,
        )
        .context("candidate symbol entry context intent ordinal overflow")?;
        let intent = intents.get(index).with_context(|| {
            format!(
                "candidate symbol entry context intent ordinal is absent ordinal={ordinal} intents={}",
                intents.len()
            )
        })?;
        anyhow::ensure!(
            intent.side_or_operation == 1
                && matches!(
                    (&*context, intent.action_kind),
                    (EntryContextRecord::Split(_), 2) | (EntryContextRecord::DirectBuy(_), 1)
                ),
            "candidate symbol entry context targets a foreign intent ordinal={ordinal}"
        );
        context.set_intent_id(intent.intent_id);
        context.validate()?;
    }
    Ok(())
}

fn remap_owner_audit_intent_ids(
    intents: &[IntentRecord],
    owner_audits: &mut [OwnerAuditRecord],
) -> Result<()> {
    let mut claimed_ordinals = BTreeSet::new();
    for audit in owner_audits {
        let ordinal = audit.execution_intent_id;
        if ordinal == 0 {
            continue;
        }
        anyhow::ensure!(
            claimed_ordinals.insert(ordinal),
            "candidate symbol owner audit execution intent ordinal reused ordinal={ordinal}"
        );
        let index = usize::try_from(ordinal - 1)
            .context("candidate symbol owner audit execution intent ordinal overflow")?;
        let intent = intents.get(index).with_context(|| {
            format!(
                "candidate symbol owner audit execution intent ordinal is absent ordinal={ordinal} intents={}",
                intents.len()
            )
        })?;
        anyhow::ensure!(
            audit.outcome == CtfLegEconomicOutcome::SellNow as u8
                && intent.action_kind == 1
                && intent.side_or_operation == 2
                && audit.market_id == intent.market_id
                && audit.token_id == intent.token_or_up_id
                && audit.action_qty_units_6dp == intent.qty_or_amount_units_6dp
                && audit.close_reason == intent.close_reason
                && audit.execution_evidence_revision == intent.execution_evidence_revision,
            "candidate symbol SELL_NOW owner audit does not match attached execution intent ordinal={ordinal}"
        );
        audit.execution_intent_id = intent.intent_id;
    }
    Ok(())
}
