use super::*;
use crate::strategy_runtime::exchange_sim::VenueSubmissionConsequence;

impl CandidateCentralState {
    pub(super) fn retain_supervisor_audit_record(
        &mut self,
        record: crate::engine_trade_journal::EngineTradeJournalRecord,
    ) -> Result<()> {
        if record.ctf_event.as_ref().is_some_and(|event| {
            matches!(
                event,
                crate::trade_session_ledger::CanonicalCtfEvent::OrderAccepted(_)
            )
        }) {
            return self.retain_supervisor_submission_identity(record);
        }
        if record.order_terminal_fact.is_some() {
            return self.ingest_audit_record(&record);
        }
        anyhow::bail!("candidate supervisor emitted unknown execution audit record")
    }

    pub(super) fn retain_supervisor_submission_identity(
        &mut self,
        record: crate::engine_trade_journal::EngineTradeJournalRecord,
    ) -> Result<()> {
        let order_id = record
            .ctf_event
            .as_ref()
            .and_then(|event| match event {
                crate::trade_session_ledger::CanonicalCtfEvent::OrderAccepted(fact) => {
                    Some(fact.order_id.clone())
                }
                _ => None,
            })
            .context("candidate supervisor submission has no typed identity")?;
        anyhow::ensure!(
            self.pending_submission_identities
                .insert(order_id.clone(), record)
                .is_none(),
            "candidate supervisor emitted duplicate order identity: order_id={}",
            order_id
        );
        Ok(())
    }

    pub(super) fn apply_venue_submission_consequence(
        &mut self,
        submitted: &crate::execution::SimSubmittedOrder,
        consequence: VenueSubmissionConsequence,
        accepted_at_ms: u64,
    ) -> Result<()> {
        let mut record = self
            .pending_submission_identities
            .remove(&submitted.order_id)
            .with_context(|| {
                format!(
                    "candidate venue consequence has no supervisor identity: order_id={}",
                    submitted.order_id
                )
            })?;
        if consequence == VenueSubmissionConsequence::Rejected {
            return Ok(());
        }
        let fact = record
            .ctf_event
            .as_mut()
            .and_then(|event| match event {
                crate::trade_session_ledger::CanonicalCtfEvent::OrderAccepted(fact) => Some(fact),
                _ => None,
            })
            .context("candidate accepted venue consequence has no accepted-order identity")?;
        anyhow::ensure!(
            submitted.intent.execution_intent_id == Some(fact.intent_id)
                && submitted.intent.market_id.as_ref() == Some(&fact.market_id)
                && submitted.intent.lifecycle_cohort.as_deref() == Some(fact.cohort_id.as_str())
                && submitted.intent.time_in_force == fact.time_in_force
                && submitted.intent.post_only == fact.post_only
                && submitted
                    .intent
                    .lifecycle_symbol
                    .as_deref()
                    .is_some_and(|symbol| symbol.eq_ignore_ascii_case(&fact.underlying)),
            "candidate accepted venue identity mismatch: order_id={}",
            submitted.order_id
        );
        fact.accepted_at_ms = accepted_at_ms;
        record.ts_ms = accepted_at_ms;
        self.ingest_audit_record(&record)
    }
}
