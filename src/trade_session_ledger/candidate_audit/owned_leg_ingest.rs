use super::*;

impl CanonicalAuditLedger {
    pub(crate) fn ingest_owned_leg_outcomes(
        &mut self,
        mut outcomes: Vec<UnjoinedOwnedLegOutcome>,
        mut admit: impl FnMut(&TradeLedgerRecord) -> Result<()>,
    ) -> Result<()> {
        outcomes.sort_by(|left, right| {
            (
                left.source_sequence,
                left.central_state_seq,
                left.symbol.as_str(),
                left.worker_epoch,
                left.audit_seq,
            )
                .cmp(&(
                    right.source_sequence,
                    right.central_state_seq,
                    right.symbol.as_str(),
                    right.worker_epoch,
                    right.audit_seq,
                ))
        });
        let mut validation_projection =
            super::super::owned_leg_outcomes::OwnedLegValidationProjection::from_records(
                &self.records,
            );
        for outcome in outcomes {
            ensure!(
                self.expected_symbols.contains(&outcome.symbol),
                "candidate canonical audit owner outcome has foreign symbol={}",
                outcome.symbol
            );
            ensure!(
                outcome.worker_epoch > 0
                    && outcome.audit_seq > 0
                    && outcome.source_sequence > 0
                    && outcome.central_state_seq > 0,
                "candidate canonical audit owner outcome has zero transport identity"
            );
            if let Some(candidate_slot) = self.candidate_slot {
                ensure!(
                    outcome.candidate_slot == candidate_slot,
                    "candidate canonical audit owner outcome has foreign slot expected={candidate_slot} actual={}",
                    outcome.candidate_slot
                );
            }
            let key = (outcome.symbol.clone(), outcome.worker_epoch);
            let expected = self
                .last_owner_audit_seq
                .get(&key)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .context("candidate canonical audit owner sequence overflow")?;
            ensure!(
                outcome.audit_seq == expected,
                "candidate canonical audit owner sequence mismatch symbol={} epoch={} expected={expected} actual={}",
                outcome.symbol,
                outcome.worker_epoch,
                outcome.audit_seq
            );
            let identity = (
                outcome.symbol.clone(),
                outcome.worker_epoch,
                outcome.source_sequence,
                outcome.central_state_seq,
                outcome.ts_ms,
                outcome.market_id.to_string(),
                outcome.token.clone(),
            );
            ensure!(
                !self.owned_leg_identities.contains_key(&identity),
                "candidate canonical audit duplicate owned-leg outcome symbol={} source_sequence={} central_state_seq={} ts_ms={} market_id={} token={}",
                outcome.symbol,
                outcome.source_sequence,
                outcome.central_state_seq,
                outcome.ts_ms,
                outcome.market_id,
                outcome.token
            );
            let next_ledger_seq = self
                .last_source_sequence
                .unwrap_or(0)
                .checked_add(1)
                .context("candidate canonical audit ledger sequence overflow")?;
            let joined = outcome.join(next_ledger_seq)?;
            validation_projection.validate(&joined)?;
            if super::super::owned_leg_outcomes::repeats_latest_hold_decision(
                &joined,
                &self.records,
                std::iter::empty(),
            ) {
                self.candidate_slot.get_or_insert(outcome.candidate_slot);
                self.last_owner_audit_seq.insert(key, outcome.audit_seq);
                self.owned_leg_identities
                    .insert(identity, outcome.audit_seq);
                continue;
            }
            let record = TradeLedgerRecord::from_owned_leg_outcome(joined, self.run_id.clone());
            admit(&record)?;
            self.candidate_slot.get_or_insert(outcome.candidate_slot);
            self.last_owner_audit_seq.insert(key, outcome.audit_seq);
            self.owned_leg_identities
                .insert(identity, outcome.audit_seq);
            apply_record_to_run_index(&mut self.index, &record);
            validation_projection.insert(&record);
            self.records.push(record);
            self.last_source_sequence = Some(next_ledger_seq);
        }
        Ok(())
    }
}
