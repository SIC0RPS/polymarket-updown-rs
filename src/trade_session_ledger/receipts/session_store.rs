use super::*;

pub(in crate::trade_session_ledger) fn load_session_rows_from_path(
    path: &Path,
) -> Result<Vec<SessionLedgerRow>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = fs::File::open(path)
        .with_context(|| format!("failed to open trade session ledger {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    for (line_index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| {
            format!(
                "failed to read trade session ledger {} line {}",
                path.display(),
                line_index.saturating_add(1)
            )
        })?;
        if line.trim().is_empty() {
            continue;
        }
        let row = serde_json::from_str::<SessionReceipt>(&line)
            .map(SessionLedgerRow::Receipt)
            .or_else(|_| {
                serde_json::from_str::<TradeLedgerRecord>(&line).map(SessionLedgerRow::Trade)
            })
            .with_context(|| {
                format!(
                    "failed to parse trade session ledger {} line {}",
                    path.display(),
                    line_index.saturating_add(1)
                )
            })?;
        rows.push(row);
    }
    Ok(rows)
}

pub fn append_session_receipt(mut receipt: SessionReceipt) -> Result<SessionReceiptAppendOutcome> {
    anyhow::ensure!(
        receipt.ledger_seq() == 0,
        "session receipt ledger_seq must be assigned by the canonical allocator"
    );
    let _append_guard = canonical_ledger_lock()?;
    if !receipt.belongs_to_run_window() {
        tracing::warn!(
            target: "trade_session_ledger",
            run_id = %receipt.run_id(),
            ts_ms = receipt.ts_ms(),
            "trade_session_ledger_rejected_out_of_window_receipt"
        );
        return Ok(SessionReceiptAppendOutcome::RejectedOutOfWindow);
    }
    ensure_run_trade_ledger_layout(receipt.run_id())?;
    let session_path = trade_session_ledger_path_for_run(receipt.run_id());
    let receipt_identity = receipt.identity();
    let existing = super::super::live_accounting::resident_receipt_by_identity(
        receipt.run_id(),
        &receipt_identity,
    )?;
    if let Some(existing) = existing {
        receipt.set_ledger_seq(existing.ledger_seq());
        receipt.validate()?;
        if existing.same_immutable_fact(&receipt) {
            return Ok(SessionReceiptAppendOutcome::IdenticalExisting);
        }
        let conflict = SessionReceipt::ReceiptConflict(ReceiptConflict {
            run_id: receipt.run_id().to_string(),
            ledger_seq: allocate_trade_ledger_seq()?,
            ts_ms: receipt.ts_ms(),
            receipt_identity,
            existing_ledger_seq: existing.ledger_seq(),
        });
        let inserted = append_session_receipt_conflict(&session_path, &conflict)?;
        let _ = inserted;
        return Ok(SessionReceiptAppendOutcome::Conflict);
    }
    receipt.set_ledger_seq(allocate_trade_ledger_seq()?);
    receipt.validate()?;
    append_new_session_receipt(&session_path, receipt)?;
    Ok(SessionReceiptAppendOutcome::Inserted)
}

pub(in crate::trade_session_ledger) fn sync_session_receipt_path(
    session_path: &Path,
) -> Result<()> {
    fs::OpenOptions::new()
        .read(true)
        .open(session_path)
        .with_context(|| {
            format!(
                "failed to open trade session receipt ledger {} for sync",
                session_path.display()
            )
        })?
        .sync_all()
        .with_context(|| {
            format!(
                "failed to sync trade session receipt ledger {}",
                session_path.display()
            )
        })?;
    let mut directory = session_path.parent();
    for _ in 0..2 {
        let Some(path) = directory else {
            break;
        };
        fs::File::open(path)
            .with_context(|| {
                format!(
                    "failed to open trade session receipt directory {} for sync",
                    path.display()
                )
            })?
            .sync_all()
            .with_context(|| {
                format!(
                    "failed to sync trade session receipt directory {}",
                    path.display()
                )
            })?;
        directory = path.parent();
    }
    Ok(())
}

fn append_session_receipt_conflict(session_path: &Path, conflict: &SessionReceipt) -> Result<bool> {
    conflict.validate()?;
    let conflict_identity = conflict.identity();
    let existing = super::super::live_accounting::resident_receipt_by_identity(
        conflict.run_id(),
        &conflict_identity,
    )?;
    if let Some(existing) = existing {
        anyhow::ensure!(
            existing.same_immutable_fact(conflict),
            "receipt conflict identity changed identity={conflict_identity}"
        );
        return Ok(false);
    }
    append_new_session_receipt(session_path, conflict.clone())?;
    Ok(true)
}

fn append_new_session_receipt(session_path: &Path, receipt: SessionReceipt) -> Result<()> {
    let (receipt, retry) = super::super::live_accounting::begin_durable_receipt_append(
        &receipt,
        SessionReceipt::same_immutable_fact,
    )?;
    super::super::append_and_sync_ledger_row(session_path, &receipt, receipt.ledger_seq(), retry)?;
    super::super::live_accounting::ingest_durable_session_receipt(receipt)
}

impl SessionReceipt {
    pub(in crate::trade_session_ledger) fn identity(&self) -> String {
        match self {
            Self::DirectEoaSubmission(receipt) => {
                format!("direct_eoa_submission:{}", receipt.tx_hash)
            }
            Self::DirectEoaGas(receipt) => format!("direct_eoa_gas:{}", receipt.tx_hash),
            Self::MakerRebate(receipt) => {
                format!("maker_rebate:{}:{}", receipt.tx_hash, receipt.log_index)
            }
            Self::SettlementCollateral(receipt) => {
                format!("settlement_collateral:{}", receipt.tx_hash)
            }
            Self::ReceiptConflict(receipt) => {
                format!("receipt_conflict:{}", receipt.receipt_identity)
            }
        }
    }

    pub(in crate::trade_session_ledger) fn run_id(&self) -> &str {
        match self {
            Self::DirectEoaSubmission(receipt) => &receipt.run_id,
            Self::DirectEoaGas(receipt) => &receipt.run_id,
            Self::MakerRebate(receipt) => &receipt.run_id,
            Self::SettlementCollateral(receipt) => &receipt.run_id,
            Self::ReceiptConflict(receipt) => &receipt.run_id,
        }
    }

    pub(in crate::trade_session_ledger) fn ledger_seq(&self) -> u64 {
        match self {
            Self::DirectEoaSubmission(receipt) => receipt.ledger_seq,
            Self::DirectEoaGas(receipt) => receipt.ledger_seq,
            Self::MakerRebate(receipt) => receipt.ledger_seq,
            Self::SettlementCollateral(receipt) => receipt.ledger_seq,
            Self::ReceiptConflict(receipt) => receipt.ledger_seq,
        }
    }

    pub(super) fn set_ledger_seq(&mut self, ledger_seq: u64) {
        match self {
            Self::DirectEoaSubmission(receipt) => receipt.ledger_seq = ledger_seq,
            Self::DirectEoaGas(receipt) => receipt.ledger_seq = ledger_seq,
            Self::MakerRebate(receipt) => receipt.ledger_seq = ledger_seq,
            Self::SettlementCollateral(receipt) => receipt.ledger_seq = ledger_seq,
            Self::ReceiptConflict(receipt) => receipt.ledger_seq = ledger_seq,
        }
    }

    pub(in crate::trade_session_ledger) fn ts_ms(&self) -> u64 {
        match self {
            Self::DirectEoaSubmission(receipt) => receipt.ts_ms,
            Self::DirectEoaGas(receipt) => receipt.ts_ms,
            Self::MakerRebate(receipt) => receipt.ts_ms,
            Self::SettlementCollateral(receipt) => receipt.ts_ms,
            Self::ReceiptConflict(receipt) => receipt.ts_ms,
        }
    }

    pub(super) fn belongs_to_run_window(&self) -> bool {
        run_start_ts_ms(self.run_id()).is_none_or(|start_ts_ms| self.ts_ms() >= start_ts_ms)
    }

    pub(super) fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            !self.run_id().trim().is_empty(),
            "session receipt run_id is empty"
        );
        anyhow::ensure!(self.ledger_seq() > 0, "session receipt ledger_seq is zero");
        anyhow::ensure!(self.ts_ms() > 0, "session receipt ts_ms is zero");
        match self {
            Self::DirectEoaSubmission(receipt) => {
                anyhow::ensure!(
                    !receipt.tx_hash.trim().is_empty(),
                    "direct EOA submission tx_hash is empty"
                );
                anyhow::ensure!(
                    !receipt.payer.trim().is_empty(),
                    "direct EOA submission payer is empty"
                );
                validate_direct_eoa_condition(receipt.operation, receipt.condition_id.as_deref())?;
                match receipt.operation {
                    DirectEoaOperation::Split => {
                        anyhow::ensure!(
                            receipt.amount_units_6dp.is_some_and(|amount| amount > 0),
                            "direct EOA split submission amount is missing or zero"
                        );
                        if let Some(identity) = receipt.split_identity.as_ref() {
                            identity.validate()?;
                            anyhow::ensure!(
                                receipt.condition_id.as_deref()
                                    == Some(identity.condition_id.as_str())
                                    && receipt.amount_units_6dp
                                        == Some(identity.amount_units_6dp),
                                "direct EOA split submission identity conflicts with transaction fields"
                            );
                            if let Some(journal) = receipt.split_entry_journal.as_ref() {
                                identity.validate_entry_journal(journal)?;
                            }
                        } else {
                            anyhow::ensure!(
                                receipt.split_entry_journal.is_none(),
                                "direct EOA split journal has no split identity"
                            );
                        }
                    }
                    DirectEoaOperation::Merge => {
                        anyhow::ensure!(
                            receipt.amount_units_6dp.is_some_and(|amount| amount > 0),
                            "direct EOA merge submission amount is missing or zero"
                        );
                        anyhow::ensure!(
                            receipt.split_identity.is_none()
                                && receipt.split_entry_journal.is_none(),
                            "direct EOA merge submission carries split identity or journal"
                        );
                    }
                    DirectEoaOperation::Approval | DirectEoaOperation::Redeem => {
                        anyhow::ensure!(
                            receipt.amount_units_6dp.is_none(),
                            "direct EOA approval/redeem submission must not carry amount"
                        );
                        anyhow::ensure!(
                            receipt.split_identity.is_none()
                                && receipt.split_entry_journal.is_none(),
                            "direct EOA approval/redeem submission carries split identity or journal"
                        );
                    }
                }
            }
            Self::DirectEoaGas(receipt) => {
                anyhow::ensure!(
                    !receipt.tx_hash.trim().is_empty(),
                    "direct EOA gas tx_hash is empty"
                );
                anyhow::ensure!(
                    !receipt.payer.trim().is_empty(),
                    "direct EOA gas payer is empty"
                );
                anyhow::ensure!(receipt.gas_used > 0, "direct EOA gas_used is zero");
                anyhow::ensure!(
                    receipt.effective_gas_price_wei > 0,
                    "direct EOA effective_gas_price_wei is zero"
                );
                validate_direct_eoa_condition(receipt.operation, receipt.condition_id.as_deref())?;
            }
            Self::MakerRebate(receipt) => {
                anyhow::ensure!(
                    !receipt.tx_hash.trim().is_empty(),
                    "maker rebate tx_hash is empty"
                );
                anyhow::ensure!(
                    !receipt.token_contract.trim().is_empty(),
                    "maker rebate token_contract is empty"
                );
                anyhow::ensure!(
                    !receipt.recipient.trim().is_empty(),
                    "maker rebate recipient is empty"
                );
                anyhow::ensure!(
                    receipt.amount_units_6dp == receipt.activity_amount_units_6dp,
                    "maker rebate activity/transfer amounts differ"
                );
            }
            Self::SettlementCollateral(receipt) => {
                receipt.validate_exact()?;
            }
            Self::ReceiptConflict(receipt) => {
                anyhow::ensure!(
                    !receipt.receipt_identity.trim().is_empty(),
                    "receipt conflict identity is empty"
                );
                anyhow::ensure!(
                    receipt.existing_ledger_seq > 0,
                    "receipt conflict existing_ledger_seq is zero"
                );
            }
        }
        Ok(())
    }

    pub(in crate::trade_session_ledger) fn same_immutable_fact(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DirectEoaSubmission(left), Self::DirectEoaSubmission(right)) => {
                left.run_id == right.run_id
                    && left.tx_hash == right.tx_hash
                    && left.payer == right.payer
                    && left.operation == right.operation
                    && left.condition_id == right.condition_id
                    && left.amount_units_6dp == right.amount_units_6dp
                    && left.split_identity == right.split_identity
                    && left.split_entry_journal == right.split_entry_journal
            }
            (Self::DirectEoaGas(left), Self::DirectEoaGas(right)) => {
                left.run_id == right.run_id
                    && left.tx_hash == right.tx_hash
                    && left.payer == right.payer
                    && left.gas_used == right.gas_used
                    && left.effective_gas_price_wei == right.effective_gas_price_wei
                    && left.block_number == right.block_number
                    && left.status == right.status
                    && left.operation == right.operation
                    && left.condition_id == right.condition_id
            }
            (Self::MakerRebate(left), Self::MakerRebate(right)) => {
                left.run_id == right.run_id
                    && left.tx_hash == right.tx_hash
                    && left.log_index == right.log_index
                    && left.token_contract == right.token_contract
                    && left.recipient == right.recipient
                    && left.amount_units_6dp == right.amount_units_6dp
                    && left.activity_amount_units_6dp == right.activity_amount_units_6dp
            }
            (Self::SettlementCollateral(left), Self::SettlementCollateral(right)) => {
                left.run_id == right.run_id
                    && left.tx_hash == right.tx_hash
                    && left.token_contract == right.token_contract
                    && left.recipient == right.recipient
                    && left.condition_id == right.condition_id
                    && left.operation == right.operation
                    && left.outcome == right.outcome
            }
            (Self::ReceiptConflict(left), Self::ReceiptConflict(right)) => {
                left.run_id == right.run_id
                    && left.receipt_identity == right.receipt_identity
                    && left.existing_ledger_seq == right.existing_ledger_seq
            }
            _ => false,
        }
    }
}

fn validate_direct_eoa_condition(
    operation: DirectEoaOperation,
    condition_id: Option<&str>,
) -> Result<()> {
    match operation {
        DirectEoaOperation::Approval => anyhow::ensure!(
            condition_id.is_none(),
            "approval receipt must not carry condition_id"
        ),
        DirectEoaOperation::Split | DirectEoaOperation::Merge | DirectEoaOperation::Redeem => {
            anyhow::ensure!(
                condition_id.is_some_and(|condition_id| !condition_id.trim().is_empty()),
                "CTF operation receipt is missing condition_id"
            )
        }
    }
    Ok(())
}
