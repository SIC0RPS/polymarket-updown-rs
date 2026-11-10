use std::collections::HashMap;
use std::time::Instant;

use anyhow::Result;
use futures_util::future::BoxFuture;
use polymarket_client_sdk::types::B256;

use crate::ctf_inventory::is_confirmed_ctf_split_cohort_id;
use crate::execution::{ConditionPayoutVector, CtfSettlementConfirmation};
use crate::portfolio::RedeemCloseRequest;
use crate::strategy::{InventoryIntent, InventoryOperation};
use crate::strategy_runtime::symbol_process::SymbolId;
use crate::trade_session_ledger::SettlementCollateralReceipt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum ReclaimKind {
    Merge,
    Redeem,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct ReclaimAttemptKey {
    pub(super) condition_id: B256,
    pub(super) kind: ReclaimKind,
}

pub(super) struct PendingConfirmedAccounting {
    pub(super) receipt: SettlementCollateralReceipt,
    pub(super) requests: Vec<RedeemCloseRequest>,
    pub(super) payout_vector: Option<ConditionPayoutVector>,
    pub(super) closed_at_ms: u64,
}

pub(super) enum TerminalSubmission {
    Pending,
    Accounting(PendingConfirmedAccounting),
    Complete,
}

pub(super) struct ReclaimOperationCompletion {
    pub(super) key: ReclaimAttemptKey,
    pub(super) command: Option<CtfTerminalMergeCommand>,
    pub(super) requests: Vec<RedeemCloseRequest>,
    pub(super) payout_vector: Option<ConditionPayoutVector>,
    pub(super) attempted_at: Option<Instant>,
    pub(super) result: Result<Option<CtfSettlementConfirmation>>,
}

pub(super) type ReclaimOperationFuture = BoxFuture<'static, ReclaimOperationCompletion>;

pub(super) fn condition_has_pending_submission(
    terminal_submissions: &HashMap<ReclaimAttemptKey, TerminalSubmission>,
    condition_id: B256,
) -> bool {
    [ReclaimKind::Merge, ReclaimKind::Redeem]
        .into_iter()
        .any(|kind| {
            matches!(
                terminal_submissions.get(&ReclaimAttemptKey { condition_id, kind }),
                Some(TerminalSubmission::Pending | TerminalSubmission::Accounting(_))
            )
        })
}

#[derive(Debug, Clone)]
pub struct CtfTerminalMergeCommand {
    pub symbol: SymbolId,
    pub intent: InventoryIntent,
}

impl CtfTerminalMergeCommand {
    pub fn new(symbol: SymbolId, intent: InventoryIntent) -> Result<Self> {
        let command = Self { symbol, intent };
        command.validate()?;
        Ok(command)
    }

    pub(super) fn validate(&self) -> Result<()> {
        anyhow::ensure!(
            self.intent.operation == InventoryOperation::MergeCompleteSet,
            "terminal merge command operation is not merge_complete_set"
        );
        anyhow::ensure!(
            self.intent.amount_units_6dp > 0,
            "terminal merge command amount is zero"
        );
        anyhow::ensure!(
            self.intent.market_id.as_str() == self.intent.condition_id.to_string(),
            "terminal merge command market/condition identity mismatch"
        );
        anyhow::ensure!(
            !self.intent.up_token.trim().is_empty()
                && !self.intent.down_token.trim().is_empty()
                && self.intent.up_token != self.intent.down_token,
            "terminal merge command token identity is missing or duplicate"
        );
        anyhow::ensure!(
            self.intent
                .cohort_id
                .as_deref()
                .is_some_and(is_confirmed_ctf_split_cohort_id),
            "terminal merge command has no exact confirmed split cohort identity"
        );
        Ok(())
    }

    pub(super) fn attempt_key(&self) -> ReclaimAttemptKey {
        ReclaimAttemptKey {
            condition_id: self.intent.condition_id,
            kind: ReclaimKind::Merge,
        }
    }

    fn same_identity(&self, other: &Self) -> bool {
        self.symbol == other.symbol
            && self.intent.operation == other.intent.operation
            && self.intent.market_id == other.intent.market_id
            && self.intent.gamma_market_id == other.intent.gamma_market_id
            && self.intent.condition_id == other.intent.condition_id
            && self.intent.up_token == other.intent.up_token
            && self.intent.down_token == other.intent.down_token
            && self.intent.cohort_id == other.intent.cohort_id
            && self.intent.amount_units_6dp == other.intent.amount_units_6dp
    }
}

pub(super) fn retain_terminal_merge_commands(
    pending: &mut Vec<CtfTerminalMergeCommand>,
    terminal_submissions: &HashMap<ReclaimAttemptKey, TerminalSubmission>,
    commands: Vec<CtfTerminalMergeCommand>,
) -> Result<()> {
    let mut command_error = None;
    for command in commands {
        let command = match CtfTerminalMergeCommand::new(command.symbol, command.intent) {
            Ok(command) => command,
            Err(error) => {
                command_error.get_or_insert(error);
                continue;
            }
        };
        let key = command.attempt_key();
        if terminal_submissions.contains_key(&key) {
            continue;
        }
        if let Some(existing) = pending
            .iter()
            .find(|existing| existing.attempt_key() == key)
        {
            if !existing.same_identity(&command) {
                command_error.get_or_insert_with(|| {
                    anyhow::anyhow!(
                        "conflicting terminal merge command condition_id={}",
                        command.intent.condition_id
                    )
                });
            }
            continue;
        }
        pending.push(command);
    }
    pending.sort_by_key(|command| (command.symbol as u8, command.intent.condition_id));
    command_error.map_or(Ok(()), Err)
}
