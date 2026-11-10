use std::collections::HashMap;

use anyhow::{ensure, Context, Result};
use polymarket_client_sdk::types::B256;

use crate::polymarket::identity::MarketId;
use crate::polymarket::state::MarketProjection;
use crate::strategy::{InventoryIntent, InventoryOperation};
use crate::strategy_runtime::SymbolId;

mod execution_adapter;
mod identity;
mod reservations;
pub use execution_adapter::{
    expected_ctf_positions_for_confirmed_split, split_receipt, submit_inventory_operation,
};
use identity::{OperationIdentity, OperationKey};

pub const CTF_INVENTORY_FAILURE_COOLDOWN_MS: u64 = 120_000;
pub const CTF_COHORT_ID_MAX_BYTES: usize = 128;
pub use identity::{
    confirmed_ctf_split_cohort_id, confirmed_direct_buy_cohort_id,
    is_confirmed_ctf_entry_cohort_id, is_confirmed_ctf_split_cohort_id,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmedCtfInventoryOperation {
    pub operation: InventoryOperation,
    pub market_id: String,
    pub gamma_market_id: Option<String>,
    pub condition_id: B256,
    pub up_token: String,
    pub down_token: String,
    pub cohort_id: String,
    pub amount_units_6dp: u128,
    pub tx_hash: String,
    pub pending_ts_ms: u64,
    pub receipt_confirmed_ts_ms: u64,
    pub materialized_ts_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CtfInventoryAction {
    Execute,
    BeginSubmission,
    PollReceipt { tx_hash: String, elapsed_ms: u64 },
    Materialize { tx_hash: String },
    PublishConfirmed(ConfirmedCtfInventoryOperation),
    Wait(CtfInventoryWait),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CtfInventoryWait {
    Pending,
    Confirmed { tx_hash: String },
    FailedCooldown { remaining_ms: u64, error: String },
    ReconciliationRequired { tx_hash: String, error: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CtfInventoryReceipt {
    Pending { tx_hash: String },
    Confirmed { tx_hash: String },
    Reverted { tx_hash: String },
    ReconciliationRequired { tx_hash: String, error: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OperationPhase {
    Reserved {
        reserved_ts_ms: u64,
    },
    Pending {
        pending_ts_ms: u64,
        tx_hash: Option<String>,
    },
    ReceiptConfirmed {
        pending_ts_ms: u64,
        receipt_confirmed_ts_ms: u64,
        tx_hash: String,
    },
    ConfirmationReady(ConfirmedCtfInventoryOperation),
    Confirmed {
        tx_hash: String,
    },
    Failed {
        failed_ts_ms: u64,
        error: String,
    },
    ReconciliationRequired {
        tx_hash: String,
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OperationRecord {
    identity: OperationIdentity,
    phase: OperationPhase,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InventoryRecord {
    Chain(OperationRecord),
    DirectBuy(crate::types::DirectBuyAuthorization),
}

impl InventoryRecord {
    fn chain(&self) -> Option<&OperationRecord> {
        match self {
            Self::Chain(record) => Some(record),
            Self::DirectBuy(_) => None,
        }
    }

    fn chain_mut(&mut self) -> Option<&mut OperationRecord> {
        match self {
            Self::Chain(record) => Some(record),
            Self::DirectBuy(_) => None,
        }
    }
}

#[derive(Debug, Default)]
pub struct CtfInventoryRuntime {
    operations: HashMap<OperationKey, InventoryRecord>,
    last_now_ms: Option<u64>,
}

impl CtfInventoryRuntime {
    pub fn confirmed_split_identity_for_cohort(
        &self,
        cohort_id: &str,
    ) -> Result<(MarketId, B256, String, String, u128)> {
        ensure!(
            is_confirmed_ctf_split_cohort_id(cohort_id),
            "confirmed CTF split cohort identity is noncanonical"
        );
        let mut matching = self
            .operations
            .values()
            .filter_map(InventoryRecord::chain)
            .filter_map(|record| {
                if record.identity.operation != InventoryOperation::SplitCompleteSet {
                    return None;
                }
                let OperationPhase::Confirmed { tx_hash } = &record.phase else {
                    return None;
                };
                match confirmed_ctf_split_cohort_id(tx_hash) {
                    Ok(confirmed_cohort) if confirmed_cohort == cohort_id => {
                        Some(Ok(&record.identity))
                    }
                    Ok(_) => None,
                    Err(error) => Some(Err(error)),
                }
            });
        let identity = matching
            .next()
            .transpose()?
            .context("confirmed CTF split cohort is absent from current session")?;
        ensure!(
            matching.next().transpose()?.is_none(),
            "confirmed CTF split cohort is duplicated in current session"
        );
        ensure!(
            identity.market_id == identity.condition_id.to_string(),
            "confirmed CTF split market/condition identity mismatch"
        );
        Ok((
            MarketId::new(identity.market_id.clone()),
            identity.condition_id,
            identity.up_token.clone(),
            identity.down_token.clone(),
            identity.amount_units_6dp,
        ))
    }

    pub fn next_action(
        &mut self,
        intent: &InventoryIntent,
        now_ms: u64,
    ) -> Result<CtfInventoryAction> {
        self.observe_time(now_ms)?;
        let identity = OperationIdentity::from_intent(intent)?;
        let key = identity.key();
        let Some(record) = self.operations.get(&key) else {
            return Ok(CtfInventoryAction::Execute);
        };
        let InventoryRecord::Chain(record) = record else {
            return Ok(CtfInventoryAction::Wait(CtfInventoryWait::Pending));
        };
        ensure!(
            record.identity.operation == identity.operation
                && record.identity.market_id == identity.market_id
                && record.identity.gamma_market_id == identity.gamma_market_id
                && record.identity.condition_id == identity.condition_id
                && record.identity.up_token == identity.up_token
                && record.identity.down_token == identity.down_token
                && record.identity.cohort_id == identity.cohort_id,
            "CTF inventory identity changed for existing operation stored={:?} proposed={identity:?}",
            record.identity
        );
        if record.identity.amount_units_6dp != identity.amount_units_6dp {
            return Ok(match &record.phase {
                OperationPhase::Confirmed { tx_hash } => {
                    CtfInventoryAction::Wait(CtfInventoryWait::Confirmed {
                        tx_hash: tx_hash.clone(),
                    })
                }
                _ => CtfInventoryAction::Wait(CtfInventoryWait::Pending),
            });
        }
        Ok(match &record.phase {
            OperationPhase::Reserved { .. } => CtfInventoryAction::BeginSubmission,
            OperationPhase::Pending {
                pending_ts_ms,
                tx_hash: Some(tx_hash),
            } => CtfInventoryAction::PollReceipt {
                tx_hash: tx_hash.clone(),
                elapsed_ms: now_ms - pending_ts_ms,
            },
            OperationPhase::Pending { tx_hash: None, .. } => {
                CtfInventoryAction::Wait(CtfInventoryWait::Pending)
            }
            OperationPhase::ReceiptConfirmed { tx_hash, .. } => CtfInventoryAction::Materialize {
                tx_hash: tx_hash.clone(),
            },
            OperationPhase::ConfirmationReady(confirmed) => {
                CtfInventoryAction::PublishConfirmed(confirmed.clone())
            }
            OperationPhase::Confirmed { tx_hash } => {
                CtfInventoryAction::Wait(CtfInventoryWait::Confirmed {
                    tx_hash: tx_hash.clone(),
                })
            }
            OperationPhase::Failed {
                failed_ts_ms,
                error,
            } => {
                let elapsed_ms = now_ms - failed_ts_ms;
                if elapsed_ms >= CTF_INVENTORY_FAILURE_COOLDOWN_MS {
                    CtfInventoryAction::Execute
                } else {
                    CtfInventoryAction::Wait(CtfInventoryWait::FailedCooldown {
                        remaining_ms: CTF_INVENTORY_FAILURE_COOLDOWN_MS - elapsed_ms,
                        error: error.clone(),
                    })
                }
            }
            OperationPhase::ReconciliationRequired { tx_hash, error } => {
                CtfInventoryAction::Wait(CtfInventoryWait::ReconciliationRequired {
                    tx_hash: tx_hash.clone(),
                    error: error.clone(),
                })
            }
        })
    }

    pub fn reserve_split(&mut self, intent: &InventoryIntent, now_ms: u64) -> Result<bool> {
        self.observe_time(now_ms)?;
        let identity = OperationIdentity::from_intent(intent)?;
        ensure!(
            identity.operation == InventoryOperation::SplitCompleteSet,
            "CTF split reservation received non-split operation"
        );
        let key = identity.key();
        if let Some(record) = self.operations.get(&key) {
            let InventoryRecord::Chain(record) = record else {
                return Ok(false);
            };
            ensure!(
                record.identity == identity,
                "CTF split reservation identity changed"
            );
            match &record.phase {
                OperationPhase::Reserved { .. } => return Ok(false),
                OperationPhase::Failed { failed_ts_ms, .. } => ensure!(
                    now_ms - failed_ts_ms >= CTF_INVENTORY_FAILURE_COOLDOWN_MS,
                    "CTF inventory failure cooldown has not elapsed"
                ),
                _ => anyhow::bail!("CTF split reservation is invalid for current state"),
            }
        }
        self.operations.insert(
            key,
            InventoryRecord::Chain(OperationRecord {
                identity,
                phase: OperationPhase::Reserved {
                    reserved_ts_ms: now_ms,
                },
            }),
        );
        Ok(true)
    }

    pub fn reserve_split_for_symbol(
        &mut self,
        projection: &MarketProjection,
        expected_symbol: SymbolId,
        intent: &InventoryIntent,
        now_ms: u64,
    ) -> Result<bool> {
        let market = projection
            .market_by_id(intent.market_id.as_str())
            .with_context(|| {
                format!(
                    "CTF split reservation market is absent market_id={}",
                    intent.market_id
                )
            })?;
        ensure!(
            market.gamma_market_id() == intent.gamma_market_id.as_deref().unwrap_or_default()
                && market.condition_id_b256() == Some(intent.condition_id)
                && market.up_token_id() == intent.up_token
                && market.down_token_id() == intent.down_token,
            "CTF split reservation market identity mismatch market_id={}",
            intent.market_id
        );
        let actual_symbol = SymbolId::parse(&market.underlying).with_context(|| {
            format!(
                "CTF split reservation market has invalid underlying market_id={} underlying={}",
                intent.market_id, market.underlying
            )
        })?;
        ensure!(
            actual_symbol == expected_symbol,
            "CTF split reservation symbol mismatch expected={} actual={}",
            expected_symbol.as_str(),
            actual_symbol.as_str()
        );
        self.reserve_split(intent, now_ms)
    }

    pub fn begin_reserved_split_submission(
        &mut self,
        intent: &InventoryIntent,
        now_ms: u64,
    ) -> Result<()> {
        self.observe_time(now_ms)?;
        let identity = OperationIdentity::from_intent(intent)?;
        ensure!(
            identity.operation == InventoryOperation::SplitCompleteSet,
            "CTF split submission received non-split operation"
        );
        let record = self
            .operations
            .get_mut(&identity.key())
            .and_then(InventoryRecord::chain_mut)
            .context("CTF split submission has no reservation")?;
        ensure!(
            record.identity == identity,
            "CTF split submission identity changed"
        );
        let OperationPhase::Reserved { reserved_ts_ms } = &record.phase else {
            anyhow::bail!("CTF split submission is invalid for current state");
        };
        ensure!(
            now_ms >= *reserved_ts_ms,
            "CTF split submission predates reservation"
        );
        record.phase = OperationPhase::Pending {
            pending_ts_ms: now_ms,
            tx_hash: None,
        };
        Ok(())
    }

    pub fn release_reserved_split(&mut self, intent: &InventoryIntent, now_ms: u64) -> Result<()> {
        self.observe_time(now_ms)?;
        let identity = OperationIdentity::from_intent(intent)?;
        ensure!(
            identity.operation == InventoryOperation::SplitCompleteSet,
            "CTF split reservation release received non-split operation"
        );
        let key = identity.key();
        let record = self
            .operations
            .get(&key)
            .and_then(InventoryRecord::chain)
            .context("CTF split reservation release has no reservation")?;
        ensure!(
            record.identity == identity,
            "CTF split reservation release identity changed"
        );
        ensure!(
            matches!(&record.phase, OperationPhase::Reserved { .. }),
            "CTF split reservation release is invalid for current state"
        );
        self.operations.remove(&key);
        Ok(())
    }

    pub fn record_receipt(
        &mut self,
        intent: &InventoryIntent,
        now_ms: u64,
        receipt: CtfInventoryReceipt,
    ) -> Result<()> {
        self.observe_time(now_ms)?;
        let identity = OperationIdentity::from_intent(intent)?;
        let record = self
            .operations
            .get_mut(&identity.key())
            .and_then(InventoryRecord::chain_mut)
            .ok_or_else(|| anyhow::anyhow!("CTF inventory receipt has no pending operation"))?;
        ensure!(
            record.identity == identity,
            "CTF inventory identity changed"
        );
        let (pending_ts_ms, prior_tx_hash) = match &record.phase {
            OperationPhase::Pending {
                pending_ts_ms,
                tx_hash,
            } => (*pending_ts_ms, tx_hash.as_deref()),
            _ => anyhow::bail!("CTF inventory receipt is invalid for current state"),
        };
        let receipt_tx_hash = match &receipt {
            CtfInventoryReceipt::Pending { tx_hash }
            | CtfInventoryReceipt::Confirmed { tx_hash }
            | CtfInventoryReceipt::Reverted { tx_hash }
            | CtfInventoryReceipt::ReconciliationRequired { tx_hash, .. } => tx_hash,
        };
        ensure!(
            !receipt_tx_hash.trim().is_empty(),
            "CTF inventory receipt transaction identity is missing"
        );
        if let CtfInventoryReceipt::ReconciliationRequired { error, .. } = &receipt {
            ensure!(
                !error.trim().is_empty(),
                "CTF inventory receipt error detail is missing"
            );
        }
        if let Some(prior_tx_hash) = prior_tx_hash {
            ensure!(
                prior_tx_hash == receipt_tx_hash,
                "CTF inventory receipt transaction identity changed"
            );
        }
        record.phase = match receipt {
            CtfInventoryReceipt::Pending { tx_hash } => OperationPhase::Pending {
                pending_ts_ms,
                tx_hash: Some(tx_hash),
            },
            CtfInventoryReceipt::Confirmed { tx_hash } => OperationPhase::ReceiptConfirmed {
                pending_ts_ms,
                receipt_confirmed_ts_ms: now_ms,
                tx_hash,
            },
            CtfInventoryReceipt::Reverted { tx_hash } => OperationPhase::Failed {
                failed_ts_ms: now_ms,
                error: format!("CTF inventory transaction reverted tx_hash={tx_hash}"),
            },
            CtfInventoryReceipt::ReconciliationRequired { tx_hash, error } => {
                OperationPhase::ReconciliationRequired { tx_hash, error }
            }
        };
        Ok(())
    }

    pub fn record_submission_failure(
        &mut self,
        intent: &InventoryIntent,
        now_ms: u64,
        error: String,
    ) -> Result<()> {
        self.observe_time(now_ms)?;
        let identity = OperationIdentity::from_intent(intent)?;
        let record = self
            .operations
            .get_mut(&identity.key())
            .and_then(InventoryRecord::chain_mut)
            .ok_or_else(|| anyhow::anyhow!("CTF inventory failure has no pending operation"))?;
        ensure!(
            record.identity == identity,
            "CTF inventory identity changed"
        );
        ensure!(
            matches!(&record.phase, OperationPhase::Pending { tx_hash: None, .. }),
            "CTF inventory submission failure is invalid for current state"
        );
        ensure!(
            !error.trim().is_empty(),
            "CTF inventory submission failure detail is missing"
        );
        record.phase = OperationPhase::Failed {
            failed_ts_ms: now_ms,
            error,
        };
        Ok(())
    }

    pub fn record_materialized(
        &mut self,
        intent: &InventoryIntent,
        now_ms: u64,
        tx_hash: &str,
    ) -> Result<ConfirmedCtfInventoryOperation> {
        self.observe_time(now_ms)?;
        let identity = OperationIdentity::from_intent(intent)?;
        let record = self
            .operations
            .get_mut(&identity.key())
            .and_then(InventoryRecord::chain_mut)
            .ok_or_else(|| anyhow::anyhow!("CTF inventory materialization has no receipt"))?;
        ensure!(
            record.identity == identity,
            "CTF inventory identity changed"
        );
        let OperationPhase::ReceiptConfirmed {
            pending_ts_ms,
            receipt_confirmed_ts_ms,
            tx_hash: confirmed_tx_hash,
        } = &record.phase
        else {
            anyhow::bail!("CTF inventory materialization is invalid for current state");
        };
        ensure!(
            confirmed_tx_hash == tx_hash,
            "CTF inventory materialization transaction identity mismatch"
        );
        let confirmed = identity.confirmed(
            tx_hash.to_string(),
            *pending_ts_ms,
            *receipt_confirmed_ts_ms,
            now_ms,
        )?;
        record.phase = OperationPhase::ConfirmationReady(confirmed.clone());
        Ok(confirmed)
    }

    pub fn acknowledge_confirmed(
        &mut self,
        confirmed: &ConfirmedCtfInventoryOperation,
        now_ms: u64,
    ) -> Result<()> {
        self.observe_time(now_ms)?;
        let key = OperationKey::new(confirmed.operation, confirmed.condition_id);
        let record = self
            .operations
            .get(&key)
            .and_then(InventoryRecord::chain)
            .ok_or_else(|| anyhow::anyhow!("CTF inventory confirmation has no operation"))?;
        let OperationPhase::ConfirmationReady(expected) = &record.phase else {
            anyhow::bail!("CTF inventory confirmation is invalid for current state");
        };
        ensure!(
            expected == confirmed,
            "CTF inventory confirmation identity changed"
        );
        if confirmed.operation == InventoryOperation::SplitCompleteSet {
            let operation = self
                .operations
                .get_mut(&key)
                .and_then(InventoryRecord::chain_mut)
                .expect("checked CTF inventory operation");
            operation.phase = OperationPhase::Confirmed {
                tx_hash: confirmed.tx_hash.clone(),
            };
        } else {
            self.operations.remove(&key);
        }
        Ok(())
    }

    fn observe_time(&mut self, now_ms: u64) -> Result<()> {
        if let Some(last_now_ms) = self.last_now_ms {
            ensure!(
                now_ms >= last_now_ms,
                "CTF inventory clock moved backwards now_ms={now_ms} last_now_ms={last_now_ms}"
            );
        }
        self.last_now_ms = Some(now_ms);
        Ok(())
    }
}
