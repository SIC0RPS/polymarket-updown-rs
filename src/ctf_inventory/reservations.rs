use std::collections::HashSet;

use crate::polymarket::state::MarketProjection;
use crate::strategy_runtime::SymbolId;
use crate::types::{DirectBuyAuthorization, OrderIntent, TimeInForce};
use anyhow::{ensure, Context, Result};

use crate::strategy::InventoryOperation;

use super::identity::OperationKey;
use super::{CtfInventoryRuntime, InventoryRecord, OperationPhase};

impl CtfInventoryRuntime {
    pub fn bind_direct_buy_order(
        order: &mut OrderIntent,
        expected_symbol: SymbolId,
        central_intent_id: u64,
        now_ms: u64,
    ) -> Result<()> {
        let OrderIntent::Buy(buy) = order else {
            anyhow::bail!("BUY authorization received a SELL order");
        };
        let mut authorization = buy
            .authorization
            .context("BUY entry authorization is missing")?;
        ensure!(
            authorization.authorized_at_ms == 0
                && (authorization.intent_id == 0
                    || buy.fields.execution_intent_id == Some(authorization.intent_id)),
            "BUY proposal already carries a foreign authorization"
        );
        authorization.intent_id = central_intent_id;
        authorization.authorized_at_ms = now_ms;
        authorization.validate()?;
        ensure!(
            authorization.symbol == expected_symbol
                && buy
                    .fields
                    .market_id
                    .as_ref()
                    .is_some_and(|market| market.as_str() == authorization.condition_id.to_string())
                && buy.fields.instrument == authorization.selected_token().to_string()
                && buy.fields.lifecycle_symbol.as_deref() == Some(expected_symbol.as_str())
                && buy.fields.lifecycle_cohort.is_none()
                && !buy.fields.post_only
                && matches!(buy.fields.time_in_force, TimeInForce::Gtd { expiration_ms }
                    if expiration_ms == authorization.market_end_ms)
                && buy.fields.execution_instruction_qty_units_6dp
                    == authorization.target_qty_units_6dp
                && crate::ctf_accounting::qty_to_units_6dp(buy.fields.qty)?
                    == authorization.target_qty_units_6dp,
            "BUY order differs from immutable entry authorization"
        );
        let price_units_8dp = buy.fields.execution_instruction_limit_px_units_8dp;
        ensure!(
            crate::polymarket::fixed_math::price_to_units_8dp(buy.fields.limit_px)
                == Some(price_units_8dp),
            "BUY order price differs from exact execution instruction"
        );
        buy.fields.execution_intent_id = Some(central_intent_id);
        buy.fields.lifecycle_cohort = Some(super::confirmed_direct_buy_cohort_id(
            authorization.condition_id,
            central_intent_id,
        )?);
        buy.authorization = Some(authorization);
        Ok(())
    }

    pub fn reserve_direct_buy_for_symbol(
        &mut self,
        projection: &MarketProjection,
        expected_symbol: SymbolId,
        authorization: &DirectBuyAuthorization,
        now_ms: u64,
    ) -> Result<bool> {
        self.observe_time(now_ms)?;
        authorization.validate()?;
        ensure!(
            authorization.symbol == expected_symbol
                && authorization.authorized_at_ms <= now_ms
                && now_ms < authorization.market_end_ms,
            "BUY reservation has foreign symbol or authorization time"
        );
        let market_id = authorization.condition_id.to_string();
        let market = projection
            .market_by_id(&market_id)
            .context("BUY reservation market is absent")?;
        ensure!(
            market.condition_id_b256() == Some(authorization.condition_id)
                && market.up_token_id() == authorization.up_token.to_string()
                && market.down_token_id() == authorization.down_token.to_string()
                && SymbolId::parse(&market.underlying)? == expected_symbol
                && market.window_end_ms == authorization.market_end_ms
                && market.standard_binary_ctf()
                && market.taker_base_fee_bps.map(f64::from) == Some(authorization.fee_rate_bps()),
            "BUY reservation differs from current market identity"
        );
        let key = OperationKey::Entry(authorization.condition_id);
        if self.operations.contains_key(&key) {
            return Ok(false);
        }
        self.operations
            .insert(key, InventoryRecord::DirectBuy(*authorization));
        Ok(true)
    }

    /// Release only after the caller establishes that this BUY was not submitted.
    pub fn release_reserved_direct_buy(&mut self, authorization: &DirectBuyAuthorization) -> bool {
        let key = OperationKey::Entry(authorization.condition_id);
        if !matches!(
            self.operations.get(&key),
            Some(InventoryRecord::DirectBuy(reserved)) if reserved == authorization
        ) {
            return false;
        }
        self.operations.remove(&key);
        true
    }

    pub fn unmaterialized_split_gross_inventory_units_6dp(&self) -> Result<u128> {
        self.unmaterialized_split_gross_inventory_units_6dp_matching(|_| true)
    }

    pub fn unmaterialized_split_gross_inventory_units_6dp_for_conditions(
        &self,
        condition_ids: impl IntoIterator<Item = [u8; 32]>,
    ) -> Result<u128> {
        let condition_ids = condition_ids.into_iter().collect::<HashSet<_>>();
        self.unmaterialized_split_gross_inventory_units_6dp_matching(|record| {
            condition_ids.contains(record.identity.condition_id.as_slice())
        })
    }

    fn unmaterialized_split_gross_inventory_units_6dp_matching(
        &self,
        include: impl Fn(&super::OperationRecord) -> bool,
    ) -> Result<u128> {
        let mut gross_units_6dp = 0_u128;
        for record in self
            .operations
            .values()
            .filter_map(InventoryRecord::chain)
            .filter(|record| {
                include(record)
                    && record.identity.operation == InventoryOperation::SplitCompleteSet
                    && matches!(
                        &record.phase,
                        OperationPhase::Reserved { .. }
                            | OperationPhase::Pending { .. }
                            | OperationPhase::ReceiptConfirmed { .. }
                            | OperationPhase::ConfirmationReady(_)
                            | OperationPhase::ReconciliationRequired { .. }
                    )
            })
        {
            let reservation_units_6dp = record
                .identity
                .amount_units_6dp
                .checked_mul(2)
                .context("CTF split gross inventory reservation overflow")?;
            gross_units_6dp = gross_units_6dp
                .checked_add(reservation_units_6dp)
                .context("aggregate CTF split gross inventory reservation overflow")?;
        }
        Ok(gross_units_6dp)
    }
}
