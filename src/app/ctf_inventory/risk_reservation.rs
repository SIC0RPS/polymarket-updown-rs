use anyhow::Result;
use polymarket_hft::ctf_inventory::CtfInventoryRuntime;
use polymarket_hft::polymarket::state::MarketProjection;
use polymarket_hft::risk::RiskHandle;
use polymarket_hft::strategy::{InventoryIntent, StrategyAction};
use polymarket_hft::strategy_runtime::actors::{
    CentralSymbolCommandOwnership, MakerRebateMmActorScope,
};
use polymarket_hft::strategy_runtime::SymbolId;
use tokio::sync::watch;

#[derive(Clone)]
pub(crate) struct CentralCtfRiskViews {
    pub(crate) risk: RiskHandle,
    pub(crate) market_projection_rx: watch::Receiver<MarketProjection>,
}

impl CentralCtfRiskViews {
    pub(crate) fn reserve_new_direct_buy(
        &self,
        expected_symbol: SymbolId,
        action: &mut StrategyAction,
        runtime: &std::sync::Mutex<CtfInventoryRuntime>,
        now_ms: u64,
        actor_scope: &MakerRebateMmActorScope,
        central_ownership: &CentralSymbolCommandOwnership,
    ) -> Result<bool> {
        let StrategyAction::Order(order) = action else {
            anyhow::bail!("BUY admission received a non-order action");
        };
        let intent_id = order
            .execution_intent_id
            .ok_or_else(|| anyhow::anyhow!("live BUY is missing central execution identity"))?;
        CtfInventoryRuntime::bind_direct_buy_order(order, expected_symbol, intent_id, now_ms)?;
        anyhow::ensure!(
            actor_scope.action_belongs(action)
                && central_ownership.action_belongs(expected_symbol.as_str(), action),
            "centrally bound BUY has foreign ownership"
        );
        let StrategyAction::Order(order) = action else {
            unreachable!("BUY action checked before binding");
        };
        let authorization = order
            .buy_authorization()
            .ok_or_else(|| anyhow::anyhow!("live BUY lost bound authorization"))?;
        let admitted = runtime
            .lock()
            .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
            .reserve_direct_buy_for_symbol(
                &self.market_projection_rx.borrow(),
                expected_symbol,
                authorization,
                now_ms,
            )?;
        if !admitted {
            return Ok(false);
        }
        match self.risk.reserve_inflight_order(order, None) {
            Ok(_) => Ok(true),
            Err(block) => {
                runtime
                    .lock()
                    .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                    .release_reserved_direct_buy(authorization);
                tracing::debug!(
                    target: "app.ctf_inventory",
                    rule = %block.rule,
                    details = %block.details,
                    intent_id,
                    "direct_buy_risk_admission_blocked"
                );
                Ok(false)
            }
        }
    }

    pub(crate) fn abandon_unsubmitted_direct_buy(
        &self,
        intent: &polymarket_hft::types::OrderIntent,
        runtime: &std::sync::Mutex<CtfInventoryRuntime>,
    ) -> Result<()> {
        if self.risk.abandon_unsubmitted_direct_buy(intent)? {
            let authorization = intent
                .buy_authorization()
                .ok_or_else(|| anyhow::anyhow!("abandoned BUY has no central authorization"))?;
            runtime
                .lock()
                .map_err(|_| anyhow::anyhow!("CTF inventory runtime lock poisoned"))?
                .release_reserved_direct_buy(authorization);
        }
        Ok(())
    }

    pub(crate) fn reserve_new_split(
        &self,
        expected_symbol: SymbolId,
        intent: &InventoryIntent,
        runtime: &mut CtfInventoryRuntime,
        now_ms: u64,
    ) -> Result<bool> {
        runtime.reserve_split_for_symbol(
            &self.market_projection_rx.borrow(),
            expected_symbol,
            intent,
            now_ms,
        )
    }
}
