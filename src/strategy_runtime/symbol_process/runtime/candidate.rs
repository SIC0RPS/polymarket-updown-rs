use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use bbwick_engine::OracleHistory;

use crate::capture::events::STRATEGY_CONSUMPTION_SCHEMA_VERSION;

use super::{CandidateState, CandidateSymbolRuntime};
use crate::config::{MakerRebateMmStrategyConfig, OptimizerParameterSchema};
use crate::orders::OpenOrdersState;
use crate::polymarket::state::PolymarketOddsState;
use crate::polymarket::symbol_ownership::SymbolOwnershipIndex;
use crate::portfolio::PortfolioState;
use crate::risk::RiskBudgetSnapshot;
use crate::strategy::OracleLearningSnapshot;
use crate::strategy_runtime::actors::{MakerRebateMmActor, MakerRebateMmSymbolActorInit};
use crate::strategy_runtime::replay_mode::PackedReplayTape;
use crate::strategy_runtime::symbol_process::{
    CandidateReset, ProtocolMode, SymbolId, WorkerBaseSnapshot,
};

impl CandidateSymbolRuntime {
    pub(crate) fn new(
        candidate_slot: u32,
        mode: ProtocolMode,
        symbol: SymbolId,
        base: WorkerBaseSnapshot,
        base_sha256: [u8; 32],
        parameter_schema: OptimizerParameterSchema,
        tape: Arc<PackedReplayTape>,
    ) -> Result<Self> {
        anyhow::ensure!(
            matches!(mode, ProtocolMode::Replay | ProtocolMode::Optimize),
            "candidate symbol runtime requires replay or optimizer mode"
        );
        anyhow::ensure!(
            base_sha256.iter().any(|byte| *byte != 0),
            "candidate symbol runtime frozen base identity is zero"
        );
        let catalog = tape.market_catalog_arc();
        anyhow::ensure!(
            catalog.symbol() == symbol.as_str() && catalog.sha256() == tape.market_catalog_sha256(),
            "candidate symbol runtime frozen catalog identity mismatch symbol={}",
            symbol.as_str()
        );
        let ownership = SymbolOwnershipIndex::default();
        ownership.claim_frozen_market_catalog(catalog.records(), symbol.as_str())?;
        Ok(Self {
            candidate_slot,
            mode,
            symbol,
            base,
            base_sha256,
            parameter_schema,
            tape,
            catalog,
            ownership,
            candidate: None,
            worker_epoch: 0,
            next_intent_id: 1,
            next_owner_audit_seq: 1,
            last_global_sequence: None,
            last_central_state_seq: 0,
            local_event_count: 0,
        })
    }

    pub(super) fn build_candidate(
        &self,
        reset: &CandidateReset,
        initial_oracle_learning: Option<OracleLearningSnapshot>,
        oracle_direct_market_research: bool,
        collect_prediction_metrics: bool,
    ) -> Result<CandidateState> {
        let raw_values = reset.parameter_values().collect::<Vec<_>>();
        let values = self.parameter_schema.decode_values(&raw_values)?;
        let value_map = self
            .parameter_schema
            .specs()
            .iter()
            .zip(values.iter().cloned())
            .map(|(spec, value)| (spec.name.clone(), value))
            .collect::<BTreeMap<_, _>>();
        anyhow::ensure!(
            self.parameter_schema
                .candidate_sha256(&self.base.strategy_config, &value_map)?
                == reset.candidate_sha256,
            "worker candidate identity does not bind frozen base/schema/ordered values"
        );
        let config = self.parameter_schema.apply_symbol_values(
            &self.base.strategy_config,
            &values,
            self.symbol.as_str(),
        )?;
        self.new_candidate(
            reset.candidate_sha256,
            config,
            initial_oracle_learning,
            oracle_direct_market_research,
            collect_prediction_metrics,
        )
    }

    fn new_candidate(
        &self,
        candidate_sha256: [u8; 32],
        config: MakerRebateMmStrategyConfig,
        initial_oracle_learning: Option<OracleLearningSnapshot>,
        oracle_direct_market_research: bool,
        collect_prediction_metrics: bool,
    ) -> Result<CandidateState> {
        let odds = PolymarketOddsState::default();
        let portfolio = PortfolioState::default()
            .filtered_for_underlying(self.symbol.as_str(), &self.ownership);
        let open_orders = OpenOrdersState::default()
            .filtered_for_underlying(self.symbol.as_str(), &self.ownership);
        let risk = RiskBudgetSnapshot::default();
        let split_reason_target_per_side = config.max_inventory_per_token;
        let oracle_cfg = &self.base.oracle_config;
        let data_root = oracle_cfg.history.resolved_data_root();
        let symbols = oracle_cfg.active_symbols();
        let timeframes = oracle_cfg.to_transport_settings().step_timeframes();
        let max_bars = oracle_cfg.history.max_bars;
        let origin = Instant::now();
        let oracle_history = Arc::new(
            match self.tape.consumption_cursor()?.source_schema_version() {
                Some(1) => {
                    OracleHistory::load_retained(data_root, symbols, timeframes, max_bars, origin)?
                }
                Some(2 | STRATEGY_CONSUMPTION_SCHEMA_VERSION) | None => {
                    OracleHistory::new(data_root, symbols, timeframes, max_bars, origin)?
                }
                Some(version) => anyhow::bail!("unsupported strategy consumption schema={version}"),
            },
        );
        let mut actor = MakerRebateMmActor::new_symbol(MakerRebateMmSymbolActorInit {
            symbol: self.symbol.as_str(),
            cfg: config,
            equity_baseline_usdt: self.base.equity_baseline_usdt,
            oracle_cfg: self.base.oracle_config.clone(),
            oracle_history,
            odds,
            portfolio: portfolio.clone(),
            open_orders: open_orders.clone(),
            risk: risk.clone(),
            blocked_markets: HashSet::new(),
            ownership: self.ownership.clone(),
        })?;
        actor
            .strategy
            .oracle_history
            .apply_consumed_trade_flow(&actor.oracle_symbol, None, 0)?;
        actor.strategy.oracle_direct_market_research = oracle_direct_market_research;
        if let Some(learning) = initial_oracle_learning {
            actor.strategy.restore_oracle_learning(learning)?;
        }
        Ok(CandidateState {
            candidate_sha256,
            actor,
            prediction: collect_prediction_metrics
                .then(|| super::prediction::PredictionCollector::new(&self.catalog)),
            portfolio,
            open_orders,
            risk,
            pending_risk: None,
            last_tick: None,
            position_revision_by_token: HashMap::new(),
            order_revision_by_handle: HashMap::new(),
            lifecycle_revision_by_identity: HashMap::new(),
            split_reason_target_per_side,
        })
    }
}
