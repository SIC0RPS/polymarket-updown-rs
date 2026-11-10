use std::sync::Arc;

use polymarket_hft::capture::session::MaybeCapture;
use polymarket_hft::config::{AppConfig, SharedStrategyConfig};
use polymarket_hft::execution::supervisor::{ExecutionIntentBatch, OracleRuntimeStatus};
use polymarket_hft::execution::ExecutionClient;
use polymarket_hft::portfolio::PortfolioState;
use polymarket_hft::reclaim_merge::CtfTerminalMergeCommand;
use polymarket_hft::runtime_control::SharedRuntimeConfigState;
use polymarket_hft::strategy_runtime::actors::MakerRebateMmActorSet;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::app::ctf_inventory::CtfSplitCommand;

pub(crate) struct RunBbwickModeArgs<'a> {
    pub(crate) run_id: &'a str,
    pub(crate) cfg: &'a AppConfig,
    pub(crate) oracle_history: Arc<bbwick_engine::OracleHistory>,
    pub(crate) shared_strategy_cfg: SharedStrategyConfig,
    pub(crate) actors: &'a mut MakerRebateMmActorSet,
    pub(crate) exec: &'a ExecutionClient,
    pub(crate) intents_tx: &'a mpsc::UnboundedSender<ExecutionIntentBatch>,
    pub(crate) split_tx: &'a mpsc::Sender<Vec<CtfSplitCommand>>,
    pub(crate) terminal_merge_tx: &'a mpsc::Sender<Vec<CtfTerminalMergeCommand>>,
    pub(crate) cancel_requests_tx:
        &'a mpsc::UnboundedSender<Vec<polymarket_hft::strategy::CancelRequest>>,
    pub(crate) runtime_config_state: SharedRuntimeConfigState,
    pub(crate) engine_state_tx: Option<watch::Sender<Arc<bbwick_engine::PolymarketOddsState>>>,
    pub(crate) oracle_runtime_tx: Option<watch::Sender<OracleRuntimeStatus>>,
    pub(crate) market_projection_tx:
        Option<watch::Sender<polymarket_hft::polymarket::state::MarketProjection>>,
    pub(crate) portfolio_rx: watch::Receiver<PortfolioState>,
    pub(crate) obs: &'a polymarket_hft::observability::Observability,
    pub(crate) capture: &'a MaybeCapture,
    pub(crate) shutdown: CancellationToken,
}
