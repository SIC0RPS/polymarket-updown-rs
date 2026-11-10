use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct BacktestProofMetadata {
    pub proof_domain: &'static str,
    pub artifact_classes_consumed: Vec<&'static str>,
    pub resolution_source: &'static str,
}

impl BacktestProofMetadata {
    pub fn normal_backtest() -> Self {
        Self {
            proof_domain: "normal_backtest",
            artifact_classes_consumed: vec![
                "captured_vendor_market_replay",
                "captured_polymarket_engine_state",
                "captured_chainlink_rtds_prices",
                "capture_local_bbwick_ohlc",
                "capture_config_snapshot_diagnostic_only",
                "current_optimizer_root_config",
                "capture_selected_execution_timing",
                "prepared_replay_sidecars",
                "typed_backtest_matrix_metrics",
            ],
            resolution_source: "captured_chainlink_boundary_resolution",
        }
    }
}
