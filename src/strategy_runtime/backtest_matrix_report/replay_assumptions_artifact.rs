use serde::Serialize;

use crate::strategy_runtime::backtest_live_latency::{
    LatencyDistribution, ReplayExecutionTimingModel, ReplayLiveLatencySummary,
    EXECUTION_TIMING_MODEL_VERSION_V5,
};
use crate::strategy_runtime::replay_mode::{
    ReplayCtfSplitLatencySummary, SIMULATED_BACKTEST_EQUITY_SOURCE, SIMULATED_BACKTEST_EQUITY_USDT,
};

#[derive(Clone, Debug, Serialize)]
pub struct BacktestMatrixReplayAssumptionsArtifact {
    pub equity_baseline_usdt: f64,
    pub equity_baseline_source: &'static str,
    pub submit_latency_fixed_ms: Option<u64>,
    pub submit_latency_jitter_ms: BacktestMatrixLatencyProfileArtifact,
    pub submit_latency_source: String,
    pub submit_latency_source_captures: Vec<String>,
    pub submit_latency_rows: u64,
    pub marketable_taker_delay_ms: Option<u64>,
    pub marketable_taker_delay_source: Option<String>,
    pub execution_timing: ReplayExecutionTimingModel,
    pub synchronous_response_approximation: &'static str,
    pub exchange_entry_measurement: &'static str,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_receipt_admission_measurement: Option<&'static str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_receipt_admission_scheduling: Option<&'static str>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receipt_admission_approximation: Option<&'static str>,
    pub timestamp_quantization: &'static str,
    pub ctf_visibility_approximation: &'static str,
    pub candle_availability_contract: &'static str,
    pub trade_flow_availability_contract: &'static str,
    pub ctf_split_latency_ms: BacktestMatrixLatencyProfileArtifact,
    pub ctf_split_latency_source: String,
    pub ctf_split_latency_run_id: Option<String>,
    pub ctf_split_latency_source_run_ids: Vec<String>,
    pub ctf_split_latency_source_captures: Vec<String>,
    pub ctf_split_latency_rows: u64,
    pub fill_model: &'static str,
    pub queue_model: &'static str,
    pub market_impact: &'static str,
    pub order_book_evidence: &'static str,
    pub order_flow_event_evidence: &'static str,
    pub cancellation_flow_authority: &'static str,
    pub queue_position_authority: &'static str,
    pub marketable_non_immediate_submit_fill_authority: &'static str,
    pub resting_fill_trade_print_requirement: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct BacktestMatrixLatencyProfileArtifact {
    pub applied_min_ms: Option<u64>,
    pub applied_avg_ms: Option<u64>,
    pub applied_max_ms: Option<u64>,
    pub distribution_ms: LatencyDistribution,
    pub issue: Option<String>,
}

pub(super) fn replay_assumptions_artifact(
    live_latency: &ReplayLiveLatencySummary,
    ctf_split_latency: &ReplayCtfSplitLatencySummary,
    execution_timing: &ReplayExecutionTimingModel,
) -> BacktestMatrixReplayAssumptionsArtifact {
    let correlated_admission = execution_timing.model_version == EXECUTION_TIMING_MODEL_VERSION_V5;
    BacktestMatrixReplayAssumptionsArtifact {
        equity_baseline_usdt: SIMULATED_BACKTEST_EQUITY_USDT,
        equity_baseline_source: SIMULATED_BACKTEST_EQUITY_SOURCE,
        submit_latency_fixed_ms: None,
        submit_latency_jitter_ms: latency_profile_artifact(
            live_latency.replay_latency_profile_ms().ok(),
            live_latency.latency_ms.clone(),
            live_latency.issue.clone(),
        ),
        submit_latency_source: live_latency.source.clone(),
        submit_latency_source_captures: live_latency.source_captures.clone(),
        submit_latency_rows: live_latency.rows,
        marketable_taker_delay_ms: live_latency.marketable_taker_delay_ms,
        marketable_taker_delay_source: live_latency.marketable_taker_delay_source.clone(),
        execution_timing: execution_timing.clone(),
        synchronous_response_approximation:
            "modeled_delay_us=max(sampled_response_duration_us,qualifying_current_venue_hold_us)",
        exchange_entry_measurement:
            "exchange_entry_unmeasured_synchronous_response_is_visibility_proxy",
        canonical_receipt_admission_measurement: correlated_admission.then_some(
            "same_observation_http_response_terminal_to_canonical_append_not_exchange_match_or_strategy_inventory_visibility",
        ),
        canonical_receipt_admission_scheduling: correlated_admission.then_some(
            "immediate_fak_sell_unique_receipt:C=S+R+A_when_unrounded_C>=M;M=S+max(R,H);ceil_each_absolute_deadline_once",
        ),
        receipt_admission_approximation: correlated_admission.then_some(
            "missing_unsupported_or_contradictory_admission_retains_response_matching_approximation_without_invented_delay_or_venue_rejection",
        ),
        timestamp_quantization: "ceil_once_after_absolute_due_us",
        ctf_visibility_approximation: "pending_ts_ms_plus_sampled_pending_to_materialized_ms",
        candle_availability_contract:
            "schema2_and_schema3=recorded_consumed_candle_state;schema1=retained_file_preload_historical_availability_approximation_no_invented_receive_times",
        trade_flow_availability_contract:
            "schema3=immutable_consumed_five_second_snapshot_with_original_receipt_publication_consumption_clocks;schema1_and_schema2=absent;no_journal_reconstruction_or_replay_clock_aging;portfolio_changes_retain_last_recorded_snapshot",
        ctf_split_latency_ms: latency_profile_artifact(
            ctf_split_latency.replay_latency_profile_ms().ok(),
            ctf_split_latency.latency_ms.clone(),
            ctf_split_latency.issue.clone(),
        ),
        ctf_split_latency_source: ctf_split_latency.source.clone(),
        ctf_split_latency_run_id: ctf_split_latency.run_id.clone(),
        ctf_split_latency_source_run_ids: ctf_split_latency.source_run_ids.clone(),
        ctf_split_latency_source_captures: ctf_split_latency.source_captures.clone(),
        ctf_split_latency_rows: ctf_split_latency.rows,
        fill_model: "captured_ladder_submit_and_captured_last_trade_mbp_queue",
        queue_model: "captured_mbp_depth_plus_last_trade_queue_depletion",
        market_impact: "captured_ladder_vwap_no_hidden_liquidity",
        order_book_evidence: "captured_snapshot_l1_l5_depth",
        order_flow_event_evidence: "captured_last_trade_price_prints_no_atomic_add_cancel",
        cancellation_flow_authority: "unavailable_not_captured",
        queue_position_authority: "captured_mbp_depth_and_last_trade_prints",
        marketable_non_immediate_submit_fill_authority:
            "captured_ladder_snapshot_at_submit_for_marketable_gtc_gtd",
        resting_fill_trade_print_requirement:
            "last_trade_delta_rows_must_be_nonzero_for_trade_print_queue_depletion",
    }
}

fn latency_profile_artifact(
    applied: Option<(u64, u64, u64)>,
    distribution_ms: LatencyDistribution,
    issue: Option<String>,
) -> BacktestMatrixLatencyProfileArtifact {
    let (applied_min_ms, applied_avg_ms, applied_max_ms) = applied
        .map(|(min_ms, avg_ms, max_ms)| (Some(min_ms), Some(avg_ms), Some(max_ms)))
        .unwrap_or((None, None, None));
    BacktestMatrixLatencyProfileArtifact {
        applied_min_ms,
        applied_avg_ms,
        applied_max_ms,
        distribution_ms,
        issue,
    }
}
