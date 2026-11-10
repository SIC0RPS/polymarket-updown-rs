use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Result;

use super::{PackedReplayTape, PreparedReceiptTape};

#[path = "candidate_runner/baseline.rs"]
mod baseline;
#[path = "candidate_runner/clob_clock.rs"]
mod clob_clock;
#[path = "candidate_runner/ctf_chain.rs"]
mod ctf_chain;
#[path = "candidate_runner/input.rs"]
mod input;
#[path = "candidate_runner/pool.rs"]
mod pool;
#[path = "candidate_runner/run.rs"]
mod run;
#[path = "candidate_runner/schedule.rs"]
mod schedule;
#[path = "candidate_runner/slot.rs"]
mod slot;
#[path = "candidate_runner/transport.rs"]
mod transport;

pub(crate) use baseline::{
    CapturedCentralBaseline, SIMULATED_BACKTEST_EQUITY_SOURCE, SIMULATED_BACKTEST_EQUITY_USDT,
};
pub(crate) use clob_clock::{
    CapturedClobSubmitClock, CapturedClobSubmitModel, CapturedClobSubmitModelIdentity,
    DueClobSubmissions,
};
pub(crate) use ctf_chain::{
    CapturedCtfChainClock, CapturedCtfChainModel, CapturedCtfChainModelIdentity,
    CapturedCtfSplitVisibility,
};
pub(crate) use input::{CandidateExternalInput, CandidateInputBlock, CandidateInputDecoder};
pub(crate) use pool::{OwnedCandidateInputs, PersistentCandidatePool};
pub(crate) use run::{
    abort_candidate, advance_candidate, finalize_candidate, initialize_candidate, CandidateRun,
};
pub(crate) use schedule::{MergedEventSchedule, ScheduledEventRef, ScheduledEventTarget};
pub(crate) use slot::CandidateSlot;

pub(crate) struct CandidateReplayResult {
    pub(crate) candidate_id: String,
    pub(crate) elapsed_ms: u128,
    pub(crate) rows: Vec<crate::strategy_runtime::backtest_matrix::SymbolRow>,
    pub(crate) canonical_session_stats: crate::trade_session_ledger::CanonicalSessionStats,
    pub(crate) portfolio_accounting: crate::portfolio::PortfolioAccountingProjectionProof,
    pub(crate) clob_submit_model: CapturedClobSubmitModelIdentity,
    pub(crate) pending_clob_submissions: usize,
    pub(crate) pending_clob_receipts: usize,
    pub(crate) clob_supported_admission_submissions: u64,
    pub(crate) clob_approximate_admission_submissions: u64,
    pub(crate) clob_timing_diagnostics:
        Vec<crate::strategy_runtime::backtest_matrix_report::BacktestClobTimingDiagnostic>,
    pub(crate) ctf_chain_model: CapturedCtfChainModelIdentity,
    pub(crate) pending_ctf_split_receipts: usize,
    pub(crate) ctf_timing_diagnostics:
        Vec<crate::strategy_runtime::backtest_matrix_report::BacktestCtfTimingDiagnostic>,
    pub(crate) oracle_learning: BTreeMap<String, crate::strategy::OracleLearningSnapshot>,
    pub(crate) prediction_metrics:
        BTreeMap<String, crate::strategy_runtime::prediction_metrics::PredictionMetrics>,
}

/// Inputs that remain immutable for the lifetime of one replay/optimizer pool.
/// Mutable strategy, venue, account, risk, lifecycle, and accounting state is
/// candidate-slot owned and rebuilt for every candidate.
pub(crate) struct CandidateRunnerInputs<'a> {
    pub(crate) symbols: &'a [String],
    pub(crate) tapes: &'a BTreeMap<String, Arc<PackedReplayTape>>,
    pub(crate) receipt_tape: &'a Arc<PreparedReceiptTape>,
    pub(crate) window: Option<(u64, u64)>,
}

impl CandidateRunnerInputs<'_> {
    pub(crate) fn merged_schedule(&self) -> Result<MergedEventSchedule> {
        MergedEventSchedule::build(self.symbols, self.tapes, self.receipt_tape, self.window)
    }
}
