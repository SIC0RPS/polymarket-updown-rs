use super::*;
use polymarket_hft::strategy_runtime::prediction_metrics::PredictionMetrics;
use polymarket_hft::trade_session_ledger::CanonicalCoverageProof;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SelectionMode {
    #[default]
    Profit,
    Prediction,
}

impl SelectionMode {
    pub(crate) fn is_profit(&self) -> bool {
        matches!(self, Self::Profit)
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Profit => "profit",
            Self::Prediction => "prediction",
        }
    }

    pub(crate) fn collect_prediction_metrics(self) -> bool {
        matches!(self, Self::Prediction)
    }
}

impl std::str::FromStr for SelectionMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "profit" => Ok(Self::Profit),
            "prediction" => Ok(Self::Prediction),
            _ => bail!("optimizer.selection_mode must be profit or prediction, got {value:?}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TrialOrigin {
    Default,
    DefaultStabilityRecheck,
    ArchivedContender,
    Search,
    ChampionRecheck,
}

impl TrialOrigin {
    pub(crate) fn is_champion_eligible(self) -> bool {
        matches!(self, Self::Default | Self::ArchivedContender | Self::Search)
    }
}

pub(crate) struct CandidateRequest {
    pub(crate) params_by_symbol: BTreeMap<String, BTreeMap<String, ParamValue>>,
    pub(crate) origin: TrialOrigin,
    pub(crate) origin_id: Option<String>,
}

impl CandidateRequest {
    pub(crate) fn new(
        params_by_symbol: BTreeMap<String, BTreeMap<String, ParamValue>>,
        origin: TrialOrigin,
    ) -> Self {
        Self {
            params_by_symbol,
            origin,
            origin_id: None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct RetainedPackScore {
    #[serde(default, skip_serializing_if = "SelectionMode::is_profit")]
    pub(crate) selection_mode: SelectionMode,
    pub(crate) capture: String,
    pub(crate) candidate_pnl_by_symbol: Option<BTreeMap<String, PortfolioAccountingPnlRow>>,
    pub(crate) default_pnl_by_symbol: BTreeMap<String, PortfolioAccountingPnlRow>,
    pub(crate) candidate_entry_count: Option<u64>,
    pub(crate) default_entry_count: u64,
    pub(crate) candidate_profitable_market_count: Option<u64>,
    pub(crate) default_profitable_market_count: u64,
    pub(crate) candidate_selection_score: Option<scoring::FeasibilityScore>,
    pub(crate) default_selection_score: scoring::FeasibilityScore,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) candidate_prediction_metrics: BTreeMap<String, PredictionMetrics>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) default_prediction_metrics: BTreeMap<String, PredictionMetrics>,
    pub(crate) initial_oracle_learning_sha256: String,
    pub(crate) initial_oracle_learning_cutoffs_ms: BTreeMap<String, u64>,
    pub(crate) final_oracle_learning_sha256: Option<String>,
    pub(crate) accounting_coverage: CanonicalCoverageProof,
    pub(crate) error: Option<String>,
}

impl RetainedPackScore {
    pub(crate) fn is_eligible(&self) -> bool {
        self.candidate_pnl_by_symbol.is_some()
            && self.candidate_entry_count.is_some()
            && self.candidate_profitable_market_count.is_some()
            && self.candidate_selection_score.is_some()
            && self.accounting_coverage.is_complete()
            && self.error.is_none()
            && self.prediction_coverage_complete()
    }

    pub(crate) fn prediction_coverage_complete(&self) -> bool {
        if self.selection_mode.is_profit() {
            return self.candidate_prediction_metrics.is_empty()
                && self.default_prediction_metrics.is_empty();
        }
        self.default_pnl_by_symbol
            .keys()
            .eq(self.candidate_prediction_metrics.keys())
            && scoring::prediction_coverage_matches(
                &self.candidate_prediction_metrics,
                &self.default_prediction_metrics,
            )
    }
}

pub(crate) struct RequestedTrialMetrics {
    pub(crate) pnl_by_symbol: BTreeMap<String, PortfolioAccountingPnlRow>,
    pub(crate) split_entries: u64,
    pub(crate) submitted_orders: u64,
    pub(crate) managed_sell_fills: u64,
    pub(crate) wrong_side_first_sells: u64,
    pub(crate) accounting_coverage: CanonicalCoverageProof,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct RetainedCaptureProof {
    pub(crate) capture: String,
    pub(crate) canonical_session_stats: CanonicalSessionStats,
    pub(crate) portfolio_accounting: PortfolioAccountingProjectionProof,
    pub(crate) initial_oracle_learning_sha256: String,
    pub(crate) initial_oracle_learning_cutoffs_ms: BTreeMap<String, u64>,
    pub(crate) final_oracle_learning_sha256: String,
}

pub(crate) struct TrialRecord {
    pub(crate) selection_mode: SelectionMode,
    pub(crate) index: usize,
    pub(crate) candidate_id: String,
    pub(crate) elapsed_ms: u128,
    pub(crate) params_by_symbol: BTreeMap<String, BTreeMap<String, ParamValue>>,
    pub(crate) requested_metrics: Option<RequestedTrialMetrics>,
    pub(crate) selection_score: Option<scoring::FeasibilityScore>,
    pub(crate) retained_pack_scores: Vec<RetainedPackScore>,
    pub(crate) retained_capture_proofs: Vec<RetainedCaptureProof>,
    pub(crate) canonical_session_stats: Option<CanonicalSessionStats>,
    pub(crate) portfolio_accounting: Option<PortfolioAccountingProjectionProof>,
    pub(crate) origin: TrialOrigin,
    pub(crate) origin_id: Option<String>,
    pub(crate) oracle_learning_sha256: String,
}

impl TrialRecord {
    pub(crate) fn from_pack_results(
        symbols: &[String],
        anchor_capture: &str,
        index: usize,
        candidate_id: String,
        elapsed_ms: u128,
        params_by_symbol: BTreeMap<String, BTreeMap<String, ParamValue>>,
        stats: CanonicalSessionStats,
        portfolio_accounting: PortfolioAccountingProjectionProof,
        retain_canonical_proof: bool,
        origin: TrialOrigin,
        origin_id: Option<String>,
        selection_score: Option<scoring::FeasibilityScore>,
        retained_pack_scores: Vec<RetainedPackScore>,
        retained_capture_proofs: Vec<RetainedCaptureProof>,
        oracle_learning_sha256: String,
    ) -> Result<Self> {
        let requested_pack = retained_pack_scores
            .iter()
            .find(|pack| pack.capture == anchor_capture)
            .context("optimizer requested-pack score missing")?;
        let selection_mode = requested_pack.selection_mode;
        ensure!(
            retained_pack_scores
                .iter()
                .all(|pack| pack.selection_mode == selection_mode),
            "optimizer trial mixes selection modes"
        );
        let requested_metrics = if let Some(pnl_by_symbol) =
            requested_pack.candidate_pnl_by_symbol.clone()
        {
            let mut split_entries = 0_u64;
            let mut submitted_orders = 0_u64;
            let mut managed_sell_fills = 0_u64;
            for symbol in symbols {
                let coverage = stats
                    .canonical
                    .summary_for_symbol(symbol)
                    .with_context(|| format!("canonical symbol coverage missing for {symbol}"))?;
                split_entries = split_entries
                    .checked_add(coverage.split_entries)
                    .context("optimizer split-entry count overflow")?;
                submitted_orders = submitted_orders
                    .checked_add(coverage.submitted_orders)
                    .context("optimizer submitted-order count overflow")?;
                managed_sell_fills = managed_sell_fills
                    .checked_add(coverage.confirmed_managed_sell_fill_count)
                    .context("optimizer managed-sell fill count overflow")?;
            }
            Some(RequestedTrialMetrics {
                pnl_by_symbol,
                split_entries,
                submitted_orders,
                managed_sell_fills,
                wrong_side_first_sells: stats.wrong_side_first_sells.unwrap_or_default(),
                accounting_coverage: requested_pack.accounting_coverage.clone(),
            })
        } else {
            None
        };
        Ok(Self {
            selection_mode,
            index,
            candidate_id,
            elapsed_ms,
            params_by_symbol,
            requested_metrics,
            selection_score,
            retained_pack_scores,
            retained_capture_proofs,
            canonical_session_stats: retain_canonical_proof.then_some(stats),
            portfolio_accounting: retain_canonical_proof.then_some(portfolio_accounting),
            origin,
            origin_id,
            oracle_learning_sha256,
        })
    }

    pub(crate) fn complete_coverage(&self) -> bool {
        self.selection_score.is_some()
            && self
                .retained_pack_scores
                .iter()
                .all(RetainedPackScore::is_eligible)
    }

    pub(crate) fn is_feasible(&self) -> bool {
        self.complete_coverage()
            && self
                .selection_score
                .is_some_and(scoring::FeasibilityScore::is_feasible)
    }
}

pub(crate) struct RunContext {
    pub(crate) selection_mode: SelectionMode,
    pub(crate) run_root: PathBuf,
    pub(crate) objective_profile: ObjectiveProfile,
    pub(crate) cfg: AppConfig,
    pub(crate) selection_inputs: Vec<capture_inputs::FrozenCaptureInput>,
    pub(crate) holdout_inputs: Vec<capture_inputs::FrozenCaptureInput>,
    pub(crate) optimizer_spec_path: PathBuf,
    pub(crate) async_runtime: tokio::runtime::Runtime,
    pub(crate) optimizer_observability: ObservabilityRuntime,
    pub(crate) candidate_slots: usize,
    pub(crate) symbols: Vec<String>,
    pub(crate) parameter_schemas: BTreeMap<String, OptimizerParameterSchema>,
    pub(crate) selection_evaluator: Mutex<Option<(String, BacktestMatrixEvaluator)>>,
    pub(crate) trial_counter: AtomicUsize,
    pub(crate) trials: Mutex<Vec<TrialRecord>>,
}

impl RunContext {
    pub(crate) fn primary_capture(&self) -> &Path {
        &self
            .selection_inputs
            .first()
            .expect("selection input invariant")
            .capture
    }

    pub(crate) fn retained_pack_count(&self) -> usize {
        self.selection_inputs.len()
    }

    pub(crate) fn evaluation_scope(&self) -> &'static str {
        if self.symbols.len() == 1 {
            "single_capture_single_symbol_selection"
        } else {
            "single_capture_four_symbol_integration"
        }
    }

    pub(crate) fn parameter_count(&self) -> usize {
        self.symbols
            .iter()
            .map(|symbol| {
                self.parameter_schemas
                    .get(symbol)
                    .expect("optimizer symbol schema invariant")
                    .specs()
                    .len()
            })
            .sum()
    }
}

#[derive(Debug)]
pub(crate) struct Args {
    pub(crate) spec: PathBuf,
    pub(crate) spec_explicit: bool,
    pub(crate) capture_session: String,
    pub(crate) selection_captures: Vec<String>,
    pub(crate) holdout_captures: Vec<String>,
    pub(crate) jobs: Option<usize>,
    pub(crate) seed: Option<u64>,
    pub(crate) no_apply: bool,
    pub(crate) frozen_capture_inputs_sha256: Option<String>,
    pub(crate) default_spec_snapshot: Option<PathBuf>,
    pub(crate) default_spec_sha256: Option<String>,
    pub(crate) root_spec_sha256: Option<String>,
    pub(crate) objective_profile_sha256: Option<String>,
    pub(crate) parent_run_root: Option<PathBuf>,
    pub(crate) proof_only_run_root: Option<PathBuf>,
    pub(crate) finalize_retained_run_root: Option<PathBuf>,
    pub(crate) symbol: String,
}

#[derive(Clone, Debug)]
pub(crate) struct OptimizerSettings {
    pub(crate) selection_mode: SelectionMode,
    pub(crate) jobs: usize,
    pub(crate) seed: u64,
    pub(crate) max_function_evals: usize,
    pub(crate) max_generations_per_run: usize,
    pub(crate) max_no_improvement_evals: usize,
    pub(crate) apply_best: bool,
}
