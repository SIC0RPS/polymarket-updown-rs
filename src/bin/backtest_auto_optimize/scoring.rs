use super::*;
use polymarket_hft::strategy_runtime::prediction_metrics::{
    equal_market_scores, PredictionMetrics,
};
use polymarket_hft::trade_session_ledger::CanonicalCoverageProof;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct ObjectiveScore {
    pub(super) numerator: i128,
    pub(super) denominator: i128,
}

impl ObjectiveScore {
    pub(super) fn compare(self, other: Self) -> Result<std::cmp::Ordering> {
        let left = self
            .numerator
            .checked_mul(other.denominator)
            .context("objective score comparison overflow")?;
        let right = other
            .numerator
            .checked_mul(self.denominator)
            .context("objective score comparison overflow")?;
        Ok(left.cmp(&right))
    }

    pub(super) fn cma_transport(self) -> Result<f64> {
        let value = self.numerator as f64 / self.denominator as f64;
        ensure!(value.is_finite(), "objective score is not finite");
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct FeasibilityScore {
    pub(super) objective_score: ObjectiveScore,
    pub(super) shortfall_units_6dp: u128,
}

impl FeasibilityScore {
    pub(super) fn is_feasible(self) -> bool {
        self.shortfall_units_6dp == 0
    }

    pub(super) fn compare(self, other: Self) -> Result<std::cmp::Ordering> {
        match (self.is_feasible(), other.is_feasible()) {
            (true, false) => Ok(std::cmp::Ordering::Greater),
            (false, true) => Ok(std::cmp::Ordering::Less),
            (true, true) => self.objective_score.compare(other.objective_score),
            (false, false) => Ok(other.shortfall_units_6dp.cmp(&self.shortfall_units_6dp)),
        }
    }

    pub(super) fn cma_transport(self) -> Result<f64> {
        let transported = if self.is_feasible() {
            let objective = self.objective_score.cma_transport()?;
            2.0 + objective / (1.0 + objective.abs())
        } else {
            let shortfall = self.shortfall_units_6dp as f64;
            -shortfall / (1.0 + shortfall)
        };
        ensure!(
            transported.is_finite(),
            "optimizer feasibility score is not finite"
        );
        Ok(transported)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum SelectionScore {
    Profit(FeasibilityScore),
    Prediction { brier: f64 },
}

impl SelectionScore {
    pub(super) fn compare(self, other: Self) -> Result<std::cmp::Ordering> {
        match (self, other) {
            (Self::Profit(left), Self::Profit(right)) => left.compare(right),
            (Self::Prediction { brier: left }, Self::Prediction { brier: right }) => {
                let left = validated_brier(left)?;
                let right = validated_brier(right)?;
                right
                    .partial_cmp(&left)
                    .context("prediction Brier comparison is undefined")
            }
            _ => bail!("optimizer selection score mode mismatch"),
        }
    }

    pub(super) fn cma_transport(self) -> Result<f64> {
        match self {
            Self::Profit(score) => score.cma_transport(),
            Self::Prediction { brier } => Ok(-validated_brier(brier)?),
        }
    }
}

fn validated_brier(value: f64) -> Result<f64> {
    ensure!(
        value.is_finite() && (0.0..=1.0).contains(&value),
        "optimizer prediction Brier is invalid"
    );
    Ok(value)
}

pub(super) fn validate_prediction_evidence(
    mode: types::SelectionMode,
    symbols: &[String],
    metrics: &BTreeMap<String, PredictionMetrics>,
) -> Result<()> {
    if mode.is_profit() {
        ensure!(
            metrics.is_empty(),
            "profit selection contains prediction evidence"
        );
        return Ok(());
    }
    ensure!(
        !symbols.is_empty()
            && metrics.len() == symbols.len()
            && symbols.iter().all(|symbol| metrics.contains_key(symbol)),
        "optimizer prediction evidence symbol scope mismatch"
    );
    for (symbol, evidence) in metrics {
        evidence
            .validate()
            .with_context(|| format!("prediction evidence symbol={symbol}"))?;
    }
    Ok(())
}

pub(super) fn prediction_coverage_matches(
    candidate: &BTreeMap<String, PredictionMetrics>,
    reference: &BTreeMap<String, PredictionMetrics>,
) -> bool {
    !candidate.is_empty()
        && candidate.len() == reference.len()
        && candidate.iter().all(|(symbol, evidence)| {
            reference
                .get(symbol)
                .is_some_and(|other| evidence.comparable_coverage(other))
        })
}

pub(super) fn selection_score(
    mode: types::SelectionMode,
    financial: FeasibilityScore,
    candidate: &BTreeMap<String, PredictionMetrics>,
    reference: &BTreeMap<String, PredictionMetrics>,
) -> Result<SelectionScore> {
    match mode {
        types::SelectionMode::Profit => {
            ensure!(
                candidate.is_empty() && reference.is_empty(),
                "profit selection contains prediction evidence"
            );
            Ok(SelectionScore::Profit(financial))
        }
        types::SelectionMode::Prediction => {
            ensure!(
                prediction_coverage_matches(candidate, reference),
                "optimizer prediction evidence is inadmissible or incomparable"
            );
            let scores =
                equal_market_scores(candidate.values().flat_map(|metrics| {
                    metrics.markets.values().map(|market| market.scores.clone())
                }));
            let brier = validated_brier(scores.brier.context("prediction Brier is unavailable")?)?;
            Ok(SelectionScore::Prediction { brier })
        }
    }
}

pub(super) fn trial_selection_score(trial: &TrialRecord) -> Result<SelectionScore> {
    ensure!(
        trial.complete_coverage(),
        "optimizer trial selection evidence is incomplete"
    );
    ensure!(
        trial.retained_pack_scores.len() == 1,
        "optimizer trial requires one frozen selection capture"
    );
    let pack = &trial.retained_pack_scores[0];
    ensure!(
        pack.selection_mode == trial.selection_mode
            && pack.candidate_selection_score == trial.selection_score,
        "optimizer trial selection evidence identity mismatch"
    );
    selection_score(
        trial.selection_mode,
        trial
            .selection_score
            .context("optimizer financial score is unavailable")?,
        &pack.candidate_prediction_metrics,
        &pack.default_prediction_metrics,
    )
}

pub(super) fn prediction_improves(
    candidate: &PredictionMetrics,
    incumbent: &PredictionMetrics,
) -> Result<bool> {
    if !candidate.comparable_coverage(incumbent) {
        return Ok(false);
    }
    let candidate_score = SelectionScore::Prediction {
        brier: candidate
            .brier
            .context("candidate prediction Brier is unavailable")?,
    };
    let incumbent_score = SelectionScore::Prediction {
        brier: incumbent
            .brier
            .context("incumbent prediction Brier is unavailable")?,
    };
    Ok(candidate_score.compare(incumbent_score)?.is_gt())
}

pub(super) struct EndpointJointScore {
    pub(super) pnl_by_symbol: BTreeMap<String, PortfolioAccountingPnlRow>,
    pub(super) accounting_coverage: CanonicalCoverageProof,
    pub(super) entry_count: u64,
    pub(super) profitable_market_count: u64,
    pub(super) selection_score: FeasibilityScore,
}

pub(super) fn endpoint_joint_score(
    profile: &ObjectiveProfile,
    expected_symbols: &[String],
    stats: &CanonicalSessionStats,
    portfolio_accounting: &PortfolioAccountingProjectionProof,
) -> Result<EndpointJointScore> {
    ensure!(
        !expected_symbols.is_empty(),
        "optimizer score scope is empty"
    );
    let expected_symbols = expected_symbols.to_vec();
    let actual_symbols = stats
        .entry_condition_ids_by_symbol()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        actual_symbols == expected_symbols,
        "optimizer scorer catalog scope mismatch expected={expected_symbols:?} actual={actual_symbols:?}"
    );
    let accounting_coverage = stats.proof_for_eligible_markets(portfolio_accounting);
    if !accounting_coverage.is_complete() {
        crate::summary::publish_progress(format_args!(
            "CMAES_AUTO_OPTIMIZE candidate_ineligible symbols={} gaps={:?}",
            expected_symbols.join(","),
            accounting_coverage.gaps
        ));
    }

    let mut pnl_by_symbol = BTreeMap::new();
    let mut shortfall_units_6dp = 0_u128;
    let mut entry_count = 0_u64;
    for symbol in &expected_symbols {
        let pnl = portfolio_accounting
            .row_for_symbol(symbol)
            .cloned()
            .with_context(|| format!("endpoint accounting row unavailable symbol={symbol}"))?;
        let realized = pnl
            .receipt_realized_pnl_units_6dp
            .with_context(|| format!("receipt realized PnL unavailable symbol={symbol}"))?;
        let shortfall = 1_i128
            .checked_sub(realized)
            .context("optimizer profitability shortfall overflow")?
            .max(0);
        shortfall_units_6dp = shortfall_units_6dp
            .checked_add(u128::try_from(shortfall).context("optimizer shortfall is negative")?)
            .context("optimizer profitability shortfall sum overflow")?;
        entry_count = entry_count
            .checked_add(
                stats
                    .canonical
                    .summary_for_symbol(symbol)
                    .with_context(|| format!("canonical entry count unavailable symbol={symbol}"))?
                    .confirmed_entry_market_count,
            )
            .context("optimizer aggregate entry count overflow")?;
        ensure!(
            pnl_by_symbol.insert(symbol.clone(), pnl).is_none(),
            "duplicate optimizer accounting row symbol={symbol}"
        );
    }
    let profitable_market_count = expected_symbols.iter().try_fold(0_u64, |total, symbol| {
        total
            .checked_add(profitable_market_count(stats, symbol)?)
            .context("optimizer aggregate profitable-market count overflow")
    })?;
    let total = portfolio_accounting
        .row_for_symbol("TOTAL")
        .context("endpoint TOTAL accounting row unavailable")?;
    let objective_score = objective_score(
        profile,
        total_pnl_units_6dp(total)?,
        total
            .receipt_realized_pnl_units_6dp
            .context("TOTAL receipt realized PnL unavailable")?,
        entry_count,
        profitable_market_count,
    )?;
    Ok(EndpointJointScore {
        pnl_by_symbol,
        accounting_coverage,
        entry_count,
        profitable_market_count,
        selection_score: FeasibilityScore {
            objective_score,
            shortfall_units_6dp,
        },
    })
}

fn profitable_market_count(stats: &CanonicalSessionStats, symbol: &str) -> Result<u64> {
    let count = stats
        .closed_cohorts
        .iter()
        .filter(|cohort| cohort.symbol == symbol && cohort.realized_pnl_units_6dp > 0)
        .count();
    u64::try_from(count).context("profitable market count overflow")
}

pub(super) fn objective_score(
    profile: &ObjectiveProfile,
    total_pnl_units_6dp: i128,
    realized_pnl_units_6dp: i128,
    entry_count: u64,
    profitable_market_count: u64,
) -> Result<ObjectiveScore> {
    profile.score(
        total_pnl_units_6dp,
        realized_pnl_units_6dp,
        entry_count,
        profitable_market_count,
    )
}

pub(super) fn total_pnl_units_6dp(row: &PortfolioAccountingPnlRow) -> Result<i128> {
    let realized = row
        .receipt_realized_pnl_units_6dp
        .with_context(|| format!("receipt realized PnL unavailable symbol={}", row.symbol))?;
    let open = row
        .open_pnl_units_6dp
        .with_context(|| format!("open PnL unavailable symbol={}", row.symbol))?;
    let total = row
        .total_pnl_units_6dp
        .with_context(|| format!("total PnL unavailable symbol={}", row.symbol))?;
    ensure!(
        realized.checked_add(open) == Some(total),
        "endpoint PnL identity mismatch symbol={}",
        row.symbol
    );
    Ok(total)
}
