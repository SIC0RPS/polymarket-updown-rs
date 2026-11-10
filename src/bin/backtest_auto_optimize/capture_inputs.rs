use super::*;
use polymarket_hft::capture::session::captured_initial_oracle_learning_bytes;
use polymarket_hft::capture::{
    finalized_capture_identity, finalized_capture_metadata_identity, FinalizedCaptureIdentity,
};
use polymarket_hft::strategy_runtime::backtest_live_latency::{
    select_replay_execution_timing, ReplayExecutionTimingModel,
};
use polymarket_hft::strategy_runtime::replay_mode::{
    prepare_backtest_replay_for_capture, prepared_market_inputs_for_capture,
    resolve_capture_session_for_replay, validate_prepared_replay_sources_for_optimizer,
    RecordedMarketPopulation,
};

#[path = "capture_inputs/discovery.rs"]
mod discovery;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CaptureRole {
    Selection,
    Holdout,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FrozenCaptureInput {
    pub(crate) role: CaptureRole,
    pub(crate) capture: PathBuf,
    pub(crate) session_id: String,
    pub(crate) run_id: String,
    pub(crate) capture_start_ms: u64,
    pub(crate) capture_end_ms: u64,
    pub(crate) replay_start_ms: u64,
    pub(crate) replay_end_ms: u64,
    pub(crate) market_lifecycles: Vec<FrozenMarketLifecycle>,
    pub(crate) prepared_proof_identities_by_symbol:
        BTreeMap<String, candidate_proof::SymbolChildProofIdentity>,
    pub(crate) initial_oracle_learning_sha256: String,
    pub(crate) initial_oracle_learning_sha256_by_symbol: BTreeMap<String, String>,
    pub(crate) initial_oracle_learning_cutoffs_ms: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) recorded_market_population: Option<RecordedMarketPopulation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) execution_timing: Option<ReplayExecutionTimingModel>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub(crate) struct FrozenMarketLifecycle {
    pub(crate) symbol: String,
    pub(crate) condition_id: String,
    pub(crate) window_start_ms: u64,
    pub(crate) window_end_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) struct FrozenCaptureInputs {
    pub(crate) selection: Vec<FrozenCaptureInput>,
    pub(crate) holdout: Vec<FrozenCaptureInput>,
}

impl FrozenCaptureInputs {
    pub(crate) fn freeze(
        cfg: &AppConfig,
        args: &Args,
        symbols: &[String],
        optimizer_spec_path: &Path,
        objective_profile_sha256: &str,
        jobs: usize,
    ) -> Result<Self> {
        let root_spec_sha256 = candidate_proof::file_sha256(optimizer_spec_path)?;
        let preparation_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(jobs.max(1))
            .build()
            .context("build bounded optimizer capture preparation pool")?;
        let mut frozen = discovery::freeze_capture_inputs(
            cfg,
            args,
            symbols,
            &root_spec_sha256,
            objective_profile_sha256,
            &preparation_pool,
        )?;
        if let Some(selection) = frozen.selection.first() {
            let symbols = selection.symbols()?;
            for holdout in &mut frozen.holdout {
                *holdout = holdout.for_symbols(&symbols)?;
            }
            frozen.symbols()?;
        }
        Ok(frozen)
    }

    pub(crate) fn freeze_retained_capture(
        cfg: &AppConfig,
        capture: &Path,
        role: CaptureRole,
        symbols: &[String],
        optimizer_spec_path: &Path,
        objective_profile_sha256: &str,
        jobs: usize,
    ) -> Result<Vec<FrozenCaptureInput>> {
        let requested =
            discovery::resolve_requested_captures(cfg, &[capture.display().to_string()])?;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(jobs.max(1))
            .build()?;
        let root_spec_sha256 = candidate_proof::file_sha256(optimizer_spec_path)?;
        freeze_role(
            cfg,
            requested,
            role,
            symbols,
            &root_spec_sha256,
            objective_profile_sha256,
            &pool,
        )
    }

    pub(crate) fn all(&self) -> impl Iterator<Item = &FrozenCaptureInput> {
        self.selection.iter().chain(&self.holdout)
    }

    pub(crate) fn sha256(&self) -> Result<String> {
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }

    pub(crate) fn validate_frozen_execution_timing(&self, cfg: &AppConfig) -> Result<()> {
        for input in self.all().filter(|input| input.execution_timing.is_some()) {
            input.validate_frozen_execution_timing(cfg)?;
        }
        Ok(())
    }

    pub(crate) fn snapshot_path(optimizer_snapshot: &Path) -> PathBuf {
        optimizer_snapshot.with_extension("frozen_capture_inputs.json")
    }

    pub(crate) fn write_symbol_snapshot(&self, optimizer_snapshot: &Path) -> Result<String> {
        let path = Self::snapshot_path(optimizer_snapshot);
        let bytes = serde_json::to_vec(self).context("serialize frozen child capture inputs")?;
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        fs::write(&path, bytes)
            .with_context(|| format!("write frozen child capture inputs {}", path.display()))?;
        ensure!(
            candidate_proof::file_sha256(&path)? == sha256,
            "frozen child capture snapshot hash mismatch"
        );
        Ok(sha256)
    }

    pub(crate) fn load_symbol_snapshot(
        optimizer_snapshot: &Path,
        symbol: &str,
        expected_sha256: &str,
        cfg: &AppConfig,
        root_spec_path: &Path,
        objective_profile_sha256: &str,
    ) -> Result<Self> {
        let path = Self::snapshot_path(optimizer_snapshot);
        let bytes = fs::read(&path)
            .with_context(|| format!("read frozen child capture inputs {}", path.display()))?;
        ensure!(
            format!("{:x}", Sha256::digest(&bytes)) == expected_sha256,
            "frozen child capture snapshot changed symbol={symbol}"
        );
        let frozen: Self = serde_json::from_slice(&bytes)
            .with_context(|| format!("parse frozen child capture inputs {}", path.display()))?;
        ensure!(
            frozen.sha256()? == expected_sha256,
            "frozen child capture inputs differ from parent symbol={symbol}"
        );
        frozen.validate_symbol_snapshot(symbol, cfg, root_spec_path, objective_profile_sha256)?;
        Ok(frozen)
    }

    fn validate_symbol_snapshot(
        &self,
        symbol: &str,
        cfg: &AppConfig,
        root_spec_path: &Path,
        objective_profile_sha256: &str,
    ) -> Result<()> {
        ensure!(
            self.selection.len() == 1 && self.holdout.is_empty(),
            "frozen child scope must contain exactly one selection capture symbol={symbol}"
        );
        let input = &self.selection[0];
        ensure!(
            input.role == CaptureRole::Selection
                && !input.market_lifecycles.is_empty()
                && input
                    .market_lifecycles
                    .iter()
                    .all(|market| market.symbol == symbol)
                && input.prepared_proof_identities_by_symbol.len() == 1
                && input
                    .prepared_proof_identities_by_symbol
                    .contains_key(symbol)
                && input.initial_oracle_learning_sha256_by_symbol.len() == 1
                && input
                    .initial_oracle_learning_sha256_by_symbol
                    .contains_key(symbol)
                && input
                    .initial_oracle_learning_cutoffs_ms
                    .keys()
                    .all(|candidate| candidate == symbol)
                && input
                    .recorded_market_population
                    .as_ref()
                    .is_none_or(|population| {
                        !population.records().is_empty()
                            && population
                                .records()
                                .iter()
                                .all(|record| record.symbol == symbol)
                    }),
            "frozen child symbol scope is invalid symbol={symbol}"
        );
        ensure!(
            input.initial_oracle_learning_sha256
                == *input
                    .initial_oracle_learning_sha256_by_symbol
                    .get(symbol)
                    .context("frozen child learner identity missing")?,
            "frozen child learner identity mismatch symbol={symbol}"
        );
        input.validate_frozen_execution_timing(cfg)?;
        validate_prepared_replay_sources_for_optimizer(
            &input.capture,
            std::slice::from_ref(&symbol.to_string()),
        )?;
        let root_spec_sha256 = candidate_proof::file_sha256(root_spec_path)?;
        let current_proofs = candidate_proof::build_symbol_proof_identities(
            std::slice::from_ref(&symbol),
            &input.capture,
            cfg,
            &root_spec_sha256,
            objective_profile_sha256,
        )?;
        ensure!(
            input.prepared_proof_identities_by_symbol.get(symbol) == Some(&current_proofs[symbol]),
            "frozen child capture content changed symbol={symbol}"
        );
        Ok(())
    }

    pub(crate) fn symbols(&self) -> Result<Vec<String>> {
        ensure!(
            self.selection.len() == 1,
            "frozen selection capture missing"
        );
        let symbols = self.selection[0].symbols()?;
        for input in self.all() {
            ensure!(
                input.symbols()? == symbols,
                "frozen participating symbol coverage differs capture={}",
                input.capture.display()
            );
        }
        Ok(symbols)
    }

    pub(crate) fn for_symbol_selection(&self, symbol: &str) -> Result<Self> {
        Ok(Self {
            selection: self
                .selection
                .iter()
                .map(|input| input.for_symbols(&[symbol.to_string()]))
                .collect::<Result<_>>()?,
            holdout: Vec::new(),
        })
    }
}

fn required_execution_timing(
    cfg: &AppConfig,
    capture: &Path,
) -> Result<ReplayExecutionTimingModel> {
    let timing = select_replay_execution_timing(
        capture,
        &cfg.polymarket.execution,
        cfg.polymarket.use_server_time,
    )
    .with_context(|| {
        format!(
            "invalid optimizer execution timing evidence capture={}",
            capture.display()
        )
    })?;
    let clob_observations = timing
        .clob
        .as_ref()
        .map_or(0, |source| source.observations.len());
    let ctf_observations = timing
        .ctf
        .as_ref()
        .map_or(0, |source| source.observations.len());
    for (operation, observations) in [("CLOB", clob_observations), ("CTF", ctf_observations)] {
        ensure!(
            observations > 0,
            "optimizer required timing observations unavailable capture={} operation={operation}",
            capture.display()
        );
    }
    Ok(timing)
}

fn freeze_role(
    cfg: &AppConfig,
    requested: Vec<discovery::DiscoveredCapture>,
    role: CaptureRole,
    symbols: &[String],
    root_spec_sha256: &str,
    objective_profile_sha256: &str,
    preparation_pool: &rayon::ThreadPool,
) -> Result<Vec<FrozenCaptureInput>> {
    let mut frozen = Vec::with_capacity(requested.len());
    let mut identities = BTreeSet::new();
    for input in requested {
        let capture = input.path;
        let finalized = input.finalized;
        let execution_timing = required_execution_timing(cfg, &capture)?;
        ensure!(
            identities.insert((finalized.session_id.clone(), finalized.run_id.clone())),
            "duplicate optimizer capture identity session={} run={}",
            finalized.session_id,
            finalized.run_id
        );
        let qualified_symbols = preparation_pool
            .install(|| {
                prepare_backtest_replay_for_capture(
                    &capture,
                    symbols,
                    &execution_timing,
                    true,
                    false,
                )
            })?
            .into_iter()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect::<Vec<_>>();
        ensure!(
            !qualified_symbols.is_empty(),
            "optimizer capture has no replay eligible requested symbols: {}",
            capture.display()
        );
        let symbols = qualified_symbols.as_slice();

        crate::summary::publish_progress(format_args!(
            "CMAES_AUTO_OPTIMIZE freeze_metadata_begin capture={}",
            capture.display()
        ));
        let replay_window = BacktestMatrixEvaluator::replay_window_for_capture(&capture)?;
        let captured_initial_oracle_learning: BTreeMap<String, OracleLearningSnapshot> =
            match captured_initial_oracle_learning_bytes(&capture)? {
                Some(bytes) => rmp_serde::from_slice(&bytes)
                    .context("decode captured initial oracle learning")?,
                None => BTreeMap::new(),
            };
        for learning_symbol in captured_initial_oracle_learning.keys() {
            ensure!(
                VALID_ASSETS
                    .iter()
                    .any(|expected| expected.eq_ignore_ascii_case(learning_symbol)),
                "captured initial oracle learning contains unknown capture symbol={learning_symbol}"
            );
        }
        let (replay_start_ms, replay_end_ms) = replay_window;
        ensure!(
            replay_start_ms < replay_end_ms,
            "optimizer capture replay window is empty"
        );
        let mut market_lifecycles = Vec::new();
        let mut recorded_market_population = RecordedMarketPopulation::default();
        let mut initial_oracle_learning = BTreeMap::new();
        let mut initial_oracle_learning_cutoffs_ms = BTreeMap::new();
        let mut initial_oracle_learning_sha256_by_symbol = BTreeMap::new();
        let mut prepared_proof_identities_by_symbol = BTreeMap::new();
        for symbol in symbols {
            let qualified = (|| -> Result<_> {
                let (lifecycles, population) =
                    prepared_market_inputs_for_capture(&capture, symbol)?;
                ensure!(
                    !lifecycles.is_empty(),
                    "prepared market lifecycle missing symbol={symbol}"
                );
                let learning = captured_initial_oracle_learning
                    .get(symbol)
                    .cloned()
                    .map(|state| BTreeMap::from([(symbol.clone(), state)]))
                    .unwrap_or_default();
                for state in learning.values() {
                    ensure!(
                        state.knowledge_cutoff_ms() < replay_start_ms,
                        "capture initial learner cutoff reaches replay symbol={symbol}"
                    );
                }
                let learning_sha256 = candidate_proof::oracle_learning_sha256(&learning)?;
                crate::summary::publish_progress(format_args!(
                    "CMAES_AUTO_OPTIMIZE freeze_proof_hash_begin capture={} symbol={symbol}",
                    capture.display()
                ));
                let proofs = candidate_proof::build_symbol_proof_identities(
                    std::slice::from_ref(symbol),
                    &capture,
                    cfg,
                    root_spec_sha256,
                    objective_profile_sha256,
                )?;
                Ok((lifecycles, population, learning, learning_sha256, proofs))
            })();
            match qualified {
                Ok((lifecycles, population, learning, learning_sha256, proofs)) => {
                    market_lifecycles.extend(lifecycles.into_iter().map(
                        |(symbol, condition_id, window_start_ms, window_end_ms)| {
                            FrozenMarketLifecycle {
                                symbol,
                                condition_id,
                                window_start_ms,
                                window_end_ms,
                            }
                        },
                    ));
                    recorded_market_population.merge(population)?;
                    initial_oracle_learning_sha256_by_symbol
                        .insert(symbol.clone(), learning_sha256);
                    for (symbol, state) in learning {
                        initial_oracle_learning_cutoffs_ms
                            .insert(symbol.clone(), state.knowledge_cutoff_ms());
                        initial_oracle_learning.insert(symbol, state);
                    }
                    prepared_proof_identities_by_symbol.extend(proofs);
                    crate::summary::publish_progress(format_args!(
                        "CMAES_AUTO_OPTIMIZE freeze_proof_hash_done capture={} symbol={symbol}",
                        capture.display()
                    ));
                }
                Err(error) => {
                    let _ = writeln!(
                        std::io::stderr().lock(),
                        "CMAES_AUTO_OPTIMIZE symbol_unavailable capture={} symbol={symbol} reason={error:#}",
                        capture.display()
                    );
                }
            }
        }
        ensure!(
            !prepared_proof_identities_by_symbol.is_empty(),
            "optimizer capture has no qualified requested symbols: {}",
            capture.display()
        );
        market_lifecycles.sort();
        market_lifecycles.dedup();
        let initial_oracle_learning_sha256 =
            candidate_proof::oracle_learning_sha256(&initial_oracle_learning)?;
        crate::summary::publish_progress(format_args!(
            "CMAES_AUTO_OPTIMIZE freeze_metadata_done capture={}",
            capture.display()
        ));
        frozen.push(FrozenCaptureInput {
            role,
            capture,
            session_id: finalized.session_id,
            run_id: finalized.run_id,
            capture_start_ms: finalized.start_ts_ms,
            capture_end_ms: finalized.end_ts_ms,
            replay_start_ms,
            replay_end_ms,
            market_lifecycles,
            prepared_proof_identities_by_symbol,
            initial_oracle_learning_sha256,
            initial_oracle_learning_sha256_by_symbol,
            initial_oracle_learning_cutoffs_ms,
            recorded_market_population: Some(recorded_market_population),
            execution_timing: Some(execution_timing),
        });
    }
    frozen.sort_by_key(|input| {
        (
            input.capture_start_ms,
            input.capture_end_ms,
            input.session_id.clone(),
            input.run_id.clone(),
        )
    });
    Ok(frozen)
}

impl FrozenCaptureInput {
    pub(crate) fn symbols(&self) -> Result<Vec<String>> {
        let symbols = VALID_ASSETS
            .iter()
            .filter(|symbol| {
                self.prepared_proof_identities_by_symbol
                    .contains_key(**symbol)
            })
            .map(|symbol| (*symbol).to_string())
            .collect::<Vec<_>>();
        ensure!(
            !symbols.is_empty() && symbols.len() == self.prepared_proof_identities_by_symbol.len(),
            "frozen prepared symbol scope is empty or invalid"
        );
        Ok(symbols)
    }

    pub(crate) fn for_symbols(&self, symbols: &[String]) -> Result<Self> {
        ensure!(
            !symbols.is_empty()
                && symbols.iter().all(|symbol| self
                    .prepared_proof_identities_by_symbol
                    .contains_key(symbol)),
            "frozen capture does not cover requested symbols capture={}",
            self.capture.display()
        );
        let mut projected = self.clone();
        projected
            .market_lifecycles
            .retain(|market| symbols.contains(&market.symbol));
        projected
            .prepared_proof_identities_by_symbol
            .retain(|symbol, _| symbols.contains(symbol));
        projected
            .initial_oracle_learning_sha256_by_symbol
            .retain(|symbol, _| symbols.contains(symbol));
        projected
            .initial_oracle_learning_cutoffs_ms
            .retain(|symbol, _| symbols.contains(symbol));
        if let Some(population) = &self.recorded_market_population {
            let mut selected = RecordedMarketPopulation::default();
            for symbol in symbols {
                let mut part = population.clone();
                part.retain_symbol(symbol);
                ensure!(
                    !part.records().is_empty(),
                    "frozen market population missing symbol={symbol}"
                );
                selected.merge(part)?;
            }
            projected.recorded_market_population = Some(selected);
        }
        let mut learning: BTreeMap<String, OracleLearningSnapshot> =
            match captured_initial_oracle_learning_bytes(&self.capture)? {
                Some(bytes) => {
                    rmp_serde::from_slice(&bytes).context("decode frozen captured learners")?
                }
                None => BTreeMap::new(),
            };
        learning.retain(|symbol, _| symbols.contains(symbol));
        for symbol in symbols {
            let symbol_learning = learning
                .get(symbol)
                .cloned()
                .map(|state| BTreeMap::from([(symbol.clone(), state)]))
                .unwrap_or_default();
            ensure!(
                self.initial_oracle_learning_sha256_by_symbol.get(symbol)
                    == Some(&candidate_proof::oracle_learning_sha256(&symbol_learning)?),
                "frozen captured learner changed symbol={symbol}"
            );
        }
        projected.initial_oracle_learning_sha256 =
            candidate_proof::oracle_learning_sha256(&learning)?;
        Ok(projected)
    }

    pub(crate) fn validate_frozen_execution_timing(&self, cfg: &AppConfig) -> Result<()> {
        let timing = self.execution_timing.as_ref().with_context(|| {
            format!(
                "frozen execution timing missing capture={}",
                self.capture.display()
            )
        })?;
        let finalized = polymarket_hft::strategy_runtime::backtest_live_latency::validate_frozen_replay_execution_timing(
            &self.capture,
            &cfg.polymarket.execution,
            cfg.polymarket.use_server_time,
            timing,
            false,
        )?;
        ensure!(
            finalized.session_id == self.session_id
                && finalized.run_id == self.run_id
                && finalized.start_ts_ms == self.capture_start_ms
                && finalized.end_ts_ms == self.capture_end_ms,
            "frozen capture temporal identity changed capture={}",
            self.capture.display()
        );
        Ok(())
    }
}
