use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::config::{AppConfig, LogFormat, MakerRebateMmStrategyConfig};
use crate::observability::{Observability, ObservabilityRuntime};
use crate::strategy::OracleLearningSnapshot;
use crate::strategy_runtime::backtest_live_latency::ReplayExecutionTimingModel;
use crate::telemetry::FloodControlSnapshot;

mod variant;
pub use variant::BacktestOracleResearchVariant;

const RESEARCH_RUNTIME_LOG_MAX_BYTES: u64 = 512 * 1024 * 1024;
const RESEARCH_RUNTIME_LOG_BUFFERED_LINES: usize = 131_072;

#[derive(Debug, Deserialize)]
struct FrozenResearchInputs {
    inputs: BTreeMap<String, FrozenInput>,
    metadata_eligible_captures: Vec<FrozenCapture>,
}

#[derive(Debug, Deserialize)]
struct FrozenInput {
    sha256: String,
    frozen_path: String,
}

#[derive(Debug, Deserialize)]
struct FrozenCapture {
    capture: String,
    run_id: String,
    manifest_sha256: String,
    start_ts_ms: u64,
    end_ts_ms: u64,
    captured_seed_sha256: String,
    prepared: BTreeMap<String, FrozenPreparedSymbol>,
}

#[derive(Debug, Deserialize)]
struct FrozenPreparedSymbol {
    manifest_sha256: String,
    prepared_payload_sha256: String,
    start_ts_ms: u64,
    end_ts_ms: u64,
}

pub(in crate::strategy_runtime) struct PreparedOracleResearchSeed {
    pub(in crate::strategy_runtime) learning: BTreeMap<String, OracleLearningSnapshot>,
    pub(in crate::strategy_runtime) sha256: String,
    pub(in crate::strategy_runtime) source: &'static str,
    pub(in crate::strategy_runtime) transformed_once: bool,
    pub(in crate::strategy_runtime) application_policy: Option<&'static str>,
}

pub(in crate::strategy_runtime) struct OracleResearchRun {
    variant: BacktestOracleResearchVariant,
    output_root: PathBuf,
    capture_name: String,
    capture_run_id: String,
    capture_start_ts_ms: u64,
    capture_end_ts_ms: u64,
    expected_captured_seed_sha256: String,
    previous_capture: Option<(String, String)>,
    frozen_manifest_sha256: String,
    common_input_identity_sha256: String,
    input_sha256: BTreeMap<String, String>,
    input_paths: BTreeMap<String, PathBuf>,
    prepared_payload_sha256: BTreeMap<String, String>,
    symbols: Vec<String>,
    observability_runtime: Option<ObservabilityRuntime>,
    observability: Option<Observability>,
}

impl OracleResearchRun {
    pub(super) fn prepare(
        variant: BacktestOracleResearchVariant,
        base_output_root: &Path,
        capture: &Path,
        symbols: &[String],
    ) -> Result<Self> {
        let repository = Path::new(env!("CARGO_MANIFEST_DIR"));
        let frozen_manifest_path = base_output_root.join("frozen_inputs.json");
        let frozen_manifest_bytes = fs::read(&frozen_manifest_path)
            .with_context(|| format!("read {}", frozen_manifest_path.display()))?;
        let frozen_manifest_sha256 = digest_bytes(&frozen_manifest_bytes);
        let frozen: FrozenResearchInputs = serde_json::from_slice(&frozen_manifest_bytes)
            .with_context(|| format!("decode {}", frozen_manifest_path.display()))?;

        let mut input_sha256 = BTreeMap::new();
        let mut input_paths = BTreeMap::new();
        for (name, input) in &frozen.inputs {
            let recorded_path = Path::new(&input.frozen_path);
            let path = if recorded_path.is_absolute() {
                recorded_path.to_path_buf()
            } else {
                repository.join(recorded_path)
            };
            let bytes =
                fs::read(&path).with_context(|| format!("read frozen input {}", path.display()))?;
            let actual = digest_bytes(&bytes);
            anyhow::ensure!(
                actual == input.sha256,
                "frozen research input content changed name={name}"
            );
            input_sha256.insert(name.clone(), actual);
            input_paths.insert(name.clone(), path);
        }

        for pair in frozen.metadata_eligible_captures.windows(2) {
            anyhow::ensure!(
                pair[0].end_ts_ms < pair[1].start_ts_ms,
                "frozen oracle research captures are not strictly chronological"
            );
        }
        let canonical_capture = fs::canonicalize(capture)
            .with_context(|| format!("resolve research capture {}", capture.display()))?;
        let capture_index = frozen
            .metadata_eligible_captures
            .iter()
            .position(|entry| {
                fs::canonicalize(repository.join(&entry.capture))
                    .is_ok_and(|path| path == canonical_capture)
            })
            .with_context(|| {
                format!(
                    "capture is not in frozen oracle research cohort {}",
                    capture.display()
                )
            })?;
        let frozen_capture = &frozen.metadata_eligible_captures[capture_index];
        let previous_capture = capture_index
            .checked_sub(1)
            .map(|index| -> Result<(String, String)> {
                let previous = &frozen.metadata_eligible_captures[index];
                let name = Path::new(&previous.capture)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .context("preceding frozen research capture has no UTF-8 name")?;
                Ok((name.to_string(), previous.run_id.clone()))
            })
            .transpose()?;

        let capture_manifest_path = capture.join("capture_artifact_manifest.json");
        let capture_manifest = fs::read(&capture_manifest_path)
            .with_context(|| format!("read {}", capture_manifest_path.display()))?;
        anyhow::ensure!(
            digest_bytes(&capture_manifest) == frozen_capture.manifest_sha256,
            "capture artifact manifest differs from frozen research identity"
        );
        anyhow::ensure!(
            frozen_capture.start_ts_ms < frozen_capture.end_ts_ms,
            "frozen research capture window is invalid"
        );

        let requested = symbols
            .iter()
            .map(|symbol| symbol.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        anyhow::ensure!(
            requested.len() == symbols.len()
                && requested.iter().all(|symbol| crate::config::VALID_ASSETS
                    .iter()
                    .any(|expected| expected.eq_ignore_ascii_case(symbol))),
            "oracle research symbol selection is invalid"
        );
        anyhow::ensure!(
            frozen_capture
                .prepared
                .keys()
                .all(|symbol| symbol == &symbol.to_ascii_lowercase()
                    && crate::config::VALID_ASSETS
                        .iter()
                        .any(|expected| expected.eq_ignore_ascii_case(symbol))),
            "frozen research prepared symbol identity is invalid"
        );
        let selected = requested
            .into_iter()
            .filter(|symbol| {
                if frozen_capture.prepared.contains_key(symbol) {
                    true
                } else {
                    let _ = writeln!(
                        std::io::stderr().lock(),
                        "PREPARED_REPLAY symbol_unavailable capture={} symbol={symbol} reason=not_in_frozen_prepared_scope",
                        capture.display()
                    );
                    false
                }
            })
            .collect::<BTreeSet<_>>();
        anyhow::ensure!(
            !selected.is_empty(),
            "research capture unavailable: no qualified requested symbols"
        );
        let mut prepared_payload_sha256 = BTreeMap::new();
        let mut identity = Sha256::new();
        identity.update(frozen_manifest_sha256.as_bytes());
        identity.update(frozen_capture.manifest_sha256.as_bytes());
        for (name, sha256) in &input_sha256 {
            identity.update(name.as_bytes());
            identity.update([0]);
            identity.update(sha256.as_bytes());
            identity.update([0]);
        }
        for symbol in &selected {
            let prepared = frozen_capture
                .prepared
                .get(symbol)
                .with_context(|| format!("frozen research capture omits symbol={symbol}"))?;
            anyhow::ensure!(
                prepared.start_ts_ms < prepared.end_ts_ms
                    && prepared.start_ts_ms >= frozen_capture.start_ts_ms
                    && prepared.end_ts_ms <= frozen_capture.end_ts_ms,
                "frozen prepared window is outside capture symbol={symbol}"
            );
            let manifest_path = capture
                .join("backtest_replay")
                .join("v2")
                .join(symbol)
                .join("manifest.json");
            let bytes = fs::read(&manifest_path)
                .with_context(|| format!("read {}", manifest_path.display()))?;
            anyhow::ensure!(
                digest_bytes(&bytes) == prepared.manifest_sha256,
                "prepared replay manifest differs from frozen identity symbol={symbol}"
            );
            prepared_payload_sha256.insert(
                symbol.to_ascii_uppercase(),
                prepared.prepared_payload_sha256.clone(),
            );
            identity.update(symbol.as_bytes());
            identity.update([0]);
            identity.update(prepared.manifest_sha256.as_bytes());
            identity.update([0]);
            identity.update(prepared.prepared_payload_sha256.as_bytes());
            identity.update([0]);
        }
        let common_input_identity_sha256 = format!("{:x}", identity.finalize());
        let capture_name = capture
            .file_name()
            .and_then(|name| name.to_str())
            .context("research capture has no UTF-8 directory name")?
            .to_string();
        let output_root = base_output_root.join(&capture_name).join(variant.as_str());
        anyhow::ensure!(
            !output_root.exists(),
            "oracle research output already exists {}",
            output_root.display()
        );

        Ok(Self {
            variant,
            output_root,
            capture_name,
            capture_run_id: frozen_capture.run_id.clone(),
            capture_start_ts_ms: frozen_capture.start_ts_ms,
            capture_end_ts_ms: frozen_capture.end_ts_ms,
            expected_captured_seed_sha256: frozen_capture.captured_seed_sha256.clone(),
            previous_capture,
            frozen_manifest_sha256,
            common_input_identity_sha256,
            input_sha256,
            input_paths,
            prepared_payload_sha256,
            symbols: selected.into_iter().collect(),
            observability_runtime: None,
            observability: None,
        })
    }

    pub(in crate::strategy_runtime) fn output_root(&self) -> &Path {
        &self.output_root
    }

    pub(super) fn frozen_execution_timing(&self) -> Result<Option<ReplayExecutionTimingModel>> {
        if self.variant == BacktestOracleResearchVariant::OperationalBaseline {
            return Ok(None);
        }
        let report = self.completed_report(
            &self.capture_name,
            &self.capture_run_id,
            BacktestOracleResearchVariant::OperationalBaseline,
        )?;
        let artifact = report
            .get("oracle_research")
            .context("operational baseline omits oracle research artifact")?;
        anyhow::ensure!(
            artifact
                .get("common_input_identity_sha256")
                .and_then(Value::as_str)
                == Some(self.common_input_identity_sha256.as_str()),
            "operational baseline common input identity mismatch"
        );
        let timing: ReplayExecutionTimingModel = serde_json::from_value(
            report
                .pointer("/replay_assumptions/execution_timing")
                .cloned()
                .context("operational baseline omits execution timing model")?,
        )
        .context("decode operational baseline execution timing model")?;
        anyhow::ensure!(
            artifact
                .get("execution_timing_sha256")
                .and_then(Value::as_str)
                == Some(timing.sha256()?.as_str()),
            "operational baseline execution timing digest mismatch"
        );
        Ok(Some(timing))
    }

    pub(in crate::strategy_runtime) fn variant(&self) -> BacktestOracleResearchVariant {
        self.variant
    }

    pub(super) fn symbols(&self) -> &[String] {
        &self.symbols
    }

    pub(super) fn frozen_config_path(&self) -> &Path {
        self.frozen_input_path("config/main.toml")
    }

    pub(super) fn frozen_optimizer_path(&self) -> &Path {
        self.frozen_input_path("optimization.yml")
    }

    pub(super) fn frozen_symbol_optimizer_path(&self, symbol: &str) -> Result<&Path> {
        let name = format!("{}.optimization.yml", symbol.to_ascii_uppercase());
        self.input_paths
            .get(&name)
            .map(PathBuf::as_path)
            .with_context(|| format!("frozen research inputs omit {name}"))
    }

    fn frozen_input_path(&self, name: &str) -> &Path {
        self.input_paths
            .get(name)
            .unwrap_or_else(|| panic!("verified frozen research inputs omit {name}"))
            .as_path()
    }

    pub(super) async fn start_observability(&mut self, cfg: &AppConfig) -> Result<Observability> {
        anyhow::ensure!(
            self.observability_runtime.is_none() && self.observability.is_none(),
            "oracle research observability already started"
        );
        fs::create_dir_all(&self.output_root)
            .with_context(|| format!("create {}", self.output_root.display()))?;
        let mut observability_cfg = cfg.observability.clone();
        observability_cfg.enabled = false;
        observability_cfg.logging.level = "strategy.oracle_probability=trace".to_string();
        observability_cfg.logging.stdout = false;
        observability_cfg.logging.format = LogFormat::Json;
        observability_cfg.logging.dir = self.output_root.display().to_string();
        observability_cfg.logging.runtime_log_max_bytes = RESEARCH_RUNTIME_LOG_MAX_BYTES;
        observability_cfg.logging.runtime_log_buffered_lines_limit =
            RESEARCH_RUNTIME_LOG_BUFFERED_LINES;
        observability_cfg.logging.flood_control_enabled = false;
        observability_cfg.logging.action_log_enabled = false;
        observability_cfg.logging.log_ticks = false;
        observability_cfg.dashboard.enabled = false;
        let runtime = ObservabilityRuntime::start(&observability_cfg, "oracle_research").await?;
        anyhow::ensure!(
            tracing::enabled!(
                target: "strategy.oracle_probability",
                tracing::Level::TRACE
            ),
            "oracle research TRACE directive is not active"
        );
        let observability = runtime.handle();
        self.observability_runtime = Some(runtime);
        self.observability = Some(observability.clone());
        Ok(observability)
    }

    pub(super) fn trace_start(
        &self,
        seed: &PreparedOracleResearchSeed,
        config: &MakerRebateMmStrategyConfig,
    ) -> Result<()> {
        let strategy_config = serde_json::to_string(config)?;
        tracing::trace!(
            target: "strategy.oracle_probability",
            event = "oracle_research_run_start",
            variant = self.variant.as_str(),
            capture = %self.capture_name,
            capture_run_id = %self.capture_run_id,
            capture_start_ts_ms = self.capture_start_ts_ms,
            capture_end_ts_ms = self.capture_end_ts_ms,
            frozen_manifest_sha256 = %self.frozen_manifest_sha256,
            common_input_identity_sha256 = %self.common_input_identity_sha256,
            seed_sha256 = %seed.sha256,
            seed_source = seed.source,
            seed_transformed_once = seed.transformed_once,
            symbols = %self.symbols.join(","),
            strategy_config = %strategy_config,
        );
        Ok(())
    }

    pub(super) fn trace_end(&self, failed: bool) {
        tracing::trace!(
            target: "strategy.oracle_probability",
            event = "oracle_research_run_end",
            variant = self.variant.as_str(),
            capture = %self.capture_name,
            common_input_identity_sha256 = %self.common_input_identity_sha256,
            failed,
        );
    }

    pub(super) async fn shutdown_observability(&mut self) -> Result<FloodControlSnapshot> {
        let runtime = self
            .observability_runtime
            .take()
            .context("oracle research observability was not started")?;
        let observability = self
            .observability
            .take()
            .context("oracle research observability handle is absent")?;
        runtime.shutdown().await;
        Ok(observability.flood_control_snapshot())
    }

    pub(in crate::strategy_runtime) fn runtime_log_path(&self) -> PathBuf {
        self.output_root.join("runtime.log")
    }

    pub(in crate::strategy_runtime) fn capture_name(&self) -> &str {
        &self.capture_name
    }

    pub(in crate::strategy_runtime) fn capture_run_id(&self) -> &str {
        &self.capture_run_id
    }

    pub(in crate::strategy_runtime) fn capture_window(&self) -> (u64, u64) {
        (self.capture_start_ts_ms, self.capture_end_ts_ms)
    }

    pub(in crate::strategy_runtime) fn frozen_manifest_sha256(&self) -> &str {
        &self.frozen_manifest_sha256
    }

    pub(in crate::strategy_runtime) fn common_input_identity_sha256(&self) -> &str {
        &self.common_input_identity_sha256
    }

    pub(in crate::strategy_runtime) fn input_sha256(&self) -> &BTreeMap<String, String> {
        &self.input_sha256
    }

    pub(in crate::strategy_runtime) fn prepared_payload_sha256(&self) -> &BTreeMap<String, String> {
        &self.prepared_payload_sha256
    }

    pub(in crate::strategy_runtime) const fn runtime_log_capacity_bytes(&self) -> u64 {
        RESEARCH_RUNTIME_LOG_MAX_BYTES
    }

    fn verify_previous_seed(
        &self,
        source_sha256: &str,
    ) -> Result<BTreeMap<String, OracleLearningSnapshot>> {
        let (capture, run_id) = self
            .previous_capture
            .as_ref()
            .context("later research capture has no frozen predecessor")?;
        let report = self.completed_report(capture, run_id, self.variant)?;
        let artifact = report
            .get("oracle_research")
            .context("preceding report omits oracle research artifact")?;
        anyhow::ensure!(
            artifact.get("seed_final_sha256").and_then(Value::as_str) == Some(source_sha256),
            "preceding report final seed identity mismatch"
        );
        anyhow::ensure!(
            report
                .get("oracle_learning_final_sha256")
                .and_then(Value::as_str)
                == Some(source_sha256),
            "preceding report learner export identity mismatch"
        );
        let seed_path = self
            .research_root()?
            .join(capture)
            .join(self.variant.as_str())
            .join("oracle_learning_final.rmp");
        let seed = fs::read(&seed_path)
            .with_context(|| format!("read preceding seed {}", seed_path.display()))?;
        anyhow::ensure!(
            digest_bytes(&seed) == source_sha256,
            "preceding research seed content changed"
        );
        rmp_serde::from_slice(&seed).context("decode preceding research seed")
    }

    pub(in crate::strategy_runtime) fn completed_report(
        &self,
        capture: &str,
        run_id: &str,
        variant: BacktestOracleResearchVariant,
    ) -> Result<Value> {
        let path = self
            .research_root()?
            .join(capture)
            .join(variant.as_str())
            .join("backtest_strategy_report.json");
        let report: Value = serde_json::from_slice(
            &fs::read(&path).with_context(|| format!("read {}", path.display()))?,
        )
        .with_context(|| format!("decode {}", path.display()))?;
        let artifact = report
            .get("oracle_research")
            .context("completed report omits oracle research artifact")?;
        anyhow::ensure!(
            artifact.get("variant").and_then(Value::as_str) == Some(variant.as_str()),
            "completed research variant mismatch"
        );
        anyhow::ensure!(
            match variant.application_policy() {
                Some(policy) => {
                    artifact.get("application_policy").and_then(Value::as_str) == Some(policy)
                }
                None => artifact.get("application_policy").is_none(),
            },
            "completed research application policy mismatch"
        );
        anyhow::ensure!(
            artifact.get("capture").and_then(Value::as_str) == Some(capture),
            "completed research capture mismatch"
        );
        anyhow::ensure!(
            artifact.get("capture_run_id").and_then(Value::as_str) == Some(run_id),
            "completed research run identity mismatch"
        );
        anyhow::ensure!(
            artifact
                .get("frozen_manifest_sha256")
                .and_then(Value::as_str)
                == Some(self.frozen_manifest_sha256.as_str()),
            "completed research frozen manifest mismatch"
        );
        Ok(report)
    }

    fn research_root(&self) -> Result<&Path> {
        self.output_root
            .parent()
            .and_then(Path::parent)
            .context("oracle research output has no research root")
    }
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
