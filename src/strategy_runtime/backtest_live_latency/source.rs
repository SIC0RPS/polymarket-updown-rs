use std::path::{Path, PathBuf};

use crate::capture::FinalizedCaptureIdentity;
use crate::config::{PolymarketExecutionConfig, PolymarketExecutionMode, PolymarketOrderType};

use super::observations::{
    load_clob_source_v1, load_clob_source_v2, load_clob_source_v5, load_ctf_source,
};
use super::*;

#[derive(Clone, Copy, Eq, PartialEq)]
enum ObservationSemantics {
    HistoricalV1,
    CurrentV2,
    RecordedV4,
    RecordedV5,
}

#[derive(Deserialize)]
struct CaptureManifest {
    start_ts_ms: u64,
    end_ts_ms: u64,
}

type LegacyExecutionConfiguration = (
    Option<PolymarketExecutionMode>,
    PolymarketOrderType,
    u64,
    String,
);

type CurrentExecutionConfiguration = (
    Option<PolymarketExecutionMode>,
    PolymarketOrderType,
    u64,
    String,
    bool,
);

struct CapturedExecutionConfiguration {
    legacy: LegacyExecutionConfiguration,
    use_server_time: Option<bool>,
}

fn legacy_execution_configuration(
    config: &PolymarketExecutionConfig,
) -> LegacyExecutionConfiguration {
    (
        config.mode,
        config.order_type,
        config.reclaim_retry_cooldown_secs,
        config.order_submit_pia_region.clone(),
    )
}

fn current_execution_configuration(
    config: &PolymarketExecutionConfig,
    use_server_time: bool,
) -> CurrentExecutionConfiguration {
    (
        config.mode,
        config.order_type,
        config.reclaim_retry_cooldown_secs,
        config.order_submit_pia_region.clone(),
        use_server_time,
    )
}

fn execution_configuration_sha256<T: Serialize>(config: &T) -> Result<String> {
    Ok(format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(config).context("serialize current execution configuration")?
        )
    ))
}

pub fn select_replay_execution_timing(
    selected_capture: &Path,
    current_execution: &PolymarketExecutionConfig,
    _use_server_time: bool,
) -> Result<ReplayExecutionTimingModel> {
    let selected_identity = crate::capture::finalized_capture_identity(selected_capture)?;
    let selected_config_path = selected_capture.join("config_snapshot.toml");
    let selected_config_sha256 = optional_file_sha256(&selected_config_path)?;
    let current_execution = legacy_execution_configuration(current_execution);
    let current_execution_config_sha256 = execution_configuration_sha256(&current_execution)?;
    let mut clob = load_clob_source_v5(
        selected_capture,
        &selected_identity,
        selected_config_sha256.clone(),
    )?;
    let mut ctf = load_ctf_source(
        selected_capture,
        &selected_identity,
        selected_config_sha256.clone(),
    )?;

    if clob.is_none() || ctf.is_none() {
        for candidate in
            earlier_timing_source_candidates(selected_capture, selected_identity.start_ts_ms)?
        {
            let config_path = candidate.join("config_snapshot.toml");
            let identity = crate::capture::finalized_capture_metadata_identity(&candidate)?;
            let candidate_config_sha256 = optional_file_sha256(&config_path)?;
            let candidate_clob = if clob.is_none() {
                load_clob_source_v5(&candidate, &identity, candidate_config_sha256.clone())?
            } else {
                None
            };
            let candidate_ctf = if ctf.is_none() {
                load_ctf_source(&candidate, &identity, candidate_config_sha256)?
            } else {
                None
            };
            if candidate_clob.is_none() && candidate_ctf.is_none() {
                continue;
            }
            anyhow::ensure!(
                crate::capture::finalized_capture_identity(&candidate)? == identity,
                "execution timing source identity changed during selection"
            );
            clob = clob.or(candidate_clob);
            ctf = ctf.or(candidate_ctf);
            if clob.is_some() && ctf.is_some() {
                break;
            }
        }
    }

    Ok(ReplayExecutionTimingModel {
        schema_version: 1,
        model_version: EXECUTION_TIMING_MODEL_VERSION_V5.to_string(),
        selected_capture: selected_capture.display().to_string(),
        selected_config_sha256,
        current_execution_config_sha256,
        current_clob_protocol: CURRENT_EXECUTION_PROTOCOL.to_string(),
        current_clob_protocol_start_ms: CURRENT_EXECUTION_PROTOCOL_START_MS,
        current_marketable_taker_hold_us: CURRENT_MARKETABLE_TAKER_HOLD_US,
        current_marketable_taker_hold_source: CURRENT_MARKETABLE_TAKER_HOLD_SOURCE.to_string(),
        clob,
        ctf,
    })
}

pub fn validate_frozen_replay_execution_timing(
    selected_capture: &Path,
    current_execution: &PolymarketExecutionConfig,
    use_server_time: bool,
    frozen: &ReplayExecutionTimingModel,
    require_current_source_compatibility: bool,
) -> Result<FinalizedCaptureIdentity> {
    let semantics = match frozen.model_version.as_str() {
        EXECUTION_TIMING_MODEL_VERSION_V1 => ObservationSemantics::HistoricalV1,
        EXECUTION_TIMING_MODEL_VERSION_V2 | EXECUTION_TIMING_MODEL_VERSION_V3 => {
            ObservationSemantics::CurrentV2
        }
        EXECUTION_TIMING_MODEL_VERSION_V4 => ObservationSemantics::RecordedV4,
        EXECUTION_TIMING_MODEL_VERSION_V5 => ObservationSemantics::RecordedV5,
        version => anyhow::bail!("unsupported frozen execution timing model {version}"),
    };
    anyhow::ensure!(
        frozen.schema_version == 1
            && frozen.selected_capture == selected_capture.display().to_string()
            && frozen.current_clob_protocol == CURRENT_EXECUTION_PROTOCOL
            && frozen.current_clob_protocol_start_ms == CURRENT_EXECUTION_PROTOCOL_START_MS
            && frozen.current_marketable_taker_hold_us == CURRENT_MARKETABLE_TAKER_HOLD_US
            && frozen.current_marketable_taker_hold_source == CURRENT_MARKETABLE_TAKER_HOLD_SOURCE,
        "frozen execution timing protocol identity changed"
    );
    let selected_identity = crate::capture::finalized_capture_identity(selected_capture)?;
    let selected_config_path = selected_capture.join("config_snapshot.toml");
    anyhow::ensure!(
        frozen.selected_config_sha256 == optional_file_sha256(&selected_config_path)?,
        "frozen execution timing selected configuration changed"
    );
    let expected_current_sha256 = match semantics {
        ObservationSemantics::HistoricalV1
        | ObservationSemantics::RecordedV4
        | ObservationSemantics::RecordedV5 => {
            execution_configuration_sha256(&legacy_execution_configuration(current_execution))?
        }
        ObservationSemantics::CurrentV2 => execution_configuration_sha256(
            &current_execution_configuration(current_execution, use_server_time),
        )?,
    };
    anyhow::ensure!(
        frozen.current_execution_config_sha256 == expected_current_sha256,
        "frozen execution timing compatibility identity changed"
    );
    let mut validated_clob_source = None;
    if let Some(source) = &frozen.clob {
        let source_capture = Path::new(&source.source_capture);
        let identity = validate_source_identity(
            source_capture,
            &source.source_run_id,
            selected_capture,
            &selected_identity,
            current_execution,
            use_server_time,
            semantics,
            require_current_source_compatibility,
            true,
        )?;
        let current = match semantics {
            ObservationSemantics::HistoricalV1 => load_clob_source_v1(
                source_capture,
                &identity,
                optional_file_sha256(&source_capture.join("config_snapshot.toml"))?,
            )?,
            ObservationSemantics::CurrentV2 | ObservationSemantics::RecordedV4 => {
                load_clob_source_v2(
                    source_capture,
                    &identity,
                    optional_file_sha256(&source_capture.join("config_snapshot.toml"))?,
                )?
            }
            ObservationSemantics::RecordedV5 => load_clob_source_v5(
                source_capture,
                &identity,
                optional_file_sha256(&source_capture.join("config_snapshot.toml"))?,
            )?,
        };
        anyhow::ensure!(
            current.as_ref() == Some(source),
            "frozen CLOB execution timing evidence changed"
        );
        validated_clob_source = Some((source.source_capture.as_str(), identity));
    }
    if let Some(source) = &frozen.ctf {
        let source_capture = Path::new(&source.source_capture);
        let owned_identity;
        let identity = match validated_clob_source.as_ref() {
            Some((capture, identity)) if *capture == source.source_capture.as_str() => {
                anyhow::ensure!(
                    identity.run_id == source.source_run_id,
                    "frozen execution timing source run changed"
                );
                identity
            }
            _ => {
                owned_identity = validate_source_identity(
                    source_capture,
                    &source.source_run_id,
                    selected_capture,
                    &selected_identity,
                    current_execution,
                    use_server_time,
                    semantics,
                    require_current_source_compatibility,
                    matches!(
                        frozen.model_version.as_str(),
                        EXECUTION_TIMING_MODEL_VERSION_V1 | EXECUTION_TIMING_MODEL_VERSION_V2
                    ),
                )?;
                &owned_identity
            }
        };
        let current = load_ctf_source(
            source_capture,
            identity,
            optional_file_sha256(&source_capture.join("config_snapshot.toml"))?,
        )?;
        anyhow::ensure!(
            current.as_ref() == Some(source),
            "frozen CTF execution timing evidence changed"
        );
    }
    Ok(selected_identity)
}

fn validate_source_identity(
    source_capture: &Path,
    source_run_id: &str,
    selected_capture: &Path,
    selected_identity: &FinalizedCaptureIdentity,
    current_execution: &PolymarketExecutionConfig,
    use_server_time: bool,
    semantics: ObservationSemantics,
    require_current_source_compatibility: bool,
    require_clob_compatibility: bool,
) -> Result<FinalizedCaptureIdentity> {
    let identity = if source_capture == selected_capture {
        selected_identity.clone()
    } else {
        let identity = crate::capture::finalized_capture_identity(source_capture)?;
        anyhow::ensure!(
            identity.end_ts_ms < selected_identity.start_ts_ms,
            "frozen execution timing source is not strictly earlier than selected capture"
        );
        identity
    };
    anyhow::ensure!(
        identity.run_id == source_run_id,
        "frozen execution timing source run changed"
    );
    if !require_clob_compatibility
        || matches!(
            semantics,
            ObservationSemantics::RecordedV4 | ObservationSemantics::RecordedV5
        )
    {
        return Ok(identity);
    }
    let captured = optional_execution_config(&source_capture.join("config_snapshot.toml"))?
        .context("frozen execution timing source configuration missing")?;
    let compatible = if semantics == ObservationSemantics::HistoricalV1
        && !require_current_source_compatibility
    {
        captured.legacy == legacy_execution_configuration(current_execution)
    } else {
        current_compatible(
            &captured,
            &current_execution_configuration(current_execution, use_server_time),
        )
    };
    anyhow::ensure!(compatible, "frozen execution timing source is incompatible");
    Ok(identity)
}

fn earlier_timing_source_candidates(
    selected_capture: &Path,
    selected_start_ms: u64,
) -> Result<Vec<PathBuf>> {
    let base = selected_capture
        .parent()
        .context("selected capture has no parent directory")?;
    let mut earlier = Vec::new();
    for entry in
        fs::read_dir(base).with_context(|| format!("read capture directory {}", base.display()))?
    {
        let path = entry?.path();
        if path.as_path() == selected_capture
            || !path.is_dir()
            || path.join("capture_incomplete.json").exists()
            || !path.join("capture_artifact_manifest.json").is_file()
        {
            continue;
        }
        let manifest = read_capture_manifest(&path)?;
        if manifest.end_ts_ms < selected_start_ms {
            earlier.push((manifest.end_ts_ms, manifest.start_ts_ms, path));
        }
    }
    earlier.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| right.1.cmp(&left.1)));
    Ok(earlier.into_iter().map(|(_, _, path)| path).collect())
}

fn read_capture_manifest(capture: &Path) -> Result<CaptureManifest> {
    let path = capture.join("capture_artifact_manifest.json");
    serde_json::from_slice(&fs::read(&path).with_context(|| format!("read {}", path.display()))?)
        .with_context(|| format!("parse {}", path.display()))
}

fn optional_file_sha256(path: &Path) -> Result<Option<String>> {
    if !path.is_file() {
        return Ok(None);
    }
    Ok(Some(format!("{:x}", Sha256::digest(fs::read(path)?))))
}

fn optional_execution_config(path: &Path) -> Result<Option<CapturedExecutionConfiguration>> {
    if !path.is_file() {
        return Ok(None);
    }
    let config: toml::Value = toml::from_str(&fs::read_to_string(path)?)?;
    let polymarket = config.get("polymarket");
    let execution = polymarket
        .and_then(|value| value.get("execution"))
        .cloned()
        .map(toml::Value::try_into::<PolymarketExecutionConfig>)
        .transpose()
        .context("decode captured polymarket execution configuration")?;
    Ok(execution.map(|execution| CapturedExecutionConfiguration {
        legacy: legacy_execution_configuration(&execution),
        use_server_time: polymarket
            .and_then(|value| value.get("use_server_time"))
            .and_then(toml::Value::as_bool),
    }))
}

fn current_compatible(
    captured: &CapturedExecutionConfiguration,
    current: &CurrentExecutionConfiguration,
) -> bool {
    captured.legacy == (current.0, current.1, current.2, current.3.clone())
        && captured.use_server_time == Some(current.4)
}
