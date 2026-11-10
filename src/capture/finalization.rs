use std::fs::{self, File};
use std::io::ErrorKind;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::capture::arrow::POLYMARKET_WS_INGRESS_ARROW_FILE;
use crate::capture::events::SessionMeta;
use crate::event_ledger::{
    append_event, make_event_id, now_ms, CanonicalEventRecord, CanonicalEventSource,
    CanonicalEventType,
};

use crate::trade_session_ledger::{
    load_session_receipts_from_path, trade_session_ledger_path_for_run,
    CAPTURED_SESSION_LEDGER_FILE,
};

use super::session::{CaptureWriterConfig, CAPTURED_ORACLE_LEARNING_INITIAL_FILE};
use super::short_retention::{capture_completeness_failure, delete_short_capture};

mod journals;
pub(crate) use journals::{CaptureJournalFile, CaptureJournalManifestEntry};

#[derive(Debug, Deserialize)]
pub(crate) struct CaptureSessionIdentity {
    session_id: String,
    run_id: String,
    start_ts_ms: u64,
    end_ts_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct CaptureArtifactManifestIdentity {
    schema_version: String,
    session_id: String,
    run_id: String,
    start_ts_ms: u64,
    end_ts_ms: u64,
    #[serde(default)]
    strategy_consumption_attempted: Option<u64>,
    #[serde(default)]
    strategy_consumption_written: Option<u64>,
    #[serde(default)]
    strategy_consumption_lost: Option<u64>,
    #[serde(default)]
    strategy_consumption_terminal_sequence: Option<u64>,
    #[serde(default)]
    strategy_consumption_last_written_sequence: Option<u64>,
    #[serde(default)]
    strategy_consumption_contiguous: Option<bool>,
    #[serde(default)]
    journals: Option<Vec<CaptureJournalManifestEntry>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinalizedCaptureIdentity {
    pub schema_version: String,
    pub session_id: String,
    pub run_id: String,
    pub start_ts_ms: u64,
    pub end_ts_ms: u64,
    pub strategy_consumption_attempted: Option<u64>,
    pub strategy_consumption_written: Option<u64>,
    pub strategy_consumption_lost: Option<u64>,
    pub strategy_consumption_terminal_sequence: Option<u64>,
    pub strategy_consumption_last_written_sequence: Option<u64>,
    pub strategy_consumption_contiguous: Option<bool>,
    pub(crate) journals: Vec<CaptureJournalManifestEntry>,
}

pub fn finalized_capture_identity(session_dir: &Path) -> Result<FinalizedCaptureIdentity> {
    let identity = finalized_capture_metadata_identity(session_dir)?;
    if identity.schema_version != "capture_artifact_manifest.v1" {
        let (failures, _, inspection_error) =
            journals::validate_journals(session_dir, &identity.journals, &identity.schema_version);
        if let Some(error) = inspection_error {
            return Err(error);
        }
        anyhow::ensure!(
            failures.is_empty(),
            "finalized capture journals are invalid: {}",
            failures.join("; ")
        );
    }
    Ok(identity)
}

/// Reads finalized capture metadata without validating journal payloads.
/// Use [`finalized_capture_identity`] to fully verify a selected capture.
pub fn finalized_capture_metadata_identity(session_dir: &Path) -> Result<FinalizedCaptureIdentity> {
    let marker_path = session_dir.join("capture_incomplete.json");
    match fs::metadata(&marker_path) {
        Ok(metadata) => anyhow::ensure!(
            !metadata.is_file(),
            "capture is marked incomplete: {}",
            session_dir.display()
        ),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect capture marker {}", marker_path.display()));
        }
    }
    let meta = read_capture_session_identity(session_dir)?;
    let manifest_path = session_dir.join("capture_artifact_manifest.json");
    let manifest_file = File::open(&manifest_path).with_context(|| {
        format!(
            "failed to open finalized capture {}",
            manifest_path.display()
        )
    })?;
    let manifest: CaptureArtifactManifestIdentity = serde_json::from_reader(manifest_file)
        .with_context(|| {
            format!(
                "failed to parse finalized capture {}",
                manifest_path.display()
            )
        })?;
    let legacy_schema = manifest.schema_version == "capture_artifact_manifest.v1";
    let journal_schema_v2 = manifest.schema_version == "capture_artifact_manifest.v2";
    let journal_schema_v3 = manifest.schema_version == "capture_artifact_manifest.v3";
    let journal_schema = journal_schema_v2 || journal_schema_v3;
    anyhow::ensure!(
        (legacy_schema || journal_schema)
            && (!journal_schema
                || manifest
                    .journals
                    .as_ref()
                    .is_some_and(|journals| !journals.is_empty()))
            && !manifest.session_id.trim().is_empty()
            && manifest.session_id == manifest.session_id.trim()
            && !manifest.run_id.trim().is_empty()
            && manifest.run_id == manifest.run_id.trim()
            && manifest.start_ts_ms > 0
            && manifest.end_ts_ms >= manifest.start_ts_ms,
        "finalized capture manifest identity is incomplete"
    );
    anyhow::ensure!(
        meta.session_id == manifest.session_id
            && meta.run_id == manifest.run_id
            && meta.start_ts_ms == manifest.start_ts_ms
            && meta.end_ts_ms == Some(manifest.end_ts_ms),
        "session metadata differs from finalized capture manifest session={}",
        session_dir.display()
    );
    Ok(FinalizedCaptureIdentity {
        schema_version: manifest.schema_version,
        session_id: manifest.session_id,
        run_id: manifest.run_id,
        start_ts_ms: manifest.start_ts_ms,
        end_ts_ms: manifest.end_ts_ms,
        strategy_consumption_attempted: manifest.strategy_consumption_attempted,
        strategy_consumption_written: manifest.strategy_consumption_written,
        strategy_consumption_lost: manifest.strategy_consumption_lost,
        strategy_consumption_terminal_sequence: manifest.strategy_consumption_terminal_sequence,
        strategy_consumption_last_written_sequence: manifest
            .strategy_consumption_last_written_sequence,
        strategy_consumption_contiguous: manifest.strategy_consumption_contiguous,
        journals: manifest.journals.unwrap_or_default(),
    })
}

pub(crate) fn read_capture_session_identity(session_dir: &Path) -> Result<CaptureSessionIdentity> {
    let meta_path = session_dir.join("session_meta.json");
    let meta_file = File::open(&meta_path)
        .with_context(|| format!("failed to open capture {}", meta_path.display()))?;
    let meta: CaptureSessionIdentity = serde_json::from_reader(meta_file)
        .with_context(|| format!("failed to parse capture {}", meta_path.display()))?;
    anyhow::ensure!(
        !meta.session_id.trim().is_empty()
            && meta.session_id == meta.session_id.trim()
            && !meta.run_id.trim().is_empty()
            && meta.run_id == meta.run_id.trim()
            && meta.start_ts_ms > 0
            && meta
                .end_ts_ms
                .is_none_or(|end_ts_ms| end_ts_ms >= meta.start_ts_ms),
        "capture session metadata identity is incomplete"
    );
    Ok(meta)
}

pub(super) async fn write_capture_writer_failure_marker(
    cfg: &CaptureWriterConfig,
    error: &anyhow::Error,
) -> Result<()> {
    let marker_path = cfg.session_dir.join("capture_incomplete.json");
    match tokio::fs::symlink_metadata(&marker_path).await {
        Ok(_) => return Ok(()),
        Err(marker_error) if marker_error.kind() == ErrorKind::NotFound => {}
        Err(marker_error) => {
            return Err(marker_error)
                .with_context(|| format!("inspect capture marker {}", marker_path.display()));
        }
    }
    let failed_ts_ms = now_ms();
    let marker = json!({
        "schema_version": "capture_incomplete.v1",
        "session_id": cfg.session_id,
        "run_id": cfg.run_id,
        "strategy_kind": cfg.strategy_kind,
        "symbols": cfg.symbols,
        "replay_required_underlyings": cfg.replay_required_underlyings,
        "config_hash": cfg.config_hash,
        "duration_ms": failed_ts_ms.saturating_sub(cfg.start_ms),
        "min_save_duration_ms": cfg.min_save_duration_ms,
        "max_duration_ms": cfg.max_duration_ms,
        "exit_reason": "writer_error",
        "journals": cfg.journal_entries,
        "reason": format!("{error:#}"),
    });
    write_capture_incomplete_marker(&cfg.session_dir, &marker).await
}

pub(super) async fn finalize_capture_writer(
    cfg: &CaptureWriterConfig,
    exit_reason: &str,
    strategy_consumption_attempted: u64,
    strategy_consumption_written: u64,
    strategy_consumption_lost: u64,
    polymarket_resolution_lost: bool,
    strategy_consumption_terminal_sequence: u64,
    strategy_consumption_last_written_sequence: u64,
    strategy_consumption_contiguous: bool,
    order_submit_latency_rows: u64,
    polymarket_ws_ingress_rows: u64,
    polymarket_ws_ingress_accepted: u64,
    polymarket_ws_ingress_lost: u64,
    polymarket_ws_ingress_validation_failures: u64,
    polymarket_ws_ingress_writer_error: Option<String>,
) -> Result<()> {
    let finalized_ts_ms = now_ms();
    let duration_ms = finalized_ts_ms.saturating_sub(cfg.start_ms);
    let market_ids = Vec::new();

    let raw_incomplete = polymarket_ws_ingress_writer_error.is_some()
        || polymarket_ws_ingress_lost > 0
        || polymarket_ws_ingress_validation_failures > 0
        || polymarket_ws_ingress_accepted != polymarket_ws_ingress_rows;
    let required_incomplete = polymarket_resolution_lost
        || strategy_consumption_written == 0
        || strategy_consumption_attempted != strategy_consumption_written
        || strategy_consumption_lost != 0
        || !strategy_consumption_contiguous
        || strategy_consumption_terminal_sequence != strategy_consumption_last_written_sequence;
    let mut failures = Vec::new();
    if raw_incomplete {
        failures.push("raw_polymarket_ws_ingress_incomplete".to_string());
    }
    if required_incomplete {
        failures.push("required_capture_incomplete".to_string());
    }
    let journals = &cfg.journal_entries;
    let (journal_failures, replayable_rows_by_underlying, _) =
        journals::validate_journals(&cfg.session_dir, journals, "capture_artifact_manifest.v3");
    failures.extend(journal_failures);
    let completeness_failure = capture_completeness_failure(
        &cfg.symbols,
        &cfg.replay_required_underlyings,
        cfg.max_duration_ms,
        cfg.min_save_duration_ms,
        exit_reason,
        duration_ms,
        &replayable_rows_by_underlying,
    );
    if let Some(reason) = completeness_failure.as_deref() {
        let short_capture = failures.is_empty()
            && duration_ms < cfg.min_save_duration_ms
            && order_submit_latency_rows == 0;
        if short_capture {
            delete_short_capture(
                &cfg.session_dir,
                &cfg.session_id,
                &cfg.strategy_kind,
                &cfg.symbols,
                &cfg.config_hash,
                finalized_ts_ms,
                duration_ms,
                cfg.min_save_duration_ms,
                order_submit_latency_rows,
                &reason,
            )
            .await;
            return Ok(());
        }
        failures.push(reason.to_string());
    }
    if !failures.is_empty() {
        let reason = failures.join("; ");
        let incomplete_marker = json!({
            "schema_version": "capture_incomplete.v1",
            "session_id": cfg.session_id,
            "run_id": cfg.run_id,
            "strategy_kind": cfg.strategy_kind,
            "symbols": cfg.symbols,
            "replay_required_underlyings": cfg.replay_required_underlyings,
            "replayable_rows_by_underlying": replayable_rows_by_underlying,
            "config_hash": cfg.config_hash,
            "duration_ms": duration_ms,
            "min_save_duration_ms": cfg.min_save_duration_ms,
            "max_duration_ms": cfg.max_duration_ms,
            "exit_reason": exit_reason,
            "order_submit_latency_rows": order_submit_latency_rows,
            "polymarket_ws_ingress_rows": polymarket_ws_ingress_rows,
            "polymarket_ws_ingress_accepted": polymarket_ws_ingress_accepted,
            "polymarket_ws_ingress_lost": polymarket_ws_ingress_lost,
            "polymarket_ws_ingress_validation_failures": polymarket_ws_ingress_validation_failures,
            "polymarket_ws_ingress_writer_error": polymarket_ws_ingress_writer_error,
            "polymarket_resolution_lost": polymarket_resolution_lost,
            "strategy_consumption_attempted": strategy_consumption_attempted,
            "strategy_consumption_written": strategy_consumption_written,
            "strategy_consumption_lost": strategy_consumption_lost,
            "strategy_consumption_terminal_sequence": strategy_consumption_terminal_sequence,
            "strategy_consumption_last_written_sequence": strategy_consumption_last_written_sequence,
            "strategy_consumption_contiguous": strategy_consumption_contiguous,
            "journals": journals,
            "failures": failures,
            "reason": reason,
        });
        write_capture_incomplete_marker(&cfg.session_dir, &incomplete_marker).await?;
        if completeness_failure.is_some() {
            let _ = append_event(&CanonicalEventRecord {
                event_id: make_event_id(
                    CanonicalEventType::Actuation,
                    finalized_ts_ms,
                    cfg.session_id.clone(),
                ),
                run_id: cfg.run_id.clone(),
                ts_ms: finalized_ts_ms,
                event_type: CanonicalEventType::Actuation,
                source: CanonicalEventSource::System,
                subject: "capture".to_string(),
                refs: json!({
                    "session_id": cfg.session_id,
                    "config_hash": cfg.config_hash,
                }),
                metrics: json!({
                    "duration_ms": duration_ms,
                    "order_submit_latency_rows": order_submit_latency_rows,
                }),
                context: json!({
                    "action": "capture_discarded_incomplete",
                    "strategy_kind": cfg.strategy_kind,
                    "symbols": cfg.symbols,
                    "reason": reason,
                }),
            });
        }
        tracing::warn!(
            session_id = cfg.session_id,
            reason,
            path = %cfg.session_dir.display(),
            "capture preserved incomplete for forensic inspection"
        );
        anyhow::bail!("capture incomplete: {reason}");
    }

    let config_timeline = crate::capture::config_timeline::write_capture_config_timeline(
        crate::capture::config_timeline::CaptureConfigTimelineInput {
            session_dir: cfg.session_dir.as_path(),
            session_id: cfg.session_id.as_str(),
            start_ts_ms: cfg.start_ms,
            end_ts_ms: finalized_ts_ms,
            startup_config_hash: cfg.config_hash.as_str(),
        },
    )
    .await
    .context("failed to write capture config timeline")?;
    let meta = SessionMeta {
        session_id: cfg.session_id.clone(),
        run_id: cfg.run_id.clone(),
        start_ts_ms: cfg.start_ms,
        end_ts_ms: Some(finalized_ts_ms),
        pid: std::process::id(),
        execution_mode: "live".to_string(),
        strategy_kind: cfg.strategy_kind.clone(),
        config_hash: cfg.config_hash.clone(),
        optimizer_config_hash: Some(cfg.optimizer_config_hash.clone()),
        final_config_hash: Some(config_timeline.final_config_hash),
        config_timeline_hash: Some(config_timeline.timeline_hash),
        config_change_count: Some(config_timeline.change_count),
        symbols: cfg.symbols.clone(),
        market_ids,
        exit_reason: Some(exit_reason.to_string()),
    };
    let meta_json = serde_json::to_string_pretty(&meta).context("serialize final session meta")?;
    tokio::fs::write(cfg.session_dir.join("session_meta.json"), &meta_json)
        .await
        .context("failed to write final session_meta.json")?;
    write_capture_finalization_artifacts(
        &cfg.session_dir,
        &cfg.session_id,
        &cfg.run_id,
        cfg.start_ms,
        finalized_ts_ms,
        polymarket_ws_ingress_rows,
        strategy_consumption_attempted,
        strategy_consumption_written,
        strategy_consumption_lost,
        strategy_consumption_terminal_sequence,
        strategy_consumption_last_written_sequence,
        strategy_consumption_contiguous,
        journals,
    )
    .await
    .context("failed to write capture finalization artifacts")?;
    let _ = append_event(&CanonicalEventRecord {
        event_id: make_event_id(
            CanonicalEventType::Actuation,
            finalized_ts_ms,
            cfg.session_id.clone(),
        ),
        run_id: cfg.run_id.clone(),
        ts_ms: finalized_ts_ms,
        event_type: CanonicalEventType::Actuation,
        source: CanonicalEventSource::System,
        subject: "capture".to_string(),
        refs: json!({
            "session_id": cfg.session_id,
            "config_hash": cfg.config_hash,
        }),
        metrics: json!({
            "duration_ms": duration_ms,
            "order_submit_latency_rows": order_submit_latency_rows,
            "polymarket_ws_ingress_rows": polymarket_ws_ingress_rows,
        }),
        context: json!({
            "action": "capture_finalized",
            "strategy_kind": cfg.strategy_kind,
            "symbols": cfg.symbols,
        }),
    });

    tracing::info!(session_id = cfg.session_id, "capture session finalized");
    Ok(())
}

async fn write_capture_incomplete_marker(session_dir: &Path, marker: &Value) -> Result<()> {
    let marker_body =
        serde_json::to_string_pretty(marker).context("serialize incomplete capture marker")?;
    tokio::fs::write(session_dir.join("capture_incomplete.json"), marker_body)
        .await
        .context("failed to write incomplete capture marker")
}

pub(super) async fn write_capture_finalization_artifacts(
    session_dir: &Path,
    session_id: &str,
    run_id: &str,
    start_ts_ms: u64,
    end_ts_ms: u64,
    polymarket_ws_ingress_rows: u64,
    strategy_consumption_attempted: u64,
    strategy_consumption_written: u64,
    strategy_consumption_lost: u64,
    strategy_consumption_terminal_sequence: u64,
    strategy_consumption_last_written_sequence: u64,
    strategy_consumption_contiguous: bool,
    journals: &[CaptureJournalManifestEntry],
) -> Result<()> {
    retain_canonical_session_ledger(session_dir, run_id)
        .await
        .context("failed to retain canonical session ledger")?;
    write_capture_artifact_manifest(
        session_dir,
        session_id,
        run_id,
        start_ts_ms,
        end_ts_ms,
        polymarket_ws_ingress_rows,
        strategy_consumption_attempted,
        strategy_consumption_written,
        strategy_consumption_lost,
        strategy_consumption_terminal_sequence,
        strategy_consumption_last_written_sequence,
        strategy_consumption_contiguous,
        journals,
    )
    .await
    .context("failed to write capture_artifact_manifest.json")
}

async fn retain_canonical_session_ledger(session_dir: &Path, run_id: &str) -> Result<()> {
    let source = trade_session_ledger_path_for_run(run_id);
    let body = tokio::fs::read(&source)
        .await
        .with_context(|| format!("read {}", source.display()))?;
    let retained = session_dir.join(CAPTURED_SESSION_LEDGER_FILE);
    tokio::fs::write(&retained, body)
        .await
        .with_context(|| format!("write retained session ledger run_id={run_id}"))?;
    load_session_receipts_from_path(&retained, run_id)?;
    Ok(())
}

async fn write_capture_artifact_manifest(
    session_dir: &Path,
    session_id: &str,
    run_id: &str,
    start_ts_ms: u64,
    end_ts_ms: u64,
    polymarket_ws_ingress_rows: u64,
    strategy_consumption_attempted: u64,
    strategy_consumption_written: u64,
    strategy_consumption_lost: u64,
    strategy_consumption_terminal_sequence: u64,
    strategy_consumption_last_written_sequence: u64,
    strategy_consumption_contiguous: bool,
    journals: &[CaptureJournalManifestEntry],
) -> Result<()> {
    let artifact_files = [
        "order_submit_latency.jsonl",
        "order_submit_reject.jsonl",
        "polymarket_user_trade.jsonl",
        "polymarket_user_order.jsonl",
        "ctf_inventory_operation_latency.jsonl",
        CAPTURED_SESSION_LEDGER_FILE,
        "replay_seed.json",
        "replay_risk_snapshot.jsonl",
        "polymarket_book_admission.jsonl",
        "polymarket_discovery_snapshot.jsonl",
        "control_timeline.jsonl",
        CAPTURED_ORACLE_LEARNING_INITIAL_FILE,
        "session_meta.json",
        POLYMARKET_WS_INGRESS_ARROW_FILE,
        "polymarket_resolution_btc.arrow",
        "polymarket_resolution_eth.arrow",
        "polymarket_resolution_sol.arrow",
        "polymarket_resolution_xrp.arrow",
    ];
    let mut artifacts = Vec::with_capacity(artifact_files.len());
    for file_name in artifact_files {
        let path = session_dir.join(file_name);
        let exists = path.is_file();
        let rows = if file_name == POLYMARKET_WS_INGRESS_ARROW_FILE {
            Some(polymarket_ws_ingress_rows)
        } else if exists && file_name.ends_with(".jsonl") {
            Some(count_file_lines(&path).await?)
        } else {
            None
        };
        let mut artifact = json!({
            "file": file_name,
            "present": exists,
            "rows": rows,
        });
        if file_name == CAPTURED_ORACLE_LEARNING_INITIAL_FILE && exists {
            let bytes = tokio::fs::read(&path)
                .await
                .with_context(|| format!("read {}", path.display()))?;
            artifact["sha256"] = json!(format!("{:x}", Sha256::digest(&bytes)));
        }
        artifacts.push(artifact);
    }
    let body = json!({
        "schema_version": "capture_artifact_manifest.v3",
        "session_id": session_id,
        "run_id": run_id,
        "start_ts_ms": start_ts_ms,
        "end_ts_ms": end_ts_ms,
        "strategy_consumption_attempted": strategy_consumption_attempted,
        "strategy_consumption_written": strategy_consumption_written,
        "strategy_consumption_lost": strategy_consumption_lost,
        "strategy_consumption_terminal_sequence": strategy_consumption_terminal_sequence,
        "strategy_consumption_last_written_sequence": strategy_consumption_last_written_sequence,
        "strategy_consumption_contiguous": strategy_consumption_contiguous,
        "journals": journals,
        "artifacts": artifacts,
        "notes": [
            "polymarket_user_order.jsonl is written only from vendor user order WS events",
            "polymarket_user_trade.jsonl is written only from vendor user trade WS events",
            "ctf_inventory_operation_latency.jsonl is written only from observed materialized CTF inventory operations",
            "control_timeline.jsonl is written only from capture-owned runtime control actuation events",
            "polymarket_resolution_<symbol>.arrow is written only from validated Polymarket CLOB market responses",
            "polymarket_market_ws_ingress.arrow preserves decoded unparsed Polymarket market WebSocket Message values before heartbeat, parser, integrity, state, and projection handling",
            "canonical_session_ledger.jsonl is the immutable capture-local canonical receipt and trade source"
        ],
    });
    let body =
        serde_json::to_string_pretty(&body).context("serialize capture artifact manifest")?;
    tokio::fs::write(session_dir.join("capture_artifact_manifest.json"), body)
        .await
        .context("write capture_artifact_manifest.json")
}

async fn count_file_lines(path: &Path) -> Result<u64> {
    let body = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("read {}", path.display()))?;
    Ok(body.lines().filter(|line| !line.trim().is_empty()).count() as u64)
}
