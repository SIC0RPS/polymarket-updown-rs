use super::*;
use std::time::Instant;

const OPTIMIZER_ARCHIVE_ROOT: &str = "data/backtest_reports/optimizer_archive";

#[derive(Default)]
struct AdmissionCounts {
    winners: usize,
    trials: usize,
    invalid: usize,
    defaults: usize,
    duplicates: usize,
}

pub(super) fn evaluate_archived_contenders(ctx: &RunContext) -> Result<usize> {
    let archive_root = Path::new(OPTIMIZER_ARCHIVE_ROOT);
    let started = Instant::now();
    let current_schema_sha256 = ctx
        .parameter_schemas
        .iter()
        .map(|(symbol, schema)| (symbol.clone(), child_artifact::hex_bytes(&schema.sha256())))
        .collect::<BTreeMap<_, _>>();
    let current_default = default_params_by_symbol(ctx)?;
    let current_default_id = candidate_id_for_params(ctx, &current_default)?;
    let mut counts = AdmissionCounts::default();
    let mut summaries = archive_summary_paths(archive_root, &mut counts);
    summaries.sort();
    let mut current_archives = Vec::new();
    for path in summaries {
        let bytes = match fs::read(&path)
            .with_context(|| format!("read optimizer archive {}", path.display()))
        {
            Ok(bytes) => bytes,
            Err(error) => {
                reject(&mut counts, &path, "unreadable_summary", &error.to_string());
                continue;
            }
        };
        let schema_version = serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|value| value["schema_version"].as_u64());
        match schema_version {
            Some(19) => match serde_json::from_slice(&bytes)
                .with_context(|| format!("parse optimizer archive {}", path.display()))
            {
                Ok(summary) => current_archives.push((path, summary)),
                Err(error) => reject(&mut counts, &path, "unreadable_summary", &error.to_string()),
            },
            Some(17) => reject(
                &mut counts,
                &path,
                "unverifiable_optimizer_binary",
                "schema 17 has no verifiable original child executable identity",
            ),
            _ => reject(
                &mut counts,
                &path,
                "unsupported_schema",
                "expected sequential schema 19",
            ),
        }
    }

    let mut contenders =
        BTreeMap::<String, (BTreeMap<String, BTreeMap<String, ParamValue>>, String)>::new();
    admit_current_archives(
        ctx,
        &current_archives,
        &current_schema_sha256,
        &current_default,
        &current_default_id,
        &mut contenders,
        &mut counts,
    )?;
    let requests = contenders
        .into_values()
        .map(|(params_by_symbol, origin_id)| CandidateRequest {
            params_by_symbol,
            origin: TrialOrigin::ArchivedContender,
            origin_id: Some(origin_id),
        })
        .collect::<Vec<_>>();
    let loaded = requests.len();
    crate::summary::publish_progress(format_args!(
        "CMAES_AUTO_OPTIMIZE archive_evaluation_begin symbols={} admitted_candidates={} completed_candidates=0 remaining_candidates={} captures={} selection_mode={}",
        ctx.symbols.join(","),
        loaded,
        loaded,
        ctx.selection_inputs.len(),
        ctx.selection_mode.as_str()
    ));
    let evaluation_started = Instant::now();
    let mut request_iter = requests.into_iter();
    let mut completed = 0usize;
    loop {
        let group = request_iter
            .by_ref()
            .take(ctx.candidate_slots)
            .collect::<Vec<_>>();
        if group.is_empty() {
            break;
        }
        let group_candidates = group.len();
        let group_started = Instant::now();
        evaluate::evaluate_params_batch(ctx, group, false)?;
        completed += group_candidates;
        summary::write_optimizer_status(ctx, "archive")?;
        crate::summary::publish_progress(format_args!(
            "CMAES_AUTO_OPTIMIZE archive_evaluation_group_done symbols={} admitted_candidates={} completed_candidates={} remaining_candidates={} completed_group_elapsed_ms={}",
            ctx.symbols.join(","),
            loaded,
            completed,
            loaded - completed,
            group_started.elapsed().as_millis()
        ));
    }
    crate::summary::publish_progress(format_args!(
        "CMAES_AUTO_OPTIMIZE archive_evaluation_done symbols={} loaded={} winners={} trials={} invalid={} defaults={} duplicates={} capture_replays={} replay_elapsed_ms={} archive_phase_elapsed_ms={}",
        ctx.symbols.join(","),
        loaded,
        counts.winners,
        counts.trials,
        counts.invalid,
        counts.defaults,
        counts.duplicates,
        loaded * ctx.selection_inputs.len(),
        evaluation_started.elapsed().as_millis(),
        started.elapsed().as_millis()
    ));
    Ok(loaded)
}

#[allow(clippy::too_many_arguments)]
fn admit_current_archives(
    ctx: &RunContext,
    archives: &[(
        PathBuf,
        polymarket_hft::optimizer_report::OptimizerSummaryArtifact,
    )],
    current_schema_sha256: &BTreeMap<String, String>,
    current_default: &BTreeMap<String, BTreeMap<String, ParamValue>>,
    current_default_id: &str,
    contenders: &mut BTreeMap<String, (BTreeMap<String, BTreeMap<String, ParamValue>>, String)>,
    counts: &mut AdmissionCounts,
) -> Result<()> {
    for (path, summary) in archives {
        if summary.status != "complete" {
            continue;
        }
        let run_root = path
            .parent()
            .context("optimizer archive run root missing")?;
        let artifact_path = run_root.join("candidate_result.json");
        let artifact = match fs::read(&artifact_path)
            .with_context(|| format!("read {}", artifact_path.display()))
            .and_then(|bytes| {
                serde_json::from_slice::<candidate_proof::CandidateResultArtifact>(&bytes)
                    .with_context(|| format!("parse {}", artifact_path.display()))
            }) {
            Ok(artifact) => artifact,
            Err(error) => {
                reject(
                    counts,
                    &artifact_path,
                    "unreadable_candidate_proof",
                    &error.to_string(),
                );
                continue;
            }
        };
        if let Err(error) = artifact.validate() {
            reject(
                counts,
                &artifact_path,
                "invalid_candidate_proof",
                &error.to_string(),
            );
            continue;
        }
        if !current_archive_source_matches(path, summary, &artifact) {
            reject(
                counts,
                path,
                "run_identity",
                "archive summary and candidate proof identity mismatch",
            );
            continue;
        }
        if let Err(error) = validate_archived_summary(summary, &artifact) {
            reject(counts, path, "invalid_original_summary", &error.to_string());
            continue;
        }
        for symbol in &ctx.symbols {
            let schema_sha256 = current_schema_sha256
                .get(symbol)
                .with_context(|| format!("current schema hash missing symbol={symbol}"))?;
            let (candidates, recorded_binary) =
                match child_artifact::validate::load_archived_symbol_candidates(
                    run_root,
                    &artifact,
                    symbol,
                    schema_sha256,
                ) {
                    Ok(candidates) => candidates,
                    Err(error) => {
                        reject(
                            counts,
                            run_root,
                            "invalid_symbol_search_archive",
                            &error.to_string(),
                        );
                        continue;
                    }
                };
            let expected_binary = ctx.selection_inputs[0]
                .prepared_proof_identities_by_symbol
                .get(symbol)
                .map(|identity| identity.optimizer_binary_sha256())
                .filter(|identity| !identity.is_empty())
                .with_context(|| {
                    format!("current optimizer binary identity missing symbol={symbol}")
                })?;
            if recorded_binary.as_deref() != Some(expected_binary) {
                reject(
                    counts,
                    &run_root
                        .join("children")
                        .join(symbol)
                        .join("child_result.json"),
                    "optimizer_binary_identity",
                    &format!(
                        "symbol={symbol} expected={expected_binary} recorded={recorded_binary:?}"
                    ),
                );
                continue;
            }
            for candidate in candidates {
                if admit_map(
                    ctx,
                    contenders,
                    current_default,
                    current_default_id,
                    symbol,
                    candidate.params,
                    candidate.origin_id,
                    run_root,
                    counts,
                )? {
                    if candidate.search_trial {
                        counts.trials += 1;
                    } else {
                        counts.winners += 1;
                    }
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn admit_map(
    ctx: &RunContext,
    contenders: &mut BTreeMap<String, (BTreeMap<String, BTreeMap<String, ParamValue>>, String)>,
    current_default: &BTreeMap<String, BTreeMap<String, ParamValue>>,
    current_default_id: &str,
    symbol: &str,
    params: BTreeMap<String, ParamValue>,
    origin_id: String,
    path: &Path,
    counts: &mut AdmissionCounts,
) -> Result<bool> {
    let schema = ctx
        .parameter_schemas
        .get(symbol)
        .with_context(|| format!("optimizer schema missing symbol={symbol}"))?;
    if let Err(error) = schema.ordered_values(&params) {
        reject(
            counts,
            path,
            "incomplete_or_invalid_map",
            &error.to_string(),
        );
        return Ok(false);
    }
    let mut params_by_symbol = current_default.clone();
    let replaced = params_by_symbol
        .insert(symbol.to_string(), params)
        .with_context(|| format!("optimizer default params missing symbol={symbol}"))?;
    ensure!(
        current_default.get(symbol) == Some(&replaced),
        "optimizer frozen default changed while admitting archive symbol={symbol}"
    );
    let candidate_id = candidate_id_for_params(ctx, &params_by_symbol)?;
    if candidate_id == current_default_id {
        counts.defaults += 1;
        return Ok(false);
    }
    if contenders.contains_key(&candidate_id) {
        counts.duplicates += 1;
        return Ok(false);
    }
    contenders.insert(candidate_id, (params_by_symbol, origin_id));
    Ok(true)
}

fn reject(counts: &mut AdmissionCounts, path: &Path, reason: &str, detail: &str) {
    counts.invalid += 1;
    let _ = writeln!(
        std::io::stderr().lock(),
        "CMAES_AUTO_OPTIMIZE archived_contender_rejected path={} reason={reason} detail={detail}",
        path.display()
    );
}

fn validate_archived_summary(
    report: &polymarket_hft::optimizer_report::OptimizerSummaryArtifact,
    artifact: &candidate_proof::CandidateResultArtifact,
) -> Result<()> {
    let defaults = Path::new(&artifact.run_root).join("defaults");
    let root_spec = defaults.join("optimization.yml");
    ensure!(
        candidate_proof::file_sha256(&root_spec)? == artifact.root_spec_sha256
            && config::load_optimizer_selection_mode(&root_spec)? == artifact.selection_mode,
        "archived optimizer mode differs from original frozen root"
    );
    let profile = ObjectiveProfile::load(&defaults.join("objective_profile.yml"))?;
    ensure!(
        profile.sha256() == artifact.objective_profile_sha256,
        "archived optimizer objective differs from original frozen profile"
    );
    summary::verify_native_summary_core(report, artifact, &profile)?;
    summary::verify_structural_summary(report, artifact, &profile)
}

fn current_archive_source_matches(
    path: &Path,
    summary: &polymarket_hft::optimizer_report::OptimizerSummaryArtifact,
    artifact: &candidate_proof::CandidateResultArtifact,
) -> bool {
    let Some(run_id) = path.parent().and_then(Path::file_name) else {
        return false;
    };
    let Some(capture_id) = path
        .parent()
        .and_then(Path::parent)
        .and_then(Path::file_name)
    else {
        return false;
    };
    let Some(selection) = summary
        .captures
        .iter()
        .find(|capture| capture.role == "selection")
    else {
        return false;
    };
    let Some(frozen_selection) = artifact.frozen_capture_inputs.selection.first() else {
        return false;
    };
    Path::new(&selection.capture).file_name() == Some(capture_id)
        && Path::new(&summary.run_root)
            == Path::new(&selection.capture)
                .join("backtest_reports")
                .join("optimizer_runs")
                .join(run_id)
        && Path::new(&summary.candidate_result_path)
            == Path::new(&summary.run_root).join("candidate_result.json")
        && artifact.run_root == summary.run_root
        && artifact.schema_version == 19
        && summary.schema_version == 19
        && artifact.search_trials == summary.search_trials
        && artifact.exercised_trials == summary.exercised_trials
        && artifact.objective_profile_sha256 == summary.objective_profile.sha256
        && summary.symbol_searches.len()
            == frozen_selection.prepared_proof_identities_by_symbol.len()
        && summary.symbol_searches.iter().all(|search| {
            frozen_selection
                .prepared_proof_identities_by_symbol
                .contains_key(&search.symbol)
        })
        && frozen_selection.capture == Path::new(&selection.capture)
}

fn archive_summary_paths(root: &Path, counts: &mut AdmissionCounts) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    let captures = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) => {
            reject(
                counts,
                root,
                "unreadable_archive_directory",
                &error.to_string(),
            );
            return paths;
        }
    };
    for capture in captures {
        let capture = match capture {
            Ok(entry) => entry,
            Err(error) => {
                reject(counts, root, "unreadable_archive_entry", &error.to_string());
                continue;
            }
        };
        let capture_path = capture.path();
        match fs::metadata(&capture_path) {
            Ok(kind) if kind.is_dir() => {}
            Ok(_) => continue,
            Err(error) => {
                reject(
                    counts,
                    &capture_path,
                    "unreadable_archive_entry",
                    &error.to_string(),
                );
                continue;
            }
        }
        let runs = match fs::read_dir(&capture_path) {
            Ok(entries) => entries,
            Err(error) => {
                reject(
                    counts,
                    &capture_path,
                    "unreadable_archive_directory",
                    &error.to_string(),
                );
                continue;
            }
        };
        for run in runs {
            let run = match run {
                Ok(entry) => entry,
                Err(error) => {
                    reject(
                        counts,
                        &capture_path,
                        "unreadable_archive_entry",
                        &error.to_string(),
                    );
                    continue;
                }
            };
            let run_path = run.path();
            match fs::metadata(&run_path) {
                Ok(kind) if kind.is_dir() => paths.push(run_path.join("summary.json")),
                Ok(_) => {}
                Err(error) => {
                    reject(
                        counts,
                        &run_path,
                        "unreadable_archive_entry",
                        &error.to_string(),
                    );
                }
            }
        }
    }
    paths
}
