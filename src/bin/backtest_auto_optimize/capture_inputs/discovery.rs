use super::*;
use rand::seq::SliceRandom;
use std::io::Write;

pub(super) struct DiscoveredCapture {
    pub(super) finalized: FinalizedCaptureIdentity,
    pub(super) path: PathBuf,
}

pub(super) fn resolve_requested_captures(
    cfg: &AppConfig,
    requests: &[String],
) -> Result<Vec<DiscoveredCapture>> {
    requests
        .iter()
        .map(|request| resolve_requested_capture(cfg, request))
        .collect()
}

fn resolve_requested_capture(cfg: &AppConfig, request: &str) -> Result<DiscoveredCapture> {
    ensure!(
        !request.trim().is_empty(),
        "optimizer capture identity is empty"
    );
    let path = resolve_capture_session_for_replay(cfg, Some(&PathBuf::from(request)))?;
    let finalized = finalized_capture_identity(&path).with_context(|| {
        format!(
            "invalid finalized optimizer capture source {}",
            path.display()
        )
    })?;
    Ok(DiscoveredCapture { finalized, path })
}

pub(super) fn freeze_capture_inputs(
    cfg: &AppConfig,
    args: &Args,
    symbols: &[String],
    root_spec_sha256: &str,
    objective_profile_sha256: &str,
    preparation_pool: &rayon::ThreadPool,
) -> Result<FrozenCaptureInputs> {
    ensure!(
        args.selection_captures.len() <= 1 && args.holdout_captures.len() <= 1,
        "optimizer accepts one selection capture and at most one validation capture"
    );
    ensure!(
        args.holdout_captures.is_empty() || !args.selection_captures.is_empty(),
        "optimizer holdout captures require internal selection captures"
    );
    let automatic_random_holdout =
        !args.selection_captures.is_empty() && args.holdout_captures.is_empty();
    let mut seen_paths = BTreeSet::new();
    let mut requested = Vec::new();
    for (role, requests) in [
        (CaptureRole::Selection, &args.selection_captures),
        (CaptureRole::Holdout, &args.holdout_captures),
    ] {
        for request in requests {
            let requested_path = PathBuf::from(request);
            if request.trim().is_empty() {
                log_capture_rejection(&requested_path, "optimizer capture identity is empty");
                continue;
            }
            let path = match resolve_capture_session_for_replay(cfg, Some(&requested_path)) {
                Ok(path) => path,
                Err(error) => {
                    log_capture_rejection(&requested_path, error);
                    continue;
                }
            };
            match std::fs::canonicalize(&path) {
                Ok(canonical) => {
                    if !seen_paths.insert(canonical) {
                        log_capture_rejection(&path, "requested capture path already considered");
                        continue;
                    }
                }
                Err(error) => {
                    log_capture_rejection(&path, error);
                    seen_paths.insert(path);
                    continue;
                }
            }
            match finalized_capture_metadata_identity(&path) {
                Ok(finalized) => requested.push((role, DiscoveredCapture { finalized, path })),
                Err(error) => log_capture_rejection(&path, error),
            }
        }
    }
    let mut candidates = discover_finalized_captures(cfg, &seen_paths);
    let identity_counts =
        count_capture_identities(requested.iter().map(|(_, input)| input).chain(&candidates));
    let verify_collision = |input: &mut DiscoveredCapture| {
        let identity = (
            input.finalized.session_id.clone(),
            input.finalized.run_id.clone(),
        );
        if identity_counts.get(&identity) == Some(&1) {
            return true;
        }
        match finalized_capture_identity(&input.path) {
            Ok(finalized) => {
                input.finalized = finalized;
                true
            }
            Err(error) => {
                log_capture_rejection(&input.path, error);
                false
            }
        }
    };
    requested.retain_mut(|(_, input)| verify_collision(input));
    candidates.retain_mut(verify_collision);
    let identity_counts =
        count_capture_identities(requested.iter().map(|(_, input)| input).chain(&candidates));
    let unambiguous = |input: &DiscoveredCapture| {
        let identity = (
            input.finalized.session_id.clone(),
            input.finalized.run_id.clone(),
        );
        if identity_counts.get(&identity) == Some(&1) {
            true
        } else {
            log_capture_rejection(
                &input.path,
                format!(
                    "duplicate finalized optimizer capture identity session={} run={}",
                    input.finalized.session_id, input.finalized.run_id
                ),
            );
            false
        }
    };
    requested.retain(|(_, input)| unambiguous(input));
    candidates.retain(unambiguous);
    candidates.sort_by(compare_captures);

    let qualify = |mut input: DiscoveredCapture, role| {
        let path = input.path.clone();
        match finalized_capture_identity(&path)
            .and_then(|finalized| {
                input.finalized = finalized;
                freeze_role(
                    cfg,
                    vec![input],
                    role,
                    symbols,
                    root_spec_sha256,
                    objective_profile_sha256,
                    preparation_pool,
                )
            })
            .and_then(|mut frozen| frozen.pop().context("qualified optimizer capture missing"))
        {
            Ok(input) => Some(input),
            Err(error) => {
                log_capture_rejection(&path, error);
                None
            }
        }
    };
    let mut selection = None;
    let mut holdout = None;
    for (role, input) in requested {
        match role {
            CaptureRole::Selection => selection = qualify(input, role),
            CaptureRole::Holdout => holdout = qualify(input, role),
        }
    }
    if !args.selection_captures.is_empty() && selection.is_none() {
        return Ok(FrozenCaptureInputs {
            selection: Vec::new(),
            holdout: Vec::new(),
        });
    }
    if let Some(selected) = selection.as_ref() {
        if let Some(validation) = holdout.as_ref() {
            if let Err(error) = validate_pair(selected, validation, automatic_random_holdout) {
                log_capture_rejection(&validation.capture, error);
                holdout = None;
            }
        }
        if holdout.is_none() {
            if automatic_random_holdout {
                candidates.shuffle(&mut rand::thread_rng());
            }
            while let Some(input) = candidates.pop() {
                let Some(validation) = qualify(input, CaptureRole::Holdout) else {
                    continue;
                };
                match validate_pair(selected, &validation, automatic_random_holdout) {
                    Ok(()) => {
                        holdout = Some(validation);
                        break;
                    }
                    Err(error) => log_capture_rejection(&validation.capture, error),
                }
            }
        }
        return Ok(FrozenCaptureInputs {
            selection: selection.into_iter().collect(),
            holdout: holdout.into_iter().collect(),
        });
    }

    if let Some(validation) = holdout.as_ref() {
        let mut available_selection = None;
        while let Some(input) = candidates.pop() {
            let Some(selected) = qualify(input, CaptureRole::Selection) else {
                continue;
            };
            match validate_pair(&selected, validation, false) {
                Ok(()) => {
                    selection = Some(selected);
                    break;
                }
                Err(error) => {
                    log_capture_rejection(&selected.capture, error);
                    if available_selection.is_none() {
                        available_selection = Some(selected);
                    }
                }
            }
        }
        if selection.is_none() {
            selection = available_selection;
            holdout = None;
        }
        return Ok(FrozenCaptureInputs {
            selection: selection.into_iter().collect(),
            holdout: holdout.into_iter().collect(),
        });
    }

    // Keep the newest usable validation input while considering older selections.
    // Qualified inputs remain owned here if a different pair is needed.
    let mut qualified: Vec<FrozenCaptureInput> = Vec::new();
    while let Some(input) = candidates.pop() {
        let Some(mut candidate) = qualify(input, CaptureRole::Holdout) else {
            continue;
        };
        if let Some(validation) = qualified.first() {
            match validate_pair(&candidate, validation, false) {
                Ok(()) => {
                    candidate.role = CaptureRole::Selection;
                    return Ok(FrozenCaptureInputs {
                        selection: vec![candidate],
                        holdout: vec![qualified.remove(0)],
                    });
                }
                Err(error) => log_capture_rejection(&candidate.capture, error),
            }
        }
        qualified.push(candidate);
    }
    for validation_index in 1..qualified.len() {
        for selection_index in (validation_index + 1)..qualified.len() {
            match validate_pair(
                &qualified[selection_index],
                &qualified[validation_index],
                false,
            ) {
                Ok(()) => {
                    let mut selected = qualified.remove(selection_index);
                    selected.role = CaptureRole::Selection;
                    return Ok(FrozenCaptureInputs {
                        selection: vec![selected],
                        holdout: vec![qualified.remove(validation_index)],
                    });
                }
                Err(error) => log_capture_rejection(&qualified[selection_index].capture, error),
            }
        }
    }
    let selection = qualified
        .into_iter()
        .next()
        .map(|mut input| {
            input.role = CaptureRole::Selection;
            input
        })
        .into_iter()
        .collect();
    Ok(FrozenCaptureInputs {
        selection,
        holdout: Vec::new(),
    })
}

fn count_capture_identities<'a>(
    inputs: impl Iterator<Item = &'a DiscoveredCapture>,
) -> BTreeMap<(String, String), usize> {
    let mut identity_counts = BTreeMap::new();
    for input in inputs {
        *identity_counts
            .entry((
                input.finalized.session_id.clone(),
                input.finalized.run_id.clone(),
            ))
            .or_insert(0usize) += 1;
    }
    identity_counts
}

fn validate_pair(
    selection: &FrozenCaptureInput,
    holdout: &FrozenCaptureInput,
    allow_earlier_holdout: bool,
) -> Result<()> {
    (|| {
        ensure!(
            selection.session_id != holdout.session_id || selection.run_id != holdout.run_id,
            "selection and holdout capture identities overlap"
        );
        if allow_earlier_holdout {
            ensure!(
                selection.capture_end_ms < holdout.capture_start_ms
                    || holdout.capture_end_ms < selection.capture_start_ms,
                "selection and random holdout capture intervals overlap"
            );
        } else {
            ensure!(
                selection.capture_end_ms < holdout.capture_start_ms,
                "holdout captures must start after every selection capture ends"
            );
        }
        ensure!(
            selection
                .prepared_proof_identities_by_symbol
                .keys()
                .all(|symbol| holdout
                    .prepared_proof_identities_by_symbol
                    .contains_key(symbol)),
            "validation capture does not cover frozen selection symbols"
        );
        let first_selection_market_start = selection
            .market_lifecycles
            .iter()
            .map(|market| market.window_start_ms)
            .min()
            .context("selection market lifecycle coverage missing")?;
        let last_selection_market_end = selection
            .market_lifecycles
            .iter()
            .map(|market| market.window_end_ms)
            .max()
            .context("selection market lifecycle coverage missing")?;
        let first_holdout_market_start = holdout
            .market_lifecycles
            .iter()
            .filter(|market| {
                selection
                    .prepared_proof_identities_by_symbol
                    .contains_key(&market.symbol)
            })
            .map(|market| market.window_start_ms)
            .min()
            .context("holdout market lifecycle coverage missing")?;
        let last_holdout_market_end = holdout
            .market_lifecycles
            .iter()
            .filter(|market| {
                selection
                    .prepared_proof_identities_by_symbol
                    .contains_key(&market.symbol)
            })
            .map(|market| market.window_end_ms)
            .max()
            .context("holdout market lifecycle coverage missing")?;
        if allow_earlier_holdout {
            ensure!(
                last_selection_market_end <= first_holdout_market_start
                    || last_holdout_market_end <= first_selection_market_start,
                "selection and random holdout market lifecycles overlap"
            );
        } else {
            ensure!(
                last_selection_market_end <= first_holdout_market_start,
                "selection and holdout market lifecycles overlap"
            );
        }
        Ok(())
    })()
    .with_context(|| {
        format!(
            "optimizer capture pair rejected selection={} holdout={}",
            selection.capture.display(),
            holdout.capture.display()
        )
    })
}

fn discover_finalized_captures(
    cfg: &AppConfig,
    excluded_paths: &BTreeSet<PathBuf>,
) -> Vec<DiscoveredCapture> {
    let base_dir = Path::new(&cfg.capture.base_dir);
    let entries = match std::fs::read_dir(base_dir) {
        Ok(entries) => entries,
        Err(error) => {
            log_capture_rejection(base_dir, error);
            return Vec::new();
        }
    };
    let mut finalized_captures = Vec::new();
    let mut seen_paths = excluded_paths.clone();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                log_capture_rejection(base_dir, error);
                continue;
            }
        };
        let path = entry.path();
        if seen_paths.contains(&path) {
            continue;
        }
        match entry.file_type() {
            Ok(kind) if kind.is_dir() => {}
            Ok(_) => continue,
            Err(error) => {
                log_capture_rejection(&path, error);
                continue;
            }
        }
        match std::fs::canonicalize(&path) {
            Ok(path) => {
                if !seen_paths.insert(path) {
                    continue;
                }
            }
            Err(error) => {
                log_capture_rejection(&path, error);
                continue;
            }
        }
        match std::fs::symlink_metadata(path.join("capture_incomplete.json")) {
            Ok(_) => {
                log_capture_rejection(&path, "capture_incomplete.json is present");
                continue;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                log_capture_rejection(&path, error);
                continue;
            }
        }
        let manifest_path = path.join("capture_artifact_manifest.json");
        match std::fs::symlink_metadata(&manifest_path) {
            Ok(metadata) if metadata.is_file() => {}
            Ok(_) => {
                log_capture_rejection(&path, "capture artifact manifest is not a file");
                continue;
            }
            Err(error) => {
                log_capture_rejection(&path, error);
                continue;
            }
        }
        match finalized_capture_metadata_identity(&path) {
            Ok(finalized) => finalized_captures.push(DiscoveredCapture { finalized, path }),
            Err(error) => log_capture_rejection(&path, error),
        }
    }
    finalized_captures
}

fn log_capture_rejection(path: &Path, reason: impl std::fmt::Display) {
    let _ = writeln!(
        std::io::stderr().lock(),
        "CMAES_AUTO_OPTIMIZE capture_rejected capture={} reason={reason:#}",
        path.display()
    );
}

fn compare_captures(left: &DiscoveredCapture, right: &DiscoveredCapture) -> std::cmp::Ordering {
    left.finalized
        .start_ts_ms
        .cmp(&right.finalized.start_ts_ms)
        .then_with(|| left.finalized.end_ts_ms.cmp(&right.finalized.end_ts_ms))
        .then_with(|| left.finalized.session_id.cmp(&right.finalized.session_id))
        .then_with(|| left.finalized.run_id.cmp(&right.finalized.run_id))
}
