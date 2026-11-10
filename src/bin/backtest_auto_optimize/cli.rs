use super::*;

pub(super) fn env_parse<T>(name: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(raw) if raw.trim().is_empty() => Ok(None),
        Ok(raw) => raw
            .parse()
            .map(Some)
            .map_err(|err| anyhow!("invalid {name}: {err}")),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(anyhow!("invalid {name}: {err}")),
    }
}

pub(super) fn env_bool(name: &str) -> Result<bool> {
    match std::env::var(name) {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "" | "0" | "false" | "no" => Ok(false),
            "1" | "true" | "yes" => Ok(true),
            other => bail!("invalid {name}: {other}"),
        },
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(err) => Err(anyhow!("invalid {name}: {err}")),
    }
}

pub(super) fn parse_args() -> Result<Args> {
    let mut args = Args {
        spec: PathBuf::from("optimization.yml"),
        spec_explicit: false,
        capture_session: "latest".to_string(),
        selection_captures: Vec::new(),
        holdout_captures: Vec::new(),
        jobs: None,
        seed: None,
        no_apply: false,
        frozen_capture_inputs_sha256: None,
        default_spec_snapshot: None,
        default_spec_sha256: None,
        root_spec_sha256: None,
        objective_profile_sha256: None,
        parent_run_root: None,
        proof_only_run_root: None,
        finalize_retained_run_root: None,
        symbol: String::new(),
    };
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--spec" => {
                args.spec = PathBuf::from(next_arg(&mut iter, &arg)?);
                args.spec_explicit = true;
            }
            "--capture-session" => args.capture_session = next_arg(&mut iter, &arg)?,
            "--selection-capture" => args.selection_captures.push(next_arg(&mut iter, &arg)?),
            "--holdout-capture" => args.holdout_captures.push(next_arg(&mut iter, &arg)?),
            "--jobs" => args.jobs = Some(parse_next(&mut iter, &arg)?),
            "--seed" => args.seed = Some(parse_next(&mut iter, &arg)?),
            "--no-apply" => args.no_apply = true,
            "--frozen-capture-inputs-sha256" => {
                args.frozen_capture_inputs_sha256 = Some(next_arg(&mut iter, &arg)?);
            }
            "--default-spec-snapshot" => {
                args.default_spec_snapshot = Some(PathBuf::from(next_arg(&mut iter, &arg)?));
            }
            "--default-spec-sha256" => {
                args.default_spec_sha256 = Some(next_arg(&mut iter, &arg)?);
            }
            "--root-spec-sha256" => {
                args.root_spec_sha256 = Some(next_arg(&mut iter, &arg)?);
            }
            "--objective-profile-sha256" => {
                args.objective_profile_sha256 = Some(next_arg(&mut iter, &arg)?);
            }
            "--parent-run-root" => {
                args.parent_run_root = Some(PathBuf::from(next_arg(&mut iter, &arg)?));
            }
            "--proof-only-run-root" => {
                args.proof_only_run_root = Some(PathBuf::from(next_arg(&mut iter, &arg)?));
            }
            "--finalize-retained-run-root" => {
                args.finalize_retained_run_root = Some(PathBuf::from(next_arg(&mut iter, &arg)?));
            }
            "--symbol" => args.symbol = normalize_symbol(&next_arg(&mut iter, &arg)?)?,
            "--help" | "-h" => {
                println!(
                    "Usage: backtest_auto_optimize [--jobs N] [--no-apply]\n       backtest_auto_optimize --selection-capture PATH [--jobs N] [--no-apply]\n       backtest_auto_optimize --selection-capture PATH --holdout-capture PATH [--jobs N] [--no-apply]\n       backtest_auto_optimize --finalize-retained-run-root PATH\n       internal symbol child: --symbol ASSET --default-spec-snapshot PATH"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument {other}"),
        }
    }

    let child = args.default_spec_snapshot.is_some();
    if child {
        ensure!(
            args.proof_only_run_root.is_none() && args.finalize_retained_run_root.is_none(),
            "retained parent modes cannot be a symbol child"
        );
        ensure!(!args.symbol.is_empty(), "optimizer child requires --symbol");
        ensure!(
            args.selection_captures.len() == 1,
            "optimizer child requires one frozen A capture"
        );
        ensure!(
            args.holdout_captures.is_empty(),
            "optimizer child must not receive validation B"
        );
        ensure!(
            args.frozen_capture_inputs_sha256.is_some()
                && args.default_spec_sha256.is_some()
                && args.root_spec_sha256.is_some()
                && args.objective_profile_sha256.is_some()
                && args.parent_run_root.is_some(),
            "optimizer child immutable inputs are incomplete"
        );
    } else {
        ensure!(args.symbol.is_empty(), "--symbol is internal-only");
        ensure!(
            args.frozen_capture_inputs_sha256.is_none(),
            "frozen digest is internal-only"
        );
    }
    if args.proof_only_run_root.is_some() {
        ensure!(args.no_apply, "proof-only mode requires --no-apply");
    }
    if args.finalize_retained_run_root.is_some() {
        ensure!(
            !child
                && args.proof_only_run_root.is_none()
                && !args.no_apply
                && args.jobs.is_none_or(|jobs| jobs == 0)
                && args.seed.is_none()
                && args.spec == Path::new("optimization.yml")
                && args.capture_session == "latest"
                && args.selection_captures.is_empty()
                && args.holdout_captures.is_empty()
                && args.default_spec_sha256.is_none()
                && args.root_spec_sha256.is_none()
                && args.objective_profile_sha256.is_none()
                && args.parent_run_root.is_none(),
            "retained finalization requires the original inputs, automatic workers and normal application"
        );
    }
    if !child && args.proof_only_run_root.is_none() && args.finalize_retained_run_root.is_none() {
        ensure!(
            (args.selection_captures.is_empty() && args.holdout_captures.is_empty())
                || (args.selection_captures.len() == 1 && args.holdout_captures.len() <= 1),
            "optimizer accepts automatic capture selection, one --selection-capture with automatic random validation, or exactly one --selection-capture and one --holdout-capture"
        );
        ensure!(
            args.capture_session == "latest",
            "optimizer parent --capture-session must remain latest"
        );
    }
    ensure!(
        !args.capture_session.trim().is_empty(),
        "optimizer capture target is empty"
    );
    Ok(args)
}

fn normalize_symbol(raw: &str) -> Result<String> {
    let symbol = raw.trim().to_ascii_uppercase();
    ensure!(
        VALID_ASSETS.contains(&symbol.as_str()),
        "invalid --symbol {raw}; expected one of {}",
        VALID_ASSETS.join(",")
    );
    Ok(symbol)
}

fn next_arg(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    iter.next()
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn parse_next<T>(iter: &mut impl Iterator<Item = String>, flag: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    next_arg(iter, flag)?
        .parse()
        .map_err(|err| anyhow!("invalid {flag}: {err}"))
}
