use super::*;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

const OPTIMIZER_LEARNING_DIR: &str = "data/oracle_learning/optimizer";

pub(super) fn export_selected_oracle_learning(
    destination_spec: &Path,
    learning_by_symbol: &BTreeMap<String, polymarket_hft::strategy::OracleLearningSnapshot>,
    eligible_symbols: &BTreeSet<String>,
) -> Result<BTreeMap<String, polymarket_hft::config::OptimizerLearningSelection>> {
    ensure!(
        !eligible_symbols.is_empty()
            && eligible_symbols
                .iter()
                .all(|symbol| VALID_ASSETS.contains(&symbol.as_str())),
        "selected oracle learning requires eligible symbols"
    );
    let root = destination_spec.parent().unwrap_or_else(|| Path::new("."));
    let mut selections = BTreeMap::new();
    for symbol in eligible_symbols {
        let learning = learning_by_symbol
            .get(symbol)
            .with_context(|| format!("selected oracle learning missing symbol={symbol}"))?;
        let bytes = rmp_serde::to_vec_named(&BTreeMap::from([(symbol.clone(), learning.clone())]))?;
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        let relative_path =
            PathBuf::from(OPTIMIZER_LEARNING_DIR).join(format!("{symbol}-{sha256}.rmp"));
        let artifact_path = root.join(&relative_path);
        if artifact_path.exists() {
            ensure!(
                fs::read(&artifact_path)? == bytes,
                "selected oracle learning immutable artifact mismatch {symbol}"
            );
        } else {
            polymarket_hft::runtime_files::write_bytes_atomic(&artifact_path, &bytes)?;
        }
        ensure!(
            selections
                .insert(
                    symbol.clone(),
                    polymarket_hft::config::OptimizerLearningSelection {
                        path: relative_path,
                        sha256,
                    },
                )
                .is_none(),
            "duplicate selected learning symbol={symbol}"
        );
    }
    Ok(selections)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn apply_joint_candidate(
    destination_spec: &Path,
    oracle_buy_and_hold: bool,
    frozen_schema_spec: &Path,
    params_by_symbol: &BTreeMap<String, BTreeMap<String, ParamValue>>,
    learning_by_symbol: &BTreeMap<String, polymarket_hft::config::OptimizerLearningSelection>,
    frozen_symbol_files: &BTreeMap<String, Vec<u8>>,
    frozen_symbol_hashes: &BTreeMap<String, String>,
    verify_and_audit: impl FnOnce() -> Result<()>,
) -> Result<BTreeMap<String, String>> {
    let frozen_root = fs::read(frozen_schema_spec)
        .with_context(|| format!("read {}", frozen_schema_spec.display()))?;
    ensure!(
        fs::read(destination_spec)? == frozen_root,
        "root optimizer file changed after parent snapshot: {}",
        destination_spec.display()
    );
    let root_schema = OptimizerParameterSchema::load_root(frozen_schema_spec)?;
    ensure!(
        !params_by_symbol.is_empty()
            && params_by_symbol
                .keys()
                .all(|symbol| VALID_ASSETS.contains(&symbol.as_str()))
            && !learning_by_symbol.is_empty()
            && learning_by_symbol
                .keys()
                .all(|symbol| params_by_symbol.contains_key(symbol))
            && frozen_symbol_files.len() == VALID_ASSETS.len()
            && frozen_symbol_hashes.len() == VALID_ASSETS.len(),
        "joint optimizer apply requires complete frozen inputs and eligible symbols"
    );
    verify_frozen_state(
        destination_spec,
        oracle_buy_and_hold,
        &frozen_root,
        frozen_symbol_files,
        frozen_symbol_hashes,
    )?;

    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let staging_dir = std::env::temp_dir().join(format!(
        "polymarket-optimizer-apply-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir(&staging_dir)
        .with_context(|| format!("create optimizer apply staging {}", staging_dir.display()))?;
    let result = prepare_and_apply_joint_candidate(
        destination_spec,
        oracle_buy_and_hold,
        &frozen_root,
        &root_schema,
        params_by_symbol,
        learning_by_symbol,
        frozen_symbol_files,
        frozen_symbol_hashes,
        &staging_dir,
        verify_and_audit,
    );
    if let Err(error) = fs::remove_dir_all(&staging_dir) {
        crate::summary::publish_progress(format_args!(
            "CMAES_AUTO_OPTIMIZE staging_cleanup_failed path={} error={error}",
            staging_dir.display()
        ));
    }
    let applied = result?;
    crate::summary::publish_progress(format_args!(
        "CMAES_AUTO_OPTIMIZE applied_joint_candidate symbols={}",
        applied
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(",")
    ));
    Ok(applied)
}

#[allow(clippy::too_many_arguments)]
fn prepare_and_apply_joint_candidate(
    destination_spec: &Path,
    oracle_buy_and_hold: bool,
    frozen_root: &[u8],
    root_schema: &OptimizerParameterSchema,
    params_by_symbol: &BTreeMap<String, BTreeMap<String, ParamValue>>,
    learning_by_symbol: &BTreeMap<String, polymarket_hft::config::OptimizerLearningSelection>,
    frozen_symbol_files: &BTreeMap<String, Vec<u8>>,
    frozen_symbol_hashes: &BTreeMap<String, String>,
    staging_dir: &Path,
    verify_and_audit: impl FnOnce() -> Result<()>,
) -> Result<BTreeMap<String, String>> {
    let root = destination_spec.parent().unwrap_or_else(|| Path::new("."));
    let mut updated_files = BTreeMap::new();
    let mut updated_hashes = BTreeMap::new();
    for symbol in learning_by_symbol.keys() {
        let params = params_by_symbol
            .get(symbol)
            .with_context(|| format!("selected optimizer map missing symbol={symbol}"))?;
        root_schema
            .ordered_values(params)
            .with_context(|| format!("validate selected {symbol} optimizer map"))?;
        let learning = learning_by_symbol
            .get(symbol)
            .with_context(|| format!("selected learning reference missing symbol={symbol}"))?;
        let frozen = frozen_symbol_files
            .get(symbol)
            .with_context(|| format!("frozen optimizer file missing symbol={symbol}"))?;
        let updated = render_optimization_defaults(frozen, params, learning)
            .with_context(|| format!("render {symbol} optimizer defaults"))?;
        let staged_path = staging_dir.join(format!("{symbol}.optimization.yml"));
        fs::write(&staged_path, &updated)
            .with_context(|| format!("stage {symbol} optimizer defaults"))?;
        let staged_schema = root_schema
            .with_symbol_defaults(&staged_path)
            .with_context(|| format!("validate staged {symbol} optimizer map"))?;
        ensure!(
            staged_schema.default_map()? == *params,
            "staged {symbol} optimizer defaults differ from selected map"
        );
        ensure!(
            polymarket_hft::config::optimizer_learning_selection(&staged_path)?.as_ref()
                == Some(learning),
            "staged {symbol} oracle learning reference differs from selected state"
        );
        updated_hashes.insert(symbol.clone(), format!("{:x}", Sha256::digest(&updated)));
        updated_files.insert(symbol.clone(), updated);
    }

    let mut written_symbols = Vec::new();
    let transaction = (|| -> Result<()> {
        verify_frozen_state(
            destination_spec,
            oracle_buy_and_hold,
            frozen_root,
            frozen_symbol_files,
            frozen_symbol_hashes,
        )?;
        for symbol in learning_by_symbol.keys() {
            let path = polymarket_hft::config::installed_optimizer_symbol_path(
                root,
                symbol,
                oracle_buy_and_hold,
            );
            let updated = updated_files
                .get(symbol)
                .with_context(|| format!("prepared optimizer file missing symbol={symbol}"))?;
            polymarket_hft::runtime_files::write_bytes_atomic(&path, updated)
                .with_context(|| format!("apply {}", path.display()))?;
            written_symbols.push(symbol.clone());
        }
        verify_applied_state(
            destination_spec,
            oracle_buy_and_hold,
            frozen_root,
            frozen_symbol_files,
            frozen_symbol_hashes,
            &updated_files,
        )?;
        verify_and_audit()
    })();
    if let Err(error) = transaction {
        if written_symbols.is_empty() {
            return Err(error).context("joint optimizer apply aborted before target writes");
        }
        let mut rollback_errors = Vec::new();
        for symbol in written_symbols.iter().rev() {
            let path = polymarket_hft::config::installed_optimizer_symbol_path(
                root,
                symbol,
                oracle_buy_and_hold,
            );
            let frozen = frozen_symbol_files
                .get(symbol.as_str())
                .with_context(|| format!("frozen optimizer file missing symbol={symbol}"))?;
            if let Err(rollback_error) =
                polymarket_hft::runtime_files::write_bytes_atomic(&path, frozen)
            {
                rollback_errors.push(format!("{symbol}:{rollback_error:#}"));
            }
        }
        if rollback_errors.is_empty() {
            verify_frozen_state(
                destination_spec,
                oracle_buy_and_hold,
                frozen_root,
                frozen_symbol_files,
                frozen_symbol_hashes,
            )?;
            return Err(error).context("joint optimizer apply rolled back");
        }
        bail!(
            "joint optimizer apply failed: {error:#}; rollback failed: {}",
            rollback_errors.join("|")
        );
    }
    Ok(updated_hashes)
}

fn verify_frozen_state(
    destination_spec: &Path,
    oracle_buy_and_hold: bool,
    frozen_root: &[u8],
    frozen_symbol_files: &BTreeMap<String, Vec<u8>>,
    frozen_symbol_hashes: &BTreeMap<String, String>,
) -> Result<()> {
    ensure!(
        fs::read(destination_spec)? == frozen_root,
        "root optimizer file changed during joint apply: {}",
        destination_spec.display()
    );
    let root = destination_spec.parent().unwrap_or_else(|| Path::new("."));
    for symbol in VALID_ASSETS {
        let path = polymarket_hft::config::installed_optimizer_symbol_path(
            root,
            symbol,
            oracle_buy_and_hold,
        );
        let frozen = frozen_symbol_files
            .get(*symbol)
            .with_context(|| format!("frozen optimizer file missing symbol={symbol}"))?;
        let expected_hash = frozen_symbol_hashes
            .get(*symbol)
            .with_context(|| format!("frozen optimizer hash missing symbol={symbol}"))?;
        ensure!(
            fs::read(&path)? == *frozen && candidate_proof::file_sha256(&path)? == *expected_hash,
            "optimizer file changed before joint apply symbol={symbol} path={}",
            path.display()
        );
    }
    Ok(())
}

fn verify_applied_state(
    destination_spec: &Path,
    oracle_buy_and_hold: bool,
    frozen_root: &[u8],
    frozen_symbol_files: &BTreeMap<String, Vec<u8>>,
    frozen_symbol_hashes: &BTreeMap<String, String>,
    updated_files: &BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    ensure!(
        fs::read(destination_spec)? == frozen_root,
        "root optimizer file changed during joint apply: {}",
        destination_spec.display()
    );
    let root = destination_spec.parent().unwrap_or_else(|| Path::new("."));
    for symbol in VALID_ASSETS {
        let path = polymarket_hft::config::installed_optimizer_symbol_path(
            root,
            symbol,
            oracle_buy_and_hold,
        );
        if let Some(updated) = updated_files.get(*symbol) {
            ensure!(
                fs::read(&path)? == *updated,
                "joint optimizer target final readback failed symbol={symbol}"
            );
        } else {
            let frozen = frozen_symbol_files
                .get(*symbol)
                .with_context(|| format!("frozen optimizer file missing symbol={symbol}"))?;
            let expected_hash = frozen_symbol_hashes
                .get(*symbol)
                .with_context(|| format!("frozen optimizer hash missing symbol={symbol}"))?;
            ensure!(
                fs::read(&path)? == *frozen
                    && candidate_proof::file_sha256(&path)? == *expected_hash,
                "noneligible optimizer file changed symbol={symbol} path={}",
                path.display()
            );
        }
    }
    Ok(())
}

fn render_optimization_defaults(
    original: &[u8],
    params: &BTreeMap<String, ParamValue>,
    learning: &polymarket_hft::config::OptimizerLearningSelection,
) -> Result<Vec<u8>> {
    let text = std::str::from_utf8(original).context("optimizer YAML is not UTF-8")?;
    let mut current_parameter = None;
    let mut seen = BTreeSet::new();
    let mut rendered = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let (body, ending) = if let Some(body) = line.strip_suffix("\r\n") {
            (body, "\r\n")
        } else if let Some(body) = line.strip_suffix('\n') {
            (body, "\n")
        } else {
            (line, "")
        };
        if !body.as_bytes().first().is_some_and(u8::is_ascii_whitespace) {
            current_parameter = body.strip_suffix(':');
        }
        if let (Some(name), Some(_)) = (current_parameter, body.strip_prefix("  default:")) {
            if let Some(value) = params.get(name) {
                ensure!(
                    seen.insert(name.to_string()),
                    "duplicate optimizer default field {name}"
                );
                rendered.push_str("  default: ");
                rendered.push_str(&param_value_text(value));
                rendered.push_str(ending);
                continue;
            }
        }
        rendered.push_str(body);
        rendered.push_str(ending);
    }
    let missing = params
        .keys()
        .filter(|name| !seen.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        missing.is_empty(),
        "optimizer YAML missing default fields: {}",
        missing.join(",")
    );
    let rendered = remove_learning_selection(&rendered);
    let mut rendered = rendered.trim_end_matches(['\r', '\n']).to_string();
    rendered.push_str("\nlearning:\n  path: ");
    rendered.push_str(&learning.path.to_string_lossy());
    rendered.push_str("\n  sha256: ");
    rendered.push_str(&learning.sha256);
    rendered.push('\n');
    Ok(rendered.into_bytes())
}

fn remove_learning_selection(text: &str) -> String {
    let mut rendered = String::with_capacity(text.len());
    let mut skipping = false;
    for line in text.split_inclusive('\n') {
        let body = line.trim_end_matches(['\r', '\n']);
        if !body.as_bytes().first().is_some_and(u8::is_ascii_whitespace) {
            skipping = body == "learning:";
            if skipping {
                continue;
            }
        } else if skipping {
            continue;
        }
        rendered.push_str(line);
    }
    rendered
}

fn param_value_text(value: &ParamValue) -> String {
    match value {
        ParamValue::Int(value) => value.to_string(),
        ParamValue::Float(value) => {
            let text = format!("{value:.12}");
            text.trim_end_matches('0').trim_end_matches('.').to_string()
        }
    }
}
