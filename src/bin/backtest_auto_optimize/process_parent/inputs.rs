use super::*;
use crate::candidate_proof::file_sha256;

pub(super) fn snapshot_objective_profile(
    defaults_root: &Path,
    profile: &ObjectiveProfile,
) -> Result<PathBuf> {
    let snapshot = defaults_root.join("objective_profile.yml");
    fs::write(&snapshot, profile.frozen_bytes())?;
    ensure!(
        file_sha256(&snapshot)? == profile.sha256(),
        "frozen objective profile snapshot hash mismatch"
    );
    Ok(snapshot)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn verify_frozen_inputs(
    destination_spec: &Path,
    oracle_buy_and_hold: bool,
    root_spec_bytes: &[u8],
    config_snapshot: &Path,
    config_sha256: &str,
    root_snapshot: &Path,
    root_sha256: &str,
    objective_profile_sha256: &str,
    symbol_files: &BTreeMap<String, Vec<u8>>,
    symbol_snapshots: &BTreeMap<String, PathBuf>,
    symbol_sha256s: &BTreeMap<String, String>,
) -> Result<()> {
    verify_frozen_snapshots(
        destination_spec,
        root_spec_bytes,
        config_snapshot,
        config_sha256,
        root_snapshot,
        root_sha256,
        objective_profile_sha256,
        symbol_snapshots,
        symbol_sha256s,
    )?;
    for symbol in VALID_ASSETS {
        let source = symbol_spec_path(destination_spec, symbol, oracle_buy_and_hold);
        ensure!(
            fs::read(&source)?
                == *symbol_files
                    .get(*symbol)
                    .with_context(|| format!("frozen symbol file missing {symbol}"))?,
            "symbol optimizer file changed after parent snapshot: {}",
            source.display()
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn verify_frozen_snapshots(
    destination_spec: &Path,
    root_spec_bytes: &[u8],
    config_snapshot: &Path,
    config_sha256: &str,
    root_snapshot: &Path,
    root_sha256: &str,
    objective_profile_sha256: &str,
    symbol_snapshots: &BTreeMap<String, PathBuf>,
    symbol_sha256s: &BTreeMap<String, String>,
) -> Result<()> {
    ensure!(
        fs::read(destination_spec)? == root_spec_bytes,
        "root optimizer file changed after parent snapshot"
    );
    ensure!(
        file_sha256(config_snapshot)? == config_sha256,
        "app config snapshot changed"
    );
    ensure!(
        file_sha256(root_snapshot)? == root_sha256,
        "root optimizer snapshot changed"
    );
    let objective_profile_snapshot = root_snapshot
        .parent()
        .context("root optimizer snapshot has no defaults directory")?
        .join("objective_profile.yml");
    ensure!(
        file_sha256(&objective_profile_snapshot)? == objective_profile_sha256,
        "frozen objective profile snapshot changed"
    );
    for symbol in VALID_ASSETS {
        let snapshot = symbol_snapshots
            .get(*symbol)
            .with_context(|| format!("symbol snapshot missing {symbol}"))?;
        let expected_sha = symbol_sha256s
            .get(*symbol)
            .with_context(|| format!("symbol snapshot hash missing {symbol}"))?;
        ensure!(
            file_sha256(snapshot)? == *expected_sha,
            "symbol optimizer snapshot changed {symbol}"
        );
    }
    Ok(())
}

pub(super) fn symbol_spec_path(
    root_spec: &Path,
    symbol: &str,
    oracle_buy_and_hold: bool,
) -> PathBuf {
    polymarket_hft::config::installed_optimizer_symbol_path(
        root_spec.parent().unwrap_or_else(|| Path::new(".")),
        symbol,
        oracle_buy_and_hold,
    )
}
