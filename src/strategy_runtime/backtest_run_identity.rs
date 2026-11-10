use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::UNIX_EPOCH;

use anyhow::{bail, Context, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};

use super::backtest_ohlc_manifest::capture_ohlc_manifest_for_default_symbols;
use crate::trade_session_ledger::CAPTURED_SESSION_LEDGER_FILE;

#[derive(Clone, Debug, Serialize)]
pub struct BacktestRunIdentity {
    pub capture_session: String,
    pub capture_path: String,
    pub config_snapshot_sha256: Option<String>,
    pub session_meta_sha256: Option<String>,
    pub canonical_session_ledger_sha256: String,
    pub prepared_manifest_sha256: BTreeMap<String, String>,
    pub bbwick_ohlc_sha256: BTreeMap<String, String>,
    pub config_input_sha256: BTreeMap<String, String>,
    pub matrix_binary: String,
    pub matrix_binary_sha256: String,
    pub matrix_binary_mtime_unix_ms: u128,
    pub git_head: Option<String>,
    pub git_dirty_tracked: bool,
    pub git_dirty_tracked_paths: Vec<String>,
}

impl BacktestRunIdentity {
    pub fn report_line(&self) -> String {
        let manifests = self
            .prepared_manifest_sha256
            .iter()
            .map(|(symbol, hash)| format!("{symbol}:{hash}"))
            .collect::<Vec<_>>()
            .join(",");
        let ohlc = self
            .bbwick_ohlc_sha256
            .iter()
            .map(|(key, hash)| format!("{key}:{hash}"))
            .collect::<Vec<_>>()
            .join(",");
        let config_inputs = self
            .config_input_sha256
            .iter()
            .map(|(path, hash)| format!("{path}:{hash}"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "RUN_IDENTITY capture_session={} capture_path={} config_input_sha256={} matrix_binary={} matrix_binary_sha256={} matrix_binary_mtime_unix_ms={} git_head={} git_dirty_tracked={} git_dirty_tracked_paths={} config_snapshot_sha256={} session_meta_sha256={} canonical_session_ledger_sha256={} prepared_manifest_sha256={} bbwick_ohlc_sha256={}",
            self.capture_session,
            self.capture_path,
            config_inputs,
            self.matrix_binary,
            self.matrix_binary_sha256,
            self.matrix_binary_mtime_unix_ms,
            self.git_head.as_deref().unwrap_or("NA"),
            self.git_dirty_tracked,
            if self.git_dirty_tracked_paths.is_empty() {
                "none".to_string()
            } else {
                self.git_dirty_tracked_paths.join(",")
            },
            self.config_snapshot_sha256.as_deref().unwrap_or("NA"),
            self.session_meta_sha256.as_deref().unwrap_or("NA"),
            self.canonical_session_ledger_sha256,
            if manifests.is_empty() { "NA" } else { &manifests },
            if ohlc.is_empty() { "NA" } else { &ohlc }
        )
    }
}

pub fn build_backtest_run_identity(
    capture_session: &str,
    capture_path: &Path,
    config_input_paths: &[PathBuf],
    binary_prefix: Option<&str>,
) -> Result<BacktestRunIdentity> {
    let config_input_sha256 = config_input_paths
        .iter()
        .map(|path| {
            let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
            Ok((path.display().to_string(), file_sha256(&path)?))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let current_exe = std::env::current_exe().context("current backtest executable")?;
    let metadata = fs::metadata(&current_exe)
        .with_context(|| format!("metadata {}", current_exe.display()))?;
    let modified = metadata
        .modified()
        .context("backtest binary modified time")?
        .duration_since(UNIX_EPOCH)
        .context("backtest binary mtime before unix epoch")?
        .as_millis();
    let binary = match binary_prefix {
        Some(prefix) => format!("{prefix}:{}", current_exe.display()),
        None => current_exe.display().to_string(),
    };
    let git_dirty_tracked_paths = git_dirty_tracked_paths();
    Ok(BacktestRunIdentity {
        capture_session: capture_session.to_string(),
        capture_path: capture_path.display().to_string(),
        config_snapshot_sha256: optional_file_sha256(&capture_path.join("config_snapshot.toml"))?,
        session_meta_sha256: optional_file_sha256(&capture_path.join("session_meta.json"))?,
        canonical_session_ledger_sha256: file_sha256(
            &capture_path.join(CAPTURED_SESSION_LEDGER_FILE),
        )?,
        prepared_manifest_sha256: prepared_manifest_hashes(capture_path)?,
        bbwick_ohlc_sha256: capture_ohlc_manifest_for_default_symbols(capture_path)?.hash_by_key(),
        config_input_sha256,
        matrix_binary: binary,
        matrix_binary_sha256: file_sha256(&current_exe)?,
        matrix_binary_mtime_unix_ms: modified,
        git_head: command_stdout("git", &["rev-parse", "HEAD"]).ok(),
        git_dirty_tracked: !git_dirty_tracked_paths.is_empty(),
        git_dirty_tracked_paths,
    })
}

fn git_dirty_tracked_paths() -> Vec<String> {
    command_stdout("git", &["status", "--short", "--untracked-files=no"])
        .unwrap_or_default()
        .lines()
        .filter_map(|line| line.split_whitespace().last())
        .filter(|path| !path.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn optional_file_sha256(path: &Path) -> Result<Option<String>> {
    if path.is_file() {
        Ok(Some(file_sha256(path)?))
    } else {
        Ok(None)
    }
}

fn prepared_manifest_hashes(capture_path: &Path) -> Result<BTreeMap<String, String>> {
    let prepared_root = capture_path.join("backtest_replay/v2");
    let mut hashes = BTreeMap::new();
    if !prepared_root.is_dir() {
        return Ok(hashes);
    }
    for entry in
        fs::read_dir(&prepared_root).with_context(|| format!("read {}", prepared_root.display()))?
    {
        let path = entry?.path();
        if !path.is_dir() {
            continue;
        }
        let Some(symbol) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if symbol == "central" {
            continue;
        }
        let manifest = path.join("manifest.json");
        if manifest.is_file() {
            hashes.insert(symbol.to_string(), file_sha256(&manifest)?);
        }
    }
    Ok(hashes)
}

fn file_sha256(path: &Path) -> Result<String> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn command_stdout(program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program).args(args).output()?;
    if !output.status.success() {
        bail!(
            "{program} {:?} failed status={:?}",
            args,
            output.status.code()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}
