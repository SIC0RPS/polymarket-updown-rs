use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use polymarket_hft::config::AppConfig;

pub(crate) const MAIN_CONFIG_PATH: &str = "config/main.toml";
const ALCHEMY_API_KEY_ENV: &str = "ALCHEMY_API_KEY";

fn find_repo_root_from_cwd() -> Option<PathBuf> {
    let mut cur = std::env::current_dir().ok()?;
    loop {
        if cur.join("Cargo.toml").is_file() && cur.join(MAIN_CONFIG_PATH).is_file() {
            return Some(cur);
        }
        cur = cur.parent()?.to_path_buf();
    }
}

pub(crate) fn load_env_file() -> Result<PathBuf> {
    let repo_root = find_repo_root_from_cwd()
        .context("could not detect repository root from current working directory")?;
    let mut candidate_paths = Vec::new();
    if let Some(parent) = repo_root.parent() {
        candidate_paths.push(parent.join(".env"));
    }
    if let Some(home_dir) = std::env::var_os("HOME") {
        let home_candidate = PathBuf::from(home_dir).join("project/.env");
        if !candidate_paths
            .iter()
            .any(|candidate| candidate == &home_candidate)
        {
            candidate_paths.push(home_candidate);
        }
    }
    if candidate_paths.is_empty() {
        anyhow::bail!("repository root has no parent directory for ../.env and no HOME candidate");
    }

    let mut last_err = None;
    for env_path in candidate_paths {
        let load_result = dotenvy::from_path(&env_path).and_then(|()| {
            for item in dotenvy::from_path_iter(&env_path)? {
                let (key, value) = item?;
                if key == ALCHEMY_API_KEY_ENV {
                    std::env::set_var(key, value);
                }
            }
            Ok(())
        });
        match load_result {
            Ok(()) => {
                return Ok(env_path);
            }
            Err(err) => last_err = Some((env_path, err)),
        }
    }

    let (env_path, err) = last_err.context("no env candidate paths were attempted")?;
    Err(err).with_context(|| format!("failed to load env file at {}", env_path.display()))
}

fn print_live_env_block_warning(reason: &str) {
    eprintln!(
        "\x1b[1;31mLIVE MODE BLOCKED: required env file ../.env missing/invalid ({reason})\x1b[0m"
    );
}

pub(crate) fn enforce_live_env_requirements(
    cfg: &AppConfig,
    env_load_res: &Result<PathBuf>,
) -> Result<()> {
    if let Err(err) = env_load_res {
        print_live_env_block_warning(&err.to_string());
        anyhow::bail!("live mode requires valid ../.env: {err}");
    }

    let key_var = cfg.polymarket.private_key_env.trim();
    if key_var.is_empty() {
        print_live_env_block_warning("polymarket.private_key_env is empty");
        anyhow::bail!("live mode requires non-empty polymarket.private_key_env");
    }
    if std::env::var(key_var).ok().map(|v| !v.trim().is_empty()) != Some(true) {
        print_live_env_block_warning(&format!("env var {key_var} is missing/empty"));
        anyhow::bail!("live mode requires env var {key_var} to be set from ../.env");
    }
    Ok(())
}

pub(crate) fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
