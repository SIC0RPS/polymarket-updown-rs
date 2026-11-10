use std::fs;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(super) const PREPARED_SCHEMA_VERSION: u32 = 24;
pub(super) const PREPARED_REPLAY_DIR: &str = "backtest_replay/v2";
pub(super) const MANIFEST_FILE: &str = "manifest.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct PreparedReplayManifest {
    pub schema_version: u32,
    pub completed: bool,
    pub symbol: String,
    pub engine_source: SourceFileManifest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub strategy_consumption_source: Option<SourceFileManifest>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub strategy_consumption_sources: Vec<SourceFileManifest>,
    #[serde(default)]
    pub resolution_source: Option<SourceFileManifest>,
    pub prepared_payload_sha256: String,
    pub engine_rows: u64,
    pub strategy_consumption_rows: u64,
    #[serde(default)]
    pub resolution_rows: u64,
    pub market_delta_rows: u64,
    pub token_book_delta_rows: u64,
    pub lifecycle_delta_rows: u64,
    pub resolution_delta_rows: u64,
    #[serde(default)]
    pub last_trade_delta_rows: u64,
    pub total_events: u64,
    pub start_ts_ms: Option<u64>,
    pub end_ts_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct SourceFileManifest {
    pub path: String,
    pub size_bytes: u64,
    pub mtime_unix_ms: u128,
    #[serde(default)]
    pub sha256: Option<String>,
}

impl SourceFileManifest {
    pub(super) fn capture(path: &Path) -> Result<Self> {
        let (size_bytes, mtime_unix_ms) = source_file_identity(path)?;
        Ok(Self {
            path: path.display().to_string(),
            size_bytes,
            mtime_unix_ms,
            sha256: Some(file_sha256(path)?),
        })
    }
}

pub(super) fn source_file_identity(path: &Path) -> Result<(u64, u128)> {
    let metadata = fs::metadata(path).with_context(|| format!("metadata {}", path.display()))?;
    let mtime_unix_ms = metadata
        .modified()
        .with_context(|| format!("mtime {}", path.display()))?
        .duration_since(UNIX_EPOCH)
        .with_context(|| format!("mtime before unix epoch {}", path.display()))?
        .as_millis();
    Ok((metadata.len(), mtime_unix_ms))
}

fn file_sha256(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buf = [0_u8; 1024 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .with_context(|| format!("read {}", path.display()))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

impl PreparedReplayManifest {
    pub(super) fn read(path: &Path) -> Result<Self> {
        let raw = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        serde_json::from_str(&raw).with_context(|| format!("parse {}", path.display()))
    }

    pub(super) fn write(&self, path: &Path) -> Result<()> {
        let raw = serde_json::to_string_pretty(self).context("serialize prepared manifest")?;
        fs::write(path, format!("{raw}\n")).with_context(|| format!("write {}", path.display()))
    }

    pub(super) fn is_complete(&self) -> bool {
        self.completed
            && self.schema_version == PREPARED_SCHEMA_VERSION
            && self.strategy_sources().is_some()
            && self.prepared_payload_sha256.len() == 64
            && self.strategy_consumption_rows > 0
            && self.resolution_rows == self.resolution_delta_rows
            && (self.resolution_source.is_some() || self.resolution_rows == 0)
    }

    pub(super) fn strategy_sources(&self) -> Option<&[SourceFileManifest]> {
        match self.schema_version {
            PREPARED_SCHEMA_VERSION
                if self.strategy_consumption_source.is_none()
                    && !self.strategy_consumption_sources.is_empty() =>
            {
                Some(&self.strategy_consumption_sources)
            }
            _ => None,
        }
    }

    pub(super) fn verify_prepared_payload(&self, dir: &Path) -> Result<[u8; 32]> {
        let digest = prepared_payload_digest(dir)?;
        let actual = encode_sha256(digest);
        anyhow::ensure!(
            actual == self.prepared_payload_sha256,
            "prepared replay payload SHA-256 mismatch expected={} actual={}",
            self.prepared_payload_sha256,
            actual
        );
        Ok(digest)
    }
}

pub(super) fn prepared_payload_sha256(dir: &Path) -> Result<String> {
    Ok(encode_sha256(prepared_payload_digest(dir)?))
}

fn prepared_payload_digest(dir: &Path) -> Result<[u8; 32]> {
    let mut hasher = Sha256::new();
    for name in super::prepared_arrow::PREPARED_PAYLOAD_FILES {
        let path = dir.join(name);
        let mut file = fs::File::open(&path).with_context(|| format!("open {}", path.display()))?;
        let size = file
            .metadata()
            .with_context(|| format!("metadata {}", path.display()))?
            .len();
        hasher.update((name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher.update(size.to_le_bytes());
        let mut buf = [0_u8; 1024 * 1024];
        loop {
            let n = file
                .read(&mut buf)
                .with_context(|| format!("read {}", path.display()))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
    }
    Ok(hasher.finalize().into())
}

fn encode_sha256(digest: [u8; 32]) -> String {
    use std::fmt::Write as _;

    digest
        .iter()
        .fold(String::with_capacity(64), |mut out, byte| {
            write!(out, "{byte:02x}").expect("writing SHA-256 to String cannot fail");
            out
        })
}

pub(super) fn prepared_symbol_dir(capture: &Path, symbol: &str) -> PathBuf {
    capture
        .join(PREPARED_REPLAY_DIR)
        .join(symbol.trim().to_ascii_lowercase())
}
