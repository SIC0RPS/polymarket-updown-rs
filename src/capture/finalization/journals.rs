use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::capture::arrow::POLYMARKET_WS_INGRESS_ARROW_FILE;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CaptureJournalFile {
    pub path: String,
    pub written: u64,
    pub sha256: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CaptureJournalManifestEntry {
    pub lane: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_evidence_version: Option<u32>,
    pub causal_identity: String,
    pub attempted: u64,
    pub accepted: u64,
    pub full: u64,
    pub closed: u64,
    pub written: u64,
    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,
    pub writer_error: Option<String>,
    pub files: Vec<CaptureJournalFile>,
}

pub(super) fn validate_journals(
    session_dir: &Path,
    entries: &[CaptureJournalManifestEntry],
    schema_version: &str,
) -> (Vec<String>, HashMap<String, u64>, Option<anyhow::Error>) {
    const ACTORS: [&str; 4] = ["BTC", "ETH", "SOL", "XRP"];
    const PRIORITY_FILES: [&str; 11] = [
        "order_submit_latency.jsonl",
        "order_submit_reject.jsonl",
        "local_order_block.jsonl",
        "risk_block.jsonl",
        "polymarket_user_order.jsonl",
        "polymarket_user_trade.jsonl",
        "replay_seed.json",
        "replay_risk_snapshot.jsonl",
        "polymarket_discovery_snapshot.jsonl",
        "control_timeline.jsonl",
        "ctf_inventory_operation_latency.jsonl",
    ];

    let mut failures = Vec::new();
    let mut inspection_error = None;
    let mut replayable_rows_by_underlying = HashMap::new();
    let mut identities = HashSet::new();
    let expected_count = match schema_version {
        "capture_artifact_manifest.v2" => 11,
        "capture_artifact_manifest.v3" => 15,
        other => {
            failures.push(format!("journal_schema_unsupported schema={other}"));
            return (failures, replayable_rows_by_underlying, inspection_error);
        }
    };
    let split_strategy = schema_version == "capture_artifact_manifest.v3";
    for entry in entries {
        let lane = entry.lane.as_str();
        let identity = entry.causal_identity.as_str();
        if let Some(version) = entry.provider_evidence_version {
            if lane != "market" || version != 1 {
                failures.push(format!(
                    "provider_evidence_version_invalid lane={lane} identity={identity} version={version}"
                ));
            }
        } else if lane == "market" {
            tracing::info!(
                target: "capture.finalization",
                identity,
                "binance_provider_evidence_unavailable"
            );
        }
        if !identities.insert((lane, identity)) {
            failures.push(format!("duplicate_journal lane={lane} identity={identity}"));
        }
        if entry
            .accepted
            .checked_add(entry.full)
            .and_then(|count| count.checked_add(entry.closed))
            != Some(entry.attempted)
            || entry.accepted != entry.written
            || entry.full != 0
            || entry.closed != 0
            || entry.writer_error.is_some()
            || entry
                .files
                .iter()
                .try_fold(0_u64, |sum, file| sum.checked_add(file.written))
                != Some(entry.written)
        {
            failures.push(format!(
                "journal_incomplete lane={lane} identity={identity}"
            ));
        }

        let actor = ACTORS.contains(&identity);
        let lower = identity.to_ascii_lowercase();
        let expected_strategy = format!("strategy_consumption/{identity}.arrow");
        let split_identity = identity.split_once('/').filter(|(actor, kind)| {
            ACTORS.contains(actor) && matches!(*kind, "runtime" | "chainlink")
        });
        let expected_split_strategy = split_identity
            .map(|(actor, kind)| format!("strategy_consumption/{kind}/{actor}.arrow"));
        let expected_engine = format!("polymarket_engine_state_replayable_{lower}.arrow");
        let expected_resolution = format!("polymarket_resolution_{lower}.arrow");
        let expected_provider = format!("binance_provider_{lower}.jsonl");
        let paths: HashSet<&str> = entry.files.iter().map(|file| file.path.as_str()).collect();
        if paths.len() != entry.files.len() {
            failures.push(format!(
                "duplicate_journal_file lane={lane} identity={identity}"
            ));
        }
        let expected_files = match lane {
            "strategy_consumption" if !split_strategy && actor => {
                entry.files.len() == 1 && paths.contains(expected_strategy.as_str())
            }
            "strategy_consumption" if split_strategy && split_identity.is_some() => {
                entry.files.len() == 1
                    && expected_split_strategy
                        .as_deref()
                        .is_some_and(|path| paths.contains(path))
            }
            "market" if actor => {
                paths.contains(expected_engine.as_str())
                    && paths.contains(expected_resolution.as_str())
                    && match entry.provider_evidence_version {
                        None => entry.files.len() == 2,
                        Some(1) => {
                            entry.files.len() == 3 && paths.contains(expected_provider.as_str())
                        }
                        Some(_) => false,
                    }
            }
            "raw_ingress" if identity == "raw_ingress" => {
                entry.files.len() == 1 && paths.contains(POLYMARKET_WS_INGRESS_ARROW_FILE)
            }
            "book_admission" if identity == "book_admission" => {
                entry.files.len() == 1 && paths.contains("polymarket_book_admission.jsonl")
            }
            "priority" if identity == "priority" => {
                paths.iter().all(|path| PRIORITY_FILES.contains(path))
            }
            _ => false,
        };
        if !expected_files {
            failures.push(format!(
                "journal_files_invalid lane={lane} identity={identity}"
            ));
        }
        if split_strategy
            && lane == "strategy_consumption"
            && ((entry.written == 0
                && (entry.first_sequence.is_some() || entry.last_sequence.is_some()))
                || (entry.written > 0
                    && (entry.first_sequence.is_none() || entry.last_sequence.is_none())))
        {
            failures.push(format!(
                "journal_sequence_bounds_invalid lane={lane} identity={identity}"
            ));
        }
        if lane == "market" && actor {
            if let Some(file) = entry.files.iter().find(|file| file.path == expected_engine) {
                replayable_rows_by_underlying.insert(lower, file.written);
            }
        }
        for file in &entry.files {
            let path = session_dir.join(&file.path);
            let actual_hash = match hash_file(&path) {
                Ok(hash) => Some(hash),
                Err(error) => {
                    if inspection_error.is_none() {
                        inspection_error = Some(
                            anyhow::Error::new(error)
                                .context(format!("inspect capture journal {}", path.display())),
                        );
                    }
                    None
                }
            };
            if file.sha256.as_deref().is_none_or(str::is_empty)
                || actual_hash.as_deref() != file.sha256.as_deref()
            {
                failures.push(format!(
                    "journal_file_hash_invalid lane={lane} identity={identity} path={}",
                    file.path
                ));
            }
        }
    }
    for actor in ACTORS {
        if split_strategy {
            for kind in ["runtime", "chainlink"] {
                let identity = format!("{actor}/{kind}");
                if !identities.contains(&("strategy_consumption", identity.as_str())) {
                    failures.push(format!(
                        "missing_journal lane=strategy_consumption identity={identity}"
                    ));
                }
            }
        } else if !identities.contains(&("strategy_consumption", actor)) {
            failures.push(format!(
                "missing_journal lane=strategy_consumption identity={actor}"
            ));
        }
        if !identities.contains(&("market", actor)) {
            failures.push(format!("missing_journal lane=market identity={actor}"));
        }
    }
    for lane in ["raw_ingress", "book_admission", "priority"] {
        if !identities.contains(&(lane, lane)) {
            failures.push(format!("missing_journal lane={lane} identity={lane}"));
        }
    }
    if entries.len() != expected_count {
        failures.push(format!("journal_count_mismatch actual={}", entries.len()));
    }
    (failures, replayable_rows_by_underlying, inspection_error)
}

fn hash_file(path: &Path) -> std::io::Result<String> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let len = reader.read(&mut buffer)?;
        if len == 0 {
            break;
        }
        hasher.update(&buffer[..len]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}
