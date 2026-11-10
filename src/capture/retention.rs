use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetentionClass {
    Preserved,
    RegularFinalized { start_ts_ms: u64 },
}

struct CaptureSession {
    path: PathBuf,
    start_ts_ms: u64,
}

pub async fn prune_capture_sessions(
    base_dir: &str,
    max_sessions: usize,
    min_save_duration_ms: u64,
) -> Result<()> {
    let root = PathBuf::from(base_dir);
    tokio::fs::create_dir_all(&root)
        .await
        .with_context(|| format!("failed to create {}", root.display()))?;
    let mut entries = tokio::fs::read_dir(&root)
        .await
        .with_context(|| format!("failed to read {}", root.display()))?;
    let mut sessions = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if !entry.file_type().await?.is_dir() {
            continue;
        }
        let path = entry.path();
        if let RetentionClass::RegularFinalized { start_ts_ms } =
            classify_capture_session(&path, min_save_duration_ms).await?
        {
            sessions.push(CaptureSession { path, start_ts_ms });
        }
    }
    sessions.sort_by(|left, right| {
        left.start_ts_ms
            .cmp(&right.start_ts_ms)
            .then_with(|| left.path.cmp(&right.path))
    });
    let mut remaining = sessions.len();
    for session in sessions {
        if remaining <= max_sessions {
            break;
        }
        // A metadata candidate is not eviction authority: verify its complete journals now.
        let verification_path = session.path.clone();
        let verification = tokio::task::spawn_blocking(move || {
            super::finalization::finalized_capture_identity(&verification_path)
        })
        .await
        .with_context(|| {
            format!(
                "join retained capture verification {}",
                session.path.display()
            )
        })?;
        match verification {
            Ok(identity) if identity.start_ts_ms == session.start_ts_ms => {}
            Ok(_) => {
                remaining -= 1;
                continue;
            }
            Err(error) if inspection_error(&error) => {
                return Err(error).with_context(|| {
                    format!("verify retained capture {}", session.path.display())
                });
            }
            Err(_) => {
                remaining -= 1;
                continue;
            }
        }
        if !matches!(
            classify_capture_session(&session.path, min_save_duration_ms).await?,
            RetentionClass::RegularFinalized { start_ts_ms } if start_ts_ms == session.start_ts_ms
        ) {
            remaining -= 1;
            continue;
        }
        tracing::warn!(
            target: "capture.retention",
            path = %session.path.display(),
            "capture retention removing verified finalized session"
        );
        tokio::fs::remove_dir_all(&session.path)
            .await
            .with_context(|| format!("remove retained capture {}", session.path.display()))?;
        remaining -= 1;
    }
    Ok(())
}

async fn classify_capture_session(
    path: &Path,
    min_save_duration_ms: u64,
) -> Result<RetentionClass> {
    let marker_path = path.join("capture_incomplete.json");
    match tokio::fs::symlink_metadata(&marker_path).await {
        Ok(_) => return Ok(RetentionClass::Preserved),
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect capture marker {}", marker_path.display()));
        }
    }

    let meta_path = path.join("session_meta.json");
    match tokio::fs::symlink_metadata(&meta_path).await {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(RetentionClass::Preserved);
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect capture metadata {}", meta_path.display()));
        }
    }
    let manifest_path = path.join("capture_artifact_manifest.json");
    match tokio::fs::symlink_metadata(&manifest_path).await {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(RetentionClass::Preserved);
        }
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect capture manifest {}", manifest_path.display()));
        }
    }
    let inspection_path = path.to_path_buf();
    let inspection = tokio::task::spawn_blocking(move || {
        super::finalization::finalized_capture_metadata_identity(&inspection_path)
    })
    .await
    .with_context(|| format!("join capture metadata inspection {}", path.display()))?;
    let identity = match inspection {
        Ok(identity) => identity,
        Err(error) if inspection_error(&error) => {
            return Err(error).with_context(|| format!("inspect capture {}", path.display()));
        }
        Err(_) => return Ok(RetentionClass::Preserved),
    };
    let start_ms = identity.start_ts_ms;
    let end_ms = identity.end_ts_ms;
    let Some(duration_ms) = end_ms.checked_sub(start_ms) else {
        return Ok(RetentionClass::Preserved);
    };
    if duration_ms < min_save_duration_ms {
        return Ok(RetentionClass::Preserved);
    }
    if !has_replayable_engine_state(path).await? {
        return Ok(RetentionClass::Preserved);
    }
    Ok(RetentionClass::RegularFinalized {
        start_ts_ms: start_ms,
    })
}

fn inspection_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.is::<std::io::Error>()
            || cause
                .downcast_ref::<serde_json::Error>()
                .is_some_and(serde_json::Error::is_io)
    })
}

async fn has_replayable_engine_state(path: &Path) -> Result<bool> {
    let mut entries = tokio::fs::read_dir(path)
        .await
        .with_context(|| format!("read replayable engine state in {}", path.display()))?;
    while let Some(entry) = entries
        .next_entry()
        .await
        .with_context(|| format!("enumerate replayable engine state in {}", path.display()))?
    {
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if !file_name.starts_with("polymarket_engine_state_replayable_")
            || !file_name.ends_with(".arrow")
        {
            continue;
        }
        let metadata = entry.metadata().await.with_context(|| {
            format!("inspect replayable engine state {}", entry.path().display())
        })?;
        if metadata.len() > 0 {
            return Ok(true);
        }
    }
    Ok(false)
}
