use super::*;

pub(crate) fn rotate_active_ledger_if_needed(active_path: &Path) -> Result<bool> {
    let metadata = match fs::metadata(active_path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => {
            return Err(err).with_context(|| format!("failed to stat {}", active_path.display()))
        }
    };
    let modified_ms = metadata
        .modified()
        .ok()
        .and_then(system_time_ms)
        .unwrap_or_else(super::write::now_ms);
    let should_rotate = metadata.len() >= EVENT_ROTATE_MAX_BYTES
        || day_key(modified_ms) != day_key(super::write::now_ms());
    if !should_rotate {
        return Ok(false);
    }

    fs::create_dir_all(EVENTS_ARCHIVE_DIR)
        .with_context(|| format!("failed to create {EVENTS_ARCHIVE_DIR}"))?;
    let archive_base = format!("events-{}.{}", day_key(modified_ms), GZIP_SUFFIX);
    let archive_path = unique_archive_path(Path::new(EVENTS_ARCHIVE_DIR).join(archive_base));
    let output = Command::new("gzip")
        .arg("-c")
        .arg(active_path)
        .output()
        .context("failed to execute gzip for ledger rotation")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("gzip rotation failed: {stderr}");
    }
    let tmp_path = archive_path.with_extension("tmp");
    fs::write(&tmp_path, output.stdout)
        .with_context(|| format!("failed to write {}", tmp_path.display()))?;
    fs::rename(&tmp_path, &archive_path)
        .with_context(|| format!("failed to move archive to {}", archive_path.display()))?;
    fs::remove_file(active_path)
        .with_context(|| format!("failed to remove rotated {}", active_path.display()))?;
    Ok(true)
}

pub(crate) fn unique_archive_path(initial: PathBuf) -> PathBuf {
    if !initial.exists() {
        return initial;
    }
    let stem = initial
        .file_stem()
        .and_then(OsStr::to_str)
        .unwrap_or("events")
        .to_string();
    let parent = initial.parent().unwrap_or_else(|| Path::new("."));
    for idx in 1.. {
        let candidate = parent.join(format!("{stem}-{idx}.{GZIP_SUFFIX}"));
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!()
}

pub(crate) fn day_key(ts_ms: u64) -> String {
    let days = ts_ms / 86_400_000;
    format!("{days}")
}

pub(crate) fn system_time_ms(value: SystemTime) -> Option<u64> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis() as u64)
}
