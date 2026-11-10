use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use serde::Serialize;

pub(super) fn write_json_file<T>(path: &str, value: &T) -> Result<()>
where
    T: Serialize + ?Sized,
{
    let bytes = serde_json::to_vec(value)?;
    write_bytes_atomic(Path::new(path), &bytes)
}

pub fn write_bytes_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    let mut tmp_name = path.as_os_str().to_os_string();
    tmp_name.push(".tmp");
    let tmp_path = PathBuf::from(tmp_name);
    fs::write(&tmp_path, bytes)?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}
