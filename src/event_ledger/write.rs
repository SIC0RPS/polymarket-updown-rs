use super::*;

use super::read::record_appended_event;
use super::rotation::rotate_active_ledger_if_needed;

pub struct RunIdProvider;

impl RunIdProvider {
    fn state() -> &'static Mutex<Option<String>> {
        CURRENT_RUN_ID.get_or_init(|| Mutex::new(None))
    }

    pub fn current() -> Option<String> {
        if let Some(value) = Self::state().lock().expect("run id state poisoned").clone() {
            return Some(value);
        }
        let from_disk = fs::read_to_string(CURRENT_RUN_ID_PATH)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if let Some(value) = from_disk.clone() {
            *Self::state().lock().expect("run id state poisoned") = Some(value);
        }
        from_disk
    }

    pub fn start_new(ts_ms: u64) -> Result<String> {
        let run_id = format!(
            "run_{}_{}_{}",
            ts_ms,
            std::process::id(),
            EVENT_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        );
        super::ensure_runtime_layout()?;
        fs::write(CURRENT_RUN_ID_PATH, &run_id)
            .with_context(|| format!("failed to write {CURRENT_RUN_ID_PATH}"))?;
        *Self::state().lock().expect("run id state poisoned") = Some(run_id.clone());
        Ok(run_id)
    }

    pub fn clear() -> Result<()> {
        let _ = fs::remove_file(CURRENT_RUN_ID_PATH);
        *Self::state().lock().expect("run id state poisoned") = None;
        Ok(())
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn append_jsonl_record<T: Serialize>(path_str: &str, record: &T) -> Result<()> {
    let path = Path::new(path_str);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    let mut bytes = serde_json::to_vec(record).context("failed to serialize jsonl record")?;
    bytes.push(b'\n');
    file.write_all(&bytes)
        .with_context(|| format!("failed to append {}", path.display()))?;
    file.flush()
        .with_context(|| format!("failed to flush {}", path.display()))?;
    Ok(())
}

pub fn append_event(record: &CanonicalEventRecord) -> Result<()> {
    let path = Path::new(EVENTS_JSONL_PATH);
    validate_event(record)?;
    let rotated = rotate_active_ledger_if_needed(path)?;

    let bytes = serde_json::to_vec(record).context("failed to serialize canonical event")?;
    if bytes.len() + 1 > MAX_EVENT_LINE_BYTES {
        bail!("canonical event exceeds line size cap");
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    file.write_all(&bytes)
        .with_context(|| format!("failed to append {}", path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("failed to append newline {}", path.display()))?;
    file.flush()
        .with_context(|| format!("failed to flush {}", path.display()))?;
    file.sync_data()
        .with_context(|| format!("failed to sync {}", path.display()))?;
    record_appended_event(record, path, rotated);
    Ok(())
}

pub fn current_run_id() -> Result<String> {
    RunIdProvider::current().context("run_id missing; runtime_start must initialize RunIdProvider")
}

pub fn make_event_id(
    event_type: CanonicalEventType,
    ts_ms: u64,
    sequence: impl Into<String>,
) -> String {
    let sequence = sequence.into();
    let seq = EVENT_ID_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let suffix = Some(sequence.as_str())
        .map(sanitize_id_component)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| seq.to_string());
    format!("evt_{}_{}_{}", event_type.as_str(), ts_ms, suffix)
}

pub fn snapshot_config_path(path: &Path) -> Result<ConfigSnapshot> {
    let raw_toml = fs::read_to_string(path)
        .with_context(|| format!("failed to read config snapshot {}", path.display()))?;
    let parsed: toml::Value =
        toml::from_str(&raw_toml).context("failed to parse TOML for config snapshot")?;
    let canonical_json = canonical_toml_value(&parsed);
    let canonical_bytes =
        serde_json::to_vec(&canonical_json).context("failed to serialize canonical config")?;
    let mut hasher = Sha256::new();
    hasher.update(&canonical_bytes);
    let hash = format!("{:x}", hasher.finalize());
    let object_path = Path::new(CONFIG_OBJECTS_DIR).join(format!("{hash}.toml"));
    if let Some(parent) = object_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    if !object_path.exists() {
        fs::write(&object_path, &raw_toml)
            .with_context(|| format!("failed to write {}", object_path.display()))?;
    }
    Ok(ConfigSnapshot { hash })
}

fn sanitize_id_component(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches('_')
        .to_string()
}

fn canonical_toml_value(value: &toml::Value) -> Value {
    match value {
        toml::Value::String(v) => Value::String(v.clone()),
        toml::Value::Integer(v) => json!(v),
        toml::Value::Float(v) => json!(v),
        toml::Value::Boolean(v) => json!(v),
        toml::Value::Datetime(v) => Value::String(v.to_string()),
        toml::Value::Array(values) => {
            Value::Array(values.iter().map(canonical_toml_value).collect())
        }
        toml::Value::Table(values) => {
            let mut map = BTreeMap::new();
            for (key, value) in values {
                map.insert(key.clone(), canonical_toml_value(value));
            }
            serde_json::to_value(map).unwrap_or(Value::Null)
        }
    }
}
