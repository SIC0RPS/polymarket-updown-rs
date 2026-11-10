use super::*;

pub(crate) fn validate_event(record: &CanonicalEventRecord) -> Result<()> {
    if record.event_id.trim().is_empty() {
        bail!("event_id is required");
    }
    if record.run_id.trim().is_empty() {
        bail!("run_id is required");
    }
    if record.subject.trim().is_empty() {
        bail!("subject is required");
    }
    validate_refs_map(&record.refs)?;
    validate_metrics_map(&record.metrics)?;
    validate_bounded_shallow_map(&record.context, "context")?;
    let encoded =
        serde_json::to_vec(record).context("failed to serialize canonical event for validation")?;
    if encoded.contains(&b'\n') {
        bail!("canonical event must serialize to a single line");
    }
    if encoded.len() + 1 > MAX_EVENT_LINE_BYTES {
        bail!("canonical event exceeds line size cap");
    }
    Ok(())
}

pub(crate) fn validate_refs_map(value: &Value) -> Result<()> {
    let Some(object) = value.as_object() else {
        bail!("refs must be an object");
    };
    if object.len() > MAX_OBJECT_KEYS {
        bail!("refs exceeds key cap");
    }
    for (key, value) in object {
        if key.trim().is_empty() {
            bail!("refs key must not be empty");
        }
        match value {
            Value::String(_) | Value::Bool(_) | Value::Null => {}
            _ => bail!("refs values must be scalar string/bool/null"),
        }
    }
    Ok(())
}

pub(crate) fn validate_metrics_map(value: &Value) -> Result<()> {
    let Some(object) = value.as_object() else {
        bail!("metrics must be an object");
    };
    if object.len() > MAX_OBJECT_KEYS {
        bail!("metrics exceeds key cap");
    }
    for (key, value) in object {
        if key.trim().is_empty() {
            bail!("metrics key must not be empty");
        }
        if !value.is_number() {
            bail!("metrics values must be numeric");
        }
    }
    Ok(())
}

pub(crate) fn validate_bounded_shallow_map(value: &Value, field: &str) -> Result<()> {
    fn visit(value: &Value, depth: usize) -> Result<()> {
        if depth > MAX_VALUE_DEPTH {
            bail!("context exceeds depth cap");
        }
        match value {
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => Ok(()),
            Value::Array(values) => {
                if values.len() > MAX_ARRAY_LEN {
                    bail!("context array exceeds length cap");
                }
                for value in values {
                    visit(value, depth + 1)?;
                }
                Ok(())
            }
            Value::Object(map) => {
                if map.len() > MAX_OBJECT_KEYS {
                    bail!("context object exceeds key cap");
                }
                for (key, value) in map {
                    if key.trim().is_empty() {
                        bail!("context key must not be empty");
                    }
                    visit(value, depth + 1)?;
                }
                Ok(())
            }
        }
    }

    let Some(object) = value.as_object() else {
        bail!("{field} must be an object");
    };
    if object.len() > MAX_OBJECT_KEYS {
        bail!("{field} exceeds key cap");
    }
    visit(value, 0)
}
