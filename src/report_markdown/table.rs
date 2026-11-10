use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

pub(super) fn document(title: &str) -> String {
    format!("# {}\n", escape(title))
}

pub(super) fn section(body: &mut String, title: &str) {
    body.push_str("\n## ");
    body.push_str(&escape(title));
    body.push('\n');
}

pub(super) fn table(body: &mut String, columns: &[&str], rows: Vec<Vec<String>>) {
    body.push_str("\n| ");
    body.push_str(
        &columns
            .iter()
            .map(|column| escape(column))
            .collect::<Vec<_>>()
            .join(" | "),
    );
    body.push_str(" |\n| ");
    body.push_str(
        &columns
            .iter()
            .map(|_| "---")
            .collect::<Vec<_>>()
            .join(" | "),
    );
    body.push_str(" |\n");
    for row in rows {
        body.push_str("| ");
        body.push_str(
            &row.iter()
                .map(|value| escape(value))
                .collect::<Vec<_>>()
                .join(" | "),
        );
        body.push_str(" |\n");
    }
}

pub(super) fn display_option(value: Option<&str>) -> String {
    value
        .filter(|value| !value.is_empty())
        .unwrap_or("NA")
        .to_string()
}

pub(super) fn display_units_6dp(value: i128) -> String {
    let negative = value < 0;
    let magnitude = value.unsigned_abs();
    let whole = magnitude / 1_000_000;
    let fraction = magnitude % 1_000_000;
    format!("{}{whole}.{fraction:06}", if negative { "-" } else { "+" })
}

pub(super) fn display_quantity_units_6dp(value: u128) -> String {
    let whole = value / 1_000_000;
    let fraction = value % 1_000_000;
    format!("{whole}.{fraction:06}")
}

pub(super) fn write(path: &Path, body: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let tmp = path.with_extension("md.tmp");
    fs::write(&tmp, body).with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("replace {}", path.display()))
}

fn escape(value: &str) -> String {
    value.replace('|', "/").replace(['\r', '\n'], " ")
}
