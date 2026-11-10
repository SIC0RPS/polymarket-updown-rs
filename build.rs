use std::path::{Path, PathBuf};
use std::process::Command;

fn git_output(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed.to_string())
}

fn emit_git_rerun_hints() {
    println!("cargo:rerun-if-changed=build.rs");
    let Some(git_dir) = git_repo_path(&["rev-parse", "--git-dir"]) else {
        return;
    };
    let common_dir =
        git_repo_path(&["rev-parse", "--git-common-dir"]).unwrap_or_else(|| git_dir.clone());

    emit_rerun_if_exists(git_dir.join("HEAD"));
    emit_rerun_if_exists(git_dir.join("logs").join("HEAD"));
    emit_rerun_if_exists(common_dir.join("packed-refs"));

    if let Some(reference) = git_output(&["symbolic-ref", "-q", "HEAD"]) {
        emit_rerun_if_exists(common_dir.join(&reference));
        emit_rerun_if_exists(common_dir.join("logs").join(reference));
    }
}

fn git_repo_path(args: &[&str]) -> Option<PathBuf> {
    let raw = git_output(args)?;
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        Some(path)
    } else {
        Some(Path::new(".").join(path))
    }
}

fn emit_rerun_if_exists(path: PathBuf) {
    if path.exists() {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn main() {
    emit_git_rerun_hints();

    let commit_sha =
        git_output(&["rev-parse", "--short=7", "HEAD"]).unwrap_or_else(|| "unknown".to_string());
    let commit_subject = git_output(&["show", "-s", "--format=%s", "HEAD"])
        .unwrap_or_else(|| "unknown commit".to_string());

    println!("cargo:rustc-env=APP_COMMIT_SHA={commit_sha}");
    println!("cargo:rustc-env=APP_COMMIT_SUBJECT={commit_subject}");
}
