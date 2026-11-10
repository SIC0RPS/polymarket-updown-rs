use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct BuildInfo {
    pub commit_sha: String,
    pub commit_subject: String,
    pub dirty_worktree: bool,
    pub changed_paths: Vec<String>,
}

static BUILD_INFO_CACHE: OnceLock<BuildInfo> = OnceLock::new();

pub fn current_build_info() -> BuildInfo {
    BUILD_INFO_CACHE.get_or_init(refreshed_build_info).clone()
}

fn refreshed_build_info() -> BuildInfo {
    let env_commit_sha = option_env!("APP_COMMIT_SHA")
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("unknown")
        .to_string();
    let env_commit_subject = option_env!("APP_COMMIT_SUBJECT")
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("unknown commit")
        .to_string();

    let startup = BuildInfo {
        commit_sha: env_commit_sha,
        commit_subject: env_commit_subject,
        dirty_worktree: false,
        changed_paths: Vec::new(),
    };
    let Some(repo_root) = find_repo_root_from_cwd() else {
        return startup;
    };

    let changed_paths = git_output(&repo_root, &["status", "--porcelain"])
        .map(|status| {
            status
                .lines()
                .filter_map(parse_status_path)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let dirty_worktree = !changed_paths.is_empty();

    BuildInfo {
        dirty_worktree,
        changed_paths,
        ..startup
    }
}

fn git_output(repo_root: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo_root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    Some(value.trim().to_string())
}

fn parse_status_path(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    let path = if trimmed.len() > 3 {
        &trimmed[3..]
    } else {
        trimmed
    };
    let path = path
        .rsplit_once(" -> ")
        .map(|(_, after)| after)
        .unwrap_or(path)
        .trim();
    (!path.is_empty()).then(|| path.to_string())
}

fn find_repo_root_from_cwd() -> Option<PathBuf> {
    let mut cur = std::env::current_dir().ok()?;
    loop {
        if cur.join("Cargo.toml").is_file() && cur.join("config/main.toml").is_file() {
            return Some(cur);
        }
        if !cur.pop() {
            return None;
        }
    }
}
