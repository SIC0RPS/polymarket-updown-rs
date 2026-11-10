use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ObservabilityConfig {
    pub enabled: bool,
    pub logging: ObservabilityLoggingConfig,
    pub dashboard: ObservabilityDashboardConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ObservabilityLoggingConfig {
    pub level: String,
    pub stdout: bool,
    pub format: LogFormat,
    pub dir: String,
    pub runtime_log_max_bytes: u64,
    pub runtime_log_buffered_lines_limit: usize,
    pub flood_control_enabled: bool,
    pub flood_summary_interval_secs: u64,
    pub flood_recovery_timeout_secs: u64,
    pub flood_max_tracked_signatures: usize,
    pub action_log_enabled: bool,
    pub action_log_path: String,
    pub action_log_max_bytes: u64,
    pub action_log_flush_every: usize,
    pub log_ticks: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ObservabilityDashboardConfig {
    pub enabled: bool,
    pub bind_host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    Json,
    Pretty,
}
