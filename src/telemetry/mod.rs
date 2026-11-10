mod fixed_name_log;
mod flood_control;

use std::path::Path;

use anyhow::{Context, Result};
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::filter::Directive;
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::reload;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::EnvFilter;

use crate::config::{LogFormat, ObservabilityLoggingConfig};
use fixed_name_log::FixedNameCappedFile;
pub(crate) use flood_control::{FloodControlConfig, FloodControlLayer};
pub use flood_control::{FloodControlHandle, FloodControlSnapshot};

const NOISY_TARGET_SUPPRESSIONS: &[&str] = &[
    // SDK emits repeated unknown-field warnings for Gamma response drift (e.g. feeType=null).
    "polymarket_client_sdk_v2::serde_helpers=error",
    // HTTP/2 internals are very noisy at debug level and drown trading logs.
    "h2=warn",
    "hyper=warn",
    "reqwest=info",
    "rustls=warn",
];

pub struct TelemetryGuards {
    pub filter_reload: reload::Handle<EnvFilter, tracing_subscriber::Registry>,
    pub flood_control: FloodControlHandle,
    _file_guard: WorkerGuard,
    _stdout_guard: Option<WorkerGuard>,
    _flood_control_guard: flood_control::FloodControlGuard,
}

pub fn init(cfg: &ObservabilityLoggingConfig) -> Result<TelemetryGuards> {
    let default_level = cfg.level.trim();
    let filter = build_filter_from_env_or_level(default_level)?;

    let (filter_layer, filter_reload) = reload::Layer::new(filter);

    let log_dir = Path::new(cfg.dir.trim());
    std::fs::create_dir_all(log_dir)
        .with_context(|| format!("failed to create log dir {}", log_dir.display()))?;
    remove_if_exists(&log_dir.join("runtime.log"))?;

    // Hard repo rule: runtime logs keep one fixed filename and must never fan out into
    // timestamped/random files. When the cap is hit, truncate the same file in place.
    let file_appender =
        FixedNameCappedFile::new(log_dir.join("runtime.log"), cfg.runtime_log_max_bytes)
            .with_context(|| format!("failed to open runtime log {}", log_dir.display()))?;
    let (flood_control_layer, flood_control) = FloodControlLayer::new(FloodControlConfig {
        enabled: cfg.flood_control_enabled,
        summary_interval: std::time::Duration::from_secs(cfg.flood_summary_interval_secs),
        recovery_timeout: std::time::Duration::from_secs(cfg.flood_recovery_timeout_secs),
        max_tracked_signatures: cfg.flood_max_tracked_signatures,
    });
    let (file_writer, file_guard) = NonBlockingBuilder::default()
        .lossy(true)
        .buffered_lines_limit(cfg.runtime_log_buffered_lines_limit)
        .thread_name("pmhft-runtime-log")
        .finish(file_appender);
    let mut sink_counters = vec![file_writer.error_counter()];

    let (stdout_writer, stdout_guard) = if cfg.stdout {
        let (stdout_writer, stdout_guard) = NonBlockingBuilder::default()
            .lossy(true)
            .buffered_lines_limit(cfg.runtime_log_buffered_lines_limit)
            .thread_name("pmhft-stdout-log")
            .finish(std::io::stdout());
        sink_counters.push(stdout_writer.error_counter());
        (Some(stdout_writer), Some(stdout_guard))
    } else {
        (None, None)
    };
    flood_control.set_sink_error_counters(sink_counters);

    match cfg.format {
        LogFormat::Json => {
            let file_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_writer(file_writer)
                .with_ansi(false)
                .with_target(false);
            let registry = tracing_subscriber::registry()
                .with(filter_layer)
                .with(flood_control_layer.clone())
                .with(file_layer);

            if let Some(stdout_writer) = stdout_writer {
                let stdout_layer = tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(stdout_writer)
                    .with_ansi(false)
                    .with_target(false);
                registry.with(stdout_layer).try_init()
            } else {
                registry.try_init()
            }
        }
        LogFormat::Pretty => {
            let file_layer = tracing_subscriber::fmt::layer()
                .pretty()
                .with_writer(file_writer)
                .with_ansi(false)
                .with_target(false);
            let registry = tracing_subscriber::registry()
                .with(filter_layer)
                .with(flood_control_layer)
                .with(file_layer);

            if let Some(stdout_writer) = stdout_writer {
                let stdout_layer = tracing_subscriber::fmt::layer()
                    .pretty()
                    .with_writer(stdout_writer)
                    .with_ansi(true)
                    .with_target(false);
                registry.with(stdout_layer).try_init()
            } else {
                registry.try_init()
            }
        }
    }
    .context("failed to initialize tracing subscriber")?;
    let flood_control_guard = flood_control.start_housekeeping();

    Ok(TelemetryGuards {
        filter_reload,
        flood_control,
        _file_guard: file_guard,
        _stdout_guard: stdout_guard,
        _flood_control_guard: flood_control_guard,
    })
}

fn build_filter_from_env_or_level(default_level: &str) -> Result<EnvFilter> {
    let from_env = EnvFilter::try_from_default_env();
    let filter = match from_env {
        // Respect explicit RUST_LOG as the source of truth for base verbosity, but
        // always layer repo-wide suppressions for vendor noise that drowns trading logs.
        Ok(filter) => apply_noise_suppressions(filter),
        Err(_) => {
            let filter = EnvFilter::try_new(default_level)
                .with_context(|| format!("invalid log filter `{default_level}`"))?;
            apply_noise_suppressions(filter)
        }
    };
    Ok(filter)
}

fn apply_noise_suppressions(mut filter: EnvFilter) -> EnvFilter {
    for directive in NOISY_TARGET_SUPPRESSIONS {
        if let Ok(parsed) = directive.parse::<Directive>() {
            filter = filter.add_directive(parsed);
        }
    }
    filter
}

fn remove_if_exists(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err)
            .with_context(|| format!("failed to remove stale runtime log {}", path.display())),
    }
}
