use std::io::{self, Write};
use std::path::PathBuf;
use std::process;

use canonical_ohlc_resync::{
    audit_continuity, build_resync_http_client, ensure_window, parse_interval_ms, repair_gaps,
    EnsureConfig, EnsureOutcome, ResyncError, ResyncErrorKind,
};
use clap::{Parser, Subcommand};
use serde::Serialize;

#[derive(Parser)]
#[command(name = "canonical_ohlc_resync", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Audit {
        arrow_path: PathBuf,
        timeframe: String,
        #[arg(long)]
        window: Option<usize>,
    },
    Repair {
        arrow_path: PathBuf,
        symbol: String,
        timeframe: String,
        gap_start_ts_ms: i64,
        gap_end_ts_ms: i64,
        #[arg(long)]
        rest_base_url: String,
    },
    Ensure {
        arrow_path: PathBuf,
        symbol: String,
        timeframe: String,
        #[arg(long)]
        rest_base_url: String,
        #[arg(long)]
        window: Option<usize>,
        #[arg(long)]
        latest_closed_ts_ms: Option<i64>,
        #[arg(long)]
        target_bars: Option<usize>,
        #[arg(long, default_value_t = 3)]
        max_gap_repairs: usize,
    },
}

#[derive(Serialize)]
struct ErrorOutput<'a> {
    status: &'a str,
    kind: ResyncErrorKind,
    error: &'a str,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli).await {
        write_error(&err);
        process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), ResyncError> {
    match cli.command {
        Command::Audit {
            arrow_path,
            timeframe,
            window,
        } => {
            let interval_ms = parse_interval_ms(&timeframe)
                .ok_or_else(|| ResyncError::invalid_input("invalid timeframe"))?;
            let report = audit_continuity(&arrow_path, interval_ms, window)?;
            write_json(&report).map_err(ResyncError::internal)?;
        }
        Command::Repair {
            arrow_path,
            symbol,
            timeframe,
            gap_start_ts_ms,
            gap_end_ts_ms,
            rest_base_url,
        } => {
            let interval_ms = parse_interval_ms(&timeframe)
                .ok_or_else(|| ResyncError::invalid_input("invalid timeframe"))?;
            let client = build_resync_http_client();
            let outcome = repair_gaps(
                &client,
                &arrow_path,
                &symbol,
                &timeframe,
                &rest_base_url,
                interval_ms,
                gap_start_ts_ms,
                gap_end_ts_ms,
            )
            .await?;
            write_json(&outcome).map_err(ResyncError::internal)?;
        }
        Command::Ensure {
            arrow_path,
            symbol,
            timeframe,
            rest_base_url,
            window,
            latest_closed_ts_ms,
            target_bars,
            max_gap_repairs,
        } => {
            let interval_ms = parse_interval_ms(&timeframe)
                .ok_or_else(|| ResyncError::invalid_input("invalid timeframe"))?;
            let client = build_resync_http_client();
            let config = EnsureConfig {
                arrow_path: &arrow_path,
                symbol: &symbol,
                timeframe: &timeframe,
                interval_ms,
                rest_base_url: &rest_base_url,
                window,
                latest_closed_ts_ms,
                fetch_bars: target_bars,
                required_bars: target_bars,
                max_gap_repairs,
            };
            let outcome: EnsureOutcome = ensure_window(&client, config).await?;
            write_json(&outcome).map_err(ResyncError::internal)?;
        }
    }
    Ok(())
}

fn write_error(err: &ResyncError) {
    let output = ErrorOutput {
        status: "error",
        kind: err.kind,
        error: err.message.as_str(),
    };
    let _ = write_json(&output);
}

fn write_json<T: Serialize>(payload: &T) -> Result<(), String> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer(&mut handle, payload).map_err(|err| err.to_string())?;
    handle.write_all(b"\n").map_err(|err| err.to_string())?;
    Ok(())
}
