use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process;

use canonical_ohlc_arrow::{
    append_ohlc_bars, read_all, read_range, read_tail, write_ohlc_bars, OhlcBar,
};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};

#[derive(Parser)]
#[command(name = "canonical_ohlc", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    WriteJson {
        input: PathBuf,
        output: PathBuf,
    },
    AppendJson {
        input: PathBuf,
        output: PathBuf,
    },
    ReadJson {
        path: PathBuf,
    },
    ReadTail {
        path: PathBuf,
        limit: usize,
    },
    ReadRange {
        path: PathBuf,
        start_ts: i64,
        end_ts: i64,
        #[arg(long)]
        limit: Option<usize>,
    },
}

#[derive(Debug, Deserialize)]
struct JsonBar {
    ts: f64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

#[derive(Debug, Serialize)]
struct OutBar {
    ts: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
}

fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli) {
        eprintln!("{err}");
        process::exit(1);
    }
}

fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::WriteJson { input, output } => {
            let raw = load_json(&input)?;
            let bars = raw.iter().map(to_bar).collect::<Result<Vec<_>, _>>()?;
            write_ohlc_bars(&output, &bars).map_err(|err| err.to_string())?;
            Ok(())
        }
        Command::AppendJson { input, output } => {
            let raw = load_json(&input)?;
            let bars = raw.iter().map(to_bar).collect::<Result<Vec<_>, _>>()?;
            append_ohlc_bars(&output, &bars).map_err(|err| err.to_string())?;
            Ok(())
        }
        Command::ReadJson { path } => {
            let bars = read_all(&path).map_err(|err| err.to_string())?;
            write_json(bars)
        }
        Command::ReadTail { path, limit } => {
            let bars = read_tail(&path, limit).map_err(|err| err.to_string())?;
            write_json(bars)
        }
        Command::ReadRange {
            path,
            start_ts,
            end_ts,
            limit,
        } => {
            if start_ts > end_ts {
                return Err("start_ts must be <= end_ts".to_string());
            }
            let bars = read_range(&path, start_ts, end_ts, limit).map_err(|err| err.to_string())?;
            write_json(bars)
        }
    }
}

fn load_json(path: &PathBuf) -> Result<Vec<JsonBar>, String> {
    let payload = fs::read(path).map_err(|err| format!("read failed: {err}"))?;
    serde_json::from_slice(&payload).map_err(|err| format!("invalid json: {err}"))
}

fn to_bar(raw: &JsonBar) -> Result<OhlcBar, String> {
    for (name, value) in [
        ("ts", raw.ts),
        ("open", raw.open),
        ("high", raw.high),
        ("low", raw.low),
        ("close", raw.close),
        ("volume", raw.volume),
    ] {
        if !value.is_finite() {
            return Err(format!("non-finite {name}: {value}"));
        }
    }
    if raw.ts < i64::MIN as f64 || raw.ts > i64::MAX as f64 {
        return Err(format!("ts out of range: {}", raw.ts));
    }
    let ts_ms = raw.ts.trunc() as i64;
    Ok(OhlcBar {
        ts_ms,
        open: raw.open,
        high: raw.high,
        low: raw.low,
        close: raw.close,
        volume: raw.volume,
    })
}

fn write_json(bars: Vec<OhlcBar>) -> Result<(), String> {
    let output: Vec<OutBar> = bars
        .into_iter()
        .map(|bar| OutBar {
            ts: bar.ts_ms,
            open: bar.open,
            high: bar.high,
            low: bar.low,
            close: bar.close,
            volume: bar.volume,
        })
        .collect();
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    serde_json::to_writer(&mut handle, &output).map_err(|err| err.to_string())?;
    handle.write_all(b"\n").map_err(|err| err.to_string())?;
    Ok(())
}
