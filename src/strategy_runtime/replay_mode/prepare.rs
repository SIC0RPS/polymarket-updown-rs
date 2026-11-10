use std::collections::BTreeMap;
use std::io::Write;

use crate::capture::arrow::StrategyConsumptionMergedArrowReader;
use crate::strategy_runtime::backtest_live_latency::ReplayExecutionTimingModel;

use super::candidate_runner::OwnedCandidateInputs;
use super::*;

#[path = "prepare/consumption.rs"]
mod consumption;

use consumption::{validate_strategy_consumption, StrategyConsumptionSourceSet};

pub fn prepare_backtest_replay_for_capture(
    capture: &Path,
    symbols: &[String],
    execution_timing: &ReplayExecutionTimingModel,
    prepare_artifacts: bool,
    force: bool,
) -> Result<Vec<String>> {
    let finalized = finalized_capture_identity(capture)?;
    let consumption_by_symbol = validate_strategy_consumption(capture, &finalized)?;
    let mut requested = symbols
        .iter()
        .map(|symbol| symbol.to_ascii_lowercase())
        .collect::<Vec<_>>();
    anyhow::ensure!(
        requested.iter().all(|symbol| crate::config::VALID_ASSETS
            .iter()
            .any(|expected| expected.eq_ignore_ascii_case(symbol))),
        "replay request contains an unsupported symbol"
    );
    requested.sort();
    requested.dedup();
    let results = requested
        .par_iter()
        .map(|symbol| {
            let result = (|| -> Result<()> {
                let consumption = consumption_by_symbol
                    .get(symbol)
                    .with_context(|| format!("no strategy consumption for symbol={symbol}"))?;
                if prepare_artifacts && force {
                    prepare_backtest_replay_symbol(capture, symbol, true, consumption)?;
                }
                match prepare_backtest_replay_symbol(capture, symbol, false, consumption) {
                    Ok(()) => {}
                    Err(validation_error) if prepare_artifacts && !force => {
                        let _ = writeln!(
                            std::io::stdout().lock(),
                            "PREPARED_REPLAY rebuild symbol={symbol} reason={validation_error:#}"
                        );
                        prepare_backtest_replay_symbol(capture, symbol, true, consumption)
                            .with_context(|| format!(
                                "rebuild incompatible prepared replay symbol={symbol} after validation error: {validation_error:#}"
                            ))?;
                        prepare_backtest_replay_symbol(capture, symbol, false, consumption)?;
                    }
                    Err(error) => return Err(error),
                }
                let tape = PackedReplayTape::load(capture, symbol)
                    .with_context(|| format!("load prepared replay tape symbol={symbol}"))?;
                anyhow::ensure!(
                    !PackedReplayTape::prepared_market_lifecycles(capture, symbol)?.is_empty(),
                    "prepared market lifecycle missing symbol={symbol}"
                );
                OwnedCandidateInputs::load(
                    capture,
                    std::slice::from_ref(symbol),
                    BTreeMap::from([(symbol.clone(), tape)]),
                    execution_timing,
                )?;
                Ok(())
            })();
            (symbol, result)
        })
        .collect::<Vec<_>>();
    let mut qualified = Vec::with_capacity(results.len());
    for (symbol, result) in results {
        match result {
            Ok(()) => qualified.push(symbol.clone()),
            Err(error) if !prepare_artifacts => {
                return Err(error).with_context(|| {
                    format!(
                        "frozen replay symbol changed capture={} symbol={symbol}",
                        capture.display()
                    )
                });
            }
            Err(error) => {
                let _ = writeln!(
                    std::io::stderr().lock(),
                    "PREPARED_REPLAY symbol_unavailable capture={} symbol={symbol} reason={error:#}",
                    capture.display()
                );
            }
        }
    }
    if qualified.is_empty() {
        let _ = writeln!(
            std::io::stderr().lock(),
            "PREPARED_REPLAY capture_unavailable capture={} reason=no_qualified_requested_symbols",
            capture.display()
        );
    }
    Ok(qualified)
}

fn source_matches(left: &SourceFileManifest, right: &SourceFileManifest) -> bool {
    left.path == right.path && left.size_bytes == right.size_bytes && left.sha256 == right.sha256
}

fn validate_prepared_sources(
    capture: &Path,
    symbol: &str,
    manifest: &PreparedReplayManifest,
) -> Result<()> {
    let engine_path = crate::capture::arrow::engine_arrow_path(capture, symbol);
    let engine_source = SourceFileManifest::capture(&engine_path)?;
    anyhow::ensure!(
        source_matches(&manifest.engine_source, &engine_source),
        "prepared replay engine source changed symbol={symbol}"
    );
    let resolution_path =
        crate::capture::polymarket_resolution::polymarket_resolution_arrow_path(capture, symbol);
    let resolution_source = if resolution_path.exists() {
        Some(SourceFileManifest::capture(&resolution_path)?)
    } else {
        None
    };
    anyhow::ensure!(
        match (&manifest.resolution_source, &resolution_source) {
            (Some(left), Some(right)) => source_matches(left, right),
            (None, None) => true,
            _ => false,
        },
        "prepared replay resolution source changed symbol={symbol}"
    );
    Ok(())
}

pub fn validate_prepared_replay_sources_for_optimizer(
    capture: &Path,
    symbols: &[String],
) -> Result<()> {
    symbols.par_iter().try_for_each(|symbol| {
        let symbol = crate::capture::arrow::normalize_capture_symbol(symbol);
        let out_dir = prepared_symbol_dir(capture, &symbol);
        let manifest_path = out_dir.join(MANIFEST_FILE);
        let manifest = PreparedReplayManifest::read(&manifest_path)?;
        anyhow::ensure!(
            manifest.is_complete(),
            "prepared replay manifest is incomplete at {}",
            manifest_path.display()
        );
        anyhow::ensure!(
            manifest.symbol == symbol,
            "prepared replay manifest symbol mismatch expected={symbol} actual={}",
            manifest.symbol
        );
        let strategy_sources = manifest
            .strategy_sources()
            .context("prepared replay strategy consumption source contract is invalid")?;
        for source in strategy_sources {
            let current = SourceFileManifest::capture(Path::new(&source.path))?;
            anyhow::ensure!(
                source_matches(source, &current),
                "prepared replay strategy consumption source changed symbol={symbol}"
            );
        }
        validate_prepared_sources(capture, &symbol, &manifest)
    })
}

fn prepare_backtest_replay_symbol(
    capture: &Path,
    symbol: &str,
    force: bool,
    consumption: &StrategyConsumptionSourceSet,
) -> Result<()> {
    let symbol = crate::capture::arrow::normalize_capture_symbol(symbol);
    let out_dir = prepared_symbol_dir(capture, &symbol);
    let manifest_path = out_dir.join(MANIFEST_FILE);
    if !force {
        let manifest = PreparedReplayManifest::read(&manifest_path)?;
        anyhow::ensure!(
            manifest.is_complete(),
            "prepared replay manifest is incomplete at {}",
            manifest_path.display()
        );
        anyhow::ensure!(
            manifest.symbol == symbol,
            "prepared replay manifest symbol mismatch expected={symbol} actual={}",
            manifest.symbol
        );
        anyhow::ensure!(
            manifest.strategy_consumption_rows == consumption.rows,
            "prepared replay strategy consumption row mismatch expected={} actual={}",
            consumption.rows,
            manifest.strategy_consumption_rows
        );
        let manifest_sources = manifest
            .strategy_sources()
            .context("prepared replay strategy consumption source contract is invalid")?;
        anyhow::ensure!(
            manifest_sources.len() == consumption.sources.len()
                && manifest_sources
                    .iter()
                    .zip(&consumption.sources)
                    .all(|(left, right)| source_matches(left, &right.manifest)),
            "prepared replay strategy consumption source changed symbol={symbol}"
        );
        validate_prepared_sources(capture, &symbol, &manifest)?;
        manifest.verify_prepared_payload(&out_dir)?;
        if let Err(error) = writeln!(
            std::io::stdout().lock(),
            "PREPARED_REPLAY skip symbol={} dir={}",
            symbol,
            out_dir.display()
        ) {
            let _ = writeln!(
                std::io::stderr().lock(),
                "PREPARED_REPLAY progress_output_failed phase=skip reason={error}"
            );
        }
        return Ok(());
    }

    let lock_path = out_dir.with_extension("prepare.lock");
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let _lock = acquire_prepare_lock(&lock_path)?;
    build_prepared_replay_symbol(capture, &symbol, &out_dir, consumption)
}

pub(super) fn acquire_prepare_lock(lock_path: &Path) -> Result<std::fs::File> {
    use std::os::fd::AsRawFd;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(lock_path)
        .with_context(|| format!("open prepared replay lock {}", lock_path.display()))?;
    // SAFETY: `file` owns a valid descriptor and remains alive for the preparation build.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| {
            format!(
                "prepared replay already building at {}",
                lock_path.display()
            )
        });
    }
    Ok(file)
}

fn build_prepared_replay_symbol(
    capture: &Path,
    symbol: &str,
    out_dir: &Path,
    consumption: &StrategyConsumptionSourceSet,
) -> Result<()> {
    let engine_path = crate::capture::arrow::engine_arrow_path(capture, symbol);
    let resolution_path =
        crate::capture::polymarket_resolution::polymarket_resolution_arrow_path(capture, symbol);
    anyhow::ensure!(
        engine_path.exists(),
        "missing engine Arrow for prepared replay: {}",
        engine_path.display()
    );
    let staging = out_dir.with_file_name(format!(".building-{symbol}"));
    let _ = replayable_engine_state_line();
    if staging.exists() {
        std::fs::remove_dir_all(&staging)
            .with_context(|| format!("remove {}", staging.display()))?;
    }
    std::fs::create_dir_all(&staging).with_context(|| format!("create {}", staging.display()))?;

    let mut writer = PreparedReplayWriter::create(&staging)?;
    let mut engine_reader = EngineArrowReader::open(&engine_path, None)?;
    let mut resolution_reader = if resolution_path.exists() {
        Some(
            crate::capture::polymarket_resolution::PolymarketResolutionArrowReader::open(
                &resolution_path,
            )?,
        )
    } else {
        None
    };
    while let Some(row) = engine_reader.next_event()? {
        validate_replay_symbol_scope(&symbol, &row.asset_id, row.payload.as_ref())?;
        writer.push_engine(
            row.session_ts_ms,
            std::sync::Arc::unwrap_or_clone(row.payload),
        )?;
    }
    let mut projected_resolution_conditions = std::collections::HashSet::new();
    if let Some(reader) = resolution_reader.as_mut() {
        while let Some(row) = reader.next_event()? {
            anyhow::ensure!(
                crate::capture::arrow::normalize_capture_symbol(&row.underlying) == symbol,
                "prepared Polymarket resolution row symbol mismatch expected={} actual={}",
                symbol,
                row.underlying
            );
            anyhow::ensure!(
                projected_resolution_conditions.insert(row.condition_id),
                "duplicate Polymarket resolution source condition={}",
                row.condition_id
            );
            writer.push_resolution(row)?;
        }
    }

    let consumption_sources = consumption
        .sources
        .iter()
        .map(|source| {
            (
                Path::new(&source.manifest.path).to_path_buf(),
                source.expected_actor,
                source.expected_kind,
            )
        })
        .collect::<Vec<_>>();
    let mut reader = StrategyConsumptionMergedArrowReader::open(&consumption_sources)?;
    while let Some((record, _payload)) = reader.next_event()? {
        if record.actor_symbol.as_str().eq_ignore_ascii_case(symbol) {
            writer.push_strategy_consumption(&record)?;
        }
    }

    let stats = writer.finish()?;
    anyhow::ensure!(
        stats.strategy_consumption_rows == consumption.rows && consumption.rows > 0,
        "prepared replay strategy consumption rows mismatch symbol={symbol} expected={} actual={}",
        consumption.rows,
        stats.strategy_consumption_rows
    );
    let manifest = PreparedReplayManifest {
        schema_version: PREPARED_SCHEMA_VERSION,
        completed: true,
        symbol: symbol.to_string(),
        engine_source: SourceFileManifest::capture(&engine_path)?,
        strategy_consumption_source: None,
        strategy_consumption_sources: consumption
            .sources
            .iter()
            .map(|source| source.manifest.clone())
            .collect(),
        resolution_source: if resolution_path.exists() {
            Some(SourceFileManifest::capture(&resolution_path)?)
        } else {
            None
        },
        prepared_payload_sha256: super::prepared_manifest::prepared_payload_sha256(&staging)?,
        engine_rows: stats.engine_rows,
        strategy_consumption_rows: stats.strategy_consumption_rows,
        resolution_rows: stats.resolution_rows,
        market_delta_rows: stats.market_delta_rows,
        token_book_delta_rows: stats.token_book_delta_rows,
        lifecycle_delta_rows: stats.lifecycle_delta_rows,
        resolution_delta_rows: stats.resolution_delta_rows,
        last_trade_delta_rows: stats.last_trade_delta_rows,
        total_events: stats.total_events,
        start_ts_ms: stats.start_ts_ms,
        end_ts_ms: stats.end_ts_ms,
    };
    manifest.write(&staging.join(MANIFEST_FILE))?;
    if out_dir.exists() {
        std::fs::remove_dir_all(out_dir)
            .with_context(|| format!("remove {}", out_dir.display()))?;
    }
    std::fs::rename(&staging, out_dir)
        .with_context(|| format!("rename {} -> {}", staging.display(), out_dir.display()))?;
    if let Err(error) = writeln!(
        std::io::stdout().lock(),
        "PREPARED_REPLAY built symbol={} dir={} events={} engine_rows={} strategy_consumption_rows={} resolution_rows={} resolution_delta_rows={}",
        symbol,
        out_dir.display(),
        manifest.total_events,
        manifest.engine_rows,
        manifest.strategy_consumption_rows,
        manifest.resolution_rows,
        manifest.resolution_delta_rows
    ) {
        let _ = writeln!(
            std::io::stderr().lock(),
            "PREPARED_REPLAY progress_output_failed phase=built reason={error}"
        );
    }
    Ok(())
}

fn validate_replay_symbol_scope(
    expected_symbol: &str,
    asset_id: &str,
    payload: &PolymarketOddsState,
) -> Result<()> {
    validate_captured_engine_state_scope(expected_symbol, asset_id, payload)
}

fn validate_captured_engine_state_scope(
    expected_symbol: &str,
    asset_id: &str,
    payload: &PolymarketOddsState,
) -> Result<()> {
    let row_asset = asset_id.trim().to_ascii_uppercase();
    let expected = expected_symbol.trim().to_ascii_uppercase();
    let projection = MarketProjection::from_odds(payload);
    let mut saw_market = false;
    for market in projection
        .markets()
        .iter()
        .filter(|market| market.standard_binary_ctf())
    {
        let underlying = market.underlying.trim().to_ascii_uppercase();
        if underlying.is_empty() {
            continue;
        }
        saw_market = true;
        if !row_asset.is_empty() {
            anyhow::ensure!(
                underlying == row_asset,
                "capture_backtest failed: replay engine state mixes asset scopes row_asset={} market_underlying={}",
                row_asset,
                underlying
            );
            // Guard contract string: underlying == &row_asset
        }
        anyhow::ensure!(
            underlying == expected,
            "capture_backtest failed: replay engine state symbol mismatch expected={} market_underlying={}",
            expected,
            underlying
        );
    }
    anyhow::ensure!(
        saw_market || row_asset == expected,
        "capture_backtest failed: no replay engine state asset scope for expected={} row_asset={}",
        expected,
        row_asset
    );
    Ok(())
}

fn replayable_engine_state_line() -> &'static str {
    "engine_state_by_underlying"
}
