use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};

#[path = "prepared_tape/market_state.rs"]
mod market_state;

use super::prepared_arrow::book_rows::BookRow;
use super::prepared_arrow::{
    read_prepared_replay_data, slice, ClockRow, LastTradeRow, LifecycleRow, MarketRow,
    PreparedBookUpdate, PreparedConsumptionCursor, PreparedReplayData, ResolutionRow,
};
use super::prepared_event_stream::{
    build_event_schedule, EventRef, EVENT_KIND_CLOCK, EVENT_KIND_LAST_TRADE,
    EVENT_KIND_PROVIDER_RESOLUTION, EVENT_KIND_STRATEGY_CONSUMPTION, EVENT_KIND_WINDOW_BOOTSTRAP,
};
use super::prepared_manifest::{prepared_symbol_dir, PreparedReplayManifest, MANIFEST_FILE};
use crate::capture::events::StrategyConsumptionRecord;
use crate::strategy_runtime::market_catalog::{MarketCatalog, RecordedMarketPopulation};
use crate::trade_session_ledger::ManagedSellReceiptIdentity;

#[derive(Debug)]
pub(crate) struct PackedReplayTape {
    symbol: &'static str,
    data: PreparedReplayData,
    prepared_payload_sha256: [u8; 32],
    market_catalog: Arc<MarketCatalog>,
    recorded_market_population: Arc<RecordedMarketPopulation>,
    events: Vec<EventRef>,
}

impl PackedReplayTape {
    pub(crate) fn prepared_market_lifecycles(
        capture: &Path,
        symbol: &str,
    ) -> Result<Vec<(String, String, u64, u64)>> {
        Ok(Self::prepared_market_inputs(capture, symbol)?.0)
    }

    pub(crate) fn prepared_market_inputs(
        capture: &Path,
        symbol: &str,
    ) -> Result<(Vec<(String, String, u64, u64)>, RecordedMarketPopulation)> {
        let symbol = symbol.trim().to_ascii_uppercase();
        let dir = prepared_symbol_dir(capture, &symbol);
        let manifest_path = dir.join(MANIFEST_FILE);
        let manifest = PreparedReplayManifest::read(&manifest_path)?;
        anyhow::ensure!(
            crate::capture::arrow::normalize_capture_symbol(&manifest.symbol)
                == symbol.to_ascii_lowercase(),
            "prepared replay manifest symbol mismatch expected={} actual={}",
            symbol,
            manifest.symbol
        );
        anyhow::ensure!(
            manifest.is_complete(),
            "prepared replay manifest is incomplete at {}",
            manifest_path.display()
        );
        let (markets, resolutions) = PreparedReplayData::read_prepared_market_rows(&dir)?;
        let interval = super::session_resolution::capture_replay_interval(capture)?
            .window()
            .context("prepared replay interval is unavailable")?;
        let prepared_catalog =
            market_state::market_catalog_from_capture(&symbol, &markets, &resolutions, interval)?;
        let catalog = prepared_catalog.eligible;
        anyhow::ensure!(
            !catalog.is_empty(),
            "prepared replay market catalog is empty symbol={symbol}"
        );
        let mut lifecycles = catalog
            .records()
            .iter()
            .map(|record| {
                let condition_id = record
                    .condition_id
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                (
                    symbol.clone(),
                    condition_id,
                    record.window_start_ms,
                    record.window_end_ms,
                )
            })
            .collect::<Vec<_>>();
        lifecycles.sort();
        lifecycles.dedup();
        Ok((lifecycles, prepared_catalog.recorded))
    }

    pub(crate) fn event_schedule_for_window(
        self: &Arc<Self>,
        window: Option<(u64, u64)>,
    ) -> Result<Arc<[EventRef]>> {
        let mut stream =
            super::prepared_event_stream::PreparedReplayEventStream::new(self.clone(), window)?;
        let mut events = Vec::new();
        while let Some(event) = stream.next_event()? {
            events.push(event);
        }
        anyhow::ensure!(!events.is_empty(), "prepared replay window has no events");
        Ok(Arc::from(events))
    }

    pub(crate) fn load(capture: &Path, symbol: &str) -> Result<Arc<Self>> {
        let symbol = match symbol.trim().to_ascii_uppercase().as_str() {
            "BTC" => "BTC",
            "ETH" => "ETH",
            "SOL" => "SOL",
            "XRP" => "XRP",
            other => anyhow::bail!("unsupported prepared replay symbol `{other}`"),
        };
        let dir = prepared_symbol_dir(capture, symbol);
        let manifest_path = dir.join(MANIFEST_FILE);
        let manifest = PreparedReplayManifest::read(&manifest_path)?;
        anyhow::ensure!(
            crate::capture::arrow::normalize_capture_symbol(&manifest.symbol)
                == symbol.to_ascii_lowercase(),
            "prepared replay manifest symbol mismatch expected={} actual={}",
            symbol,
            manifest.symbol
        );
        anyhow::ensure!(
            manifest.is_complete(),
            "prepared replay manifest is incomplete at {}",
            manifest_path.display()
        );
        let prepared_payload_sha256 = manifest.verify_prepared_payload(&dir)?;
        let data = read_prepared_replay_data(&dir, symbol)
            .with_context(|| format!("load prepared replay {}", dir.display()))?;
        data.validate_symbol_scope(symbol)?;
        let interval = super::session_resolution::capture_replay_interval(capture)?
            .window()
            .context("prepared replay interval is unavailable")?;
        let prepared_catalog = market_state::market_catalog_from_capture(
            symbol,
            &data.markets,
            &data.resolutions,
            interval,
        )?;
        let market_catalog = prepared_catalog.eligible;
        anyhow::ensure!(
            !market_catalog.is_empty(),
            "prepared replay market catalog is empty symbol={symbol}"
        );
        for (name, len) in [
            ("engine_clock", data.clocks.len()),
            ("market_delta", data.markets.len()),
            ("token_book_delta", data.books.len()),
            ("strategy_consumption", data.consumptions.len()),
            ("lifecycle_delta", data.lifecycles.len()),
            ("resolution_delta", data.resolutions.len()),
            ("last_trade_delta", data.last_trades.len()),
        ] {
            anyhow::ensure!(
                len <= u32::MAX as usize,
                "prepared {name} row count exceeds EventRef u32 range"
            );
        }
        for (name, expected, actual) in [
            (
                "engine_clock",
                manifest.total_events,
                u64::try_from(data.clocks.len()).context("engine_clock row count exceeds u64")?,
            ),
            (
                "strategy_consumption",
                manifest.strategy_consumption_rows,
                u64::try_from(data.consumptions.len())
                    .context("strategy consumption row count exceeds u64")?,
            ),
            (
                "market_delta",
                manifest.market_delta_rows,
                u64::try_from(data.markets.len()).context("market_delta row count exceeds u64")?,
            ),
            (
                "token_book_delta",
                manifest.token_book_delta_rows,
                u64::try_from(data.books.len())
                    .context("token_book_delta row count exceeds u64")?,
            ),
            (
                "lifecycle_delta",
                manifest.lifecycle_delta_rows,
                u64::try_from(data.lifecycles.len())
                    .context("lifecycle_delta row count exceeds u64")?,
            ),
            (
                "resolution_delta",
                manifest.resolution_delta_rows,
                u64::try_from(data.resolutions.len())
                    .context("resolution_delta row count exceeds u64")?,
            ),
            (
                "last_trade_delta",
                manifest.last_trade_delta_rows,
                u64::try_from(data.last_trades.len())
                    .context("last_trade_delta row count exceeds u64")?,
            ),
        ] {
            anyhow::ensure!(
                actual == expected,
                "prepared {name} row count mismatch manifest={expected} actual={actual}"
            );
        }
        let mut source_priority_counts = [0_u64; 2];
        for clock in &data.clocks {
            let priority = usize::from(clock.source_priority);
            anyhow::ensure!(
                priority < source_priority_counts.len(),
                "invalid engine_clock source_priority={priority}"
            );
            source_priority_counts[priority] = source_priority_counts[priority]
                .checked_add(1)
                .context("engine_clock source-priority count overflow")?;
            anyhow::ensure!(
                clock.sequence > 0,
                "engine_clock sequence is zero actual={}",
                clock.sequence
            );
        }
        anyhow::ensure!(
            source_priority_counts[0] <= manifest.engine_rows,
            "prepared engine clock count exceeds source engine rows"
        );
        anyhow::ensure!(
            source_priority_counts[1] == manifest.strategy_consumption_rows,
            "prepared strategy consumption clock count mismatch manifest={} actual={}",
            manifest.strategy_consumption_rows,
            source_priority_counts[1]
        );
        let actual_start = data.clocks.iter().map(|row| row.session_ts_ms).min();
        let actual_end = data.clocks.iter().map(|row| row.session_ts_ms).max();
        anyhow::ensure!(
            actual_start == manifest.start_ts_ms && actual_end == manifest.end_ts_ms,
            "prepared engine_clock timestamp bounds mismatch manifest={:?}..{:?} actual={:?}..{:?}",
            manifest.start_ts_ms,
            manifest.end_ts_ms,
            actual_start,
            actual_end
        );
        let mut tape = Self {
            symbol,
            data,
            prepared_payload_sha256,
            market_catalog: Arc::new(market_catalog),
            recorded_market_population: Arc::new(prepared_catalog.recorded),
            events: Vec::new(),
        };
        tape.events = build_event_schedule(&tape)?;
        let eligible_last_trade_rows =
            tape.last_trades().iter().try_fold(0_u64, |count, trade| {
                if tape.last_trade_belongs_to_catalog(trade) {
                    count
                        .checked_add(1)
                        .context("eligible last-trade count overflow")
                } else {
                    Ok(count)
                }
            })?;
        let eligible_resolution_rows =
            tape.resolutions()
                .iter()
                .try_fold(0_u64, |count, resolution| {
                    if tape.resolution_belongs_to_catalog(resolution)? {
                        count
                            .checked_add(1)
                            .context("eligible resolution count overflow")
                    } else {
                        Ok(count)
                    }
                })?;
        let expected_event_refs = manifest
            .total_events
            .checked_add(eligible_last_trade_rows)
            .and_then(|events| events.checked_add(eligible_resolution_rows))
            .context("prepared EventRef count overflow")?;
        anyhow::ensure!(
            u64::try_from(tape.events.len()).context("prepared EventRef count exceeds u64")?
                == expected_event_refs,
            "prepared EventRef count mismatch expected={} actual={}",
            expected_event_refs,
            tape.events.len()
        );
        anyhow::ensure!(
            tape.market_catalog_sha256().iter().any(|byte| *byte != 0),
            "prepared replay market catalog hash is zero symbol={symbol}"
        );
        anyhow::ensure!(
            !tape.market_catalog_records().is_empty(),
            "prepared replay market catalog has no records symbol={symbol}"
        );
        Ok(Arc::new(tape))
    }

    pub(crate) fn symbol(&self) -> &'static str {
        self.symbol
    }

    pub(crate) fn symbol_id(&self) -> u8 {
        match self.symbol {
            "BTC" => 0,
            "ETH" => 1,
            "SOL" => 2,
            "XRP" => 3,
            _ => unreachable!("validated prepared replay symbol"),
        }
    }

    pub(crate) fn market_catalog(&self) -> &MarketCatalog {
        self.market_catalog.as_ref()
    }

    pub(crate) fn market_catalog_arc(&self) -> Arc<MarketCatalog> {
        self.market_catalog.clone()
    }

    pub(crate) fn market_catalog_sha256(&self) -> [u8; 32] {
        self.market_catalog().sha256()
    }

    pub(crate) fn recorded_market_population(&self) -> &RecordedMarketPopulation {
        self.recorded_market_population.as_ref()
    }

    pub(crate) fn prepared_payload_sha256(&self) -> [u8; 32] {
        self.prepared_payload_sha256
    }

    pub(crate) fn earliest_consumed_input_ms(
        &self,
        cursor: &mut PreparedConsumptionCursor,
        event: EventRef,
    ) -> Result<u64> {
        anyhow::ensure!(
            event.symbol == self.symbol_id(),
            "prepared input boundary EventRef symbol mismatch"
        );
        let mut earliest = event.timestamp_ms;
        match event.kind {
            EVENT_KIND_CLOCK | EVENT_KIND_WINDOW_BOOTSTRAP => {
                for clock in self.event_slice(self.clocks(), event, "engine_clock")? {
                    if !clock_has_engine_delta(clock) {
                        continue;
                    }
                    for timestamp_ms in [clock.session_ts_ms, clock.updated_at_ms] {
                        if timestamp_ms > 0 {
                            earliest = earliest.min(timestamp_ms);
                        }
                    }
                    self.visit_book_rows(
                        clock.book_start,
                        clock.book_len,
                        "token_book_delta",
                        |row| {
                            let timestamp_ms = row.ts_ms;
                            let update = row.into_update(clock.session_ts_ms);
                            let belongs_to_catalog = match &update {
                                PreparedBookUpdate::Ignore => false,
                                PreparedBookUpdate::Remove(token) => self
                                    .market_catalog()
                                    .token_handle(token.to_be_bytes::<32>())
                                    .is_some(),
                                PreparedBookUpdate::Upsert { book, .. } => self
                                    .market_catalog()
                                    .token_handle(book.token_id.to_be_bytes::<32>())
                                    .is_some(),
                            };
                            if belongs_to_catalog && timestamp_ms > 0 {
                                earliest = earliest.min(timestamp_ms);
                            }
                            Ok(())
                        },
                    )?;
                }
            }
            EVENT_KIND_STRATEGY_CONSUMPTION => {
                event.validate_wire(self.symbol_id())?;
                anyhow::ensure!(
                    event.kind == EVENT_KIND_STRATEGY_CONSUMPTION && event.len == 1,
                    "prepared consumption ref must reference exactly one consumption row"
                );
                earliest = self.data.consumptions.earliest_input_ms(
                    cursor,
                    event.start,
                    event.sequence,
                    event.timestamp_ms,
                )?;
            }
            EVENT_KIND_LAST_TRADE => {
                let trade = self.last_trade(event)?;
                earliest = earliest.min(trade.session_ts_ms).min(trade.trade_ts_ms);
            }
            EVENT_KIND_PROVIDER_RESOLUTION => {
                earliest = earliest.min(self.resolution(event)?.observed_at_ms);
            }
            other => anyhow::bail!("unsupported prepared input boundary event kind={other}"),
        }
        anyhow::ensure!(earliest > 0, "prepared replay input boundary is zero");
        Ok(earliest)
    }

    pub(in crate::strategy_runtime::replay_mode) fn captured_receipt_identity(
        &self,
        event: EventRef,
    ) -> Result<ManagedSellReceiptIdentity> {
        anyhow::ensure!(
            event.symbol == self.symbol_id(),
            "captured receipt EventRef symbol mismatch expected={} actual={}",
            self.symbol_id(),
            event.symbol
        );
        anyhow::ensure!(event.timestamp_ms > 0, "captured receipt timestamp is zero");
        anyhow::ensure!(event.len > 0, "captured receipt EventRef length is zero");
        Ok(ManagedSellReceiptIdentity::CapturedSimulation {
            tape_sha256: self.prepared_payload_sha256,
            symbol: self.symbol.to_string(),
            event_kind: event.kind,
            event_timestamp_ms: event.timestamp_ms,
            event_start: event.start,
            event_len: event.len,
        })
    }

    pub(crate) fn market_catalog_records(
        &self,
    ) -> &[crate::strategy_runtime::market_catalog::MarketCatalogRecord] {
        self.market_catalog().records()
    }

    pub(super) fn clocks(&self) -> &[ClockRow] {
        &self.data.clocks
    }

    pub(super) fn markets(&self) -> &[MarketRow] {
        &self.data.markets
    }

    pub(super) fn visit_book_rows<F>(
        &self,
        start: i64,
        len: i64,
        name: &str,
        visit: F,
    ) -> Result<()>
    where
        F: FnMut(BookRow) -> Result<()>,
    {
        self.data.books.visit_rows(start, len, name, visit)
    }

    pub(super) fn lifecycles(&self) -> &[LifecycleRow] {
        &self.data.lifecycles
    }

    pub(super) fn resolutions(&self) -> &[ResolutionRow] {
        &self.data.resolutions
    }

    pub(super) fn resolution(&self, event: EventRef) -> Result<&ResolutionRow> {
        self.event_slice(self.resolutions(), event, "resolution_delta")?
            .first()
            .context("resolution event row is unavailable")
    }

    pub(super) fn last_trades(&self) -> &[LastTradeRow] {
        &self.data.last_trades
    }

    pub(crate) fn events(&self) -> &[EventRef] {
        &self.events
    }

    pub(crate) fn consumption_cursor(&self) -> Result<PreparedConsumptionCursor> {
        self.data.consumptions.cursor()
    }

    pub(crate) fn strategy_consumption_location(
        &self,
        event: EventRef,
    ) -> Result<(&Path, u32, u32)> {
        self.data.consumptions.location(event.start)
    }

    pub(crate) fn strategy_consumptions(
        &self,
        cursor: &mut PreparedConsumptionCursor,
        events: &[EventRef],
        pool: &rayon::ThreadPool,
    ) -> Result<Vec<StrategyConsumptionRecord>> {
        let rows = events
            .iter()
            .map(|event| {
                event.validate_wire(self.symbol_id())?;
                anyhow::ensure!(
                    event.kind == EVENT_KIND_STRATEGY_CONSUMPTION && event.len == 1,
                    "prepared consumption ref must reference exactly one consumption row"
                );
                Ok(event.start)
            })
            .collect::<Result<Vec<_>>>()?;
        let records = self.data.consumptions.read_rows(cursor, &rows, pool)?;
        anyhow::ensure!(
            records.len() == events.len(),
            "strategy consumption batch result count mismatch expected={} actual={}",
            events.len(),
            records.len()
        );
        for (event, record) in events.iter().zip(&records) {
            let (path, file_batch, batch_row) = self.data.consumptions.location(event.start)?;
            anyhow::ensure!(
                record.consumption_sequence == event.sequence
                    && record.consumed_at_ms == event.timestamp_ms,
                "strategy consumption EventRef differs from captured sequence or timestamp file={} batch={file_batch} row={batch_row} global_row={} expected_sequence={} actual_sequence={} expected_timestamp_ms={} actual_timestamp_ms={}",
                path.display(),
                event.start,
                event.sequence,
                record.consumption_sequence,
                event.timestamp_ms,
                record.consumed_at_ms
            );
        }
        Ok(records)
    }

    pub(in crate::strategy_runtime::replay_mode) fn last_trade(
        &self,
        event: EventRef,
    ) -> Result<&LastTradeRow> {
        anyhow::ensure!(
            event.kind == super::prepared_event_stream::EVENT_KIND_LAST_TRADE,
            "prepared last-trade apply received kind={}",
            event.kind
        );
        let rows = self.event_slice(self.last_trades(), event, "last_trade_delta")?;
        anyhow::ensure!(
            rows.len() == 1,
            "last trade prepared ref must reference exactly one row"
        );
        Ok(&rows[0])
    }

    fn event_slice<'a, T>(&self, rows: &'a [T], event: EventRef, name: &str) -> Result<&'a [T]> {
        slice(rows, i64::from(event.start), i64::from(event.len), name)
    }
}

pub(super) fn clock_has_engine_delta(clock: &ClockRow) -> bool {
    clock.market_len > 0 || clock.book_len > 0 || clock.lifecycle_len > 0
}
