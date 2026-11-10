use std::collections::HashSet;
use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};

use super::*;
use crate::polymarket::state::PolymarketOddsState;
use crate::strategy_runtime::market_catalog::{
    MarketCatalog, MarketCatalogSource, RecordedMarketDisposition, RecordedMarketPopulation,
    RecordedMarketRecord,
};
use crate::strategy_runtime::replay_mode::prepared_arrow::{
    apply_lifecycle_rows, apply_market_rows, refresh_projection_books, PreparedBookUpdate,
    ResolutionRow,
};

pub(super) struct PreparedMarketCatalog {
    pub(super) eligible: MarketCatalog,
    pub(super) recorded: RecordedMarketPopulation,
}

pub(super) fn market_catalog_from_capture(
    symbol: &str,
    rows: &[MarketRow],
    resolutions: &[ResolutionRow],
    interval: (u64, u64),
) -> Result<PreparedMarketCatalog> {
    let all_sources = market_sources(rows);
    let all_catalog = MarketCatalog::from_sources(symbol, &all_sources)
        .with_context(|| format!("validate captured market catalog symbol={symbol}"))?;
    let resolved_conditions = resolutions
        .iter()
        .filter(|row| row.observed_at_ms <= interval.1)
        .map(|row| {
            B256::from_str(&row.condition_id)
                .map(|condition| condition.to_string())
                .context("prepared resolution condition is invalid")
        })
        .collect::<Result<HashSet<_>>>()?;
    let complete_sources = all_sources
        .iter()
        .copied()
        .filter(|source| {
            source.window_start_ms >= interval.0
                && source.window_end_ms <= interval.1
                && source
                    .condition_id_b256
                    .is_some_and(|condition| resolved_conditions.contains(condition))
        })
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !complete_sources.is_empty(),
        "prepared replay has no complete entry-to-terminal markets symbol={symbol}"
    );
    let eligible = MarketCatalog::from_sources(symbol, &complete_sources)
        .with_context(|| format!("validate resolution-backed market catalog symbol={symbol}"))?;
    let recorded = RecordedMarketPopulation::from_records(
        all_catalog
            .records()
            .iter()
            .map(|record| {
                let market_id = B256::from(record.condition_id).to_string();
                let disposition =
                    if record.window_start_ms < interval.0 || record.window_end_ms > interval.1 {
                        RecordedMarketDisposition::ExcludedReplayBoundary
                    } else if resolved_conditions.contains(&market_id) {
                        RecordedMarketDisposition::ResolutionBackedEligible
                    } else {
                        RecordedMarketDisposition::ExcludedMissingResolution
                    };
                RecordedMarketRecord {
                    symbol: symbol.to_string(),
                    window_start_ms: record.window_start_ms,
                    window_end_ms: record.window_end_ms,
                    market_id,
                    disposition,
                }
            })
            .collect(),
    )?;
    Ok(PreparedMarketCatalog { eligible, recorded })
}

impl PackedReplayTape {
    pub(in crate::strategy_runtime::replay_mode) fn apply_market_state(
        &self,
        event: EventRef,
        state: &mut PolymarketOddsState,
        mut apply_external_book: impl FnMut(PreparedBookUpdate) -> Result<()>,
    ) -> Result<bool> {
        anyhow::ensure!(
            matches!(event.kind, EVENT_KIND_CLOCK | EVENT_KIND_WINDOW_BOOTSTRAP),
            "prepared market-state apply received non-clock kind={}",
            event.kind
        );
        let clocks = self.event_slice(self.clocks(), event, "engine_clock")?;
        let mut full_state = event.kind == EVENT_KIND_WINDOW_BOOTSTRAP;
        for clock in clocks {
            if !clock_has_engine_delta(clock) {
                continue;
            }
            let market_rows = slice(
                self.markets(),
                clock.market_start,
                clock.market_len,
                "market_delta",
            )?;
            let market_rows = market_rows
                .iter()
                .filter(|row| self.market_belongs_to_catalog(&row.market))
                .cloned()
                .collect::<Vec<_>>();
            let lifecycle_rows = slice(
                self.lifecycles(),
                clock.lifecycle_start,
                clock.lifecycle_len,
                "lifecycle_delta",
            )?;
            let mut catalog_lifecycle_rows = Vec::new();
            for row in lifecycle_rows {
                match row.parsed_token()? {
                    Some(token) if !self.token_belongs_to_catalog(token) => {}
                    _ => catalog_lifecycle_rows.push(row.clone()),
                }
            }
            full_state |= !market_rows.is_empty() || !catalog_lifecycle_rows.is_empty();
            let has_book_rows = clock.book_len > 0;
            state.updated_at_ms = clock.updated_at_ms;
            state.revision = clock.revision;
            apply_market_rows(state, &market_rows);
            self.visit_book_rows(
                clock.book_start,
                clock.book_len,
                "token_book_delta",
                |row| {
                    let update = row.into_update(clock.session_ts_ms);
                    if !self.book_update_belongs_to_catalog(&update) {
                        return Ok(());
                    }
                    match &update {
                        PreparedBookUpdate::Ignore => {}
                        PreparedBookUpdate::Remove(token) => {
                            state.books.remove(token);
                            state.depths.remove(token);
                        }
                        PreparedBookUpdate::Upsert { book, depth } => {
                            state.books.insert(book.token_id, book.clone());
                            state.depths.remove(&book.token_id);
                            if let Some(depth) = depth {
                                state.depths.insert(depth.token_id, depth.clone());
                            }
                            state.retain_validated_canonical_bid(book);
                        }
                    }
                    apply_external_book(update)
                },
            )?;
            apply_lifecycle_rows(state, &catalog_lifecycle_rows);
            if has_book_rows || event.projection_changed() {
                refresh_projection_books(state);
            }
        }
        Ok(full_state)
    }

    pub(crate) fn has_window_snapshot_before(&self, clock_index: usize) -> Result<bool> {
        if clock_index == 0 {
            return Ok(false);
        }
        let mut state = PolymarketOddsState::default();
        let len = u32::try_from(clock_index).context("window bootstrap clock count exceeds u32")?;
        self.apply_market_state(
            EventRef {
                sequence: 1,
                timestamp_ms: self.clocks()[clock_index - 1].session_ts_ms,
                symbol: self.symbol_id(),
                kind: EVENT_KIND_WINDOW_BOOTSTRAP,
                flags: 0,
                start: 0,
                len,
            },
            &mut state,
            |_| Ok(()),
        )?;
        Ok(state.selection.is_some() || !state.markets.is_empty() || !state.books.is_empty())
    }

    pub(in crate::strategy_runtime::replay_mode) fn market_projection_changed(
        &self,
        clock: &ClockRow,
    ) -> Result<bool> {
        Ok(slice(
            self.markets(),
            clock.market_start,
            clock.market_len,
            "market_delta",
        )?
        .iter()
        .any(|row| self.market_belongs_to_catalog(&row.market)))
    }

    pub(in crate::strategy_runtime::replay_mode) fn lifecycle_projection_changed(
        &self,
        clock: &ClockRow,
    ) -> Result<bool> {
        for row in slice(
            self.lifecycles(),
            clock.lifecycle_start,
            clock.lifecycle_len,
            "lifecycle_delta",
        )? {
            if row
                .parsed_token()?
                .is_some_and(|token| self.token_belongs_to_catalog(token))
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(in crate::strategy_runtime::replay_mode) fn book_projection_changed(
        &self,
        start: i64,
        len: i64,
        name: &str,
    ) -> Result<bool> {
        let mut changed = false;
        self.visit_book_rows(start, len, name, |row| {
            changed |= match row.into_update(0) {
                PreparedBookUpdate::Ignore => false,
                PreparedBookUpdate::Remove(token) => self.token_belongs_to_catalog(token),
                PreparedBookUpdate::Upsert { book, .. } => {
                    self.token_belongs_to_catalog(book.token_id)
                }
            };
            Ok(())
        })?;
        Ok(changed)
    }

    pub(in crate::strategy_runtime::replay_mode) fn last_trade_belongs_to_catalog(
        &self,
        row: &LastTradeRow,
    ) -> bool {
        self.token_belongs_to_catalog(row.token_id)
    }

    pub(in crate::strategy_runtime::replay_mode) fn resolution_belongs_to_catalog(
        &self,
        row: &ResolutionRow,
    ) -> Result<bool> {
        let condition = B256::from_str(&row.condition_id)
            .context("prepared resolution condition is invalid")?;
        Ok(self
            .market_catalog()
            .market_handle_for_condition(condition.0)
            .is_some())
    }

    fn market_belongs_to_catalog(
        &self,
        market: &bbwick_engine::state::MarketSelectionSnapshot,
    ) -> bool {
        self.token_belongs_to_catalog(market.up_token)
            && self.token_belongs_to_catalog(market.down_token)
    }

    fn token_belongs_to_catalog(&self, token: U256) -> bool {
        self.market_catalog()
            .token_handle(token.to_be_bytes::<32>())
            .is_some()
    }

    fn book_update_belongs_to_catalog(&self, update: &PreparedBookUpdate) -> bool {
        match update {
            PreparedBookUpdate::Ignore => true,
            PreparedBookUpdate::Remove(token) => self.token_belongs_to_catalog(*token),
            PreparedBookUpdate::Upsert { book, .. } => self.token_belongs_to_catalog(book.token_id),
        }
    }
}

fn market_sources(rows: &[MarketRow]) -> Vec<MarketCatalogSource<'_>> {
    rows.iter()
        .map(|row| MarketCatalogSource {
            market_id: row.market.market_id.as_str(),
            condition_id: row.market.condition_id.as_str(),
            condition_id_b256: row.market.condition_id_b256.as_deref(),
            gamma_market_id: row.market.gamma_market_id.as_str(),
            underlying: row.market.underlying.as_str(),
            up_token: row.market.up_token,
            down_token: row.market.down_token,
            window_start_ms: row.market.window_start_ms,
            window_end_ms: row.market.window_end_ms,
        })
        .collect()
}
