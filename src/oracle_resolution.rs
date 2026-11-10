use std::collections::{BTreeMap, HashMap};

use anyhow::{bail, ensure, Result};
use serde::{Deserialize, Serialize};

use crate::polymarket::discovery::MarketSelection;
use crate::polymarket::identity::MarketId;
use crate::strategy::hold_to_resolution::PairTokenSide;

pub(crate) const CHAINLINK_RESOLUTION_PROVENANCE: &str = "polymarket_rtds_chainlink";
pub(crate) const CHAINLINK_TWAP_PRICE_PRECISION: u8 = 38;
pub(crate) const CHAINLINK_TWAP_THIRTY_TOPIC: &str = "crypto_prices_twap_thirty";
pub(crate) const CHAINLINK_TWAP_SIXTY_TOPIC: &str = "crypto_prices_twap_sixty";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChainlinkResolutionFeed {
    pub(crate) topic: &'static str,
    pub(crate) window_s: u64,
}

struct ChainlinkResolutionSource {
    symbol: &'static str,
    resolution_source: &'static str,
    feed: ChainlinkResolutionFeed,
}

const CHAINLINK_RESOLUTION_SOURCES: &[ChainlinkResolutionSource] = &[
    ChainlinkResolutionSource {
        symbol: "btc/usd",
        resolution_source: "https://data.chain.link/streams/btc-usd-twap-30s-streams",
        feed: ChainlinkResolutionFeed {
            topic: CHAINLINK_TWAP_THIRTY_TOPIC,
            window_s: 30,
        },
    },
    ChainlinkResolutionSource {
        symbol: "btc/usd",
        resolution_source: "https://data.chain.link/streams/btc-usd-twap-60s-streams",
        feed: ChainlinkResolutionFeed {
            topic: CHAINLINK_TWAP_SIXTY_TOPIC,
            window_s: 60,
        },
    },
    ChainlinkResolutionSource {
        symbol: "eth/usd",
        resolution_source: "https://data.chain.link/streams/eth-usd-twap-30s-streams",
        feed: ChainlinkResolutionFeed {
            topic: CHAINLINK_TWAP_THIRTY_TOPIC,
            window_s: 30,
        },
    },
    ChainlinkResolutionSource {
        symbol: "eth/usd",
        resolution_source: "https://data.chain.link/streams/eth-usd-twap-60s-streams",
        feed: ChainlinkResolutionFeed {
            topic: CHAINLINK_TWAP_SIXTY_TOPIC,
            window_s: 60,
        },
    },
    ChainlinkResolutionSource {
        symbol: "sol/usd",
        resolution_source: "https://data.chain.link/streams/sol-usd-twap-30s-streams",
        feed: ChainlinkResolutionFeed {
            topic: CHAINLINK_TWAP_THIRTY_TOPIC,
            window_s: 30,
        },
    },
    ChainlinkResolutionSource {
        symbol: "sol/usd",
        resolution_source: "https://data.chain.link/streams/sol-usd-twap-60s-streams",
        feed: ChainlinkResolutionFeed {
            topic: CHAINLINK_TWAP_SIXTY_TOPIC,
            window_s: 60,
        },
    },
    ChainlinkResolutionSource {
        symbol: "xrp/usd",
        resolution_source: "https://data.chain.link/streams/xrp-usd-twap-30s-streams",
        feed: ChainlinkResolutionFeed {
            topic: CHAINLINK_TWAP_THIRTY_TOPIC,
            window_s: 30,
        },
    },
    ChainlinkResolutionSource {
        symbol: "xrp/usd",
        resolution_source: "https://data.chain.link/streams/xrp-usd-twap-60s-streams",
        feed: ChainlinkResolutionFeed {
            topic: CHAINLINK_TWAP_SIXTY_TOPIC,
            window_s: 60,
        },
    },
];

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChainlinkResolutionPrice {
    pub symbol: String,
    pub topic: String,
    pub window_s: u64,
    pub payload_ts_ms: u64,
    pub observed_ts_ms: u64,
    pub value_e18: i128,
    pub capture_sequence: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResolutionBoundaryValue {
    pub(crate) price_e18: i128,
    pub(crate) source_ts_ms: u64,
    pub(crate) observed_ts_ms: u64,
    pub(crate) capture_sequence: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ChainlinkReferenceDistance<'a> {
    pub(crate) symbol: &'a str,
    pub(crate) topic: &'static str,
    pub(crate) window_s: u64,
    pub(crate) open: &'a ResolutionBoundaryValue,
    pub(crate) current: &'a ChainlinkResolutionPrice,
}

impl ChainlinkReferenceDistance<'_> {
    pub(crate) fn log_return(self) -> Option<f64> {
        if self.open.price_e18 <= 0 || self.current.value_e18 <= 0 {
            return None;
        }
        let value = (self.current.value_e18 as f64 / self.open.price_e18 as f64).ln();
        value.is_finite().then_some(value)
    }

    pub(crate) fn evidence(self, log_return: f64) -> ChainlinkReferenceEvidence {
        ChainlinkReferenceEvidence {
            symbol: self.symbol.to_string(),
            topic: self.topic.to_string(),
            window_s: self.window_s,
            open_price_e18: self.open.price_e18,
            open_source_ts_ms: self.open.source_ts_ms,
            open_observed_ts_ms: self.open.observed_ts_ms,
            open_capture_sequence: self.open.capture_sequence,
            current_price_e18: self.current.value_e18,
            current_source_ts_ms: self.current.payload_ts_ms,
            current_observed_ts_ms: self.current.observed_ts_ms,
            current_capture_sequence: self.current.capture_sequence,
            log_return,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct ChainlinkReferenceEvidence {
    pub(crate) symbol: String,
    pub(crate) topic: String,
    pub(crate) window_s: u64,
    pub(crate) open_price_e18: i128,
    pub(crate) open_source_ts_ms: u64,
    pub(crate) open_observed_ts_ms: u64,
    pub(crate) open_capture_sequence: u64,
    pub(crate) current_price_e18: i128,
    pub(crate) current_source_ts_ms: u64,
    pub(crate) current_observed_ts_ms: u64,
    pub(crate) current_capture_sequence: u64,
    pub(crate) log_return: f64,
}

impl ChainlinkReferenceEvidence {
    pub(crate) fn valid_for_window(&self, expected_symbol: &str, window_start_ms: u64) -> bool {
        let expected_symbol = canonical_chainlink_symbol(expected_symbol).ok();
        let computed_return = (self.current_price_e18 as f64 / self.open_price_e18 as f64).ln();
        expected_symbol.as_deref() == Some(self.symbol.as_str())
            && validate_chainlink_resolution_feed(&self.symbol, &self.topic, self.window_s).is_ok()
            && self.window_s > 0
            && self.open_price_e18 > 0
            && self.open_source_ts_ms == window_start_ms
            && self.open_observed_ts_ms > 0
            && self.open_capture_sequence > 0
            && self.current_price_e18 > 0
            && self.current_source_ts_ms > 0
            && self.current_observed_ts_ms > 0
            && self.current_capture_sequence > 0
            && self.log_return.is_finite()
            && computed_return.is_finite()
            && self.log_return.to_bits() == computed_return.to_bits()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OracleResolvedLabel {
    pub(crate) market_id: MarketId,
    pub(crate) condition_id: String,
    pub(crate) symbol: String,
    pub(crate) window_start_ms: u64,
    pub(crate) window_end_ms: u64,
    pub(crate) open: ResolutionBoundaryValue,
    pub(crate) close: ResolutionBoundaryValue,
    pub(crate) winner: PairTokenSide,
    pub(crate) release_ts_ms: u64,
    pub(crate) provenance: &'static str,
}

impl OracleResolvedLabel {
    pub(crate) fn y_up(&self) -> f64 {
        if self.winner == PairTokenSide::Up {
            1.0
        } else {
            0.0
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.market_id.as_str().is_empty() && self.market_id.as_str() == self.condition_id,
            "oracle label market/condition identity mismatch"
        );
        ensure!(
            canonical_chainlink_symbol(&self.symbol)? == self.symbol,
            "oracle label symbol is noncanonical"
        );
        ensure!(
            self.window_start_ms > 0 && self.window_start_ms < self.window_end_ms,
            "invalid oracle label market window"
        );
        ensure!(
            self.provenance == CHAINLINK_RESOLUTION_PROVENANCE,
            "invalid oracle label provenance"
        );
        validate_resolved_boundary(&self.open, self.window_start_ms, "open")?;
        validate_resolved_boundary(&self.close, self.window_end_ms, "close")?;
        ensure!(
            self.release_ts_ms == self.open.observed_ts_ms.max(self.close.observed_ts_ms),
            "oracle label release timestamp mismatch"
        );
        ensure!(
            self.winner
                == if self.close.price_e18 >= self.open.price_e18 {
                    PairTokenSide::Up
                } else {
                    PairTokenSide::Down
                },
            "oracle label winner conflicts with boundary prices"
        );
        Ok(())
    }
}

fn validate_resolved_boundary(
    boundary: &ResolutionBoundaryValue,
    boundary_ts_ms: u64,
    name: &str,
) -> Result<()> {
    ensure!(
        boundary.price_e18 > 0,
        "oracle label {name} boundary price is invalid"
    );
    ensure!(
        boundary.source_ts_ms == boundary_ts_ms,
        "invalid oracle label {name} boundary timestamp"
    );
    ensure!(
        boundary.observed_ts_ms > 0,
        "oracle label {name} observation timestamp is zero"
    );
    ensure!(
        boundary.capture_sequence > 0,
        "invalid oracle label {name} capture sequence"
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct BoundaryMarket {
    condition_id: String,
    symbol: String,
    feed: ChainlinkResolutionFeed,
    window_start_ms: u64,
    window_end_ms: u64,
    open: Option<ResolutionBoundaryValue>,
    close: Option<ResolutionBoundaryValue>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ChainlinkResolutionReducer {
    history_by_feed: HashMap<(String, String, u64), Vec<ChainlinkResolutionPrice>>,
    markets: BTreeMap<MarketId, BoundaryMarket>,
    resolved: BTreeMap<MarketId, OracleResolvedLabel>,
}

impl ChainlinkResolutionReducer {
    pub(crate) fn reference_distance(
        &self,
        market_id: &MarketId,
    ) -> Option<ChainlinkReferenceDistance<'_>> {
        let market = self.markets.get(market_id)?;
        let open = market.open.as_ref()?;
        let history =
            self.history_by_feed
                .iter()
                .find_map(|((symbol, topic, window_s), history)| {
                    (symbol == &market.symbol
                        && topic == market.feed.topic
                        && *window_s == market.feed.window_s)
                        .then_some(history)
                })?;
        let current = history.last()?;
        Some(ChainlinkReferenceDistance {
            symbol: &market.symbol,
            topic: market.feed.topic,
            window_s: market.feed.window_s,
            open,
            current,
        })
    }

    pub(crate) fn register_market(
        &mut self,
        market: &MarketSelection,
    ) -> Result<Vec<OracleResolvedLabel>> {
        let symbol = canonical_chainlink_symbol(&market.underlying)?;
        let resolution_source = market.resolution_source.as_deref().unwrap_or("missing");
        let feed = chainlink_resolution_feed(&symbol, resolution_source).map_err(|_| {
            anyhow::anyhow!(
                "unsupported Chainlink resolution source market_id={} symbol={} resolution_source={}",
                market.market_id,
                symbol,
                resolution_source
            )
        })?;
        ensure!(
            market.window_start_ms < market.window_end_ms,
            "invalid Chainlink resolution window market_id={} start={} end={}",
            market.market_id,
            market.window_start_ms,
            market.window_end_ms
        );
        let candidate = BoundaryMarket {
            condition_id: market.condition_id.to_string(),
            symbol,
            feed,
            window_start_ms: market.window_start_ms,
            window_end_ms: market.window_end_ms,
            open: None,
            close: None,
        };
        if let Some(existing) = self.markets.get(&market.market_id) {
            ensure!(
                existing.condition_id == candidate.condition_id
                    && existing.symbol == candidate.symbol
                    && existing.feed == candidate.feed
                    && existing.window_start_ms == candidate.window_start_ms
                    && existing.window_end_ms == candidate.window_end_ms,
                "conflicting Chainlink resolution market identity market_id={}",
                market.market_id
            );
            return Ok(Vec::new());
        }
        let mut candidate = candidate;
        let feed_key = (
            candidate.symbol.clone(),
            candidate.feed.topic.to_string(),
            candidate.feed.window_s,
        );
        if let Some(history) = self.history_by_feed.get(&feed_key) {
            refresh_market_boundaries(&mut candidate, history);
        }
        let label = resolved_label(&market.market_id, &candidate);
        self.markets.insert(market.market_id.clone(), candidate);
        if let Some(label) = label {
            self.resolved
                .insert(market.market_id.clone(), label.clone());
            return Ok(vec![label]);
        }
        Ok(Vec::new())
    }

    pub(crate) fn observe(
        &mut self,
        price: &ChainlinkResolutionPrice,
    ) -> Result<Vec<OracleResolvedLabel>> {
        let symbol = canonical_chainlink_symbol(&price.symbol)?;
        validate_chainlink_resolution_feed(&symbol, &price.topic, price.window_s)?;
        ensure!(
            price.payload_ts_ms > 0 && price.value_e18 > 0,
            "invalid Chainlink resolution price symbol={} ts={} value={}",
            price.symbol,
            price.payload_ts_ms,
            price.value_e18
        );
        ensure!(
            price.observed_ts_ms > 0,
            "invalid Chainlink resolution observation timestamp symbol={}",
            price.symbol
        );
        let feed_key = (symbol.clone(), price.topic.clone(), price.window_s);
        if let Some(existing) = self.history_by_feed.get(&feed_key).and_then(|history| {
            history
                .binary_search_by_key(&price.payload_ts_ms, |fact| fact.payload_ts_ms)
                .ok()
                .map(|index| &history[index])
        }) {
            ensure!(
                price.value_e18 == existing.value_e18,
                "conflicting Chainlink resolution price at duplicate timestamp symbol={} topic={} window_s={} ts={}",
                price.symbol,
                price.topic,
                price.window_s,
                price.payload_ts_ms
            );
            return Ok(Vec::new());
        }
        let mut price = price.clone();
        price.symbol = symbol;
        let history = self.history_by_feed.entry(feed_key).or_default();
        let insertion_index = history
            .binary_search_by(|fact| {
                (fact.payload_ts_ms, fact.capture_sequence)
                    .cmp(&(price.payload_ts_ms, price.capture_sequence))
            })
            .unwrap_or_else(|index| index);
        history.insert(insertion_index, price);
        let price = &history[insertion_index];

        let mut newly_resolved = Vec::new();
        for (market_id, market) in &mut self.markets {
            if market.symbol != price.symbol
                || market.feed.topic != price.topic
                || market.feed.window_s != price.window_s
                || self.resolved.contains_key(market_id)
            {
                continue;
            }
            refresh_market_boundaries(market, history);
            if market.open.is_none() || market.close.is_none() {
                continue;
            }
            let label = resolved_label(market_id, market)
                .expect("open and close Chainlink boundaries checked above");
            self.resolved.insert(market_id.clone(), label.clone());
            newly_resolved.push(label);
        }
        Ok(newly_resolved)
    }
}

fn refresh_market_boundaries(market: &mut BoundaryMarket, history: &[ChainlinkResolutionPrice]) {
    market.open = boundary_from_history(history, market.window_start_ms);
    market.close = boundary_from_history(history, market.window_end_ms);
}

fn boundary_from_history(
    history: &[ChainlinkResolutionPrice],
    boundary_ts_ms: u64,
) -> Option<ResolutionBoundaryValue> {
    let exact_index = history
        .binary_search_by_key(&boundary_ts_ms, |fact| fact.payload_ts_ms)
        .ok()?;
    let exact = &history[exact_index];
    Some(ResolutionBoundaryValue {
        price_e18: exact.value_e18,
        source_ts_ms: exact.payload_ts_ms,
        observed_ts_ms: exact.observed_ts_ms,
        capture_sequence: exact.capture_sequence,
    })
}

fn resolved_label(market_id: &MarketId, market: &BoundaryMarket) -> Option<OracleResolvedLabel> {
    let open = market.open.clone()?;
    let close = market.close.clone()?;
    let release_ts_ms = open.observed_ts_ms.max(close.observed_ts_ms);
    Some(OracleResolvedLabel {
        market_id: market_id.clone(),
        condition_id: market.condition_id.clone(),
        symbol: market.symbol.clone(),
        window_start_ms: market.window_start_ms,
        window_end_ms: market.window_end_ms,
        winner: if close.price_e18 >= open.price_e18 {
            PairTokenSide::Up
        } else {
            PairTokenSide::Down
        },
        open,
        release_ts_ms,
        close,
        provenance: CHAINLINK_RESOLUTION_PROVENANCE,
    })
}

pub(crate) fn canonical_chainlink_symbol(raw: &str) -> Result<String> {
    let normalized = raw.trim().to_ascii_lowercase();
    let symbol = match normalized.as_str() {
        "btc" | "btcusd" | "btcusdt" | "btc/usd" | "bitcoin" => "btc/usd",
        "eth" | "ethusd" | "ethusdt" | "eth/usd" | "ethereum" => "eth/usd",
        "sol" | "solusd" | "solusdt" | "sol/usd" | "solana" => "sol/usd",
        "xrp" | "xrpusd" | "xrpusdt" | "xrp/usd" => "xrp/usd",
        _ => bail!("unsupported Chainlink resolution symbol {raw}"),
    };
    Ok(symbol.to_string())
}

pub(crate) fn chainlink_resolution_feed(
    symbol: &str,
    resolution_source: &str,
) -> Result<ChainlinkResolutionFeed> {
    let canonical = canonical_chainlink_symbol(symbol)?;
    CHAINLINK_RESOLUTION_SOURCES
        .iter()
        .find(|source| {
            source.symbol == canonical && source.resolution_source == resolution_source
        })
        .map(|source| source.feed)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "unsupported Chainlink resolution source symbol={canonical} resolution_source={resolution_source}"
            )
        })
}

pub(crate) fn validate_chainlink_resolution_feed(
    symbol: &str,
    topic: &str,
    window_s: u64,
) -> Result<()> {
    let canonical = canonical_chainlink_symbol(symbol)?;
    ensure!(
        CHAINLINK_RESOLUTION_SOURCES.iter().any(|source| {
            source.symbol == canonical
                && source.feed.topic == topic
                && source.feed.window_s == window_s
        }),
        "unsupported Chainlink resolution feed symbol={canonical} topic={topic} window_s={window_s}"
    );
    Ok(())
}
