use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const CATALOG_SCHEMA: &[u8; 8] = b"PMCAT007";

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordedMarketDisposition {
    ResolutionBackedEligible,
    ExcludedMissingResolution,
    ExcludedReplayBoundary,
}

impl RecordedMarketDisposition {
    pub fn included_in_expected(self) -> bool {
        self != Self::ExcludedReplayBoundary
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::ResolutionBackedEligible => "resolution_backed_eligible",
            Self::ExcludedMissingResolution => "excluded_missing_resolution_by_replay_end",
            Self::ExcludedReplayBoundary => "excluded_replay_boundary",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct RecordedMarketRecord {
    pub symbol: String,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub market_id: String,
    pub disposition: RecordedMarketDisposition,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordedMarketReportDisposition {
    EligibleNotEntered,
    ExcludedBeforeReplayEntry,
}

impl RecordedMarketReportDisposition {
    pub fn label(self) -> &'static str {
        match self {
            Self::EligibleNotEntered => "eligible_not_entered",
            Self::ExcludedBeforeReplayEntry => "excluded_before_replay_entry",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecordedMarketMissingRecord {
    #[serde(flatten)]
    pub market: RecordedMarketRecord,
    pub report_disposition: RecordedMarketReportDisposition,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecordedMarketCoverageSummary {
    pub symbol: String,
    pub entered_markets: usize,
    pub expected_markets: usize,
    pub resolution_backed_eligible_markets: usize,
    pub missing_resolution_markets: usize,
    pub missing_entry_markets: usize,
    pub boundary_excluded_markets: usize,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecordedMarketCoverageReport {
    pub entered_markets: usize,
    pub expected_markets: usize,
    pub resolution_backed_eligible_markets: usize,
    pub missing_resolution_markets: usize,
    pub missing_entry_markets: usize,
    pub boundary_excluded_market_count: usize,
    pub by_symbol: Vec<RecordedMarketCoverageSummary>,
    pub missing_markets: Vec<RecordedMarketMissingRecord>,
    pub boundary_excluded_markets: Vec<RecordedMarketRecord>,
    pub recorded_markets: Vec<RecordedMarketRecord>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RecordedMarketPopulation {
    pub records: Vec<RecordedMarketRecord>,
}

impl RecordedMarketPopulation {
    pub(crate) fn from_records(records: Vec<RecordedMarketRecord>) -> Result<Self> {
        let mut by_identity = BTreeMap::new();
        for mut record in records {
            record.symbol = canonical_symbol(&record.symbol)?.to_string();
            anyhow::ensure!(
                record.window_start_ms < record.window_end_ms,
                "recorded market population has invalid window symbol={} market_id={} start={} end={}",
                record.symbol,
                record.market_id,
                record.window_start_ms,
                record.window_end_ms
            );
            let key = (record.symbol.clone(), record.market_id.clone());
            if let Some(previous) = by_identity.insert(key, record.clone()) {
                anyhow::ensure!(
                    previous == record,
                    "recorded market population identity changed symbol={} market_id={}",
                    record.symbol,
                    record.market_id
                );
            }
        }
        let mut records = by_identity.into_values().collect::<Vec<_>>();
        records.sort_by(|left, right| {
            (&left.symbol, left.window_start_ms, &left.market_id).cmp(&(
                &right.symbol,
                right.window_start_ms,
                &right.market_id,
            ))
        });
        Ok(Self { records })
    }

    pub fn merge(&mut self, other: Self) -> Result<()> {
        self.records.extend(other.records);
        *self = Self::from_records(std::mem::take(&mut self.records))?;
        Ok(())
    }

    pub fn retain_symbol(&mut self, symbol: &str) {
        self.records
            .retain(|record| record.symbol.eq_ignore_ascii_case(symbol));
    }

    pub fn records(&self) -> &[RecordedMarketRecord] {
        &self.records
    }

    pub fn expected_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| record.disposition.included_in_expected())
            .count()
    }

    pub fn eligible_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| {
                record.disposition == RecordedMarketDisposition::ResolutionBackedEligible
            })
            .count()
    }

    pub fn boundary_excluded_count(&self) -> usize {
        self.records
            .iter()
            .filter(|record| {
                record.disposition == RecordedMarketDisposition::ExcludedReplayBoundary
            })
            .count()
    }

    pub fn project_entered<'a>(
        &self,
        entered_markets: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> RecordedMarketCoverageReport {
        let entered_markets = entered_markets
            .into_iter()
            .map(|(symbol, market_id)| {
                (symbol.to_ascii_uppercase(), market_id.to_ascii_lowercase())
            })
            .collect::<BTreeSet<_>>();
        let mut by_symbol = BTreeMap::<String, RecordedMarketCoverageSummary>::new();
        let mut missing_markets = Vec::new();
        let mut boundary_excluded_markets = Vec::new();
        for market in &self.records {
            let summary = by_symbol.entry(market.symbol.clone()).or_insert_with(|| {
                RecordedMarketCoverageSummary {
                    symbol: market.symbol.clone(),
                    ..RecordedMarketCoverageSummary::default()
                }
            });
            match market.disposition {
                RecordedMarketDisposition::ExcludedReplayBoundary => {
                    summary.boundary_excluded_markets += 1;
                    boundary_excluded_markets.push(market.clone());
                }
                disposition => {
                    summary.expected_markets += 1;
                    if disposition == RecordedMarketDisposition::ResolutionBackedEligible {
                        summary.resolution_backed_eligible_markets += 1;
                    } else {
                        summary.missing_resolution_markets += 1;
                    }
                    if entered_markets
                        .contains(&(market.symbol.clone(), market.market_id.to_ascii_lowercase()))
                    {
                        summary.entered_markets += 1;
                    } else {
                        summary.missing_entry_markets += 1;
                        missing_markets.push(RecordedMarketMissingRecord {
                            market: market.clone(),
                            report_disposition: if disposition
                                == RecordedMarketDisposition::ResolutionBackedEligible
                            {
                                RecordedMarketReportDisposition::EligibleNotEntered
                            } else {
                                RecordedMarketReportDisposition::ExcludedBeforeReplayEntry
                            },
                        });
                    }
                }
            }
        }
        let by_symbol = by_symbol.into_values().collect::<Vec<_>>();
        RecordedMarketCoverageReport {
            entered_markets: by_symbol.iter().map(|row| row.entered_markets).sum(),
            expected_markets: by_symbol.iter().map(|row| row.expected_markets).sum(),
            resolution_backed_eligible_markets: by_symbol
                .iter()
                .map(|row| row.resolution_backed_eligible_markets)
                .sum(),
            missing_resolution_markets: by_symbol
                .iter()
                .map(|row| row.missing_resolution_markets)
                .sum(),
            missing_entry_markets: by_symbol.iter().map(|row| row.missing_entry_markets).sum(),
            boundary_excluded_market_count: by_symbol
                .iter()
                .map(|row| row.boundary_excluded_markets)
                .sum(),
            by_symbol,
            missing_markets,
            boundary_excluded_markets,
            recorded_markets: self.records.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MarketCatalogSource<'a> {
    pub market_id: &'a str,
    pub condition_id: &'a str,
    pub condition_id_b256: Option<&'a str>,
    pub gamma_market_id: &'a str,
    pub underlying: &'a str,
    pub up_token: U256,
    pub down_token: U256,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct MarketIdentityKey {
    window_start_ms: u64,
    condition_id: [u8; 32],
    up_token: [u8; 32],
    down_token: [u8; 32],
    window_end_ms: u64,
    gamma_market_id: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MarketCatalogRecord {
    pub market_handle: u32,
    pub condition_id: [u8; 32],
    pub up_token_handle: u32,
    pub up_token: [u8; 32],
    pub down_token_handle: u32,
    pub down_token: [u8; 32],
    pub window_start_ms: u64,
    pub window_end_ms: u64,
    pub gamma_market_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MarketCatalog {
    symbol: &'static str,
    records: Box<[MarketCatalogRecord]>,
    sha256: [u8; 32],
}

impl MarketCatalog {
    pub(crate) fn from_sources(symbol: &str, sources: &[MarketCatalogSource<'_>]) -> Result<Self> {
        Self::build(symbol, sources)
    }

    fn build(symbol: &str, sources: &[MarketCatalogSource<'_>]) -> Result<Self> {
        let symbol = canonical_symbol(symbol)?;
        let mut condition_owners = BTreeMap::new();
        let mut token_owners = BTreeMap::new();
        let mut identities = BTreeSet::new();
        for source in sources {
            let identity = validate_source(symbol, source)?;
            if let Some(previous) = condition_owners.insert(identity.condition_id, identity) {
                anyhow::ensure!(
                    previous == identity,
                    "market catalog condition identity changed condition={}",
                    source.condition_id
                );
            }
            for token in [identity.up_token, identity.down_token] {
                if let Some(previous) = token_owners.insert(token, identity.condition_id) {
                    anyhow::ensure!(
                        previous == identity.condition_id,
                        "market catalog token reused across conditions token={} condition={}",
                        U256::from_be_bytes(token),
                        source.condition_id
                    );
                }
            }
            identities.insert(identity);
        }
        anyhow::ensure!(
            !identities.is_empty(),
            "market catalog is empty symbol={symbol}"
        );
        anyhow::ensure!(
            identities.len() <= (u32::MAX / 2) as usize,
            "market catalog exceeds derived token handle range symbol={symbol} markets={}",
            identities.len()
        );
        let records = identities
            .into_iter()
            .enumerate()
            .map(|(index, identity)| record_for_index(index, identity))
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice();
        let sha256 = catalog_sha256(symbol, &records)?;
        let catalog = Self {
            symbol,
            records,
            sha256,
        };
        catalog.validate_resolvers()?;
        Ok(catalog)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub(crate) fn sha256(&self) -> [u8; 32] {
        self.sha256
    }

    pub(crate) fn symbol(&self) -> &'static str {
        self.symbol
    }

    pub(crate) fn records(&self) -> &[MarketCatalogRecord] {
        &self.records
    }

    pub(crate) fn record(&self, market_handle: u32) -> Option<&MarketCatalogRecord> {
        let index = usize::try_from(market_handle.checked_sub(1)?).ok()?;
        self.records.get(index)
    }

    pub(crate) fn market_handle_for_condition(&self, condition_id: [u8; 32]) -> Option<u32> {
        self.records
            .iter()
            .find(|record| record.condition_id == condition_id)
            .map(|record| record.market_handle)
    }

    pub(crate) fn token_handle(&self, token: [u8; 32]) -> Option<u32> {
        self.records.iter().find_map(|record| {
            if record.up_token == token {
                Some(record.up_token_handle)
            } else if record.down_token == token {
                Some(record.down_token_handle)
            } else {
                None
            }
        })
    }

    fn validate_resolvers(&self) -> Result<()> {
        for record in self.records() {
            anyhow::ensure!(
                self.record(record.market_handle)
                    .is_some_and(|found| found.condition_id == record.condition_id),
                "market catalog handle resolver mismatch handle={}",
                record.market_handle
            );
            anyhow::ensure!(
                self.market_handle_for_condition(record.condition_id) == Some(record.market_handle),
                "market catalog condition resolver mismatch handle={}",
                record.market_handle
            );
            anyhow::ensure!(
                self.token_handle(record.up_token) == Some(record.up_token_handle)
                    && self.token_handle(record.down_token) == Some(record.down_token_handle),
                "market catalog token resolver mismatch handle={}",
                record.market_handle
            );
        }
        Ok(())
    }
}

fn validate_source(symbol: &str, source: &MarketCatalogSource<'_>) -> Result<MarketIdentityKey> {
    anyhow::ensure!(
        source.underlying.trim().eq_ignore_ascii_case(symbol),
        "market catalog row has foreign symbol expected={} actual={}",
        symbol,
        source.underlying
    );
    let market_id = parse_identity(source.market_id, "market_id")?;
    let condition_id = parse_identity(source.condition_id, "condition_id")?;
    let raw_gamma_market_id = source.gamma_market_id.trim();
    let gamma_market_id = raw_gamma_market_id
        .parse::<u64>()
        .with_context(|| format!("invalid gamma_market_id `{}`", source.gamma_market_id))?;
    anyhow::ensure!(
        gamma_market_id > 0 && gamma_market_id.to_string() == raw_gamma_market_id,
        "gamma_market_id is not canonical positive decimal `{}`",
        source.gamma_market_id
    );
    anyhow::ensure!(
        market_id == condition_id,
        "market catalog market/condition identity mismatch market_id={} condition_id={}",
        source.market_id,
        source.condition_id
    );
    if let Some(raw) = source.condition_id_b256 {
        anyhow::ensure!(
            parse_identity(raw, "condition_id_b256")? == condition_id,
            "market catalog condition identity mismatch condition_id={} condition_id_b256={}",
            source.condition_id,
            raw
        );
    }
    let identity = MarketIdentityKey {
        window_start_ms: source.window_start_ms,
        condition_id,
        up_token: source.up_token.to_be_bytes::<32>(),
        down_token: source.down_token.to_be_bytes::<32>(),
        window_end_ms: source.window_end_ms,
        gamma_market_id,
    };
    validate_identity_values(identity)?;
    Ok(identity)
}

fn record_for_index(index: usize, identity: MarketIdentityKey) -> Result<MarketCatalogRecord> {
    validate_identity_values(identity)?;
    let market_handle = u32::try_from(index)
        .context("market catalog index exceeds u32")?
        .checked_add(1)
        .context("market catalog handle overflow")?;
    let down_token_handle = market_handle
        .checked_mul(2)
        .context("market catalog down-token handle overflow")?;
    Ok(MarketCatalogRecord {
        market_handle,
        condition_id: identity.condition_id,
        up_token_handle: down_token_handle - 1,
        up_token: identity.up_token,
        down_token_handle,
        down_token: identity.down_token,
        window_start_ms: identity.window_start_ms,
        window_end_ms: identity.window_end_ms,
        gamma_market_id: identity.gamma_market_id,
    })
}

fn validate_identity_values(identity: MarketIdentityKey) -> Result<()> {
    anyhow::ensure!(
        identity.window_start_ms < identity.window_end_ms,
        "market catalog invalid window start={} end={}",
        identity.window_start_ms,
        identity.window_end_ms
    );
    anyhow::ensure!(
        identity.gamma_market_id > 0,
        "market catalog gamma identity is zero"
    );
    anyhow::ensure!(
        identity.condition_id.iter().any(|byte| *byte != 0),
        "market catalog zero condition identity"
    );
    anyhow::ensure!(
        identity.up_token.iter().any(|byte| *byte != 0)
            && identity.down_token.iter().any(|byte| *byte != 0),
        "market catalog zero token condition={}",
        B256::from(identity.condition_id)
    );
    anyhow::ensure!(
        identity.up_token != identity.down_token,
        "market catalog duplicate leg token condition={}",
        B256::from(identity.condition_id)
    );
    Ok(())
}

fn canonical_symbol(symbol: &str) -> Result<&'static str> {
    match symbol.trim().to_ascii_uppercase().as_str() {
        "BTC" => Ok("BTC"),
        "ETH" => Ok("ETH"),
        "SOL" => Ok("SOL"),
        "XRP" => Ok("XRP"),
        other => anyhow::bail!("unsupported market catalog symbol `{other}`"),
    }
}

fn symbol_id(symbol: &str) -> u8 {
    match symbol {
        "BTC" => 0,
        "ETH" => 1,
        "SOL" => 2,
        "XRP" => 3,
        _ => unreachable!("validated market catalog symbol"),
    }
}

fn parse_identity(raw: &str, name: &str) -> Result<[u8; 32]> {
    let identity = B256::from_str(raw.trim()).with_context(|| format!("invalid {name} `{raw}`"))?;
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(identity.as_slice());
    Ok(bytes)
}

fn catalog_sha256(symbol: &str, records: &[MarketCatalogRecord]) -> Result<[u8; 32]> {
    let count = u32::try_from(records.len()).context("market catalog count exceeds u32")?;
    let mut hasher = Sha256::new();
    hasher.update(CATALOG_SCHEMA);
    hasher.update([symbol_id(symbol), 0, 0, 0]);
    hasher.update(count.to_le_bytes());
    for record in records {
        hasher.update(record.market_handle.to_le_bytes());
        hasher.update(record.condition_id);
        hasher.update(record.up_token_handle.to_le_bytes());
        hasher.update(record.up_token);
        hasher.update(record.down_token_handle.to_le_bytes());
        hasher.update(record.down_token);
        hasher.update(record.window_start_ms.to_le_bytes());
        hasher.update(record.window_end_ms.to_le_bytes());
        hasher.update(record.gamma_market_id.to_le_bytes());
    }
    Ok(hasher.finalize().into())
}
