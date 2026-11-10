use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, RwLock};

use anyhow::Result;
use polymarket_client_sdk::types::{B256, U256};

use crate::polymarket::state::MarketProjection;
use crate::strategy_runtime::market_catalog::MarketCatalogRecord;

#[derive(Debug, Default)]
struct SymbolOwnershipInner {
    market_symbols: BTreeMap<String, String>,
    token_symbols: BTreeMap<String, String>,
    cohort_symbols: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct SymbolOwnershipIndex {
    inner: Arc<RwLock<SymbolOwnershipInner>>,
}

impl SymbolOwnershipIndex {
    pub fn from_projection(projection: &MarketProjection) -> Result<Self> {
        let index = Self::default();
        index.register_projection(projection)?;
        Ok(index)
    }

    pub fn register_projection(&self, projection: &MarketProjection) -> Result<()> {
        for market in projection.markets() {
            self.claim_market(market.market_id.as_str(), &market.underlying)?;
            self.claim_token(market.up_token_id(), &market.underlying)?;
            self.claim_token(market.down_token_id(), &market.underlying)?;
        }
        Ok(())
    }

    pub fn market_belongs(&self, market_id: &str, symbol: &str) -> bool {
        self.symbol_for_market(market_id)
            .is_some_and(|owner| owner.eq_ignore_ascii_case(symbol))
    }

    pub fn token_belongs(&self, token: &str, symbol: &str) -> bool {
        self.symbol_for_token(token)
            .is_some_and(|owner| owner.eq_ignore_ascii_case(symbol))
    }

    pub fn cohort_belongs(&self, cohort: &str, symbol: &str) -> bool {
        self.inner
            .read()
            .expect("symbol ownership lock poisoned")
            .cohort_symbols
            .get(cohort)
            .cloned()
            .is_some_and(|owner| owner.eq_ignore_ascii_case(symbol))
    }

    pub fn symbol_for_market(&self, market_id: &str) -> Option<String> {
        self.inner
            .read()
            .expect("symbol ownership lock poisoned")
            .market_symbols
            .get(market_id)
            .cloned()
    }

    pub fn symbol_for_token(&self, token: &str) -> Option<String> {
        self.inner
            .read()
            .expect("symbol ownership lock poisoned")
            .token_symbols
            .get(token)
            .cloned()
    }

    pub fn facts_belong_to(
        &self,
        expected_symbol: &str,
        explicit_symbol: Option<&str>,
        market_id: Option<&str>,
        tokens: &[&str],
    ) -> bool {
        let mut owners = Vec::new();
        if let Some(symbol) = explicit_symbol.filter(|symbol| !symbol.trim().is_empty()) {
            owners.push(symbol.trim().to_ascii_uppercase());
        }
        if let Some(market_id) = market_id {
            let Some(owner) = self.symbol_for_market(market_id) else {
                return false;
            };
            owners.push(owner);
        }
        for token in tokens {
            let Some(owner) = self.symbol_for_token(token) else {
                return false;
            };
            owners.push(owner);
        }
        !owners.is_empty()
            && owners
                .iter()
                .all(|owner| owner.eq_ignore_ascii_case(expected_symbol))
    }

    pub fn markets_for_symbol(&self, symbol: &str) -> HashSet<String> {
        identities_for_symbol(
            &self
                .inner
                .read()
                .expect("symbol ownership lock poisoned")
                .market_symbols,
            symbol,
        )
    }

    pub fn tokens_for_symbol(&self, symbol: &str) -> HashSet<String> {
        identities_for_symbol(
            &self
                .inner
                .read()
                .expect("symbol ownership lock poisoned")
                .token_symbols,
            symbol,
        )
    }

    pub fn claim_cohort(&self, cohort: &str, symbol: &str) -> Result<()> {
        claim(
            &mut self
                .inner
                .write()
                .expect("symbol ownership lock poisoned")
                .cohort_symbols,
            cohort,
            symbol,
            "cohort",
        )
    }

    pub(crate) fn claim_frozen_market_catalog(
        &self,
        records: &[MarketCatalogRecord],
        symbol: &str,
    ) -> Result<()> {
        let (market_claims, token_claims) = canonical_catalog_claims(records, symbol)?;
        let mut inner = self.inner.write().expect("symbol ownership lock poisoned");
        ensure_claims_compatible(&inner.market_symbols, &market_claims, "market")?;
        ensure_claims_compatible(&inner.token_symbols, &token_claims, "token")?;
        inner.market_symbols.extend(market_claims);
        inner.token_symbols.extend(token_claims);
        Ok(())
    }

    fn claim_market(&self, market_id: &str, symbol: &str) -> Result<()> {
        claim(
            &mut self
                .inner
                .write()
                .expect("symbol ownership lock poisoned")
                .market_symbols,
            market_id,
            symbol,
            "market",
        )
    }

    fn claim_token(&self, token: &str, symbol: &str) -> Result<()> {
        claim(
            &mut self
                .inner
                .write()
                .expect("symbol ownership lock poisoned")
                .token_symbols,
            token,
            symbol,
            "token",
        )
    }
}

fn identities_for_symbol(index: &BTreeMap<String, String>, symbol: &str) -> HashSet<String> {
    index
        .iter()
        .filter(|(_, owner)| owner.eq_ignore_ascii_case(symbol))
        .map(|(identity, _)| identity.clone())
        .collect()
}

fn claim(
    index: &mut BTreeMap<String, String>,
    identity: &str,
    symbol: &str,
    kind: &str,
) -> Result<()> {
    let identity = identity.trim();
    let symbol = normalized_symbol(symbol, kind, identity)?;
    anyhow::ensure!(
        !identity.is_empty(),
        "empty {kind} symbol ownership identity"
    );
    if let Some(previous) = index.get(identity) {
        anyhow::ensure!(
            previous == &symbol,
            "conflicting {kind} symbol ownership identity={identity} previous={previous} next={symbol}"
        );
        return Ok(());
    }
    index.insert(identity.to_string(), symbol);
    Ok(())
}

fn canonical_catalog_claims(
    records: &[MarketCatalogRecord],
    symbol: &str,
) -> Result<(BTreeMap<String, String>, BTreeMap<String, String>)> {
    let symbol = normalized_symbol(symbol, "market catalog", "batch")?;
    let mut market_claims = BTreeMap::new();
    let mut token_claims = BTreeMap::new();
    for record in records {
        let market_id = B256::from(record.condition_id).to_string();
        let up_token = U256::from_be_bytes(record.up_token).to_string();
        let down_token = U256::from_be_bytes(record.down_token).to_string();
        stage_claim(&mut market_claims, market_id.clone(), &symbol, "market")?;
        for token in [&up_token, &down_token] {
            stage_claim(&mut token_claims, token.clone(), &symbol, "token")?;
        }
    }
    Ok((market_claims, token_claims))
}

fn stage_claim(
    claims: &mut BTreeMap<String, String>,
    identity: String,
    symbol: &str,
    kind: &str,
) -> Result<()> {
    anyhow::ensure!(
        !identity.is_empty(),
        "empty {kind} symbol ownership identity"
    );
    if let Some(previous) = claims.insert(identity.clone(), symbol.to_string()) {
        anyhow::ensure!(
            previous == symbol,
            "conflicting staged {kind} symbol ownership identity={identity} previous={previous} next={symbol}"
        );
    }
    Ok(())
}

fn ensure_claims_compatible(
    existing: &BTreeMap<String, String>,
    staged: &BTreeMap<String, String>,
    kind: &str,
) -> Result<()> {
    for (identity, symbol) in staged {
        if let Some(previous) = existing.get(identity) {
            anyhow::ensure!(
                previous == symbol,
                "conflicting {kind} symbol ownership identity={identity} previous={previous} next={symbol}"
            );
        }
    }
    Ok(())
}

fn normalized_symbol(symbol: &str, kind: &str, identity: &str) -> Result<String> {
    let symbol = symbol.trim().to_ascii_uppercase();
    anyhow::ensure!(
        crate::config::VALID_ASSETS.contains(&symbol.as_str()),
        "invalid {kind} symbol ownership symbol={symbol} identity={identity}"
    );
    Ok(symbol)
}
