use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};

use super::{IntentRecord, MarketCatalog};
use crate::strategy_runtime::SymbolId;
use crate::types::DirectBuyAuthorization;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryContextRecord {
    Split(SplitEntryContextRecord),
    DirectBuy(DirectBuyAuthorization),
}

impl EntryContextRecord {
    pub fn intent_id(self) -> u64 {
        match self {
            Self::Split(record) => record.intent_id,
            Self::DirectBuy(authorization) => authorization.intent_id,
        }
    }

    pub fn set_intent_id(&mut self, intent_id: u64) {
        match self {
            Self::Split(record) => record.intent_id = intent_id,
            Self::DirectBuy(authorization) => authorization.intent_id = intent_id,
        }
    }

    pub fn decision_ts_ms(self) -> u64 {
        match self {
            Self::Split(record) => record.decision_ts_ms,
            Self::DirectBuy(authorization) => authorization.prediction_at_ms,
        }
    }

    pub fn validate(self) -> Result<Self> {
        match self {
            Self::Split(record) => {
                record.validate()?;
            }
            Self::DirectBuy(authorization) => {
                anyhow::ensure!(
                    authorization.intent_id > 0
                        && authorization.authorized_at_ms == 0
                        && authorization.prediction_at_ms > 0
                        && authorization.prediction_at_ms < authorization.market_end_ms
                        && authorization.target_qty_units_6dp > 0,
                    "BUY entry context contains invalid proposal fields"
                );
            }
        }
        Ok(self)
    }

    pub fn validate_intent(
        self,
        intent: IntentRecord,
        catalog: &MarketCatalog,
        source_symbol: SymbolId,
    ) -> Result<Self> {
        self.validate()?;
        let intent = intent.validate()?.validate_catalog(catalog)?;
        anyhow::ensure!(
            self.intent_id() == intent.intent_id && catalog.symbol() == source_symbol.as_str(),
            "entry context intent or source identity mismatch"
        );
        match self {
            Self::Split(_) => anyhow::ensure!(
                intent.action_kind == 2 && intent.side_or_operation == 1,
                "split context targets a non-split intent"
            ),
            Self::DirectBuy(authorization) => {
                let market = catalog
                    .record(intent.market_id)
                    .context("BUY context market is absent from frozen catalog")?;
                let selected_token = if authorization.selected_up {
                    market.up_token_handle
                } else {
                    market.down_token_handle
                };
                anyhow::ensure!(
                    intent.action_kind == 1
                        && intent.side_or_operation == 1
                        && authorization.symbol == source_symbol
                        && authorization.condition_id == B256::from(market.condition_id)
                        && authorization.up_token == U256::from_be_bytes(market.up_token)
                        && authorization.down_token == U256::from_be_bytes(market.down_token)
                        && intent.token_or_up_id == selected_token
                        && authorization.target_qty_units_6dp == intent.qty_or_amount_units_6dp
                        && authorization.market_end_ms == market.window_end_ms
                        && intent.expiration_ms == authorization.market_end_ms,
                    "BUY entry context differs from intent or frozen catalog"
                );
            }
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitEntryContextRecord {
    pub intent_id: u64,
    pub decision_ts_ms: u64,
    pub presence_mask: u64,
    pub oracle_side_reason: u8,
    pub oracle_symbol_len: u8,
    pub flags: u8,
    pub oracle_symbol: [u8; 32],
    pub reason_target_per_side_bits: u64,
    pub up_bid_bits: u64,
    pub up_ask_bits: u64,
    pub up_ts_ms: u64,
    pub down_bid_bits: u64,
    pub down_ask_bits: u64,
    pub down_ts_ms: u64,
    pub optional_metric_bits: [u64; 36],
    pub book_size_bits: [u64; 4],
}

impl SplitEntryContextRecord {
    pub fn validate(self) -> Result<Self> {
        anyhow::ensure!(self.intent_id > 0, "split context intent identity is zero");
        anyhow::ensure!(
            self.presence_mask >> 40 == 0,
            "split context presence mask contains unknown bits"
        );
        anyhow::ensure!(
            matches!(self.oracle_side_reason, 0..=8),
            "split context oracle-side-reason tag is invalid"
        );
        let oracle_symbol_len = usize::from(self.oracle_symbol_len);
        anyhow::ensure!(
            (1..=self.oracle_symbol.len()).contains(&oracle_symbol_len)
                && self.oracle_symbol[oracle_symbol_len..]
                    .iter()
                    .all(|byte| *byte == 0),
            "split context oracle-symbol identity is invalid"
        );
        let oracle_symbol = std::str::from_utf8(&self.oracle_symbol[..oracle_symbol_len])
            .context("split context oracle-symbol identity is not UTF-8")?;
        anyhow::ensure!(
            oracle_symbol.trim() == oracle_symbol
                && oracle_symbol
                    .chars()
                    .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit()),
            "split context oracle-symbol identity is noncanonical"
        );
        anyhow::ensure!(self.flags == 0, "split context contains forbidden flags");
        let target_per_side = f64::from_bits(self.reason_target_per_side_bits);
        anyhow::ensure!(
            target_per_side.is_finite() && target_per_side > 0.0,
            "split context reason target is invalid"
        );
        for (name, bits) in [
            ("up_bid", self.up_bid_bits),
            ("up_ask", self.up_ask_bits),
            ("down_bid", self.down_bid_bits),
            ("down_ask", self.down_ask_bits),
        ] {
            let value = f64::from_bits(bits);
            anyhow::ensure!(
                value.is_finite() && (0.0..=1.0).contains(&value),
                "split context {name} is outside probability range"
            );
        }
        anyhow::ensure!(
            f64::from_bits(self.up_ask_bits) >= f64::from_bits(self.up_bid_bits)
                && f64::from_bits(self.down_ask_bits) >= f64::from_bits(self.down_bid_bits),
            "split context book top is crossed"
        );
        anyhow::ensure!(
            self.decision_ts_ms > 0 && self.up_ts_ms > 0 && self.down_ts_ms > 0,
            "split context book/decision timestamps are invalid"
        );
        for (index, bits) in self.optional_metric_bits.iter().copied().enumerate() {
            validate_optional_f64(self.presence_mask, index, bits, false)?;
        }
        for (offset, bits) in self.book_size_bits.iter().copied().enumerate() {
            validate_optional_f64(self.presence_mask, 36 + offset, bits, true)?;
        }
        Ok(self)
    }
}

fn validate_optional_f64(mask: u64, index: usize, bits: u64, nonnegative: bool) -> Result<()> {
    if mask & (1u64 << index) == 0 {
        anyhow::ensure!(bits == 0, "absent split context value has nonzero bits");
        return Ok(());
    }
    let value = f64::from_bits(bits);
    anyhow::ensure!(value.is_finite(), "split context value is nonfinite");
    anyhow::ensure!(
        !nonnegative || value >= 0.0,
        "split context book size is negative"
    );
    Ok(())
}
