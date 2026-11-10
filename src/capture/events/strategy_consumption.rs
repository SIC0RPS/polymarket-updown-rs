use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum StrategyConsumptionInput {
    Runtime(crate::strategy_runtime::runtime_driver::RuntimeInput),
    Chainlink {
        price: crate::oracle_resolution::ChainlinkResolutionPrice,
        learning_applied_ts_ms: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StrategyConsumptionKind {
    Runtime,
    Chainlink,
}

impl StrategyConsumptionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Runtime => "runtime",
            Self::Chainlink => "chainlink",
        }
    }
}

impl StrategyConsumptionInput {
    pub const fn kind(&self) -> StrategyConsumptionKind {
        match self {
            Self::Runtime(_) => StrategyConsumptionKind::Runtime,
            Self::Chainlink { .. } => StrategyConsumptionKind::Chainlink,
        }
    }
}

pub const STRATEGY_CONSUMPTION_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Serialize, Deserialize)]
pub struct StrategyConsumptionRecord {
    pub schema_version: u32,
    pub consumption_sequence: u64,
    pub consumed_at_ms: u64,
    #[serde(default)]
    pub consumed_mono_ns: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_flow: Option<bbwick_engine::OracleTradeFlowSnapshot>,
    #[serde(
        default,
        with = "consumed_candles",
        skip_serializing_if = "Option::is_none"
    )]
    pub candle_state: Option<std::sync::Arc<[std::sync::Arc<bbwick_engine::OracleCandleWindow>]>>,
    pub actor_symbol: crate::strategy_runtime::SymbolId,
    #[serde(with = "exact_odds_snapshot")]
    pub odds: std::sync::Arc<bbwick_engine::PolymarketOddsState>,
    pub input: StrategyConsumptionInput,
}

impl StrategyConsumptionRecord {
    pub fn validate_schema(&self) -> anyhow::Result<()> {
        Self::validate_schema_fields(
            self.schema_version,
            self.consumed_mono_ns,
            self.candle_state.as_ref().map(|state| state.len()),
            self.trade_flow.as_ref(),
        )
    }

    pub fn validate_schema_fields(
        version: u32,
        clock: u64,
        state_len: Option<usize>,
        trade_flow: Option<&bbwick_engine::OracleTradeFlowSnapshot>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            matches!(version, 1 | 2 | STRATEGY_CONSUMPTION_SCHEMA_VERSION),
            "unsupported strategy consumption schema={version}"
        );
        if version == STRATEGY_CONSUMPTION_SCHEMA_VERSION {
            let flow = trade_flow
                .ok_or_else(|| anyhow::anyhow!("missing consumed trade-flow snapshot"))?;
            flow.validate(clock)?;
        } else {
            anyhow::ensure!(
                trade_flow.is_none(),
                "legacy consumption carries trade flow"
            );
        }
        if version == 1 {
            anyhow::ensure!(
                clock == 0 && state_len.is_none(),
                "legacy strategy consumption carries candle availability"
            );
        } else {
            anyhow::ensure!(clock > 0, "missing consumption monotonic clock");
            anyhow::ensure!(state_len != Some(0), "empty consumed candle key set");
        }
        Ok(())
    }
}

mod consumed_candles {
    use bbwick_engine::OracleCandleWindow;
    use serde::{ser::SerializeSeq, Deserialize, Deserializer, Serializer};
    use std::sync::Arc;

    pub fn serialize<S: Serializer>(
        state: &Option<Arc<[Arc<OracleCandleWindow>]>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let Some(state) = state else {
            return serializer.serialize_none();
        };
        let mut sequence = serializer.serialize_seq(Some(state.len()))?;
        for window in state.iter() {
            sequence.serialize_element(window.as_ref())?;
        }
        sequence.end()
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Arc<[Arc<OracleCandleWindow>]>>, D::Error> {
        Ok(
            Option::<Vec<OracleCandleWindow>>::deserialize(deserializer)?
                .map(|windows| windows.into_iter().map(Arc::new).collect()),
        )
    }
}

mod exact_odds_snapshot {
    use std::collections::HashSet;
    use std::sync::Arc;

    use alloy::primitives::U256;
    use bbwick_engine::PolymarketOddsState;
    use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    struct ObservedClock {
        token: U256,
        observed_at_ms: u64,
    }

    #[derive(Serialize)]
    struct SnapshotRef<'a> {
        state: &'a PolymarketOddsState,
        books: Vec<ObservedClock>,
        depths: Vec<ObservedClock>,
        up: Option<u64>,
        down: Option<u64>,
        next_up: Option<u64>,
        next_down: Option<u64>,
    }

    #[derive(Deserialize)]
    struct SnapshotOwned {
        state: PolymarketOddsState,
        books: Vec<ObservedClock>,
        depths: Vec<ObservedClock>,
        up: Option<u64>,
        down: Option<u64>,
        next_up: Option<u64>,
        next_down: Option<u64>,
    }

    pub fn serialize<S>(value: &Arc<PolymarketOddsState>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        SnapshotRef {
            state: value.as_ref(),
            books: value
                .books
                .iter()
                .map(|(token, book)| ObservedClock {
                    token: *token,
                    observed_at_ms: book.observed_at_ms,
                })
                .collect(),
            depths: value
                .depths
                .iter()
                .map(|(token, depth)| ObservedClock {
                    token: *token,
                    observed_at_ms: depth.observed_at_ms,
                })
                .collect(),
            up: value.up.as_ref().map(|book| book.observed_at_ms),
            down: value.down.as_ref().map(|book| book.observed_at_ms),
            next_up: value.next_up.as_ref().map(|book| book.observed_at_ms),
            next_down: value.next_down.as_ref().map(|book| book.observed_at_ms),
        }
        .serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Arc<PolymarketOddsState>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let SnapshotOwned {
            mut state,
            books,
            depths,
            up,
            down,
            next_up,
            next_down,
        } = SnapshotOwned::deserialize(deserializer)?;

        if books.len() != state.books.len() || depths.len() != state.depths.len() {
            return Err(D::Error::custom("odds snapshot clock cardinality mismatch"));
        }
        let mut seen_books = HashSet::with_capacity(books.len());
        for clock in books {
            if !seen_books.insert(clock.token) {
                return Err(D::Error::custom("duplicate odds book clock"));
            }
            state
                .books
                .get_mut(&clock.token)
                .ok_or_else(|| D::Error::custom("odds book clock has no matching book"))?
                .observed_at_ms = clock.observed_at_ms;
        }
        let mut seen_depths = HashSet::with_capacity(depths.len());
        for clock in depths {
            if !seen_depths.insert(clock.token) {
                return Err(D::Error::custom("duplicate odds depth clock"));
            }
            state
                .depths
                .get_mut(&clock.token)
                .ok_or_else(|| D::Error::custom("odds depth clock has no matching depth"))?
                .observed_at_ms = clock.observed_at_ms;
        }
        restore_optional_clock(state.up.as_mut(), up).map_err(D::Error::custom)?;
        restore_optional_clock(state.down.as_mut(), down).map_err(D::Error::custom)?;
        restore_optional_clock(state.next_up.as_mut(), next_up).map_err(D::Error::custom)?;
        restore_optional_clock(state.next_down.as_mut(), next_down).map_err(D::Error::custom)?;
        Ok(Arc::new(state))
    }

    fn restore_optional_clock(
        book: Option<&mut bbwick_engine::state::BookTop>,
        clock: Option<u64>,
    ) -> Result<(), &'static str> {
        match (book, clock) {
            (Some(book), Some(clock)) => {
                book.observed_at_ms = clock;
                Ok(())
            }
            (None, None) => Ok(()),
            _ => Err("optional odds book clock presence mismatch"),
        }
    }
}
