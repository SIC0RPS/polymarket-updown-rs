use serde::de::{self, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};

use super::{deserialize_numeric_f64, positive_finite};
use crate::ids::{IdTickFrame, MarketDataKeyRegistry};
use crate::oracle_history::AggregateTrade;

#[derive(Debug, Deserialize)]
pub(super) struct AggTradeEvent<'a> {
    #[serde(borrow, rename = "s")]
    pub(super) symbol: &'a str,
    #[serde(rename = "p", deserialize_with = "deserialize_numeric_f64")]
    price: f64,
    #[serde(default, rename = "T")]
    trade_ts_ms: Option<u64>,
    #[serde(default, rename = "E")]
    event_ts_ms: Option<u64>,
    #[serde(default, rename = "q")]
    quantity: FlowField,
    #[serde(default, rename = "a")]
    aggregate_id: FlowField,
    #[serde(default, rename = "m")]
    buyer_is_maker: FlowField,
}

// Invalid optional facts must not reject the existing price payload.
#[derive(Debug, Default)]
enum FlowField {
    #[default]
    Missing,
    Boolean(bool),
    Unsigned(u64),
    Number(f64),
}

impl<'de> Deserialize<'de> for FlowField {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FlowFieldVisitor;

        impl<'de> Visitor<'de> for FlowFieldVisitor {
            type Value = FlowField;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an optional aggregate-trade fact")
            }

            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Self::Value, E> {
                Ok(FlowField::Boolean(value))
            }

            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Self::Value, E> {
                Ok(FlowField::Unsigned(value))
            }

            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Self::Value, E> {
                Ok(u64::try_from(value)
                    .map(FlowField::Unsigned)
                    .unwrap_or_default())
            }

            fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
                Ok(FlowField::Number(value))
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                Ok(value.parse().map(FlowField::Number).unwrap_or_default())
            }

            fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
                Ok(FlowField::Missing)
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                while sequence.next_element::<IgnoredAny>()?.is_some() {}
                Ok(FlowField::Missing)
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                while map.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(FlowField::Missing)
            }
        }

        deserializer.deserialize_any(FlowFieldVisitor)
    }
}

impl AggTradeEvent<'_> {
    pub(super) fn flow_trade(&self) -> Option<AggregateTrade> {
        let FlowField::Unsigned(aggregate_id) = self.aggregate_id else {
            return None;
        };
        let FlowField::Boolean(buyer_is_maker) = self.buyer_is_maker else {
            return None;
        };
        let quantity = match self.quantity {
            FlowField::Unsigned(value) => value as f64,
            FlowField::Number(value) => value,
            _ => return None,
        };
        Some(AggregateTrade {
            aggregate_id,
            quantity: positive_finite(quantity)?,
            buyer_is_maker,
            event_ts_ms: self.event_ts_ms,
            trade_ts_ms: self.trade_ts_ms,
        })
    }
}

pub(super) fn parse_agg_trade_event(
    payload: &AggTradeEvent<'_>,
    key_registry: &MarketDataKeyRegistry,
    default_wall_ms: u64,
) -> Option<IdTickFrame> {
    let symbol_id = key_registry.symbol_id(payload.symbol.trim())?;
    let price = positive_finite(payload.price)?;
    let ts_ms = payload
        .trade_ts_ms
        .or(payload.event_ts_ms)
        .unwrap_or(default_wall_ms);
    Some(IdTickFrame {
        symbol_id,
        ts_ms,
        price,
    })
}
