use std::fmt;

use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub(crate) mod u128_string {
    pub(crate) use super::{
        deserialize_u128_string as deserialize, serialize_u128_string as serialize,
    };
}

pub(crate) mod optional_u128_string {
    pub(crate) use super::{
        deserialize_optional_u128_string as deserialize,
        serialize_optional_u128_string as serialize,
    };
}

pub(crate) mod optional_i128_string {
    pub(crate) use super::{
        deserialize_optional_i128_string as deserialize,
        serialize_optional_i128_string as serialize,
    };
}

pub(crate) mod u128_string_array_2 {
    pub(crate) use super::{
        deserialize_u128_string_array_2 as deserialize, serialize_u128_string_array_2 as serialize,
    };
}

pub(crate) mod u64_string {
    pub(crate) use super::{
        deserialize_u64_string as deserialize, serialize_u64_string as serialize,
    };
}

pub(crate) fn serialize_i128_string<S>(value: &i128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

pub(crate) fn deserialize_i128_string<'de, D>(deserializer: D) -> Result<i128, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ExactI128 {
        String(String),
        Integer(i128),
    }

    match ExactI128::deserialize(deserializer)? {
        ExactI128::String(value) => value.parse().map_err(serde::de::Error::custom),
        ExactI128::Integer(value) => Ok(value),
    }
}

pub(crate) fn serialize_optional_i128_string<S>(
    value: &Option<i128>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}

pub(crate) fn deserialize_optional_i128_string<'de, D>(
    deserializer: D,
) -> Result<Option<i128>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| value.parse().map_err(serde::de::Error::custom))
        .transpose()
}

pub(crate) fn serialize_u128_string<S>(value: &u128, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

pub(crate) fn deserialize_u128_string<'de, D>(deserializer: D) -> Result<u128, D::Error>
where
    D: Deserializer<'de>,
{
    ExactU128::deserialize(deserializer).map(|value| value.0)
}

pub(crate) fn serialize_optional_u128_string<S>(
    value: &Option<u128>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}

pub(crate) fn deserialize_optional_u128_string<'de, D>(
    deserializer: D,
) -> Result<Option<u128>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<ExactU128>::deserialize(deserializer).map(|value| value.map(|value| value.0))
}

pub(crate) fn serialize_u128_string_array_2<S>(
    value: &[u128; 2],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    [value[0].to_string(), value[1].to_string()].serialize(serializer)
}

pub(crate) fn deserialize_u128_string_array_2<'de, D>(
    deserializer: D,
) -> Result<[u128; 2], D::Error>
where
    D: Deserializer<'de>,
{
    <[ExactU128; 2]>::deserialize(deserializer).map(|[left, right]| [left.0, right.0])
}

struct ExactU128(u128);

impl<'de> Deserialize<'de> for ExactU128 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(ExactU128Visitor)
    }
}

struct ExactU128Visitor;

impl<'de> Visitor<'de> for ExactU128Visitor {
    type Value = ExactU128;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a decimal u128 string or an unsigned integer fitting u64")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(ExactU128(u128::from(value)))
    }

    fn visit_u128<E>(self, value: u128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        u64::try_from(value)
            .map(|value| ExactU128(u128::from(value)))
            .map_err(|_| E::custom("numeric u128 exceeds the supported u64 input range"))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        u64::try_from(value)
            .map(|value| ExactU128(u128::from(value)))
            .map_err(|_| E::custom("signed integer is negative"))
    }

    fn visit_i128<E>(self, value: i128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        u64::try_from(value)
            .map(|value| ExactU128(u128::from(value)))
            .map_err(|_| E::custom("signed integer is outside the supported u64 input range"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        value.parse::<u128>().map(ExactU128).map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(&value)
    }
}

pub(crate) fn serialize_u64_string<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

pub(crate) fn deserialize_u64_string<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ExactU64 {
        String(String),
        Integer(u64),
    }

    match ExactU64::deserialize(deserializer)? {
        ExactU64::String(value) => value.parse().map_err(serde::de::Error::custom),
        ExactU64::Integer(value) => Ok(value),
    }
}

pub(crate) fn serialize_optional_u64_string<S>(
    value: &Option<u64>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}

pub(crate) fn deserialize_optional_u64_string<'de, D>(
    deserializer: D,
) -> Result<Option<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| value.parse().map_err(serde::de::Error::custom))
        .transpose()
}
