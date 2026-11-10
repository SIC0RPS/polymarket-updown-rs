use std::borrow::Borrow;
use std::fmt;
use std::ops::Deref;

use polymarket_client_sdk::types::B256;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MarketId(String);

impl MarketId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into().trim().to_string())
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for MarketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Deref for MarketId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl Borrow<str> for MarketId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for MarketId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq<&str> for MarketId {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for MarketId {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other.as_str()
    }
}

impl From<&str> for MarketId {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for MarketId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct MarketIdentity {
    pub id: MarketId,
    pub gamma_market_id: String,
    pub condition_id_b256: Option<B256>,
}

impl MarketIdentity {
    pub fn new(
        id: impl Into<MarketId>,
        gamma_market_id: impl Into<String>,
        condition_id_b256: Option<B256>,
    ) -> Self {
        Self {
            id: id.into(),
            gamma_market_id: gamma_market_id.into(),
            condition_id_b256,
        }
    }
}
