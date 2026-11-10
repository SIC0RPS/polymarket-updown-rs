use anyhow::{Context, Result};
use polymarket_client_sdk::data::types::response::Position;
use polymarket_client_sdk::data::types::{
    PositionSortBy, PositionStatus, SortDirection, TradeFilter,
};
use polymarket_client_sdk::data::Client as DataClient;
use polymarket_client_sdk::types::{Address, Decimal};

use crate::polymarket::api_control::retry_sdk_read;

const POSITIONS_PAGE_SIZE: i32 = 500;

#[derive(Debug, Clone, Copy)]
pub(crate) enum PositionScope {
    Redeemable,
    Mergeable,
}

impl PositionScope {
    fn label(self) -> &'static str {
        match self {
            Self::Redeemable => "redeemable",
            Self::Mergeable => "mergeable",
        }
    }
}

pub(crate) async fn fetch_complete_positions(
    data: &DataClient,
    user: Address,
    scope: PositionScope,
) -> Result<Vec<Position>> {
    let mut request = polymarket_client_sdk::data::types::request::PositionsRequest::builder()
        .user(user)
        .limit(POSITIONS_PAGE_SIZE)
        .context("invalid positions page size")?
        .sort_by(PositionSortBy::Tokens)
        .sort_direction(SortDirection::Desc)
        .status(match scope {
            PositionScope::Redeemable => PositionStatus::Redeemable,
            PositionScope::Mergeable => PositionStatus::Mergeable,
        })
        .trade_filter(TradeFilter::tokens(Decimal::ZERO).context("invalid positions token filter")?)
        .build();
    let mut positions = Vec::new();
    loop {
        let page = retry_sdk_read(|| data.positions(&request))
            .await
            .with_context(|| format!("{} positions request failed", scope.label()))?;
        let next_cursor = page
            .pagination
            .into_next_cursor()
            .with_context(|| format!("{} positions pagination is invalid", scope.label()))?;
        positions.extend(page.data);
        match next_cursor {
            Some(cursor) => request.cursor = Some(cursor),
            None => return Ok(positions),
        }
    }
}
