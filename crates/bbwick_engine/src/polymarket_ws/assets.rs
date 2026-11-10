use polymarket_client_sdk::types::U256;

pub(super) fn dedup_asset_ids(asset_ids: Vec<U256>) -> Vec<U256> {
    let mut out = asset_ids;
    out.sort_unstable();
    out.dedup();
    out
}
