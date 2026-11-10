pub(super) fn market_ws_endpoint(ws_url: &str) -> String {
    let trimmed = ws_url.trim_end_matches('/');
    if trimmed.ends_with("/ws/market") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/ws/market")
    }
}
