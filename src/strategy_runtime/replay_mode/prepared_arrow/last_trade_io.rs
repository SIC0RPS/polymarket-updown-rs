use std::str::FromStr;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::U256;

use super::io::{float_col, int_col, opt_str, read_batches, req_f64, req_i64, req_str, str_col};
use super::LastTradeRow;
use crate::polymarket::state::TradeSide;

pub(super) fn read_last_trade_rows(path: &std::path::Path) -> Result<Vec<LastTradeRow>> {
    let mut rows = Vec::new();
    for batch in read_batches(path)? {
        let session_ts = int_col(&batch, 0, "session_ts_ms")?;
        let seq = int_col(&batch, 1, "seq")?;
        let token = str_col(&batch, 2, "token_id")?;
        let price = float_col(&batch, 3, "price")?;
        let size = float_col(&batch, 4, "size")?;
        let side = str_col(&batch, 5, "side")?;
        let trade_ts = int_col(&batch, 6, "trade_ts_ms")?;
        for idx in 0..batch.num_rows() {
            rows.push(LastTradeRow {
                session_ts_ms: req_i64(session_ts, idx)? as u64,
                seq: req_i64(seq, idx)? as u64,
                token_id: U256::from_str(req_str(token, idx)?).with_context(|| {
                    format!("invalid last_trade_delta token_id at row {}", rows.len())
                })?,
                price: req_f64(price, idx)?,
                size: req_f64(size, idx)?,
                side: opt_str(side, idx).and_then(parse_trade_side),
                trade_ts_ms: req_i64(trade_ts, idx)? as u64,
            });
        }
    }
    rows.sort_by_key(|row| (row.session_ts_ms, row.seq));
    Ok(rows)
}

fn parse_trade_side(raw: &str) -> Option<TradeSide> {
    match raw {
        "BUY" | "buy" => Some(TradeSide::Buy),
        "SELL" | "sell" => Some(TradeSide::Sell),
        _ => None,
    }
}
