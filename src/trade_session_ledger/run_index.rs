use std::io::Write;
use std::sync::MutexGuard;

use super::*;
pub(super) fn update_run_index(record: &TradeLedgerRecord) -> Result<()> {
    let _guard = run_index_lock()?;
    let mut index = super::live_accounting::resident_run_index(&record.run_id)?;
    apply_record_to_run_index(&mut index, record);
    index.projection_source = TRADE_LEDGER_PROJECTION_SOURCE.to_string();
    index.projection_version = TRADE_LEDGER_PROJECTION_VERSION;
    index.market_count = index.markets.len();
    sort_run_index(&mut index);
    write_run_index_unlocked(&record.run_id, &index)
}

fn sort_run_index(index: &mut TradeLedgerRunIndex) {
    index
        .markets
        .sort_by(|left, right| left.market_id.cmp(&right.market_id));
    index
        .trades
        .sort_by(|left, right| left.trade_id.cmp(&right.trade_id));
}

pub(super) fn ensure_run_trade_ledger_layout(run_id: &str) -> Result<()> {
    for dir in [
        trade_session_ledger_run_dir(run_id),
        trade_session_ledger_run_dir(run_id).join(TRADE_LEDGER_DIR_NAME),
    ] {
        fs::create_dir_all(&dir).with_context(|| format!("failed to create {}", dir.display()))?;
    }
    Ok(())
}

pub(super) fn write_run_index(run_id: &str, index: &TradeLedgerRunIndex) -> Result<()> {
    let _guard = run_index_lock()?;
    write_run_index_unlocked(run_id, index)
}

fn write_run_index_unlocked(run_id: &str, index: &TradeLedgerRunIndex) -> Result<()> {
    let path = trade_session_run_index_path_for_run(run_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let seq = RUN_INDEX_WRITE_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp_path = path.with_file_name(format!(
        ".{RUN_INDEX_FILE_NAME}.{}.{}.tmp",
        std::process::id(),
        seq
    ));
    {
        let mut file = fs::File::create(&tmp_path)
            .with_context(|| format!("failed to create {}", tmp_path.display()))?;
        file.write_all(&serde_json::to_vec_pretty(index)?)
            .with_context(|| format!("failed to write {}", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to flush {}", tmp_path.display()))?;
    }
    fs::rename(&tmp_path, &path)
        .with_context(|| format!("failed to replace {}", path.display()))?;
    super::receipts::sync_session_receipt_path(&path)?;
    Ok(())
}

fn run_index_lock() -> Result<MutexGuard<'static, ()>> {
    RUN_INDEX_LOCK
        .lock()
        .map_err(|_| anyhow!("trade session run index lock poisoned"))
}

pub(super) fn apply_record_to_run_index(
    index: &mut TradeLedgerRunIndex,
    record: &TradeLedgerRecord,
) {
    index.event_count = index.event_count.saturating_add(1);
    let trade_exists = index
        .trades
        .iter()
        .any(|entry| entry.trade_id == record.trade_id);
    if !trade_exists {
        index.trade_count = index.trade_count.saturating_add(1);
    }
    if let Some(market_id) = record.market_id.as_ref().map(|value| value.to_string()) {
        if !index
            .markets
            .iter()
            .any(|entry| entry.market_id == market_id)
        {
            index
                .markets
                .push(TradeLedgerMarketIndexEntry { market_id });
        }
    }

    if !trade_exists {
        index.trades.push(TradeLedgerTradeIndexEntry {
            trade_id: record.trade_id.clone(),
        });
    }
}
