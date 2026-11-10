use std::time::Duration;

use anyhow::Result;
use polymarket_client_sdk::data::Client as DataClient;
use polymarket_client_sdk::types::Address;
use polymarket_hft::config::AppConfig;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[path = "maker_rebate_receipts/ingest.rs"]
mod ingest;

const RECEIPT_INGESTION_INTERVAL_SECS: u64 = 60;

pub(crate) struct MakerRebateReceiptStart {
    pub(crate) cfg: AppConfig,
    pub(crate) shutdown: CancellationToken,
    pub(crate) http: reqwest::Client,
    pub(crate) data: DataClient,
    pub(crate) tracked_wallet: Address,
}

pub(crate) fn spawn_maker_rebate_receipt_task(
    start: MakerRebateReceiptStart,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(RECEIPT_INGESTION_INTERVAL_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut latest_result = Ok(());
        loop {
            tokio::select! {
                biased;
                _ = start.shutdown.cancelled() => break,
                _ = tick.tick() => latest_result = ingest_tick(&start).await,
            }
        }
        latest_result
    })
}

async fn ingest_tick(start: &MakerRebateReceiptStart) -> Result<()> {
    let result = ingest::ingest_current_session_maker_rebate_receipts(
        &start.http,
        &start.data,
        &start.cfg.polymarket.resolved_account_rpc_url(),
        start.tracked_wallet,
    )
    .await;
    if let Err(error) = &result {
        tracing::warn!(
            target: "app.maker_rebate_receipts",
            error = %error,
            "maker_rebate_receipt_ingestion_failed"
        );
    }
    result
}
