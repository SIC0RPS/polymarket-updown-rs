#[path = "../app/bootstrap.rs"]
mod bootstrap;

use anyhow::Result;
use polymarket_client_sdk::clob::types::SignatureType;
use polymarket_hft::config::AppConfig;
use polymarket_hft::execution::ExecutionClient;
use polymarket_hft::types::{OrderCloseReason, OrderIntent, OrderIntentFields, TimeInForce};

const TOKEN: &str = "8657583076626179175476499886234821037965510975349829444912976438054194877923";
const PRICE: f64 = 0.50;
const QUANTITY: f64 = 5.0;

#[tokio::main]
async fn main() -> Result<()> {
    bootstrap::install_rustls_crypto_provider();
    bootstrap::load_env_file()?;
    let config = AppConfig::load()?;
    let client = ExecutionClient::from_config(&config).await?;

    let fields = OrderIntentFields {
        execution_intent_id: None,
        market_frame_received_at: None,
        market_state_published_at: None,
        market_connection_session_id: None,
        market_frame_sequence: None,
        market_state_revision: None,
        strategy_consumption_sequence: None,
        strategy_input_kind: None,
        decision_at: None,
        execution_evidence_revision: 0,
        execution_instruction_qty_units_6dp: 5_000_000,
        execution_instruction_limit_px_units_8dp: 50_000_000,
        instrument: TOKEN.to_owned(),
        market_id: None,
        qty: QUANTITY,
        limit_px: PRICE,
        time_in_force: TimeInForce::Gtc,
        post_only: false,
        quote_level: None,
        quote_group: None,
        lifecycle_cohort: None,
        lifecycle_symbol: None,
    };
    let buy = OrderIntent::buy(fields.clone());
    let sell = OrderIntent::sell(fields, OrderCloseReason::PairedFirstManagedSell);

    for (side, intent) in [("BUY", buy), ("SELL", sell)] {
        let (maker_amount, taker_amount, chain_id, signature_type) =
            client.inspect_signed_order(&intent).await?;
        let signature_kind = if signature_type == SignatureType::Eoa as u8 {
            "EOA"
        } else {
            "other"
        };
        println!(
            "token={TOKEN} side={side} price={PRICE:.2} quantity={QUANTITY} chain={chain_id} signature_type={signature_type} ({signature_kind}) makerAmount={maker_amount} takerAmount={taker_amount}"
        );
    }

    Ok(())
}
