use bbwick_engine::{OracleEngineConfig, OracleHistoryConfig};
use polymarket_hft::capture::scope::required_bbwick_symbols_for_capture;
use polymarket_hft::capture::session::MaybeCapture;
use polymarket_hft::config::AppConfig;

pub(super) fn oracle_engine_config(cfg: &AppConfig, capture: &MaybeCapture) -> OracleEngineConfig {
    OracleEngineConfig {
        transport: cfg.bbwick_oracle.to_transport_settings(),
        ingest_channel_capacity: cfg.bbwick_oracle.runtime.ingest_channel_capacity,
        tick_channel_capacity: cfg.bbwick_oracle.runtime.tick_channel_capacity,
        active_symbols: required_bbwick_symbols_for_capture(cfg),
        history: OracleHistoryConfig {
            enabled: true,
            data_root: cfg.bbwick_oracle.history.resolved_data_root(),
            max_bars: cfg.bbwick_oracle.history.max_bars,
        },
        polymarket_ws_ingress_capture: capture.polymarket_ws_ingress_capture(),
        polymarket_book_admission_capture: capture.polymarket_book_admission_capture(),
        binance_provider_capture: capture.binance_provider_capture(),
    }
}
