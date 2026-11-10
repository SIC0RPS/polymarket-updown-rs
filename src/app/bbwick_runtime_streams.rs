use bbwick_engine::{CanonicalTopOfBook, StepFrame};
use polymarket_hft::config::AppConfig;
use polymarket_hft::observability::action::ActionEvent;
use polymarket_hft::types::{MarketTick, MarketTickTrigger, SourceTsKind};

pub(crate) fn to_market_tick(
    tick: CanonicalTopOfBook,
    instrument: String,
    observed_wall_ms: u64,
) -> MarketTick {
    let observed_ts_ns = observed_wall_ms.saturating_mul(1_000_000) as i64;
    MarketTick {
        instrument,
        bid: tick.bid_px,
        ask: tick.ask_px,
        bid_sz: Some(tick.bid_sz),
        ask_sz: Some(tick.ask_sz),
        queue_imbalance_l5: tick.queue_imbalance_l5,
        source_ts_ns: tick.source_ts_ns as i64,
        source_ts_kind: match tick.source_ts_kind {
            bbwick_engine::SourceTsKind::Unavailable => SourceTsKind::Unavailable,
            bbwick_engine::SourceTsKind::Exchange => SourceTsKind::Exchange,
            bbwick_engine::SourceTsKind::ProviderEvent => SourceTsKind::ProviderEvent,
            bbwick_engine::SourceTsKind::EngineIngestWallclock => {
                SourceTsKind::EngineIngestWallclock
            }
        },
        ingest_mono_ns: tick.ingest_mono_ns,
        ts_ns: observed_ts_ns,
        trigger: MarketTickTrigger::OracleOnly,
    }
}

pub(crate) fn record_oracle_step(
    cfg: &AppConfig,
    obs: &polymarket_hft::observability::Observability,
    step: &StepFrame,
) {
    let event = ActionEvent::OracleStep {
        ts_ms: step.bar.ts_ms,
        symbol: step.symbol.clone(),
        timeframe: step.timeframe.clone(),
        open: step.bar.open,
        high: step.bar.high,
        low: step.bar.low,
        close: step.bar.close,
        volume: step.bar.volume,
    };
    if cfg.observability.logging.log_ticks {
        obs.emit_action(event);
    } else {
        obs.emit_ui(event);
    }
}
