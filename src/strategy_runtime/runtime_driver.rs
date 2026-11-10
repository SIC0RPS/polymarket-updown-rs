use std::collections::HashSet;

use crate::observability::action::ActionEvent;
use crate::observability::strategy_status::StrategyStatus;
use crate::observability::strategy_status::StrategyStatusState;
use crate::strategy::{MakerRebateMmStrategy, StrategyDecision, StrategyFact, StrategyFactSink};
use crate::strategy_runtime::driver::{
    append_translated_strategy_facts, drain_runtime_action_events, drive_accepted_tick_core,
};
use crate::strategy_runtime::recompute::next_strategy_deadline_ms;
use crate::strategy_runtime::state::StrategyRuntimeState;
use crate::types::{MarketTick, MarketTickTrigger};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuntimeInput {
    AcceptedTick {
        tick: MarketTick,
    },
    Heartbeat {
        tick: MarketTick,
        now_ms: u64,
        now_ingest_mono_ns: u64,
    },
    PortfolioChanged {
        last_tick: MarketTick,
        now_ms: u64,
        now_ingest_mono_ns: u64,
    },
    PolymarketDataChanged {
        last_tick: MarketTick,
        now_ms: u64,
        now_ingest_mono_ns: u64,
    },
}

impl RuntimeInput {
    pub fn tick(&self) -> &MarketTick {
        match self {
            Self::AcceptedTick { tick } | Self::Heartbeat { tick, .. } => tick,
            Self::PortfolioChanged { last_tick, .. }
            | Self::PolymarketDataChanged { last_tick, .. } => last_tick,
        }
    }
}

pub struct RuntimeDriverOutput {
    pub decisions: Vec<StrategyDecision>,
    pub facts: Vec<StrategyFact>,
    pub action_events: Vec<ActionEvent>,
    pub status: Option<StrategyStatus>,
    pub runtime_managed_paired_tokens: HashSet<String>,
}

impl RuntimeDriverOutput {
    pub fn has_runtime_output(&self) -> bool {
        !self.decisions.is_empty()
            || !self.facts.is_empty()
            || !self.action_events.is_empty()
            || self.status.is_some()
    }
}

pub struct RuntimeDriver;

impl RuntimeDriver {
    pub fn process(
        strategy: &mut MakerRebateMmStrategy,
        fact_sink: &mut StrategyFactSink,
        runtime_state: &mut StrategyRuntimeState,
        status_state: &mut StrategyStatusState,
        input: &mut RuntimeInput,
        max_decisions: usize,
    ) -> anyhow::Result<Option<RuntimeDriverOutput>> {
        strategy.pin_oracle_candles()?;
        let saved_tick = match input {
            RuntimeInput::AcceptedTick { .. } => None,
            RuntimeInput::Heartbeat {
                tick,
                now_ms,
                now_ingest_mono_ns,
            } => {
                let deadline_ms = next_strategy_deadline_ms(&runtime_state.control);
                if deadline_ms.is_none_or(|deadline_ms| deadline_ms > *now_ms) {
                    return Ok(None);
                }
                let saved = (tick.ts_ns, tick.ingest_mono_ns, tick.trigger);
                tick.ts_ns = now_ms.saturating_mul(1_000_000) as i64;
                tick.ingest_mono_ns = *now_ingest_mono_ns;
                tick.trigger = MarketTickTrigger::MarketData;
                Some(saved)
            }
            RuntimeInput::PortfolioChanged {
                last_tick,
                now_ms,
                now_ingest_mono_ns,
            }
            | RuntimeInput::PolymarketDataChanged {
                last_tick,
                now_ms,
                now_ingest_mono_ns,
            } => {
                let saved = (last_tick.ts_ns, last_tick.ingest_mono_ns, last_tick.trigger);
                last_tick.ts_ns = now_ms.saturating_mul(1_000_000) as i64;
                last_tick.ingest_mono_ns = *now_ingest_mono_ns;
                last_tick.trigger = MarketTickTrigger::MarketData;
                Some(saved)
            }
        };
        let driven = drive_accepted_tick_core(
            strategy,
            fact_sink,
            runtime_state,
            status_state,
            input.tick(),
            max_decisions,
        );
        if let Some((ts_ns, ingest_mono_ns, trigger)) = saved_tick {
            match input {
                RuntimeInput::Heartbeat { tick, .. } => {
                    tick.ts_ns = ts_ns;
                    tick.ingest_mono_ns = ingest_mono_ns;
                    tick.trigger = trigger;
                }
                RuntimeInput::PortfolioChanged { last_tick, .. } => {
                    last_tick.ts_ns = ts_ns;
                    last_tick.ingest_mono_ns = ingest_mono_ns;
                    last_tick.trigger = trigger;
                }
                RuntimeInput::PolymarketDataChanged { last_tick, .. } => {
                    last_tick.ts_ns = ts_ns;
                    last_tick.ingest_mono_ns = ingest_mono_ns;
                    last_tick.trigger = trigger;
                }
                RuntimeInput::AcceptedTick { .. } => unreachable!(),
            }
        }
        append_translated_strategy_facts(runtime_state, driven.facts.clone());
        let action_events = drain_runtime_action_events(runtime_state);
        Ok(Some(RuntimeDriverOutput {
            decisions: driven.decisions,
            facts: driven.facts,
            action_events,
            status: driven.status,
            runtime_managed_paired_tokens: driven.runtime_managed_paired_tokens,
        }))
    }
}
