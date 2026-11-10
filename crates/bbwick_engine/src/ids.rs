use std::collections::HashMap;

use anyhow::{anyhow, Result};

use crate::market_data::{CanonicalTopOfBook, SourceTsKind};
use crate::oracle::OracleTick;
use crate::wire::{BarPayload, OverlayPayload, StepFrame, StepTradePayload};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SymbolId(u16);

impl SymbolId {
    pub(crate) fn index(self) -> usize {
        usize::from(self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeframeId(u8);

impl TimeframeId {
    pub(crate) fn index(self) -> usize {
        usize::from(self.0)
    }
}

#[derive(Debug, Clone)]
pub struct MarketDataKeyRegistry {
    symbols: Vec<Box<str>>,
    symbol_by_wire: HashMap<Box<str>, SymbolId>,
    timeframes: Vec<Box<str>>,
    timeframe_by_wire: HashMap<Box<str>, TimeframeId>,
}

impl MarketDataKeyRegistry {
    pub(crate) fn new(active_symbols: Vec<String>, step_timeframes: Vec<String>) -> Result<Self> {
        if active_symbols.len() > usize::from(u16::MAX) {
            return Err(anyhow!("too many active bbwick symbols for SymbolId"));
        }
        if step_timeframes.len() > usize::from(u8::MAX) {
            return Err(anyhow!("too many bbwick step timeframes for TimeframeId"));
        }

        let mut symbols = Vec::with_capacity(active_symbols.len());
        let mut symbol_by_wire = HashMap::with_capacity(active_symbols.len());
        for (index, symbol) in active_symbols.into_iter().enumerate() {
            let stored = symbol.into_boxed_str();
            let symbol_id = SymbolId(index as u16);
            symbol_by_wire.insert(stored.clone(), symbol_id);
            symbols.push(stored);
        }

        let mut timeframes = Vec::with_capacity(step_timeframes.len());
        let mut timeframe_by_wire = HashMap::with_capacity(step_timeframes.len());
        for (index, timeframe) in step_timeframes.into_iter().enumerate() {
            let stored = timeframe.into_boxed_str();
            let timeframe_id = TimeframeId(index as u8);
            timeframe_by_wire.insert(stored.clone(), timeframe_id);
            timeframes.push(stored);
        }

        Ok(Self {
            symbols,
            symbol_by_wire,
            timeframes,
            timeframe_by_wire,
        })
    }

    pub(crate) fn symbol_id(&self, wire_symbol: &str) -> Option<SymbolId> {
        self.symbol_by_wire.get(wire_symbol).copied()
    }

    pub(crate) fn timeframe_id(&self, wire_timeframe: &str) -> Option<TimeframeId> {
        self.timeframe_by_wire.get(wire_timeframe).copied()
    }

    pub(crate) fn symbol(&self, symbol_id: SymbolId) -> &str {
        self.symbols[symbol_id.index()].as_ref()
    }

    pub(crate) fn timeframe(&self, timeframe_id: TimeframeId) -> &str {
        self.timeframes[timeframe_id.index()].as_ref()
    }

    pub(crate) fn symbol_count(&self) -> usize {
        self.symbols.len()
    }

    pub(crate) fn timeframe_count(&self) -> usize {
        self.timeframes.len()
    }

    pub(crate) fn step_slot_count(&self) -> usize {
        self.symbol_count().saturating_mul(self.timeframe_count())
    }

    pub(crate) fn step_slot(&self, symbol_id: SymbolId, timeframe_id: TimeframeId) -> usize {
        symbol_id.index() * self.timeframe_count() + timeframe_id.index()
    }

    pub(crate) fn symbols(&self) -> impl Iterator<Item = &str> {
        self.symbols.iter().map(|symbol| symbol.as_ref())
    }

    pub(crate) fn timeframes(&self) -> impl Iterator<Item = &str> {
        self.timeframes.iter().map(|timeframe| timeframe.as_ref())
    }

    pub(crate) fn materialize_tick(&self, frame: IdTickFrame) -> OracleTick {
        OracleTick {
            symbol: self.symbol(frame.symbol_id).to_string(),
            ts_ms: frame.ts_ms,
            price: frame.price,
        }
    }

    pub(crate) fn materialize_canonical_top_of_book(
        &self,
        frame: IdCanonicalTopOfBookFrame,
    ) -> CanonicalTopOfBook {
        CanonicalTopOfBook {
            symbol: self.symbol(frame.symbol_id).to_string(),
            bid_px: frame.bid_px,
            ask_px: frame.ask_px,
            bid_sz: frame.bid_sz,
            ask_sz: frame.ask_sz,
            queue_imbalance_l5: frame.queue_imbalance_l5,
            source_ts_ns: frame.source_ts_ns,
            source_ts_kind: frame.source_ts_kind,
            ingest_mono_ns: frame.ingest_mono_ns,
            seq_no: frame.seq_no,
        }
    }

    pub(crate) fn materialize_step(&self, frame: &IdStepFrame) -> StepFrame {
        StepFrame {
            symbol: self.symbol(frame.symbol_id).to_string(),
            timeframe: self.timeframe(frame.timeframe_id).to_string(),
            received_mono_ns: frame.received_mono_ns,
            bar: frame.bar.clone(),
            overlays: frame.overlays.clone(),
            trades: frame.trades.clone(),
            seq: frame.seq,
            ledger_epoch: frame.ledger_epoch,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct IdTickFrame {
    pub(crate) symbol_id: SymbolId,
    pub(crate) ts_ms: u64,
    pub(crate) price: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct IdHeartbeatFrame {
    pub(crate) symbol_id: SymbolId,
    pub(crate) ts_ms: u64,
    pub(crate) seq: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct IdTopOfBookIngressFrame {
    pub(crate) symbol_id: SymbolId,
    pub(crate) source_ts_ms: u64,
    pub(crate) source_ts_kind: SourceTsKind,
    pub(crate) bid_px: f64,
    pub(crate) ask_px: f64,
    pub(crate) bid_sz: f64,
    pub(crate) ask_sz: f64,
    pub(crate) queue_imbalance_l5: Option<f64>,
}

#[derive(Debug, Clone, Copy)]
pub struct IdCanonicalTopOfBookFrame {
    pub(crate) symbol_id: SymbolId,
    pub(crate) bid_px: f64,
    pub(crate) ask_px: f64,
    pub(crate) bid_sz: f64,
    pub(crate) ask_sz: f64,
    pub(crate) queue_imbalance_l5: Option<f64>,
    pub(crate) source_ts_ns: u64,
    pub(crate) source_ts_kind: SourceTsKind,
    pub(crate) ingest_mono_ns: u64,
    pub(crate) seq_no: u64,
}

#[derive(Debug, Clone)]
pub struct IdStepFrame {
    pub(crate) symbol_id: SymbolId,
    pub(crate) timeframe_id: TimeframeId,
    pub(crate) received_mono_ns: u64,
    pub(crate) bar: BarPayload,
    pub(crate) overlays: Vec<OverlayPayload>,
    pub(crate) trades: Vec<StepTradePayload>,
    pub(crate) seq: u32,
    pub(crate) ledger_epoch: u64,
}
