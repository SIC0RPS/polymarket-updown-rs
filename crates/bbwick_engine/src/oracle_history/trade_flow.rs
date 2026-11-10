use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use super::{OracleHistory, OracleHistoryView};

const WINDOW_NS: u64 = 5_000_000_000;
const MAX_RECORDS: usize = 65_536;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OracleTradeFlowCoverage {
    Complete,
    NoVolume,
    WarmingUp,
    Disconnected,
    Discontinuity,
    Malformed,
    Overflow,
}

/// Immutable local-receipt window (end - five seconds, end].
/// Exchange clocks are provenance and never determine window membership.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OracleTradeFlowSnapshot {
    pub window_end_mono_ns: u64,
    pub coverage_started_mono_ns: u64,
    pub buy_quantity: f64,
    pub sell_quantity: f64,
    pub coverage: OracleTradeFlowCoverage,
    pub revision: u64,
    pub received_mono_ns: u64,
    pub published_mono_ns: u64,
    pub last_aggregate_id: Option<u64>,
    pub event_ts_ms: Option<u64>,
    pub trade_ts_ms: Option<u64>,
}

impl OracleTradeFlowSnapshot {
    pub fn imbalance(&self) -> Option<f64> {
        let total = self.buy_quantity + self.sell_quantity;
        (self.coverage == OracleTradeFlowCoverage::Complete && total.is_finite() && total > 0.0)
            .then(|| ((self.buy_quantity - self.sell_quantity) / total).clamp(-1.0, 1.0))
    }

    pub fn validate(&self, consumed_mono_ns: u64) -> Result<()> {
        anyhow::ensure!(
            self.window_end_mono_ns == consumed_mono_ns
                && self.coverage_started_mono_ns <= consumed_mono_ns
                && self.received_mono_ns <= self.published_mono_ns
                && self.published_mono_ns <= consumed_mono_ns,
            "invalid consumed trade-flow clocks"
        );
        let total = self.buy_quantity + self.sell_quantity;
        anyhow::ensure!(
            self.buy_quantity.is_finite()
                && self.buy_quantity >= 0.0
                && self.sell_quantity.is_finite()
                && self.sell_quantity >= 0.0
                && total.is_finite(),
            "invalid consumed trade-flow quantities"
        );
        match self.coverage {
            OracleTradeFlowCoverage::Complete | OracleTradeFlowCoverage::NoVolume => {
                anyhow::ensure!(
                    consumed_mono_ns.saturating_sub(self.coverage_started_mono_ns) >= WINDOW_NS,
                    "incomplete trade-flow coverage"
                );
                anyhow::ensure!(
                    (self.coverage == OracleTradeFlowCoverage::Complete) == (total > 0.0),
                    "trade-flow coverage disagrees with observed volume"
                );
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AggregateTrade {
    pub aggregate_id: u64,
    pub quantity: f64,
    pub buyer_is_maker: bool,
    pub event_ts_ms: Option<u64>,
    pub trade_ts_ms: Option<u64>,
}

#[derive(Debug)]
struct FlowRecord {
    id: u64,
    quantity: f64,
    seller_aggressor: bool,
    received_mono_ns: u64,
}

#[derive(Debug)]
struct FlowWindow {
    records: VecDeque<FlowRecord>,
    identities: HashSet<u64>,
    connected: bool,
    coverage_started_mono_ns: u64,
    coverage: OracleTradeFlowCoverage,
    revision: u64,
    received_mono_ns: u64,
    published_mono_ns: u64,
    last_aggregate_id: Option<u64>,
    event_ts_ms: Option<u64>,
    trade_ts_ms: Option<u64>,
    buy_quantity: f64,
    sell_quantity: f64,
    recorded: Option<(Option<OracleTradeFlowSnapshot>, u64)>,
}

impl Default for FlowWindow {
    fn default() -> Self {
        Self {
            records: VecDeque::new(),
            identities: HashSet::new(),
            connected: false,
            coverage_started_mono_ns: 0,
            coverage: OracleTradeFlowCoverage::Disconnected,
            revision: 0,
            received_mono_ns: 0,
            published_mono_ns: 0,
            last_aggregate_id: None,
            event_ts_ms: None,
            trade_ts_ms: None,
            buy_quantity: 0.0,
            sell_quantity: 0.0,
            recorded: None,
        }
    }
}

impl FlowWindow {
    fn note_change(&mut self, received_mono_ns: u64, published_mono_ns: u64) {
        self.revision = self.revision.saturating_add(1);
        self.received_mono_ns = received_mono_ns;
        self.published_mono_ns = published_mono_ns;
    }

    fn break_coverage(&mut self, reason: OracleTradeFlowCoverage, received_mono_ns: u64) {
        self.coverage_started_mono_ns = received_mono_ns;
        self.coverage = reason;
    }

    fn remove_front(&mut self) {
        if let Some(record) = self.records.pop_front() {
            self.identities.remove(&record.id);
            let quantity = if record.seller_aggressor {
                &mut self.sell_quantity
            } else {
                &mut self.buy_quantity
            };
            *quantity = (*quantity - record.quantity).max(0.0);
        }
        if self.records.is_empty() {
            self.buy_quantity = 0.0;
            self.sell_quantity = 0.0;
        }
    }

    fn expire(&mut self, end_mono_ns: u64) {
        let start = end_mono_ns.saturating_sub(WINDOW_NS);
        while self
            .records
            .front()
            .is_some_and(|record| record.received_mono_ns <= start)
        {
            self.remove_front();
        }
    }

    fn admit(&mut self, trade: Option<AggregateTrade>, received: u64, published: u64) {
        self.expire(received);
        self.note_change(received, published);
        let Some(trade) = trade else {
            self.break_coverage(OracleTradeFlowCoverage::Malformed, received);
            return;
        };
        if self.identities.contains(&trade.aggregate_id)
            || self.last_aggregate_id == Some(trade.aggregate_id)
        {
            return;
        }
        if let Some(last) = self.last_aggregate_id {
            // Adjacency is conservative local coverage evidence, not a venue ordering promise.
            if last.checked_add(1) != Some(trade.aggregate_id) {
                self.break_coverage(OracleTradeFlowCoverage::Discontinuity, received);
                if trade.aggregate_id <= last {
                    // An old identity cannot be admitted again after bounded retention expires.
                    return;
                }
            }
        }
        self.last_aggregate_id = Some(trade.aggregate_id);
        self.event_ts_ms = trade.event_ts_ms;
        self.trade_ts_ms = trade.trade_ts_ms;
        if self.records.len() == MAX_RECORDS {
            self.remove_front();
            self.break_coverage(OracleTradeFlowCoverage::Overflow, received);
        }
        let total = self.buy_quantity + self.sell_quantity + trade.quantity;
        if !total.is_finite() {
            self.break_coverage(OracleTradeFlowCoverage::Overflow, received);
            return;
        }
        if trade.buyer_is_maker {
            self.sell_quantity += trade.quantity;
        } else {
            self.buy_quantity += trade.quantity;
        }
        self.identities.insert(trade.aggregate_id);
        self.records.push_back(FlowRecord {
            id: trade.aggregate_id,
            quantity: trade.quantity,
            seller_aggressor: trade.buyer_is_maker,
            received_mono_ns: received,
        });
    }

    fn snapshot(&mut self, end: u64) -> OracleTradeFlowSnapshot {
        self.expire(end);
        let coverage = if !self.connected {
            OracleTradeFlowCoverage::Disconnected
        } else if end.saturating_sub(self.coverage_started_mono_ns) < WINDOW_NS {
            self.coverage
        } else if self.buy_quantity + self.sell_quantity > 0.0 {
            OracleTradeFlowCoverage::Complete
        } else {
            OracleTradeFlowCoverage::NoVolume
        };
        OracleTradeFlowSnapshot {
            window_end_mono_ns: end,
            coverage_started_mono_ns: self.coverage_started_mono_ns,
            buy_quantity: self.buy_quantity,
            sell_quantity: self.sell_quantity,
            coverage,
            revision: self.revision,
            received_mono_ns: self.received_mono_ns,
            published_mono_ns: self.published_mono_ns,
            last_aggregate_id: self.last_aggregate_id,
            event_ts_ms: self.event_ts_ms,
            trade_ts_ms: self.trade_ts_ms,
        }
    }
}

#[derive(Debug)]
pub(super) struct FlowHistory {
    symbols: Vec<Mutex<FlowWindow>>,
}

impl FlowHistory {
    pub(super) fn new(symbol_count: usize) -> Self {
        Self {
            symbols: (0..symbol_count)
                .map(|_| Mutex::new(FlowWindow::default()))
                .collect(),
        }
    }
}

impl OracleHistory {
    pub fn pin(&self) -> Result<Arc<OracleHistoryView>> {
        self.state
            .read()
            .map(|state| Arc::clone(&state))
            .map_err(|_| anyhow::anyhow!("OHLC memory lock poisoned"))
    }

    /// Pin existing candles and the symbol's flow at one local consumption boundary.
    pub fn pin_for_symbol(
        &self,
        symbol: &str,
    ) -> Result<(Arc<OracleHistoryView>, Option<OracleTradeFlowSnapshot>, u64)> {
        let id = self
            .registry
            .symbol_id(symbol)
            .context("unknown trade-flow symbol")?;
        let mut flow = self.flow.symbols[id.index()]
            .lock()
            .map_err(|_| anyhow::anyhow!("trade-flow memory lock poisoned"))?;
        let candles = self.pin()?;
        let (snapshot, consumed_mono_ns) = match flow.recorded {
            Some(recorded) => recorded,
            None => {
                let consumed = self.current_mono_ns();
                (Some(flow.snapshot(consumed)), consumed)
            }
        };
        Ok((candles, snapshot, consumed_mono_ns))
    }

    /// Install the immutable recorded input, including explicit historical absence.
    /// Candidate portfolio-only invocations retain this original clock and snapshot.
    pub fn apply_consumed_trade_flow(
        &self,
        symbol: &str,
        snapshot: Option<OracleTradeFlowSnapshot>,
        consumed_mono_ns: u64,
    ) -> Result<()> {
        if let Some(snapshot) = snapshot {
            snapshot.validate(consumed_mono_ns)?;
        }
        let id = self
            .registry
            .symbol_id(symbol)
            .context("unknown consumed trade-flow symbol")?;
        let mut flow = self.flow.symbols[id.index()]
            .lock()
            .map_err(|_| anyhow::anyhow!("trade-flow memory lock poisoned"))?;
        if let Some((_, previous)) = flow.recorded {
            anyhow::ensure!(
                consumed_mono_ns >= previous,
                "trade-flow consumption clock regressed"
            );
        }
        flow.recorded = Some((snapshot, consumed_mono_ns));
        Ok(())
    }

    pub(crate) fn admit_aggregate_trade(
        &self,
        symbol: &str,
        trade: Option<AggregateTrade>,
        received_mono_ns: u64,
    ) -> Result<()> {
        let Some(id) = self.registry.symbol_id(symbol) else {
            return Ok(());
        };
        let mut flow = self.flow.symbols[id.index()]
            .lock()
            .map_err(|_| anyhow::anyhow!("trade-flow memory lock poisoned"))?;
        flow.admit(trade, received_mono_ns, self.current_mono_ns());
        Ok(())
    }

    pub(crate) fn set_trade_flow_connection(&self, connected: bool, received: u64) -> Result<()> {
        for slot in &self.flow.symbols {
            let mut flow = slot
                .lock()
                .map_err(|_| anyhow::anyhow!("trade-flow memory lock poisoned"))?;
            flow.connected = connected;
            flow.break_coverage(
                if connected {
                    OracleTradeFlowCoverage::WarmingUp
                } else {
                    OracleTradeFlowCoverage::Disconnected
                },
                received,
            );
            flow.note_change(received, self.current_mono_ns());
        }
        Ok(())
    }

    pub(crate) fn mark_trade_flow_malformed(&self, received: u64) -> Result<()> {
        for slot in &self.flow.symbols {
            let mut flow = slot
                .lock()
                .map_err(|_| anyhow::anyhow!("trade-flow memory lock poisoned"))?;
            flow.break_coverage(OracleTradeFlowCoverage::Malformed, received);
            flow.note_change(received, self.current_mono_ns());
        }
        Ok(())
    }
}
