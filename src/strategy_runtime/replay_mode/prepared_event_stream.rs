use std::sync::Arc;

use anyhow::{Context, Result};

use super::prepared_arrow::ClockRow;
use super::prepared_tape::PackedReplayTape;

pub(super) const EVENT_KIND_CLOCK: u8 = 1;
pub(crate) const EVENT_KIND_STRATEGY_CONSUMPTION: u8 = 2;
pub(super) const EVENT_KIND_LAST_TRADE: u8 = 4;
pub(super) const EVENT_KIND_WINDOW_BOOTSTRAP: u8 = 5;
pub(super) const EVENT_KIND_CONDITION_RESOLUTION: u8 = 6;
pub(super) const EVENT_KIND_SETTLEMENT_COLLATERAL: u8 = 7;
pub(super) const EVENT_KIND_PROVIDER_RESOLUTION: u8 = 8;
pub(super) const CENTRAL_EVENT_SYMBOL: u8 = u8::MAX;
pub(super) const EVENT_FLAG_PROJECTION_CHANGED: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EventRef {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub symbol: u8,
    pub kind: u8,
    pub flags: u16,
    pub start: u32,
    pub len: u32,
}

impl EventRef {
    pub(crate) fn validate_wire(self, expected_symbol: u8) -> Result<Self> {
        anyhow::ensure!(
            self.symbol == expected_symbol,
            "foreign EventRef symbol expected={expected_symbol} actual={}",
            self.symbol
        );
        anyhow::ensure!(self.sequence > 0, "EventRef sequence is zero");
        anyhow::ensure!(
            matches!(
                self.kind,
                EVENT_KIND_CLOCK
                    | EVENT_KIND_STRATEGY_CONSUMPTION
                    | EVENT_KIND_LAST_TRADE
                    | EVENT_KIND_WINDOW_BOOTSTRAP
                    | EVENT_KIND_PROVIDER_RESOLUTION
            ),
            "invalid EventRef kind={}",
            self.kind
        );
        anyhow::ensure!(
            self.flags & !EVENT_FLAG_PROJECTION_CHANGED == 0,
            "EventRef contains unknown flags={}",
            self.flags
        );
        anyhow::ensure!(self.timestamp_ms > 0, "EventRef timestamp is zero");
        anyhow::ensure!(self.len > 0, "EventRef len is zero");
        self.start
            .checked_add(self.len)
            .context("EventRef range overflow")?;
        Ok(self)
    }

    pub(crate) fn validate_central_wire(self) -> Result<Self> {
        anyhow::ensure!(
            self.symbol == CENTRAL_EVENT_SYMBOL,
            "central EventRef has foreign symbol={}",
            self.symbol
        );
        anyhow::ensure!(self.sequence > 0, "central EventRef sequence is zero");
        anyhow::ensure!(
            matches!(
                self.kind,
                EVENT_KIND_CONDITION_RESOLUTION | EVENT_KIND_SETTLEMENT_COLLATERAL
            ),
            "invalid central EventRef kind={}",
            self.kind
        );
        anyhow::ensure!(self.flags == 0, "central EventRef contains flags");
        anyhow::ensure!(self.timestamp_ms > 0, "central EventRef timestamp is zero");
        anyhow::ensure!(self.len == 1, "central EventRef must reference one row");
        self.start
            .checked_add(self.len)
            .context("central EventRef range overflow")?;
        Ok(self)
    }

    pub(super) fn projection_changed(&self) -> bool {
        self.flags & EVENT_FLAG_PROJECTION_CHANGED != 0
    }
}

pub(super) struct PreparedReplayEventStream {
    tape: Arc<PackedReplayTape>,
    next_event: usize,
    end_event: usize,
    bootstrap: Option<(usize, EventRef)>,
}

impl PreparedReplayEventStream {
    pub(super) fn new(tape: Arc<PackedReplayTape>, window: Option<(u64, u64)>) -> Result<Self> {
        let (next_event, end_event, bootstrap) = match window {
            None => (0, tape.events().len(), None),
            Some((start_ms, end_ms)) => {
                let next_event = tape
                    .events()
                    .partition_point(|event| event.timestamp_ms < start_ms);
                let end_event = tape
                    .events()
                    .partition_point(|event| event.timestamp_ms <= end_ms);
                let first_clock = tape
                    .clocks()
                    .partition_point(|clock| clock.session_ts_ms < start_ms);
                let bootstrap = (next_event < end_event
                    && tape.has_window_snapshot_before(first_clock)?)
                .then(|| {
                    (
                        next_event,
                        EventRef {
                            sequence: 1,
                            timestamp_ms: start_ms,
                            symbol: tape.symbol_id(),
                            kind: EVENT_KIND_WINDOW_BOOTSTRAP,
                            flags: EVENT_FLAG_PROJECTION_CHANGED,
                            start: 0,
                            len: u32::try_from(first_clock)
                                .expect("validated engine_clock EventRef capacity"),
                        },
                    )
                });
                (next_event, end_event, bootstrap)
            }
        };
        Ok(Self {
            tape,
            next_event,
            end_event,
            bootstrap,
        })
    }

    pub(super) fn next_event(&mut self) -> Result<Option<EventRef>> {
        if self
            .bootstrap
            .as_ref()
            .is_some_and(|(before, _)| self.next_event >= *before)
        {
            let (_, event) = self.bootstrap.take().expect("checked replay bootstrap");
            return Ok(Some(event));
        }
        let Some(event) = self
            .tape
            .events()
            .get(self.next_event)
            .copied()
            .filter(|_| self.next_event < self.end_event)
        else {
            return Ok(None);
        };
        self.next_event = self.next_event.saturating_add(1);
        Ok(Some(event))
    }
}

pub(super) fn build_event_schedule(tape: &PackedReplayTape) -> Result<Vec<EventRef>> {
    let mut events = Vec::new();
    let mut next_trade = 0usize;
    for (clock_index, clock) in tape.clocks().iter().enumerate() {
        while tape
            .last_trades()
            .get(next_trade)
            .is_some_and(|trade| trade.session_ts_ms < clock.session_ts_ms)
        {
            let trade = &tape.last_trades()[next_trade];
            if tape.last_trade_belongs_to_catalog(trade) {
                events.push(make_ref(
                    tape,
                    trade.session_ts_ms,
                    EVENT_KIND_LAST_TRADE,
                    0,
                    next_trade,
                    1,
                )?);
            }
            next_trade = next_trade.saturating_add(1);
        }
        if clock_has_engine_delta(clock) {
            let flags = if clock_has_projection_delta(tape, clock)? {
                EVENT_FLAG_PROJECTION_CHANGED
            } else {
                0
            };
            events.push(make_ref(
                tape,
                clock.session_ts_ms,
                EVENT_KIND_CLOCK,
                flags,
                clock_index,
                1,
            )?);
        }
        if clock.consumption_len > 0 {
            anyhow::ensure!(
                clock.consumption_len == 1,
                "strategy consumption clock must reference exactly one record"
            );
            let mut event = make_ref(
                tape,
                clock.session_ts_ms,
                EVENT_KIND_STRATEGY_CONSUMPTION,
                0,
                range_start(clock.consumption_start, "strategy consumption")?,
                1,
            )?;
            event.sequence = clock.sequence;
            events.push(event);
        }
    }
    for (trade_index, trade) in tape.last_trades().iter().enumerate().skip(next_trade) {
        if tape.last_trade_belongs_to_catalog(trade) {
            events.push(make_ref(
                tape,
                trade.session_ts_ms,
                EVENT_KIND_LAST_TRADE,
                0,
                trade_index,
                1,
            )?);
        }
    }
    for (resolution_index, resolution) in tape.resolutions().iter().enumerate() {
        if tape.resolution_belongs_to_catalog(resolution)? {
            events.push(make_ref(
                tape,
                resolution.observed_at_ms,
                EVENT_KIND_PROVIDER_RESOLUTION,
                0,
                resolution_index,
                1,
            )?);
        }
    }
    events.sort_by_key(|event| event.timestamp_ms);
    for (sequence, event) in events.iter_mut().enumerate() {
        if event.kind == EVENT_KIND_STRATEGY_CONSUMPTION {
            continue;
        }
        event.sequence = u64::try_from(sequence)
            .context("prepared event sequence exceeds u64")?
            .checked_add(1)
            .context("prepared event sequence overflow")?;
    }
    Ok(events)
}

fn clock_has_engine_delta(clock: &ClockRow) -> bool {
    clock.market_len > 0 || clock.book_len > 0 || clock.lifecycle_len > 0
}

fn clock_has_projection_delta(tape: &PackedReplayTape, clock: &ClockRow) -> Result<bool> {
    Ok(tape.market_projection_changed(clock)?
        || tape.lifecycle_projection_changed(clock)?
        || tape.book_projection_changed(clock.book_start, clock.book_len, "token_book_delta")?)
}

fn make_ref(
    tape: &PackedReplayTape,
    timestamp_ms: u64,
    kind: u8,
    flags: u16,
    start: usize,
    len: usize,
) -> Result<EventRef> {
    Ok(EventRef {
        sequence: 0,
        timestamp_ms,
        symbol: tape.symbol_id(),
        kind,
        flags,
        start: u32::try_from(start).context("prepared event start exceeds u32")?,
        len: u32::try_from(len).context("prepared event len exceeds u32")?,
    })
}

fn range_start(start: i64, name: &str) -> Result<usize> {
    usize::try_from(start).with_context(|| format!("{name} negative start"))
}
