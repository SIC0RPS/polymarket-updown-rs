use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};

use super::super::{PackedReplayTape, PreparedReceiptTape};
use crate::strategy_runtime::symbol_process::{EventRef, SymbolId};

const SYMBOL_ORDER: [SymbolId; 4] = [SymbolId::Btc, SymbolId::Eth, SymbolId::Sol, SymbolId::Xrp];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScheduledEventTarget {
    Central,
    CentralResolution(SymbolId),
    Symbol(SymbolId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ScheduledEventRef {
    pub(crate) target: ScheduledEventTarget,
    pub(crate) event: EventRef,
}

#[derive(Debug, Clone)]
pub(crate) struct MergedEventSchedule {
    events: Arc<[ScheduledEventRef]>,
    last_timestamp_ms: u64,
    initial_sequences: [u64; 4],
    last_sequences: [u64; 4],
    local_event_counts: [u64; 4],
}

impl MergedEventSchedule {
    pub(crate) fn build(
        symbols: &[String],
        tapes: &BTreeMap<String, Arc<PackedReplayTape>>,
        receipt_tape: &Arc<PreparedReceiptTape>,
        window: Option<(u64, u64)>,
    ) -> Result<Self> {
        anyhow::ensure!(
            !symbols.is_empty(),
            "candidate event schedule has no symbols"
        );
        anyhow::ensure!(
            symbols.len() == tapes.len(),
            "candidate event schedule symbol/tape coverage mismatch symbols={} tapes={}",
            symbols.len(),
            tapes.len()
        );

        let mut selected = Vec::with_capacity(symbols.len());
        let mut seen = std::collections::BTreeSet::new();
        for symbol in symbols {
            let symbol_id = SymbolId::parse(symbol)?;
            anyhow::ensure!(
                seen.insert(symbol_id.as_str()),
                "candidate event schedule contains duplicate symbol={}",
                symbol_id.as_str()
            );
            let tape = tapes
                .iter()
                .find(|(candidate, _)| candidate.eq_ignore_ascii_case(symbol_id.as_str()))
                .map(|(_, tape)| tape)
                .with_context(|| {
                    format!(
                        "candidate event schedule is missing tape symbol={}",
                        symbol_id.as_str()
                    )
                })?;
            anyhow::ensure!(
                tape.symbol() == symbol_id.as_str(),
                "candidate event schedule tape is foreign expected={} actual={}",
                symbol_id.as_str(),
                tape.symbol()
            );
            selected.push((symbol_id, tape.event_schedule_for_window(window)?));
        }
        selected.sort_unstable_by_key(|(symbol, _)| symbol_rank(*symbol));
        let central_events = receipt_tape
            .events()
            .iter()
            .copied()
            .filter(|event| {
                window.is_none_or(|(start_ms, end_ms)| {
                    event.timestamp_ms >= start_ms && event.timestamp_ms <= end_ms
                })
            })
            .map(EventRef::validate_central_wire)
            .collect::<Result<Vec<_>>>()?;

        let expected = SYMBOL_ORDER
            .into_iter()
            .filter(|symbol| seen.contains(symbol.as_str()))
            .collect::<Vec<_>>();
        anyhow::ensure!(
            selected
                .iter()
                .map(|(symbol, _)| *symbol)
                .eq(expected.iter().copied()),
            "candidate event schedule symbol order is not canonical"
        );

        let total_events = selected
            .iter()
            .try_fold(0usize, |total, (_, events)| {
                total
                    .checked_add(events.len())
                    .context("candidate event schedule size overflow")
            })?
            .checked_add(central_events.len())
            .context("candidate central event schedule size overflow")?;

        let mut raw_events = Vec::with_capacity(total_events);
        let mut consumption_events = Vec::new();
        for (symbol, events) in &selected {
            for event in events.iter().copied() {
                event.validate_wire(*symbol as u8)?;
                let scheduled = ScheduledEventRef {
                    target: if event.kind
                        == crate::strategy_runtime::replay_mode::prepared_event_stream::EVENT_KIND_PROVIDER_RESOLUTION
                    {
                        ScheduledEventTarget::CentralResolution(*symbol)
                    } else {
                        ScheduledEventTarget::Symbol(*symbol)
                    },
                    event,
                };
                if event.kind
                    == crate::strategy_runtime::replay_mode::prepared_event_stream::EVENT_KIND_STRATEGY_CONSUMPTION
                {
                    consumption_events.push(scheduled);
                } else {
                    raw_events.push(scheduled);
                }
            }
        }
        raw_events.extend(central_events.into_iter().map(|event| ScheduledEventRef {
            target: ScheduledEventTarget::Central,
            event,
        }));
        raw_events.sort_by_key(scheduled_order_key);
        consumption_events.sort_by_key(|scheduled| scheduled.event.sequence);
        for pair in consumption_events.windows(2) {
            anyhow::ensure!(
                pair[0].event.sequence < pair[1].event.sequence,
                "captured strategy consumption sequence is not strictly increasing"
            );
        }

        let mut raw = raw_events.into_iter().peekable();
        let mut consumption = consumption_events.into_iter().peekable();
        let mut merged = Vec::with_capacity(total_events);
        while raw.peek().is_some() || consumption.peek().is_some() {
            let take_raw = match (raw.peek(), consumption.peek()) {
                (Some(raw_event), Some(consumption_event)) => {
                    raw_event.event.timestamp_ms <= consumption_event.event.timestamp_ms
                }
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            merged.push(if take_raw {
                raw.next().expect("checked raw replay event")
            } else {
                consumption
                    .next()
                    .expect("checked consumption replay event")
            });
        }
        anyhow::ensure!(
            merged.len() == total_events,
            "candidate event schedule did not consume every selected event"
        );
        let mut previous_raw_key = None;
        let mut previous_consumption_sequence = None;
        let mut last_timestamp_ms = 0;
        let mut initial_sequences = [0; 4];
        let mut last_sequences = [0; 4];
        let mut local_event_counts = [0usize; 4];
        let mut has_local_consumption = [false; 4];
        for scheduled in &merged {
            last_timestamp_ms = last_timestamp_ms.max(scheduled.event.timestamp_ms);
            if scheduled.event.kind
                == crate::strategy_runtime::replay_mode::prepared_event_stream::EVENT_KIND_STRATEGY_CONSUMPTION
            {
                if let Some(previous) = previous_consumption_sequence {
                    anyhow::ensure!(
                        previous < scheduled.event.sequence,
                        "captured strategy consumption sequence regressed in merged schedule"
                    );
                }
                previous_consumption_sequence = Some(scheduled.event.sequence);
                if let ScheduledEventTarget::Symbol(symbol) = scheduled.target {
                    let index = usize::from(symbol_rank(symbol));
                    if !has_local_consumption[index] {
                        initial_sequences[index] = scheduled.event.sequence - 1;
                        has_local_consumption[index] = true;
                    }
                    last_sequences[index] = scheduled.event.sequence;
                    local_event_counts[index] += 1;
                }
            } else {
                let key = scheduled_order_key(scheduled);
                if let Some(previous) = previous_raw_key {
                    anyhow::ensure!(previous <= key, "raw replay event order regressed");
                }
                previous_raw_key = Some(key);
            }
        }
        let local_event_counts = local_event_counts
            .map(|count| {
                u64::try_from(count).context("candidate local consumption count exceeds u64")
            })
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .try_into()
            .expect("candidate local consumption metadata has four symbols");

        Ok(Self {
            events: Arc::from(merged),
            last_timestamp_ms,
            initial_sequences,
            last_sequences,
            local_event_counts,
        })
    }

    pub(crate) fn events(&self) -> &[ScheduledEventRef] {
        &self.events
    }

    pub(crate) fn local_event_count(&self, symbol: SymbolId) -> Result<u64> {
        Ok(self.local_event_counts[usize::from(symbol_rank(symbol))])
    }

    pub(crate) fn initial_sequence(&self, symbol: SymbolId) -> u64 {
        self.initial_sequences[usize::from(symbol_rank(symbol))]
    }

    pub(crate) fn last_sequence(&self, symbol: SymbolId) -> u64 {
        self.last_sequences[usize::from(symbol_rank(symbol))]
    }

    pub(crate) fn last_timestamp_ms(&self) -> u64 {
        self.last_timestamp_ms
    }
}

fn scheduled_order_key(scheduled: &ScheduledEventRef) -> (u64, u8, u64) {
    let target_rank = match scheduled.target {
        ScheduledEventTarget::CentralResolution(symbol) => symbol_rank(symbol).saturating_add(2),
        ScheduledEventTarget::Central => 1,
        ScheduledEventTarget::Symbol(symbol) => symbol_rank(symbol).saturating_add(2),
    };
    (
        scheduled.event.timestamp_ms,
        target_rank,
        scheduled.event.sequence,
    )
}

const fn symbol_rank(symbol: SymbolId) -> u8 {
    match symbol {
        SymbolId::Btc => 0,
        SymbolId::Eth => 1,
        SymbolId::Sol => 2,
        SymbolId::Xrp => 3,
    }
}
