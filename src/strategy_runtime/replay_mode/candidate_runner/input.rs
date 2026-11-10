use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use rayon::prelude::*;

use super::run::prepare_provider_resolution_fact;
use super::{MergedEventSchedule, ScheduledEventRef, ScheduledEventTarget};
use crate::capture::events::StrategyConsumptionRecord;
use crate::polymarket::state::{MarketProjection, PolymarketOddsState};
use crate::strategy_runtime::market_catalog::MarketCatalog;
use crate::strategy_runtime::replay_mode::prepared_arrow::{
    LastTradeRow, PreparedBookUpdate, PreparedConsumptionCursor, ResolutionRow,
};
use crate::strategy_runtime::replay_mode::prepared_event_stream::{
    EVENT_KIND_CLOCK, EVENT_KIND_LAST_TRADE, EVENT_KIND_PROVIDER_RESOLUTION,
    EVENT_KIND_STRATEGY_CONSUMPTION, EVENT_KIND_WINDOW_BOOTSTRAP,
};
use crate::strategy_runtime::replay_mode::PackedReplayTape;
use crate::trade_session_ledger::ManagedSellReceiptIdentity;

const MAX_INPUT_BLOCK_EVENTS: usize = 4096;

pub(crate) struct CandidateInputBlock {
    pub(crate) start_cursor: usize,
    pub(crate) events: Vec<CandidateInputEvent>,
}

pub(crate) struct CandidateInputEvent {
    pub(crate) scheduled: ScheduledEventRef,
    pub(crate) consumption: Option<CandidateConsumptionInput>,
    pub(crate) external: Option<CandidateExternalInput>,
}

pub(crate) struct CandidateConsumptionInput {
    pub(crate) record: StrategyConsumptionRecord,
    pub(crate) projection: MarketProjection,
    pub(crate) catalog_sha256: [u8; 32],
}

pub(crate) enum CandidateExternalInput {
    MarketState {
        odds: Arc<PolymarketOddsState>,
        projection: Option<MarketProjection>,
        book_updates: Arc<[PreparedBookUpdate]>,
        receipt_identity: ManagedSellReceiptIdentity,
        publish_account_state: bool,
    },
    LastTrade {
        trade: Arc<LastTradeRow>,
        receipt_identity: ManagedSellReceiptIdentity,
    },
    ProviderResolution {
        resolution: Arc<ResolutionRow>,
        changed: bool,
        odds: Option<Arc<PolymarketOddsState>>,
        projection: Option<MarketProjection>,
    },
}

pub(crate) struct CandidateInputDecoder<'a> {
    tapes: &'a BTreeMap<String, Arc<PackedReplayTape>>,
    consumption_cursors: BTreeMap<String, PreparedConsumptionCursor>,
    external_odds: PolymarketOddsState,
}

impl<'a> CandidateInputDecoder<'a> {
    pub(crate) fn new(tapes: &'a BTreeMap<String, Arc<PackedReplayTape>>) -> Result<Self> {
        let consumption_cursors = tapes
            .iter()
            .map(|(symbol, tape)| Ok((symbol.clone(), tape.consumption_cursor()?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        Ok(Self {
            tapes,
            consumption_cursors,
            external_odds: PolymarketOddsState::default(),
        })
    }

    pub(crate) fn decode_block(
        &mut self,
        schedule: &MergedEventSchedule,
        start_cursor: usize,
        pool: &rayon::ThreadPool,
    ) -> Result<CandidateInputBlock> {
        let mut events = Vec::new();
        let mut consumption_events = BTreeMap::<
            String,
            Vec<(usize, crate::strategy_runtime::symbol_process::EventRef)>,
        >::new();
        let remaining = schedule
            .events()
            .get(start_cursor..)
            .context("candidate input block cursor exceeds merged schedule")?;
        for scheduled in remaining.iter().copied() {
            if events.len() >= MAX_INPUT_BLOCK_EVENTS {
                break;
            }
            let is_consumption = scheduled.event.kind == EVENT_KIND_STRATEGY_CONSUMPTION;
            if is_consumption {
                let ScheduledEventTarget::Symbol(symbol) = scheduled.target else {
                    anyhow::bail!("strategy consumption event has no symbol target");
                };
                let (tape_key, _) = tape_for_symbol(self.tapes, symbol.as_str())?;
                consumption_events
                    .entry(tape_key.clone())
                    .or_default()
                    .push((events.len(), scheduled.event));
            }
            let external = if is_consumption {
                None
            } else {
                self.prepare_external(scheduled)?
            };
            events.push(CandidateInputEvent {
                scheduled,
                consumption: None,
                external,
            });
        }
        anyhow::ensure!(
            !events.is_empty(),
            "candidate input decoder produced an empty block cursor={start_cursor}"
        );

        for (tape_key, indexed_events) in consumption_events {
            let tape = self
                .tapes
                .get(&tape_key)
                .with_context(|| format!("candidate tape missing symbol={tape_key}"))?;
            let cursor = self
                .consumption_cursors
                .get_mut(&tape_key)
                .with_context(|| {
                    format!("candidate consumption cursor missing symbol={tape_key}")
                })?;
            let refs = indexed_events
                .iter()
                .map(|(_, event)| *event)
                .collect::<Vec<_>>();
            let records = tape.strategy_consumptions(cursor, &refs, pool)?;
            let catalog = tape.market_catalog_arc();
            let catalog_sha256 = catalog.sha256();
            let prepared = pool.install(|| {
                records
                    .into_par_iter()
                    .map(|record| CandidateConsumptionInput {
                        projection: market_projection_for_catalog(&record.odds, &catalog),
                        record,
                        catalog_sha256,
                    })
                    .collect::<Vec<_>>()
            });
            anyhow::ensure!(
                prepared.len() == indexed_events.len(),
                "candidate consumption preparation count mismatch symbol={tape_key}"
            );
            for ((event_index, _), consumption) in indexed_events.into_iter().zip(prepared) {
                events[event_index].consumption = Some(consumption);
            }
        }

        Ok(CandidateInputBlock {
            start_cursor,
            events,
        })
    }

    fn prepare_external(
        &mut self,
        scheduled: ScheduledEventRef,
    ) -> Result<Option<CandidateExternalInput>> {
        match scheduled.target {
            ScheduledEventTarget::Central => Ok(None),
            ScheduledEventTarget::Symbol(symbol) => {
                let (_, tape) = tape_for_symbol(self.tapes, symbol.as_str())?;
                match scheduled.event.kind {
                    EVENT_KIND_CLOCK | EVENT_KIND_WINDOW_BOOTSTRAP => {
                        let receipt_identity = tape.captured_receipt_identity(scheduled.event)?;
                        let mut book_updates = Vec::new();
                        let publish_account_state = tape.apply_market_state(
                            scheduled.event,
                            &mut self.external_odds,
                            |update| {
                                book_updates.push(update);
                                Ok(())
                            },
                        )?;
                        let projection = publish_account_state
                            .then(|| MarketProjection::from_odds(&self.external_odds));
                        Ok(Some(CandidateExternalInput::MarketState {
                            odds: Arc::new(self.external_odds.clone()),
                            projection,
                            book_updates: Arc::from(book_updates),
                            receipt_identity,
                            publish_account_state,
                        }))
                    }
                    EVENT_KIND_LAST_TRADE => Ok(Some(CandidateExternalInput::LastTrade {
                        trade: Arc::new(tape.last_trade(scheduled.event)?.clone()),
                        receipt_identity: tape.captured_receipt_identity(scheduled.event)?,
                    })),
                    other => anyhow::bail!(
                        "candidate external symbol event kind is invalid kind={other}"
                    ),
                }
            }
            ScheduledEventTarget::CentralResolution(symbol) => {
                anyhow::ensure!(
                    scheduled.event.kind == EVENT_KIND_PROVIDER_RESOLUTION,
                    "candidate central resolution event has invalid kind={}",
                    scheduled.event.kind
                );
                let (_, tape) = tape_for_symbol(self.tapes, symbol.as_str())?;
                let resolution = Arc::new(tape.resolution(scheduled.event)?.clone());
                let changed = prepare_provider_resolution_fact(
                    &mut self.external_odds,
                    tape.market_catalog(),
                    resolution.as_ref(),
                    scheduled.event.timestamp_ms,
                )?;
                let (odds, projection) = if changed {
                    (
                        Some(Arc::new(self.external_odds.clone())),
                        Some(MarketProjection::from_odds(&self.external_odds)),
                    )
                } else {
                    (None, None)
                };
                Ok(Some(CandidateExternalInput::ProviderResolution {
                    resolution,
                    changed,
                    odds,
                    projection,
                }))
            }
        }
    }
}

fn tape_for_symbol<'a>(
    tapes: &'a BTreeMap<String, Arc<PackedReplayTape>>,
    symbol: &str,
) -> Result<(&'a String, &'a Arc<PackedReplayTape>)> {
    tapes
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(symbol))
        .with_context(|| format!("candidate tape missing symbol={symbol}"))
}

fn market_projection_for_catalog(
    odds: &PolymarketOddsState,
    catalog: &MarketCatalog,
) -> MarketProjection {
    let market_is_eligible =
        |up_token: polymarket_client_sdk::types::U256,
         down_token: polymarket_client_sdk::types::U256| {
            catalog.token_handle(up_token.to_be_bytes::<32>()).is_some()
                && catalog
                    .token_handle(down_token.to_be_bytes::<32>())
                    .is_some()
        };
    let projection_source = PolymarketOddsState {
        updated_at_ms: odds.updated_at_ms,
        revision: odds.revision,
        selection_updated_at_ms: odds.selection_updated_at_ms,
        next_selection_updated_at_ms: odds.next_selection_updated_at_ms,
        selection: odds
            .selection
            .as_ref()
            .filter(|market| market_is_eligible(market.up_token, market.down_token))
            .cloned(),
        next_selection: odds
            .next_selection
            .as_ref()
            .filter(|market| market_is_eligible(market.up_token, market.down_token))
            .cloned(),
        markets: odds
            .markets
            .iter()
            .filter(|market| market_is_eligible(market.up_token, market.down_token))
            .cloned()
            .collect(),
        ..PolymarketOddsState::default()
    };
    MarketProjection::from_odds(&projection_source)
}
