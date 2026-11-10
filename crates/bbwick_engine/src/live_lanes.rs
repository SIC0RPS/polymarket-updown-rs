use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use polymarket_client_sdk::clob::types::response::OrderBookSummaryResponse;
use polymarket_client_sdk::types::B256;
use polymarket_client_sdk::types::U256;
use tokio::sync::{broadcast, mpsc, watch};

use crate::ids::{IdCanonicalTopOfBookFrame, IdTopOfBookIngressFrame, MarketDataKeyRegistry};
use crate::ingest_event::IngestEvent;
use crate::market_data::CanonicalTopOfBook;
use crate::oracle_state_updates;
use crate::state::{PolymarketOddsState, PolymarketStateStore, PriceChangeApplyOutcome};
use crate::wire::{
    PolymarketBookFrame, PolymarketLastTradeFrame, PolymarketPriceChangeFrame,
    PolymarketTickSizeFrame,
};

#[derive(Clone)]
pub(crate) struct ProviderQuoteLane {
    key_registry: Arc<MarketDataKeyRegistry>,
    top_of_book_tx: broadcast::Sender<CanonicalTopOfBook>,
    ingest_mono_origin: Instant,
    seq_no: Arc<AtomicU64>,
}

impl ProviderQuoteLane {
    pub(crate) fn new(
        key_registry: Arc<MarketDataKeyRegistry>,
        top_of_book_tx: broadcast::Sender<CanonicalTopOfBook>,
        ingest_mono_origin: Instant,
    ) -> Self {
        Self {
            key_registry,
            top_of_book_tx,
            ingest_mono_origin,
            seq_no: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn publish_top_of_book(&self, frame: IdTopOfBookIngressFrame) -> Result<()> {
        let seq_no = self
            .seq_no
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        let canonical_frame = IdCanonicalTopOfBookFrame {
            symbol_id: frame.symbol_id,
            bid_px: frame.bid_px,
            ask_px: frame.ask_px,
            bid_sz: frame.bid_sz,
            ask_sz: frame.ask_sz,
            queue_imbalance_l5: frame.queue_imbalance_l5,
            source_ts_ns: frame.source_ts_ms.saturating_mul(1_000_000),
            source_ts_kind: frame.source_ts_kind,
            ingest_mono_ns: self.ingest_mono_origin.elapsed().as_nanos() as u64,
            seq_no,
        };
        let canonical = self
            .key_registry
            .materialize_canonical_top_of_book(canonical_frame);
        self.top_of_book_tx
            .send(canonical)
            .map(|_| ())
            .map_err(|_| anyhow::anyhow!("bbwick top-of-book receiver unavailable"))
    }
}

#[derive(Clone)]
pub struct PolymarketStateLane {
    state_store: Arc<Mutex<PolymarketStateStore>>,
    polymarket_state_tx: watch::Sender<PolymarketOddsState>,
}

impl PolymarketStateLane {
    pub fn new(
        state_store: Arc<Mutex<PolymarketStateStore>>,
        polymarket_state_tx: watch::Sender<PolymarketOddsState>,
    ) -> Self {
        Self {
            state_store,
            polymarket_state_tx,
        }
    }

    pub(crate) fn sync_book(
        &self,
        market: B256,
        frame: &PolymarketBookFrame,
        frame_received_at: Option<Instant>,
        connection_session_id: Option<u64>,
        frame_sequence: Option<u64>,
    ) -> Result<bool> {
        self.sync_market_state(
            market,
            frame_received_at,
            connection_session_id,
            frame_sequence,
            |store, state, now| store.sync_book(state, market, frame, now),
        )
    }

    pub(crate) fn sync_book_with_provenance(
        &self,
        market: B256,
        frame: &PolymarketBookFrame,
        digest: u64,
        first_seen: Instant,
        expires_at: Instant,
        payload: &Arc<[u8]>,
        frame_received_at: Option<Instant>,
        connection_session_id: Option<u64>,
        frame_sequence: Option<u64>,
    ) -> Result<Option<u64>> {
        let now = oracle_state_updates::now_ms();
        let mut store = self
            .state_store
            .lock()
            .map_err(|_| anyhow::anyhow!("polymarket state store lock poisoned"))?;
        let next_expiry = store.prune_expired_provenance(Instant::now());
        let mut validated_epoch = None;
        let _ = self.polymarket_state_tx.send_if_modified(|state| {
            let (changed, valid_delivery) = store.sync_book_with_outcome(state, market, frame, now);
            if valid_delivery {
                validated_epoch = store.record_validated_book(
                    frame.asset_id,
                    digest,
                    first_seen,
                    expires_at,
                    payload,
                    next_expiry,
                );
            }
            if changed {
                self.record_frame_timing(
                    &mut store,
                    state,
                    market,
                    frame_received_at,
                    connection_session_id,
                    frame_sequence,
                );
            }
            changed
        });
        Ok(validated_epoch)
    }

    pub(crate) fn validated_book_epochs(
        &self,
        token_ids: &[U256],
        digest: u64,
        first_seen: Instant,
        payload: &[u8],
    ) -> Result<Vec<(U256, u64)>> {
        let mut store = self
            .state_store
            .lock()
            .map_err(|_| anyhow::anyhow!("polymarket state store lock poisoned"))?;
        Ok(store.validated_book_epochs(token_ids, digest, first_seen, payload))
    }

    pub(crate) fn with_book_authorities<T>(
        &self,
        check: impl FnOnce(&PolymarketStateStore) -> T,
    ) -> Result<T> {
        let store = self
            .state_store
            .lock()
            .map_err(|_| anyhow::anyhow!("polymarket state store lock poisoned"))?;
        Ok(check(&store))
    }

    pub(crate) fn finish_transport(
        &self,
        finish: impl FnOnce(&PolymarketStateStore) -> (usize, Vec<U256>),
    ) -> Result<usize> {
        let now = oracle_state_updates::now_ms();
        let mut store = self
            .state_store
            .lock()
            .map_err(|_| anyhow::anyhow!("polymarket state store lock poisoned"))?;
        let (remaining, unserved) = finish(&store);
        if !unserved.is_empty() {
            let _ = self.polymarket_state_tx.send_if_modified(|state| {
                unserved.into_iter().fold(false, |changed, token_id| {
                    (state.token_lifecycle.contains_key(&token_id)
                        && store.sync_needs_snapshot(state, token_id, now))
                        || changed
                })
            });
        }
        Ok(remaining)
    }

    pub fn sync_needs_snapshot(
        &self,
        token_id: polymarket_client_sdk::types::U256,
    ) -> Result<bool> {
        self.sync_state(|store, state, now| store.sync_needs_snapshot(state, token_id, now))
    }

    pub(crate) fn sync_invalid_ws_event(
        &self,
        market: B256,
        token_id: polymarket_client_sdk::types::U256,
        frame_received_at: Option<Instant>,
        connection_session_id: Option<u64>,
        frame_sequence: Option<u64>,
    ) -> Result<bool> {
        self.sync_market_state(
            market,
            frame_received_at,
            connection_session_id,
            frame_sequence,
            |store, state, now| store.sync_invalid_ws_event(state, market, token_id, now),
        )
    }

    pub(crate) fn sync_price_change(
        &self,
        market: B256,
        frame: &PolymarketPriceChangeFrame,
        frame_received_at: Option<Instant>,
        connection_session_id: Option<u64>,
        frame_sequence: Option<u64>,
    ) -> Result<PriceChangeApplyOutcome> {
        let now = oracle_state_updates::now_ms();
        let mut store = self
            .state_store
            .lock()
            .map_err(|_| anyhow::anyhow!("polymarket state store lock poisoned"))?;
        let mut outcome = PriceChangeApplyOutcome::default();
        let _ = self.polymarket_state_tx.send_if_modified(|state| {
            outcome = store.sync_price_change_with_outcome(state, market, frame, now);
            if outcome.changed {
                self.record_frame_timing(
                    &mut store,
                    state,
                    market,
                    frame_received_at,
                    connection_session_id,
                    frame_sequence,
                );
            }
            outcome.changed
        });
        Ok(outcome)
    }

    pub(crate) fn sync_tick_size(
        &self,
        market: B256,
        frame: &PolymarketTickSizeFrame,
        frame_received_at: Option<Instant>,
        connection_session_id: Option<u64>,
        frame_sequence: Option<u64>,
    ) -> Result<bool> {
        self.sync_market_state(
            market,
            frame_received_at,
            connection_session_id,
            frame_sequence,
            |store, state, now| store.sync_tick_size(state, market, frame, now),
        )
    }

    pub fn sync_rest_book(
        &self,
        token_id: polymarket_client_sdk::types::U256,
        response: &OrderBookSummaryResponse,
    ) -> Result<bool> {
        self.sync_state(|store, state, now| store.sync_rest_book(state, token_id, response, now))
    }

    pub fn sync_tombstone_no_orderbook(
        &self,
        token_id: polymarket_client_sdk::types::U256,
    ) -> Result<bool> {
        self.sync_state(|store, state, now| store.sync_tombstone_no_orderbook(state, token_id, now))
    }

    pub(crate) fn sync_last_trade(
        &self,
        market: B256,
        frame: &PolymarketLastTradeFrame,
        frame_received_at: Option<Instant>,
        connection_session_id: Option<u64>,
        frame_sequence: Option<u64>,
    ) -> Result<bool> {
        self.sync_market_state(
            market,
            frame_received_at,
            connection_session_id,
            frame_sequence,
            |store, state, now| store.sync_ws_last_trade(state, market, frame, now),
        )
    }

    fn sync_market_state(
        &self,
        market: B256,
        frame_received_at: Option<Instant>,
        connection_session_id: Option<u64>,
        frame_sequence: Option<u64>,
        apply: impl FnOnce(&mut PolymarketStateStore, &mut PolymarketOddsState, u64) -> bool,
    ) -> Result<bool> {
        self.sync_state(|store, state, now| {
            let changed = apply(store, state, now);
            if changed {
                self.record_frame_timing(
                    store,
                    state,
                    market,
                    frame_received_at,
                    connection_session_id,
                    frame_sequence,
                );
            }
            changed
        })
    }

    fn record_frame_timing(
        &self,
        store: &mut PolymarketStateStore,
        state: &mut PolymarketOddsState,
        market: B256,
        frame_received_at: Option<Instant>,
        connection_session_id: Option<u64>,
        frame_sequence: Option<u64>,
    ) {
        if let (Some(frame_received_at), Some(connection_session_id), Some(frame_sequence)) =
            (frame_received_at, connection_session_id, frame_sequence)
        {
            store.record_market_frame_timing(
                state,
                market,
                frame_received_at,
                Instant::now(),
                connection_session_id,
                frame_sequence,
            );
        }
    }

    pub(crate) fn sync_state(
        &self,
        apply: impl FnOnce(&mut PolymarketStateStore, &mut PolymarketOddsState, u64) -> bool,
    ) -> Result<bool> {
        let now = oracle_state_updates::now_ms();
        let mut store = self
            .state_store
            .lock()
            .map_err(|_| anyhow::anyhow!("polymarket state store lock poisoned"))?;
        let mut changed = false;
        let _ = self.polymarket_state_tx.send_if_modified(|state| {
            changed = apply(&mut store, state, now);
            changed
        });
        Ok(changed)
    }
}

#[derive(Clone)]
pub(crate) struct OracleTapeLane {
    ingest_tx: mpsc::Sender<IngestEvent>,
}

impl OracleTapeLane {
    pub(crate) fn new(ingest_tx: mpsc::Sender<IngestEvent>) -> Self {
        Self { ingest_tx }
    }

    pub(crate) fn try_send(&self, event: IngestEvent, lane: &'static str) -> Result<()> {
        match self.ingest_tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    target: "engine.oracle_tape",
                    lane,
                    "bbwick_oracle_tape_queue_full"
                );
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                anyhow::bail!("{lane} ingest queue closed")
            }
        }
    }
}
