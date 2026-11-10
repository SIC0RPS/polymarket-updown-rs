use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use polymarket_client_sdk::types::{B256, U256};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::super::CentralSymbolProjection;
use super::{
    transport, CandidateInputBlock, CandidateReplayResult, CandidateRunnerInputs, CandidateSlot,
    CapturedCentralBaseline, CapturedClobSubmitClock, CapturedClobSubmitModel,
    CapturedCtfChainClock, CapturedCtfChainModel, CapturedCtfSplitVisibility, DueClobSubmissions,
    ScheduledEventTarget,
};
use crate::capture::session::MaybeCapture;
use crate::config::AppConfig;
use crate::ctf_inventory::{expected_ctf_positions_for_confirmed_split, CtfInventoryRuntime};
use crate::engine_trade_journal::build_confirmed_ctf_split_entry_record;
use crate::execution::order_lifecycle::is_ctf_market_entry_sell_intent;
use crate::execution::supervisor::{
    CapturedExecutionSupervisorBatch, CapturedExecutionSupervisorRuntime,
    CapturedExecutionSupervisorStart,
};
use crate::execution::{ExecutionClient, ExecutionSimHandle};
use crate::observability::Observability;
use crate::orders::OrderUpdateEvent;
use crate::polymarket::order_context::{FillClassification, OrderFillContextRegistry};
use crate::polymarket::state::{MarketProjection, PolymarketOddsState};
use crate::portfolio::{FillAdmission, FillEvent, FillSide, PortfolioState};
use crate::risk::{AccountEquitySnapshot, RiskHandle};
use crate::strategy::{CancelRequest, StrategyAction};
use crate::strategy_runtime::backtest_matrix::{BacktestMatrixCandidate, SymbolRow};
use crate::strategy_runtime::exchange_sim::{CapturedVenueFact, ExchangeSim};
use crate::strategy_runtime::market_catalog::MarketCatalog;
use crate::strategy_runtime::symbol_process::{
    CentralWorkerCommandOwner, DecodedIntent, Drive, DriveKind, SymbolId, WorkerCommandBatch,
};
use crate::trade_session_ledger::FinancialAdmissionDisposition;
use crate::trade_session_ledger::{
    CanonicalAuditLedger, LocalAccountingOwner, ReceiptBlockIdentity, SessionReceipt,
};

#[path = "run/ctf_visibility.rs"]
mod ctf_visibility;
use ctf_visibility::drain_next_due_ctf_split;
#[path = "run/ctf_admission.rs"]
mod ctf_admission;
use ctf_admission::{reserve_ctf_split_intent, schedule_ctf_inventory_intent};
#[path = "run/central_receipts.rs"]
mod central_receipts;
use central_receipts::{
    apply_central_event, symbol_for_condition, symbol_for_inventory_intent,
    CandidateTerminalSettlementOwner,
};
#[path = "run/external_events.rs"]
mod external_events;
#[path = "run/provider_resolution.rs"]
mod provider_resolution;
use external_events::{apply_external_event, ensure_initial_admission, tape_for_symbol};
pub(super) use provider_resolution::prepare_provider_resolution_fact;
#[path = "run/submission_audit.rs"]
mod submission_audit;

const FIXED_POINT_LIMIT: usize = 256;
const CONFIRMED_CTF_COHORT_INVENTORY_MISMATCH: &str = "confirmed_ctf_cohort_inventory_mismatch";

#[path = "run/central_state.rs"]
mod central_state;
use central_state::CandidateCentralState;
#[path = "run/convergence.rs"]
mod convergence;
use convergence::{converge_central_consequences, drain_next_due_clob_batch};
#[path = "run/views.rs"]
mod views;
use views::{send_admitted_views, send_symbol_views};

pub(crate) struct CandidateRun {
    candidate_id: String,
    collect_prediction_metrics: bool,
    central: CandidateCentralState,
    cursor: usize,
    active_timestamp_ms: Option<u64>,
}

pub(crate) async fn initialize_candidate(
    slot: &mut CandidateSlot,
    cfg: &AppConfig,
    candidate: &BacktestMatrixCandidate,
    inputs: &CandidateRunnerInputs<'_>,
    baseline: &CapturedCentralBaseline,
    obs: Observability,
) -> Result<CandidateRun> {
    slot.reset_workers(candidate)?;
    let candidate_id = candidate.id.clone();
    let schedule = slot.schedule().clone();
    let clob_submit_model = Arc::clone(slot.clob_submit_model());
    let ctf_chain_model = Arc::clone(slot.ctf_chain_model());
    let central = CandidateCentralState::start(
        cfg,
        candidate,
        inputs,
        &schedule,
        baseline,
        &clob_submit_model,
        &ctf_chain_model,
        obs,
    )
    .await?;
    Ok(CandidateRun {
        candidate_id,
        collect_prediction_metrics: candidate.collect_prediction_metrics,
        central,
        cursor: 0,
        active_timestamp_ms: None,
    })
}

pub(crate) async fn advance_candidate(
    slot: &mut CandidateSlot,
    inputs: &CandidateRunnerInputs<'_>,
    run: &mut CandidateRun,
    block: &CandidateInputBlock,
) -> Result<()> {
    anyhow::ensure!(
        run.cursor == block.start_cursor,
        "candidate block cursor mismatch candidate={} expected={} actual={}",
        run.candidate_id,
        run.cursor,
        block.start_cursor
    );
    let schedule = slot.schedule().clone();
    let events = schedule.events();
    for input_event in &block.events {
        let scheduled = *events
            .get(run.cursor)
            .context("candidate block exceeds merged event schedule")?;
        anyhow::ensure!(
            scheduled == input_event.scheduled,
            "candidate block event differs from merged schedule candidate={} cursor={}",
            run.candidate_id,
            run.cursor
        );
        let timestamp_ms = scheduled.event.timestamp_ms;
        if run.active_timestamp_ms != Some(timestamp_ms) {
            anyhow::ensure!(
                run.active_timestamp_ms.is_none(),
                "candidate timestamp group changed before prior group completed candidate={} previous={:?} next={timestamp_ms}",
                run.candidate_id,
                run.active_timestamp_ms
            );
            drain_internal_before(slot, inputs, &mut run.central, timestamp_ms).await?;
            drain_due_central_visibility(slot, inputs, &mut run.central, timestamp_ms).await?;
            run.active_timestamp_ms = Some(timestamp_ms);
        }

        match scheduled.target {
            ScheduledEventTarget::Central => {
                anyhow::ensure!(
                    input_event.consumption.is_none() && input_event.external.is_none(),
                    "central replay event carries a prepared symbol input"
                );
                slot.observe_central_timestamp(timestamp_ms);
                apply_central_event(
                    run.candidate_id.as_str(),
                    slot,
                    inputs,
                    &mut run.central,
                    scheduled.event,
                )
                .await?;
            }
            ScheduledEventTarget::CentralResolution(symbol) => {
                anyhow::ensure!(
                    input_event.consumption.is_none(),
                    "central resolution event carries a strategy consumption record"
                );
                let external = input_event
                    .external
                    .as_ref()
                    .context("central resolution event has no prepared external input")?;
                slot.observe_central_timestamp(timestamp_ms);
                provider_resolution::apply_provider_resolution(
                    slot,
                    inputs,
                    &mut run.central,
                    symbol,
                    scheduled.event,
                    external,
                )
                .await?;
            }
            ScheduledEventTarget::Symbol(symbol) => {
                let is_consumption = scheduled.event.kind
                    == super::super::prepared_event_stream::EVENT_KIND_STRATEGY_CONSUMPTION;
                anyhow::ensure!(
                    is_consumption == input_event.consumption.is_some(),
                    "candidate block consumption coverage mismatch candidate={} cursor={}",
                    run.candidate_id,
                    run.cursor
                );
                let intents = if let Some(consumption) = input_event.consumption.as_ref() {
                    anyhow::ensure!(
                        input_event.external.is_none(),
                        "strategy consumption event carries prepared external input"
                    );
                    ensure_initial_admission(slot, &mut run.central, symbol, timestamp_ms)?;
                    let output = slot.process_mut(symbol)?.process_event(
                        scheduled.event,
                        &consumption.record,
                        consumption.projection.clone(),
                        consumption.catalog_sha256,
                    )?;
                    let intents = run.central.accept_worker_output(output)?;
                    run.central
                        .worker_source_sequences
                        .insert(symbol.as_str().to_string(), scheduled.event.sequence);
                    intents
                } else {
                    let external = input_event
                        .external
                        .as_ref()
                        .context("external replay event has no prepared input")?;
                    let publish_account_state =
                        apply_external_event(external, scheduled.event, &mut run.central)?;
                    let newly_admitted =
                        ensure_initial_admission(slot, &mut run.central, symbol, timestamp_ms)?;
                    if scheduled.event.projection_changed()
                        && !newly_admitted
                        && run.central.admitted.contains(symbol.as_str())
                    {
                        let source_sequence = run.central.worker_source_sequences[symbol.as_str()];
                        send_symbol_views(
                            slot,
                            &mut run.central,
                            symbol,
                            source_sequence,
                            timestamp_ms,
                            publish_account_state,
                            false,
                            &[],
                        )?;
                    }
                    Vec::new()
                };
                converge_central_consequences(
                    slot,
                    inputs,
                    &mut run.central,
                    timestamp_ms,
                    intents,
                )
                .await?;
            }
        }
        drain_due_central_visibility(slot, inputs, &mut run.central, timestamp_ms).await?;
        run.cursor = run
            .cursor
            .checked_add(1)
            .context("candidate merged event cursor overflow")?;
        if events
            .get(run.cursor)
            .is_none_or(|next| next.event.timestamp_ms != timestamp_ms)
        {
            run.active_timestamp_ms = None;
        }
    }
    Ok(())
}

pub(crate) async fn finalize_candidate(
    slot: &mut CandidateSlot,
    inputs: &CandidateRunnerInputs<'_>,
    run: &mut CandidateRun,
) -> Result<CandidateReplayResult> {
    let schedule_len = slot.schedule().events().len();
    anyhow::ensure!(
        run.cursor == schedule_len && run.active_timestamp_ms.is_none(),
        "candidate finalized before replay endpoint candidate={} cursor={}/{} timestamp_phase={:?}",
        run.candidate_id,
        run.cursor,
        schedule_len,
        run.active_timestamp_ms
    );
    let run_result = async {
        if let Some((_, end_ms)) = inputs.window {
            drain_internal_through(slot, inputs, &mut run.central, end_ms).await?;
        }

        let replay_end_ts_ms = inputs
            .window
            .context("candidate replay endpoint is unavailable")?
            .1;
        let (oracle_learning, prediction_metrics) =
            slot.finalize_workers(&run.central.central_sequences, replay_end_ts_ms)?;
        anyhow::ensure!(
            if run.collect_prediction_metrics {
                prediction_metrics.len() == inputs.symbols.len()
                    && prediction_metrics.keys().eq(oracle_learning.keys())
            } else {
                prediction_metrics.is_empty()
            },
            "candidate prediction collection scope mismatch"
        );
        let clob_submit_model = run.central.clob_submit_clock.identity().clone();
        let pending_clob_submissions = run.central.clob_submit_clock.pending_len();
        let pending_clob_receipts = run.central.clob_submit_clock.pending_receipts_len();
        let (clob_supported_admission_submissions, clob_approximate_admission_submissions) =
            run.central.clob_submit_clock.admission_coverage();
        let clob_timing_diagnostics = run.central.clob_submit_clock.diagnostics().to_vec();
        run.central.clob_submit_clock.reset();
        let ctf_chain_model = run.central.ctf_chain.identity().clone();
        let pending_ctf_split_receipts = run.central.ctf_chain.pending_len();
        let ctf_timing_diagnostics = run.central.ctf_chain.diagnostics().to_vec();
        run.central.ctf_chain.reset();
        let accounting = run.central.accounting.current();
        let mut stats = run
            .central
            .audit
            .take()
            .context("candidate audit ledger is unavailable")?
            .finish(accounting.financial.as_ref())?;
        stats.project_venue_outcomes(None, Some(run.central.odds.as_ref()));
        let portfolio_accounting = run
            .central
            .portfolio
            .canonical_accounting_projection(
                Some(accounting.financial.financial_view()),
                Some(accounting.confirmed_portfolio_receipts.as_ref()),
                Some(run.central.odds.as_ref()),
                replay_end_ts_ms,
            )
            .proof();
        let rows = inputs
            .symbols
            .iter()
            .map(|symbol| SymbolRow {
                symbol: symbol.to_ascii_uppercase(),
                status: "PASS".to_string(),
                error: None,
            })
            .collect();
        Ok(CandidateReplayResult {
            candidate_id: run.candidate_id.clone(),
            elapsed_ms: 0,
            rows,
            canonical_session_stats: stats,
            portfolio_accounting,
            clob_submit_model,
            pending_clob_submissions,
            pending_clob_receipts,
            clob_supported_admission_submissions,
            clob_approximate_admission_submissions,
            clob_timing_diagnostics,
            ctf_chain_model,
            pending_ctf_split_receipts,
            ctf_timing_diagnostics,
            oracle_learning,
            prediction_metrics,
        })
    }
    .await;
    let shutdown = run.central.shutdown_supervisor().await;
    match (run_result, shutdown) {
        (Ok(result), Ok(())) => Ok(result),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error.context("shutdown candidate central supervisor")),
        (Err(error), Err(shutdown_error)) => Err(error.context(format!(
            "candidate central supervisor shutdown also failed: {shutdown_error:#}"
        ))),
    }
}

pub(crate) async fn abort_candidate(mut run: CandidateRun) -> Result<()> {
    run.central.shutdown_supervisor().await
}

fn next_central_visibility_ms(central: &CandidateCentralState) -> Option<u64> {
    [
        central.exchange.next_gtd_expiry_ms(),
        central.ctf_chain.next_due_ts(),
        central.clob_submit_clock.next_due_ts(),
    ]
    .into_iter()
    .flatten()
    .min()
}

async fn drain_internal_before(
    slot: &mut CandidateSlot,
    inputs: &CandidateRunnerInputs<'_>,
    central: &mut CandidateCentralState,
    before_ts_ms: u64,
) -> Result<()> {
    loop {
        let Some(next_due_ms) =
            next_central_visibility_ms(central).filter(|deadline_ms| *deadline_ms < before_ts_ms)
        else {
            return Ok(());
        };
        drain_due_central_visibility(slot, inputs, central, next_due_ms).await?;
    }
}

async fn drain_internal_through(
    slot: &mut CandidateSlot,
    inputs: &CandidateRunnerInputs<'_>,
    central: &mut CandidateCentralState,
    through_ts_ms: u64,
) -> Result<()> {
    loop {
        let Some(next_due_ms) =
            next_central_visibility_ms(central).filter(|deadline_ms| *deadline_ms <= through_ts_ms)
        else {
            return Ok(());
        };
        drain_due_central_visibility(slot, inputs, central, next_due_ms).await?;
    }
}

async fn drain_due_central_visibility(
    slot: &mut CandidateSlot,
    inputs: &CandidateRunnerInputs<'_>,
    central: &mut CandidateCentralState,
    up_to_ts_ms: u64,
) -> Result<()> {
    loop {
        let ctf_due = central.ctf_chain.next_due_ts();
        let clob_due = central.clob_submit_clock.next_due_ts();
        let gtd_due = central.exchange.next_gtd_expiry_ms();
        let Some(due_ts_ms) = [gtd_due, ctf_due, clob_due]
            .into_iter()
            .flatten()
            .min()
            .filter(|due| *due <= up_to_ts_ms)
        else {
            return Ok(());
        };
        if gtd_due == Some(due_ts_ms) {
            slot.observe_central_timestamp(due_ts_ms);
            converge_central_consequences(slot, inputs, central, due_ts_ms, Vec::new()).await?;
        } else if ctf_due == Some(due_ts_ms) {
            drain_next_due_ctf_split(slot, inputs, central, due_ts_ms).await?;
        } else {
            drain_next_due_clob_batch(slot, inputs, central, due_ts_ms).await?;
        }
    }
}

fn apply_confirmed_fills(
    central: &mut CandidateCentralState,
    fills: &[FillEvent],
    timestamp_ms: u64,
) -> Result<()> {
    for fill in fills {
        match central.order_fill_context.normalize_fill(fill.clone()) {
            FillClassification::Normalized(fill) => {
                let exact_fill = fill.fill();
                let fill_id = exact_fill
                    .canonical_fill_id()
                    .context("candidate confirmed fill has no canonical receipt identity")?;
                let token = exact_fill.token.clone();
                let is_buy = exact_fill.side == FillSide::Buy;
                let is_direct_buy = is_buy && exact_fill.direct_buy_authorization.is_some();
                let qty_units_6dp = exact_fill
                    .receipt_qty_units_6dp
                    .context("candidate confirmed fill has no exact receipt quantity")?;
                let mut accounting_error = None;
                let audit = central
                    .audit
                    .as_mut()
                    .context("candidate audit ledger is unavailable")?;
                let accounting = &mut central.accounting;
                let next_audit_sequence = &mut central.next_audit_sequence;
                let admission = central
                    .portfolio
                    .apply_runtime_fill_with_accounting_destination(fill, |record| {
                        let sequence = *next_audit_sequence;
                        let next_sequence = sequence
                            .checked_add(1)
                            .context("candidate audit sequence overflow")?;
                        let mut disposition = None;
                        if let Err(error) = audit.ingest(sequence, &record, |record| {
                            disposition = Some(accounting.admit_record(record.clone())?);
                            Ok(())
                        }) {
                            accounting_error = Some(format!("{error:#}"));
                            return Err(error);
                        }
                        *next_audit_sequence = next_sequence;
                        disposition.context("candidate accounting admission did not execute")
                    });
                anyhow::ensure!(
                    accounting_error.is_none(),
                    "candidate managed-sell accounting failed: {}",
                    accounting_error.unwrap_or_default()
                );
                let confirmed_buy_condition = match &admission {
                    FillAdmission::DirectBuyAdmitted(fact) => Some(fact.authorization.condition_id),
                    _ => None,
                };
                match admission {
                    FillAdmission::Admitted => {}
                    FillAdmission::DirectBuyAdmitted(fact) => {
                        central.symbol_projections.get(fact.authorization.symbol.as_str())
                            .context("BUY central symbol projection is absent")?
                            .claim_cohort(&fact.cohort_id)?;
                        central.portfolio_tx.send_replace(central.portfolio.clone());
                    }
                    FillAdmission::DefinitivelyRejected => anyhow::bail!(
                        "candidate portfolio definitively rejected confirmed fill order_id={} ts_ms={timestamp_ms} reason={}",
                        fills.first().map(|fill| fill.order_id.as_str()).unwrap_or("unknown"),
                        central.portfolio.quarantined_fills.back().map(|fill| fill.reason.as_str()).unwrap_or("unknown")
                    ),
                    FillAdmission::Unresolved { error, .. } => {
                        return Err(error.context(
                            "candidate managed-sell accounting admission is unresolved",
                        ));
                    }
                }
                if is_direct_buy && confirmed_buy_condition.is_none() {
                    continue;
                }
                central.ctf_chain.apply_confirmed_token_fill(
                    &fill_id,
                    &token,
                    qty_units_6dp,
                    is_buy,
                    timestamp_ms,
                )?;
                if let Some(condition_id) = confirmed_buy_condition {
                    let retained_resolution = match (
                        central.odds.resolved_market_winners.get(&condition_id),
                        central.odds.resolved_market_contexts.get(&condition_id),
                    ) {
                        (Some(winner), Some(context)) => Some(
                            crate::strategy_runtime::replay_mode::prepared_arrow::ResolutionRow {
                                condition_id: condition_id.to_string(),
                                up_token: context.up_token.to_string(),
                                down_token: context.down_token.to_string(),
                                winning_asset_id: winner.to_string(),
                                observed_at_ms: context.observed_at_ms,
                                raw_response_sha256: alloy::hex::encode(
                                    context.raw_response_sha256,
                                ),
                            },
                        ),
                        (None, None) => None,
                        _ => anyhow::bail!("late BUY has incomplete retained provider resolution"),
                    };
                    if let Some(resolution) = retained_resolution {
                        central_receipts::apply_simulated_resolution_redemption(
                            central,
                            &resolution,
                            timestamp_ms,
                        )?;
                    }
                }
            }
            FillClassification::Quarantined(quarantined) => anyhow::bail!(
                "candidate venue fill quarantined reason={} order_id={}",
                quarantined.reason,
                quarantined.fill.order_id
            ),
        }
    }
    Ok(())
}

fn reserve_submitted_order_handle(
    inputs: &CandidateRunnerInputs<'_>,
    central: &mut CandidateCentralState,
    submitted: &crate::execution::SimSubmittedOrder,
) -> Result<Option<bool>> {
    let symbol = submitted
        .intent
        .lifecycle_symbol
        .as_deref()
        .context("submitted candidate order has no lifecycle symbol")?
        .to_ascii_uppercase();
    anyhow::ensure!(
        inputs
            .symbols
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&symbol)),
        "submitted candidate order carries foreign lifecycle symbol={symbol}"
    );
    let catalog = central
        .catalogs
        .get(&symbol)
        .context("submitted candidate order has no frozen symbol catalog")?;
    let market_id = submitted
        .intent
        .market_id
        .as_ref()
        .context("submitted candidate order has no market identity")?;
    let condition_id = market_id
        .as_str()
        .parse::<B256>()
        .context("submitted candidate order market identity is invalid")?;
    let condition_id: [u8; 32] = condition_id.into();
    let market_handle = catalog
        .market_handle_for_condition(condition_id)
        .context("submitted candidate order market is absent from frozen symbol catalog")?;
    let market = catalog
        .record(market_handle)
        .context("submitted candidate order market handle is absent from frozen symbol catalog")?;
    let token = submitted
        .intent
        .instrument
        .parse::<U256>()
        .context("submitted candidate order token identity is invalid")?;
    anyhow::ensure!(
        token == U256::from_be_bytes(market.up_token)
            || token == U256::from_be_bytes(market.down_token),
        "submitted candidate order token is foreign to frozen market identity"
    );
    let current_market = central
        .odds
        .markets
        .iter()
        .chain(central.odds.selection.iter())
        .chain(central.odds.next_selection.iter())
        .find(|candidate| candidate.condition_id == market_id.as_str())
        .context("submitted candidate order market is absent from current captured state")?;
    anyhow::ensure!(
        current_market.up_token == U256::from_be_bytes(market.up_token)
            && current_market.down_token == U256::from_be_bytes(market.down_token),
        "submitted candidate order current market token identity differs from frozen catalog"
    );
    let current_itode = current_market.itode;
    central
        .symbol_projections
        .get_mut(&symbol)
        .context("submitted candidate order has no central symbol projection")?
        .reserve_order_handle(&submitted.order_id)?;
    Ok(current_itode)
}
