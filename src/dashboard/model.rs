use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Serialize;

use super::DashboardState;
use crate::orders::OpenOrderSnapshot;
use crate::trade_session_ledger::CanonicalSessionStats;

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct OperatorSnapshot {
    pub schema_version: u32,
    pub sequence: u64,
    pub generated_at_ms: u64,
    pub runtime: RuntimeHealthSnapshot,
    pub account: AccountSnapshot,
    pub open_orders: Vec<OpenOrderSnapshot>,
    pub positions: Vec<PositionSnapshot>,
    pub redeemables: Vec<RedeemableSnapshot>,
    pub redeem_conditions: Vec<RedeemConditionSnapshot>,
    pub canonical_stats: CanonicalSessionStats,
    pub closed_realized_pnl_by_symbol:
        BTreeMap<String, crate::trade_session_ledger::ExactRealizedPnl>,
    pub exit_type_pnl: Vec<(String, crate::report_markdown::SymbolExitAgg)>,
    pub strategy: Option<StrategySnapshot>,
    pub symbols: Vec<SymbolOracleSnapshot>,
    pub chart: Vec<PricePointSnapshot>,
    pub raw_log_tail: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct RuntimeHealthSnapshot {
    pub mode: String,
    pub started_at_ms: u64,
    pub execution_backend: String,
    pub market_data_source: String,
    pub pause: bool,
    pub trading_allowed: Option<bool>,
    pub heartbeat_active: bool,
    pub last_error: Option<RuntimeErrorSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct RuntimeErrorSnapshot {
    pub ts_ms: u64,
    pub scope: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct AccountSnapshot {
    pub wallet_address: String,
    pub wallet_kind: String,
    pub updated_at_ms: Option<u64>,
    pub wallet_observed_at_ms: Option<u64>,
    pub wallet_usdc_balance: Option<f64>,
    pub trading_collateral_balance: Option<f64>,
    pub wallet_matic_balance: Option<f64>,
    pub exchange_allowance: Option<f64>,
    pub current_wallet_equity: Option<f64>,
    pub reconciliation_at_ms: Option<u64>,
    pub marked_remaining_value_units_6dp: Option<String>,
    pub receipt_realized_pnl_units_6dp: Option<String>,
    pub open_pnl_units_6dp: Option<String>,
    pub total_pnl_units_6dp: Option<String>,
    pub market_marked_unrealized_pnl_units_6dp: Option<String>,
    pub market_marked_total_pnl_units_6dp: Option<String>,
    pub valuation_portfolio_revision: Option<u64>,
    pub valuation_canonical_revision: Option<u64>,
    pub valuation_odds_revision: Option<u64>,
    pub valued_at_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct PositionSnapshot {
    pub token: String,
    pub market_id: Option<String>,
    pub display_label: String,
    pub quantity_units_6dp: String,
    pub sellable_quantity_units_6dp: String,
    pub average_price: f64,
    pub cost_basis: f64,
    pub mark_price: Option<f64>,
    pub unrealized_pnl_units_6dp: Option<String>,
    pub remaining_basis_units_6dp: Option<String>,
    pub mark_updated_at_ms: Option<u64>,
    pub opened_at_ms: Option<u64>,
    pub updated_at_ms: u64,
    pub end_time_ms: Option<u64>,
    pub redeemable: bool,
    pub mergeable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RedeemableSnapshot {
    pub condition_id: String,
    pub token: String,
    pub title: String,
    pub outcome: String,
    pub outcome_index: u8,
    pub quantity_units_6dp: String,
    pub payout_numerator: String,
    pub payout_denominator: String,
    pub claim_value_units_6dp: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RedeemConditionSnapshot {
    pub condition_id: String,
    pub route: String,
    pub status: String,
    pub attempts: u32,
    pub first_seen_ms: u64,
    pub last_update_ms: u64,
    pub tx_hash: Option<String>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct StrategySnapshot {
    pub ts_ms: u64,
    pub kind: String,
    pub status: String,
    pub p_up: Option<f64>,
    pub edge: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct SymbolOracleSnapshot {
    pub symbol: String,
    pub ts_ms: u64,
    pub price: f64,
    pub volatility: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct PricePointSnapshot {
    pub symbol: String,
    pub ts_ms: u64,
    pub price: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub(crate) struct OperatorStreamEvent {
    pub kind: &'static str,
    pub snapshot: OperatorSnapshot,
}

pub(crate) async fn capture_operator_snapshot(
    state: &DashboardState,
    include_chart_history: bool,
) -> Result<OperatorSnapshot> {
    let runtime_config = state.runtime_config_state.snapshot().await;
    let ui_state = state.observability.ui_state();
    let ui = {
        let ui = ui_state.borrow();
        if include_chart_history {
            ui.clone()
        } else {
            ui.dashboard_snapshot_clone()
        }
    };
    let trading_allowed = state
        .risk
        .as_ref()
        .map(|handle| handle.current_snapshot().trading_allowed);
    let mut stats = state
        .canonical_session_report
        .borrow()
        .as_ref()
        .context("canonical in-memory session stats are unavailable")?
        .stats
        .clone();
    let market_display_name = |condition_id| {
        ui.market_names.get(condition_id).cloned().or_else(|| {
            ui.market_snapshots
                .get(condition_id)
                .and_then(|market| market.question.clone().or_else(|| market.slug.clone()))
        })
    };
    for cohort in &mut stats.closed_cohorts {
        if cohort.market_name.is_none() {
            cohort.market_name = cohort.market_id.as_ref().and_then(&market_display_name);
        }
    }

    let mut open_orders = ui
        .open_orders
        .orders
        .values()
        .map(OpenOrderSnapshot::from)
        .collect::<Vec<_>>();
    open_orders.sort_by(|left, right| left.order_id.cmp(&right.order_id));

    let mut positions = ui
        .portfolio
        .positions
        .values()
        .map(|position| {
            let market_id = ui.portfolio.token_market_hint.get(&position.token);
            let display_label = market_id
                .map(|condition_id| {
                    let market_name = market_display_name(condition_id)
                        .unwrap_or_else(|| condition_id.to_string());
                    match ui.market_snapshots.get(condition_id) {
                        Some(market) if market.up_token == position.token => {
                            format!("UP | {market_name}")
                        }
                        Some(market) if market.down_token == position.token => {
                            format!("DOWN | {market_name}")
                        }
                        _ => market_name,
                    }
                })
                .unwrap_or_else(|| position.token.clone());
            PositionSnapshot {
                token: position.token.clone(),
                market_id: market_id.map(ToString::to_string),
                display_label,
                quantity_units_6dp: position.qty_units_6dp.to_string(),
                sellable_quantity_units_6dp: position.sellable_qty_units_6dp.to_string(),
                average_price: position.avg_px,
                cost_basis: position.cost_basis,
                mark_price: position.mark_price,
                unrealized_pnl_units_6dp: position
                    .unrealized_pnl_units_6dp
                    .map(|units| units.to_string()),
                remaining_basis_units_6dp: position
                    .remaining_basis_units_6dp
                    .map(|units| units.to_string()),
                mark_updated_at_ms: position.mark_updated_at_ms,
                opened_at_ms: position.opened_at_ms,
                updated_at_ms: position.last_update_ms,
                end_time_ms: position.end_time_ms,
                redeemable: position.resolved_redeemable,
                mergeable: position.mergeable,
            }
        })
        .collect::<Vec<_>>();
    positions.sort_by(|left, right| left.token.cmp(&right.token));
    let redeem_ops = state.execution.live_ops().snapshot().redeem;
    let redeem_conditions = redeem_ops
        .conditions
        .into_iter()
        .map(|condition| RedeemConditionSnapshot {
            condition_id: condition.condition_id.to_string(),
            route: condition.route,
            status: condition.status,
            attempts: condition.attempts,
            first_seen_ms: condition.first_seen_ms,
            last_update_ms: condition.last_update_ms,
            tx_hash: condition.tx_hash,
            error: condition.error,
        })
        .collect();
    let redeemables = redeem_ops
        .redeemables
        .into_iter()
        .map(|claim| RedeemableSnapshot {
            condition_id: claim.condition_id.to_string(),
            token: claim.token,
            title: claim.title,
            outcome: claim.outcome,
            outcome_index: claim.outcome_index,
            quantity_units_6dp: claim.quantity_units_6dp.to_string(),
            payout_numerator: claim.payout_numerator.to_string(),
            payout_denominator: claim.payout_denominator.to_string(),
            claim_value_units_6dp: claim.claim_value_units_6dp.to_string(),
        })
        .collect::<Vec<_>>();

    let symbols = ui
        .oracle_quotes
        .values()
        .map(|oracle| {
            let symbol = oracle.symbol.clone();
            (
                symbol.clone(),
                SymbolOracleSnapshot {
                    symbol,
                    ts_ms: oracle.ts_ms,
                    price: oracle.px,
                    volatility: oracle.vol,
                },
            )
        })
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect();
    let mut chart = if include_chart_history {
        ui.oracle_tick_history
            .iter()
            .flat_map(|(symbol, points)| {
                points.iter().map(|point| PricePointSnapshot {
                    symbol: symbol.clone(),
                    ts_ms: point.ts_ms,
                    price: point.px,
                })
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    chart.sort_by(|left, right| {
        left.ts_ms
            .cmp(&right.ts_ms)
            .then_with(|| left.symbol.cmp(&right.symbol))
    });

    Ok(OperatorSnapshot {
        schema_version: 9,
        sequence: state.next_sequence(),
        generated_at_ms: now_ms(),
        runtime: RuntimeHealthSnapshot {
            mode: ui.mode,
            started_at_ms: ui.started_at_ms,
            execution_backend: ui.execution_backend,
            market_data_source: ui.market_data_source,
            pause: runtime_config.pause,
            trading_allowed,
            heartbeat_active: state.execution.heartbeats_active(),
            last_error: ui.last_error.map(|error| RuntimeErrorSnapshot {
                ts_ms: error.ts_ms,
                scope: error.scope,
                message: error.msg,
            }),
        },
        account: AccountSnapshot {
            wallet_address: state.execution.polymarket_address().to_string(),
            wallet_kind: state.execution.polymarket_wallet_kind().to_string(),
            updated_at_ms: ui.risk.account_equity_updated_at_ms,
            wallet_observed_at_ms: ui.wallet_observed_at_ms,
            wallet_usdc_balance: ui.risk.wallet_usdc_balance,
            trading_collateral_balance: ui.risk.trading_collateral_balance,
            wallet_matic_balance: ui.risk.wallet_matic_balance,
            exchange_allowance: ui.risk.exchange_allowance,
            current_wallet_equity: ui.risk.current_wallet_equity,
            reconciliation_at_ms: ui.risk.last_recon_at_ms,
            marked_remaining_value_units_6dp: ui
                .portfolio
                .marked_remaining_value_units_6dp
                .map(|units| units.to_string()),
            receipt_realized_pnl_units_6dp: ui
                .portfolio
                .receipt_realized_pnl_units_6dp
                .map(|units| units.to_string()),
            open_pnl_units_6dp: ui
                .portfolio
                .total_unrealized_pnl_units_6dp
                .map(|units| units.to_string()),
            total_pnl_units_6dp: ui
                .portfolio
                .total_pnl_units_6dp
                .map(|units| units.to_string()),
            market_marked_unrealized_pnl_units_6dp: ui
                .portfolio
                .market_marked_unrealized_pnl_units_6dp
                .map(|units| units.to_string()),
            market_marked_total_pnl_units_6dp: ui
                .portfolio
                .market_marked_total_pnl_units_6dp
                .map(|units| units.to_string()),
            valuation_portfolio_revision: ui.portfolio.valued_at_ms.map(|_| ui.portfolio.revision),
            valuation_canonical_revision: ui.portfolio.valuation_canonical_revision,
            valuation_odds_revision: ui.portfolio.valuation_odds_revision,
            valued_at_ms: ui.portfolio.valued_at_ms,
        },
        open_orders,
        positions,
        redeemables,
        redeem_conditions,
        closed_realized_pnl_by_symbol: stats.closed_realized_pnl_by_symbol(),
        exit_type_pnl: crate::report_markdown::aggregate_exit_type_pnl(
            &stats.canonical.ctf_exit_summary,
            &stats.canonical.ctf_exit_cohorts,
            Some(&stats),
        ),
        canonical_stats: stats,
        strategy: ui.last_strategy_status.map(|strategy| StrategySnapshot {
            ts_ms: strategy.ts_ms,
            kind: strategy.kind,
            status: strategy.status,
            p_up: strategy.p_up,
            edge: strategy.edge,
        }),
        symbols,
        chart,
        raw_log_tail: None,
    })
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
