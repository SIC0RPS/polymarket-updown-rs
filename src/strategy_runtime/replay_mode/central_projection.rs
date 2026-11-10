use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::ctf_inventory::CtfInventoryRuntime;
use crate::orders::{ActiveOrderLifecycle, OpenOrdersState, OrderLifecycleKey};
use crate::polymarket::symbol_ownership::SymbolOwnershipIndex;
use crate::portfolio::{PortfolioState, PositionState};
use crate::strategy_runtime::market_catalog::MarketCatalog;
use crate::strategy_runtime::symbol_process::{
    ActiveLifecycleDeltaRecord, CohortIdentityRecord, OpenOrderDeltaRecord, PositionDeltaRecord,
};

#[path = "central_projection/encoding.rs"]
mod encoding;
#[path = "central_projection/open_order.rs"]
mod open_order;
#[path = "central_projection/validation.rs"]
mod validation;

use encoding::{encode_active_lifecycle, encode_position, reserve_order_handle_in_map};
use open_order::encode_open_order;
use validation::{ensure_qty_view, position_token_handle};

const POSITION_UPSERT: u8 = 1;
const POSITION_REMOVE: u8 = 2;
const POSITION_RESOLVED_REDEEMABLE: u8 = 1;
const POSITION_MERGEABLE: u8 = 2;
const POSITION_END_TIME_PRESENT: u8 = 4;
const POSITION_TOKEN_EXPIRY_PRESENT: u8 = 8;

const OPEN_ORDER_UPSERT: u8 = 1;
const OPEN_ORDER_REMOVE: u8 = 2;
const OPEN_ORDER_ACTIVE: u8 = 1;
const OPEN_ORDER_TERMINAL: u8 = 2;
const OPEN_ORDER_POST_ONLY: u8 = 1;
const OPEN_ORDER_QUOTE_LEVEL_PRESENT: u8 = 2;

const ACTIVE_LIFECYCLE_UPSERT: u8 = 1;
const ACTIVE_LIFECYCLE_REMOVE: u8 = 2;
const ACTIVE_LIFECYCLE_ORDER_PRESENT: u8 = 1;
const ACTIVE_LIFECYCLE_EXPIRY_PRESENT: u8 = 2;
const ACTIVE_LIFECYCLE_TERMINAL_NO_FILL: u8 = 4;
const ACTIVE_LIFECYCLE_POST_ONLY: u8 = 8;

#[derive(Debug)]
pub(crate) struct CentralSymbolProjection {
    catalog: Arc<MarketCatalog>,
    ownership: SymbolOwnershipIndex,
    positions: BTreeMap<u32, PositionDeltaRecord>,
    orders: BTreeMap<String, OpenOrderDeltaRecord>,
    active_lifecycles: BTreeMap<LifecycleIdentity, ActiveLifecycleDeltaRecord>,
    order_handles: BTreeMap<String, u64>,
    next_order_handle: u64,
    last_portfolio_revision: u64,
    last_open_orders_revision: u64,
    portfolio: PortfolioState,
    open_orders_state: OpenOrdersState,
}

#[derive(Debug, Default)]
pub(crate) struct CentralSymbolViewDeltas {
    pub(crate) positions: Vec<PositionDeltaRecord>,
    pub(crate) open_orders: Vec<OpenOrderDeltaRecord>,
    pub(crate) active_lifecycles: Vec<ActiveLifecycleDeltaRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct LifecycleIdentity {
    market_id: u32,
    token_id: u32,
    side: u8,
    close_reason: u8,
}

struct ProjectedOpenOrders {
    open_order_deltas: Vec<OpenOrderDeltaRecord>,
    orders: BTreeMap<String, OpenOrderDeltaRecord>,
    lifecycle_deltas: Vec<ActiveLifecycleDeltaRecord>,
    active_lifecycles: BTreeMap<LifecycleIdentity, ActiveLifecycleDeltaRecord>,
    order_handles: BTreeMap<String, u64>,
    next_order_handle: u64,
    revision: u64,
}

impl CentralSymbolProjection {
    pub(crate) fn new(catalog: Arc<MarketCatalog>) -> Result<Self> {
        anyhow::ensure!(
            !catalog.is_empty(),
            "central symbol projection market catalog is empty"
        );
        let ownership = SymbolOwnershipIndex::default();
        ownership.claim_frozen_market_catalog(catalog.records(), catalog.symbol())?;
        Ok(Self {
            catalog,
            ownership,
            positions: BTreeMap::new(),
            orders: BTreeMap::new(),
            active_lifecycles: BTreeMap::new(),
            order_handles: BTreeMap::new(),
            next_order_handle: 1,
            last_portfolio_revision: 0,
            last_open_orders_revision: 0,
            portfolio: PortfolioState::default(),
            open_orders_state: OpenOrdersState::default(),
        })
    }

    pub(crate) fn claim_cohort(&self, cohort: &str) -> Result<()> {
        self.ownership.claim_cohort(cohort, self.catalog.symbol())
    }

    pub(crate) fn project(
        &mut self,
        portfolio: &PortfolioState,
        open_orders: &OpenOrdersState,
    ) -> Result<CentralSymbolViewDeltas> {
        anyhow::ensure!(
            portfolio.revision >= self.last_portfolio_revision,
            "symbol portfolio revision regressed current={} prior={}",
            portfolio.revision,
            self.last_portfolio_revision
        );
        anyhow::ensure!(
            open_orders.revision >= self.last_open_orders_revision,
            "symbol open-order revision regressed current={} prior={}",
            open_orders.revision,
            self.last_open_orders_revision
        );

        let positions = if portfolio.revision > self.last_portfolio_revision {
            let portfolio =
                portfolio.filtered_for_underlying(self.catalog.symbol(), &self.ownership);
            let (deltas, next_positions, revision) = self.project_positions(&portfolio)?;
            self.positions = next_positions;
            self.last_portfolio_revision = revision;
            self.portfolio = portfolio;
            deltas
        } else {
            Vec::new()
        };
        let (open_order_deltas, active_lifecycle_deltas) =
            if open_orders.revision > self.last_open_orders_revision {
                let open_orders =
                    open_orders.filtered_for_underlying(self.catalog.symbol(), &self.ownership);
                let projected_orders = self.project_open_orders(&open_orders)?;
                let deltas = projected_orders.open_order_deltas;
                let lifecycle_deltas = projected_orders.lifecycle_deltas;
                self.orders = projected_orders.orders;
                self.active_lifecycles = projected_orders.active_lifecycles;
                self.order_handles = projected_orders.order_handles;
                self.next_order_handle = projected_orders.next_order_handle;
                self.last_open_orders_revision = projected_orders.revision;
                self.open_orders_state = open_orders;
                (deltas, lifecycle_deltas)
            } else {
                (Vec::new(), Vec::new())
            };

        Ok(CentralSymbolViewDeltas {
            positions,
            open_orders: open_order_deltas,
            active_lifecycles: active_lifecycle_deltas,
        })
    }

    pub(crate) fn portfolio(&self) -> &PortfolioState {
        &self.portfolio
    }

    pub(crate) fn open_orders(&self) -> &OpenOrdersState {
        &self.open_orders_state
    }

    pub(crate) fn reserved_inventory_units_6dp(
        &self,
        runtime: &CtfInventoryRuntime,
    ) -> Result<u128> {
        runtime.unmaterialized_split_gross_inventory_units_6dp_for_conditions(
            self.catalog
                .records()
                .iter()
                .map(|record| record.condition_id),
        )
    }

    pub(crate) fn order_handle(&self, order_id: &str) -> Result<u64> {
        anyhow::ensure!(
            !order_id.is_empty() && order_id.trim() == order_id,
            "central order identity is empty or noncanonical"
        );
        self.order_handles
            .get(order_id)
            .copied()
            .context("central order identity has no stable projected handle")
    }

    pub(crate) fn reserve_order_handle(&mut self, order_id: &str) -> Result<u64> {
        reserve_order_handle_in_map(
            &mut self.order_handles,
            &mut self.next_order_handle,
            order_id,
        )
    }

    fn project_positions(
        &self,
        portfolio: &PortfolioState,
    ) -> Result<(
        Vec<PositionDeltaRecord>,
        BTreeMap<u32, PositionDeltaRecord>,
        u64,
    )> {
        anyhow::ensure!(
            portfolio.revision >= self.last_portfolio_revision,
            "symbol portfolio revision regressed current={} prior={}",
            portfolio.revision,
            self.last_portfolio_revision
        );

        let mut next = BTreeMap::new();
        for (token, position) in &portfolio.positions {
            anyhow::ensure!(
                token == &position.token,
                "symbol portfolio position key differs from position token"
            );
            let token_id = position_token_handle(&self.catalog, position.token.as_str())?;
            if position.qty_units_6dp == 0 {
                anyhow::ensure!(
                    position.sellable_qty_units_6dp == 0,
                    "zero position carries sellable quantity"
                );
                ensure_qty_view(position.qty, 0, "zero position quantity")?;
                ensure_qty_view(position.sellable_qty, 0, "zero position sellable quantity")?;
                continue;
            }
            let token_expiry_ms = portfolio.token_expiry_ms.get(token).copied();
            anyhow::ensure!(
                position.end_time_ms.is_none()
                    || token_expiry_ms.is_none()
                    || position.end_time_ms == token_expiry_ms,
                "position expiry conflicts with token expiry owner"
            );
            let record =
                encode_position(token_id, portfolio.updated_at_ms, position, token_expiry_ms)?;
            anyhow::ensure!(
                next.insert(record.token_id, record).is_none(),
                "symbol portfolio contains duplicate catalog token handle={}",
                record.token_id
            );
        }

        let changed = next != self.positions;
        if changed {
            anyhow::ensure!(
                portfolio.revision > self.last_portfolio_revision,
                "symbol portfolio changed without advancing its revision current={} prior={}",
                portfolio.revision,
                self.last_portfolio_revision
            );
        }

        let mut deltas = Vec::new();
        for (token_id, current) in &next {
            if self.positions.get(token_id) != Some(current) {
                let mut record = *current;
                record.central_revision = portfolio.revision;
                record.validate()?.validate_catalog(&self.catalog)?;
                deltas.push(record);
            }
        }
        for token_id in self.positions.keys() {
            if !next.contains_key(token_id) {
                let record = PositionDeltaRecord {
                    token_id: *token_id,
                    operation: POSITION_REMOVE,
                    central_revision: portfolio.revision,
                    ..PositionDeltaRecord::default()
                };
                record.validate()?.validate_catalog(&self.catalog)?;
                deltas.push(record);
            }
        }
        deltas.sort_unstable_by_key(|record| record.token_id);
        Ok((deltas, next, portfolio.revision))
    }

    fn project_open_orders(&self, open_orders: &OpenOrdersState) -> Result<ProjectedOpenOrders> {
        anyhow::ensure!(
            open_orders.revision >= self.last_open_orders_revision,
            "symbol open-order revision regressed current={} prior={}",
            open_orders.revision,
            self.last_open_orders_revision
        );

        let mut order_handles = self.order_handles.clone();
        let mut next_order_handle = self.next_order_handle;
        let mut referenced_order_ids = open_orders
            .orders
            .keys()
            .collect::<std::collections::BTreeSet<_>>();
        referenced_order_ids.extend(
            open_orders
                .active_order_lifecycles
                .values()
                .filter_map(|active| active.order_id.as_ref()),
        );
        for order_id in referenced_order_ids {
            reserve_order_handle_in_map(
                &mut order_handles,
                &mut next_order_handle,
                order_id.as_str(),
            )?;
        }

        let mut next = BTreeMap::new();
        for (order_id, order) in &open_orders.orders {
            anyhow::ensure!(
                order_id == &order.order_id,
                "symbol open-order key differs from order identity"
            );
            let order_handle = *order_handles
                .get(order_id)
                .context("open-order handle assignment is missing")?;
            let record = encode_open_order(
                &self.catalog,
                open_orders.updated_at_ms,
                order_handle,
                order,
            )?;
            next.insert(order_id.clone(), record);
        }

        let mut next_lifecycles = BTreeMap::new();
        for (key, active) in &open_orders.active_order_lifecycles {
            let (identity, record) =
                encode_active_lifecycle(&self.catalog, &order_handles, key, active)?;
            anyhow::ensure!(
                next_lifecycles.insert(identity, record).is_none(),
                "symbol open orders contain duplicate active lifecycle identity"
            );
        }

        let changed = next != self.orders || next_lifecycles != self.active_lifecycles;
        if changed {
            anyhow::ensure!(
                open_orders.revision > self.last_open_orders_revision,
                "symbol open orders changed without advancing their revision current={} prior={}",
                open_orders.revision,
                self.last_open_orders_revision
            );
        }

        let mut deltas = Vec::new();
        for (order_id, current) in &next {
            if self.orders.get(order_id) != Some(current) {
                let mut record = *current;
                record.central_revision = open_orders.revision;
                record.validate()?.validate_catalog(&self.catalog)?;
                deltas.push(record);
            }
        }
        for (order_id, previous) in &self.orders {
            if !next.contains_key(order_id) {
                let mut record = *previous;
                record.operation = OPEN_ORDER_REMOVE;
                record.status = OPEN_ORDER_TERMINAL;
                record.central_revision = open_orders.revision;
                record.validate()?.validate_catalog(&self.catalog)?;
                deltas.push(record);
            }
        }
        deltas.sort_unstable_by_key(|record| record.order_handle);

        let mut lifecycle_deltas = Vec::new();
        for (identity, current) in &next_lifecycles {
            if self.active_lifecycles.get(identity) != Some(current) {
                let mut record = *current;
                record.central_revision = open_orders.revision;
                record.validate()?.validate_catalog(&self.catalog)?;
                lifecycle_deltas.push(record);
            }
        }
        for (identity, previous) in &self.active_lifecycles {
            if !next_lifecycles.contains_key(identity) {
                let record = ActiveLifecycleDeltaRecord {
                    operation: ACTIVE_LIFECYCLE_REMOVE,
                    side: previous.side,
                    close_reason: previous.close_reason,
                    market_id: previous.market_id,
                    token_id: previous.token_id,
                    cohort_id: previous.cohort_id,
                    central_revision: open_orders.revision,
                    ..ActiveLifecycleDeltaRecord::default()
                };
                record.validate()?.validate_catalog(&self.catalog)?;
                lifecycle_deltas.push(record);
            }
        }
        lifecycle_deltas.sort_unstable_by_key(|record| {
            (
                record.market_id,
                record.token_id,
                record.side,
                record.close_reason,
            )
        });
        Ok(ProjectedOpenOrders {
            open_order_deltas: deltas,
            orders: next,
            lifecycle_deltas,
            active_lifecycles: next_lifecycles,
            order_handles,
            next_order_handle,
            revision: open_orders.revision,
        })
    }
}
