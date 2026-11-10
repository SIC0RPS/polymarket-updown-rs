use super::*;

#[derive(Clone)]
pub(in crate::execution::supervisor) struct CancelTarget {
    pub order_id: String,
    pub scope: ScopedOrderFenceKey,
}

pub(in crate::execution::supervisor) struct PendingReplacement {
    pub decision_ts_ms: u64,
    pub decision_instant: Option<Instant>,
    pub handoff_instant: Option<Instant>,
    pub targets: Vec<CancelTarget>,
    pub intents: Vec<OrderIntent>,
}

pub(in crate::execution::supervisor) enum CancellationCompletion {
    Orders {
        targets: Vec<CancelTarget>,
        label: &'static str,
        terminal_status: &'static str,
        outcome: Result<CancelAck>,
    },
    Market {
        request: CancelMarketRequest,
        attempt: usize,
        targets: Vec<CancelTarget>,
        cancel_outcome: Result<CancelAck>,
        verify_outcome: Result<Vec<String>>,
    },
}

enum MarketCancellation {
    Pending {
        request: CancelMarketRequest,
        attempt: usize,
        delay: bool,
    },
    Active,
    Unresolved,
}

struct PendingScopeHealing {
    request: ScopedCancelHealRequest,
    targets: HashSet<String>,
    dispatched: bool,
}

pub(in crate::execution::supervisor) struct ActiveCancellations {
    futures: FuturesUnordered<BoxFuture<'static, CancellationCompletion>>,
    targets: HashMap<String, ScopedOrderFenceKey>,
    markets: HashMap<String, MarketCancellation>,
    replacements: Vec<PendingReplacement>,
    heals: HashMap<ScopedOrderFenceKey, PendingScopeHealing>,
}

impl ActiveCancellations {
    pub fn new() -> Self {
        Self {
            futures: FuturesUnordered::new(),
            targets: HashMap::new(),
            markets: HashMap::new(),
            replacements: Vec::new(),
            heals: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.futures.is_empty()
            && self.targets.is_empty()
            && self
                .markets
                .values()
                .all(|market| matches!(market, MarketCancellation::Unresolved))
            && self.replacements.is_empty()
    }

    pub fn has_unresolved_markets(&self) -> bool {
        self.markets
            .values()
            .any(|market| matches!(market, MarketCancellation::Unresolved))
    }

    fn market_pending(&self, market: &str) -> bool {
        self.markets
            .get(market)
            .is_some_and(|market| !matches!(market, MarketCancellation::Unresolved))
    }

    pub fn has_network_work(&self) -> bool {
        !self.futures.is_empty()
    }

    pub fn blocks_scope(&self, scope: &ScopedOrderFenceKey) -> bool {
        self.markets.contains_key(&scope.market) || self.heals.contains_key(scope)
    }

    pub fn targets_for_ids(open_orders: &OpenOrdersState, ids: &[String]) -> Vec<CancelTarget> {
        ids.iter()
            .filter_map(|id| {
                open_orders.orders.get(id).map(|order| CancelTarget {
                    order_id: id.clone(),
                    scope: ScopedOrderFenceKey {
                        market: order.market.clone(),
                        token: order.token.clone(),
                    },
                })
            })
            .collect()
    }

    pub fn schedule_orders(
        &mut self,
        exec: &ExecutionClient,
        targets: Vec<CancelTarget>,
        label: &'static str,
        terminal_status: &'static str,
    ) {
        let targets: Vec<_> = targets
            .into_iter()
            .filter(|target| {
                !self.targets.contains_key(&target.order_id)
                    && !self.market_pending(&target.scope.market)
                    && !self.heals.contains_key(&target.scope)
            })
            .collect();
        if targets.is_empty() {
            return;
        }
        let ids: Vec<_> = targets
            .iter()
            .map(|target| target.order_id.clone())
            .collect();
        for target in &targets {
            self.targets
                .insert(target.order_id.clone(), target.scope.clone());
        }
        let send_exec = exec.clone();
        self.futures.push(
            async move {
                let outcome = send_exec.cancel_orders(&ids).await;
                CancellationCompletion::Orders {
                    targets,
                    label,
                    terminal_status,
                    outcome,
                }
            }
            .boxed(),
        );
    }

    pub fn finish_order_targets(&mut self, targets: &[CancelTarget]) {
        for target in targets {
            self.targets.remove(&target.order_id);
        }
    }

    pub fn retain_replacement(&mut self, replacement: PendingReplacement) {
        if !replacement.intents.is_empty() {
            self.replacements.push(replacement);
        }
    }

    pub fn take_ready_replacement(&mut self) -> Option<PendingReplacement> {
        let ready = self.replacements.iter().position(|replacement| {
            replacement.targets.iter().all(|target| {
                !self.targets.contains_key(&target.order_id)
                    && !self.market_pending(&target.scope.market)
                    && !self.heals.contains_key(&target.scope)
            })
        })?;
        Some(self.replacements.remove(ready))
    }

    pub fn schedule_market(&mut self, request: CancelMarketRequest) {
        let market = request.market_id.to_string();
        let pending = MarketCancellation::Pending {
            request,
            attempt: 1,
            delay: false,
        };
        match self.markets.entry(market) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(pending);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if matches!(entry.get(), MarketCancellation::Unresolved) {
                    entry.insert(pending);
                }
            }
        }
    }

    pub fn retry_market(&mut self, request: CancelMarketRequest, attempt: usize) {
        self.markets.insert(
            request.market_id.to_string(),
            MarketCancellation::Pending {
                request,
                attempt,
                delay: true,
            },
        );
    }

    pub fn finish_market(&mut self, market_id: &str, verified: bool) {
        if verified {
            self.markets.remove(market_id);
        } else {
            self.markets
                .insert(market_id.to_string(), MarketCancellation::Unresolved);
        }
    }

    pub fn dispatch_ready_markets(
        &mut self,
        exec: &ExecutionClient,
        open_orders: &OpenOrdersState,
        active_submissions: &ActiveSubmissions,
    ) -> bool {
        let mut dispatched = false;
        for (market_id, pending) in &mut self.markets {
            if !matches!(pending, MarketCancellation::Pending { .. })
                || active_submissions.has_pending_market(market_id)
                || self
                    .targets
                    .values()
                    .any(|scope| scope.market == *market_id)
                || self.heals.keys().any(|scope| scope.market == *market_id)
            {
                continue;
            }
            let MarketCancellation::Pending {
                request,
                attempt,
                delay,
            } = std::mem::replace(pending, MarketCancellation::Active)
            else {
                unreachable!("ready market cancellation is retained");
            };
            let targets = open_orders
                .orders
                .values()
                .filter(|order| order.market == *market_id)
                .map(|order| CancelTarget {
                    order_id: order.order_id.clone(),
                    scope: ScopedOrderFenceKey {
                        market: order.market.clone(),
                        token: order.token.clone(),
                    },
                })
                .collect();
            let send_exec = exec.clone();
            self.futures.push(
                async move {
                    if delay {
                        tokio::time::sleep(Duration::from_millis(
                            INVENTORY_MARKET_CLEAN_VERIFY_DELAY_MS,
                        ))
                        .await;
                    }
                    let cancel_outcome = send_exec
                        .cancel_market_orders(Some(request.market_id.as_str()), None)
                        .await;
                    let verify_outcome = send_exec
                        .open_order_ids_in_scope(Some(request.market_id.as_str()), None)
                        .await;
                    CancellationCompletion::Market {
                        request,
                        attempt,
                        targets,
                        cancel_outcome,
                        verify_outcome,
                    }
                }
                .boxed(),
            );
            dispatched = true;
        }
        dispatched
    }

    pub fn retain_scope_heals(
        &mut self,
        targets: &[CancelTarget],
        label: &'static str,
        fenced_scopes: &mut HashSet<ScopedOrderFenceKey>,
        healing_scopes: &mut HashSet<ScopedOrderFenceKey>,
    ) {
        for target in targets {
            fenced_scopes.insert(target.scope.clone());
            let pending = self.heals.entry(target.scope.clone()).or_insert_with(|| {
                healing_scopes.insert(target.scope.clone());
                PendingScopeHealing {
                    request: ScopedCancelHealRequest {
                        scope: target.scope.clone(),
                        order_id: target.order_id.clone(),
                        label,
                    },
                    targets: HashSet::new(),
                    dispatched: false,
                }
            });
            pending.targets.insert(target.order_id.clone());
        }
    }

    pub fn dispatch_ready_heals(
        &mut self,
        open_orders: &OpenOrdersState,
        active_submissions: &ActiveSubmissions,
        scoped_heal_tx: &mpsc::UnboundedSender<ScopedCancelHealRequest>,
    ) -> Result<bool> {
        let mut dispatched = false;
        for (scope, pending) in &mut self.heals {
            if pending.dispatched
                || active_submissions.has_pending_scope(scope)
                || self.targets.values().any(|target| target == scope)
                || self
                    .markets
                    .get(&scope.market)
                    .is_some_and(|market| matches!(market, MarketCancellation::Active))
            {
                continue;
            }
            pending.targets.extend(
                open_orders
                    .orders
                    .values()
                    .filter(|order| order.market == scope.market && order.token == scope.token)
                    .map(|order| order.order_id.clone()),
            );
            scoped_heal_tx
                .send(pending.request.clone())
                .map_err(|_| anyhow::Error::new(ScopedHealQueueClosed))?;
            pending.dispatched = true;
            dispatched = true;
        }
        Ok(dispatched)
    }

    pub fn clear_unavailable_heals(&mut self) -> bool {
        let had_pending = !self.heals.is_empty();
        self.heals.clear();
        had_pending
    }

    pub fn take_heal_targets(&mut self, scope: &ScopedOrderFenceKey) -> Option<HashSet<String>> {
        self.heals.remove(scope).map(|pending| pending.targets)
    }

    pub async fn next_completion(&mut self) -> Option<CancellationCompletion> {
        self.futures.next().await
    }
}
