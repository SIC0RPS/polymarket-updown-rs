use super::*;
use std::sync::Mutex;

pub(super) struct CandidateCentralState {
    pub(super) portfolio: PortfolioState,
    pub(super) portfolio_tx: watch::Sender<PortfolioState>,
    pub(super) odds: Arc<PolymarketOddsState>,
    pub(super) odds_tx: watch::Sender<Arc<PolymarketOddsState>>,
    pub(super) projection: MarketProjection,
    pub(super) projection_tx: watch::Sender<MarketProjection>,
    pub(super) risk: RiskHandle,
    pub(super) order_fill_context: OrderFillContextRegistry,
    pub(super) execution: ExecutionClient,
    pub(super) execution_sim: ExecutionSimHandle,
    pub(super) supervisor: Option<CapturedExecutionSupervisorRuntime>,
    pub(super) exchange: ExchangeSim,
    pub(super) catalogs: BTreeMap<String, Arc<MarketCatalog>>,
    pub(super) symbol_projections: BTreeMap<String, CentralSymbolProjection>,
    pub(super) max_inventory_total_by_symbol: BTreeMap<String, f64>,
    pub(super) central_sequences: BTreeMap<String, u64>,
    pub(super) worker_source_sequences: BTreeMap<String, u64>,
    pub(super) admitted: BTreeSet<String>,
    pub(super) sent_managed_receipts: BTreeSet<String>,
    pub(super) next_fill_handle: u64,
    pub(super) command_owner: CentralWorkerCommandOwner,
    pub(super) next_inventory_receipt_handle: u64,
    pub(super) next_inventory_transaction_handle: u64,
    pub(super) next_supervisor_sequence: u64,
    pub(super) next_audit_sequence: u64,
    pub(super) pending_submission_identities:
        BTreeMap<String, crate::engine_trade_journal::EngineTradeJournalRecord>,
    pub(super) ctf_inventory: Arc<Mutex<CtfInventoryRuntime>>,
    pub(super) clob_submit_clock: CapturedClobSubmitClock,
    pub(super) ctf_chain: CapturedCtfChainClock,
    pub(super) terminal_settlements: CandidateTerminalSettlementOwner,
    pub(super) accounting: LocalAccountingOwner,
    pub(super) audit: Option<CanonicalAuditLedger>,
}

impl CandidateCentralState {
    pub(super) async fn start(
        cfg: &AppConfig,
        candidate: &BacktestMatrixCandidate,
        inputs: &CandidateRunnerInputs<'_>,
        schedule: &super::super::MergedEventSchedule,
        baseline: &CapturedCentralBaseline,
        clob_submit_model: &CapturedClobSubmitModel,
        ctf_chain_model: &CapturedCtfChainModel,
        obs: Observability,
    ) -> Result<Self> {
        let portfolio = PortfolioState::default();
        let (portfolio_tx, portfolio_rx) = watch::channel(portfolio.clone());
        let odds = Arc::new(PolymarketOddsState::default());
        let (odds_tx, odds_rx) = watch::channel(Arc::clone(&odds));
        let projection = MarketProjection::from_odds(odds.as_ref());
        let (projection_tx, projection_rx) = watch::channel(projection.clone());
        let (blocked_markets_tx, _) = watch::channel(HashSet::new());
        let (risk, _) =
            RiskHandle::from_captured_snapshot(cfg.risk.clone(), baseline.risk_snapshot())?;
        let order_fill_context = OrderFillContextRegistry::default();
        let (execution, execution_sim) = ExecutionClient::simulated(true);
        let mut candidate_cfg = cfg.clone();
        candidate_cfg.strategy.maker_rebate_mm =
            candidate.strategy_config_for_symbols(inputs.symbols)?;
        let max_inventory_total_by_symbol = inputs
            .symbols
            .iter()
            .map(|symbol| {
                (
                    symbol.to_ascii_uppercase(),
                    candidate_cfg
                        .strategy
                        .maker_rebate_mm
                        .config_for_symbol(symbol)
                        .max_inventory_total,
                )
            })
            .collect();
        if let [symbol] = inputs.symbols {
            let mut central_symbol_config = candidate_cfg
                .strategy
                .maker_rebate_mm
                .config_for_symbol(symbol)
                .clone();
            central_symbol_config.optimizer_symbol_configs.clear();
            central_symbol_config.enabled_assets.clear();
            candidate_cfg.strategy.maker_rebate_mm = central_symbol_config;
        }
        let shared_strategy_cfg = watch::channel(candidate_cfg.strategy.maker_rebate_mm.clone()).0;
        let catalogs = inputs
            .symbols
            .iter()
            .map(|symbol| {
                let tape = tape_for_symbol(inputs.tapes, symbol)?;
                Ok((symbol.to_ascii_uppercase(), tape.market_catalog_arc()))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let symbol_projections = catalogs
            .iter()
            .map(|(symbol, catalog)| {
                Ok((
                    symbol.clone(),
                    CentralSymbolProjection::new(catalog.clone())?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let central_sequences = inputs
            .symbols
            .iter()
            .map(|symbol| (symbol.to_ascii_uppercase(), 0))
            .collect();
        let worker_source_sequences = inputs
            .symbols
            .iter()
            .map(|symbol| {
                let symbol = SymbolId::parse(symbol)?;
                Ok((
                    symbol.as_str().to_string(),
                    schedule.initial_sequence(symbol),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let command_owner = CentralWorkerCommandOwner::new(1)?;
        let audit = CanonicalAuditLedger::new(candidate.id.clone(), inputs.symbols)?;
        let mut accounting = LocalAccountingOwner::new(&candidate.id, inputs.symbols)?;
        accounting.reconcile(
            ReceiptBlockIdentity {
                number: 0,
                hash: None,
            },
            0,
            crate::ctf_accounting::qty_to_units_6dp(baseline.equity_baseline_usdt())?,
            baseline.session_ts_ms(),
        )?;
        let ctf_inventory = Arc::new(Mutex::new(CtfInventoryRuntime::default()));
        let supervisor =
            CapturedExecutionSupervisorRuntime::start(CapturedExecutionSupervisorStart {
                cfg: candidate_cfg,
                exec: execution.clone(),
                risk: risk.clone(),
                ctf_inventory_runtime: Arc::clone(&ctf_inventory),
                shared_strategy_cfg,
                order_fill_ctx: order_fill_context.clone(),
                blocked_markets_tx,
                portfolio_rx,
                odds_rx: Some(odds_rx),
                oracle_runtime_rx: None,
                market_projection_rx: Some(projection_rx),
                obs,
                capture: MaybeCapture::default(),
                shutdown: CancellationToken::new(),
                initial_pause: false,
                initial_timestamp_ms: baseline.session_ts_ms(),
            });
        Ok(Self {
            portfolio,
            portfolio_tx,
            odds,
            odds_tx,
            projection,
            projection_tx,
            risk,
            order_fill_context,
            execution,
            execution_sim,
            supervisor: Some(supervisor),
            exchange: ExchangeSim::new(),
            catalogs,
            symbol_projections,
            max_inventory_total_by_symbol,
            central_sequences,
            worker_source_sequences,
            admitted: BTreeSet::new(),
            sent_managed_receipts: BTreeSet::new(),
            next_fill_handle: 1,
            command_owner,
            next_inventory_receipt_handle: 1,
            next_inventory_transaction_handle: 1,
            next_supervisor_sequence: 1,
            next_audit_sequence: 1,
            pending_submission_identities: BTreeMap::new(),
            ctf_inventory,
            clob_submit_clock: clob_submit_model.clock(),
            ctf_chain: ctf_chain_model.clock(),
            terminal_settlements: CandidateTerminalSettlementOwner::default(),
            accounting,
            audit: Some(audit),
        })
    }

    pub(super) fn supervisor(&self) -> Result<&CapturedExecutionSupervisorRuntime> {
        self.supervisor
            .as_ref()
            .context("candidate central supervisor is unavailable")
    }

    pub(super) fn install_market_state(
        &mut self,
        odds: Arc<PolymarketOddsState>,
        projection: Option<&MarketProjection>,
    ) -> Result<()> {
        self.odds = odds;
        if let Some(projection) = projection {
            self.projection = projection.clone();
            self.projection_tx.send_replace(self.projection.clone());
        }
        self.odds_tx.send_replace(Arc::clone(&self.odds));
        Ok(())
    }

    pub(super) fn ingest_audit_record(
        &mut self,
        record: &crate::engine_trade_journal::EngineTradeJournalRecord,
    ) -> Result<()> {
        let sequence = self.next_audit_sequence;
        let next_sequence = sequence
            .checked_add(1)
            .context("candidate audit sequence overflow")?;
        let audit = self
            .audit
            .as_mut()
            .context("candidate audit ledger is unavailable")?;
        let accounting = &mut self.accounting;
        audit.ingest(sequence, record, |record| {
            anyhow::ensure!(
                accounting.admit_record(record.clone())? == FinancialAdmissionDisposition::Admitted,
                "candidate audit record was definitively rejected"
            );
            Ok(())
        })?;
        if let Some(crate::trade_session_ledger::CanonicalCtfEvent::OrderAccepted(fact)) =
            record.ctf_event.as_ref()
        {
            self.order_fill_context.mark_audit_complete(&fact.order_id);
        }
        self.next_audit_sequence = next_sequence;
        Ok(())
    }

    pub(super) fn ingest_oracle_label(
        &mut self,
        label: crate::oracle_resolution::OracleResolvedLabel,
    ) -> Result<()> {
        let sequence = self.next_audit_sequence;
        let next_sequence = sequence
            .checked_add(1)
            .context("candidate audit sequence overflow")?;
        let audit = self
            .audit
            .as_mut()
            .context("candidate audit ledger is unavailable")?;
        let accounting = &mut self.accounting;
        audit.ingest_oracle_resolved_label(sequence, label, |record| {
            anyhow::ensure!(
                accounting.admit_record(record.clone())? == FinancialAdmissionDisposition::Admitted,
                "candidate oracle record was definitively rejected"
            );
            Ok(())
        })?;
        self.next_audit_sequence = next_sequence;
        Ok(())
    }

    pub(super) fn accept_worker_output(
        &mut self,
        output: WorkerCommandBatch,
    ) -> Result<Vec<DecodedIntent>> {
        let mut oracle_labels = Vec::new();
        let mut owned_leg_outcomes = Vec::new();
        let intents = self.command_owner.accept_and_retain(
            output,
            &mut owned_leg_outcomes,
            &mut oracle_labels,
        )?;
        let audit = self
            .audit
            .as_mut()
            .context("candidate audit ledger is unavailable")?;
        let accounting = &mut self.accounting;
        let result = audit.ingest_owned_leg_outcomes(owned_leg_outcomes, |record| {
            anyhow::ensure!(
                accounting.admit_record(record.clone())? == FinancialAdmissionDisposition::Admitted,
                "candidate owned-leg record was definitively rejected"
            );
            Ok(())
        });
        self.next_audit_sequence = audit.next_source_sequence()?;
        result?;
        for label in oracle_labels {
            self.ingest_oracle_label(label)?;
        }
        Ok(intents)
    }

    pub(super) fn ingest_settlement_receipt(
        &mut self,
        receipt: crate::trade_session_ledger::SettlementCollateralReceipt,
    ) -> Result<()> {
        let audit = self
            .audit
            .as_mut()
            .context("candidate audit ledger is unavailable")?;
        let accounting = &mut self.accounting;
        audit.ingest_settlement_receipt(receipt, |receipt| {
            accounting
                .admit_receipt(SessionReceipt::SettlementCollateral(receipt.clone()))
                .map(|_| ())
        })?;
        Ok(())
    }

    pub(super) fn refresh_account_equity(&self, timestamp_ms: u64) -> Result<()> {
        let accounting = self.accounting.current();
        let projection = self.portfolio.canonical_accounting_projection(
            Some(accounting.financial.financial_view()),
            Some(accounting.confirmed_portfolio_receipts.as_ref()),
            Some(self.odds.as_ref()),
            timestamp_ms,
        );
        if !projection.is_complete() {
            return Ok(());
        }
        let Some(marked_positions_value) = projection
            .row_for_symbol("TOTAL")
            .and_then(|row| row.marked_remaining_value_units_6dp)
            .and_then(|units| u128::try_from(units).ok())
        else {
            return Ok(());
        };
        let Some(wallet) = accounting.wallet.as_ref() else {
            return Ok(());
        };
        let Ok(account) = AccountEquitySnapshot::from_units(
            Some(wallet.reconciled_usdc_units_6dp),
            self.risk.current_snapshot().wallet_matic_balance,
            Some(marked_positions_value),
            Some(wallet.projected_pusd_units_6dp),
            timestamp_ms,
        ) else {
            return Ok(());
        };
        self.risk.set_account_equity_snapshot(&account);
        self.risk.confirm_direct_buy_acquisitions(&self.portfolio)
    }

    pub(super) async fn process_supervisor_batch(
        &mut self,
        timestamp_ms: u64,
        order_updates: Vec<OrderUpdateEvent>,
        matched_fills: Vec<crate::portfolio::FillEvent>,
        cancel_requests: Vec<CancelRequest>,
        intents: Vec<crate::types::OrderIntent>,
    ) -> Result<()> {
        let sequence = self.next_supervisor_sequence;
        self.next_supervisor_sequence = sequence
            .checked_add(1)
            .context("candidate supervisor sequence overflow")?;
        let ack = self
            .supervisor()?
            .control()
            .process_batch(
                &self.risk,
                &self.ctf_inventory,
                CapturedExecutionSupervisorBatch {
                    input_sequence: sequence,
                    timestamp_ms,
                    pause: None,
                    order_updates,
                    matched_fills,
                    cancel_requests,
                    intents,
                },
            )
            .await?;
        for record in ack.execution_audit_records {
            self.retain_supervisor_audit_record(record)?;
        }
        Ok(())
    }

    pub(super) async fn shutdown_supervisor(&mut self) -> Result<()> {
        if let Some(supervisor) = self.supervisor.take() {
            supervisor.shutdown().await?;
        }
        Ok(())
    }
}
