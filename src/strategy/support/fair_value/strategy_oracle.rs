use super::*;
use crate::strategy::support::oracle_authority::probability_interval::ProbabilityInterval;

impl MakerRebateMmStrategy {
    pub(crate) fn pair_books_for_tokens(
        &self,
        odds: &crate::polymarket::state::PolymarketOddsState,
        up_token: &str,
        down_token: &str,
    ) -> Option<(BookTop, BookTop)> {
        self.current_books_for_tokens(odds, up_token, down_token)
    }

    pub(crate) fn oracle_symbol_for_underlying(&self, underlying: &str) -> String {
        self.bbwick_oracle_cfg
            .resolve_symbol_for_underlying(underlying)
    }

    pub fn reset_oracle_epoch_state(&mut self) {
        self.oracle_state = crate::strategy::support::shared::oracle_state::OracleState::default();
        self.current_engine_mono_ns = 0;
        self.last_oracle_px_mode = None;
        self.oracle_price_degraded_mode = None;
        self.last_oracle_volatility = None;
    }

    pub(crate) fn apply_tick_update(
        &mut self,
        tick: &MarketTick,
        now_ms: u64,
        _market_projection: &MarketProjection,
    ) -> Option<()> {
        if !tick.trigger.should_update_oracle() {
            self.current_engine_mono_ns = self.current_engine_mono_ns.max(tick.ingest_mono_ns);
            return Some(());
        }

        let symbol = canonical_symbol(&tick.instrument)?.into_owned();
        if !symbol.eq_ignore_ascii_case(&self.oracle_symbol) {
            return None;
        }
        self.current_engine_mono_ns = self.current_engine_mono_ns.max(tick.ingest_mono_ns);
        let best_bid = (tick.bid.is_finite() && tick.bid > 0.0).then_some(tick.bid);
        let best_ask = (tick.ask.is_finite() && tick.ask > 0.0).then_some(tick.ask);
        let top = crate::strategy::support::shared::oracle_state::OracleTopOfBook::new(
            tick.bid,
            tick.ask,
            tick.bid_sz,
            tick.ask_sz,
            tick.queue_imbalance_l5,
        );
        let best_px = match (best_bid, best_ask, top) {
            (Some(_), Some(_), Some(top)) => {
                note_oracle_price_mode(
                    &mut self.last_oracle_px_mode,
                    &mut self.oracle_price_degraded_mode,
                    &symbol,
                    OraclePxMode::Microprice,
                    tick,
                    now_ms,
                );
                top.microprice()
            }
            (Some(bid), Some(ask), None) if ask >= bid => {
                note_oracle_price_mode(
                    &mut self.last_oracle_px_mode,
                    &mut self.oracle_price_degraded_mode,
                    &symbol,
                    OraclePxMode::MidpointSecondary,
                    tick,
                    now_ms,
                );
                0.5 * (bid + ask)
            }
            (Some(bid), None, _) => {
                note_oracle_price_mode(
                    &mut self.last_oracle_px_mode,
                    &mut self.oracle_price_degraded_mode,
                    &symbol,
                    OraclePxMode::MidpointSecondary,
                    tick,
                    now_ms,
                );
                bid
            }
            (None, Some(ask), _) => {
                note_oracle_price_mode(
                    &mut self.last_oracle_px_mode,
                    &mut self.oracle_price_degraded_mode,
                    &symbol,
                    OraclePxMode::MidpointSecondary,
                    tick,
                    now_ms,
                );
                ask
            }
            _ => return None,
        };
        let ts_ms = ts_ms_from_ns(tick.source_ts_ns)
            .or_else(|| ts_ms_from_ns(tick.ts_ns))
            .unwrap_or(now_ms);
        self.update_oracle_state(
            best_px,
            ts_ms,
            tick.source_ts_kind,
            tick.ingest_mono_ns,
            top,
        );
        Some(())
    }

    pub(crate) fn update_oracle_state(
        &mut self,
        px: f64,
        ts_ms: u64,
        source_ts_kind: SourceTsKind,
        ingest_mono_ns: u64,
        top: Option<crate::strategy::support::shared::oracle_state::OracleTopOfBook>,
    ) {
        if self
            .oracle_state
            .latest_ts_ms
            .is_some_and(|latest_ts_ms| ts_ms < latest_ts_ms)
        {
            return;
        }
        let symbol = self.oracle_symbol.as_str();
        let state = &mut self.oracle_state;
        let diagnostics = self
            .last_oracle_volatility
            .get_or_insert(OracleVolatilityDiagnostics {
                vol_update_dt_ms: None,
                vol_return_bps_raw: None,
                vol_return_bps_clipped: None,
                vol_skip_reason: None,
            });
        let previous_skip_reason = diagnostics.vol_skip_reason;
        *diagnostics = OracleVolatilityDiagnostics {
            vol_update_dt_ms: None,
            vol_return_bps_raw: None,
            vol_return_bps_clipped: None,
            vol_skip_reason: None,
        };
        state.observe_top_of_book(ts_ms, top);
        state.prev_latest_px = state.latest_px;
        state.prev_latest_ts_ms = state.latest_ts_ms;
        state.latest_px = Some(px);
        state.latest_ts_ms = Some(ts_ms);
        state.latest_source_ts_kind = source_ts_kind;
        state.latest_ingest_mono_ns = Some(ingest_mono_ns);
        state.observe_recent_px(ts_ms, px);

        let Some((prev_px, prev_ns)) = state.last_px.zip(state.last_ingest_mono_ns) else {
            (state.last_px, state.last_ingest_mono_ns) = (Some(px), Some(ingest_mono_ns));
            state.last_ts_ms = Some(ts_ms);
            return;
        };
        let bad_px = !px.is_finite() || !prev_px.is_finite() || px <= 0.0 || prev_px <= 0.0;
        let dt_ms = monotonic_dt_ms(ingest_mono_ns, prev_ns);
        state.last_ts_ms = Some(ts_ms);
        if bad_px || dt_ms > 30_000 {
            (state.last_px, state.last_ingest_mono_ns) = (Some(px), Some(ingest_mono_ns));
            state.vol_samples = 0;
            state.last_return_bps = 0.0;
            if bad_px {
                return;
            }
            *diagnostics = OracleVolatilityDiagnostics {
                vol_update_dt_ms: Some(dt_ms),
                vol_return_bps_raw: None,
                vol_return_bps_clipped: None,
                vol_skip_reason: Some(VolSkipReason::DtZeroOrGap),
            };
            if previous_skip_reason != diagnostics.vol_skip_reason {
                tracing::warn!(
                    target: "oracle.volatility",
                    symbol,
                    vol_update_dt_ms = diagnostics.vol_update_dt_ms,
                    vol_skip_reason = ?diagnostics.vol_skip_reason,
                    "oracle_volatility_degraded"
                );
            }
            return;
        }

        let dt_s = (dt_ms as f64) / 1_000.0;
        if dt_s <= 0.0 {
            return;
        }
        let raw_return_bps = ((px / prev_px) - 1.0) * 10_000.0;
        state.last_return_bps = raw_return_bps;
        diagnostics.vol_update_dt_ms = Some(dt_ms);
        diagnostics.vol_return_bps_raw = Some(raw_return_bps);
        if dt_ms < ORACLE_VOL_UPDATE_MIN_DT_MS {
            diagnostics.vol_return_bps_clipped = Some(raw_return_bps);
            diagnostics.vol_skip_reason = Some(VolSkipReason::DtBelowFloor);
            if previous_skip_reason != diagnostics.vol_skip_reason {
                tracing::debug!(
                    target: "oracle.volatility",
                    symbol,
                    vol_update_dt_ms = diagnostics.vol_update_dt_ms,
                    vol_skip_reason = ?diagnostics.vol_skip_reason,
                    "oracle_volatility_skip"
                );
            }
            return;
        }
        (state.last_px, state.last_ingest_mono_ns) = (Some(px), Some(ingest_mono_ns));
        let capped_return_bps = raw_return_bps.clamp(
            -ORACLE_MAX_ABS_RETURN_BPS_PER_UPDATE,
            ORACLE_MAX_ABS_RETURN_BPS_PER_UPDATE,
        );
        diagnostics.vol_return_bps_clipped = Some(capped_return_bps);
        let return_ratio = 1.0 + capped_return_bps / 10_000.0;
        if !(return_ratio.is_finite() && return_ratio > 0.0) {
            diagnostics.vol_skip_reason = Some(VolSkipReason::InvalidReturnRatio);
            if previous_skip_reason != diagnostics.vol_skip_reason {
                tracing::warn!(
                    target: "oracle.volatility",
                    symbol,
                    vol_update_dt_ms = diagnostics.vol_update_dt_ms,
                    vol_return_bps_raw = diagnostics.vol_return_bps_raw,
                    vol_skip_reason = ?diagnostics.vol_skip_reason,
                    "oracle_volatility_degraded"
                );
            }
            return;
        }
        let r = return_ratio.ln();
        if !r.is_finite() {
            diagnostics.vol_skip_reason = Some(VolSkipReason::InvalidLogReturn);
            if previous_skip_reason != diagnostics.vol_skip_reason {
                tracing::warn!(
                    target: "oracle.volatility",
                    symbol,
                    vol_update_dt_ms = diagnostics.vol_update_dt_ms,
                    vol_return_bps_raw = diagnostics.vol_return_bps_raw,
                    vol_skip_reason = ?diagnostics.vol_skip_reason,
                    "oracle_volatility_degraded"
                );
            }
            return;
        }
        if previous_skip_reason != diagnostics.vol_skip_reason
            || (raw_return_bps - capped_return_bps).abs() > f64::EPSILON
        {
            tracing::debug!(
                target: "oracle.volatility",
                symbol,
                vol_update_dt_ms = diagnostics.vol_update_dt_ms,
                vol_return_bps_raw = diagnostics.vol_return_bps_raw,
                vol_return_bps_clipped = diagnostics.vol_return_bps_clipped,
                vol_skip_reason = ?diagnostics.vol_skip_reason,
                "oracle_volatility_update"
            );
        }

        let vol2_est = (r * r) / dt_s;
        let half_life_s = (self.cfg.ewma_half_life_ms as f64) / 1_000.0;
        let alpha = 1.0 - (-std::f64::consts::LN_2 * dt_s / half_life_s).exp();
        state.sigma2 = (1.0 - alpha) * state.sigma2 + alpha * vol2_est;
        state.vol_samples = state.vol_samples.saturating_add(1);
    }

    pub(crate) fn market_oracle_input(
        &self,
        market: &MarketSelection,
        now_ms: u64,
    ) -> Option<(String, u64, f64, f64, f64)> {
        let symbol = self.oracle_symbol_for_underlying(&market.underlying);
        if !symbol.eq_ignore_ascii_case(&self.oracle_symbol) {
            return None;
        }
        if now_ms < market.window_start_ms {
            return None;
        }
        let state = &self.oracle_state;
        let ts_ms = state.latest_ts_ms?;
        let px = state.latest_px?;
        let latest_ingest_mono_ns = state.latest_ingest_mono_ns?;
        if ingest_age_ms(self.current_engine_mono_ns, latest_ingest_mono_ns)?
            > self.cfg.max_oracle_staleness_ms
        {
            return None;
        }
        Some((
            self.oracle_symbol.clone(),
            ts_ms,
            px,
            state.sigma2,
            state.last_return_bps,
        ))
    }

    pub(crate) fn required_oracle_symbols_for_status(
        &mut self,
        _market_projection: &MarketProjection,
    ) -> Vec<String> {
        vec![self.oracle_symbol.clone()]
    }

    pub(crate) fn ready_oracle_symbols_for_status(
        &self,
        _now_ms: u64,
        required: &[String],
    ) -> Vec<String> {
        let mut ready: Vec<String> = required
            .iter()
            .filter(|symbol| {
                symbol
                    .eq_ignore_ascii_case(&self.oracle_symbol)
                    .then_some(self.oracle_state.latest_ingest_mono_ns)
                    .flatten()
                    .and_then(|ingest_mono_ns| {
                        ingest_age_ms(self.current_engine_mono_ns, ingest_mono_ns)
                    })
                    .is_some_and(|age_ms| age_ms <= self.cfg.max_oracle_staleness_ms)
            })
            .cloned()
            .collect();
        ready.sort();
        ready.dedup();
        ready
    }

    pub(crate) fn executable_unwind_value(&self, top: &BookTop) -> Option<f64> {
        let bid = top.bid.filter(|bid| bid.is_finite() && *bid > 0.0)?;
        let bid_after_slippage =
            (bid * (1.0 - self.cfg.paired_first_managed_sell_slippage_tolerance)).clamp(0.0, 1.0);
        Some(bid_after_slippage)
    }

    pub(crate) fn market_live_fair_result(
        &mut self,
        market: &MarketSelection,
        up: &BookTop,
        down: &BookTop,
        up_depth: Option<&BookDepth>,
        down_depth: Option<&BookDepth>,
        now_ms: u64,
    ) -> Option<FairValueResult> {
        self.market_oracle_input(market, now_ms)?;
        self.market_calibrated_live_fair_result(market, up, down, up_depth, down_depth, now_ms)
            .map(|(_, fair_result, _)| fair_result)
    }

    pub(crate) fn market_live_fair_result_with_market_probability(
        &mut self,
        market: &MarketSelection,
        up: &BookTop,
        down: &BookTop,
        up_depth: Option<&BookDepth>,
        down_depth: Option<&BookDepth>,
        now_ms: u64,
        p_market: Option<f64>,
        market_interval: Option<ProbabilityInterval>,
    ) -> Option<(FairValueResult, Option<ProbabilityInterval>)> {
        let (_, ts_ms, oracle_px, sigma2, last_return_bps) =
            self.market_oracle_input(market, now_ms)?;

        let open_ctx = self.upsert_window_open_px(market, ts_ms, oracle_px)?;
        let penalty_surface = live_market_penalty_surface(
            now_ms,
            up.observed_at_ms,
            down.observed_at_ms,
            market.window_end_ms,
            self.cfg.ranking_book_max_age_ms,
            self.cfg.hard_entry_gate_ms(),
        );
        let late_conf_penalty = OPEN_PX_LATE_MAX_CONFIDENCE_PENALTY * open_ctx.open_bracket_penalty;
        let late_toxicity_penalty =
            OPEN_PX_LATE_MAX_TOXICITY_PENALTY * open_ctx.open_bracket_penalty;
        let shock_toxicity = Self::shock_toxicity(last_return_bps, self.cfg.oracle_shock_bps)
            + late_toxicity_penalty
            + penalty_surface.stale_soft_penalty
            + penalty_surface.close_window_penalty;
        let markout_toxicity = self
            .markout_toxicity_at(market.up_token_id(), now_ms)
            .max(self.markout_toxicity_at(market.down_token_id(), now_ms));
        let fair_ctx = FairValueContext {
            spot_px: oracle_px,
            open_px: open_ctx.open_px,
            horizon_s: (penalty_surface.time_left_ms as f64) / 1_000.0,
            sigma2,
            p_market,
            spot_l1_imbalance: self
                .oracle_state
                .latest_top
                .and_then(|top| top.queue_imbalance()),
            depth_pressure: market_depth_pressure(up_depth, down_depth),
            shock_toxicity,
            markout_toxicity,
            confidence_scale: (1.0 - late_conf_penalty).clamp(0.0, 1.0),
        };
        let fair_result = FairValueEngine::new()
            .compute_live_with_config(&fair_ctx, &self.cfg.live_fair_value_model);
        let base_interval = market_interval.and_then(|interval| {
            let model_point = ProbabilityInterval::new(fair_result.p_model, fair_result.p_model)?;
            CurrentBlended::new(&self.cfg.live_fair_value_model).blend_probability_interval(
                model_point,
                fair_result.p_fair,
                p_market,
                Some(interval),
                fair_ctx.shock_toxicity,
                fair_ctx.markout_toxicity,
                1.0,
            )
        });
        Some((fair_result, base_interval))
    }

    pub(crate) fn exit_fairs_for_market(
        &mut self,
        market: &MarketSelection,
        up: &BookTop,
        down: &BookTop,
        up_depth: Option<&BookDepth>,
        down_depth: Option<&BookDepth>,
        now_ms: u64,
    ) -> Option<PairedFirstManagedSellFairValues> {
        let p_up = self
            .market_live_fair_result(market, up, down, up_depth, down_depth, now_ms)?
            .p_fair;
        Some(PairedFirstManagedSellFairValues {
            up_fair: p_up.clamp(0.0, 1.0),
            down_fair: (1.0 - p_up).clamp(0.0, 1.0),
        })
    }

    pub(crate) fn upsert_window_open_px(
        &mut self,
        market: &MarketSelection,
        ts_ms: u64,
        oracle_px: f64,
    ) -> Option<OpenWindowContext> {
        if ts_ms < market.window_start_ms {
            return Some(OpenWindowContext {
                open_px: oracle_px,
                open_delay_ms: 0,
                open_bracket_penalty: 0.0,
            });
        }

        if !self.open_px_by_market.contains_key(&market.market_id)
            && ts_ms >= market.window_start_ms
        {
            let state = self
                .oracle_symbol_for_underlying(&market.underlying)
                .eq_ignore_ascii_case(&self.oracle_symbol)
                .then_some(&self.oracle_state);
            let interpolated = state.and_then(|oracle_state| {
                let t_minus = oracle_state.prev_latest_ts_ms?;
                let p_minus = oracle_state.prev_latest_px?;
                let t_plus = oracle_state.latest_ts_ms?;
                let p_plus = oracle_state.latest_px?;
                if !(t_minus < market.window_start_ms
                    && market.window_start_ms <= t_plus
                    && p_minus.is_finite()
                    && p_plus.is_finite()
                    && p_minus > 0.0
                    && p_plus > 0.0)
                {
                    return None;
                }
                let span_ms = t_plus.saturating_sub(t_minus);
                if span_ms == 0 {
                    return None;
                }
                let left_gap_ms = market.window_start_ms.saturating_sub(t_minus);
                let right_gap_ms = t_plus.saturating_sub(market.window_start_ms);
                interpolate_log_price(t_minus, p_minus, t_plus, p_plus, market.window_start_ms).map(
                    |open_px| OpenPriceState {
                        px: open_px,
                        span_ms,
                        capture_mode: OpenCaptureMode::BracketInterpolated,
                        left_gap_ms: Some(left_gap_ms),
                        right_gap_ms: Some(right_gap_ms),
                        asymmetry_ms: Some(left_gap_ms.abs_diff(right_gap_ms)),
                    },
                )
            });
            let open_state = interpolated.unwrap_or(OpenPriceState {
                px: oracle_px,
                span_ms: ts_ms.saturating_sub(market.window_start_ms),
                capture_mode: OpenCaptureMode::PostOpenProjection,
                left_gap_ms: None,
                right_gap_ms: Some(ts_ms.saturating_sub(market.window_start_ms)),
                asymmetry_ms: None,
            });
            self.open_px_by_market
                .insert(market.market_id.clone(), open_state);
            tracing::debug!(
                target: "oracle.open_capture",
                market_id = %market.market_id,
                open_capture_mode = ?open_state.capture_mode,
                open_left_gap_ms = open_state.left_gap_ms,
                open_right_gap_ms = open_state.right_gap_ms,
                open_bracket_span_ms = open_state.span_ms,
                open_bracket_asymmetry_ms = open_state.asymmetry_ms,
                "open_capture_initialized"
            );
        }

        let open_state = self.open_px_by_market.get(&market.market_id).copied()?;
        let open_delay_ms = open_state.span_ms;
        let open_bracket_penalty = open_bracket_penalty(
            open_delay_ms,
            matches!(open_state.capture_mode, OpenCaptureMode::PostOpenProjection),
        );
        let alert_state = (matches!(open_state.capture_mode, OpenCaptureMode::PostOpenProjection)
            || open_bracket_penalty > 0.0)
            .then_some(OpenCaptureAlertState {
                capture_mode: open_state.capture_mode,
                penalty_bucket: open_capture_penalty_bucket(open_bracket_penalty),
            });
        note_open_capture_diagnostic(
            &mut self.open_capture_diagnostic_by_market,
            &market.market_id,
            alert_state,
            open_state,
            open_bracket_penalty,
            ts_ms,
        );
        Some(OpenWindowContext {
            open_px: open_state.px,
            open_delay_ms,
            open_bracket_penalty,
        })
    }
}
