use std::collections::HashMap;
use std::path::PathBuf;

use super::strategy::MakerRebateMmStrategyConfigWire;
use super::{
    BbwickOracleConfig, BbwickOracleHistoryConfig, BbwickOracleRuntimeConfig,
    BbwickOracleTransportConfig, CurrentBlendedFairValueConfig, LogFormat,
    MakerRebateMmStrategyConfig, ObservabilityConfig, ObservabilityDashboardConfig,
    ObservabilityLoggingConfig, PolymarketConfig, PolymarketDiscoveryConfig,
    PolymarketExecutionConfig, PolymarketMarketDataMode, PolymarketOrderType, RiskConfig,
    POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS,
};

impl Default for CurrentBlendedFairValueConfig {
    fn default() -> Self {
        Self {
            micro_logistic_enabled: true,
            micro_logistic_trade_flow_enabled: false,
            micro_logistic_l2_penalty: 1.0,
            market_calibration_enabled: false,
            market_calibration_min_samples: 8,
            market_calibration_l2_penalty: 1.0,
            prob_calibration_logit_bias: 0.0,
            prob_calibration_logit_scale: 1.0,
            depth_pressure_logit_weight: 0.0,
            min_forecast_sigma2: 1e-8,
            model_blend_min_weight: 0.20,
            model_blend_max_weight: 0.95,
            model_blend_edge_full: 0.08,
            model_blend_toxicity_k: 0.20,
            beta_calibration_enabled: false,
            beta_calibration_min_samples: 8,
            beta_calibration_l2_penalty: 1.0,
            uncertainty_edge_weight: 0.0,
            executable_ev_scale: 0.10,
            adverse_selection_k: 0.0,
            top_loser_adverse_selection_k: 0.0,
        }
    }
}

impl Default for BbwickOracleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            quote_asset: "USDT".to_string(),
            symbols: HashMap::from([
                ("BTCUSDT".to_string(), true),
                ("ETHUSDT".to_string(), true),
                ("SOLUSDT".to_string(), true),
                ("XRPUSDT".to_string(), true),
            ]),
            underlying_symbol_map: HashMap::from([
                ("btc".to_string(), "BTCUSDT".to_string()),
                ("eth".to_string(), "ETHUSDT".to_string()),
                ("sol".to_string(), "SOLUSDT".to_string()),
                ("xrp".to_string(), "XRPUSDT".to_string()),
            ]),
            normalized_underlying_symbol_map: Default::default(),
            transport: BbwickOracleTransportConfig::default(),
            runtime: BbwickOracleRuntimeConfig::default(),
            history: BbwickOracleHistoryConfig::default(),
        }
    }
}

impl Default for BbwickOracleTransportConfig {
    fn default() -> Self {
        Self {
            ws_url: "wss://stream.binance.com:9443/stream".to_string(),
            rest_base_url: "https://api.binance.com".to_string(),
            polymarket_ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string(),
            polymarket_clob_http_url: "https://clob.polymarket.com".to_string(),
            polymarket_use_server_time: false,
            provider_subscribe_trades: true,
            provider_subscribe_top_of_book: true,
            provider_subscribe_klines: true,
            polymarket_subscribe_orderbook: true,
            polymarket_subscribe_prices: true,
            polymarket_subscribe_tick_size: true,
            polymarket_subscribe_last_trade: true,
            step_timeframes: vec!["1m".to_string(), "5m".to_string()],
        }
    }
}

impl Default for BbwickOracleRuntimeConfig {
    fn default() -> Self {
        Self {
            ingest_channel_capacity: 2048,
            tick_channel_capacity: 2048,
        }
    }
}

impl Default for BbwickOracleHistoryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            data_root: PathBuf::from("crates/bbwick_engine/mData"),
            max_bars: 1_000,
        }
    }
}

impl Default for PolymarketExecutionConfig {
    fn default() -> Self {
        Self {
            mode: None,
            pause: true,
            order_type: PolymarketOrderType::default(),
            reclaim_retry_cooldown_secs: 120,
            order_submit_pia_region: "lu".to_string(),
        }
    }
}

impl Default for PolymarketOrderType {
    fn default() -> Self {
        Self::Gtd
    }
}

impl Default for MakerRebateMmStrategyConfig {
    fn default() -> Self {
        Self {
            oracle_buy_and_hold: false,
            max_orders_per_cycle: 15,
            normal_rebalance_interval_ms: 100,
            fast_rebalance_interval_ms: 200,
            base_half_spread: 0.01,
            ranking_book_max_age_ms: 5_000,
            paired_first_managed_sell_book_max_age_ms: 1_500,
            max_oracle_staleness_ms: 1_500,
            entry_cutoff_before_expiry_ms: 70_000,
            ewma_half_life_ms: 2_000,
            oracle_shock_bps: 10.0,
            max_inventory_per_token: 150.0,
            max_inventory_total: 2_000.0,
            markout_windows_ms: vec![1_000, 3_000, 10_000],
            admission_buffer_ticks: 0,
            passive_min_rest_ms: 1_000,
            post_only_reject_cooldown_ms: 2_500,
            post_only_reject_max_extra_buffer_ticks: 2,
            post_only_reject_size_multiplier: 0.5,
            paired_first_managed_sell_slippage_tolerance: 0.05,
            paired_first_managed_sell_aggressiveness: 0.03,
            oracle_side_min_advantage: 0.01,
            oracle_side_flip_hysteresis: 0.005,
            oracle_side_noise_weight: 1.5,
            oracle_side_flip_noise_weight: 1.0,
            oracle_side_cleanup_flip_bump: 0.01,
            oracle_market_confirmation_probability_weight: 0.55,
            oracle_market_confirmation_spread_weight: 0.25,
            oracle_market_confirmation_depth_weight: 0.10,
            oracle_market_confirmation_book_weight: 0.10,
            oracle_ta_boundary_scale: 3.0,
            oracle_ta_boundary_weight: 0.75,
            oracle_ta_token_agreement_weight: 0.25,
            oracle_ta_component_alignment_weight: 0.0,
            ctf_likely_winner_min_confidence: 0.25,
            min_hold_probability_edge: 0.10,
            ctf_remaining_leg_liquidity_drawdown_weight: 1.0,
            ctf_remaining_leg_time_risk_weight: 1.0,
            ctf_sell_limit_delay_cushion_ticks: 1.0,
            ctf_first_managed_sell_min_market_confirmation: 0.50,
            ctf_probability_calibration_min_confidence: 0.35,
            ctf_probability_confidence_market_weight: 0.50,
            ctf_probability_confidence_spread_weight: 0.25,
            ctf_probability_confidence_depth_weight: 0.25,
            ctf_probability_model_market_loser_bid_ambiguity_weight: 0.0,
            live_fair_value_model: CurrentBlendedFairValueConfig::default(),
            protection_dwell_ms: 500,
            markout_window_weights: vec![0.6, 0.3, 0.1],
            enabled_assets: Vec::new(),
            optimizer_symbol_configs: Default::default(),
        }
    }
}

impl Default for MakerRebateMmStrategyConfigWire {
    fn default() -> Self {
        let cfg = MakerRebateMmStrategyConfig::default();
        Self {
            oracle_buy_and_hold: cfg.oracle_buy_and_hold,
            max_orders_per_cycle: cfg.max_orders_per_cycle,
            normal_rebalance_interval_ms: cfg.normal_rebalance_interval_ms,
            fast_rebalance_interval_ms: cfg.fast_rebalance_interval_ms,
            base_half_spread: cfg.base_half_spread,
            ranking_book_max_age_ms: cfg.ranking_book_max_age_ms,
            paired_first_managed_sell_book_max_age_ms: cfg
                .paired_first_managed_sell_book_max_age_ms,
            max_oracle_staleness_ms: cfg.max_oracle_staleness_ms,
            entry_cutoff_before_expiry_ms: cfg.entry_cutoff_before_expiry_ms,
            ewma_half_life_ms: cfg.ewma_half_life_ms,
            oracle_shock_bps: cfg.oracle_shock_bps,
            max_inventory_per_token: cfg.max_inventory_per_token,
            max_inventory_total: cfg.max_inventory_total,
            markout_windows_ms: cfg.markout_windows_ms,
            admission_buffer_ticks: cfg.admission_buffer_ticks,
            passive_min_rest_ms: cfg.passive_min_rest_ms,
            post_only_reject_cooldown_ms: cfg.post_only_reject_cooldown_ms,
            post_only_reject_max_extra_buffer_ticks: cfg.post_only_reject_max_extra_buffer_ticks,
            post_only_reject_size_multiplier: cfg.post_only_reject_size_multiplier,
            paired_first_managed_sell_slippage_tolerance: cfg
                .paired_first_managed_sell_slippage_tolerance,
            paired_first_managed_sell_aggressiveness: cfg.paired_first_managed_sell_aggressiveness,
            oracle_side_min_advantage: cfg.oracle_side_min_advantage,
            oracle_side_flip_hysteresis: cfg.oracle_side_flip_hysteresis,
            oracle_side_noise_weight: cfg.oracle_side_noise_weight,
            oracle_side_flip_noise_weight: cfg.oracle_side_flip_noise_weight,
            oracle_side_cleanup_flip_bump: cfg.oracle_side_cleanup_flip_bump,
            oracle_market_confirmation_probability_weight: cfg
                .oracle_market_confirmation_probability_weight,
            oracle_market_confirmation_spread_weight: cfg.oracle_market_confirmation_spread_weight,
            oracle_market_confirmation_depth_weight: cfg.oracle_market_confirmation_depth_weight,
            oracle_market_confirmation_book_weight: cfg.oracle_market_confirmation_book_weight,
            oracle_ta_boundary_scale: cfg.oracle_ta_boundary_scale,
            oracle_ta_boundary_weight: cfg.oracle_ta_boundary_weight,
            oracle_ta_token_agreement_weight: cfg.oracle_ta_token_agreement_weight,
            oracle_ta_component_alignment_weight: cfg.oracle_ta_component_alignment_weight,
            ctf_likely_winner_min_confidence: cfg.ctf_likely_winner_min_confidence,
            min_hold_probability_edge: cfg.min_hold_probability_edge,
            ctf_remaining_leg_liquidity_drawdown_weight: cfg
                .ctf_remaining_leg_liquidity_drawdown_weight,
            ctf_remaining_leg_time_risk_weight: cfg.ctf_remaining_leg_time_risk_weight,
            ctf_sell_limit_delay_cushion_ticks: cfg.ctf_sell_limit_delay_cushion_ticks,
            ctf_first_managed_sell_min_market_confirmation: cfg
                .ctf_first_managed_sell_min_market_confirmation,
            ctf_probability_calibration_min_confidence: cfg
                .ctf_probability_calibration_min_confidence,
            ctf_probability_confidence_market_weight: cfg.ctf_probability_confidence_market_weight,
            ctf_probability_confidence_spread_weight: cfg.ctf_probability_confidence_spread_weight,
            ctf_probability_confidence_depth_weight: cfg.ctf_probability_confidence_depth_weight,
            ctf_probability_model_market_loser_bid_ambiguity_weight: cfg
                .ctf_probability_model_market_loser_bid_ambiguity_weight,
            live_fair_value_model: cfg.live_fair_value_model,
            protection_dwell_ms: cfg.protection_dwell_ms,
            markout_window_weights: cfg.markout_window_weights,
            enabled_assets: cfg.enabled_assets,
        }
    }
}

impl Default for PolymarketConfig {
    fn default() -> Self {
        Self {
            market_data_enabled: false,
            market_data_mode: PolymarketMarketDataMode::default(),
            execution: PolymarketExecutionConfig::default(),
            account_rpc_url: "https://polygon-bor-rpc.publicnode.com".to_string(),
            clob_http_url: "https://clob.polymarket.com/".to_string(),
            clob_ws_url: "wss://ws-subscriptions-clob.polymarket.com".to_string(),
            gamma_http_url: "https://gamma-api.polymarket.com/".to_string(),
            data_http_url: "https://data-api.polymarket.com".to_string(),
            use_server_time: true,
            token_id: None,
            chain_id: 137,
            private_key_env: "POLYMARKET_PRIVATE_KEY".to_string(),
            wallet_address: String::new(),
            discovery: PolymarketDiscoveryConfig::default(),
        }
    }
}

impl Default for PolymarketMarketDataMode {
    fn default() -> Self {
        Self::Ws
    }
}

impl Default for PolymarketDiscoveryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            query: "5m updown".to_string(),
            poll_interval_ms: 2_000,
            timeout_ms: 30_000,
            window_ms: 300_000,
            window_tolerance_ms: 30_000,
            slug_must_contain: vec!["5m".to_string(), "updown".to_string()],
            underlyings_allowlist: vec![
                "btc".to_string(),
                "eth".to_string(),
                "sol".to_string(),
                "xrp".to_string(),
            ],
            min_time_left_ms: 30_000,
            max_time_left_ms: 15 * 60 * 1000,
            require_order_book: true,
            require_accepting_orders: true,
        }
    }
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            startup_cooldown_ms: 30_000,
            max_open_orders: 128,
            max_window_cost: 50.0,
            max_market_equity_pct: 0.0,
            order_timeout_ms: POLYMARKET_GTD_MIN_EFFECTIVE_LIFETIME_MS,
            min_usdc_balance: 10.0,
            max_drawdown_usd: 20.0,
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            logging: ObservabilityLoggingConfig::default(),
            dashboard: ObservabilityDashboardConfig::default(),
        }
    }
}

impl Default for ObservabilityLoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            stdout: false,
            format: LogFormat::Json,
            dir: "logs/process".to_string(),
            runtime_log_max_bytes: 64 * 1024 * 1024,
            runtime_log_buffered_lines_limit: 8_192,
            flood_control_enabled: true,
            flood_summary_interval_secs: 30,
            flood_recovery_timeout_secs: 60,
            flood_max_tracked_signatures: 4_096,
            action_log_enabled: true,
            action_log_path: "logs/actions.jsonl".to_string(),
            action_log_max_bytes: 128 * 1024 * 1024,
            action_log_flush_every: 1,
            log_ticks: false,
        }
    }
}

impl Default for ObservabilityDashboardConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_host: "127.0.0.1".to_string(),
            port: 3000,
        }
    }
}
