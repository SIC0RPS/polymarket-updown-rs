use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};

use crate::capture::events::ReplayRiskSnapshotCapture;
use crate::risk::RiskSnapshot;

const RISK_SNAPSHOT_FILE: &str = "replay_risk_snapshot.jsonl";

pub(crate) const SIMULATED_BACKTEST_EQUITY_SOURCE: &str = "simulated_backtest";
pub(crate) const SIMULATED_BACKTEST_EQUITY_USDT: f64 = 1_000.0;

#[derive(Debug, Clone)]
pub(crate) struct CapturedCentralBaseline {
    session_ts_ms: u64,
    risk: RiskSnapshot,
    equity_baseline_usdt: f64,
}

impl CapturedCentralBaseline {
    pub(crate) fn load(capture: &Path) -> Result<Self> {
        let path = capture.join(RISK_SNAPSHOT_FILE);
        let file = File::open(&path)
            .with_context(|| format!("open captured central risk baseline {}", path.display()))?;
        let mut first = None;
        for (index, line) in BufReader::new(file).lines().enumerate() {
            let line = line.with_context(|| {
                format!("read captured central risk baseline {}", path.display())
            })?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let row: ReplayRiskSnapshotCapture = serde_json::from_str(line).with_context(|| {
                format!(
                    "decode captured central risk baseline {} line {}",
                    path.display(),
                    index + 1
                )
            })?;
            first = Some(row);
            break;
        }
        let row = first.context("captured central risk baseline has no rows")?;
        validate_capture_row(&row)?;
        anyhow::ensure!(
            row.rest_positions.unwrap_or(0) == 0,
            "captured central baseline contains positions without exact reconstruction"
        );
        anyhow::ensure!(
            row.rest_open_orders.unwrap_or(0) == 0,
            "captured central baseline contains open orders without exact reconstruction"
        );
        anyhow::ensure!(
            row.authoritative_market_exposures.is_empty()
                && row.inflight_buy_notional_by_market.is_empty(),
            "captured central baseline contains exposure state without exact reservation identity"
        );
        let session_ts_ms = row.session_ts_ms;
        let mut risk = row.into_snapshot();
        risk.updated_at_ms = session_ts_ms;
        risk.usdc_balance = Some(SIMULATED_BACKTEST_EQUITY_USDT);
        risk.wallet_usdc_balance = Some(0.0);
        risk.wallet_matic_balance = Some(0.0);
        risk.marked_positions_value = Some(0.0);
        risk.current_wallet_equity = Some(SIMULATED_BACKTEST_EQUITY_USDT);
        risk.trading_collateral_balance = Some(SIMULATED_BACKTEST_EQUITY_USDT);
        risk.account_equity_updated_at_ms = Some(session_ts_ms);
        risk.exchange_allowance = Some(SIMULATED_BACKTEST_EQUITY_USDT);
        risk.peak_equity = SIMULATED_BACKTEST_EQUITY_USDT;
        risk.current_equity = SIMULATED_BACKTEST_EQUITY_USDT;
        risk.last_positive_equity = Some(SIMULATED_BACKTEST_EQUITY_USDT);
        validate_risk_snapshot(&risk, session_ts_ms)?;
        Ok(Self {
            session_ts_ms,
            risk,
            equity_baseline_usdt: SIMULATED_BACKTEST_EQUITY_USDT,
        })
    }

    pub(crate) const fn session_ts_ms(&self) -> u64 {
        self.session_ts_ms
    }

    pub(crate) const fn equity_baseline_usdt(&self) -> f64 {
        self.equity_baseline_usdt
    }

    pub(crate) fn risk_snapshot(&self) -> RiskSnapshot {
        self.risk.clone()
    }
}

fn validate_capture_row(row: &ReplayRiskSnapshotCapture) -> Result<()> {
    anyhow::ensure!(
        row.session_ts_ms > 0 && row.updated_at_ms > 0,
        "captured central risk baseline timestamp is missing"
    );
    Ok(())
}

fn validate_risk_snapshot(snapshot: &RiskSnapshot, session_ts_ms: u64) -> Result<()> {
    anyhow::ensure!(
        snapshot.updated_at_ms <= session_ts_ms,
        "captured central risk baseline update is later than session timestamp"
    );
    for (name, value) in [
        ("peak_equity", snapshot.peak_equity),
        ("current_equity", snapshot.current_equity),
        ("max_order_qty", snapshot.max_order_qty),
        ("max_window_cost", snapshot.max_window_cost),
        ("max_market_equity_pct", snapshot.max_market_equity_pct),
    ] {
        anyhow::ensure!(
            value.is_finite() && value >= 0.0,
            "captured central risk baseline {name} is invalid"
        );
    }
    Ok(())
}
