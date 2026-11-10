use crate::config::RiskConfig;

use super::snapshot::RiskSnapshot;

pub(super) fn apply_hard_stop_limits(s: &mut RiskSnapshot, cfg: &RiskConfig) {
    if !cfg.enabled || !s.trading_allowed {
        return;
    }

    if cfg.max_drawdown_usd > 0.0 {
        let drawdown_usd = (s.peak_equity - s.current_equity).max(0.0);
        if drawdown_usd >= cfg.max_drawdown_usd {
            s.trading_allowed = false;
            s.freeze_level = 2;
            s.frozen_rule = Some("max_drawdown".to_string());
            s.frozen_details = Some(format!(
                "dd_usd={drawdown_usd:.2} max_usd={:.2} peak={:.2} cur={:.2}",
                cfg.max_drawdown_usd, s.peak_equity, s.current_equity
            ));
            return;
        }
    }
}
