use super::*;

pub(super) fn live_strategy_cfg(
    shared_strategy_cfg: &SharedStrategyConfig,
) -> crate::config::MakerRebateMmStrategyConfig {
    shared_strategy_cfg.borrow().clone()
}

pub(super) fn is_unsafe_buy_without_close_hint(intent: &OrderIntent) -> bool {
    intent.side() == Side::Buy
        && (intent.post_only
            || intent.time_in_force.is_immediate()
            || intent.buy_authorization().is_none_or(|authorization| {
                authorization.validate().is_err()
                    || intent.execution_intent_id != Some(authorization.intent_id)
                    || intent.instrument != authorization.selected_token().to_string()
                    || intent
                        .time_in_force
                        .expiration_ms()
                        .is_some_and(|expiry| expiry != authorization.market_end_ms)
            }))
}

pub(super) fn is_ctf_inventory_quote_group(intent: &OrderIntent) -> bool {
    intent
        .quote_group
        .as_deref()
        .is_some_and(|group| group.starts_with("ctf_inventory:"))
}

pub(super) fn intent_market_id(intent: &OrderIntent) -> Option<MarketId> {
    intent.market_id.clone()
}

pub(super) fn market_selection_for_intent(
    intent: &OrderIntent,
    market_projection: Option<&crate::polymarket::state::MarketProjection>,
) -> Option<MarketSelection> {
    let market = intent_market_id(intent)?;
    market_projection?.market_by_id(market.as_str()).cloned()
}

pub(super) fn filter_buy_entry_intents(
    intents: Vec<OrderIntent>,
    blocked: bool,
    kind: &str,
    now_ms: u64,
    risk: &RiskHandle,
    ctf_inventory_runtime: &Mutex<CtfInventoryRuntime>,
    submission_recovery: &SubmissionRecovery,
) -> Result<(Vec<OrderIntent>, usize)> {
    if !blocked {
        return Ok((intents, 0));
    }
    let mut allowed = Vec::with_capacity(intents.len());
    let mut blocked_count = 0;
    let mut first_error = None;
    for intent in intents {
        if intent.side() == Side::Buy {
            blocked_count += 1;
            if let Err(error) = record_pre_submission_abandoned(
                risk,
                ctf_inventory_runtime,
                submission_recovery,
                &intent,
                kind,
                now_ms,
                &mut Vec::new(),
            ) {
                first_error.get_or_insert(error);
            }
        } else {
            allowed.push(intent);
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    Ok((allowed, blocked_count))
}
