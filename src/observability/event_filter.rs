use super::*;

pub(crate) fn should_log_action_event(event: &ActionEvent, log_ticks: bool) -> bool {
    if log_ticks {
        return true;
    }
    !matches!(
        event,
        ActionEvent::OracleTick { .. }
            | ActionEvent::OracleStep { .. }
            | ActionEvent::CexQuote { .. }
            | ActionEvent::StrategyStatus { .. }
    )
}
