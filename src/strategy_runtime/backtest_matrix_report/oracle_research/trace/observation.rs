use serde_json::Value;

use crate::oracle_resolution::ChainlinkReferenceEvidence;

#[derive(Debug, PartialEq)]
pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) struct Observation {
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) observation_ms:
        Option<u64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) snapshot_ms:
        Option<u64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) spot_z: Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) trade_flow_imbalance_5s:
        Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) trade_flow_enabled:
        Option<bool>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) reference:
        Option<ChainlinkReferenceEvidence>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) partial_reference:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) learner_p: Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) learner_prior_p:
        Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) learner_prior_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) learner_interval:
        (Option<f64>, Option<f64>),
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) learner_interval_fields_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) market_raw_p:
        Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) market_prior_p:
        Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) original_base_p:
        Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) base_blend_p:
        Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) post_logistic_p:
        Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) final_p: Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) fit_available:
        Option<bool>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) fit_sample_count:
        Option<u64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) fit_sample_count_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) prediction_valid:
        Option<bool>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) trusted: Option<bool>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) attempted: bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) applied: bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) contributing: bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) application_policy:
        Option<String>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) application_policy_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) phase: Option<String>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) phase_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) phase_sample_count:
        Option<u64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) phase_sample_count_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) phase_trusted:
        Option<bool>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) phase_trusted_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) phase_p0: Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) phase_p0_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) phase_p1: Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) phase_p1_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) trust_source:
        Option<String>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) trust_source_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) trust_sample_count:
        Option<u64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) trust_sample_count_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) trust_trusted:
        Option<bool>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) trust_trusted_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) first_sample_count:
        Option<u64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) first_sample_count_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) probability_calibration_attempted:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) probability_calibration_effective:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) beta_calibration_attempted:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) beta_calibration_applied:
        Option<bool>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) oracle_mode:
        Option<String>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) oracle_signal_side:
        Option<String>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) oracle_signal_side_field_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) learning_enabled:
        Option<bool>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) direct_market_selected:
        Option<bool>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) effective_market_p:
        Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) effective_market_interval:
        (Option<f64>, Option<f64>),
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) effective_market_interval_fields_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) final_interval:
        (Option<f64>, Option<f64>),
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) final_interval_fields_present:
        bool,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) beta_observation_p:
        Option<f64>,
    pub(in crate::strategy_runtime::backtest_matrix_report::oracle_research) confidence_transform_applied:
        Option<bool>,
}

pub(super) fn parse(value: &Value) -> Observation {
    let (reference, partial_reference) = chainlink_reference(value);
    Observation {
        observation_ms: option_u64(value, "micro_logistic_observation_ts_ms"),
        snapshot_ms: option_u64(value, "snapshot_last_eval_ts_ms"),
        spot_z: option_f64(value, "spot_z"),
        trade_flow_imbalance_5s: option_f64(value, "trade_flow_imbalance_5s")
            .filter(|flow| (-1.0..=1.0).contains(flow)),
        trade_flow_enabled: bool_field(value, "micro_logistic_trade_flow_enabled"),
        reference,
        partial_reference,
        learner_p: option_f64(value, "micro_logistic_p_up"),
        learner_prior_p: option_f64(value, "micro_logistic_prior_p_up"),
        learner_prior_field_present: fields_present(value, &["micro_logistic_prior_p_up"]),
        learner_interval: (
            option_f64(value, "micro_logistic_interval_low"),
            option_f64(value, "micro_logistic_interval_high"),
        ),
        learner_interval_fields_present: fields_present(
            value,
            &[
                "micro_logistic_interval_low",
                "micro_logistic_interval_high",
            ],
        ),
        market_raw_p: option_f64(value, "market_raw_p_up"),
        market_prior_p: option_f64(value, "market_calibrated_p_up"),
        original_base_p: f64_field(value, "base_model_p_up"),
        base_blend_p: f64_field(value, "base_blended_p_up"),
        post_logistic_p: f64_field(value, "post_logistic_blend_p_up"),
        final_p: option_f64(value, "final_p_up"),
        fit_available: bool_field(value, "micro_logistic_fit_available"),
        fit_sample_count: option_u64(value, "micro_logistic_fit_sample_count"),
        fit_sample_count_field_present: field(value, "micro_logistic_fit_sample_count").is_some(),
        prediction_valid: bool_field(value, "micro_logistic_prediction_valid"),
        trusted: option_bool(value, "micro_logistic_prequentially_trusted"),
        attempted: bool_field(value, "micro_logistic_application_attempted").unwrap_or(false),
        applied: bool_field(value, "micro_logistic_applied").unwrap_or(false),
        contributing: bool_field(value, "micro_logistic_effective_contribution").unwrap_or(false),
        application_policy: option_string(value, "micro_logistic_application_policy"),
        application_policy_field_present: field(value, "micro_logistic_application_policy")
            .is_some(),
        phase: option_string(value, "micro_logistic_phase"),
        phase_field_present: field(value, "micro_logistic_phase").is_some(),
        phase_sample_count: option_u64(value, "micro_logistic_phase_sample_count"),
        phase_sample_count_field_present: field(value, "micro_logistic_phase_sample_count")
            .is_some(),
        phase_trusted: option_bool(value, "micro_logistic_phase_trusted"),
        phase_trusted_field_present: field(value, "micro_logistic_phase_trusted").is_some(),
        phase_p0: option_f64(value, "micro_logistic_phase_p0"),
        phase_p0_field_present: field(value, "micro_logistic_phase_p0").is_some(),
        phase_p1: option_f64(value, "micro_logistic_phase_p1"),
        phase_p1_field_present: field(value, "micro_logistic_phase_p1").is_some(),
        trust_source: option_string(value, "micro_logistic_trust_source"),
        trust_source_field_present: field(value, "micro_logistic_trust_source").is_some(),
        trust_sample_count: option_u64(value, "micro_logistic_trust_sample_count"),
        trust_sample_count_field_present: field(value, "micro_logistic_trust_sample_count")
            .is_some(),
        trust_trusted: option_bool(value, "micro_logistic_trust_trusted"),
        trust_trusted_field_present: field(value, "micro_logistic_trust_trusted").is_some(),
        first_sample_count: option_u64(value, "micro_logistic_first_sample_count"),
        first_sample_count_field_present: field(value, "micro_logistic_first_sample_count")
            .is_some(),
        probability_calibration_attempted: option_f64(
            value,
            "probability_calibration_attempted_p_up",
        )
        .is_some(),
        probability_calibration_effective: option_f64(
            value,
            "probability_calibration_effective_p_up",
        )
        .is_some(),
        beta_calibration_attempted: option_f64(value, "beta_calibration_attempted_p_up").is_some(),
        beta_calibration_applied: bool_field(value, "beta_calibration_applied"),
        oracle_mode: string(value, "oracle_mode"),
        oracle_signal_side: option_string(value, "oracle_signal_side"),
        oracle_signal_side_field_present: field(value, "oracle_signal_side").is_some(),
        learning_enabled: bool_field(value, "oracle_learning_enabled"),
        direct_market_selected: bool_field(value, "direct_market_selected"),
        effective_market_p: option_f64(value, "effective_market_p_up"),
        effective_market_interval: (
            option_f64(value, "effective_market_interval_low"),
            option_f64(value, "effective_market_interval_high"),
        ),
        effective_market_interval_fields_present: fields_present(
            value,
            &[
                "effective_market_interval_low",
                "effective_market_interval_high",
            ],
        ),
        final_interval: (
            option_f64(value, "final_interval_low"),
            option_f64(value, "final_interval_high"),
        ),
        final_interval_fields_present: fields_present(
            value,
            &["final_interval_low", "final_interval_high"],
        ),
        beta_observation_p: option_f64(value, "final_beta_observation_p_up"),
        confidence_transform_applied: bool_field(value, "confidence_transform_applied"),
    }
}

fn chainlink_reference(value: &Value) -> (Option<ChainlinkReferenceEvidence>, bool) {
    const FIELDS: [&str; 12] = [
        "chainlink_symbol",
        "chainlink_topic",
        "chainlink_window_s",
        "chainlink_open_price_e18",
        "chainlink_open_source_ts_ms",
        "chainlink_open_observed_ts_ms",
        "chainlink_open_capture_sequence",
        "chainlink_current_price_e18",
        "chainlink_current_source_ts_ms",
        "chainlink_current_observed_ts_ms",
        "chainlink_current_capture_sequence",
        "chainlink_reference_log_return",
    ];
    if !FIELDS.iter().any(|name| optional(value, name).is_some()) {
        return (None, false);
    }
    let evidence = (|| {
        Some(ChainlinkReferenceEvidence {
            symbol: option_string(value, "chainlink_symbol")?,
            topic: option_string(value, "chainlink_topic")?,
            window_s: option_u64(value, "chainlink_window_s")?,
            open_price_e18: option_i128(value, "chainlink_open_price_e18")?,
            open_source_ts_ms: option_u64(value, "chainlink_open_source_ts_ms")?,
            open_observed_ts_ms: option_u64(value, "chainlink_open_observed_ts_ms")?,
            open_capture_sequence: option_u64(value, "chainlink_open_capture_sequence")?,
            current_price_e18: option_i128(value, "chainlink_current_price_e18")?,
            current_source_ts_ms: option_u64(value, "chainlink_current_source_ts_ms")?,
            current_observed_ts_ms: option_u64(value, "chainlink_current_observed_ts_ms")?,
            current_capture_sequence: option_u64(value, "chainlink_current_capture_sequence")?,
            log_return: option_f64(value, "chainlink_reference_log_return")?,
        })
    })();
    let partial = evidence.is_none();
    (evidence, partial)
}

pub(super) fn field<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    value.get("fields")?.get(name)
}

fn fields_present(value: &Value, names: &[&str]) -> bool {
    names.iter().all(|name| field(value, name).is_some())
}

pub(super) fn string(value: &Value, name: &str) -> Option<String> {
    field(value, name)?.as_str().map(ToString::to_string)
}

pub(super) fn bool_field(value: &Value, name: &str) -> Option<bool> {
    let value = field(value, name)?;
    value.as_bool().or_else(|| value.as_str()?.parse().ok())
}

pub(super) fn u64_field(value: &Value, name: &str) -> Option<u64> {
    parse_u64(field(value, name)?)
}

fn f64_field(value: &Value, name: &str) -> Option<f64> {
    parse_f64(field(value, name)?)
}

pub(super) fn option_u64(value: &Value, name: &str) -> Option<u64> {
    match optional(value, name)? {
        Decoded::Json(value) => value.as_u64(),
        Decoded::Text(value) => value.parse().ok(),
    }
}

fn option_i128(value: &Value, name: &str) -> Option<i128> {
    match optional(value, name)? {
        Decoded::Json(value) => value.as_i64().map(i128::from),
        Decoded::Text(value) => value.parse().ok(),
    }
}

pub(super) fn option_f64(value: &Value, name: &str) -> Option<f64> {
    let value = match optional(value, name)? {
        Decoded::Json(value) => value.as_f64(),
        Decoded::Text(value) => value.parse().ok(),
    }?;
    value.is_finite().then_some(value)
}

pub(super) fn option_bool(value: &Value, name: &str) -> Option<bool> {
    match optional(value, name)? {
        Decoded::Json(value) => value.as_bool(),
        Decoded::Text(value) => value.parse().ok(),
    }
}

pub(super) fn option_string(value: &Value, name: &str) -> Option<String> {
    match optional(value, name)? {
        Decoded::Json(value) => value.as_str().map(ToString::to_string),
        Decoded::Text(value) => Some(value.trim_matches('"').to_string()),
    }
}

enum Decoded<'a> {
    Json(&'a Value),
    Text(&'a str),
}

fn optional<'a>(value: &'a Value, name: &str) -> Option<Decoded<'a>> {
    let value = field(value, name)?;
    let Some(raw) = value.as_str() else {
        return (!value.is_null()).then_some(Decoded::Json(value));
    };
    let raw = raw.trim();
    if raw == "None" {
        return None;
    }
    let inner = raw
        .strip_prefix("Some(")
        .and_then(|value| value.strip_suffix(')'))
        .unwrap_or(raw);
    Some(Decoded::Text(inner.trim_matches('"')))
}

pub(super) fn parse_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str()?.trim_matches('"').parse().ok())
}

fn parse_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.trim_matches('"').parse().ok())
        .filter(|value| value.is_finite())
}
